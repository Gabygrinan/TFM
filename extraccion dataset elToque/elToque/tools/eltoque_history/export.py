#!/usr/bin/env python3
"""
elToque Historical Exchange Rates Exporter
==========================================

Downloads daily exchange rates from the elToque TRMI API and exports them to CSV.

API docs: https://tasas.eltoque.com/static/swagger.json
Rate limit: 1 request per second
Max query window: 24 hours per request

Usage:
    python export.py --out data/eltoque_history.csv
    python export.py --out data/eltoque_history.csv --start 2023-01-01
    python export.py --out data/eltoque_history.csv --start 2021-01-01 --end 2026-03-06
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library is required. Install with: pip install requests")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_BASE = "https://tasas.eltoque.com"
TRMI_ENDPOINT = f"{API_BASE}/v1/trmi"

# The API enforces 1 request/second.  We add a margin to avoid 429s.
REQUEST_DELAY_SECONDS = 1.5

# Earliest date with data (determined empirically)
EARLIEST_DATE = date(2021, 1, 1)

# All currencies historically observed in API responses
ALL_KNOWN_CURRENCIES = ["USD", "ECU", "MLC", "USDT_TRC20", "BTC", "TRX", "BNB"]

# Headers required to pass Cloudflare and authenticate
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Retry configuration
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2  # seconds, exponential backoff


# ---------------------------------------------------------------------------
# Token loader
# ---------------------------------------------------------------------------

def load_token(token_path: str | None = None) -> str:
    """Load the JWT token from the credential file or environment variable.

    Priority:
      1. ELTOQUE_API_TOKEN env var
      2. Explicit file path
      3. Default location (clave api/claveapi.rtf)
    """
    # 1) Environment variable
    env_token = os.environ.get("ELTOQUE_API_TOKEN")
    if env_token:
        return env_token.strip()

    # 2) Explicit path or default
    if token_path is None:
        # Walk up from this script to the workspace root
        script_dir = Path(__file__).resolve().parent
        # tools/eltoque_history/ -> workspace root
        workspace_root = script_dir.parent.parent
        token_path = workspace_root / "clave api" / "claveapi.rtf"
    else:
        token_path = Path(token_path)

    if not token_path.exists():
        print(f"ERROR: Token file not found at {token_path}")
        print("Set ELTOQUE_API_TOKEN env var or provide --token-file path.")
        sys.exit(1)

    raw = token_path.read_text(encoding="utf-8", errors="ignore")

    # The RTF file contains the JWT as the main text content.
    # Extract it by looking for the JWT pattern (three base64 segments separated by dots).
    import re
    jwt_pattern = re.compile(r"(eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)")
    match = jwt_pattern.search(raw)
    if not match:
        print(f"ERROR: Could not extract JWT token from {token_path}")
        sys.exit(1)

    return match.group(1)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

class ElToqueClient:
    """Thin wrapper around the elToque TRMI API."""

    def __init__(self, token: str, delay: float = REQUEST_DELAY_SECONDS):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json",
        })
        self.delay = delay
        self._last_request_time: float = 0.0

    def _throttle(self) -> None:
        """Ensure we respect the API rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def fetch_day(self, day: date) -> dict[str, Any]:
        """Fetch TRMI data for a single calendar day.

        Returns the parsed JSON dict or an empty dict on failure.
        """
        self._throttle()

        params = {
            "date_from": f"{day.isoformat()} 00:00:01",
            "date_to": f"{day.isoformat()} 23:59:01",
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._last_request_time = time.time()
                resp = self.session.get(TRMI_ENDPOINT, params=params, timeout=30)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    print(f"  ⏳ Rate limited (429). Retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})...")
                    time.sleep(wait)
                    continue

                if resp.status_code in (401, 422):
                    print(f"  ❌ Auth error {resp.status_code} for {day}. Check token.")
                    return {}

                if resp.status_code == 400:
                    # Bad request – likely invalid date range (DST, etc.). Skip.
                    print(f"  ⚠️  HTTP 400 for {day}. Skipping (bad request).")
                    return {}

                # Other errors – retry with backoff
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"  ⚠️  HTTP {resp.status_code} for {day}. Retrying in {wait}s...")
                time.sleep(wait)

            except requests.RequestException as exc:
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"  ⚠️  Request error for {day}: {exc}. Retrying in {wait}s...")
                time.sleep(wait)

        print(f"  ❌ Failed to fetch {day} after {MAX_RETRIES} attempts.")
        return {}


# ---------------------------------------------------------------------------
# Extraction logic
# ---------------------------------------------------------------------------

def extract_history(
    client: ElToqueClient,
    start: date,
    end: date,
    checkpoint_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Iterate day-by-day and collect rates.

    If *checkpoint_path* is given, intermediate results are flushed every
    50 days so that progress isn't lost on interruption.
    """
    rows: list[dict[str, Any]] = []
    all_currencies: set[str] = set()

    # Load checkpoint if exists
    if checkpoint_path and checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            saved = json.load(f)
            rows = saved.get("rows", [])
            all_currencies = set(saved.get("currencies", []))
            if rows:
                last_date = date.fromisoformat(rows[-1]["date"])
                if last_date >= start:
                    start = last_date + timedelta(days=1)
                    print(f"  ♻️  Resuming from checkpoint. Continuing from {start}")

    total_days = (end - start).days + 1
    current = start

    print(f"\n📅 Fetching {total_days} days: {start} → {end}\n")

    day_index = 0
    while current <= end:
        day_index += 1
        pct = (day_index / total_days) * 100

        data = client.fetch_day(current)
        tasas = data.get("tasas", {})

        if tasas:
            row = {"date": current.isoformat()}
            for currency, value in tasas.items():
                row[currency] = value
                all_currencies.add(currency)
            rows.append(row)
            symbols = ", ".join(f"{k}={v}" for k, v in tasas.items())
            print(f"  [{day_index}/{total_days}] {pct:5.1f}% | {current} ✅ {symbols}")
        else:
            print(f"  [{day_index}/{total_days}] {pct:5.1f}% | {current} — (no data)")

        # Checkpoint every 50 days
        if checkpoint_path and day_index % 50 == 0:
            _save_checkpoint(checkpoint_path, rows, all_currencies)

        current += timedelta(days=1)

    # Final checkpoint save
    if checkpoint_path:
        _save_checkpoint(checkpoint_path, rows, all_currencies)

    return rows


def _save_checkpoint(path: Path, rows: list, currencies: set) -> None:
    """Persist intermediate progress to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"rows": rows, "currencies": sorted(currencies)}, f)
    print(f"  💾 Checkpoint saved ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict], output_path: str) -> None:
    """Write collected rows to a CSV file.

    Columns are: date, then each currency alphabetically.
    Extra computed columns:
      - EUR_USD_pair: cross rate ECU_CUP / USD_CUP  (≈ EUR/USD forex pair)
      - spread_USD_EUR, spread_USD_MLC, spread_USD_USDT (difference in CUP).
    """
    if not rows:
        print("⚠️  No data to write.")
        return

    # Determine all currency columns present
    currency_cols: set[str] = set()
    for row in rows:
        currency_cols.update(k for k in row if k != "date")

    # Sort columns: date first, then alphabetically
    sorted_currencies = sorted(currency_cols)
    fieldnames = ["date"] + sorted_currencies

    # Add derived columns
    derived = []
    if "USD" in currency_cols and "ECU" in currency_cols:
        derived.append("EUR_USD_pair")
        derived.append("spread_USD_EUR")
    if "USD" in currency_cols and "MLC" in currency_cols:
        derived.append("spread_USD_MLC")
    if "USD" in currency_cols and "USDT_TRC20" in currency_cols:
        derived.append("spread_USD_USDT")
    fieldnames += sorted(derived)

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for row in rows:
            # Compute derived columns
            if "EUR_USD_pair" in fieldnames:
                usd = row.get("USD")
                ecu = row.get("ECU")
                if usd is not None and ecu is not None and usd != 0:
                    row["EUR_USD_pair"] = round(ecu / usd, 6)
            if "spread_USD_EUR" in fieldnames:
                usd = row.get("USD")
                ecu = row.get("ECU")
                if usd is not None and ecu is not None:
                    row["spread_USD_EUR"] = round(usd - ecu, 4)
            if "spread_USD_MLC" in fieldnames:
                usd = row.get("USD")
                mlc = row.get("MLC")
                if usd is not None and mlc is not None:
                    row["spread_USD_MLC"] = round(usd - mlc, 4)
            if "spread_USD_USDT" in fieldnames:
                usd = row.get("USD")
                usdt = row.get("USDT_TRC20")
                if usd is not None and usdt is not None:
                    row["spread_USD_USDT"] = round(usd - usdt, 4)
            writer.writerow(row)

    print(f"\n✅ CSV written: {out_path}")
    print(f"   Rows:     {len(rows)}")
    print(f"   Columns:  {', '.join(fieldnames)}")
    print(f"   Date range: {rows[0]['date']} → {rows[-1]['date']}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_date(s: str) -> date:
    """Parse YYYY-MM-DD string to date."""
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{s}'. Use YYYY-MM-DD.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export historical daily exchange rates from elToque TRMI API to CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python export.py --out data/eltoque_history.csv
  python export.py --out data/eltoque_history.csv --start 2023-01-01
  python export.py --out data/eltoque_history.csv --start 2021-01-01 --end 2026-03-05
  ELTOQUE_API_TOKEN=eyJ... python export.py --out data/eltoque_history.csv
        """,
    )

    parser.add_argument(
        "--out", "-o",
        required=True,
        help="Output CSV file path (e.g. data/eltoque_history.csv)",
    )
    parser.add_argument(
        "--start", "-s",
        type=parse_date,
        default=EARLIEST_DATE,
        help=f"Start date in YYYY-MM-DD (default: {EARLIEST_DATE})",
    )
    parser.add_argument(
        "--end", "-e",
        type=parse_date,
        default=date.today() - timedelta(days=1),
        help="End date in YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--token-file",
        default=None,
        help="Path to the token file (default: auto-detect from 'clave api/claveapi.rtf')",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY_SECONDS,
        help=f"Seconds between requests (default: {REQUEST_DELAY_SECONDS})",
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpoint saving (not recommended for large ranges)",
    )

    args = parser.parse_args()

    # Validate date range
    if args.start < EARLIEST_DATE:
        print(f"⚠️  Adjusting start date to earliest available: {EARLIEST_DATE}")
        args.start = EARLIEST_DATE

    if args.end > date.today():
        args.end = date.today()

    if args.start > args.end:
        print(f"ERROR: Start date ({args.start}) is after end date ({args.end}).")
        sys.exit(1)

    # Load token
    print("🔑 Loading API token...")
    token = load_token(args.token_file)
    print(f"   Token loaded (length: {len(token)} chars)")

    # Verify token with a quick test
    print("🔍 Verifying token with test request...")
    client = ElToqueClient(token, delay=args.delay)
    test_data = client.fetch_day(date.today() - timedelta(days=1))
    if not test_data.get("tasas"):
        test_data = client.fetch_day(date.today() - timedelta(days=2))
    if test_data.get("tasas"):
        currencies = list(test_data["tasas"].keys())
        print(f"   ✅ Token valid. Available currencies: {', '.join(currencies)}")
    else:
        print("   ⚠️  Token verification returned no data. Proceeding anyway...")

    # Checkpoint file
    checkpoint_path = None
    if not args.no_checkpoint:
        out_p = Path(args.out)
        checkpoint_path = out_p.parent / f".{out_p.stem}_checkpoint.json"

    # Extract
    rows = extract_history(client, args.start, args.end, checkpoint_path)

    # Write CSV
    write_csv(rows, args.out)

    # Clean up checkpoint
    if checkpoint_path and checkpoint_path.exists():
        checkpoint_path.unlink()
        print("   🧹 Checkpoint file removed.")

    total_days = (args.end - args.start).days + 1
    print(f"\n🏁 Done! Fetched {len(rows)} days with data out of {total_days} calendar days.")


if __name__ == "__main__":
    main()

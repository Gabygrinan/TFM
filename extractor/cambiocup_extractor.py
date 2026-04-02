#!/usr/bin/env python3
"""
CambioCUP Historical Exchange Rate Extractor
=============================================
Extracts historical exchange rate data from cambiocup.com's public API.

Data source: https://www.cambiocup.com/api/history?coin={COIN}&days={DAYS}
Behind it: Supabase "exchange" table, populated every ~10 min by a cron
           that fetches from QvaPay P2P API and stores (avg_buy + avg_sell) / 2.

The API returns data sorted ascending by date, with a Supabase default limit
of 1000 rows. This extractor uses a sliding-window pagination strategy:
  1. Request a large window to get the earliest 1000 rows.
  2. Note the last timestamp returned.
  3. Compute a new 'days' parameter so the window starts just after that timestamp.
  4. Repeat until we reach the present.

Output: CSV with all individual data points (not just daily averages).
        Optionally also a daily-aggregated CSV and Parquet files.

Author: TFM - Mercado cambiario informal cubano
"""

import argparse
import csv
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "https://www.cambiocup.com/api/history"
COINS = {
    "CUP":      {"coin_id": 1, "description": "Peso Cubano (CUP)"},
    "MLC":      {"coin_id": 2, "description": "Moneda Libremente Convertible (MLC)"},
    "CLASICA":  {"coin_id": 3, "description": "Tarjeta Clásica"},
    "ETECSA":   {"coin_id": 4, "description": "Saldo ETECSA"},
    "TROPICAL": {"coin_id": 5, "description": "BANDEC Prepago (Tropical)"},
}
SUPABASE_ROW_LIMIT = 1000  # Default Supabase row limit
REQUEST_DELAY = 1.0        # seconds between API calls (be polite)
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0        # exponential backoff base

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("cambiocup")
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


# ---------------------------------------------------------------------------
# API fetching with retries
# ---------------------------------------------------------------------------
def fetch_history(coin: str, days: int, logger: logging.Logger) -> list[dict]:
    """
    Fetch history from cambiocup.com/api/history.
    Returns list of {time: unix_ts, value: float} dicts, ascending by time.
    """
    url = f"{BASE_URL}?coin={coin}&days={days}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.debug(f"  Request: coin={coin}, days={days} (attempt {attempt})")
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("data", [])
            logger.debug(f"  Got {len(records)} records")
            return records
        except requests.exceptions.RequestException as e:
            wait = RETRY_BACKOFF ** attempt
            logger.warning(f"  Request failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"  Retrying in {wait:.0f}s...")
                time.sleep(wait)
            else:
                logger.error(f"  Max retries reached for coin={coin}, days={days}")
                return []


def fetch_all_history_for_coin(coin: str, logger: logging.Logger) -> list[dict]:
    """
    Paginate through the entire history for a coin using sliding windows.

    Strategy:
    - The API returns up to 1000 rows, ascending from (now - days*86400).
    - Start with days = very large number (e.g., 100000) to get the earliest data.
    - After each batch, compute how many days ago the last record was.
    - Set days to that value (minus a small overlap) and repeat.
    - Stop when we get fewer than 1000 rows (meaning we've reached the end).
    """
    all_records = []
    seen_times = set()
    now_ts = time.time()

    # Start with the maximum possible window
    current_days = 100000
    page = 0

    logger.info(f"Fetching history for {coin}...")

    while True:
        page += 1
        records = fetch_history(coin, current_days, logger)

        if not records:
            logger.info(f"  No more records for {coin} (page {page})")
            break

        # Deduplicate and add new records
        new_count = 0
        for r in records:
            t = r["time"]
            if t not in seen_times:
                seen_times.add(t)
                all_records.append(r)
                new_count += 1

        logger.info(
            f"  Page {page}: got {len(records)} records, {new_count} new "
            f"(total: {len(all_records)})"
        )

        # If we got fewer than the limit, we've reached the present
        if len(records) < SUPABASE_ROW_LIMIT:
            logger.info(f"  Reached end of data for {coin}")
            break

        # If no new records were added, we're stuck
        if new_count == 0:
            logger.warning(f"  No new records in page {page}, breaking to avoid loop")
            break

        # Calculate new window: the last record's timestamp
        last_ts = records[-1]["time"]
        # How many days ago is the last record?
        days_ago = (now_ts - last_ts) / 86400.0
        # Set window to start just slightly before the last record
        # Subtract a small buffer (0.5 days) to ensure overlap for dedup
        current_days = max(1, math.ceil(days_ago - 0.5))

        logger.debug(
            f"  Last record at {datetime.fromtimestamp(last_ts, tz=timezone.utc)}, "
            f"~{days_ago:.1f} days ago. Next window: days={current_days}"
        )

        # Be polite to the server
        time.sleep(REQUEST_DELAY)

    # Sort by time
    all_records.sort(key=lambda r: r["time"])
    logger.info(
        f"  {coin} complete: {len(all_records)} total records"
    )
    if all_records:
        earliest = datetime.fromtimestamp(all_records[0]["time"], tz=timezone.utc)
        latest = datetime.fromtimestamp(all_records[-1]["time"], tz=timezone.utc)
        logger.info(f"  Date range: {earliest.date()} → {latest.date()}")

    return all_records


# ---------------------------------------------------------------------------
# QvaPay API (for enrichment - individual offers for current snapshot)
# ---------------------------------------------------------------------------
QVAPAY_URL = "https://api.qvapay.com/p2p/completed_pairs_average"
QVAPAY_COIN_MAP = {
    "CUP": "BANK_CUP",
    "MLC": "BANK_MLC",
    "CLASICA": "CLASICA",
    "ETECSA": "ETECSA",
    "TROPICAL": "BANDECPREPAGO",
}


def fetch_qvapay_current(coin: str, logger: logging.Logger) -> dict | None:
    """Fetch current QvaPay P2P data (includes individual offers)."""
    qvapay_coin = QVAPAY_COIN_MAP.get(coin)
    if not qvapay_coin:
        return None
    try:
        resp = requests.get(
            f"{QVAPAY_URL}?coin={qvapay_coin}", timeout=15
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"  QvaPay fetch failed for {coin}: {e}")
        return None


# ---------------------------------------------------------------------------
# Data processing
# ---------------------------------------------------------------------------
def records_to_dataframe(records: list[dict], coin: str) -> pd.DataFrame:
    """Convert raw API records to a pandas DataFrame."""
    if not records:
        return pd.DataFrame(columns=["datetime_utc", "timestamp", "coin", "value"])

    df = pd.DataFrame(records)
    df["coin"] = coin
    df["datetime_utc"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df.rename(columns={"time": "timestamp"})
    df = df[["datetime_utc", "timestamp", "coin", "value"]]
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def compute_daily_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Compute daily OHLC-style aggregates from raw data."""
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["date"] = df["datetime_utc"].dt.date

    daily = (
        df.groupby(["date", "coin"])
        .agg(
            open=("value", "first"),
            high=("value", "max"),
            low=("value", "min"),
            close=("value", "last"),
            mean=("value", "mean"),
            median=("value", "median"),
            std=("value", "std"),
            count=("value", "count"),
        )
        .reset_index()
    )
    daily["std"] = daily["std"].fillna(0)
    return daily


# ---------------------------------------------------------------------------
# Export functions
# ---------------------------------------------------------------------------
def save_csv(df: pd.DataFrame, filepath: str, logger: logging.Logger):
    df.to_csv(filepath, index=False, float_format="%.6f")
    logger.info(f"Saved CSV: {filepath} ({len(df)} rows)")


def save_parquet(df: pd.DataFrame, filepath: str, logger: logging.Logger):
    try:
        df.to_parquet(filepath, index=False, engine="pyarrow")
        logger.info(f"Saved Parquet: {filepath} ({len(df)} rows)")
    except Exception as e:
        logger.warning(f"Could not save Parquet {filepath}: {e}")


# ---------------------------------------------------------------------------
# Main extraction pipeline
# ---------------------------------------------------------------------------
def run_extraction(
    coins: list[str],
    output_dir: str,
    include_parquet: bool,
    include_qvapay: bool,
    verbose: bool,
):
    logger = setup_logging(verbose)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("CambioCUP Historical Exchange Rate Extractor")
    logger.info("=" * 60)
    logger.info(f"Coins: {', '.join(coins)}")
    logger.info(f"Output directory: {out.resolve()}")
    logger.info(f"Include Parquet: {include_parquet}")
    logger.info("")

    all_raw_dfs = []
    all_daily_dfs = []
    summary = {}

    for coin in coins:
        logger.info("-" * 40)
        logger.info(f"Processing: {coin}")
        logger.info("-" * 40)

        # Fetch all history from cambiocup.com API
        records = fetch_all_history_for_coin(coin, logger)

        if not records:
            logger.warning(f"No data retrieved for {coin}")
            summary[coin] = {"status": "NO DATA", "records": 0}
            continue

        # Build DataFrame
        df_raw = records_to_dataframe(records, coin)
        all_raw_dfs.append(df_raw)

        # Daily aggregates
        df_daily = compute_daily_aggregates(df_raw)
        all_daily_dfs.append(df_daily)

        # Per-coin files
        coin_lower = coin.lower()
        save_csv(df_raw, str(out / f"raw_{coin_lower}.csv"), logger)
        save_csv(df_daily, str(out / f"daily_{coin_lower}.csv"), logger)

        if include_parquet:
            save_parquet(df_raw, str(out / f"raw_{coin_lower}.parquet"), logger)
            save_parquet(df_daily, str(out / f"daily_{coin_lower}.parquet"), logger)

        earliest = df_raw["datetime_utc"].min()
        latest = df_raw["datetime_utc"].max()
        summary[coin] = {
            "status": "OK",
            "records": len(df_raw),
            "daily_records": len(df_daily),
            "earliest": str(earliest.date()),
            "latest": str(latest.date()),
            "days_span": (latest - earliest).days,
        }

        time.sleep(REQUEST_DELAY)

    # Combined files
    if all_raw_dfs:
        df_all_raw = pd.concat(all_raw_dfs, ignore_index=True)
        df_all_raw = df_all_raw.sort_values(["coin", "timestamp"]).reset_index(drop=True)
        save_csv(df_all_raw, str(out / "raw_all_coins.csv"), logger)
        if include_parquet:
            save_parquet(df_all_raw, str(out / "raw_all_coins.parquet"), logger)

    if all_daily_dfs:
        df_all_daily = pd.concat(all_daily_dfs, ignore_index=True)
        df_all_daily = df_all_daily.sort_values(["coin", "date"]).reset_index(drop=True)
        save_csv(df_all_daily, str(out / "daily_all_coins.csv"), logger)
        if include_parquet:
            save_parquet(df_all_daily, str(out / "daily_all_coins.parquet"), logger)

    # QvaPay snapshot (individual offers at this moment)
    if include_qvapay:
        logger.info("")
        logger.info("Fetching current QvaPay P2P snapshots...")
        qvapay_rows = []
        snapshot_time = datetime.now(timezone.utc).isoformat()
        for coin in coins:
            qdata = fetch_qvapay_current(coin, logger)
            if qdata:
                # Save individual offers
                offers = qdata.get("offers", [])
                for offer_val in offers:
                    qvapay_rows.append({
                        "snapshot_utc": snapshot_time,
                        "coin": coin,
                        "offer_value": offer_val,
                        "average_buy": qdata.get("average_buy"),
                        "average_sell": qdata.get("average_sell"),
                        "average": qdata.get("average"),
                        "median_buy": qdata.get("median_buy"),
                        "median_sell": qdata.get("median_sell"),
                    })
                logger.info(
                    f"  {coin}: {len(offers)} individual offers, "
                    f"avg_buy={qdata.get('average_buy'):.4f}, "
                    f"avg_sell={qdata.get('average_sell'):.4f}"
                )
            time.sleep(0.5)

        if qvapay_rows:
            df_qvapay = pd.DataFrame(qvapay_rows)
            save_csv(df_qvapay, str(out / "qvapay_snapshot.csv"), logger)
            if include_parquet:
                save_parquet(df_qvapay, str(out / "qvapay_snapshot.parquet"), logger)

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("=" * 60)
    for coin, info in summary.items():
        if info["status"] == "OK":
            logger.info(
                f"  {coin:10s} | {info['records']:>7,} records | "
                f"{info['daily_records']:>5,} days | "
                f"{info['earliest']} → {info['latest']} "
                f"({info['days_span']} days)"
            )
        else:
            logger.info(f"  {coin:10s} | NO DATA")

    # Save summary as JSON
    with open(out / "extraction_summary.json", "w") as f:
        json.dump(
            {
                "extraction_time": datetime.now(timezone.utc).isoformat(),
                "source": "https://www.cambiocup.com/api/history",
                "coins": summary,
            },
            f,
            indent=2,
        )
    logger.info(f"\nSummary saved to: {out / 'extraction_summary.json'}")
    logger.info("Done!")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Extract historical exchange rates from CambioCUP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract all coins to ./output/
  python cambiocup_extractor.py

  # Extract only CUP and MLC, with Parquet output
  python cambiocup_extractor.py --coins CUP MLC --parquet

  # Extract all coins + QvaPay snapshot, verbose
  python cambiocup_extractor.py --qvapay --verbose

  # Custom output directory
  python cambiocup_extractor.py -o ./data/cambiocup/
        """,
    )
    parser.add_argument(
        "--coins",
        nargs="+",
        choices=list(COINS.keys()),
        default=list(COINS.keys()),
        help="Coins to extract (default: all)",
    )
    parser.add_argument(
        "-o", "--output",
        default="./output",
        help="Output directory (default: ./output)",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="Also save Parquet files (requires pyarrow)",
    )
    parser.add_argument(
        "--qvapay",
        action="store_true",
        help="Also fetch current QvaPay P2P snapshot with individual offers",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()
    run_extraction(
        coins=args.coins,
        output_dir=args.output,
        include_parquet=args.parquet,
        include_qvapay=args.qvapay,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()

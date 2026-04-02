#!/usr/bin/env python3
"""
CambioCUP Periodic Collector
=============================
Lightweight script to collect current exchange rates every N minutes
and append to a growing CSV. Designed to run as a cron job or background
process for building forward-looking history.

Usage:
  # One-shot (run from crontab every 10 min):
  python periodic_collector.py

  # Continuous mode (runs forever, collects every 10 min):
  python periodic_collector.py --continuous --interval 600

Crontab example (every 10 minutes):
  */10 * * * * /path/to/venv/bin/python /path/to/periodic_collector.py -o /path/to/data/
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
QVAPAY_URL = "https://api.qvapay.com/p2p/completed_pairs_average"
CAMBIOCUP_API = "https://www.cambiocup.com/api"

COINS = {
    "CUP": "BANK_CUP",
    "MLC": "BANK_MLC",
    "CLASICA": "CLASICA",
    "ETECSA": "ETECSA",
    "TROPICAL": "BANDECPREPAGO",
}

# CSV headers
RATE_HEADERS = [
    "datetime_utc", "timestamp", "coin",
    "cambiocup_value",
    "qvapay_avg_buy", "qvapay_avg_sell", "qvapay_average",
    "qvapay_median_buy", "qvapay_median_sell",
]

OFFERS_HEADERS = [
    "datetime_utc", "timestamp", "coin", "offer_value",
]

logger = logging.getLogger("collector")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def fetch_cambiocup_current() -> dict:
    """Fetch latest values from cambiocup.com/api."""
    try:
        resp = requests.get(CAMBIOCUP_API, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        result = {}
        for key, coin in [
            ("cupHistory", "CUP"),
            ("mlcHistory", "MLC"),
            ("clasicaHistory", "CLASICA"),
            ("etecsaHistory", "ETECSA"),
            ("bandecprepagoHistory", "TROPICAL"),
        ]:
            entries = data.get(key, [])
            if entries:
                result[coin] = entries[0]["value"]  # Most recent
        return result
    except Exception as e:
        logger.error(f"CambioCUP API error: {e}")
        return {}


def fetch_qvapay(coin: str) -> dict | None:
    """Fetch current QvaPay P2P data for a coin."""
    qvapay_coin = COINS.get(coin)
    if not qvapay_coin:
        return None
    try:
        resp = requests.get(
            f"{QVAPAY_URL}?coin={qvapay_coin}", timeout=15
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"QvaPay error for {coin}: {e}")
        return None


def ensure_csv(filepath: Path, headers: list):
    """Create CSV file with headers if it doesn't exist."""
    if not filepath.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def collect_once(output_dir: Path):
    """Perform one collection cycle."""
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    now_ts = int(now.timestamp())

    rates_file = output_dir / "periodic_rates.csv"
    offers_file = output_dir / "periodic_offers.csv"

    ensure_csv(rates_file, RATE_HEADERS)
    ensure_csv(offers_file, OFFERS_HEADERS)

    # Get CambioCUP current values
    cambiocup = fetch_cambiocup_current()

    # Get QvaPay data per coin
    for coin, qvapay_coin in COINS.items():
        qdata = fetch_qvapay(coin)

        row = {
            "datetime_utc": now_iso,
            "timestamp": now_ts,
            "coin": coin,
            "cambiocup_value": cambiocup.get(coin, ""),
            "qvapay_avg_buy": qdata.get("average_buy", "") if qdata else "",
            "qvapay_avg_sell": qdata.get("average_sell", "") if qdata else "",
            "qvapay_average": qdata.get("average", "") if qdata else "",
            "qvapay_median_buy": qdata.get("median_buy", "") if qdata else "",
            "qvapay_median_sell": qdata.get("median_sell", "") if qdata else "",
        }

        with open(rates_file, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=RATE_HEADERS)
            writer.writerow(row)

        # Individual offers from QvaPay
        if qdata and "offers" in qdata:
            with open(offers_file, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=OFFERS_HEADERS)
                for offer_val in qdata["offers"]:
                    writer.writerow({
                        "datetime_utc": now_iso,
                        "timestamp": now_ts,
                        "coin": coin,
                        "offer_value": offer_val,
                    })

        logger.info(
            f"  {coin}: cambiocup={cambiocup.get(coin, 'N/A')}, "
            f"qvapay_avg={qdata.get('average', 'N/A') if qdata else 'N/A'}, "
            f"offers={len(qdata.get('offers', [])) if qdata else 0}"
        )
        time.sleep(0.5)

    logger.info(f"Collection complete. Files: {rates_file}, {offers_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Periodic collector for CambioCUP exchange rates"
    )
    parser.add_argument(
        "-o", "--output",
        default="./output",
        help="Output directory (default: ./output)",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously instead of one-shot",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=600,
        help="Collection interval in seconds for continuous mode (default: 600 = 10 min)",
    )

    args = parser.parse_args()
    setup_logging()
    output_dir = Path(args.output)

    if args.continuous:
        logger.info(f"Starting continuous collection every {args.interval}s...")
        while True:
            try:
                logger.info("--- Collection cycle ---")
                collect_once(output_dir)
            except Exception as e:
                logger.error(f"Collection error: {e}")
            time.sleep(args.interval)
    else:
        collect_once(output_dir)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Fetch DTB3 (3-Month T-Bill Secondary Market Rate) from FRED and save as CSV.

DTB3 is the standard risk-free rate used in Black-Scholes pricing.

Uses the official FRED API (api.stlouisfed.org), which requires a free API
key in the FRED_API_KEY environment variable. Get one at
https://fredaccount.stlouisfed.org/apikeys. We deliberately avoid the public
fredgraph.csv graph endpoint: it is fronted by Akamai, which silently drops
requests from datacenter IPs (e.g. our Hetzner VPS).

Examples:
  # Incremental (default: fetch what's missing)
  python fetch_rates.py

  # Full history from 1954
  python fetch_rates.py --full-history

  # Specific range
  python fetch_rates.py --start 2020-01-01 --end 2023-12-31

  # Dry run
  python fetch_rates.py --dry-run
"""

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("airflow.task")

FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
SERIES_ID = "DTB3"
DATASET_START = date(1954, 1, 4)


def get_latest_downloaded_date(files_dir: Path) -> date | None:
    """Return the max end-date embedded in existing filenames, or None.

    Filename format: DTB3_{start}_{end}.csv
    """
    latest: date | None = None
    for f in files_dir.glob("*.csv"):
        parts = f.name.removesuffix(".csv").split("_")
        try:
            d = date.fromisoformat(parts[2])
            if latest is None or d > latest:
                latest = d
        except (IndexError, ValueError):
            pass
    return latest


def fetch_fred(start: date, end: date) -> list[tuple[date, float]]:
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise SystemExit(
            "FRED_API_KEY is not set. Get a free key at "
            "https://fredaccount.stlouisfed.org/apikeys and add it to the environment."
        )

    log.info("Downloading DTB3 from FRED API...")
    resp = requests.get(
        FRED_API_URL,
        params={
            "series_id": SERIES_ID,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": start.isoformat(),
            "observation_end": end.isoformat(),
        },
        timeout=60,
    )
    resp.raise_for_status()

    rows = []
    for obs in resp.json().get("observations", []):
        if obs.get("value") in (None, "", "."):
            continue
        try:
            d = date.fromisoformat(obs["date"])
            v = float(obs["value"])
        except (KeyError, ValueError):
            continue
        rows.append((d, v))

    return rows


def parse_args():
    p = argparse.ArgumentParser(
        description="Download DTB3 daily rates from FRED and save as CSV."
    )
    p.add_argument("--start", help="Start date YYYY-MM-DD")
    p.add_argument("--end", help="End date YYYY-MM-DD, inclusive (default: yesterday)")
    p.add_argument("--output-dir", help="Output directory (default: ./data/rates)")
    p.add_argument("--full-history", action="store_true", help="Fetch from 1954-01-04")
    p.add_argument("--dry-run", action="store_true", help="Print range only, no download")
    return p.parse_args()


def main():
    args = parse_args()

    output_dir = Path(args.output_dir or "./data/rates")
    files_dir = output_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    end_date = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)

    if args.full_history:
        start_date = date.fromisoformat(args.start) if args.start else DATASET_START
    elif args.start:
        start_date = date.fromisoformat(args.start)
    else:
        latest = get_latest_downloaded_date(files_dir)
        if latest:
            start_date = latest + timedelta(days=1)
            log.info(f"Latest file ended at {latest}; fetching from {start_date}")
        else:
            start_date = DATASET_START
            log.info(f"No existing files; fetching full history from {start_date}")

    if start_date > end_date:
        log.info(f"Nothing to fetch: start {start_date} is after end {end_date}")
        return

    log.info(f"Series:  {SERIES_ID}  (FRED — free, no cost)")
    log.info(f"Range:   {start_date} → {end_date}")
    log.info(f"Output:  {files_dir}")

    if args.dry_run:
        log.info("Dry run complete. No data downloaded.")
        return

    rows = fetch_fred(start_date, end_date)
    if not rows:
        log.info("No data returned for this date range.")
        return

    out_file = files_dir / f"{SERIES_ID}_{start_date}_{end_date}.csv"
    with out_file.open("w") as f:
        f.write("trade_date,dtb3\n")
        for d, v in rows:
            f.write(f"{d},{v}\n")

    log.info(f"fetch_rates done: {len(rows)} rows → {out_file}")


if __name__ == "__main__":
    main()

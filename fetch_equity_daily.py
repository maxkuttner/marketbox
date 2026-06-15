#!/usr/bin/env python3
"""
Fetch historical daily OHLCV equity data via Databento (DBEQ.BASIC).

Always prints estimated cost; aborts if it exceeds --max-cost.

Examples:
  export DATABENTO_API_KEY="..."

  # Incremental: fetch only what's missing (default)
  python fetch_equity_daily.py

  # Different symbol
  python fetch_equity_daily.py --symbol AAPL

  # Specific range
  python fetch_equity_daily.py --symbol SPY --start 2025-01-01 --end 2025-12-31

  # Full history from dataset start
  python fetch_equity_daily.py --full-history

  # Cost estimate only, no download
  python fetch_equity_daily.py --dry-run
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import databento as db
from databento.common.error import BentoClientError
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("airflow.task")

load_dotenv()

DATASET = "ARCX.PILLAR"
SCHEMA = "ohlcv-1d"
DEFAULT_MAX_COST = 1.00


def get_client() -> db.Historical:
    if not os.environ.get("DATABENTO_API_KEY"):
        raise RuntimeError("Missing DATABENTO_API_KEY environment variable.")
    return db.Historical()


def get_dataset_start(client: db.Historical) -> date:
    r = client.metadata.get_dataset_range(dataset=DATASET)
    return datetime.fromisoformat(r["start"].replace("Z", "+00:00")).date()


def get_dataset_end(client: db.Historical) -> date:
    """Databento's last available (inclusive) date for our schema.

    Read the per-schema range for SCHEMA: the daily bars lag the dataset-level
    range (which tracks higher-frequency schemas like trades/mbo), so using the
    dataset-level end would overshoot into data_schema_not_fully_available. The
    schema end is exclusive, so the last available bar is the day before it.
    """
    r = client.metadata.get_dataset_range(dataset=DATASET)
    rng = r.get("schema", {}).get(SCHEMA, r)  # fall back to dataset-level range
    exclusive_end = datetime.fromisoformat(rng["end"].replace("Z", "+00:00")).date()
    return exclusive_end - timedelta(days=1)


def estimate_cost(client: db.Historical, symbol: str, start: date, end: date) -> float:
    cost = client.metadata.get_cost(
        dataset=DATASET,
        symbols=[symbol],
        stype_in="raw_symbol",
        schema=SCHEMA,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),  # Databento end is exclusive
    )
    return float(cost)


def get_latest_downloaded_date(files_dir: Path) -> date | None:
    """Return the max end-date embedded in existing filenames, or None.

    Filename format: {SYMBOL}_{SCHEMA}_{start}_{end}.dbn.zst
    """
    latest: date | None = None
    for f in files_dir.glob("*.dbn.zst"):
        parts = f.name.removesuffix(".dbn.zst").split("_")
        try:
            d = date.fromisoformat(parts[3])
            if latest is None or d > latest:
                latest = d
        except (IndexError, ValueError):
            pass
    return latest


def parse_args():
    p = argparse.ArgumentParser(
        description="Download historical daily equity OHLCV data via Databento."
    )
    p.add_argument("--symbol", default="SPY", help="Equity symbol (default: SPY)")
    p.add_argument("--start", help="Start date YYYY-MM-DD")
    p.add_argument("--end", help="End date YYYY-MM-DD, inclusive (default: Databento's last available date)")
    p.add_argument("--output-dir", help="Output directory")
    p.add_argument("--full-history", action="store_true", help="Fetch from dataset start")
    p.add_argument(
        "--max-cost",
        type=float,
        default=DEFAULT_MAX_COST,
        help=f"Abort if estimated cost exceeds this USD amount (default: ${DEFAULT_MAX_COST:.2f})",
    )
    p.add_argument("--dry-run", action="store_true", help="Print cost estimate only, no download")
    return p.parse_args()


def main():
    args = parse_args()

    symbol = args.symbol.upper()
    output_dir = Path(args.output_dir or f"./data/{symbol.lower()}_equity")
    files_dir = output_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    client = get_client()

    # Default end: Databento's last available date (never today — the data
    # lags real time, and querying past it errors).
    end_date = date.fromisoformat(args.end) if args.end else get_dataset_end(client)

    if args.full_history:
        start_date = date.fromisoformat(args.start) if args.start else get_dataset_start(client)
    elif args.start:
        start_date = date.fromisoformat(args.start)
    else:
        latest = get_latest_downloaded_date(files_dir)
        if latest:
            start_date = latest + timedelta(days=1)
            log.info(f"Latest file ended at {latest}; fetching from {start_date}")
        else:
            start_date = get_dataset_start(client)
            log.info(f"No existing files; fetching full history from {start_date}")

    if start_date > end_date:
        log.info(
            f"Up to date: Databento's last available date is {end_date}, "
            "already stored. Nothing to fetch."
        )
        return

    log.info(f"Dataset: {DATASET}")
    log.info(f"Symbol:  {symbol}")
    log.info(f"Schema:  {SCHEMA}")
    log.info(f"Range:   {start_date} → {end_date}")
    log.info(f"Output:  {files_dir}")

    log.info("Estimating cost...")
    try:
        cost = estimate_cost(client, symbol, start_date, end_date)
    except BentoClientError as e:
        if "data_no_data_found_for_request" in str(e):
            log.info("No data available for this date range; will retry tomorrow.")
            return
        raise

    log.info(f"Estimated cost: ${cost:.6f}")

    if cost > args.max_cost:
        log.error(
            f"Estimated cost ${cost:.6f} exceeds --max-cost ${args.max_cost:.2f}. "
            "Aborting. Pass a higher --max-cost if this is intentional."
        )
        sys.exit(1)

    if args.dry_run:
        log.info("Dry run complete. No data downloaded.")
        return

    out_file = files_dir / f"{symbol}_{SCHEMA}_{start_date}_{end_date}.dbn.zst"
    log.info(f"Downloading → {out_file}")

    try:
        client.timeseries.get_range(
            dataset=DATASET,
            symbols=[symbol],
            stype_in="raw_symbol",
            schema=SCHEMA,
            start=start_date.isoformat(),
            end=(end_date + timedelta(days=1)).isoformat(),  # exclusive end
            path=out_file,
        )
    except BentoClientError as e:
        if "data_no_data_found_for_request" in str(e):
            log.info("No data available for this date range; will retry tomorrow.")
            return
        raise

    log.info(f"fetch_equity_daily done: {out_file}")


if __name__ == "__main__":
    main()

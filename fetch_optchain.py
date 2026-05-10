#!/usr/bin/env python3
"""
Fetch historical OPRA option-chain data via Databento batch API.

Examples:
  export DATABENTO_API_KEY="..."

  # Last 14 days
  python fetch_optchain.py --symbol SPY

  # Specific range
  python fetch_optchain.py --symbol SPY --start 2026-04-01 --end 2026-05-01

  # From dataset start
  python fetch_optchain.py --symbol SPY --full-history

  # Cost guarded
  python fetch_optchain.py --symbol SPY --full-history --max-cost 100
"""

import argparse
import logging
import os
import shutil
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import databento as db
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("airflow.task")

load_dotenv()


DATASET = "OPRA.PILLAR"


def get_client() -> db.Historical:
    if not os.environ.get("DATABENTO_API_KEY"):
        raise RuntimeError("Missing DATABENTO_API_KEY environment variable.")
    return db.Historical()


def get_dataset_start(client: db.Historical) -> date:
    r = client.metadata.get_dataset_range(dataset=DATASET)
    return datetime.fromisoformat(r["start"].replace("Z", "+00:00")).date()


def get_dataset_end(client: db.Historical) -> date:
    r = client.metadata.get_dataset_range(dataset=DATASET)
    return datetime.fromisoformat(r["end"].replace("Z", "+00:00")).date()


def estimate_cost(client: db.Historical, parent_symbol: str, schema: str, start: date, end: date) -> float:
    cost = client.metadata.get_cost(
        dataset=DATASET,
        symbols=[parent_symbol],
        stype_in="parent",
        schema=schema,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),
    )
    return float(cost)


def submit_job(client: db.Historical, parent_symbol: str, schema: str, start: date, end: date) -> str:
    details = client.batch.submit_job(
        dataset=DATASET,
        symbols=[parent_symbol],
        stype_in="parent",
        schema=schema,
        encoding="dbn",
        start=start.isoformat(),
        end=end.isoformat(),
    )
    return details["id"]


def wait_for_job(client: db.Historical, job_id: str, since: datetime, poll_seconds: int) -> dict[str, Any]:
    while True:
        jobs = client.batch.list_jobs(
            states=["queued", "processing", "done", "expired"],
            since=since,
        )
        job = next((j for j in jobs if j["id"] == job_id), None)
        if not job:
            raise RuntimeError(f"Could not find batch job {job_id}")

        state = job["state"]
        log.info(f"{datetime.now().isoformat(timespec='seconds')}  {job_id}  state={state}")

        if state in {"done", "expired"}:
            return job

        time.sleep(poll_seconds)


def get_latest_downloaded_date(files_dir: Path) -> date | None:
    """Return the max end-date embedded in existing filenames, or None."""
    # filename format: {SYMBOL}_{SCHEMA}_{start}_{end}_{job_id}_{original}
    latest: date | None = None
    for f in files_dir.glob("*.dbn.zst"):
        parts = f.name.split("_")
        try:
            d = date.fromisoformat(parts[3])
            if latest is None or d > latest:
                latest = d
        except (IndexError, ValueError):
            pass
    return latest


def parse_args():
    p = argparse.ArgumentParser(
        description="Download historical option-chain data via Databento batch API."
    )
    p.add_argument("--symbol", default="SPY", help="Underlying symbol, e.g. SPY or AAPL")
    p.add_argument("--start", help="Start date YYYY-MM-DD")
    p.add_argument("--end", help="End date YYYY-MM-DD; inclusive")
    p.add_argument("--schema", default="ohlcv-1d", help="Databento schema")
    p.add_argument("--output-dir", help="Output directory")
    p.add_argument("--full-history", action="store_true", help="Fetch from dataset start")
    p.add_argument("--max-cost", type=float, help="Abort if estimated cost exceeds this")
    p.add_argument("--skip-cost-check", action="store_true")
    p.add_argument("--poll-seconds", type=int, default=10)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    symbol = args.symbol.upper()
    parent_symbol = f"{symbol}.OPT"
    output_dir = Path(args.output_dir or f"./data/{symbol.lower()}_optchain")
    output_dir.mkdir(parents=True, exist_ok=True)

    client = get_client()
    available_end = get_dataset_end(client)

    end_date = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    end_date = min(end_date, available_end - timedelta(days=1))

    if args.full_history:
        start_date = date.fromisoformat(args.start) if args.start else get_dataset_start(client)
    elif args.start:
        start_date = date.fromisoformat(args.start)
    else:
        files_dir = output_dir / "files"
        latest = get_latest_downloaded_date(files_dir)
        if latest:
            log.info(f"Latest file covers up to {latest}; fetching from {latest + timedelta(days=1)}")
            start_date = latest + timedelta(days=1)
        else:
            start_date = end_date - timedelta(days=13)

    if start_date > end_date:
        log.info(f"Nothing to fetch: start {start_date} is after end {end_date}")
        return

    log.info(f"Dataset: {DATASET}")
    log.info(f"Symbol:  {parent_symbol}")
    log.info(f"Schema:  {args.schema}")
    log.info(f"Range:   {start_date} → {end_date}")
    log.info(f"Output:  {output_dir}")

    if not args.skip_cost_check:
        log.info("Estimating cost...")
        cost = estimate_cost(client, parent_symbol, args.schema, start_date, end_date)
        log.info(f"Estimated cost: ${cost:.4f}")
        if args.max_cost is not None and cost > args.max_cost:
            raise RuntimeError(f"Estimated cost ${cost:.4f} exceeds --max-cost ${args.max_cost:.4f}")

    if args.dry_run:
        log.info("Dry run complete. No job submitted.")
        return

    submitted_at = datetime.now(timezone.utc)
    log.info("Submitting batch job...")
    job_id = submit_job(client, parent_symbol, args.schema, start_date, end_date)
    log.info(f"Job submitted: {job_id}")

    job = wait_for_job(client, job_id=job_id, since=submitted_at - timedelta(minutes=5), poll_seconds=args.poll_seconds)

    if job["state"] != "done":
        raise RuntimeError(f"Job {job_id} ended with state={job['state']}")

    log.info(f"Job {job_id} done. Downloading files...")
    job_dir = output_dir / "_jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    files = client.batch.download(job_id, output_dir=job_dir)
    log.info(f"Downloaded {len(files)} file(s) to staging dir")

    dest_dir = output_dir / "files"
    dest_dir.mkdir(parents=True, exist_ok=True)

    for f in files:
        src = Path(f)
        dst_name = f"{symbol}_{args.schema}_{start_date}_{end_date}_{job_id}_{src.name}"
        shutil.move(str(src), str(dest_dir / dst_name))
        log.info(f"Moved {src.name} → {dest_dir / dst_name}")

    log.info(f"fetch_optchain done: {len(files)} file(s) in {dest_dir}")


if __name__ == "__main__":
    main()

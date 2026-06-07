#!/usr/bin/env python3
"""
Load DTB3 rate CSV files into PostgreSQL rates_1d table.

Examples:
  export DB_HOST=localhost DB_PORT=5432 DB_NAME=mds DB_USER=max DB_PASSWORD=...

  # Dry run — list files that would be loaded
  python load_rates.py --dry-run

  # Load everything
  python load_rates.py

  # Reset table and reload from scratch
  python load_rates.py --reset
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("airflow.task")

load_dotenv()

DATA_DIR = "./data/rates/files"
TABLE = "rates_1d"


def get_conn() -> psycopg.Connection:
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")

    if not name:
        raise RuntimeError("Missing DB_NAME environment variable.")
    if not user:
        raise RuntimeError("Missing DB_USER environment variable.")

    return psycopg.connect(
        host=host,
        port=int(port),
        dbname=name,
        user=user,
        password=password,
        autocommit=True,
    )


def reset_table(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(f"TRUNCATE {TABLE}")
        conn.execute("DELETE FROM _loaded_files WHERE file_path LIKE '%/rates/%'")


def already_loaded(conn: psycopg.Connection) -> set[str]:
    rows = conn.execute("SELECT file_path FROM _loaded_files").fetchall()
    return {r[0] for r in rows}


def load_file(conn: psycopg.Connection, path: Path) -> int:
    rows = []
    with path.open() as f:
        for i, line in enumerate(f):
            if i == 0:
                continue
            parts = line.strip().split(",")
            if len(parts) != 2:
                continue
            rows.append((parts[0], float(parts[1])))

    if not rows:
        return 0

    row_count = 0
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE _stage_{TABLE} (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        with cur.copy(f"COPY _stage_{TABLE} (trade_date, dtb3) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
                row_count += 1

        cur.execute(
            f"""
            INSERT INTO {TABLE} (trade_date, dtb3)
            SELECT trade_date, dtb3 FROM _stage_{TABLE}
            ON CONFLICT (trade_date) DO NOTHING
            """
        )
        cur.execute(
            "INSERT INTO _loaded_files (file_path, row_count) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (str(path), row_count),
        )

    return row_count


def parse_args():
    p = argparse.ArgumentParser(description="Load DTB3 CSV files into PostgreSQL.")
    p.add_argument("--input-dir", default=DATA_DIR, help="Directory of .csv rate files")
    p.add_argument("--reset", action="store_true", help="Truncate table before loading")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    input_dir = Path(args.input_dir)
    log.info(f"Input directory: {input_dir}")

    if not input_dir.exists():
        raise RuntimeError(f"Input directory not found: {input_dir}")

    files = sorted(input_dir.glob("*.csv"))
    if not files:
        log.info(f"No CSV files found in {input_dir}")
        return

    log.info(f"Found {len(files)} file(s)")

    if args.dry_run:
        for f in files:
            log.info(f"  {f.name}")
        log.info("Dry run complete. No data loaded.")
        return

    conn = get_conn()
    log.info("Connected to database")

    if args.reset:
        log.info(f"Resetting {TABLE}...")
        reset_table(conn)

    done = already_loaded(conn)
    pending = [f for f in files if str(f) not in done]

    log.info(f"Already loaded: {len(done)}  |  Pending: {len(pending)}")

    if not pending:
        log.info("All files already loaded.")
        conn.close()
        return

    total_rows = 0
    for i, path in enumerate(pending, 1):
        log.info(f"[{i}/{len(pending)}] Loading {path.name}...")
        row_count = load_file(conn, path)
        total_rows += row_count
        log.info(f"[{i}/{len(pending)}] {path.name}: {row_count:,} rows loaded")

    conn.close()
    log.info(f"load_rates done: {total_rows:,} rows from {len(pending)} file(s) into {TABLE}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Deserialize Databento .dbn.zst files and load them into PostgreSQL.

Examples:
  export DB_HOST=localhost DB_PORT=5432 DB_NAME=mds DB_USER=max DB_PASSWORD=...

  # Dry run — list files that would be loaded
  python load_optchain.py --symbol SPY --dry-run

  # Load everything
  python load_optchain.py --symbol SPY

  # Custom input directory
  python load_optchain.py --input-dir ./data/spy_optchain/files

  # Reset table and reload from scratch
  python load_optchain.py --symbol SPY --reset
"""

import argparse
import gc
import logging
import os
import sys
from datetime import date
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


DATA_DIR_TEMPLATE = "./data/{symbol}_optchain/files"


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


def reset_table(conn: psycopg.Connection, table: str) -> None:
    with conn.transaction():
        conn.execute(f"TRUNCATE {table}")
        conn.execute("DELETE FROM _loaded_files")


def get_last_loaded_date(conn: psycopg.Connection, table: str) -> date | None:
    row = conn.execute(f"SELECT MAX(ts_event)::date FROM {table}").fetchone()
    return row[0] if row and row[0] else None


def already_loaded(conn: psycopg.Connection) -> set[str]:
    rows = conn.execute("SELECT file_path FROM _loaded_files").fetchall()
    return {r[0] for r in rows}


def _iter_rows(df):
    import math
    cols = df.columns.tolist()
    for row in df.itertuples(index=False):
        yield (
            row.ts_event,
            str(row.symbol),
            int(row.instrument_id) if "instrument_id" in cols else None,
            float(row.open)   if "open"   in cols and not math.isnan(row.open)   else None,
            float(row.high)   if "high"   in cols and not math.isnan(row.high)   else None,
            float(row.low)    if "low"    in cols and not math.isnan(row.low)    else None,
            float(row.close)  if "close"  in cols and not math.isnan(row.close)  else None,
            int(row.volume)   if "volume" in cols else None,
        )


def load_file(conn: psycopg.Connection, path: Path, table: str) -> int:
    import databento as db

    store = db.DBNStore.from_file(str(path))
    df = store.to_df(map_symbols=True)
    del store

    if df.empty:
        return 0

    df = df.reset_index()

    if "symbol" not in df.columns:
        raise RuntimeError(f"No symbol column found in {path.name}. Columns: {list(df.columns)}")

    staging = f"_stage_{table}"
    row_count = 0

    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SET LOCAL temp_buffers = '64MB'")
        cur.execute(
            f"CREATE TEMP TABLE {staging} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        with cur.copy(
            f"COPY {staging} (ts_event, symbol, instrument_id, open, high, low, close, volume) FROM STDIN"
        ) as copy:
            for row in _iter_rows(df):
                copy.write_row(row)
                row_count += 1

        del df

        cur.execute(
            f"""
            INSERT INTO {table} (ts_event, symbol, instrument_id, open, high, low, close, volume)
            SELECT ts_event, symbol, instrument_id, open, high, low, close, volume FROM {staging}
            ON CONFLICT (ts_event, symbol) DO NOTHING
            """
        )
        cur.execute(
            "INSERT INTO _loaded_files (file_path, row_count) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (str(path), row_count),
        )

    return row_count


def parse_args():
    p = argparse.ArgumentParser(description="Load Databento DBN files into PostgreSQL.")
    p.add_argument("--symbol", default="SPY")
    p.add_argument("--input-dir", help="Directory of .dbn.zst files (default: ./data/{symbol}_optchain/files)")
    p.add_argument("--table", default="option_ohlcv_1d")
    p.add_argument("--reset", action="store_true", help="Drop and recreate the data table before loading")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    symbol = args.symbol.upper()

    input_dir = Path(args.input_dir or DATA_DIR_TEMPLATE.format(symbol=symbol.lower()))

    log.info(f"Input directory: {input_dir}")

    if not input_dir.exists():
        raise RuntimeError(f"Input directory not found: {input_dir}")

    files = sorted(input_dir.glob("*.dbn.zst"))
    if not files:
        log.info(f"No .dbn.zst files found in {input_dir}")
        return

    log.info(f"Found {len(files)} file(s) in {input_dir}")

    if args.dry_run:
        for f in files:
            log.info(f"  {f.name}")
        log.info("Dry run complete. No data loaded.")
        return

    conn = get_conn()
    log.info("Connected to database")

    if args.reset:
        log.info(f"Resetting table {args.table}...")
        reset_table(conn, args.table)

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
        row_count = load_file(conn, path, args.table)
        total_rows += row_count
        log.info(f"[{i}/{len(pending)}] {path.name}: {row_count:,} rows loaded")

    conn.close()
    log.info(f"load_optchain done: {total_rows:,} rows from {len(pending)} file(s) into {args.table}")


if __name__ == "__main__":
    main()

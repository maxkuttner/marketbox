#!/usr/bin/env python3
"""
One-time/idempotent backfill: stamp the master instrument_id onto existing
mds price rows by resolving their symbol against the ods instrument master.

The price tables (equity_ohlcv_1d, option_ohlcv_1d) historically carried
Databento's publisher instrument_id; going forward the loaders stamp the master
id (ods.instrument.id). This fills the column for rows already loaded. Symbols
with no active master row (e.g. expired option contracts that predate the
instrument universe) are left NULL — that is expected.

Safe to re-run: only rows whose instrument_id differs from the resolved master
id are touched.

Examples:
  python backfill_instrument_ids.py --table equity_ohlcv_1d --instrument-class SPOT
  python backfill_instrument_ids.py --table option_ohlcv_1d --instrument-class OPTION
"""

import argparse
import logging
import os
import sys

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


def _conn(dbname: str) -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=dbname,
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        autocommit=True,
    )


def load_instrument_map(instrument_class: str) -> dict[str, int]:
    with _conn(os.environ.get("ODS_DB", "ods")) as conn:
        rows = conn.execute(
            "SELECT symbol, id FROM instrument WHERE instrument_class = %s",
            (instrument_class,),
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def main():
    p = argparse.ArgumentParser(description="Backfill master instrument_id on mds price rows.")
    p.add_argument("--table", required=True, choices=["equity_ohlcv_1d", "option_ohlcv_1d"])
    p.add_argument("--instrument-class", required=True, choices=["SPOT", "OPTION"])
    p.add_argument("--dry-run", action="store_true", help="Report match counts only; no UPDATE")
    args = p.parse_args()

    id_map = load_instrument_map(args.instrument_class)
    log.info(f"Resolved {len(id_map)} active {args.instrument_class} instrument(s) from ods")
    if not id_map:
        log.info("Empty instrument map; nothing to backfill.")
        return

    # One transaction so the temp map survives (autocommit would drop it).
    with _conn(os.environ["DB_NAME"]) as conn, conn.transaction(), conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE _idmap (symbol text PRIMARY KEY, id bigint) ON COMMIT DROP")
        with cur.copy("COPY _idmap (symbol, id) FROM STDIN") as copy:
            for sym, iid in id_map.items():
                copy.write_row((sym, iid))

        # The column must mean master-id-or-NULL only. Two passes:
        #   (1) resolve: set the master id where the symbol is in the active universe
        #   (2) clear:   NULL any remaining non-master ids (stale Databento publisher
        #                ids on expired contracts), so the namespace is unambiguous.
        resolve_where = (
            f"FROM _idmap s WHERE t.symbol = s.symbol AND t.instrument_id IS DISTINCT FROM s.id"
        )
        clear_where = (
            f"WHERE t.instrument_id IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM _idmap s WHERE s.symbol = t.symbol)"
        )

        if args.dry_run:
            cur.execute(
                f"SELECT count(*) FROM {args.table} t JOIN _idmap s ON t.symbol = s.symbol "
                f"WHERE t.instrument_id IS DISTINCT FROM s.id"
            )
            resolve_n = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM {args.table} t {clear_where}")
            clear_n = cur.fetchone()[0]
            log.info(
                f"[dry-run] {args.table}: resolve {resolve_n:,} row(s) to master id, "
                f"clear {clear_n:,} stale vendor id(s) to NULL"
            )
            return

        log.info(f"Resolving master ids in {args.table} (this may take a while)...")
        cur.execute(f"UPDATE {args.table} t SET instrument_id = s.id {resolve_where}")
        resolved = cur.rowcount
        log.info(f"Clearing stale vendor ids in {args.table}...")
        cur.execute(f"UPDATE {args.table} t SET instrument_id = NULL {clear_where}")
        cleared = cur.rowcount
        log.info(
            f"backfill done: {resolved:,} row(s) stamped with master id, "
            f"{cleared:,} stale id(s) cleared to NULL in {args.table}"
        )


if __name__ == "__main__":
    main()

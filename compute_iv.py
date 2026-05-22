#!/usr/bin/env python3
"""
Compute implied volatility for option chain rows and store in option_iv_1d.

Uses Black-Scholes with continuous dividend yield (Merton model):
  - price input : mid = (high + low) / 2
  - risk-free rate : DTB3 from rates_1d (annualized %, divided by 100)
  - time to expiry : dte / 365.0
  - dividend yield : --div-yield (default 0.013 for SPY ~1.3% annual)

NULL iv is stored when no solution exists (DTE=0, price ≤ intrinsic, deep ITM/OTM noise).
These rows are not retried on subsequent runs.

Examples:
  python compute_iv.py --dry-run
  python compute_iv.py --symbol SPY
  python compute_iv.py --full-history
  python compute_iv.py --full-history --symbol SPY --div-yield 0.015
"""

import argparse
import logging
import math
import os
import sys
from datetime import date

import psycopg
from dotenv import load_dotenv
from scipy.optimize import brentq
from scipy.stats import norm

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("airflow.task")

load_dotenv()

TABLE = "option_iv_1d"
DEFAULT_DIV_YIELD = 0.013


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


def bs_price(S: float, K: float, T: float, r: float, q: float, sigma: float, flag: str) -> float:
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if flag == "C":
        return S * math.exp(-q * T) * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * math.exp(-q * T) * norm.cdf(-d1)


def implied_vol(
    price: float, S: float, K: float, T: float, r: float, q: float, flag: str
) -> float | None:
    if T <= 0 or price <= 0 or S <= 0 or K <= 0:
        return None
    # Discard prices at or below discounted intrinsic (no IV solution possible)
    if flag == "C":
        intrinsic = max(S * math.exp(-q * T) - K * math.exp(-r * T), 0.0)
    else:
        intrinsic = max(K * math.exp(-r * T) - S * math.exp(-q * T), 0.0)
    if price <= intrinsic:
        return None
    try:
        return brentq(
            lambda v: bs_price(S, K, T, r, q, v, flag) - price,
            1e-6, 20.0, xtol=1e-6, maxiter=100,
        )
    except ValueError:
        return None


def get_pending_dates(conn: psycopg.Connection, symbol: str | None) -> list[date]:
    sym_filter = "AND o.root_symbol = %(symbol)s" if symbol else ""
    rows = conn.execute(
        f"""
        SELECT DISTINCT o.ts_event::date
        FROM option_ohlcv_1d_v o
        WHERE o.ts_event::date NOT IN (SELECT DISTINCT trade_date FROM {TABLE})
          AND o.ts_event::date IN (SELECT trade_date FROM rates_1d)
        {sym_filter}
        ORDER BY o.ts_event::date
        """,
        {"symbol": symbol} if symbol else {},
    ).fetchall()
    return [r[0] for r in rows]


def fetch_day(
    conn: psycopg.Connection, trade_date: date, symbol: str | None
) -> list[tuple]:
    sym_filter = "AND o.root_symbol = %(symbol)s" if symbol else ""
    return conn.execute(
        f"""
        SELECT
            o.root_symbol,
            o.expiry,
            o.option_type,
            o.strike::float,
            o.dte,
            (o.high + o.low) / 2.0   AS mid,
            e.close                   AS s,
            r.dtb3
        FROM option_ohlcv_1d_v o
        JOIN equity_ohlcv_1d e
            ON e.ts_event::date = o.ts_event::date
           AND e.symbol = o.root_symbol
        JOIN rates_1d r ON r.trade_date = o.ts_event::date
        WHERE o.ts_event::date = %(trade_date)s
        {sym_filter}
        """,
        {"trade_date": trade_date, **({"symbol": symbol} if symbol else {})},
    ).fetchall()


def insert_day(conn: psycopg.Connection, results: list[tuple]) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE _stage_{TABLE} (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        with cur.copy(
            f"COPY _stage_{TABLE} (trade_date, root_symbol, expiry, option_type, strike, iv) FROM STDIN"
        ) as copy:
            for row in results:
                copy.write_row(row)

        cur.execute(
            f"""
            INSERT INTO {TABLE} (trade_date, root_symbol, expiry, option_type, strike, iv)
            SELECT trade_date, root_symbol, expiry, option_type, strike, iv
            FROM _stage_{TABLE}
            ON CONFLICT (trade_date, root_symbol, expiry, option_type, strike) DO NOTHING
            """
        )


def parse_args():
    p = argparse.ArgumentParser(
        description="Compute implied volatility and store in option_iv_1d."
    )
    p.add_argument("--symbol", help="Filter by root symbol (default: all)")
    p.add_argument(
        "--full-history", action="store_true",
        help="Truncate option_iv_1d and recompute all dates",
    )
    p.add_argument(
        "--div-yield", type=float, default=DEFAULT_DIV_YIELD,
        help=f"Continuous annual dividend yield (default: {DEFAULT_DIV_YIELD} for SPY)",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    q = args.div_yield

    conn = get_conn()
    log.info("Connected to database")

    if args.full_history:
        log.info(f"Full history: truncating {TABLE}...")
        conn.execute(f"TRUNCATE {TABLE}")

    pending = get_pending_dates(conn, args.symbol)
    log.info(f"Dates to process: {len(pending)}")

    if args.dry_run:
        if pending:
            log.info(f"  First: {pending[0]}  Last: {pending[-1]}")
        log.info("Dry run complete. No IV computed.")
        conn.close()
        return

    if not pending:
        log.info("Nothing to compute.")
        conn.close()
        return

    total_rows = total_null = 0

    for i, trade_date in enumerate(pending, 1):
        rows = fetch_day(conn, trade_date, args.symbol)
        if not rows:
            log.info(f"[{i}/{len(pending)}] {trade_date}: skipped (no equity or rate data)")
            continue

        results = []
        null_count = 0
        for root_symbol, expiry, option_type, strike, dte, mid, s, dtb3 in rows:
            T = (dte / 365.0) if dte and dte > 0 else 0.0
            r = dtb3 / 100.0
            iv = implied_vol(
                float(mid or 0), float(s or 0), float(strike), T, r, q, option_type
            )
            if iv is None:
                null_count += 1
            results.append((trade_date, root_symbol, expiry, option_type, float(strike), iv))

        insert_day(conn, results)

        row_count = len(results)
        null_pct = null_count / row_count * 100 if row_count else 0
        log.info(
            f"[{i}/{len(pending)}] {trade_date}: "
            f"{row_count:,} rows, {null_count:,} NULL ({null_pct:.1f}%)"
        )
        total_rows += row_count
        total_null += null_count

    conn.close()
    log.info(
        f"compute_iv done: {total_rows:,} rows, {total_null:,} NULL "
        f"across {len(pending)} date(s)"
    )


if __name__ == "__main__":
    main()

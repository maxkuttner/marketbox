"""
Airflow DAG: seed the master instrument universe (equities + option chains) from
Databento instrument definitions into ods.public.instrument.

This is the sole minting authority for instrument ids (upsert on (symbol, venue)).
It runs daily and *ahead* of the price loads so that every symbol that could be
priced that day has already been matched-or-minted; the equity_daily and
optchain_daily DAGs are triggered by the INSTRUMENTS dataset this DAG emits, so
their loaders can resolve symbol -> master instrument_id.

Writes to the `ods` database as market_user (see mdm/access/ods.sql), not the
mds DB the ohlcv loaders use — seed_instruments.py reads ODS_DB for its connection.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.decorators import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

SCHEDULE  = "0 5 * * *"          # daily, 05:00 UTC — before the 06:00 price loads
WATCHLIST = ["SPY"]              # tracked underlyings; expand over time

# Completing the seed updates this dataset, which triggers the price-load DAGs.
INSTRUMENTS = Dataset("marketbox://ods/instrument")


@dag(
    dag_id="instruments_daily",
    schedule=SCHEDULE,
    start_date=datetime(2026, 6, 14, tzinfo=timezone.utc),
    catchup=False,
    tags=["marketbox", "instruments"],
)
def instruments_daily():
    for symbol in WATCHLIST:
        BashOperator(
            task_id=f"seed_{symbol.lower()}",
            bash_command=f"{PYTHON} seed_instruments.py --symbol {symbol}",
            cwd=PROJECT_ROOT,
            retries=2,
            retry_delay=timedelta(minutes=10),
            outlets=[INSTRUMENTS],
        )


instruments_daily()

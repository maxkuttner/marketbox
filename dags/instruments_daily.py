"""
Airflow DAG: seed the master instrument universe (equities + option chains) from
Databento instrument definitions into ods.public.instrument.

Instrument definitions change rarely, so this runs weekly. Writes to the `ods`
database as market_user (see mdm/access/ods.sql), not the mds DB the ohlcv
loaders use — seed_instruments.py reads ODS_DB for its connection.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.decorators import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

SCHEDULE  = "0 5 * * 1"          # weekly, Monday 05:00 UTC
WATCHLIST = ["SPY"]              # tracked underlyings; expand over time


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
        )


instruments_daily()

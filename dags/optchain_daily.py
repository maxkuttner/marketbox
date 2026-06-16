"""
Daily Airflow DAG: fetch latest SPY option chain data from Databento and load into PostgreSQL.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.decorators import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

# Triggered when instruments_daily finishes seeding, so the loader can resolve
# symbol -> master instrument_id against a freshly minted universe.
INSTRUMENTS = Dataset("marketbox://ods/instrument")
SYMBOL   = "SPY"


@dag(
    dag_id="optchain_daily",
    schedule=[INSTRUMENTS],
    start_date=datetime(2026, 5, 9, tzinfo=timezone.utc),
    catchup=False,
    tags=["marketbox", "options"],
)
def optchain_daily():

    fetch = BashOperator(
        task_id="fetch_optchain",
        bash_command=f"{PYTHON} fetch_optchain.py --symbol {SYMBOL}",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    load = BashOperator(
        task_id="load_optchain",
        bash_command=f"{PYTHON} load_optchain.py --symbol {SYMBOL}",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    fetch >> load


optchain_daily()

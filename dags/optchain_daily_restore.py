"""
Daily Airflow DAG: fetch latest SPY option chain data from Databento and load into PostgreSQL.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.decorators import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

SYMBOL = "SPY"


@dag(
    dag_id="optchain_daily_restore",
    catchup=False,
    tags=["marketbox", "options"],
)
def optchain_daily_restore():

    load = BashOperator(
        task_id="load_optchain",
        bash_command=f"{PYTHON} load_optchain.py --symbol {SYMBOL}",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    load


optchain_daily_restore()

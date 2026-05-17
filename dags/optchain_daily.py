"""
Daily Airflow DAG: fetch latest SPY option chain data from Databento and load into PostgreSQL.
"""

import os
from datetime import datetime, timezone
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.sdk import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

SCHEDULE = "0 6 * * *"
SYMBOL   = "SPY"


@dag(
    dag_id="optchain_daily",
    schedule=SCHEDULE,
    start_date=datetime(2026, 5, 9, tzinfo=timezone.utc),
    catchup=False,
    tags=["marketbox", "options"],
)
def optchain_daily():

    fetch = BashOperator(
        task_id="fetch_optchain",
        bash_command=f"{PYTHON} fetch_optchain.py --symbol {SYMBOL}",
        cwd=PROJECT_ROOT,
    )

    load = BashOperator(
        task_id="load_optchain",
        bash_command=f"{PYTHON} load_optchain.py --symbol {SYMBOL}",
        cwd=PROJECT_ROOT,
    )

    fetch >> load


optchain_daily()

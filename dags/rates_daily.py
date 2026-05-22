"""
Daily Airflow DAG: fetch DTB3 T-bill rates from FRED and load into PostgreSQL.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.decorators import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

SCHEDULE = "0 6 * * *"


@dag(
    dag_id="rates_daily",
    schedule=SCHEDULE,
    start_date=datetime(2026, 5, 22, tzinfo=timezone.utc),
    catchup=False,
    tags=["marketbox", "rates"],
)
def rates_daily():

    fetch_rates = BashOperator(
        task_id="fetch_rates",
        bash_command=f"{PYTHON} fetch_rates.py",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    load_rates = BashOperator(
        task_id="load_rates",
        bash_command=f"{PYTHON} load_rates.py",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    fetch_rates >> load_rates


rates_daily()

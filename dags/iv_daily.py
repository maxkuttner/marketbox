"""
Daily Airflow DAG: fetch T-bill rates from FRED, then compute implied volatility.

Runs at 07:00 UTC — one hour after equity and optchain DAGs (06:00 UTC)
to ensure both are loaded before IV is computed.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.decorators import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

SCHEDULE = "0 7 * * *"


@dag(
    dag_id="iv_daily",
    schedule=SCHEDULE,
    start_date=datetime(2026, 5, 22, tzinfo=timezone.utc),
    catchup=False,
    tags=["marketbox", "iv"],
)
def iv_daily():

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

    compute_iv = BashOperator(
        task_id="compute_iv",
        bash_command=f"{PYTHON} compute_iv.py",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    fetch_rates >> load_rates >> compute_iv


iv_daily()

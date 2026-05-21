"""
Daily Airflow DAG: fetch latest SPY equity OHLCV data from Databento and load into PostgreSQL.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.decorators import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

SCHEDULE = "0 6 * * *"
SYMBOL   = "SPY"


@dag(
    dag_id="equity_daily",
    schedule=SCHEDULE,
    start_date=datetime(2026, 5, 21, tzinfo=timezone.utc),
    catchup=False,
    tags=["marketbox", "equity"],
)
def equity_daily():

    fetch = BashOperator(
        task_id="fetch_equity_daily",
        bash_command=f"{PYTHON} fetch_equity_daily.py --symbol {SYMBOL}",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    load = BashOperator(
        task_id="load_equity_daily",
        bash_command=f"{PYTHON} load_equity_daily.py --symbol {SYMBOL}",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    fetch >> load


equity_daily()

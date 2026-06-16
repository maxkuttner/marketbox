"""
Airflow DAG: sync broker symbology (Alpaca, equities) into ods.broker_instrument.

Triggered by the INSTRUMENTS dataset that instruments_daily emits, so the broker
mapping is refreshed right after the master universe is minted — every master
instrument the broker also lists gets a broker_instrument row (broker symbol +
native id + tradability) for the OMS to resolve at order time.
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.decorators import dag

PROJECT_ROOT = str(Path(os.environ.get("AIRFLOW_HOME", Path(__file__).parent.parent)))
PYTHON       = str(Path(PROJECT_ROOT) / ".venv" / "bin" / "python")

# Same dataset instruments_daily emits — run after the master universe is minted.
INSTRUMENTS = Dataset("marketbox://ods/instrument")


@dag(
    dag_id="broker_sync_daily",
    schedule=[INSTRUMENTS],
    start_date=datetime(2026, 6, 16, tzinfo=timezone.utc),
    catchup=False,
    tags=["marketbox", "brokers"],
)
def broker_sync_daily():
    BashOperator(
        task_id="broker_sync_alpaca",
        bash_command=f"{PYTHON} broker_sync.py",
        cwd=PROJECT_ROOT,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )


broker_sync_daily()

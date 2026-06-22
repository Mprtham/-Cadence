"""
retail_daily — hourly Airflow DAG for the Cadence UK-retail pipeline.

Task order:
  generate -> load_to_duckdb -> dbt_run -> dbt_test

Every task: retries=2 with exponential backoff.
Failure after retries: Discord webhook via on_failure_callback.
Idempotency: load keyed on {{ ds }}, safe to backfill or re-run any date.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).parents[1] / "include"))

from alerts import discord_alert
from generate import generate_orders
from load import load_orders

TRANSFORM_DIR = str(Path(__file__).parents[1] / "transform")
DATA_DIR = os.environ.get("CADENCE_DATA_DIR", "/tmp/cadence")

DEFAULT_ARGS = {
    "owner": "prathamesh",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": discord_alert,
    "email_on_failure": False,
    "email_on_retry": False,
}


def _generate(ds: str, **_) -> str:
    output_dir = os.path.join(DATA_DIR, ds)
    csv_path = generate_orders(run_date=ds, output_dir=output_dir)
    return csv_path


def _load(ds: str, ti, **_) -> int:
    csv_path = ti.xcom_pull(task_ids="generate")
    if not csv_path:
        raise ValueError(f"No CSV path returned by generate task for ds={ds}")
    row_count = load_orders(run_date=ds, csv_path=csv_path)
    return row_count


with DAG(
    dag_id="retail_daily",
    description="Hourly UK-retail pipeline: generate -> load -> dbt_run -> dbt_test",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cadence", "retail", "dbt"],
) as dag:

    generate = PythonOperator(
        task_id="generate",
        python_callable=_generate,
    )

    load_to_duckdb = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=_load,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {TRANSFORM_DIR} && "
            "dbt run --profiles-dir . --target dev --no-use-colors"
        ),
        env={
            "CADENCE_DB_PATH": os.environ.get(
                "CADENCE_DB_PATH", "/opt/airflow/data/cadence.duckdb"
            ),
            **{k: v for k, v in os.environ.items()},
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {TRANSFORM_DIR} && "
            "dbt test --profiles-dir . --target dev --no-use-colors"
        ),
        env={
            "CADENCE_DB_PATH": os.environ.get(
                "CADENCE_DB_PATH", "/opt/airflow/data/cadence.duckdb"
            ),
            **{k: v for k, v in os.environ.items()},
        },
    )

    generate >> load_to_duckdb >> dbt_run >> dbt_test

"""
Luồng:
  run_golang_producer  (BashOperator, CSV -> Kafka)
    >> run_python_loader (BashOperator, Kafka -> Databricks Volume inbox)
    >> dbt_load_bronze   (dbt run-operation load_bronze_raw -> COPY INTO inbox -> raw)
    >> dbt_build         (dbt build: models Bronze/Silver/Gold + snapshot SCD2 + tests)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerts import on_failure_callback

DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt_project"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="cosmetics_medallion",
    default_args=default_args,
    description="Bronze->Silver->Gold via dbt on SQL Warehouse",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 6, 9),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "dbt", "cosmetics"],
) as dag:

    run_producer = BashOperator(
        task_id="run_golang_producer",
        bash_command="cd /opt/airflow/dags/scripts && chmod +x producer && ./producer",
    )

    run_loader = BashOperator(
        task_id="run_python_loader",
        bash_command="cd /opt/airflow/dags/scripts && python kafka_to_bronze_loader.py",
    )

    dbt_load_bronze = BashOperator(
        task_id="dbt_load_bronze",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run-operation load_bronze_raw --profiles-dir .",
        env={"DBT_PROFILES_DIR": DBT_DIR, **os.environ},
    )

    # dbt build = models + snapshot (SCD2) + tests (fail-fast DQ gate).
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --profiles-dir .",
        env={"DBT_PROFILES_DIR": DBT_DIR, **os.environ},
    )

    run_producer >> run_loader >> dbt_load_bronze >> dbt_build

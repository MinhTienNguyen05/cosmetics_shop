from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10)
}

with DAG(
    'cosmetics_bronze_ingestion',
    max_active_tasks=2,
    default_args=default_args,
    description='Pipeline đưa dữ liệu E-commerce từ CSV qua Kafka lên Databricks Volumes',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026, 6, 9),
    catchup=False,
    tags=['bronze', 'ingestion', 'kafka'],
) as dag:

    trigger_producer = BashOperator(
        task_id='run_golang_producer',
        bash_command='cd /opt/airflow/dags/scripts && chmod +x producer && ./producer',
    )


    push_to_databricks = BashOperator(
        task_id='run_python_loader',
        bash_command='cd /opt/airflow/dags/scripts && python kafka_to_bronze_loader.py',
    )

    trigger_producer >> push_to_databricks
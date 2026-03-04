from datetime import timedelta
import logging

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


logger = logging.getLogger(__name__)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="weather_lakehouse_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "lakehouse", "iceberg"],
) as dag:
    logger.info("Initializing DAG weather_lakehouse_pipeline")

    extract_task = BashOperator(
        task_id="extract_task",
        cwd="/opt/airflow/project",
        bash_command="python extract/weather_api.py",
        execution_timeout=timedelta(minutes=10),
    )

    bronze_task = BashOperator(
        task_id="bronze_task",
        cwd="/opt/airflow/project",
        bash_command="spark-submit spark/jobs/bronze_ingest.py",
        execution_timeout=timedelta(minutes=20),
    )

    silver_task = BashOperator(
        task_id="silver_task",
        cwd="/opt/airflow/project",
        bash_command="spark-submit spark/jobs/silver_transform.py",
        execution_timeout=timedelta(minutes=20),
    )

    gold_task = BashOperator(
        task_id="gold_task",
        cwd="/opt/airflow/project",
        bash_command="spark-submit spark/jobs/gold_aggregate.py",
        execution_timeout=timedelta(minutes=20),
    )

    iceberg_maintenance_task = BashOperator(
        task_id="iceberg_maintenance_task",
        cwd="/opt/airflow/project",
        bash_command="spark-submit spark/jobs/iceberg_maintenance.py",
        execution_timeout=timedelta(minutes=20),
    )

    extract_task >> bronze_task >> silver_task >> gold_task >> iceberg_maintenance_task

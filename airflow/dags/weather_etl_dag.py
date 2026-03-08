from datetime import timedelta
import os
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


# --------------------------------------------------
# Paths
# --------------------------------------------------
PROJECT_ROOT = "/opt/airflow/project"
SPARK_JOBS_ROOT = f"{PROJECT_ROOT}/spark/jobs"

SPARK_SUBMIT = os.getenv(
    "SPARK_SUBMIT",
    "/opt/spark/bin/spark-submit",
)

COMMON_ENV = {
    "SPARK_MASTER_URL": os.getenv(
        "SPARK_MASTER_URL",
        "spark://spark-master:7077",
    )
}

# --------------------------------------------------
# Default args
# --------------------------------------------------
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --------------------------------------------------
# DAG
# --------------------------------------------------
with DAG(
    dag_id="weather_lakehouse_pipeline",
    start_date=pendulum.datetime(
        2024, 1, 1,
        tz="Asia/Ho_Chi_Minh",
    ),
    schedule="0 * * * *",  # hourly
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "lakehouse", "iceberg"],
) as dag:

    # ---------------- EXTRACT ----------------
    extract_task = BashOperator(
        task_id="extract_task",
        cwd=PROJECT_ROOT,
        env=COMMON_ENV,
        bash_command="python extract/weather_api.py",
        execution_timeout=timedelta(minutes=10),
    )

    # ---------------- BRONZE ----------------
    bronze_task = BashOperator(
        task_id="bronze_task",
        cwd=PROJECT_ROOT,
        env=COMMON_ENV,
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_ROOT}/bronze_ingest.py",
        execution_timeout=timedelta(minutes=20),
    )

    # ---------------- SILVER ----------------
    silver_task = BashOperator(
        task_id="silver_task",
        cwd=PROJECT_ROOT,
        env=COMMON_ENV,
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_ROOT}/silver_transform.py",
        execution_timeout=timedelta(minutes=20),
    )

    # ---------------- GOLD ----------------
    gold_task = BashOperator(
        task_id="gold_task",
        cwd=PROJECT_ROOT,
        env=COMMON_ENV,
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_ROOT}/gold_aggregate.py",
        execution_timeout=timedelta(minutes=20),
    )

    # Pipeline order
    extract_task >> bronze_task >> silver_task >> gold_task
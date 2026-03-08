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
    "retry_delay": timedelta(minutes=10),
}

# --------------------------------------------------
# DAG
# --------------------------------------------------
with DAG(
    dag_id="iceberg_maintenance",
    start_date=pendulum.datetime(
        2024, 1, 1,
        tz="Asia/Ho_Chi_Minh",
    ),
    schedule="0 2 * * *",  # 2AM daily (VN time)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["iceberg", "maintenance"],
) as dag:

    maintenance_task = BashOperator(
        task_id="iceberg_maintenance_task",
        cwd=PROJECT_ROOT,
        env=COMMON_ENV,
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_ROOT}/iceberg_maintenance.py",
        execution_timeout=timedelta(minutes=30),
    )
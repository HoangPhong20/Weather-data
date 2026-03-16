from datetime import timedelta
import os
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


# ==================================================
# CONFIG
# ==================================================

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
SPARK_JOBS_ROOT = f"{PROJECT_ROOT}/spark/jobs"

SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", "spark-submit")
PYTHON_BIN = os.getenv("PYTHON_BIN", "python")

COMMON_ENV = {
    **os.environ,
    "SPARK_MASTER_URL": os.getenv(
        "SPARK_MASTER_URL",
        "spark://spark-master:7077",
    ),
}

# ==================================================
# DEFAULT DAG ARGS
# ==================================================

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ==================================================
# TASK FACTORIES
# ==================================================

def spark_task(task_id: str, job: str, timeout: int = 20):
    return BashOperator(
        task_id=task_id,
        env=COMMON_ENV,
        bash_command=f"""
docker exec spark-master \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark/app/jobs/{job}
""",
        execution_timeout=timedelta(minutes=timeout),
    )


def python_task(task_id: str, script: str, timeout: int = 10):
    """Create Python execution task"""
    return BashOperator(
        task_id=task_id,
        cwd=PROJECT_ROOT,
        env=COMMON_ENV,
        bash_command=f"{PYTHON_BIN} {script}",
        execution_timeout=timedelta(minutes=timeout),
    )

# ==================================================
# DAG
# ==================================================

with DAG(
    dag_id="weather_lakehouse_pipeline",
    description="Weather ETL pipeline (Extract → Bronze → Silver → Gold)",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["weather", "lakehouse", "iceberg"],
) as dag:

    # ---------------- EXTRACT ----------------
    extract = python_task(
        task_id="extract_weather",
        script=f"{PROJECT_ROOT}/app/extract/weather_api.py",
    )

    # ---------------- SPARK LAYERS ----------------
    bronze = spark_task("bronze_ingest", "bronze_ingest.py")
    silver = spark_task("silver_transform", "silver_transform.py")
    gold = spark_task("gold_aggregate", "gold_aggregate.py")

    # ---------------- PIPELINE ORDER ----------------
    extract >> bronze >> silver >> gold
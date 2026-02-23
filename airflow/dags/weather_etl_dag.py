"""
Weather ETL Pipeline DAG
Orchestrates:
1. Optional schema init
2. Extract weather -> Kafka
3. Spark streaming load -> DB
"""

from datetime import timedelta
import os
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

PROJECT_ROOT = os.getenv("WEATHER_PROJECT_ROOT", "/opt/airflow/weather")
PYTHON_BIN = os.getenv("PYTHON_BIN", "python")


def get_var(name, default=None):
    try:
        return Variable.get(name)
    except Exception:
        return default


CONFIG = {
    "API_KEY": get_var("API_KEY"),
    "API_URL": get_var("API_URL", "https://api.openweathermap.org/data/2.5/weather"),
    "JSON_PATH": get_var(
        "json_path",
        f"{PROJECT_ROOT}/ETL/extract/get_cities/city.list.json",
    ),
    "ENABLE_SCHEMA": get_var("ENABLE_DB_SCHEMA_INIT", "false").lower() == "true",
}


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def should_init_schema():
    return CONFIG["ENABLE_SCHEMA"]


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------

with DAG(
    dag_id="weather_etl_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["weather", "streaming"],
) as dag:

    check_schema = ShortCircuitOperator(
        task_id="check_schema_flag",
        python_callable=should_init_schema,
    )

    init_schema = BashOperator(
        task_id="init_schema",
        cwd=PROJECT_ROOT,
        bash_command=f"{PYTHON_BIN} src/main.py",
        execution_timeout=timedelta(minutes=10),
    )

    skip_schema = EmptyOperator(task_id="skip_schema")

    extract_weather = BashOperator(
        task_id="extract_weather",
        cwd=PROJECT_ROOT,
        bash_command=f"{PYTHON_BIN} ETL/extract/extract.py",
        env={
            "API_KEY": CONFIG["API_KEY"],
            "API_URL": CONFIG["API_URL"],
            "json_path": CONFIG["JSON_PATH"],
            "KAFKA_BOOTSTRAP": get_var("KAFKA_BOOTSTRAP", "localhost:9092"),
        },
        retries=3,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=20),
    )

    stream_load = BashOperator(
        task_id="stream_load",
        cwd=PROJECT_ROOT,
        bash_command=f"{PYTHON_BIN} ETL/TransformAndLoad.py",
        execution_timeout=timedelta(minutes=45),
    )

    check_schema >> [init_schema, skip_schema]
    [init_schema, skip_schema] >> extract_weather >> stream_load

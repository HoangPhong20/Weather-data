from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["weather", "etl"],
) as dag:
    run_weather_etl = BashOperator(
        task_id="run_weather_etl",
        bash_command="cd /opt/airflow/project && python3 src/main.py",
        append_env=True,
        env={"PYTHONPATH": "/opt/airflow/project"},
    )

    run_weather_etl

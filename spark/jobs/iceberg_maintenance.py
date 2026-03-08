from pyspark.sql import SparkSession
from spark.spark_config import SparkConnect
import logging
import os

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("maintenance")

TARGET_TABLES = [
    "weather.bronze.weather_raw",
    "weather.silver.weather_clean",
    "weather.gold.avg_temp_by_country",
    "weather.gold.daily_weather_summary",
]


def run_maintenance(spark: SparkSession, table: str):

    if not spark.catalog.tableExists(table):
        logger.warning("Skip missing table %s", table)
        return

    logger.info("Maintaining %s", table)

    spark.sql(f"""
        CALL weather.system.rewrite_data_files(
            table => '{table}',
            options => map('min-input-files','5')
        )
    """)

    spark.sql(f"""
        CALL weather.system.expire_snapshots(
            table => '{table}',
            older_than => current_timestamp() - INTERVAL 7 DAYS
        )
    """)

    spark.sql(f"""
        CALL weather.system.remove_orphan_files(
            table => '{table}'
        )
    """)


def main():

    connector = SparkConnect(
        app_name=f"maintenance-{os.getenv('AIRFLOW_RUN_ID','local')}",
        master_url=os.getenv("SPARK_MASTER_URL"),
    )

    spark = connector.spark

    try:
        for table in TARGET_TABLES:
            try:
                run_maintenance(spark, table)
            except Exception:
                logger.exception("Failed maintenance: %s", table)

    finally:
        connector.stop()


if __name__ == "__main__":
    main()
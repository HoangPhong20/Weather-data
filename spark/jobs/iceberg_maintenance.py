from pyspark.sql import SparkSession
from spark.spark_config import SparkConnect
import os

TARGET_TABLES = [
    "weather.bronze.weather_raw",
    "weather.silver.weather_clean",
    "weather.gold.avg_temp_by_country",
    "weather.gold.daily_weather_summary",
    "weather.gold.hottest_city_per_day",
]


def run_maintenance(spark: SparkSession, table_name: str) -> None:
    print(f"Running maintenance for: {table_name}")

    spark.sql(f"""
        CALL weather.system.rewrite_data_files(
            table => '{table_name}'
        )
    """)

    spark.sql(f"""
        CALL weather.system.expire_snapshots(
            table => '{table_name}',
            older_than => current_timestamp() - INTERVAL 7 DAYS
        )
    """)

    spark.sql(f"""
        CALL weather.system.remove_orphan_files(
            table => '{table_name}'
        )
    """)


def main() -> None:

    connector = SparkConnect(
        app_name="iceberg-maintenance",
        master_url=os.getenv("SPARK_MASTER_URL"),
    )

    spark = connector.spark

    try:
        for table in TARGET_TABLES:
            run_maintenance(spark, table)
    finally:
        connector.stop()


if __name__ == "__main__":
    main()
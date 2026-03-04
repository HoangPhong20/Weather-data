import logging
import os

from pyspark.sql import SparkSession


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

TARGET_TABLES = [
    "weather.bronze.weather_raw",
    "weather.silver.weather_clean",
    "weather.gold.avg_temp_by_country",
    "weather.gold.daily_weather_summary",
    "weather.gold.hottest_city_per_day",
]


def run_maintenance(spark: SparkSession, table_name: str) -> None:
    logger.info("Running maintenance for table=%s", table_name)
    spark.sql(
        f"CALL weather.system.rewrite_data_files(table => '{table_name}')"
    )

    spark.sql(
        f"""
        CALL weather.system.expire_snapshots(
            table => '{table_name}',
            older_than => current_timestamp() - INTERVAL 7 DAYS
        )
        """
    )

    spark.sql(
        f"CALL weather.system.remove_orphan_files(table => '{table_name}')"
    )


def main() -> None:
    logger.info("Starting iceberg maintenance")
    spark = (
        SparkSession.builder.appName("iceberg-maintenance")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .getOrCreate()
    )

    for table in TARGET_TABLES:
        run_maintenance(spark, table)

    logger.info("Iceberg maintenance completed")
    spark.stop()


if __name__ == "__main__":
    main()

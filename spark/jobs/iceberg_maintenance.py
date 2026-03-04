from pyspark.sql import SparkSession
from spark.spark_config import Spark_connect


# Tables cần maintenance
TARGET_TABLES = [
    "weather.bronze.weather_raw",
    "weather.silver.weather_clean",
    "weather.gold.avg_temp_by_country",
    "weather.gold.daily_weather_summary",
    "weather.gold.hottest_city_per_day",
]


# --------------------------------------------------
# Iceberg maintenance operations
# --------------------------------------------------
def run_maintenance(spark: SparkSession, table_name: str) -> None:
    print(f"Running maintenance for: {table_name}")

    # Compact small files
    spark.sql(
        f"""
        CALL weather.system.rewrite_data_files(
            table => '{table_name}'
        )
        """
    )

    # Remove old snapshots
    spark.sql(
        f"""
        CALL weather.system.expire_snapshots(
            table => '{table_name}',
            older_than => current_timestamp() - INTERVAL 7 DAYS
        )
        """
    )

    # Cleanup orphan files
    spark.sql(
        f"""
        CALL weather.system.remove_orphan_files(
            table => '{table_name}'
        )
        """
    )


# --------------------------------------------------
# Main
# --------------------------------------------------
def main() -> None:

    # Spark config đã load từ spark-defaults.conf
    connector = Spark_connect(app_name="iceberg-maintenance")
    spark = connector.spark

    for table in TARGET_TABLES:
        run_maintenance(spark, table)

    connector.stop()


if __name__ == "__main__":
    main()
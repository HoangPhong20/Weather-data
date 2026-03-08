import os
import logging
from spark.spark_config import SparkConnect

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting gold aggregate job")

    connector = SparkConnect(
        app_name=f"gold-aggregate-{os.getenv('AIRFLOW_RUN_ID','local')}",
        master_url=os.getenv("SPARK_MASTER_URL"),
    )

    spark = connector.spark

    try:
        if not spark.catalog.tableExists("weather.silver.weather_clean"):
            raise RuntimeError("Missing silver table")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.gold")

        # -----------------------------------------
        # Avg temp
        # -----------------------------------------
        spark.sql("""
            CREATE TABLE IF NOT EXISTS weather.gold.avg_temp_by_country (
                country STRING,
                avg_temp DOUBLE
            )
            USING ICEBERG
        """)

        spark.sql("""
            INSERT OVERWRITE weather.gold.avg_temp_by_country
            SELECT country, AVG(temperature)
            FROM weather.silver.weather_clean
            GROUP BY country
        """)

        logger.info("Updated avg_temp_by_country")

        # -----------------------------------------
        # Daily summary
        # -----------------------------------------
        spark.sql("""
            CREATE TABLE IF NOT EXISTS weather.gold.daily_weather_summary (
                weather_day DATE,
                country STRING,
                avg_temperature DOUBLE,
                avg_humidity DOUBLE,
                max_temperature DOUBLE,
                min_temperature DOUBLE
            )
            USING ICEBERG
        """)

        spark.sql("""
            INSERT OVERWRITE weather.gold.daily_weather_summary
            SELECT date(event_time),
                   country,
                   AVG(temperature),
                   AVG(humidity),
                   MAX(temperature),
                   MIN(temperature)
            FROM weather.silver.weather_clean
            GROUP BY date(event_time), country
        """)

        logger.info("Updated daily_weather_summary")

    finally:
        connector.stop()


if __name__ == "__main__":
    main()
from spark.spark_config import SparkConnect
import logging
import os

from weather_pipeline.business_rules import filter_valid_temperature
from weather_pipeline.transformations import clean_weather, parse_weather


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting silver transform job")

    connector = SparkConnect(
        app_name="silver-transform",
        master_url=os.getenv("SPARK_MASTER_URL"),
    )

    spark = connector.spark

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.silver")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS weather.silver.weather_clean (
                city STRING,
                country STRING,
                event_time TIMESTAMP,
                temperature DOUBLE,
                humidity INT,
                wind_speed DOUBLE
            )
            USING ICEBERG
            PARTITIONED BY (days(event_time))
        """)

        bronze_df = spark.table("weather.bronze.weather_raw")
        logger.info("Read bronze records: %d", bronze_df.count())

        parsed_df = parse_weather(bronze_df)
        clean_df = clean_weather(parsed_df)
        quality_df = filter_valid_temperature(clean_df)

        quality_df.writeTo("weather.silver.weather_clean").overwritePartitions()
        logger.info("Silver transform wrote records: %d", quality_df.count())

    finally:
        connector.stop()


if __name__ == "__main__":
    main()

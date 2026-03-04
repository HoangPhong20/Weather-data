from spark.spark_config import SparkConnect
from pyspark.sql.functions import col, current_timestamp
import logging
import os

INPUT_FILE = "data/weather_raw.jsonl"
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main() -> None:
    logger.info("Starting bronze ingest job with input file: %s", INPUT_FILE)

    # Spark session (configs auto-loaded from spark-defaults.conf)
    connector = SparkConnect(
        app_name="bronze-ingest",
        master_url=os.getenv("SPARK_MASTER_URL"),
    )

    spark = connector.spark

    # --------------------------------------------------
    # Create namespace
    # --------------------------------------------------
    spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.bronze")

    # --------------------------------------------------
    # Create bronze table (RAW layer)
    # --------------------------------------------------
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS weather.bronze.weather_raw (
            raw_json STRING,
            ingestion_time TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(ingestion_time))
        """
    )

    # --------------------------------------------------
    # Bronze ingest (no parsing)
    # --------------------------------------------------
    source_df = spark.read.text(INPUT_FILE)
    logger.info("Loaded raw source records: %d", source_df.count())

    bronze_df = source_df.select(
        col("value").alias("raw_json"),
        current_timestamp().alias("ingestion_time"),
    )

    bronze_df.writeTo("weather.bronze.weather_raw").append()
    logger.info("Appended bronze records: %d", bronze_df.count())

    connector.stop()


if __name__ == "__main__":
    main()

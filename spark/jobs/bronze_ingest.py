from app.spark_cfg.spark_config import SparkConnect
from pyspark.sql.functions import col, current_timestamp
import logging
import os

INPUT_FILE = "data/weather_raw.jsonl"

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("bronze")


def main() -> None:
    logger.info("Bronze ingest started | input=%s", INPUT_FILE)

    connector = SparkConnect(
        app_name=f"bronze-{os.getenv('AIRFLOW_RUN_ID','local')}",
        master_url=os.getenv("SPARK_MASTER_URL"),
    )

    spark = connector.spark

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.bronze")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS weather.bronze.weather_raw (
                raw_json STRING,
                ingestion_time TIMESTAMP
            )
            USING ICEBERG
            PARTITIONED BY (days(ingestion_time))
        """)

        bronze_df = (
            spark.read.text(INPUT_FILE)
            .select(
                col("value").alias("raw_json"),
                current_timestamp().alias("ingestion_time"),
            )
        )

        bronze_df.writeTo(
            "weather.bronze.weather_raw"
        ).append()

        logger.info("Bronze ingest completed")

    finally:
        connector.stop()


if __name__ == "__main__":
    main()
from spark.spark_config import SparkConnect
from pyspark.sql.functions import col, current_timestamp
import os

INPUT_FILE = "data/weather_raw.jsonl"

def main() -> None:

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

    bronze_df = source_df.select(
        col("value").alias("raw_json"),
        current_timestamp().alias("ingestion_time"),
    )

    bronze_df.writeTo("weather.bronze.weather_raw").append()

    connector.stop()


if __name__ == "__main__":
    main()
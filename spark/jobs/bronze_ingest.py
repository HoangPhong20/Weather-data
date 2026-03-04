from spark.spark_config import Spark_connect
from config.spark_iceberg import JAR_PACKAGES, ICEBERG_CONF

from pyspark.sql.functions import col, current_timestamp

INPUT_FILE = "data/weather_raw.jsonl"


def main() -> None:
    connector = Spark_connect(
        app_name="bronze-ingest",
        jar_packages=JAR_PACKAGES,
        spark_conf=ICEBERG_CONF,
    )

    spark = connector.spark

    # namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.bronze")

    # bronze table
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

    # ✅ Bronze = RAW (không parse JSON)
    source_df = spark.read.text(INPUT_FILE)

    bronze_df = source_df.select(
        col("value").alias("raw_json"),
        current_timestamp().alias("ingestion_time"),
    )

    bronze_df.writeTo("weather.bronze.weather_raw").append()

    connector.stop()


if __name__ == "__main__":
    main()
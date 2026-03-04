from spark.spark_config import Spark_connect
from pyspark.sql.functions import col, to_timestamp

JAR_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]

INPUT_FILE = "data/weather_raw.jsonl"


def main() -> None:
    connector = Spark_connect(app_name="bronze-ingest", jar_packages=JAR_PACKAGES)
    spark = connector.spark

    spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.bronze")
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

    source_df = spark.read.json(INPUT_FILE)
    bronze_df = source_df.select(
        col("raw_json").cast("string"),
        to_timestamp(col("ingestion_time")).alias("ingestion_time"),
    )

    bronze_df.writeTo("weather.bronze.weather_raw").append()
    connector.stop()


if __name__ == "__main__":
    main()

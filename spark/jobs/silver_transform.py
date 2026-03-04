from spark.spark_config import SparkConnect
from pyspark.sql.functions import col, from_json, upper, from_unixtime
from pyspark.sql.types import *
import logging
import os

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

schema = StructType([
    StructField("name", StringType()),
    StructField("dt", LongType()),
    StructField("sys", StructType([
        StructField("country", StringType())
    ])),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType()),
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType())
    ])),
])


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

        parsed_df = (
            bronze_df
            .select(from_json(col("raw_json"), schema).alias("w"))
            .select("w.*")
        )

        clean_df = (
            parsed_df.select(
                col("name").alias("city"),
                upper(col("sys.country")).alias("country"),
                from_unixtime(col("dt")).cast("timestamp").alias("event_time"),
                (col("main.temp") - 273.15).alias("temperature"),
                col("main.humidity").alias("humidity"),
                col("wind.speed").alias("wind_speed"),
            )
            .dropna(subset=["city", "country", "event_time"])
            .dropDuplicates(["city", "country", "event_time"])
        )

        clean_df.writeTo(
            "weather.silver.weather_clean"
        ).overwritePartitions()
        logger.info("Silver transform wrote records: %d", clean_df.count())

    finally:
        connector.stop()


if __name__ == "__main__":
    main()

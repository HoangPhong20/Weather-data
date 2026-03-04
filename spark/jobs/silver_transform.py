from spark.spark_config import Spark_connect
from pyspark.sql.functions import col, from_json, upper
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


schema = StructType(
    [
        StructField("name", StringType()),
        StructField("dt", LongType()),
        StructField("sys", StructType([StructField("country", StringType())])),
        StructField(
            "main",
            StructType(
                [
                    StructField("temp", DoubleType()),
                    StructField("humidity", IntegerType()),
                ]
            ),
        ),
        StructField("wind", StructType([StructField("speed", DoubleType())])),
    ]
)

JAR_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]


def main() -> None:
    connector = Spark_connect(app_name="silver-transform", jar_packages=JAR_PACKAGES)
    spark = connector.spark

    spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.silver")
    spark.sql(
        """
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
        """
    )

    bronze_df = spark.table("weather.bronze.weather_raw")
    parsed_df = bronze_df.select(from_json(col("raw_json"), schema).alias("w")).select("w.*")

    clean_df = (
        parsed_df.select(
            col("name").alias("city"),
            upper(col("sys.country")).alias("country"),
            col("dt").cast("timestamp").alias("event_time"),
            (col("main.temp") - 273.15).alias("temperature"),
            col("main.humidity").alias("humidity"),
            col("wind.speed").alias("wind_speed"),
        )
        .dropna(subset=["city", "country", "event_time", "temperature"])
        .dropDuplicates(["city", "country", "event_time"])
    )

    clean_df.writeTo("weather.silver.weather_clean").overwritePartitions()
    connector.stop()


if __name__ == "__main__":
    main()

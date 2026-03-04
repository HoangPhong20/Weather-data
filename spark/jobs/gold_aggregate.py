from spark.spark_config import Spark_connect

JAR_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
]


def main() -> None:
    connector = Spark_connect(app_name="gold-aggregate", jar_packages=JAR_PACKAGES)
    spark = connector.spark

    spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.gold")

    spark.sql(
        """
        CREATE OR REPLACE TABLE weather.gold.avg_temp_by_country
        USING ICEBERG
        AS
        SELECT country, AVG(temperature) AS avg_temp
        FROM weather.silver.weather_clean
        GROUP BY country
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE weather.gold.daily_weather_summary
        USING ICEBERG
        AS
        SELECT date(event_time) AS weather_day,
               country,
               AVG(temperature) AS avg_temperature,
               AVG(humidity) AS avg_humidity,
               MAX(temperature) AS max_temperature,
               MIN(temperature) AS min_temperature
        FROM weather.silver.weather_clean
        GROUP BY date(event_time), country
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE weather.gold.hottest_city_per_day
        USING ICEBERG
        AS
        SELECT weather_day, country, city, temperature
        FROM (
            SELECT date(event_time) AS weather_day,
                   country,
                   city,
                   temperature,
                   row_number() OVER (
                       PARTITION BY date(event_time), country
                       ORDER BY temperature DESC
                   ) AS rank_no
            FROM weather.silver.weather_clean
        )
        WHERE rank_no = 1
        """
    )

    connector.stop()


if __name__ == "__main__":
    main()

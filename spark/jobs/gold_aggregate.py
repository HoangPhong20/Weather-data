from spark.spark_config import Spark_connect
from config.spark_iceberg import JAR_PACKAGES, ICEBERG_CONF


def main() -> None:
    connector = Spark_connect(
        app_name="gold-aggregate",
        jar_packages=JAR_PACKAGES,
        spark_conf=ICEBERG_CONF,
    )

    spark = connector.spark

    spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.gold")

    # avg temp
    spark.sql("""
        CREATE OR REPLACE TABLE weather.gold.avg_temp_by_country
        USING ICEBERG
        AS
        SELECT country, AVG(temperature) AS avg_temp
        FROM weather.silver.weather_clean
        GROUP BY country
    """)

    # daily summary
    spark.sql("""
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
    """)

    # hottest city
    spark.sql("""
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
    """)

    connector.stop()


if __name__ == "__main__":
    main()
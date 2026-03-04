import os
from spark.spark_config import SparkConnect


def main() -> None:

    connector = SparkConnect(
        app_name="gold-aggregate",
        master_url=os.getenv("SPARK_MASTER_URL"),
    )

    spark = connector.spark

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS weather.gold")

        # -----------------------------------------
        # Avg temperature by country
        # -----------------------------------------
        spark.sql("""
            CREATE OR REPLACE TABLE weather.gold.avg_temp_by_country
            USING ICEBERG AS
            SELECT country,
                   AVG(temperature) AS avg_temp
            FROM weather.silver.weather_clean
            GROUP BY country
        """)

        # -----------------------------------------
        # Daily summary
        # -----------------------------------------
        spark.sql("""
            CREATE OR REPLACE TABLE weather.gold.daily_weather_summary
            USING ICEBERG AS
            SELECT date(event_time) AS weather_day,
                   country,
                   AVG(temperature) AS avg_temperature,
                   AVG(humidity) AS avg_humidity,
                   MAX(temperature) AS max_temperature,
                   MIN(temperature) AS min_temperature
            FROM weather.silver.weather_clean
            GROUP BY date(event_time), country
        """)

        # -----------------------------------------
        # Hottest city per day
        # -----------------------------------------
        spark.sql("""
            CREATE OR REPLACE TABLE weather.gold.hottest_city_per_day
            USING ICEBERG AS
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

    finally:
        connector.stop()


if __name__ == "__main__":
    main()
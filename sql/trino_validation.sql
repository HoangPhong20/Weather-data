-- Query bronze
SELECT * FROM iceberg.bronze.weather_raw LIMIT 10;

-- Query silver
SELECT country, AVG(temperature) AS avg_temp
FROM iceberg.silver.weather_clean
GROUP BY country
ORDER BY avg_temp DESC;

-- Query gold
SELECT * FROM iceberg.gold.avg_temp_by_country ORDER BY avg_temp DESC;

-- Example join silver + gold
SELECT s.city, s.country, s.temperature, g.avg_temp
FROM iceberg.silver.weather_clean s
JOIN iceberg.gold.avg_temp_by_country g
  ON s.country = g.country
LIMIT 20;

-- Iceberg optimize (file compaction)
ALTER TABLE iceberg.gold.daily_weather_summary EXECUTE optimize;

-- Iceberg vacuum equivalent (snapshot cleanup + orphan file cleanup)
ALTER TABLE iceberg.gold.daily_weather_summary EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE iceberg.gold.daily_weather_summary EXECUTE remove_orphan_files(retention_threshold => '7d');

-- Time travel demo by timestamp
SELECT *
FROM iceberg.gold.daily_weather_summary
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-01 00:00:00 UTC'
LIMIT 20;

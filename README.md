# Weather Lakehouse Mini Data Platform

Pipeline kiến trúc:

Airflow -> Extract Weather API -> MinIO Bronze (Iceberg) -> Spark Transform -> MinIO Silver (Iceberg) -> Spark Aggregation -> MinIO Gold (Iceberg) -> Trino Query

## Core Components

- **Airflow**: orchestration theo DAG `extract_task -> bronze_task -> silver_task -> gold_task`
- **Spark**: ingest + transform + aggregate
- **MinIO**: S3-compatible object storage (bucket `weather-lake`)
- **Apache Iceberg**: table format cho Bronze/Silver/Gold
- **Trino**: query engine cho SQL analytics

## Iceberg Partition Strategy

Các bảng đang dùng chiến lược partition theo ngày để cân bằng truy vấn theo time-window và quản trị file:

- `weather.bronze.weather_raw` -> `PARTITIONED BY (days(ingestion_time))`
- `weather.silver.weather_clean` -> `PARTITIONED BY (days(event_time))`

## Iceberg Optimization (Advanced)

### 1) OPTIMIZE / file compaction
Dùng Trino:

```sql
ALTER TABLE iceberg.gold.daily_weather_summary EXECUTE optimize;
```

Hoặc chạy Spark maintenance job:

```bash
spark-submit spark/jobs/iceberg_maintenance.py
```

### 2) VACUUM equivalent for Iceberg
Iceberg không dùng cú pháp `VACUUM` như một số hệ khác; thay vào đó dùng:

- `expire_snapshots` để dọn snapshot cũ
- `remove_orphan_files` để dọn file mồ côi

Ví dụ Trino:

```sql
ALTER TABLE iceberg.gold.daily_weather_summary EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE iceberg.gold.daily_weather_summary EXECUTE remove_orphan_files(retention_threshold => '7d');
```

### 3) Snapshot / Time Travel query demo

```sql
SELECT *
FROM iceberg.gold.daily_weather_summary
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-01 00:00:00 UTC'
LIMIT 20;
```

## Claim

**Supports time travel & ACID using Apache Iceberg**.

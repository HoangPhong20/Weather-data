# Weather Data ETL

Dự án ETL dữ liệu thời tiết với Python/Spark, bổ sung orchestration bằng **Apache Airflow**.

## 1) Cài dependencies (bao gồm Airflow theo Python hiện tại)

Repository đang dùng `requirement.txt`.

```bash
python3 --version
pip install -r requirement.txt
```

`requirement.txt` đã thêm:
- `apache-airflow==2.10.3`
- constraints file tương ứng Python 3.10:
  `https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.10.txt`

## 2) Chạy Airflow nhanh bằng Docker Compose

File cấu hình: `docker-compose.airflow.yml`.

### Thành phần
- `airflow-db`: metadata DB (PostgreSQL)
- `airflow-init`: init DB + tạo user admin
- `airflow-webserver`: Airflow UI tại `http://localhost:8080`
- `airflow-scheduler`: scheduler chạy DAG

`airflow-webserver` và `airflow-scheduler` được cấu hình phụ thuộc `airflow-init` (chỉ start sau khi init thành công).

### AIRFLOW_HOME + mounts
- `AIRFLOW_HOME=/opt/airflow`
- mount DAGs: `./airflow/dags:/opt/airflow/dags`
- mount project: `./:/opt/airflow/project`

### Env vars cho ETL task
`docker-compose.airflow.yml` đã map sẵn các biến sau để DAG `weather_etl_dag` dùng:
- `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`

Ngoài ra, compose có `_PIP_ADDITIONAL_REQUIREMENTS` để cài thêm package Python cần cho task ETL trong container Airflow (`mysql-connector-python`, `psycopg2-binary`, `python-dotenv`).

Bạn có thể override bằng cách export trước khi chạy compose:

```bash
export MYSQL_HOST=host.docker.internal
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=root
export MYSQL_DATABASE=southeast_asia

export POSTGRES_HOST=host.docker.internal
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DATABASE=east_asia
```

## 3) Vận hành Airflow

### Bước A - Init DB Airflow + tạo user admin

```bash
docker compose -f docker-compose.airflow.yml up airflow-init
```

Tài khoản mặc định trong compose:
- username: `admin`
- password: `admin`

### Bước B - Bật scheduler + webserver

```bash
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

### Bước C - Kiểm tra DAG `weather_etl_dag`

1. Mở Airflow UI: `http://localhost:8080`
2. Đăng nhập bằng tài khoản admin.
3. Tìm DAG: **weather_etl_dag**.
4. Trigger thủ công hoặc chờ lịch `*/30 * * * *`.

CLI check nhanh:

```bash
docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags list | grep weather_etl_dag
```

## 4) Dừng hệ thống

```bash
docker compose -f docker-compose.airflow.yml down
```

Muốn xoá volume metadata DB:

```bash
docker compose -f docker-compose.airflow.yml down -v
```

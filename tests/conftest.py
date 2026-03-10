import sys
from pathlib import Path

import pytest


# ---- Add project root to PYTHONPATH ----
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ---- Shared Spark session for tests ----
@pytest.fixture(scope="session") # Tạo một object dùng chung cho toàn bộ test session và pytest tự động truyền nó vào các test cần dùng.
def spark():
    pyspark = pytest.importorskip("pyspark") # Nếu pyspark không được cài đặt, pytest sẽ bỏ qua tất cả các test sử dụng fixture
    spark = (
        pyspark.sql.SparkSession.builder
        .master("local[*]")
        .appName("weather-data-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark # Sau khi tất cả các test sử dụng fixture này hoàn thành, pytest sẽ tiếp tục thực hiện phần code sau yield để dọn dẹp tài nguyên, ở đây là dừng Spark session.

    spark.stop()
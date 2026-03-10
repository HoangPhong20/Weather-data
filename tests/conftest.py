import os
import sys

import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    SparkSession = pyspark.sql.SparkSession
    session = (
        SparkSession.builder.master("local[1]")
        .appName("weather-data-tests")
        .getOrCreate()
    )
    yield session
    session.stop()

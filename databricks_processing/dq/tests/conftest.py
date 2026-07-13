"""Pytest config: Spark session fixture cho DQ tests."""

import os
import sys

# Thêm thư mục cha `databricks_processing` vào sys.path để `import dq` hoạt động
# (chạy được từ bất kỳ cwd nào, kể cả CI).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Ép worker python khớp driver (tránh PYTHON_VERSION_MISMATCH khi system có nhiều python,
# vd shell đã set PYSPARK_PYTHON=python3 trỏ python 3.13). Test luôn chạy bằng python của venv.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# Nếu máy có cài Spark khác version (vd brew Spark 4.x) qua SPARK_HOME, unset để dùng
# JAR đi kèm pyspark pip. CI thường không có SPARK_HOME nên không ảnh hưởng.
if os.environ.get("SPARK_HOME") and not os.environ.get("DQ_KEEP_SPARK_HOME"):
    os.environ.pop("SPARK_HOME", None)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("dq-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()

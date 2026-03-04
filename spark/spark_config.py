from pyspark.sql import SparkSession
from typing import Optional, List, Dict


class Spark_connect:
    def __init__(
        self,
        app_name: str,
        master_url: str = "local[*]",
        executor_memory: Optional[str] = None,
        executor_cores: Optional[int] = None,
        driver_memory: Optional[str] = None,
        num_executors: Optional[int] = None,
        jar_packages: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "INFO",
    ):
        self.app_name = app_name

        self.spark = self.create_spark_session(
            master_url,
            executor_memory,
            executor_cores,
            driver_memory,
            num_executors,
            jar_packages,
            spark_conf,
            log_level,
        )

    def create_spark_session(
        self,
        master_url: str = "local[*]",
        executor_memory: Optional[str] = None,
        executor_cores: Optional[int] = None,
        driver_memory: Optional[str] = None,
        num_executors: Optional[int] = None,
        jar_packages: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "INFO",
    ) -> SparkSession:

        builder = (
            SparkSession.builder
            .appName(self.app_name)
            .master(master_url)
        )

        # Optional configs (override only if provided)
        if executor_memory:
            builder.config("spark.executor.memory", executor_memory)

        if executor_cores:
            builder.config("spark.executor.cores", executor_cores)

        if driver_memory:
            builder.config("spark.driver.memory", driver_memory)

        # ✅ FIX: đúng key Spark
        if num_executors:
            builder.config("spark.executor.instances", num_executors)

        # Jar packages (Iceberg, Kafka...)
        if jar_packages:
            builder.config(
                "spark.jars.packages",
                ",".join(jar_packages),
            )

        # Extra spark configs
        if spark_conf:
            for key, value in spark_conf.items():
                builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            print("-------- stop spark session --------")
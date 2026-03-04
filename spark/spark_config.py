from pyspark.sql import SparkSession
from typing import Optional, Dict, List


class Spark_connect:

    def __init__(
        self,
        app_name: str,
        master_url: Optional[str] = None,
        jar_packages: Optional[List[str]] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        log_level: str = "INFO",
    ):
        self.spark = self.create_session(
            app_name,
            master_url,
            jar_packages,
            spark_conf,
            log_level,
        )

    # --------------------------------------------------
    # Create Spark Session
    # --------------------------------------------------
    def create_session(
        self,
        app_name: str,
        master_url: Optional[str],
        jar_packages: Optional[List[str]],
        spark_conf: Optional[Dict[str, str]],
        log_level: str,
    ) -> SparkSession:

        builder = SparkSession.builder.appName(app_name)

        # master
        if master_url:
            builder = builder.master(master_url)

        # jars (Iceberg + S3)
        if jar_packages:
            builder = builder.config(
                "spark.jars.packages",
                ",".join(jar_packages),
            )

        # spark configs
        if spark_conf:
            for k, v in spark_conf.items():
                builder = builder.config(k, v)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        return spark

    # --------------------------------------------------
    def stop(self) -> None:
        if self.spark:
            self.spark.stop()
            print("-------- stop spark session --------")
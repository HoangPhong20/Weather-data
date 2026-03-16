import logging
from pyspark.sql import SparkSession
from typing import Optional, Dict, List


logger = logging.getLogger(__name__)

class SparkConnect:

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
    def create_session(
        self,
        app_name: str,
        master_url: Optional[str],
        jar_packages: Optional[List[str]],
        spark_conf: Optional[Dict[str, str]],
        log_level: str,
    ) -> SparkSession:

        builder = SparkSession.builder.appName(app_name)

        # ✅ only override master if provided
        if master_url:
            builder = builder.master(master_url)

        # ✅ extra jars (NOT replace defaults)
        if jar_packages:
            builder = builder.config(
                "spark.jars.packages",
                ",".join(jar_packages),
            )

        # ✅ optional overrides
        if spark_conf:
            for key, value in spark_conf.items():
                builder = builder.config(key, value)

        # ⭐ Spark will automatically read:
        # spark-defaults.conf
        spark = builder.getOrCreate()

        spark.sparkContext.setLogLevel(log_level)

        return spark

    # --------------------------------------------------
    def stop(self):
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

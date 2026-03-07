import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "TradeAnalyticsLakehouse") -> SparkSession:
    """
    Returns a SparkSession singleton.
    - Inside Databricks: Reuse the existing spark session
    - Local dev: Creates a new spark session with Delta Lake support
    """

    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ

    if is_databricks:
        spark = SparkSession.builder.getOrCreate()

    else:
        try:
            from delta import configure_spark_with_delta_pip
            builder = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.shuffle.partitions", "8")   # Sane local default
                .config("spark.driver.memory", "4g")
            )
            spark = configure_spark_with_delta_pip(builder).getOrCreate()

        except ImportError:
            raise RuntimeError(
                "delta-spark not installed. Run: pip install delta-spark"
            )

    _log_session_info(spark, is_databricks)
    return spark


def _log_session_info(spark: SparkSession, is_databricks: bool) -> None:
    env  = "Databricks" if is_databricks else "Local"
    print(f"[SparkSession] Environment : {env}")
    print(f"[SparkSession] App name    : {spark.conf.get('spark.app.name')}")
    print(f"[SparkSession] Spark ver   : {spark.version}")
    if not is_databricks:
        print(f"[SparkSession] Master  : {spark.conf.get('spark.master')}")
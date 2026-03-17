import os
from pyspark.sql import SparkSession

JAVA_17_OPTIONS = " ".join([
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "-Dio.netty.tryReflectionSetAccessible=true",
])


def get_spark(app_name: str = "TradeAnalyticsLakehouse") -> SparkSession:
    """
    Returns a SparkSession singleton.
    - SPARK_ENV=local  → Force local mode (ignores Databricks runtime)
    - Inside Databricks → Reuse the existing Databricks session
    - Local dev default → Creates a new spark session with Delta Lake support
    """
    spark_env = os.getenv("SPARK_ENV", "local")          # Default to local
    is_databricks = (
        spark_env != "local"                              # Respect forced local
        and "DATABRICKS_RUNTIME_VERSION" in os.environ   # Or actual Databricks
    )
    
    if is_databricks:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    else:
        try:
            from delta import configure_spark_with_delta_pip
            builder = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.driver.extraJavaOptions",  JAVA_17_OPTIONS)
                .config("spark.executor.extraJavaOptions", JAVA_17_OPTIONS)
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
    env = os.getenv("SPARK_ENV", "local")
    print(f"[SparkSession] Environment : {env}")
    print(f"[SparkSession] App name    : {spark.conf.get('spark.app.name')}")
    print(f"[SparkSession] Spark ver   : {spark.version}")
    if not is_databricks:
        print(f"[SparkSession] Master  : {spark.conf.get('spark.master')}")
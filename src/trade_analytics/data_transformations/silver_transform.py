import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
)
from pyspark.sql.window import Window

from src.trade_analytics.config.settings import (
    BRONZE_PATH,
    FX_RATES_TO_EUR,
    LARGE_TRADE_THRESHOLD_EUR,
    SILVER_PATH,
    VELOCITY_MAX_TRADES,
)
from src.trade_analytics.config.spark_session import get_spark
from src.trade_analytics.config.utils import configure_adls_auth

_FX_MAP: dict[str, float] = FX_RATES_TO_EUR
_EQUITY_INSTRUMENTS = {"AAPL", "MSFT", "BMW.DE", "SAP.DE"}
_COMMODITY_INSTRUMENTS = {"BRENT", "GOLD"}


def _build_fx_udf(spark: SparkSession):
    """
    Returns a PySpark UDF that converts notional to EUR using the FX map.
    Defined inside a function so the broadcast reference is captured correctly
    whether running locally or on Databricks.
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType

    fx_map = _FX_MAP

    @udf(returnType=DoubleType())
    def to_eur(instrument: str, notional: float) -> float:
        if instrument is None or notional is None:
            return None
        rate = fx_map.get(instrument.upper(), 1.0)
        return round(notional * rate, 2)

    return to_eur


def _read_bronze(spark: SparkSession) -> DataFrame:
    """
    Reads the Bronze Delta table (append-only, raw validated records).
    Keeps the two audit columns (_ingested_at, _source) so Silver
    preserves full lineage.
    """
    df = spark.read.format("delta").load(BRONZE_PATH)
    print(f"[Bronze] Row count : {df.count():,}")
    print("[Bronze] Schema    :")
    df.printSchema()
    return df


def _cast_and_clean(df: DataFrame) -> DataFrame:
    """
    - Cast every field to its canonical type (Spark infers strings from JSON).
    - Parse trade_timestamp to TimestampType; derive trade_date and trade_hour.
    - Drop any row where trade_id, trader_id, notional, or timestamp is null
      (these are hard invariants — should already be caught by Bronze Pydantic
      validation, but we defend in depth).
    """
    df = (
        df
        # ── String normalisation
        .withColumn("instrument", F.upper(F.trim(F.col("instrument"))))
        .withColumn("trader_id", F.upper(F.trim(F.col("trader_id"))))
        .withColumn("direction", F.upper(F.trim(F.col("direction"))))
        .withColumn("desk", F.initcap(F.trim(F.col("desk"))))
        .withColumn("region", F.upper(F.trim(F.col("region"))))
        .withColumn("counterparty", F.trim(F.col("counterparty")))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("notional", F.col("notional").cast(DecimalType(24, 4)))
        .withColumn("price", F.col("price").cast(DecimalType(18, 6)))
        .withColumn("trade_timestamp", F.to_timestamp(F.col("trade_timestamp")))
        .withColumn("trade_date", F.col("trade_timestamp").cast(DateType()))
        .withColumn("trade_hour", F.hour(F.col("trade_timestamp")).cast(IntegerType()))
        .withColumn("trade_minute", F.minute(F.col("trade_timestamp")).cast(IntegerType()))
        .withColumn("is_anomaly", F.col("is_anomaly").cast(BooleanType()))
        .dropna(subset=["trade_id", "trader_id", "notional", "trade_timestamp"])
    )

    print(f"[Clean ] Row count after null drop : {df.count():,}")
    return df


def _apply_business_filters(df: DataFrame) -> DataFrame:
    """
    Remove CANCELLED trades — they are not relevant for P&L or risk exposure.
    PENDING trades are kept (they may settle later).
    """
    before = df.count()
    df = df.filter(F.col("status") != "CANCELLED")
    after = df.count()

    print(f"[Filter] Removed {before - after:,} CANCELLED rows → {after:,} remain")
    return df


def _engineer_features(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Adds all derived columns used by the dbt Gold layer downstream.

    Columns added
    ─────────────
    notional_eur              : notional converted to EUR via FX rate UDF
    signed_notional_eur       : positive for BUY, negative for SELL
    is_large_trade            : bool — notional_eur > threshold from settings
    risk_tier                 : HIGH / MEDIUM / LOW based on notional_eur bands
    time_of_day               : PREMARKET / MORNING / AFTERNOON / CLOSE / AFTERHOURS
    trades_in_hour            : count of trades by same trader in same hour (window)
    velocity_flag             : bool — trades_in_hour > VELOCITY_MAX_TRADES
    trader_daily_notional_run : running cumulative notional per trader per day (window)
    alert_level               : CRITICAL / WARNING / NORMAL — composite anomaly signal
    """

    to_eur_udf = _build_fx_udf(spark)

    df = df.withColumn(
        "notional_eur", to_eur_udf(F.col("instrument"), F.col("notional").cast("double"))
    )

    df = df.withColumn(
        "signed_notional_eur",
        F.when(F.col("direction") == "BUY", F.col("notional_eur"))
        .when(F.col("direction") == "SELL", -F.col("notional_eur"))
        .otherwise(F.lit(0.0)),
    )

    df = df.withColumn(
        "is_large_trade", (F.col("notional_eur") > LARGE_TRADE_THRESHOLD_EUR).cast(BooleanType())
    )

    df = df.withColumn(
        "risk_tier",
        F.when(F.col("notional_eur") > 5_000_000, F.lit("HIGH"))
        .when(F.col("notional_eur") > LARGE_TRADE_THRESHOLD_EUR, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )

    df = df.withColumn(
        "time_of_day",
        F.when(F.col("trade_hour") < 8, F.lit("PREMARKET"))
        .when(F.col("trade_hour") < 12, F.lit("MORNING"))
        .when(F.col("trade_hour") < 16, F.lit("AFTERNOON"))
        .when(F.col("trade_hour") < 18, F.lit("CLOSE"))
        .otherwise(F.lit("AFTERHOURS")),
    )

    trader_hour_window = Window.partitionBy("trader_id", "trade_date", "trade_hour")
    df = df.withColumn(
        "trades_in_hour", F.count("trade_id").over(trader_hour_window).cast(IntegerType())
    )
    df = df.withColumn(
        "velocity_flag", (F.col("trades_in_hour") > VELOCITY_MAX_TRADES).cast(BooleanType())
    )

    trader_day_running_window = (
        Window.partitionBy("trader_id", "trade_date")
        .orderBy(F.col("trade_timestamp").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn(
        "trader_daily_notional_run", F.sum(F.col("notional_eur")).over(trader_day_running_window)
    )

    df = df.withColumn(
        "alert_level",
        F.when(F.col("is_anomaly") & F.col("velocity_flag"), F.lit("CRITICAL"))
        .when(F.col("is_anomaly") | F.col("velocity_flag"), F.lit("WARNING"))
        .otherwise(F.lit("NORMAL")),
    )

    return df


def _select_silver_schema(df: DataFrame) -> DataFrame:
    """
    Explicitly select and order the Silver columns.
    Dropping intermediate or redundant columns here keeps the Silver schema
    clean and makes downstream dbt models easier to reason about.
    """
    return df.select(
        F.col("trade_id"),
        F.col("trader_id"),
        F.col("instrument"),
        F.col("direction"),
        F.col("desk"),
        F.col("region"),
        F.col("counterparty"),
        F.col("status"),
        F.col("notional").cast(DecimalType(24, 4)),
        F.col("notional_eur").cast(DecimalType(24, 4)),
        F.col("signed_notional_eur").cast(DecimalType(24, 4)),
        F.col("price").cast(DecimalType(18, 6)),
        F.col("trade_timestamp"),
        F.col("trade_date"),
        F.col("trade_hour"),
        F.col("trade_minute"),
        F.col("time_of_day"),
        F.col("is_large_trade"),
        F.col("risk_tier"),
        F.col("velocity_flag"),
        F.col("trades_in_hour"),
        F.col("trader_daily_notional_run").cast(DecimalType(28, 4)),
        F.col("alert_level"),
        F.col("is_anomaly"),
        F.col("_ingested_at"),
        F.col("_source"),
    )


def _write_silver(df: DataFrame, spark: SparkSession) -> None:
    """
    Writes Silver Delta table with:
    - Partition by trade_date  → efficient date-range queries
    - Overwrite mode           → idempotent; re-running the flow is safe
    - OPTIMIZE + ZORDER        → speeds up trader/instrument point lookups
                                 in the dbt Gold layer and Streamlit dashboard

    On Databricks Community Edition, OPTIMIZE runs on the cluster.
    Locally it's a no-op (Delta OSS doesn't support OPTIMIZE in the same way)
    so we guard with a try/except to avoid failing local dev runs.
    """
    print(f"[Silver] Writing to {SILVER_PATH} ...")

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("trade_date")
        .save(SILVER_PATH)
    )

    row_count = spark.read.format("delta").load(SILVER_PATH).count()
    print(f"[Silver] Write complete. Row count: {row_count:,}")

    try:
        spark.sql(f"""
            OPTIMIZE delta.`{SILVER_PATH}`
            ZORDER BY (trader_id, instrument)
        """)
        print("[Silver] OPTIMIZE + ZORDER complete")
    except Exception as e:
        print(f"[Silver] OPTIMIZE skipped (local env): {e}")


def _print_summary(spark: SparkSession) -> None:
    """
    Prints a quick sanity-check summary after the Silver write.
    Useful for Prefect task logs and manual notebook runs.
    """
    df = spark.read.format("delta").load(SILVER_PATH)

    print("\n── Silver Summary ────────────────────────────────────")
    print(f"  Total rows          : {df.count():,}")
    print(
        f"  Date range          : {df.selectExpr('min(trade_date)').collect()[0][0]}"
        f"  →  {df.selectExpr('max(trade_date)').collect()[0][0]}"
    )
    print(f"  Distinct traders    : {df.select('trader_id').distinct().count():,}")
    print(f"  Distinct instruments: {df.select('instrument').distinct().count():,}")

    print("\n  Alert level breakdown:")
    (df.groupBy("alert_level").count().orderBy("alert_level").show(truncate=False))

    print("  Risk tier breakdown:")
    (df.groupBy("risk_tier").count().orderBy("risk_tier").show(truncate=False))

    print("  Velocity-flagged trades:")
    (
        df.filter(F.col("velocity_flag"))
        .groupBy("trader_id")
        .agg(F.count("trade_id").alias("flagged_trades"))
        .orderBy(F.col("flagged_trades").desc())
        .limit(10)
        .show(truncate=False)
    )


def transform(bronze_path: str = None, silver_path: str = None) -> None:
    """
    Full Bronze → Silver transformation pipeline.

    Args:
        bronze_path: override for testing; defaults to settings.BRONZE_PATH
        silver_path: override for testing; defaults to settings.SILVER_PATH

    Called by:
        - flows/trade_pipeline_flow.py  (Prefect @task)
        - databricks_notebooks/02_silver_transform.ipynb
        - python transformations/silver_transform.py  (direct CLI run)
    """
    spark = get_spark("TradeAnalyticsLakehouse-SilverTransform")

    global BRONZE_PATH, SILVER_PATH
    if bronze_path:
        BRONZE_PATH = bronze_path
    if silver_path:
        SILVER_PATH = silver_path

    configure_adls_auth(spark)

    df = _read_bronze(spark)
    df = _cast_and_clean(df)
    df = _apply_business_filters(df)
    df = _engineer_features(df, spark)
    df = _select_silver_schema(df)

    _write_silver(df, spark)
    _print_summary(spark)

    print("Silver transform complete ✓\n")


if __name__ == "__main__":
    transform()

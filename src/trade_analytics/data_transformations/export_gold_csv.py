import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.trade_analytics.config.enums import SPARK_ENV, SparkEnv
from src.trade_analytics.config.settings import GOLD_PATH
from src.trade_analytics.config.spark_session import get_spark

_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

GOLD_TABLES = [
    "mart_daily_volume",
    "mart_risk_exposure",
    "mart_top_instruments",
]

DBT_SCHEMA = os.getenv("DBT_SCHEMA", "trade_analytics_gold")
_PROJECT_ROOT = Path(_SRC)
DASHBOARD_DATA = _PROJECT_ROOT / "dashboard" / "data"


def _read_from_unity_catalog(spark: SparkSession, table: str) -> DataFrame:
    full_name = f"{DBT_SCHEMA}.{table}"
    print(f"[Export] Reading from Unity Catalog: {full_name}")
    df = spark.sql(f"SELECT * FROM {full_name}")
    return df


def _read_from_local_delta(spark: SparkSession, table: str) -> DataFrame:
    path = f"{GOLD_PATH}/{table}"
    print(f"[Export] Reading from local Delta: {path}")
    df = spark.read.format("delta").load(path)
    return df


def _read_gold_table(spark: SparkSession, table: str) -> DataFrame:
    if SPARK_ENV == SparkEnv.DATABRICKS:
        return _read_from_unity_catalog(spark, table)
    else:
        return _read_from_local_delta(spark, table)


def _write_csv(df: DataFrame, table: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    final_path = output_dir / f"{table}.csv"
    tmp_path = output_dir / f"_{table}_tmp"

    # Remove existing temp dir if a previous run left it behind
    if tmp_path.exists():
        import shutil

        shutil.rmtree(str(tmp_path))

    # Write single-partition CSV
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(str(tmp_path))
    )

    # Find the part file Spark wrote
    part_files = list(tmp_path.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(
            f"No part-*.csv file found in {tmp_path} after writing {table}. "
            f"Contents: {list(tmp_path.iterdir())}"
        )

    # Move to final location
    import shutil

    shutil.move(str(part_files[0]), str(final_path))
    shutil.rmtree(str(tmp_path))

    return final_path


def _print_summary(results: list[dict]) -> None:
    print("\n── Gold CSV Export Summary ───────────────────────────────────")
    print(f"  {'Table':<35} {'Rows':>7}  {'Status'}")
    print(f"  {'-' * 35} {'-' * 7}  {'-' * 8}")
    for r in results:
        status = "✓ OK" if r["status"] == "success" else f"✗ {r.get('error', 'FAILED')}"
        print(f"  {r['table']:<35} {r['rows']:>7,}  {status}")
    print(f"\n  Output directory: {DASHBOARD_DATA}")
    print("──────────────────────────────────────────────────────────────\n")


# ── Public entry point ────────────────────────────────────────────────────────


def export(
    tables: list[str] = None,
    output_dir: Path = None,
) -> dict:
    if tables is None:
        tables = GOLD_TABLES

    if output_dir is None:
        output_dir = DASHBOARD_DATA

    spark = get_spark("TradeAnalytics-GoldExport")

    print("\n═══════════════════════════════════════════════════════")
    print("  Gold Export  —  Delta/Unity Catalog → CSV           ")
    print(f"  Environment  : {SPARK_ENV.value}")
    print(f"  Schema       : {DBT_SCHEMA}")
    print(f"  Output dir   : {output_dir}")
    print("═══════════════════════════════════════════════════════\n")

    results = []
    exported_tables = []

    for table in tables:
        print(f"[Export] Processing: {table}")
        try:
            # Read
            df = _read_gold_table(spark, table)

            # Row count before write (triggers Spark action)
            row_count = df.count()
            print(f"[Export] Row count: {row_count:,}")

            # Write CSV
            csv_path = _write_csv(df, table, output_dir)
            print(f"[Export] Written  : {csv_path} ✓\n")

            results.append(
                {
                    "table": table,
                    "rows": row_count,
                    "path": str(csv_path),
                    "status": "success",
                }
            )
            exported_tables.append(table)

        except Exception as e:
            print(f"[Export] ERROR on {table}: {e}\n")
            results.append(
                {
                    "table": table,
                    "rows": 0,
                    "path": "",
                    "status": "failed",
                    "error": str(e),
                }
            )

    _print_summary(results)

    # Fail loudly if any table failed — Prefect task will catch this
    failed = [r for r in results if r["status"] == "failed"]
    if failed:
        failed_names = [r["table"] for r in failed]
        raise RuntimeError(
            f"Gold CSV export failed for tables: {failed_names}. Check the logs above for details."
        )

    return {
        "exported_tables": exported_tables,
        "output_dir": str(output_dir),
        "results": results,
    }


if __name__ == "__main__":
    export()

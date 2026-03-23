"""
src/trade_analytics/flows/trade_pipeline_flow.py

Prefect flow — orchestrates the full Trade Analytics pipeline end-to-end.

Task execution order (linear — each waits for the previous):
    generate_trades -> ingest_bronze -> transform_silver -> run_dbt -> test_dbt -> export_gold_csv

Run locally:
    python -m trade_analytics.flows.trade_pipeline_flow

Deploy to Prefect Cloud:
    prefect deploy src/trade_analytics/flows/trade_pipeline_flow.py:trade_pipeline \
        --name "trade-pipeline-daily" \
        --cron "0 5 * * *"

Called by:
    - Prefect Cloud (scheduled)
    - GitHub Actions (manual trigger for testing)
    - Direct CLI run during development
"""

import os
import subprocess
import sys
import time
from datetime import timedelta
import requests
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

_HERE = os.path.dirname(os.path.abspath(__file__))
_SRC = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


@task(
    name="generate-trades",
    description="Generate 50K synthetic trade events -> data/trades.jsonl",
    retries=2,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    tags=["data-generation", "bronze"],
)
def generate_trades(n: int = 50_000, output_path: str = "./data/trades.jsonl") -> str:
    logger = get_run_logger()
    logger.info(f"Generating {n:,} trade events -> {output_path}")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    from src.trade_analytics.data_generator.generate_trades import generate_dataset

    generate_dataset(n=n, output_path=output_path)

    # Verify output
    if not os.path.exists(output_path):
        raise FileNotFoundError(f"generate_dataset ran but {output_path} was not created")

    line_count = sum(1 for _ in open(output_path))
    logger.info(f"Generated {line_count:,} records -> {output_path} ✓")

    return os.path.abspath(output_path)


@task(
    name="ingest-bronze",
    description="Validate trades.jsonl via Pydantic -> Bronze Delta + Quarantine Delta",
    retries=2,
    retry_delay_seconds=60,
    tags=["ingestion", "bronze", "delta"],
)
def ingest_bronze(input_path: str = "./data/trades.jsonl") -> dict:
    logger = get_run_logger()
    logger.info(f"Ingesting Bronze from: {input_path}")

    from src.trade_analytics.data_ingestion.ingest_bronze import ingest_bronze as _ingest

    result = _ingest(input_path=input_path)

    logger.info(
        f"Bronze ingest complete — "
        f"valid: {result['valid_count']:,} | "
        f"quarantined: {result['quarantine_count']:,} ✓"
    )
    return result


@task(
    name="transform-silver",
    description="PySpark: Bronze -> Silver Delta (FX normalisation, window functions, risk flags)",
    retries=1,
    retry_delay_seconds=120,
    tags=["transformation", "silver", "pyspark", "delta"],
)
def transform_silver() -> dict:
    logger = get_run_logger()
    logger.info("Starting Silver transform: Bronze -> Silver Delta")

    from src.trade_analytics.data_transformations.silver_transform import transform

    transform()

    from src.trade_analytics.config.settings import SILVER_PATH

    logger.info(f"Silver transform complete -> {SILVER_PATH} ✓")

    return {"silver_path": SILVER_PATH}


@task(name="wake-warehouse", description="Ensure SQL Warehouse is running before dbt")
def wake_warehouse() -> None:
    logger = get_run_logger()

    host          = os.environ["DATABRICKS_HOST"].rstrip("/")
    token         = os.environ["DATABRICKS_TOKEN"]
    http_path     = os.environ.get("DATABRICKS_HTTP_PATH", "")
    warehouse_id  = http_path.split("/")[-1]

    if not warehouse_id:
        logger.warning("DATABRICKS_HTTP_PATH not set — skipping warehouse wake")
        return

    headers = {"Authorization": f"Bearer {token}"}

    logger.info(f"Starting SQL Warehouse: {warehouse_id}")
    requests.post(
        f"{host}/api/2.0/sql/warehouses/{warehouse_id}/start",
        headers=headers,
    )

    for attempt in range(30):
        resp  = requests.get(
            f"{host}/api/2.0/sql/warehouses/{warehouse_id}",
            headers=headers,
        )
        state = resp.json().get("state", "UNKNOWN")
        logger.info(f"Warehouse state: {state} (attempt {attempt + 1}/30)")
        if state == "RUNNING":
            logger.info("Warehouse is RUNNING ✓")
            return
        time.sleep(10)

    raise Exception("SQL Warehouse did not start within 5 minutes")


@task(
    name="run-dbt",
    description="dbt run: materialise staging -> intermediate -> Gold mart tables",
    retries=2,
    retry_delay_seconds=30,
    tags=["dbt", "gold", "transformation"],
)
def run_dbt(dbt_project_dir: str = "./dbt_project") -> None:
    logger = get_run_logger()
    logger.info(f"Running dbt models from: {dbt_project_dir}")

    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        capture_output=True,
        text=True,
        cwd=dbt_project_dir,
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(
            f"dbt run failed with exit code {result.returncode}.\n"
            f"Check the task logs above for the failing model.\n"
            f"STDERR: {result.stderr}"
        )

    logger.info("dbt run complete ✓")


@task(
    name="test-dbt",
    description="dbt test: schema.yml + custom SQL assertions on Gold layer",
    retries=1,
    retry_delay_seconds=30,
    tags=["dbt", "testing", "data-quality", "gold"],
)
def test_dbt(dbt_project_dir: str = "./dbt_project") -> None:
    logger = get_run_logger()
    logger.info("Running dbt tests...")

    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        capture_output=True,
        text=True,
        cwd=dbt_project_dir,
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception(
            f"dbt tests FAILED — Gold layer has data quality issues.\n"
            f"export_gold_csv will NOT run until all tests pass.\n"
            f"Fix the root cause and re-trigger the flow manually.\n"
            f"STDERR: {result.stderr}"
        )

    logger.info("All dbt tests passed ✓ — Gold layer is clean")


@task(
    name="export-gold-csv",
    description="Export Gold Delta mart tables -> CSV files for Streamlit dashboard",
    retries=2,
    retry_delay_seconds=30,
    tags=["export", "gold", "dashboard"],
)
def export_gold_csv() -> dict:
    logger = get_run_logger()
    logger.info("Exporting Gold Delta tables -> CSV for Streamlit dashboard")

    from src.trade_analytics.data_transformations.export_gold_csv import export

    result = export()

    logger.info(
        f"CSV export complete — {len(result['exported_tables'])} tables -> {result['output_dir']} ✓"
    )
    return result


@flow(
    name="Trade Analytics Pipeline",
    description=(
        "End-to-end Lakehouse pipeline: "
        "generate -> Bronze Delta -> Silver Delta -> dbt Gold -> CSV export. "
        "Runs daily at 05:00 UTC (06:00 CET)."
    ),
    log_prints=True,
)
def trade_pipeline(
    n_trades: int = 50_000,
    trades_path: str = "./data/trades.jsonl",
    dbt_project_dir: str = "./dbt_project",
) -> None:
    logger = get_run_logger()

    logger.info("=" * 54)
    logger.info("  Trade Analytics Pipeline — starting")
    logger.info(f"  Trades      : {n_trades:,}")
    logger.info(f"  Trades path : {trades_path}")
    logger.info(f"  dbt project : {dbt_project_dir}")
    logger.info("=" * 54)

    trades_file = generate_trades(
        n=n_trades,
        output_path=trades_path,
    )

    bronze_result = ingest_bronze(
        input_path=trades_file,
        wait_for=[trades_file],
    )

    silver_result = transform_silver(
        wait_for=[bronze_result],
    )
    
    warehouse_result = wake_warehouse(
        wait_for=[silver_result]
    )

    dbt_run_result = run_dbt(
        dbt_project_dir=dbt_project_dir,
        wait_for=[warehouse_result],
    )

    dbt_test_result = test_dbt(
        dbt_project_dir=dbt_project_dir,
        wait_for=[dbt_run_result],
    )

    export_result = export_gold_csv(
        wait_for=[dbt_test_result],
    )

    logger.info(
        f"Exported {len(export_result['exported_tables'])} tables "
        f"-> {export_result['output_dir']}: "
        f"{', '.join(export_result['exported_tables'])}"
    )

    logger.info("=" * 54)
    logger.info("  Trade Analytics Pipeline — complete")
    logger.info("=" * 54)


if __name__ == "__main__":
    trade_pipeline()

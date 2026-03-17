from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta import configure_spark_with_delta_pip
from pydantic import ValidationError
import json
from databricks.ingestion.schema import TradeEvent
from config.settings import BRONZE_PATH, QUARANTINE_PATH
from config.spark_session import get_spark

spark = get_spark("TradeAnalyticsLakehouse")


def ingest_bronze(input_path: str = "./data/trades.jsonl"):
    valid_records   = []
    invalid_records = []

    with open(input_path) as f:
        for line in f:
            raw = json.loads(line)
            try:
                TradeEvent(**raw)
                valid_records.append(raw)
            except ValidationError as e:
                raw["_error"] = str(e)
                invalid_records.append(raw)

    print(f"Valid: {len(valid_records)} | Quarantined: {len(invalid_records)}")

    if valid_records:
        df_valid = spark.createDataFrame(valid_records)
        df_valid = df_valid \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit("trades_jsonl"))

        df_valid.write.format("delta") \
            .mode("append") \
            .save(BRONZE_PATH)

    if invalid_records:
        df_invalid = spark.createDataFrame(invalid_records)
        df_invalid = df_invalid \
            .withColumn("_quarantined_at", current_timestamp())

        df_invalid.write.format("delta") \
            .mode("append") \
            .save(QUARANTINE_PATH)

    print(f"Bronze write complete → {BRONZE_PATH}")

if __name__ == "__main__":
    ingest_bronze()
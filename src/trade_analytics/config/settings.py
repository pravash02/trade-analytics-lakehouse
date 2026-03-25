import os

from trade_analytics.config.enums import SPARK_ENV, SparkEnv

if SPARK_ENV == SparkEnv.DATABRICKS:
    # abfss location
    # from config.utils import abfss
    # BRONZE_PATH     = abfss(StorageLayer.BRONZE,     "trade-analytics/bronze/raw_trades")
    # SILVER_PATH     = abfss(StorageLayer.SILVER,     "trade-analytics/silver/trades_enriched")
    # GOLD_PATH       = abfss(StorageLayer.GOLD,       "trade-analytics/gold")
    # QUARANTINE_PATH = abfss(StorageLayer.QUARANTINE, "trade-analytics/bronze/quarantine")

    # dbfs location
    BRONZE_PATH = "/Volumes/workspace/default/trade-analytics/bronze/raw_trades"
    SILVER_PATH = "/Volumes/workspace/default/trade-analytics/silver/trades_enriched"
    GOLD_PATH = "/Volumes/workspace/default/trade-analytics/gold"
    QUARANTINE_PATH = "/Volumes/workspace/default/trade-analytics/bronze/quarantine"
else:
    _BASE = "./delta"
    BRONZE_PATH = f"{_BASE}/bronze/raw_trades"
    SILVER_PATH = f"{_BASE}/silver/trades_enriched"
    GOLD_PATH = f"{_BASE}/gold"
    QUARANTINE_PATH = f"{_BASE}/bronze/quarantine"


DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")


LARGE_TRADE_THRESHOLD_EUR = 1_000_000
VELOCITY_WINDOW_MINUTES = 60
VELOCITY_MAX_TRADES = 10


FX_RATES_TO_EUR = {
    "EURUSD": 1.0,
    "GBPUSD": 1.17,
    "USDJPY": 0.0063,
    "USDCHF": 1.08,
}
INSTRUMENTS = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "AAPL",
    "MSFT",
    "BRENT",
    "GOLD",
    "BMW.DE",
    "SAP.DE",
]
DESKS = ["FX", "Equities", "Commodities", "Fixed Income"]
REGIONS = ["EMEA", "APAC", "AMER"]

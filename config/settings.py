import os

# Paths (local dev — mirrors DBFS structure)
BASE_PATH = "./delta"
BRONZE_PATH = f"{BASE_PATH}/bronze/raw_trades"
SILVER_PATH = f"{BASE_PATH}/silver/trades_enriched"
GOLD_PATH   = f"{BASE_PATH}/gold"
QUARANTINE_PATH = f"{BASE_PATH}/bronze/quarantine"

# Databricks (loaded from env vars — never hardcode)
DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")

# Business thresholds
LARGE_TRADE_THRESHOLD_EUR = 1_000_000
VELOCITY_WINDOW_MINUTES   = 60
VELOCITY_MAX_TRADES       = 10

# Instruments and FX rates (mock — realistic enough)
FX_RATES_TO_EUR = {
    "EURUSD": 1.0,
    "GBPUSD": 1.17,
    "USDJPY": 0.0063,
    "USDCHF": 1.08,
}
INSTRUMENTS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AAPL", "MSFT",
               "BRENT", "GOLD", "BMW.DE", "SAP.DE"]
DESKS       = ["FX", "Equities", "Commodities", "Fixed Income"]
REGIONS     = ["EMEA", "APAC", "AMER"]
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from src.trade_analytics.config.settings import INSTRUMENTS, REGIONS

fake = Faker()
random.seed(42)

COUNTERPARTIES = [fake.company() for _ in range(25)]
TRADERS = [f"TDR_{str(i).zfill(3)}" for i in range(1, 51)]

def generate_trade(timestamp: datetime) -> dict:
    instrument = random.choice(INSTRUMENTS)
    desk = "FX" if instrument in ["EURUSD","GBPUSD","USDJPY","USDCHF"] \
           else "Equities" if instrument in ["AAPL","MSFT","BMW.DE","SAP.DE"] \
           else "Commodities"

    notional = round(random.lognormvariate(mu=11, sigma=2), 2)
    notional = max(10_000.00, min(notional, 50_000_000.00))

    is_anomaly = random.random() < 0.02
    if is_anomaly:
        notional = round(notional * random.uniform(50, 200), 2)

    return {
        "trade_id":       str(uuid.uuid4()),
        "trader_id":      random.choice(TRADERS),
        "instrument":     instrument,
        "direction":      random.choice(["BUY", "SELL"]),
        "notional":       float(notional),
        "price":          round(random.uniform(1.0, 500.0), 4),
        "desk":           desk,
        "region":         random.choice(REGIONS),
        "counterparty":   random.choice(COUNTERPARTIES),
        "status":         random.choices(
                              ["EXECUTED", "CANCELLED", "PENDING"],
                              weights=[95, 3, 2])[0],
        "trade_timestamp": timestamp.isoformat(),
        "is_anomaly":     is_anomaly,
    }

def generate_dataset(n: int = 50_000, output_path: str = "./data/trades.jsonl"):

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    start = datetime.now() - timedelta(days=30)
    with open(output_path, "w") as f:
        for i in range(n):
            ts = start + timedelta(seconds=i * 52)
            trade = generate_trade(ts)
            f.write(json.dumps(trade) + "\n")
    print(f"Generated {n} trades -> {output_path}")


if __name__ == "__main__":
    generate_dataset()
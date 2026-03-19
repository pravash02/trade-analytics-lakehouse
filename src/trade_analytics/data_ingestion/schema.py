from datetime import datetime
from typing import Literal

from pydantic import BaseModel, field_validator


class TradeEvent(BaseModel):
    trade_id:        str
    trader_id:       str
    instrument:      str
    direction:       Literal["BUY", "SELL"]
    notional:        float
    price:           float
    desk:            str
    region:          str
    counterparty:    str
    status:          Literal["EXECUTED", "CANCELLED", "PENDING"]
    trade_timestamp: datetime
    is_anomaly:      bool

    @field_validator("notional")
    def notional_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("notional must be positive")
        return v

    @field_validator("trader_id")
    def trader_id_format(cls, v):
        if not v.startswith("TDR_"):
            raise ValueError("trader_id must start with TDR_")
        return v
import os
from enum import Enum
from dataclasses import dataclass


class SparkEnv(str, Enum):
    LOCAL      = "local"
    DATABRICKS = "databricks"


SPARK_ENV = SparkEnv(os.getenv("SPARK_ENV", SparkEnv.LOCAL.value))


class StorageLayer(str, Enum):
    BRONZE     = "bronze"
    SILVER     = "silver"
    GOLD       = "gold"
    QUARANTINE = "quarantine"


@dataclass(frozen=True)
class AdlsLayerConfig:
    account:   str
    container: str
    sas_token: str


ADLS: dict[StorageLayer, AdlsLayerConfig] = {
    StorageLayer.BRONZE: AdlsLayerConfig(
        account   = os.getenv("ADLS_ACCOUNT_BRONZE",       "tradeanalyticslakehouse"),
        container = os.getenv("ADLS_CONTAINER_BRONZE",     "raw"),
        sas_token = os.getenv("ADLS_SAS_TOKEN_BRONZE",     ""),
    ),
    StorageLayer.SILVER: AdlsLayerConfig(
        account   = os.getenv("ADLS_ACCOUNT_SILVER",       "tradeanalyticslakehouse"),
        container = os.getenv("ADLS_CONTAINER_SILVER",     "processed"),
        sas_token = os.getenv("ADLS_SAS_TOKEN_SILVER",     ""),
    ),
    StorageLayer.GOLD: AdlsLayerConfig(
        account   = os.getenv("ADLS_ACCOUNT_GOLD",         "tradeanalyticslakehouse"),
        container = os.getenv("ADLS_CONTAINER_GOLD",       "serving"),
        sas_token = os.getenv("ADLS_SAS_TOKEN_GOLD",       ""),
    ),
    StorageLayer.QUARANTINE: AdlsLayerConfig(
        account   = os.getenv("ADLS_ACCOUNT_QUARANTINE",   "tradeanalyticslakehouse"),
        container = os.getenv("ADLS_CONTAINER_QUARANTINE", "quarantine"),
        sas_token = os.getenv("ADLS_SAS_TOKEN_QUARANTINE", ""),
    ),
}
from config.enums import ADLS, SPARK_ENV, SparkEnv, StorageLayer


def abfss(layer: StorageLayer, subpath: str) -> str:
    cfg = ADLS[layer]
    return (
        f"abfss://{cfg.container}"
        f"@{cfg.account}.dfs.core.windows.net"
        f"/{subpath.lstrip('/')}"
    )


def configure_adls_auth(spark) -> None:
    if SPARK_ENV != SparkEnv.DATABRICKS:
        return

    registered: set[str] = set()

    for layer, cfg in ADLS.items():
        key = f"{cfg.account}.{cfg.container}"
        if key in registered:
            continue

        if not cfg.sas_token:
            print(f"[ADLS] WARNING: no SAS token for layer '{layer.value}' "
                  f"({cfg.container}@{cfg.account}) — reads/writes will fail")
            continue

        spark.conf.set(
            f"fs.azure.sas.{cfg.container}.{cfg.account}.dfs.core.windows.net",
            cfg.sas_token
        )
        registered.add(key)
        print(f"[ADLS] Configured auth → {cfg.container}@{cfg.account}")
import json
import logging
import os
import pathlib
import requests
import yaml

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

_ROOT = pathlib.Path(__file__).parent.parent.parent.resolve()


def _load_yaml(path: str) -> dict:
    with open(path, "r") as fh:
        return yaml.safe_load(fh)


def _get_settings(env: str) -> dict:
    path = _ROOT / "deploy" / "targets" / env / "settings.yml"
    if not path.exists():
        raise FileNotFoundError(f"settings.yml not found for env '{env}': {path}")

    raw = _load_yaml(str(path))
    try:
        return raw["targets"]["TARGET"]
    except KeyError as e:
        raise KeyError(
            f"settings.yml for '{env}' is missing key: {e}\n"
            f"Expected structure: targets > TARGET > workspace / variables"
        )


def get_host(env: str) -> str:
    settings = _get_settings(env)
    host = settings.get("workspace", {}).get("host", "").strip().rstrip("/")
    if not host:
        raise ValueError(f"[{env}] workspace.host is empty in settings.yml")
    log.info(f"[{env}] Host: {host}")
    return host


def get_token(env: str) -> str:
    settings  = _get_settings(env)
    variables = settings.get("variables", {})
    strategy  = variables.get("auth_strategy", "pat").strip().lower()

    log.info(f"[{env}] Auth strategy: {strategy}")

    if strategy == "pat":
        return _get_pat(env, variables)
    elif strategy == "spn":
        return _get_spn_token(env, variables)
    else:
        raise ValueError(
            f"[{env}] Unknown auth_strategy '{strategy}' in settings.yml. "
            f"Valid values: 'pat' | 'spn'"
        )


def get_http_path(env: str) -> str:
    settings  = _get_settings(env)
    variables = settings.get("variables", {})
    http_path = str(variables.get("http_path", "")).strip()
    if not http_path:
        log.warning(f"[{env}] http_path is empty in settings.yml")
    log.info(f"[{env}] HTTP path: {http_path}")
    return http_path


def _get_pat(env: str, variables: dict) -> str:
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if token:
        log.info(f"[{env}] PAT resolved from DATABRICKS_TOKEN env var")
        return token

    token = str(variables.get("pat_token", "")).strip()
    if token:
        log.info(f"[{env}] PAT resolved from settings.yml (pat_token)")
        return token

    raise EnvironmentError(
        f"\n[{env}] No PAT token found. Do one of:\n"
        f"  1. Set GitHub Secret DATABRICKS_TOKEN_DEV and export as DATABRICKS_TOKEN in CI\n"
        f"  2. Set pat_token in deploy/targets/DEV/settings.yml (dev only — never commit)\n"
    )


def _get_spn_token(env: str, variables: dict) -> str:
    tenant_id = os.environ.get("AZ_TENANT_ID", "").strip()
    if not tenant_id:
        raise EnvironmentError(
            f"[{env}] AZ_TENANT_ID env var is not set. "
            f"Set it as a GitHub Secret and export it in your workflow."
        )

    # Allow env vars to override Vault lookup for CI convenience
    client_id     = os.environ.get("AZ_CLIENT_ID", "").strip()
    client_secret = os.environ.get("AZ_CLIENT_SECRET", "").strip()

    if not (client_id and client_secret):
        log.info(f"[{env}] AZ_CLIENT_ID/SECRET not in env — fetching from Vault")
        creds         = _get_spn_creds_from_vault(env, variables)
        client_id     = creds["client_id"]
        client_secret = creds["client_secret"]

    return _get_aad_token(env, tenant_id, client_id, client_secret)


def _get_spn_creds_from_vault(env: str, variables: dict) -> dict:
    vault_url     = variables.get("vault_url", "").rstrip("/")
    namespace     = variables.get("namespace", "")
    secret_path   = variables.get("secret_path", "")
    cert_role     = variables.get("cert_role", "")
    cert_path     = variables.get("cert_path", "")
    cert_key_path = variables.get("cert_key_path", "")

    if not all([vault_url, namespace, secret_path, cert_role, cert_path, cert_key_path]):
        raise ValueError(
            f"[{env}] Vault config incomplete in settings.yml. "
            f"Required: vault_url, namespace, secret_path, cert_role, cert_path, cert_key_path"
        )

    cert = (cert_path, cert_key_path)

    # Authenticate to Vault via mTLS cert → get client token
    login_url = f"{vault_url}/v1/auth/cert/login"
    headers   = {"X-Vault-Namespace": namespace}
    data      = {"name": cert_role}

    try:
        resp = requests.post(
            login_url,
            headers=headers,
            data=json.dumps(data),
            cert=cert,
            timeout=30,
        )
        resp.raise_for_status()
        vault_token = resp.json().get("auth", {}).get("client_token")
        if not vault_token:
            raise RuntimeError(f"[{env}] Vault login succeeded but no client_token in response")
        log.info(f"[{env}] Vault authentication successful")

    except requests.RequestException as e:
        raise RuntimeError(f"[{env}] Vault login failed: {e}")

    # Read SPN credentials from Vault KV
    secret_url = f"{vault_url}/v1/secret/data/{secret_path.lstrip('/')}"
    headers    = {"X-Vault-Token": vault_token, "X-Vault-Namespace": namespace}

    try:
        resp = requests.get(secret_url, headers=headers, cert=cert, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("data", {})
        client_id     = data.get("client-id", "")
        client_secret = data.get("secret", "")

        if not (client_id and client_secret):
            raise RuntimeError(
                f"[{env}] Vault secret at '{secret_path}' is missing 'client-id' or 'secret' key"
            )
        log.info(f"[{env}] SPN credentials retrieved from Vault")
        return {"client_id": client_id, "client_secret": client_secret}

    except requests.RequestException as e:
        raise RuntimeError(f"[{env}] Vault secret read failed: {e}")


def _get_aad_token(env: str, tenant_id: str, client_id: str, client_secret: str) -> str:
    resource_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
        "resource":      resource_id,
    }

    try:
        resp = requests.post(url, data=payload, timeout=30)
        resp.raise_for_status()
        token = resp.json().get("access_token", "")
        if not token:
            raise RuntimeError(f"[{env}] AAD token response missing access_token")
        log.info(f"[{env}] AAD token acquired successfully")
        return token

    except requests.RequestException as e:
        raise RuntimeError(f"[{env}] AAD token request failed: {e}")

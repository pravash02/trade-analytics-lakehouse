import pathlib, os, yaml, logging, requests, json
import token
import urllib.parse as urljoin


logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)
_ROOT = pathlib.Path(__file__).parent.resolve()


def get_config(path: str) -> dict:
    """
    Loads a YAML configuration file and returns its contents as a dictionary.
    """
    with open(path, "r") as file:
        config = yaml.safe_load(file)
        return config


def get_host(env: str) -> str:
    """
    Determines the Databricks host to connect to based on the environment.
    """
    config = get_config(f"{_ROOT}/deploy/targets/{env}/settings.yaml")
    host = config["targets"][env]["workspace"]["host"]

    print(f"Determined host for environment '{env}': {host}")
    return host


def get_spn_creds(vault_url: str, namespace: str, secret_path: str, cert_role: str, cert_path: str, cert_key_path: str) -> dict:
    """
    Retrieves service principal credentials from EVA vault.
    """
    if not ca_certs:
        ca_certs = ""

    url = f"{vault_url}/api/v1/secrets/{namespace}/{secret_path}"
    cert = (cert_path, cert_key_path)

    try:
        url = urljoin(vault_url, "v1/auth/cert/login")
        headers = {"X-Vault-Namespace": namespace}
        data = {"name": cert_role}
        response = requests.post(url, headers=headers, data=json.dumps(data), cert=(cert_path, cert_key_path), verify=ca_certs)
        token = response.json().get("auth", {}).get("client_token")

        url = urljoin(vault_url, os.path.join(f"v1/secrets/data", secret_path))
        headers = {"X-Vault-Token": token, "X-Vault-Namespace": namespace}
        response = requests.get(url, headers=headers, verify=ca_certs)
        data = response.json().get("data", {}).get("data", {})

        return {
            "client_id": data.get("client-id"),
            "client_secret": data.get("secret")
        }


    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to retrieve SPN credentials: {e}")
        raise RuntimeError("SPN credentials retrieval failed.")


def get_aad_token(tenant_id: str, client_id: str, client_secret: str, resource_id: str) -> str:
    """
    Retrieves an Azure AD access token for the given service principal credentials and resource ID.
    """
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": resource_id
    }

    response = requests.post(url, data=payload)
    if response.status_code == 200:
        token = response.json().get("access_token")
        return token
    else:
        logging.error(f"Failed to retrieve AAD token: {response.status_code} {response.text}")
        raise RuntimeError("AAD token retrieval failed.")


def get_access_token(variables: dict, resource_id: str) -> str:
    """
    Retrieves the Databricks access token for a service principle using credentials stroed in EVA.
    """
    tenant_id = os.getenv("AZ_TENANT_ID")
    client_id = os.getenv("AZ_CLIENT_ID")
    client_secret = os.getenv("AZ_CLIENT_SECRET")

    if not tenant_id:
        tenant_id = ""
    
    if not client_secret:
        spn_creds = (
            get_spn_creds(
                variables["vault_url"],
                variables["namespace"],
                variables["secret_path"],
                variables["cert_role"],
                variables["cert_path"],
                variables["cert_key_path"]
            )
        )
    
    if spn_creds:
        client_id = spn_creds["client_id"]
        client_secret = spn_creds["client_secret"]
    
    if not client_id:
        logging.error("Missing deploy SPN client ID or client secret. Check environment variables and/or SPN credentials retrieval.")

    return get_aad_token(tenant_id, client_id, client_secret, resource_id)


def get_databricks_access_token(variables: dict) -> str:
    """
    Retrieves the Databricks access token for a service principle using credentials stroed in EVA.
    """
    resource_id = "" 
    return get_access_token(variables, resource_id)


def get_token(env: str) -> str:
    """
    Determines the Databricks token to connect to based on the environment.
    """
    config = get_config(f"{_ROOT}/deploy/targets/{env}/settings.yaml")
    variables = config["targets"][env]["variables"]

    strategy  = variables.get("auth_strategy", "pat").lower()
    if strategy == "pat":
        token = _get_pat(env, variables)
    else:
        raise ValueError(
            f"[{env}] Unknown auth_strategy '{strategy}' in settings.yaml.\n"
            f"Valid values: 'pat'  |  'spn' (uncomment spn block in config.py first)"
        )

    print(token)
    return token


def get_http_path(env: str) -> str:
    """
    Returns the SQL Warehouse HTTP path for the given environment.
    """
    config = get_config(f"{_ROOT}/deploy/targets/{env}/settings.yaml")
    variables = config["targets"][env]["variables"]
    http_path = str(variables.get("http_path", "")).strip()

    log.info(f"[{env}] HTTP Path: {http_path}")
    return http_path


def _get_pat(env: str, variables: dict) -> str:
    """
    Returns a Personal Access Token.
    """
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if token:
        log.info(f"[{env}] PAT from DATABRICKS_TOKEN env var")
        return token

    token = str(variables.get("pat_token", "")).strip()
    if token:
        log.info(f"[{env}] PAT from settings.yaml")
        return token

    raise EnvironmentError(
        f"\n[{env}] No PAT token found. Do one of:\n"
    )
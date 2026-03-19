set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"

ENV="${ENV:-}"
if [[ -z "$ENV" ]]; then
    echo "[ERROR] ENV is not set. Usage: ENV=DEV ./deploy.sh"
    exit 1
fi
echo "[INFO] Deploying to environment: $ENV"


init() {
    echo "[INFO] Downloading Databricks CLI..."
    DBX_CLI_VER="0.217.1"
    DBX_CLI_ZIP="databricks-cli-${DBX_CLI_VER}_linux_amd64.zip"
    DBX_CLI_URL="https://github.com/databricks/databricks-cli/releases/download/v${DBX_CLI_VER}/${DBX_CLI_ZIP}"

    rm -rf "${PROJECT_ROOT}/tmp"
    mkdir -p "${PROJECT_ROOT}/tmp"
    cd "${PROJECT_ROOT}/tmp"

    curl -fLOs "$DBX_CLI_URL"
    python3 -c "from zipfile import PyZipFile; PyZipFile('${DBX_CLI_ZIP}').extractall()"

    cp databricks "${PROJECT_ROOT}/databricks"
    cd "${PROJECT_ROOT}"
    rm -rf tmp

    chmod +x databricks
    ./databricks --version
    echo "[INFO] Databricks CLI ready ✓"
}


copy_env_settings() {
    local env_settings="${PROJECT_ROOT}/deploy/targets/${ENV}/settings.yml"
    local target_settings="${PROJECT_ROOT}/deploy/targets/TARGET/settings.yml"

    if [[ ! -f "$env_settings" ]]; then
        echo "[ERROR] settings.yml not found: $env_settings"
        exit 1
    fi

    cp "$env_settings" "$target_settings"
    echo "[INFO] Copied ${ENV}/settings.yml -> TARGET/settings.yml"
}


patch_terraform_path() {
    TERRAFORM_PATH="${TERRAFORM_PATH:-$(which terraform 2>/dev/null || echo '')}"
    if [[ -n "$TERRAFORM_PATH" ]]; then
        sed -i "s|exec_path:.*|exec_path: ${TERRAFORM_PATH}|" "${PROJECT_ROOT}/databricks.yml"
        echo "[INFO] Terraform exec_path set: $TERRAFORM_PATH"
    else
        echo "[WARN] Terraform not found — exec_path left blank (OK if using serverless)"
    fi
}


copy_config() {
    # cp "${SCRIPT_DIR}/config.py" "${PROJECT_ROOT}/config.py"
    echo "[INFO] config.py copied to project root"
}


install_deps() {
    echo "[INFO] Installing deploy dependencies..."
    python3 -m pip install --quiet --user -r "${SCRIPT_DIR}/requirements.txt"
    echo "[INFO] Dependencies installed ✓"
}


resolve_auth() {
    cd "${PROJECT_ROOT}"
    echo "[INFO] Resolving Databricks host..."

    export DATABRICKS_HOST
    DATABRICKS_HOST=$(python3 -c "
import sys
sys.path.insert(0, '${SCRIPT_DIR}')
import config
print(config.get_host('TARGET'))
")
    echo "[INFO] DATABRICKS_HOST = $DATABRICKS_HOST"

    echo "[INFO] Resolving Databricks token..."
    export DATABRICKS_TOKEN
    DATABRICKS_TOKEN=$(python3 -c "
import sys
sys.path.insert(0, '${SCRIPT_DIR}')
import config
print(config.get_token('TARGET'))
")
    echo "[INFO] DATABRICKS_TOKEN = *** (set)"
}


build_wheel() {
    echo "[INFO] Building Python wheel..."
    cd "${PROJECT_ROOT}"

    python3 -m pip install --quiet build
    python3 -m build --wheel --outdir dist/

    WHEEL_FILE=$(ls dist/*.whl | head -1)
    if [[ -z "$WHEEL_FILE" ]]; then
        echo "[ERROR] No wheel file found in dist/ after build"
        exit 1
    fi
    echo "[INFO] Wheel built: $WHEEL_FILE"
    export WHEEL_FILE
}


upload_wheel() {
    echo "[INFO] Uploading wheel to Databricks Volume..."
    cd "${PROJECT_ROOT}"

    WHEEL_VOLUME_PATH=$(python3 -c "
import sys
sys.path.insert(0, '${SCRIPT_DIR}')
import config, yaml
s = yaml.safe_load(open('deploy/targets/TARGET/settings.yml'))
print(s['targets']['TARGET']['variables'].get('wheel_path', ''))
")

    if [[ -z "$WHEEL_VOLUME_PATH" ]]; then
        echo "[WARN] wheel_path not set — skipping upload"
        return
    fi

    WHEEL_DIR=$(dirname "${WHEEL_VOLUME_PATH}")
    WHEEL_BASENAME=$(basename "${WHEEL_FILE}")
    WHEEL_VERSIONED_PATH="${WHEEL_DIR}/${WHEEL_BASENAME}"

    echo "[INFO] Wheel volume path : ${WHEEL_VOLUME_PATH}"
    echo "[INFO] Wheel versioned   : ${WHEEL_VERSIONED_PATH}"

    # Use Databricks REST API to upload to Unity Catalog Volume
    # databricks fs cp does not support /Volumes/ paths — use the Files API instead
    python3 -c "
import sys, requests, os
sys.path.insert(0, '${SCRIPT_DIR}')

host  = os.environ['DATABRICKS_HOST'].rstrip('/')
token = os.environ['DATABRICKS_TOKEN']

headers = {'Authorization': f'Bearer {token}'}

def upload(local_path, volume_path):
    url = f'{host}/api/2.0/fs/files{volume_path}'
    with open(local_path, 'rb') as f:
        resp = requests.put(url, headers=headers, data=f)
    if resp.status_code not in (200, 204):
        raise RuntimeError(f'Upload failed: {resp.status_code} {resp.text}')
    print(f'[INFO] Uploaded → {volume_path} ✓')

# Upload as latest
upload('${WHEEL_FILE}', '${WHEEL_VOLUME_PATH}')

# Upload as versioned
upload('${WHEEL_FILE}', '${WHEEL_VERSIONED_PATH}')
"
}


# deploy_bundle() {
#     echo "[INFO] Deploying Databricks Asset Bundle..."
#     cd "${PROJECT_ROOT}"

#     databricks bundle deploy -t TARGET

#     echo ""
#     echo "======================================================"
#     echo "  Deployment complete ✓"
#     echo "  Environment : $ENV"
#     echo "  Host        : $DATABRICKS_HOST"
#     echo "  Wheel       : $WHEEL_FILE"
#     echo "======================================================"
# }
deploy_bundle() {
    echo "[INFO] Deploying Databricks Asset Bundle..."
    cd "${PROJECT_ROOT}"

    databricks bundle deploy -t TARGET 2>&1
    EXIT_CODE=$?

    if [[ $EXIT_CODE -ne 0 ]]; then
        echo "[WARN] bundle deploy failed (possibly CE SCIM restriction)"
        echo "[INFO] Falling back to direct Jobs API deploy..."
        deploy_via_jobs_api
    fi
}


deploy_via_jobs_api() {
    echo "[INFO] Deploying job via REST API (Serverless)..."
    python3 -c "
import sys, requests, json, os
sys.path.insert(0, '${SCRIPT_DIR}')

host  = os.environ['DATABRICKS_HOST'].rstrip('/')
token = os.environ['DATABRICKS_TOKEN']
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type':  'application/json',
}

wheel_path = '/Volumes/workspace/default/trade-analytics/wheels/trade_analytics-latest.whl'
nb_base    = '/Workspace/Shared/trade-analytics-lakehouse/databricks_notebooks'

job_payload = {
    'name': 'trade-analytics-pipeline-DEV',

    'environments': [
        {
            'environment_key': 'trade_analytics_env',
            'spec': {
                'client': '1',
                'dependencies': [
                    wheel_path,
                    'dbt-databricks',
                    'pydantic>=2.0.0',
                    'loguru>=0.7.0',
                ],
            },
        }
    ],

    'tasks': [
        {
            'task_key': 'bronze_ingest',
            'description': 'Ingest trades.jsonl → Bronze Delta',
            'environment_key': 'trade_analytics_env',
            'notebook_task': {
                'notebook_path': f'{nb_base}/01_bronze_ingest',
                'source': 'WORKSPACE',
            },
            'timeout_seconds': 1800,
            'max_retries': 1,
        },
        {
            'task_key': 'silver_transform',
            'description': 'PySpark Bronze → Silver Delta',
            'depends_on': [{'task_key': 'bronze_ingest'}],
            'environment_key': 'trade_analytics_env',
            'notebook_task': {
                'notebook_path': f'{nb_base}/02_silver_transform',
                'source': 'WORKSPACE',
            },
            'timeout_seconds': 3600,
            'max_retries': 1,
        },
        {
            'task_key': 'dbt_gold',
            'description': 'dbt run + test → Gold Delta marts',
            'depends_on': [{'task_key': 'silver_transform'}],
            'environment_key': 'trade_analytics_env',
            'notebook_task': {
                'notebook_path': f'{nb_base}/03_run_dbt',
                'source': 'WORKSPACE',
            },
            'timeout_seconds': 3600,
            'max_retries': 0,
        },
    ],
}

# Upsert — update if exists, create if not
resp  = requests.get(f'{host}/api/2.1/jobs/list', headers=headers)
jobs  = resp.json().get('jobs', [])
match = [j for j in jobs if j['settings']['name'] == job_payload['name']]

if match:
    job_id = match[0]['job_id']
    resp   = requests.post(
        f'{host}/api/2.1/jobs/reset',
        headers=headers,
        json={'job_id': job_id, 'new_settings': job_payload},
    )
    print(f'[INFO] Job updated  (id={job_id}) → {resp.status_code}')
else:
    resp = requests.post(
        f'{host}/api/2.1/jobs/create',
        headers=headers,
        json=job_payload,
    )
    data = resp.json()
    print(f'[INFO] Job created  (id={data.get(\"job_id\")}) → {resp.status_code}')

if resp.status_code not in (200, 204):
    raise RuntimeError(f'Job deploy failed: {resp.status_code} {resp.text}')

print('[INFO] Job deploy complete ✓')
"
}

# deploy_via_jobs_api() {
#     echo "[INFO] Deploying job via REST API..."
#     python3 -c "
# import sys, requests, json, os, yaml
# sys.path.insert(0, '${SCRIPT_DIR}')

# host  = os.environ['DATABRICKS_HOST'].rstrip('/')
# token = os.environ['DATABRICKS_TOKEN']
# headers = {
#     'Authorization': f'Bearer {token}',
#     'Content-Type':  'application/json',
# }

# # Load job definition from pipeline.yml
# # For CE, create a minimal job directly via API
# job_payload = {
#     'name': 'trade-analytics-pipeline-TARGET',
#     'tasks': [
#         {
#             'task_key': 'bronze_ingest',
#             'notebook_task': {
#                 'notebook_path': '/Workspace/Shared/trade-analytics-lakehouse/databricks_notebooks/01_bronze_ingest',
#             },
#             'existing_cluster_id': '${CLUSTER_ID}',
#             'libraries': [{'whl': '/Volumes/workspace/default/trade-analytics/wheels/trade_analytics-latest.whl'}],
#         },
#         {
#             'task_key': 'silver_transform',
#             'depends_on': [{'task_key': 'bronze_ingest'}],
#             'notebook_task': {
#                 'notebook_path': '/Workspace/Shared/trade-analytics-lakehouse/databricks_notebooks/02_silver_transform',
#             },
#             'existing_cluster_id': '${CLUSTER_ID}',
#             'libraries': [{'whl': '/Volumes/workspace/default/trade-analytics/wheels/trade_analytics-latest.whl'}],
#         },
#         {
#             'task_key': 'dbt_gold',
#             'depends_on': [{'task_key': 'silver_transform'}],
#             'notebook_task': {
#                 'notebook_path': '/Workspace/Shared/trade-analytics-lakehouse/databricks_notebooks/03_run_dbt',
#             },
#             'existing_cluster_id': '${CLUSTER_ID}',
#             'libraries': [{'whl': '/Volumes/workspace/default/trade-analytics/wheels/trade_analytics-latest.whl'}],
#         },
#     ],
# }

# # Check if job already exists
# resp = requests.get(f'{host}/api/2.1/jobs/list', headers=headers)
# jobs = resp.json().get('jobs', [])
# existing = [j for j in jobs if j['settings']['name'] == job_payload['name']]

# if existing:
#     job_id = existing[0]['job_id']
#     resp = requests.post(
#         f'{host}/api/2.1/jobs/reset',
#         headers=headers,
#         json={'job_id': job_id, 'new_settings': job_payload},
#     )
#     print(f'Job updated (id={job_id}): {resp.status_code}')
# else:
#     resp = requests.post(
#         f'{host}/api/2.1/jobs/create',
#         headers=headers,
#         json=job_payload,
#     )
#     print(f'Job created: {resp.status_code} {resp.json()}')
# "
# }


main() {
    echo ""
    echo "======================================================"
    echo "  Trade Analytics Lakehouse — Deploy"
    echo "  Target: $ENV"
    echo "======================================================"
    echo ""

    # init
    copy_env_settings
    patch_terraform_path
    copy_config
    install_deps
    resolve_auth
    build_wheel
    upload_wheel
    deploy_bundle
}

main
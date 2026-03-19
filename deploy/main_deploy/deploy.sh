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
    cp "${SCRIPT_DIR}/config.py" "${PROJECT_ROOT}/config.py"
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
    DATABRICKS_HOST=$(python3 -c "import config; print(config.get_host('TARGET'))")
    echo "[INFO] DATABRICKS_HOST = $DATABRICKS_HOST"

    echo "[INFO] Resolving Databricks token..."
    export DATABRICKS_TOKEN
    DATABRICKS_TOKEN=$(python3 -c "import config; print(config.get_token('TARGET'))")
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
import config, yaml
s = yaml.safe_load(open('deploy/targets/TARGET/settings.yml'))
print(s['targets']['TARGET']['variables'].get('wheel_path', ''))
")

    if [[ -z "$WHEEL_VOLUME_PATH" ]]; then
        echo "[WARN] wheel_path not set in settings.yml — skipping upload"
        return
    fi

    # Upload wheel file
    databricks fs cp "${WHEEL_FILE}" "${WHEEL_VOLUME_PATH}" --overwrite
    echo "[INFO] Wheel uploaded -> $WHEEL_VOLUME_PATH"
}


deploy_bundle() {
    echo "[INFO] Deploying Databricks Asset Bundle..."
    cd "${PROJECT_ROOT}"

    databricks bundle deploy -t TARGET

    echo ""
    echo "======================================================"
    echo "  Deployment complete ✓"
    echo "  Environment : $ENV"
    echo "  Host        : $DATABRICKS_HOST"
    echo "  Wheel       : $WHEEL_FILE"
    echo "======================================================"
}


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
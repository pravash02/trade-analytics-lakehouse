set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
export install_dir="$(cd "${SCRIPT_DIR}/../.." && pwd -P)"

echo "[env.sh] install_dir = $install_dir"
echo "[env.sh] ENV         = ${ENV:-NOT SET}"


install_my_app() {
    echo "[env.sh] install_my_app: starting deploy for ENV=${ENV:-}"

    if [[ -z "${ENV:-}" ]]; then
        echo "[ERROR] ENV is not set. Export ENV=DEV|UAT|PROD before calling install_my_app."
        exit 1
    fi

    cd "${install_dir}/deploy/main_deploy"
    chmod -R a+x .
    echo "[env.sh] Working directory: $(pwd)"
    echo "[env.sh] ENV: $ENV"

    ./deploy.sh
}

start() {
    echo "[env.sh] start: no-op (pipeline is event-driven)"
}

stop() {
    echo "[env.sh] stop: no-op"
}

check() {
    echo "[env.sh] check: verifying deploy artifacts..."
    if [[ -f "${install_dir}/databricks" ]]; then
        echo "[env.sh] databricks CLI present"
        "${install_dir}/databricks" --version
    else
        echo "[env.sh] databricks CLI not found — deploy has not run yet"
    fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    install_my_app
fi
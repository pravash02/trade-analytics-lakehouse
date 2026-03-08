#!/usr/bin/env bash
# =============================================================================
# Trade Analytics Lakehouse — Bootstrap Setup (macOS / Linux)
# =============================================================================
# Zero dependencies. Just run:
#
#   chmod +x setup.sh && ./setup.sh
#
# What this does:
#   1. Installs uv (the only tool installed system-wide, via curl)
#   2. Uses uv to download + manage Python 3.10 (no system Python needed)
#   3. Creates .venv pinned to Python 3.10
#   4. Installs all project packages inside .venv
#   5. Configures PYTHONPATH via .pth file
#   6. Checks Java + JAVA_TOOL_OPTIONS for PySpark/Java 17 compatibility
#   7. Runs a SparkSession sanity check
# =============================================================================

set -euo pipefail   # Exit on error, undefined var, or pipe failure

# ─── Colours ─────────────────────────────────────────────────────────────────
RED='\033[0;31m';  GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m';     RESET='\033[0m'

info()    { echo -e "${BLUE}[INFO]${RESET}  $*"; }
success() { echo -e "${GREEN}[OK]${RESET}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*"; }
header()  { echo -e "\n${BOLD}${BLUE}$(printf '─%.0s' {1..55})\n$*\n$(printf '─%.0s' {1..55})${RESET}"; }

# ─── Config ───────────────────────────────────────────────────────────────────
PYTHON_VERSION="3.10"
PROJECT_NAME="trade-analytics-lakehouse"

# ─── OS Detection ─────────────────────────────────────────────────────────────
OS="$(uname -s)"
case "$OS" in
    Darwin) OS_NAME="macOS" ;;
    Linux)  OS_NAME="Linux" ;;
    *)
        error "Unsupported OS: $OS. Use setup.ps1 on Windows."
        exit 1
        ;;
esac

header "${PROJECT_NAME} Setup  |  ${OS_NAME}"


# ─── Step 1: Install uv ───────────────────────────────────────────────────────
header "Step 1/7 — Installing uv"

if command -v uv &>/dev/null; then
    success "uv already installed: $(uv --version)"
else
    info "uv not found — installing via curl (no Python required)..."
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Add uv to PATH for this session
    export PATH="$HOME/.local/bin:$PATH"

    if command -v uv &>/dev/null; then
        success "uv installed: $(uv --version)"
    else
        error "uv installation failed or not on PATH."
        warn  "Try: export PATH=\"\$HOME/.local/bin:\$PATH\"  then re-run."
        exit 1
    fi
fi


# ─── Step 2: Check / Install Python 3.10 via uv ──────────────────────────────
header "Step 2/7 — Ensuring Python ${PYTHON_VERSION}"

# uv can download and manage its own Python runtimes — no system Python needed
if uv python find "${PYTHON_VERSION}" &>/dev/null; then
    PYTHON_PATH="$(uv python find ${PYTHON_VERSION})"
    success "Python ${PYTHON_VERSION} already available: ${PYTHON_PATH}"
else
    info "Python ${PYTHON_VERSION} not found — downloading via uv..."
    info "This is a one-time download (~30MB). Please wait..."
    uv python install "${PYTHON_VERSION}"
    PYTHON_PATH="$(uv python find ${PYTHON_VERSION})"
    success "Python ${PYTHON_VERSION} installed: ${PYTHON_PATH}"
fi

# Verify version
PY_VER="$("${PYTHON_PATH}" --version 2>&1)"
info "Using: ${PY_VER}"


# ─── Step 3: Check Java ───────────────────────────────────────────────────────
header "Step 3/7 — Checking Java (required by PySpark)"

if ! command -v java &>/dev/null; then
    error "Java not found. PySpark requires Java 11 or 17."
    echo ""
    warn  "Install Java with one of these commands:"
    if [ "$OS_NAME" = "macOS" ]; then
        echo "    brew install openjdk@17"
        echo "    Then: export JAVA_HOME=\$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
    else
        echo "    sudo apt install openjdk-17-jdk   # Debian/Ubuntu"
        echo "    sudo dnf install java-17-openjdk  # Fedora/RHEL"
    fi
    echo ""
    warn  "After installing Java, re-run this script."
    exit 1
fi

JAVA_VERSION="$(java -version 2>&1 | head -1)"
success "Java found: ${JAVA_VERSION}"

# Check JAVA_TOOL_OPTIONS for Java 17 module flags
if [[ "${JAVA_TOOL_OPTIONS:-}" != *"--add-opens"* ]]; then
    warn "JAVA_TOOL_OPTIONS not set — required for PySpark + Java 17."
    warn "Adding it to your shell profile automatically...\n"

    JAVA_FLAGS='--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true'

    EXPORT_LINE="export JAVA_TOOL_OPTIONS=\"${JAVA_FLAGS}\""

    # Detect shell profile
    if [ -f "$HOME/.zshrc" ]; then
        PROFILE="$HOME/.zshrc"
    elif [ -f "$HOME/.bash_profile" ]; then
        PROFILE="$HOME/.bash_profile"
    elif [ -f "$HOME/.bashrc" ]; then
        PROFILE="$HOME/.bashrc"
    else
        PROFILE="$HOME/.profile"
    fi

    # Only append if not already there
    if ! grep -q "JAVA_TOOL_OPTIONS" "$PROFILE" 2>/dev/null; then
        echo ""                           >> "$PROFILE"
        echo "# PySpark + Java 17 flags" >> "$PROFILE"
        echo "$EXPORT_LINE"              >> "$PROFILE"
        success "Added JAVA_TOOL_OPTIONS to ${PROFILE}"
    else
        warn "JAVA_TOOL_OPTIONS already in ${PROFILE} — skipping."
    fi

    # Apply for this session immediately
    export JAVA_TOOL_OPTIONS="${JAVA_FLAGS}"
    success "JAVA_TOOL_OPTIONS applied for this session."
else
    success "JAVA_TOOL_OPTIONS already set correctly."
fi


# ─── Step 4: Create .venv ────────────────────────────────────────────────────
header "Step 4/7 — Creating virtual environment"

if [ -d ".venv" ]; then
    warn ".venv already exists — skipping creation."
    warn "To recreate: rm -rf .venv && ./setup.sh"
else
    info "Creating .venv with Python ${PYTHON_VERSION}..."
    uv venv .venv --python "${PYTHON_VERSION}"
    success ".venv created."
fi


# ─── Step 5: Install packages ────────────────────────────────────────────────
header "Step 5/7 — Installing packages"

VENV_PYTHON=".venv/bin/python"

if [ -f "pyproject.toml" ]; then
    info "Installing from pyproject.toml..."
    uv pip install -r pyproject.toml --python "${VENV_PYTHON}" 2>/dev/null || \
    uv pip install --requirement pyproject.toml --python "${VENV_PYTHON}" 2>/dev/null || {
        warn "pyproject.toml install failed — falling back to core packages."
        install_core_packages
    }
else
    warn "pyproject.toml not found — installing core stack directly."
    install_core_packages
fi

install_core_packages() {
    uv pip install \
        "pyspark==3.5.3" \
        "delta-spark==3.2.1" \
        "pydantic==2.7.1" \
        "faker==24.11.0" \
        "pandas==2.2.2" \
        "pyarrow==16.0.0" \
        "prefect==2.19.4" \
        "dbt-databricks==1.8.0" \
        "streamlit==1.35.0" \
        "plotly==5.22.0" \
        "pytest==8.2.0" \
        "pytest-cov==5.0.0" \
        "databricks-cli==0.18.0" \
        --python "${VENV_PYTHON}"
}

success "All packages installed."


# ─── Guard: ensure databricks-connect is NOT installed ───────────────────────
if .venv/bin/pip show databricks-connect &>/dev/null 2>&1; then
    warn "databricks-connect detected — removing it."
    warn "It hijacks SparkSession and breaks local PySpark."
    uv pip uninstall databricks-connect --python "${VENV_PYTHON}"
    success "databricks-connect removed."
fi


# ─── Step 6: Configure PYTHONPATH via .pth ───────────────────────────────────
header "Step 6/7 — Configuring PYTHONPATH"

PROJECT_ROOT="$(pwd)"
SITE_PACKAGES="$(ls -d .venv/lib/python*/site-packages 2>/dev/null | head -1)"

if [ -z "$SITE_PACKAGES" ]; then
    error "Could not find site-packages in .venv."
    exit 1
fi

PTH_FILE="${SITE_PACKAGES}/lakehouse.pth"

if [ -f "$PTH_FILE" ]; then
    warn "lakehouse.pth already exists — skipping."
else
    echo "${PROJECT_ROOT}" > "$PTH_FILE"
    success "Created: ${PTH_FILE}"
    info    "Project root added to PYTHONPATH: ${PROJECT_ROOT}"
    info    "Imports like 'from config.spark_session import get_spark' now resolve automatically."
fi


# ─── Step 7: SparkSession sanity check ───────────────────────────────────────
header "Step 7/7 — SparkSession sanity check"

info "Starting SparkSession (first run downloads Spark JARs ~200MB — be patient)..."

SANITY_RESULT="$(.venv/bin/python - <<'PYEOF'
import os, sys
sys.path.insert(0, os.getcwd())
try:
    from config.spark_session import get_spark
    spark = get_spark("SetupSanityCheck")
    count = spark.range(5).count()
    assert count == 5, f"Expected 5, got {count}"
    spark.stop()
    print("PASSED")
except Exception as e:
    print(f"FAILED: {e}")
    sys.exit(1)
PYEOF
)"

if echo "$SANITY_RESULT" | grep -q "PASSED"; then
    success "SparkSession sanity check PASSED."
else
    error "SparkSession sanity check FAILED."
    echo  "$SANITY_RESULT"
    warn  "Common fixes:"
    warn  "  1. Delete .venv and re-run: rm -rf .venv && ./setup.sh"
    warn  "  2. Check databricks-connect is not installed:"
    warn  "     .venv/bin/pip list | grep databricks"
    warn  "  3. Check Java: java -version"
    exit 1
fi


# ─── Done ─────────────────────────────────────────────────────────────────────
header "Setup complete!"
echo -e "${BOLD}Activate your environment:${RESET}\n"
echo -e "  ${GREEN}source .venv/bin/activate${RESET}\n"
echo "Then verify:"
echo -e "  ${GREEN}python --version${RESET}           # 3.10.x"
echo -e "  ${GREEN}pip list | grep pyspark${RESET}    # 3.5.3"
echo -e "  ${GREEN}pip list | grep delta${RESET}      # 3.2.1"
echo -e "  ${GREEN}pytest tests/${RESET}              # run test suite"
echo ""
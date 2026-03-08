# =============================================================================
# Trade Analytics Lakehouse — Bootstrap Setup (Windows)
# =============================================================================
# Zero dependencies. Just run in PowerShell (as Administrator):
#
#   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#   .\setup.ps1
#
# What this does:
#   1. Installs uv (via PowerShell, no Python needed)
#   2. Uses uv to download + manage Python 3.10
#   3. Creates .venv pinned to Python 3.10
#   4. Installs all project packages inside .venv
#   5. Configures PYTHONPATH via .pth file
#   6. Checks Java + sets JVM flags for PySpark/Java 17 compatibility
#   7. Runs a SparkSession sanity check
# =============================================================================

$ErrorActionPreference = "Stop"

# ─── Colours ─────────────────────────────────────────────────────────────────
function Info($msg)    { Write-Host "[INFO]  $msg" -ForegroundColor Cyan }
function Ok($msg)      { Write-Host "[OK]    $msg" -ForegroundColor Green }
function Warn($msg)    { Write-Host "[WARN]  $msg" -ForegroundColor Yellow }
function Err($msg)     { Write-Host "[ERROR] $msg" -ForegroundColor Red }
function Header($msg)  {
    $line = "-" * 55
    Write-Host "`n$line`n$msg`n$line" -ForegroundColor Cyan
}

$PYTHON_VERSION = "3.10"
$PROJECT_NAME   = "trade-analytics-lakehouse"

Header "$PROJECT_NAME Setup  |  Windows"


# ─── Step 1: Install uv ───────────────────────────────────────────────────────
Header "Step 1/7 - Installing uv"

if (Get-Command uv -ErrorAction SilentlyContinue) {
    Ok "uv already installed: $(uv --version)"
} else {
    Info "Installing uv via PowerShell (no Python required)..."
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

    # Add uv to PATH for this session
    $uvBin = "$env:USERPROFILE\.cargo\bin"
    $env:PATH = "$uvBin;$env:PATH"

    if (Get-Command uv -ErrorAction SilentlyContinue) {
        Ok "uv installed: $(uv --version)"
    } else {
        Err "uv installation failed or not on PATH."
        Warn "Try restarting PowerShell and re-running this script."
        exit 1
    }
}


# ─── Step 2: Check / Install Python 3.10 via uv ──────────────────────────────
Header "Step 2/7 - Ensuring Python $PYTHON_VERSION"

$pythonPath = $null
try {
    $pythonPath = (uv python find $PYTHON_VERSION 2>$null).Trim()
} catch {}

if ($pythonPath -and (Test-Path $pythonPath)) {
    Ok "Python $PYTHON_VERSION already available: $pythonPath"
} else {
    Info "Python $PYTHON_VERSION not found - downloading via uv..."
    Info "One-time download (~30MB). Please wait..."
    uv python install $PYTHON_VERSION
    $pythonPath = (uv python find $PYTHON_VERSION).Trim()
    Ok "Python $PYTHON_VERSION installed: $pythonPath"
}

$pyVer = & $pythonPath --version 2>&1
Info "Using: $pyVer"


# ─── Step 3: Check Java ───────────────────────────────────────────────────────
Header "Step 3/7 - Checking Java (required by PySpark)"

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    Err "Java not found. PySpark requires Java 11 or 17."
    Write-Host ""
    Warn "Download Java 17 from: https://adoptium.net/en-GB/temurin/releases/?version=17"
    Warn "After installing, restart PowerShell and re-run this script."
    exit 1
}

$javaVersion = (java -version 2>&1)[0]
Ok "Java found: $javaVersion"

# Check JAVA_TOOL_OPTIONS
$javaOpts = [System.Environment]::GetEnvironmentVariable("JAVA_TOOL_OPTIONS", "User")
if (-not ($javaOpts -like "*--add-opens*")) {
    Warn "JAVA_TOOL_OPTIONS not set - required for PySpark + Java 17."
    Warn "Setting it in your User environment variables automatically..."

    $javaFlags = "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                 "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
                 "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
                 "--add-opens=java.base/java.io=ALL-UNNAMED " +
                 "--add-opens=java.base/java.net=ALL-UNNAMED " +
                 "--add-opens=java.base/java.nio=ALL-UNNAMED " +
                 "--add-opens=java.base/java.util=ALL-UNNAMED " +
                 "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
                 "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
                 "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
                 "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
                 "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
                 "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
                 "-Dio.netty.tryReflectionSetAccessible=true"

    # Set permanently in user environment
    [System.Environment]::SetEnvironmentVariable("JAVA_TOOL_OPTIONS", $javaFlags, "User")
    # Apply for this session
    $env:JAVA_TOOL_OPTIONS = $javaFlags

    Ok "JAVA_TOOL_OPTIONS set in User environment and applied for this session."
} else {
    Ok "JAVA_TOOL_OPTIONS already set correctly."
    $env:JAVA_TOOL_OPTIONS = $javaOpts
}


# ─── Step 4: Create .venv ────────────────────────────────────────────────────
Header "Step 4/7 - Creating virtual environment"

if (Test-Path ".venv") {
    Warn ".venv already exists - skipping creation."
    Warn "To recreate: Remove-Item -Recurse -Force .venv  then re-run."
} else {
    Info "Creating .venv with Python $PYTHON_VERSION..."
    uv venv .venv --python $PYTHON_VERSION
    Ok ".venv created."
}

$venvPython = ".venv\Scripts\python.exe"


# ─── Step 5: Install packages ────────────────────────────────────────────────
Header "Step 5/7 - Installing packages"

function Install-CorePackages {
    uv pip install `
        "pyspark==3.5.3" `
        "delta-spark==3.2.1" `
        "pydantic==2.7.1" `
        "faker==24.11.0" `
        "pandas==2.2.2" `
        "pyarrow==16.0.0" `
        "prefect==2.19.4" `
        "dbt-databricks==1.8.0" `
        "streamlit==1.35.0" `
        "plotly==5.22.0" `
        "pytest==8.2.0" `
        "pytest-cov==5.0.0" `
        "databricks-cli==0.18.0" `
        --python $venvPython
}

if (Test-Path "pyproject.toml") {
    Info "Installing from pyproject.toml..."
    try {
        uv pip install --requirement pyproject.toml --python $venvPython
    } catch {
        Warn "pyproject.toml install failed - falling back to core packages."
        Install-CorePackages
    }
} else {
    Warn "pyproject.toml not found - installing core stack directly."
    Install-CorePackages
}

Ok "All packages installed."

# Guard: remove databricks-connect if present
$dcCheck = & $venvPython -m pip show databricks-connect 2>&1
if ($LASTEXITCODE -eq 0) {
    Warn "databricks-connect detected - removing it (hijacks SparkSession)."
    uv pip uninstall databricks-connect --python $venvPython
    Ok "databricks-connect removed."
}


# ─── Step 6: Configure PYTHONPATH via .pth ───────────────────────────────────
Header "Step 6/7 - Configuring PYTHONPATH"

$projectRoot  = (Get-Location).Path
$sitePackages = ".venv\Lib\site-packages"
$pthFile      = "$sitePackages\lakehouse.pth"

if (-not (Test-Path $sitePackages)) {
    Err "Could not find site-packages in .venv."
    exit 1
}

if (Test-Path $pthFile) {
    Warn "lakehouse.pth already exists - skipping."
} else {
    $projectRoot | Out-File -FilePath $pthFile -Encoding utf8 -NoNewline
    Ok "Created: $pthFile"
    Info "Project root added to PYTHONPATH: $projectRoot"
}


# ─── Step 7: SparkSession sanity check ───────────────────────────────────────
Header "Step 7/7 - SparkSession sanity check"

Info "Starting SparkSession (first run downloads Spark JARs ~200MB - be patient)..."

$sanityScript = @"
import os, sys
sys.path.insert(0, r'$projectRoot')
try:
    from config.spark_session import get_spark
    spark = get_spark('SetupSanityCheck')
    count = spark.range(5).count()
    assert count == 5, f'Expected 5, got {count}'
    spark.stop()
    print('PASSED')
except Exception as e:
    print(f'FAILED: {e}')
    sys.exit(1)
"@

$result = & $venvPython -c $sanityScript 2>&1
if ($result -like "*PASSED*") {
    Ok "SparkSession sanity check PASSED."
} else {
    Err "SparkSession sanity check FAILED."
    Write-Host $result
    Warn "Common fixes:"
    Warn "  1. Delete .venv and re-run: Remove-Item -Recurse -Force .venv"
    Warn "  2. Check databricks-connect: .venv\Scripts\pip list | findstr databricks"
    Warn "  3. Check Java: java -version"
    exit 1
}


# ─── Done ─────────────────────────────────────────────────────────────────────
Header "Setup complete!"
Write-Host "`nActivate your environment:`n" -ForegroundColor White
Write-Host "  .venv\Scripts\activate`n" -ForegroundColor Green
Write-Host "Then verify:"
Write-Host "  python --version" -ForegroundColor Green
Write-Host "  pip list | findstr pyspark" -ForegroundColor Green
Write-Host "  pip list | findstr delta" -ForegroundColor Green
Write-Host "  pytest tests\" -ForegroundColor Green
Write-Host ""
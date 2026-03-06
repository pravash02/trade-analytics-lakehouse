#!/usr/bin/env python3
"""
Cross-platform environment setup script.
Installs all dependencies from pyproject.toml.
Run with: python local_setup.py
"""

import os
import platform
import subprocess
import sys
from pathlib import Path


class C:
    BLUE   = "\033[94m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    RED    = "\033[91m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"

def info(msg):    print(f"{C.BLUE}[INFO]{C.RESET}  {msg}")
def success(msg): print(f"{C.GREEN}[OK]{C.RESET}    {msg}")
def warn(msg):    print(f"{C.YELLOW}[WARN]{C.RESET}  {msg}")
def error(msg):   print(f"{C.RED}[ERROR]{C.RESET} {msg}")
def header(msg):  print(f"\n{C.BOLD}{C.BLUE}{'─' * 50}\n{msg}\n{'─' * 50}{C.RESET}")


system     = platform.system()
IS_MAC     = system == "Darwin"
IS_LINUX   = system == "Linux"
IS_WINDOWS = system == "Windows"
OS_NAME    = "macOS" if IS_MAC else "Linux" if IS_LINUX else "Windows" if IS_WINDOWS else system


def run(cmd, check=True):
    """Runs a shell command, Prints output live. Exits on failure."""
    result = subprocess.run(cmd, shell=isinstance(cmd, str), text=True)
    if check and result.returncode != 0:
        error(f"Command failed (exit {result.returncode}):\n  {cmd}")
        sys.exit(result.returncode)
    return result

def run_capture(cmd):
    result = subprocess.run(
        cmd, shell=isinstance(cmd, str),
        capture_output=True, text=True
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()

def command_exists(cmd):
    check_cmd = f"where {cmd}" if IS_WINDOWS else f"command -v {cmd}"
    code, _, _ = run_capture(check_cmd)
    return code == 0


# Install uv 
def install_uv():
    header(f"Installing uv on {OS_NAME}")

    if IS_MAC or IS_LINUX:
        info("Running uv installer via curl...")
        run('curl -LsSf https://astral.sh/uv/install.sh | sh')
        uv_bin = Path.home() / ".local" / "bin"
        os.environ["PATH"] = str(uv_bin) + os.pathsep + os.environ["PATH"]

    elif IS_WINDOWS:
        info("Running uv installer via PowerShell...")
        run('powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"')
        uv_bin = Path.home() / ".cargo" / "bin"
        os.environ["PATH"] = str(uv_bin) + os.pathsep + os.environ["PATH"]

    else:
        error(f"Unsupported OS: {system}")
        sys.exit(1)

    if command_exists("uv"):
        success("uv installed successfully!")
    else:
        error("uv installed but is not on PATH yet.")
        warn("Please restart your Terminal and re-run this script.")
        sys.exit(1)


# Check Python Version
def check_python():
    header("Checking Python version")
    major, minor = sys.version_info.major, sys.version_info.minor
    info(f"Detected Python {major}.{minor} (system interpreter)")
    if (major, minor) >= (3, 13):
        warn(f"You are running Python {major}.{minor}.")
        warn("databricks-connect requires Python <3.13 — venv will be pinned to 3.12.")
        warn("uv will auto-download Python 3.12 if not already installed.")
    success("Python check done.")


# Create venv
def create_venv():
    header("Creating virtual environment (Python 3.12)")
    venv_path = Path(".venv")

    if venv_path.exists():
        warn(".venv already exists — skipping creation.")
        warn("To recreate, delete the .venv folder and re-run this script.")
        return

    info("Creating .venv pinned to Python 3.12 with uv...")
    run("uv venv .venv --python 3.12")
    success(".venv created with Python 3.12!")


# Install Dependencies
def install_deps():
    header("Installing dependencies from pyproject.toml")

    if not Path("pyproject.toml").exists():
        error("pyproject.toml not found in the current directory.")
        warn("Make sure local_setup.py is in the same folder as pyproject.toml.")
        sys.exit(1)

    python_bin = Path(".venv") / ("Scripts/python" if IS_WINDOWS else "bin/python")

    # Install directly from pyproject.toml without editable mode
    # This avoids setuptools package-discovery issues with complex project layouts
    info("Installing core dependencies...")
    run(f'uv pip install --requirement pyproject.toml --python {python_bin}', check=False)

    info("Installing packages from pyproject.toml ...")
    deps = parse_deps("pyproject.toml")
    if deps:
        deps_str = " ".join(f'"{d}"' for d in deps)
        run(f'uv pip install {deps_str} --python {python_bin}')
        success(f"Installed {len(deps)} packages!")
    else:
        error("Could not parse dependencies from pyproject.toml.")
        sys.exit(1)


def parse_deps(toml_path):
    """Extract all dependencies from pyproject.toml"""
    deps = []
    in_deps = False
    in_dev_deps = False

    with open(toml_path) as f:
        for line in f:
            stripped = line.strip()

            # Detect sections
            if stripped == "dependencies = [":
                in_deps = True
                continue
            if stripped == 'dev = [' or stripped == '"dev" = [':
                in_dev_deps = True
                continue
            if stripped == "]" and (in_deps or in_dev_deps):
                in_deps = False
                in_dev_deps = False
                continue

            if (in_deps or in_dev_deps) and stripped.startswith('"'):
                # Strip quotes, trailing comma, and inline comments
                dep = stripped.strip('",').split('#')[0].strip().strip('"')
                if dep:
                    deps.append(dep)

    return deps


def create_pth_file():
    header("Configuring PYTHONPATH")

    project_root = Path(__file__).parent.resolve()

    # Find site-packages inside the venv
    if IS_WINDOWS:
        site_packages = Path(".venv") / "Lib" / "site-packages"
    else:
        matches = list(Path(".venv/lib").glob("python3*/site-packages"))
        if not matches:
            error("Could not find site-packages inside .venv — was the venv created?")
            sys.exit(1)
        site_packages = matches[0]

    pth_file = site_packages / "lakehouse.pth"

    if pth_file.exists():
        warn("lakehouse.pth already exists — skipping.")
        return

    pth_file.write_text(str(project_root) + "\n")
    success(f"Created: {pth_file}")
    info(f"Project root added to PYTHONPATH: {project_root}")
    info("All local modules (config, ingestion, flows, etc.) will now resolve automatically.")


# Activation hint
def print_activation_hint():
    header("Setup complete!")
    print(f"{C.BOLD}Activate your environment:{C.RESET}\n")
    if IS_WINDOWS:
        print(f"  {C.GREEN}.venv\\Scripts\\activate{C.RESET}\n")
    else:
        print(f"  {C.GREEN}source .venv/bin/activate{C.RESET}\n")
    print("Verify setup:")
    print(f"  {C.GREEN}python --version{C.RESET}   # should show 3.12.x")
    print(f"  {C.GREEN}uv pip list{C.RESET}        # all installed packages\n")


def main():
    header(f"Lakehouse Project Setup  |  {OS_NAME}")

    check_python()

    if command_exists("uv"):
        success("uv already installed — skipping.")
    else:
        warn("uv not found — installing now...")
        install_uv()

    create_venv()
    install_deps()
    create_pth_file()
    print_activation_hint()


if __name__ == "__main__":
    main()
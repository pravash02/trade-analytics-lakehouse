#!/usr/bin/env python3
"""
Trade Analytics Lakehouse — Setup entry point.

This script is a convenience wrapper for users who already have Python.
It delegates to the proper bootstrap script for your OS:

  macOS/Linux  →  setup.sh   (handles uv + Python 3.12 install automatically)
  Windows      →  setup.ps1  (handles uv + Python 3.12 install automatically)

If you don't have Python yet, run the bootstrap script directly:

  macOS/Linux:
    chmod +x setup.sh && ./setup.sh

  Windows (PowerShell as Administrator):
    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
    .\\setup.ps1
"""

import os
import platform
import subprocess
import sys
from pathlib import Path


class C:
    BLUE   = "\033[94m"; GREEN  = "\033[92m"
    YELLOW = "\033[93m"; RED    = "\033[91m"
    BOLD   = "\033[1m";  RESET  = "\033[0m"

def info(msg):    print(f"{C.BLUE}[INFO]{C.RESET}  {msg}")
def success(msg): print(f"{C.GREEN}[OK]{C.RESET}    {msg}")
def warn(msg):    print(f"{C.YELLOW}[WARN]{C.RESET}  {msg}")
def error(msg):   print(f"{C.RED}[ERROR]{C.RESET} {msg}")
def header(msg):
    print(f"\n{C.BOLD}{C.BLUE}{'─'*55}\n{msg}\n{'─'*55}{C.RESET}")


def main():
    header("Trade Analytics Lakehouse — Setup")

    system = platform.system()
    root   = Path(__file__).parent

    # ── Check Python version of whoever is running THIS script ───────────────
    major, minor = sys.version_info.major, sys.version_info.minor
    info(f"Detected Python {major}.{minor} (running this script)")

    if (major, minor) < (3, 8):
        error(f"Python {major}.{minor} is too old to run this script.")
        warn("Install Python 3.8+ first, OR skip this and use the bootstrap:")
        _print_bootstrap_instructions(system)
        sys.exit(1)

    if (major, minor) >= (3, 12):
        warn(f"Python {major}.{minor} cannot be used for the venv.")
        warn("PySpark 3.5.x requires Python <=3.11 inside the venv.")
        warn("The bootstrap script will install Python 3.12 automatically.")
        warn("Delegating to bootstrap now...\n")

    # ── Delegate to the real bootstrap script ────────────────────────────────
    if system in ("Darwin", "Linux"):
        script = root / "setup.sh"
        if not script.exists():
            error(f"setup.sh not found at: {script}")
            warn("Ensure setup.sh is in the same directory as this file.")
            sys.exit(1)

        info("Delegating to setup.sh ...")
        script.chmod(script.stat().st_mode | 0o755)   # ensure executable
        result = subprocess.run(["bash", str(script)], cwd=root)
        sys.exit(result.returncode)

    elif system == "Windows":
        script = root / "setup.ps1"
        if not script.exists():
            error(f"setup.ps1 not found at: {script}")
            warn("Ensure setup.ps1 is in the same directory as this file.")
            sys.exit(1)

        info("Delegating to setup.ps1 ...")
        result = subprocess.run(
            ["powershell", "-ExecutionPolicy", "RemoteSigned",
             "-File", str(script)],
            cwd=root
        )
        sys.exit(result.returncode)

    else:
        error(f"Unsupported OS: {system}")
        sys.exit(1)


def _print_bootstrap_instructions(system: str):
    print()
    if system in ("Darwin", "Linux"):
        print("  chmod +x setup.sh && ./setup.sh")
    else:
        print("  # In PowerShell (run as Administrator):")
        print("  Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser")
        print("  .\\setup.ps1")
    print()


if __name__ == "__main__":
    main()
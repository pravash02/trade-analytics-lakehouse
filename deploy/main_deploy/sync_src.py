"""
deploy/main_deploy/sync_src.py

Uploads the entire src/ directory to a Databricks Unity Catalog Volume.
Preserves directory structure.

Usage (called by deploy.sh):
    python3 deploy/main_deploy/sync_src.py

Environment variables required:
    DATABRICKS_HOST   — workspace URL
    DATABRICKS_TOKEN  — PAT or session token
    VOLUME_SRC_PATH   — target Volume path (default: /Volumes/workspace/default/trade-analytics/src)
    SRC_DIR           — local source directory (default: src/)
"""

import os
import sys
from pathlib import Path

import requests


host  = os.environ.get("DATABRICKS_HOST", "").strip().strip("_").strip("*").rstrip("/")
token = os.environ.get("DATABRICKS_TOKEN", "").strip()

if not host or not token:
    print("[ERROR] DATABRICKS_HOST or DATABRICKS_TOKEN not set")
    sys.exit(1)

headers = {"Authorization": f"Bearer {token}"}

# Resolve paths
project_root    = Path(__file__).parent.parent.parent.resolve()
src_dir         = Path(os.environ.get("SRC_DIR", str(project_root / "src")))
volume_src_path = os.environ.get(
    "VOLUME_SRC_PATH",
    "/Volumes/workspace/default/trade-analytics/src"
).rstrip("/")

print(f"[INFO] Source dir    : {src_dir}")
print(f"[INFO] Volume target : {volume_src_path}")
print(f"[INFO] Host          : {host}")


def upload_file(local_path: Path, volume_path: str) -> None:
    url = f"{host}/api/2.0/fs/files{volume_path}"
    with open(local_path, "rb") as f:
        resp = requests.put(url, headers=headers, data=f)
    if resp.status_code not in (200, 204):
        raise RuntimeError(
            f"Upload failed for {local_path} → {volume_path}: "
            f"{resp.status_code} {resp.text}"
        )


py_files = list(src_dir.rglob("*.py"))

if not py_files:
    print(f"[WARN] No .py files found in {src_dir}")
    sys.exit(0)

print(f"[INFO] Found {len(py_files)} Python files to upload\n")

uploaded = []
failed   = []

for local_path in sorted(py_files):
    # Build Volume path by replacing local src_dir prefix with volume_src_path
    # e.g. src/trade_analytics/config/settings.py
    #   → /Volumes/.../src/trade_analytics/config/settings.py
    relative    = local_path.relative_to(src_dir)
    volume_path = f"{volume_src_path}/{relative}"

    try:
        upload_file(local_path, volume_path)
        print(f"  ✓  {relative}")
        uploaded.append(str(relative))
    except RuntimeError as e:
        print(f"  ✗  {relative}  —  {e}")
        failed.append(str(relative))

print(f"\n[INFO] ── Sync Summary ─────────────────────────────────")
print(f"[INFO]   Uploaded : {len(uploaded)}")
print(f"[INFO]   Failed   : {len(failed)}")
print(f"[INFO]   Target   : {volume_src_path}")
print(f"[INFO] ──────────────────────────────────────────────────")

if failed:
    print(f"[ERROR] Failed files: {failed}")
    sys.exit(1)

print("[INFO] src/ sync complete ✓")
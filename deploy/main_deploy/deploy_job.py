"""
deploy/main_deploy/deploy_job.py

Creates or updates the Databricks Job via REST API.
Called by deploy.sh — runs with DATABRICKS_HOST and DATABRICKS_TOKEN env vars.

Why a separate file instead of inline python3 -c:
  - Easier to debug (full tracebacks, not truncated inline errors)
  - Proper exit codes (sys.exit(1) on failure stops deploy.sh)
  - Readable and testable
  - No shell escaping issues with quotes/newlines
"""

import json
import os
import sys
import requests

# ── Auth from environment ─────────────────────────────────────────────────────
host  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
token = os.environ.get("DATABRICKS_TOKEN", "")

if not host or not token:
    print("[ERROR] DATABRICKS_HOST or DATABRICKS_TOKEN not set")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type":  "application/json",
}

wheel_path = os.environ.get(
    "WHEEL_VOLUME_PATH",
    "/Volumes/workspace/default/trade-analytics/wheels/trade_analytics-0.1.0-py3-none-any.whl"
)
nb_base = "/Workspace/Shared/trade-analytics-lakehouse/databricks_notebooks"
env     = os.environ.get("ENV", "DEV")

job_payload = {
    "name": f"trade-analytics-pipeline-{env}",
    "environments": [
        {
            "environment_key": "trade_analytics_env",
            "spec": {
                "client": "1",
                "dependencies": [
                    wheel_path,
                ],
            },
        }
    ],
    "tasks": [
        {
            "task_key": "bronze_ingest",
            "description": "Ingest trades.jsonl → Bronze Delta",
            "environment_key": "trade_analytics_env",
            "notebook_task": {
                "notebook_path": f"{nb_base}/01_ingest_bronze",
                "source": "WORKSPACE",
            },
            "timeout_seconds": 1800,
            "max_retries": 1,
        },
        {
            "task_key": "silver_transform",
            "description": "PySpark Bronze → Silver Delta",
            "depends_on": [{"task_key": "bronze_ingest"}],
            "environment_key": "trade_analytics_env",
            "notebook_task": {
                "notebook_path": f"{nb_base}/02_silver_transform",
                "source": "WORKSPACE",
            },
            "timeout_seconds": 3600,
            "max_retries": 1,
        },
        {
            "task_key": "dbt_gold",
            "description": "dbt run + test → Gold Delta marts",
            "depends_on": [{"task_key": "silver_transform"}],
            "environment_key": "trade_analytics_env",
            "notebook_task": {
                "notebook_path": f"{nb_base}/03_run_dbt",
                "source": "WORKSPACE",
            },
            "timeout_seconds": 3600,
            "max_retries": 0,
        },
    ],
}

print(f"[INFO] Connecting to: {host}")
resp = requests.get(f"{host}/api/2.1/jobs/list", headers=headers)

if resp.status_code != 200:
    print(f"[ERROR] Failed to list jobs: {resp.status_code} {resp.text}")
    sys.exit(1)

jobs  = resp.json().get("jobs", [])
match = [j for j in jobs if j["settings"]["name"] == job_payload["name"]]

if match:
    job_id = match[0]["job_id"]
    print(f"[INFO] Job exists (id={job_id}) — updating...")
    resp = requests.post(
        f"{host}/api/2.1/jobs/reset",
        headers=headers,
        json={"job_id": job_id, "new_settings": job_payload},
    )
    if resp.status_code not in (200, 204):
        print(f"[ERROR] Job update failed: {resp.status_code} {resp.text}")
        sys.exit(1)
    print(f"[INFO] Job updated (id={job_id}) ✓")

else:
    print(f"[INFO] Job not found — creating...")
    resp = requests.post(
        f"{host}/api/2.1/jobs/create",
        headers=headers,
        json=job_payload,
    )
    if resp.status_code != 200:
        print(f"[ERROR] Job creation failed: {resp.status_code} {resp.text}")
        sys.exit(1)

    job_id = resp.json().get("job_id")
    print(f"[INFO] Job created (id={job_id}) ✓")

resp = requests.get(
    f"{host}/api/2.1/jobs/get?job_id={job_id}",
    headers=headers,
)
if resp.status_code != 200:
    print(f"[ERROR] Job verification failed: {resp.status_code} {resp.text}")
    sys.exit(1)

job = resp.json()
print(f"\n[INFO] ── Job Verification ──────────────────────────")
print(f"[INFO]   Name      : {job['settings']['name']}")
print(f"[INFO]   Job ID    : {job_id}")
print(f"[INFO]   Tasks     :")
for t in job["settings"]["tasks"]:
    deps = [d["task_key"] for d in t.get("depends_on", [])]
    dep_str = f" (depends on: {', '.join(deps)})" if deps else " (no dependencies)"
    print(f"[INFO]     - {t['task_key']}{dep_str}")
print(f"[INFO] ─────────────────────────────────────────────")
print(f"[INFO] Job deploy complete ✓")
print(f"[INFO] View at: {host}/jobs/{job_id}")
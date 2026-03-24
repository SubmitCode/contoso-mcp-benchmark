#!/usr/bin/env python3
"""Create a Fabric Spark notebook that downloads Contoso data, then run it."""
import os, sys, json, time, base64
from pathlib import Path
import httpx
from msal import ConfidentialClientApplication

WORKSPACE_ID = "ca6ce43f-4131-4728-bc83-08d9671f10e6"
LAKEHOUSE_ID = "c6dfd361-0163-4226-b803-babb833061ec"
LAKEHOUSE_NAME = "ContosoLakehouse"
NOTEBOOK_NAME = "LoadContosoData4"

# Notebook source code (runs inside Fabric Spark)
NOTEBOOK_CODE = """\
import subprocess, os, urllib.request

# ---- 1. Install py7zr ----
subprocess.run(["pip", "install", "-q", "py7zr"], check=True)
import py7zr

# ---- 2. Download ----
url = "https://github.com/sql-bi/Contoso-Data-Generator-V2-Data/releases/download/ready-to-use-data-2024/delta-1m.7z"
dest_7z = "/tmp/delta-1m.7z"
if not os.path.exists(dest_7z):
    print("Downloading delta-1m.7z...")
    urllib.request.urlretrieve(url, dest_7z)
print("Download done")

# ---- 3. Extract ----
extract_dir = "/tmp/delta-1m"
os.makedirs(extract_dir, exist_ok=True)
print("Extracting...")
with py7zr.SevenZipFile(dest_7z, mode="r") as z:
    z.extractall(path=extract_dir)
print("Extracted")

# ---- 4. Load each Delta table into Lakehouse via Spark ----
import glob
for table_dir in sorted(glob.glob(f"{extract_dir}/*/")):
    table_name = os.path.basename(table_dir.rstrip("/"))
    print(f"Loading {table_name}...")
    df = spark.read.format("delta").load(table_dir)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \\
        .save(f"Tables/{table_name}")
    print(f"  {table_name}: {df.count()} rows")

print("All tables loaded!")
"""

def get_token(tenant_id: str, client_id: str, client_secret: str, scope: str) -> str:
    app = ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client([scope])
    if "access_token" not in result:
        raise RuntimeError(f"Token error: {result.get('error_description')}")
    return result["access_token"]


def main():
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    tenant_id     = os.environ["FABRIC_TENANT_ID"]
    client_id     = os.environ["FABRIC_CLIENT_ID"]
    client_secret = os.environ["FABRIC_CLIENT_SECRET"]

    # Fabric API uses Power BI scope
    token = get_token(tenant_id, client_id, client_secret,
                      "https://api.fabric.microsoft.com/.default")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Build notebook payload (Fabric notebook definition format)
    # ipynb format requires source to be a list of lines
    source_lines = NOTEBOOK_CODE.splitlines(keepends=True)
    ipynb = {
        "cells": [{"cell_type": "code", "source": source_lines, "metadata": {}, "outputs": [], "execution_count": None}],
        "metadata": {
            "kernelspec": {"display_name": "Synapse PySpark", "name": "synapse_pyspark"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    ipynb_b64 = base64.b64encode(json.dumps(ipynb).encode()).decode()

    payload = {
        "displayName": NOTEBOOK_NAME,
        "type": "Notebook",
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": ipynb_b64,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }

    base = "https://api.fabric.microsoft.com/v1"

    # Check if notebook already exists; reuse it
    r = httpx.get(f"{base}/workspaces/{WORKSPACE_ID}/items?type=Notebook", headers=headers)
    r.raise_for_status()
    existing = [i for i in r.json().get("value", []) if i["displayName"] == NOTEBOOK_NAME]
    if existing:
        nb_id = existing[0]["id"]
        print(f"Reusing existing notebook {nb_id}")
    else:
        # Create notebook
        r = httpx.post(f"{base}/workspaces/{WORKSPACE_ID}/items", headers=headers, json=payload, timeout=30)
        if r.status_code not in (200, 201, 202):
            print(f"Create notebook failed: {r.status_code} {r.text}")
            sys.exit(1)

        # Handle async creation (202 with Location header)
        nb_id = None
        if r.status_code == 202:
            print(f"Waiting for notebook creation...")
            for _ in range(30):
                time.sleep(5)
                check = httpx.get(r.headers["Location"], headers=headers)
                data = check.json()
                status = data.get("status", "")
                if status in ("Succeeded", "succeeded"):
                    nb_id = data.get("createdItemId") or data.get("result", {}).get("id")
                    break
                elif status in ("Failed", "failed"):
                    print("Notebook creation failed:", data)
                    sys.exit(1)
        else:
            nb_id = r.json().get("id")

        if not nb_id:
            # Try to find it by name
            r2 = httpx.get(f"{base}/workspaces/{WORKSPACE_ID}/items?type=Notebook", headers=headers)
            items = [i for i in r2.json().get("value", []) if i["displayName"] == NOTEBOOK_NAME]
            if items:
                nb_id = items[0]["id"]

        print(f"Notebook created: {nb_id}")

    # Attach lakehouse as default
    r = httpx.post(
        f"{base}/workspaces/{WORKSPACE_ID}/notebooks/{nb_id}/updateDefinition",
        headers=headers,
        json={
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.ipynb",
                        "payload": ipynb_b64,
                        "payloadType": "InlineBase64",
                    }
                ],
            }
        },
        timeout=30,
    )
    # ignore errors here - the run might still work

    # Run notebook
    print("Running notebook...")
    run_payload = {
        "executionData": {
            "parameters": {},
            "configuration": {
                "defaultLakehouse": {
                    "name": LAKEHOUSE_NAME,
                    "id": LAKEHOUSE_ID,
                    "workspaceId": WORKSPACE_ID,
                }
            },
        }
    }
    r = httpx.post(
        f"{base}/workspaces/{WORKSPACE_ID}/items/{nb_id}/jobs/instances?jobType=RunNotebook",
        headers=headers,
        json=run_payload,
        timeout=30,
    )
    if r.status_code not in (200, 201, 202):
        print(f"Run failed: {r.status_code} {r.text}")
        sys.exit(1)

    print(f"Job started: {r.status_code}")
    job_url = r.headers.get("Location")
    if job_url:
        print(f"Polling: {job_url}")
        for i in range(120):  # poll up to 10 min
            time.sleep(5)
            jr = httpx.get(job_url, headers=headers)
            status = jr.json().get("status", "")
            print(f"  [{i*5}s] status={status}")
            if status in ("Completed", "Succeeded", "succeeded", "completed"):
                print("Notebook ran successfully!")
                return
            elif status in ("Failed", "failed", "Cancelled", "cancelled"):
                print("Notebook FAILED:", jr.json())
                sys.exit(1)
    else:
        print("No polling URL. Check Fabric UI for notebook run status.")


if __name__ == "__main__":
    main()

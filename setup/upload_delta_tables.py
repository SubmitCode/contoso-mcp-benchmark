#!/usr/bin/env python3
"""Upload local Delta tables to Fabric OneLake via ADLS Gen2 API."""
import os, sys
from pathlib import Path
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

def main():
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    tenant_id     = os.environ["FABRIC_TENANT_ID"]
    client_id     = os.environ["FABRIC_CLIENT_ID"]
    client_secret = os.environ["FABRIC_CLIENT_SECRET"]
    workspace_id  = os.environ["FABRIC_WORKSPACE_ID"]
    lakehouse_id  = os.environ.get("FABRIC_LAKEHOUSE_ID", "c6dfd361-0163-4226-b803-babb833061ec")

    data_dir = Path(__file__).parent.parent / "data" / "contoso-1m"

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    account_url = "https://onelake.dfs.fabric.microsoft.com"
    client = DataLakeServiceClient(account_url=account_url, credential=credential)

    # In OneLake, filesystem = workspace_id, and paths are relative to the lakehouse
    fs_client = client.get_file_system_client(workspace_id)

    # Find Delta table directories
    delta_tables = [d for d in data_dir.iterdir() if d.is_dir() and (d / "_delta_log").is_dir()]

    if not delta_tables:
        print("ERROR: no Delta tables found in", data_dir)
        sys.exit(1)

    for table_dir in sorted(delta_tables):
        table_name = table_dir.name
        print(f"Uploading {table_name}...")
        upload_dir(fs_client, table_dir, f"{lakehouse_id}/Tables/{table_name}")

    print("Done.")


def upload_dir(fs_client, local_dir: Path, remote_prefix: str):
    for local_file in sorted(local_dir.rglob("*")):
        if local_file.is_dir():
            continue
        rel = local_file.relative_to(local_dir)
        remote_path = f"{remote_prefix}/{rel}"
        file_client = fs_client.get_file_client(remote_path)
        with open(local_file, "rb") as f:
            data = f.read()
        file_client.upload_data(data, overwrite=True)
        print(f"  {rel}")


if __name__ == "__main__":
    main()

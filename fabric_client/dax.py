import os
import httpx
from fabric_client.auth import get_access_token

POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"


def execute_dax(query: str, top: int = None) -> list[dict]:
    """Execute a DAX query against the Power BI dataset. Returns list of row dicts."""
    if top is not None:
        query = f"EVALUATE TOPN({top}, {query.removeprefix('EVALUATE').strip()})"

    dataset_id = os.environ.get("FABRIC_DATASET_ID", "")
    token = get_access_token()

    response = httpx.post(
        f"{POWERBI_API_BASE}/datasets/{dataset_id}/executeQueries",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    return data["results"][0]["tables"][0].get("rows", [])

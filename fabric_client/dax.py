import httpx
from fabric_client.auth import get_access_token, _require_env

POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"


def execute_dax(query: str) -> list[dict]:
    """Execute a DAX query against the Power BI dataset. Returns list of row dicts.

    The query should be a complete DAX expression including EVALUATE and any TOPN wrappers.
    Example: "EVALUATE TOPN(50, Sales, Sales[OrderDate], DESC)"
    """
    dataset_id = _require_env("FABRIC_DATASET_ID")
    token = get_access_token()

    response = httpx.post(
        f"{POWERBI_API_BASE}/datasets/{dataset_id}/executeQueries",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    results = data.get("results", [])
    if not results or not results[0].get("tables"):
        return []
    return results[0]["tables"][0].get("rows", [])

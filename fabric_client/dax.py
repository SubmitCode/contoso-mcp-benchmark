"""Power BI executeQueries REST API client."""
import os

import httpx
from dotenv import load_dotenv

from fabric_client.auth import get_access_token

load_dotenv()

_POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"


def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise ValueError(f"Required environment variable '{key}' is not set. Check your .env file.")
    return value


def _strip_prefix(key: str) -> str:
    """'Products[Brand]' → 'Brand', '[Net Sales]' → 'Net Sales'"""
    if "[" in key:
        return key.split("[")[-1].rstrip("]")
    return key


def execute_dax(query: str) -> list[dict]:
    """Execute a DAX query via the Power BI executeQueries REST API.

    Returns list of row dicts with table prefixes stripped from column names.
    Example: 'Products[Brand]' → 'Brand', '[Net Sales]' → 'Net Sales'
    """
    workspace_id = _require_env("FABRIC_WORKSPACE_ID")
    dataset_id = _require_env("FABRIC_DATASET_ID")
    token = get_access_token()

    response = httpx.post(
        f"{_POWERBI_API_BASE}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}},
        timeout=60,
    )
    response.raise_for_status()

    data = response.json()
    results = data.get("results", [])
    if not results or not results[0].get("tables"):
        return []
    rows = results[0]["tables"][0].get("rows", [])
    return [{_strip_prefix(k): v for k, v in row.items()} for row in rows]

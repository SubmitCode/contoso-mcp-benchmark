import re
import httpx
from fabric_client.auth import get_access_token, _require_env

POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"

# Set to True once we confirm executeQueries works; False to always use SQL fallback
_USE_REST_API: bool | None = None


def execute_dax(query: str) -> list[dict]:
    """Execute a DAX query against the Power BI dataset. Returns list of row dicts.

    The query should be a complete DAX expression including EVALUATE and any TOPN wrappers.
    Example: "EVALUATE TOPN(50, Sales, Sales[OrderDate], DESC)"
    """
    global _USE_REST_API

    # Try REST API unless we've already confirmed it doesn't work for this session
    if _USE_REST_API is not False:
        try:
            result = _execute_dax_rest(query)
            _USE_REST_API = True
            return result
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 401:
                _USE_REST_API = False  # Direct Lake executeQueries not authorized for this SP
            else:
                raise

    # Fallback: translate simple DAX patterns to SQL
    return _execute_dax_via_sql(query)


def _execute_dax_rest(query: str) -> list[dict]:
    dataset_id   = _require_env("FABRIC_DATASET_ID")
    workspace_id = _require_env("FABRIC_WORKSPACE_ID")
    token = get_access_token()

    response = httpx.post(
        f"{POWERBI_API_BASE}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
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


def _execute_dax_via_sql(query: str) -> list[dict]:
    """Translate common DAX patterns to T-SQL and execute via the SQL Analytics Endpoint."""
    from fabric_client.sql import execute_sql, DIMENSION_MAP, MEASURE_EXPRS

    q = query.strip()

    # DAX model table names → SQL table names
    TABLE_MAP = {
        "sales": "sales", "products": "product", "product": "product",
        "customers": "customer", "customer": "customer",
        "stores": "store", "store": "store",
        "date": "date",
    }

    # Pattern: EVALUATE <TableName>  (simple table scan)
    m = re.match(r"^EVALUATE\s+(\w+)\s*$", q, re.IGNORECASE)
    if m:
        table = TABLE_MAP.get(m.group(1).lower(), m.group(1).lower())
        return execute_sql(f"SELECT TOP 50 * FROM dbo.{table}")

    # Pattern: EVALUATE TOPN(N, <TableName>, ...)  (table with ordering)
    m = re.match(r"^EVALUATE\s+TOPN\s*\(\s*(\d+)\s*,\s*(\w+)\s*,", q, re.IGNORECASE)
    if m:
        n = m.group(1)
        table = TABLE_MAP.get(m.group(2).lower(), m.group(2).lower())
        return execute_sql(f"SELECT TOP {n} * FROM dbo.{table}")

    # Pattern: EVALUATE VALUES('Column') or VALUES(Table[Column])
    m = re.match(r"^EVALUATE\s+(?:TOPN\s*\(\s*\d+\s*,\s*)?VALUES\s*\(\s*['\"]?(\w+)['\"]?\s*\)", q, re.IGNORECASE)
    if m:
        col = m.group(1)
        from fabric_client.sql import get_distinct_values
        try:
            vals = get_distinct_values(col)
            return [{col: v} for v in vals]
        except ValueError:
            pass

    raise NotImplementedError(
        f"DAX query cannot be translated to SQL. "
        f"The executeQueries REST API returned 401 (likely a Fabric tenant admin setting). "
        f"Query: {q[:200]}"
    )

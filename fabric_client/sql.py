"""SQL Analytics Endpoint client for Fabric Lakehouse."""
import decimal
import struct
import pyodbc
from msal import ConfidentialClientApplication
from fabric_client.auth import _require_env

_token_cache: dict = {}

SQL_SERVER = "zlhoxuko56xulc6qxwe2ozcrza-h7sgzsrrieueppedbdmwohyq4y.datawarehouse.fabric.microsoft.com"
DATABASE = "ContosoLakehouse"

# Dimension column → (table alias, actual_column)
DIMENSION_MAP: dict[str, tuple[str, str]] = {
    # Products
    "ProductName":      ("p", "ProductName"),
    "Brand":            ("p", "Brand"),
    "Category":         ("p", "CategoryName"),
    "CategoryName":     ("p", "CategoryName"),
    "Subcategory":      ("p", "SubCategoryName"),
    "SubCategoryName":  ("p", "SubCategoryName"),
    # Customers
    "Country":          ("st", "CountryName"),   # store country (where sale occurred)
    "Continent":        ("c", "Continent"),
    # Stores
    "CountryName":      ("st", "CountryName"),
    "StoreName":        ("st", "CountryName"),    # no StoreName column, fall back to country
    # Date
    "Year":             ("d", "Year"),
    "Quarter":          ("d", "Quarter"),
    "Month":            ("d", "Month"),
}

MEASURE_EXPRS: dict[str, str] = {
    "Net Sales":     "SUM(s.Quantity * s.NetPrice)",
    "Margin":        "SUM((s.NetPrice - s.UnitCost) * s.Quantity)",
    "Margin %":      "CASE WHEN SUM(s.Quantity * s.NetPrice) = 0 THEN NULL "
                     "ELSE SUM((s.NetPrice - s.UnitCost) * s.Quantity) * 1.0 / SUM(s.Quantity * s.NetPrice) END",
    "Quantity":      "SUM(s.Quantity)",
    "Total Quantity": "SUM(s.Quantity)",
}


def _get_sql_token() -> str:
    tenant_id = _require_env("FABRIC_TENANT_ID")
    client_id = _require_env("FABRIC_CLIENT_ID")
    client_secret = _require_env("FABRIC_CLIENT_SECRET")
    key = (tenant_id, client_id)
    if key not in _token_cache:
        _token_cache[key] = ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
        )
    result = _token_cache[key].acquire_token_for_client(["https://database.windows.net/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"SQL token error: {result.get('error_description')}")
    return result["access_token"]


def get_connection() -> pyodbc.Connection:
    token = _get_sql_token()
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={SQL_SERVER},1433;"
        f"Database={DATABASE};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str, attrs_before={1256: token_struct}, timeout=30)


def _coerce_row(row: dict) -> dict:
    return {k: float(v) if isinstance(v, decimal.Decimal) else v for k, v in row.items()}


def execute_sql(query: str, params: tuple = ()) -> list[dict]:
    """Execute a T-SQL query and return list of row dicts."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        cols = [col[0] for col in cursor.description]
        return [_coerce_row(dict(zip(cols, row))) for row in cursor.fetchall()]
    finally:
        conn.close()


def _build_measure_query(
    measure: str,
    dimensions: list[str],
    date_from: str,
    date_to: str,
    filters: dict | None,
    top_n: int = 100,
) -> tuple[str, list]:
    """Build a T-SQL query equivalent to the SUMMARIZECOLUMNS DAX pattern.

    Returns (sql, params) where params is a list of bind values for pyodbc.
    """
    if measure not in MEASURE_EXPRS:
        raise ValueError(f"Unknown measure '{measure}'. Available: {list(MEASURE_EXPRS)}")

    measure_expr = MEASURE_EXPRS[measure]

    # Map dimension names to SELECT and GROUP BY expressions
    select_cols = []
    group_cols = []
    for dim in dimensions:
        if dim not in DIMENSION_MAP:
            raise ValueError(f"Unknown dimension '{dim}'. Available: {list(DIMENSION_MAP)}")
        alias, col = DIMENSION_MAP[dim]
        select_cols.append(f"{alias}.{col} AS [{dim}]")
        group_cols.append(f"{alias}.{col}")

    select_clause = ", ".join(select_cols)
    group_clause = ", ".join(group_cols)

    where_parts = ["s.OrderDate >= ?", "s.OrderDate <= ?"]
    params: list = [date_from, date_to]

    if filters:
        for col, val in filters.items():
            if col in DIMENSION_MAP:
                a, c = DIMENSION_MAP[col]
                where_parts.append(f"{a}.{c} = ?")
                params.append(val)

    where_clause = " AND ".join(where_parts)

    sql = f"""
SELECT TOP {top_n}
    {select_clause},
    {measure_expr} AS [{measure}]
FROM dbo.sales s
LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey
LEFT JOIN dbo.customer c ON s.CustomerKey = c.CustomerKey
LEFT JOIN dbo.store st ON s.StoreKey = st.StoreKey
LEFT JOIN dbo.date d ON CAST(s.OrderDate AS DATE) = CAST(d.Date AS DATE)
WHERE {where_clause}
GROUP BY {group_clause}
ORDER BY {measure_expr} DESC
""".strip()
    return sql, params


def execute_measure_query(
    measure: str,
    dimensions: list[str],
    date_from: str,
    date_to: str,
    filters: dict | None = None,
    top_n: int = 100,
) -> list[dict]:
    """Execute a measure query against the SQL Analytics Endpoint."""
    sql, params = _build_measure_query(measure, dimensions, date_from, date_to, filters, top_n)
    return execute_sql(sql, tuple(params))


def get_distinct_values(column: str, top_n: int = 50) -> list:
    """Get distinct values for a dimension column."""
    if column not in DIMENSION_MAP:
        raise ValueError(f"Unknown column '{column}'. Available: {list(DIMENSION_MAP)}")
    alias, col = DIMENSION_MAP[column]

    table_map = {
        "p": "dbo.product p",
        "c": "dbo.customer c",
        "st": "dbo.store st",
        "d": "dbo.date d",
        "s": "dbo.sales s",
    }
    table = table_map[alias]
    sql = f"SELECT DISTINCT TOP {top_n} {alias}.{col} AS [{column}] FROM {table} ORDER BY {alias}.{col}"
    rows = execute_sql(sql)
    return [r[column] for r in rows]

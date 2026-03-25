from fabric_client.sql import execute_sql

_DENORMALIZED_SQL = """
SELECT TOP 200
    s.OrderDate, s.Quantity, s.UnitPrice, s.NetPrice, s.UnitCost,
    s.CurrencyCode, s.ExchangeRate,
    p.ProductName, p.Brand, p.CategoryName, p.SubCategoryName,
    c.Continent,
    st.CountryName,
    d.Year, d.Quarter, d.Month
FROM dbo.sales s
LEFT JOIN dbo.product p ON s.ProductKey = p.ProductKey
LEFT JOIN dbo.customer c ON s.CustomerKey = c.CustomerKey
LEFT JOIN dbo.store st ON s.StoreKey = st.StoreKey
LEFT JOIN dbo.date d ON CAST(s.OrderDate AS DATE) = CAST(d.Date AS DATE)
"""

_TABLE_MAP = {
    "Sales": "sales", "Products": "product", "Customers": "customer",
    "Stores": "store", "Date": "date",
}


def query_raw_table(table_name: str) -> list[dict]:
    """Query a table and return rows.
    Available tables: Sales, Products, Customers, Stores, Date.
    The Sales table is returned denormalized with product, customer, store, and date columns joined in.
    Returns up to 200 rows."""
    if table_name in ("Sales", "sales"):
        return execute_sql(_DENORMALIZED_SQL)
    sql_table = _TABLE_MAP.get(table_name, table_name.lower())
    return execute_sql(f"SELECT TOP 200 * FROM dbo.{sql_table}")


def run_sql(query: str) -> list[dict]:
    """Execute a raw SQL query against the Contoso database and return results.
    Tables: dbo.sales, dbo.product, dbo.customer, dbo.store, dbo.date.
    Returns up to 1000 rows."""
    return execute_sql(query)


if __name__ == "__main__":
    try:
        from mcp.server.fastmcp import FastMCP
        mcp = FastMCP("contoso-bad")
        mcp.tool()(query_raw_table)
        mcp.tool()(run_sql)
        mcp.run()
    except ImportError:
        raise SystemExit("mcp package not installed. Run: pip install mcp")

from fabric_client.dax import execute_dax

_TABLE_NAMES = {"Sales", "Products", "Customers", "Stores", "Date"}


def query_table(table_name: str) -> list[dict]:
    """Query a model table and return up to 200 rows.
    Available tables: Sales, Products, Customers, Stores, Date."""
    if table_name not in _TABLE_NAMES:
        raise ValueError(f"Unknown table '{table_name}'. Available: {sorted(_TABLE_NAMES)}")
    return execute_dax(f"EVALUATE TOPN(200, {table_name})")


def run_dax(query: str) -> list[dict]:
    """Execute a raw DAX query against the Contoso semantic model and return results.
    Use EVALUATE to start the query. Model measures available: [Net Sales], [Margin], [Margin %], [Quantity].
    Tables: Sales, Products, Customers, Stores, Date."""
    return execute_dax(query)


if __name__ == "__main__":
    try:
        from mcp.server.fastmcp import FastMCP
        mcp = FastMCP("contoso-bad")
        mcp.tool()(query_table)
        mcp.tool()(run_dax)
        mcp.run()
    except ImportError:
        raise SystemExit("mcp package not installed. Run: pip install mcp")

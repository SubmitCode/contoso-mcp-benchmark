from fabric_client.dax import execute_dax


def query_raw_table(table_name: str) -> list[dict]:
    """Query a table and return all rows. Available tables: Sales, Products, Customers, Stores, Date."""
    return execute_dax(f"EVALUATE {table_name}")


def execute_dax_query(dax_query: str) -> list[dict]:
    """Execute any DAX query against the Contoso dataset."""
    return execute_dax(dax_query)


if __name__ == "__main__":
    try:
        from mcp.server.fastmcp import FastMCP
        mcp = FastMCP("contoso-bad")
        mcp.tool()(query_raw_table)
        mcp.tool()(execute_dax_query)
        mcp.run()
    except ImportError:
        raise SystemExit("mcp package not installed. Run: pip install mcp")

import json
from pathlib import Path
from fabric_client.sql import execute_measure_query, get_distinct_values, MEASURE_EXPRS, DIMENSION_MAP

_config = json.loads((Path(__file__).parent / "tool_config.json").read_text())
TOP_N = _config["top_n_default"]
MEASURES = _config["measures"]
SAMPLES = _config["sample_values"]


def _validate_measure_filters(measure: str, date_range: dict = None):
    if measure not in MEASURES:
        raise ValueError(f"Unknown measure '{measure}'. Available: {list(MEASURES)}")
    if not date_range:
        raise ValueError(
            f"Measure '{measure}' requires a date_range filter: "
            '{"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}'
        )


def get_kpi(
    measure: str,
    dimensions: list,
    date_range: dict = None,
    filters: dict = None,
    _top_n: int = None,   # internal override, not exposed in docstring
) -> list[dict]:
    """
    Query a KPI measure grouped by dimensions.

    Available measures: Net Sales, Margin, Margin %, Quantity
    Available dimensions: ProductName, Category, Subcategory, Brand, Country, Year, Quarter, Month

    All measures require date_range: {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}

    Results are capped at 50 rows. Use filters to narrow results.
    Sample countries: United States, Germany, France, United Kingdom, Australia, Italy, Canada, Netherlands
    Sample categories: Computers, Cell phones, TV and Video, Audio, Cameras and camcorders
    """
    _validate_measure_filters(measure, date_range)
    top_n = _top_n if _top_n is not None else TOP_N
    return execute_measure_query(
        measure=measure,
        dimensions=dimensions,
        date_from=date_range["from"],
        date_to=date_range["to"],
        filters=filters,
        top_n=top_n,
    )


def get_top_products(
    measure: str,
    date_range: dict,
    n: int = 10,
    category: str = None,
) -> list[dict]:
    """
    Get the top N products by a measure for a date range.

    measure: Net Sales, Margin, Margin %, or Quantity
    date_range: {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}
    n: number of results (max 50)
    category: optional — filter to a specific product category

    Sample categories: Computers, Cell phones, TV and Video, Audio, Cameras and camcorders
    """
    _validate_measure_filters(measure, date_range)
    n = min(n, TOP_N)
    filters = {"Category": category} if category else None
    return execute_measure_query(
        measure=measure,
        dimensions=["ProductName", "Category"],
        date_from=date_range["from"],
        date_to=date_range["to"],
        filters=filters,
        top_n=n,
    )


def get_dimension_values(dimension_column: str) -> list:
    """
    Get distinct values for a dimension column. Use before filtering to see valid values.

    Available columns: Country, Category, Subcategory, Brand, ProductName, Year, Quarter, Month
    Returns at most 50 values.
    """
    return get_distinct_values(dimension_column, top_n=TOP_N)


if __name__ == "__main__":
    try:
        from mcp.server.fastmcp import FastMCP
        mcp = FastMCP("contoso-good")
        mcp.tool()(get_kpi)
        mcp.tool()(get_top_products)
        mcp.tool()(get_dimension_values)
        mcp.run()
    except ImportError:
        raise SystemExit("mcp package not installed. Run: pip install mcp")

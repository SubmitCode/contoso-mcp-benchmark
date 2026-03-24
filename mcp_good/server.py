import json
from pathlib import Path
from fabric_client.dax import execute_dax

_config = json.loads((Path(__file__).parent / "tool_config.json").read_text())
TOP_N = _config["top_n_default"]
MEASURES = _config["measures"]
DIMENSIONS = _config["dimensions"]
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
) -> list[dict]:
    """
    Query a KPI measure grouped by dimensions.

    Available measures: Net Sales (time_span), Margin (time_span), Margin % (time_span), Quantity (time_span)
    Available dimensions: product (ProductName, Category, Subcategory, Brand), store (StoreName, Country), date (Year, Quarter, Month)

    All measures are time_span and require date_range: {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}

    Results are capped at 50 rows. Use filters to narrow results.
    Sample countries: United States, Germany, France, United Kingdom, Australia, Italy, Canada, Netherlands
    Sample categories: Computers, Cell phones, TV and Video, Audio, Cameras and camcorders
    """
    _validate_measure_filters(measure, date_range)

    dim_str = ", ".join(f"'{d}'" for d in dimensions)
    filter_parts = [
        f"'Date'[Date] >= DATE({date_range['from'].replace('-', ',')}) && "
        f"'Date'[Date] <= DATE({date_range['to'].replace('-', ',')})"
    ]

    if filters:
        for col, val in filters.items():
            filter_parts.append(f"'{col}' = \"{val}\"")

    where = " && ".join(filter_parts)

    # Use CALCULATETABLE to correctly scope filters within the model's filter context
    dax = (
        f"EVALUATE TOPN({TOP_N}, "
        f"CALCULATETABLE("
        f"SUMMARIZECOLUMNS({dim_str}, \"[{measure}]\", [{measure}]), "
        f"FILTER(ALL(), {where})"
        f"), [{measure}], DESC)"
    )
    return execute_dax(dax)


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
    return get_kpi(measure=measure, dimensions=["ProductName", "Category"], date_range=date_range, filters=filters)


def get_dimension_values(dimension_column: str) -> list:
    """
    Get distinct values for a dimension column. Use before filtering to see valid values.

    Available columns: Country, Region, Category, Subcategory, ChannelName, Year, Quarter
    Returns at most 50 values.
    """
    dax = f"EVALUATE TOPN({TOP_N}, VALUES('{dimension_column}'), '{dimension_column}'[{dimension_column}])"
    rows = execute_dax(dax)
    return [r.get(f"[{dimension_column}]", r.get(dimension_column)) for r in rows]


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

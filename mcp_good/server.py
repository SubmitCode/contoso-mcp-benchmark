import datetime
import json
from pathlib import Path

from fabric_client.sql import execute_measure_query, get_distinct_values, MEASURE_EXPRS, DIMENSION_MAP

_config = json.loads((Path(__file__).parent / "tool_config.json").read_text())
TOP_N = 100
MEASURES = _config["measures"]

_RESOURCES_PATH = Path(__file__).parent / "resources.json"

HIGH_CARDINALITY_DIMS = {"ProductName"}


def get_data_model() -> dict:
    """Return the Contoso data model: available measures, dimensions, sample queries,
    and anti-patterns. Call this first before constructing any query to understand
    what fields and combinations are valid."""
    return json.loads(_RESOURCES_PATH.read_text())


def _validate_date_range(date_range: dict) -> None:
    try:
        d_from = datetime.date.fromisoformat(date_range["from"])
        d_to   = datetime.date.fromisoformat(date_range["to"])
    except (KeyError, ValueError) as e:
        raise ValueError(f"date_range must have ISO-format 'from' and 'to' keys: {e}")
    if d_from > d_to:
        raise ValueError(f"date_range 'from' ({d_from}) must be <= 'to' ({d_to})")
    if d_from > datetime.date.today():
        raise ValueError(f"date_range 'from' ({d_from}) is in the future")


def _validate_measure_filters(measure: str, date_range: dict = None):
    if measure not in MEASURES:
        raise ValueError(f"Unknown measure '{measure}'. Available: {list(MEASURES)}")
    if not date_range:
        raise ValueError(
            f"Measure '{measure}' requires a date_range filter: "
            '{"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}'
        )
    _validate_date_range(date_range)


def _check_cardinality(dimensions: list, filters: dict | None) -> None:
    narrowing = {"Brand", "Category", "CategoryName", "Subcategory", "SubCategoryName"}
    for dim in dimensions:
        if dim in HIGH_CARDINALITY_DIMS:
            if not filters or not any(f in narrowing for f in filters):
                raise ValueError(
                    f"Dimension '{dim}' is high-cardinality. "
                    "Add a Brand, Category, or Subcategory filter to narrow results."
                )


def _wrap_result(rows: list[dict], top_n: int) -> dict:
    if not rows:
        return {
            "rows": [],
            "_note": "No data found for the given filters and date range. "
                     "Try broadening the date range or removing filters.",
        }
    result: dict = {"rows": rows}
    if len(rows) == top_n:
        result["_note"] = (
            f"Results truncated at {top_n} rows. "
            "Add filters or narrow the date range to see all data."
        )
    return result


def get_kpi(
    measure: str,
    dimensions: list,
    date_range: dict = None,
    filters: dict = None,
    _top_n: int = None,   # internal override, not exposed in docstring
) -> dict:
    """
    Query a KPI measure grouped by one or more dimensions.

    Call get_data_model() first to see all valid measures, dimensions, and sample queries.

    measure: one of Net Sales, Margin, Margin %, Quantity
    dimensions: list of dimension names (e.g. ["Country", "Year"])
    date_range: required — {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}
    filters: optional dict to filter results (e.g. {"Brand": "Contoso"})

    Returns {"rows": [...], "_note": "..."} where _note is present when results
    are truncated (100-row cap) or empty.
    """
    _validate_measure_filters(measure, date_range)
    _check_cardinality(dimensions, filters)
    top_n = _top_n if _top_n is not None else TOP_N
    rows = execute_measure_query(
        measure=measure,
        dimensions=dimensions,
        date_from=date_range["from"],
        date_to=date_range["to"],
        filters=filters,
        top_n=top_n,
    )
    return _wrap_result(rows, top_n)


def get_top_products(
    measure: str,
    date_range: dict,
    n: int = 10,
    category: str = None,
) -> dict:
    """
    Get the top N products by a measure for a date range.

    Call get_data_model() first to see valid measures and sample queries.

    measure: Net Sales, Margin, Margin %, or Quantity
    date_range: {"from": "YYYY-MM-DD", "to": "YYYY-MM-DD"}
    n: number of results (max 100)
    category: optional — filter to a specific product category

    Returns {"rows": [...], "_note": "..."} where _note is present when results
    are truncated or empty.
    """
    _validate_measure_filters(measure, date_range)
    top_n = min(n, TOP_N)
    filters = {"Category": category} if category else None
    rows = execute_measure_query(
        measure=measure,
        dimensions=["ProductName", "Category"],
        date_from=date_range["from"],
        date_to=date_range["to"],
        filters=filters,
        top_n=top_n,
    )
    return _wrap_result(rows, top_n)


def get_dimension_values(dimension_column: str) -> list:
    """
    Get distinct values for a dimension column. Use before filtering to see valid values.

    Call get_data_model() to see all available dimension columns.
    Returns at most 100 values.
    """
    return get_distinct_values(dimension_column, top_n=TOP_N)


if __name__ == "__main__":
    try:
        from mcp.server.fastmcp import FastMCP
        mcp = FastMCP("contoso-good")
        mcp.tool()(get_data_model)
        mcp.tool()(get_kpi)
        mcp.tool()(get_top_products)
        mcp.tool()(get_dimension_values)
        mcp.run()
    except ImportError:
        raise SystemExit("mcp package not installed. Run: pip install mcp")

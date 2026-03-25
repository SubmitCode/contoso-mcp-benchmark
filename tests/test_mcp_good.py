import pytest
from unittest.mock import patch
from mcp_good.server import get_kpi, get_top_product_skus, get_data_model


def test_get_kpi_accepts_no_date_range():
    # date_range is now optional — omitting it queries all-time data
    mock_rows = [{"Year": 2024, "Net Sales": 100}]
    with patch("mcp_good.server.execute_dax", return_value=mock_rows):
        result = get_kpi(measure="Net Sales", dimensions=["Year"])
    assert result["rows"] == mock_rows


def test_get_kpi_raises_on_unknown_measure():
    with pytest.raises(ValueError, match="Unknown measure"):
        get_kpi(measure="Revenue", dimensions=["Country"], date_range={"from": "2024-01-01", "to": "2024-12-31"})


def test_get_kpi_applies_top_n_limit():
    mock_rows = [{"Country": f"C{i}", "Net Sales": i} for i in range(50)]
    with patch("mcp_good.server.execute_dax", return_value=mock_rows) as mock:
        result = get_kpi(
            measure="Net Sales",
            dimensions=["Country"],
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
        )
    mock.assert_called_once()
    # Verify TOPN(100, ...) is in the query
    dax_query = mock.call_args[0][0]
    assert "TOPN(100," in dax_query
    assert "Net Sales" in dax_query


def test_get_top_product_skus_returns_limited_results():
    mock_rows = [{"SubCategoryName": f"P{i}", "Net Sales": i} for i in range(10)]
    with patch("mcp_good.server.execute_dax", return_value=mock_rows) as mock:
        result = get_top_product_skus(
            measure="Net Sales",
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
            n=10,
        )
    assert len(result["rows"]) <= 10
    dax_query = mock.call_args[0][0]
    assert "TOPN(10," in dax_query


def test_get_data_model_returns_measures_and_dimensions():
    model = get_data_model()
    assert "measures" in model
    assert "dimensions" in model
    assert "sample_queries" in model
    assert "anti_patterns" in model
    assert "Net Sales" in model["measures"]
    assert "Country" in model["dimensions"]


def test_get_kpi_truncation_note():
    mock_rows = [{"Country": f"C{i}", "Net Sales": i} for i in range(100)]
    with patch("mcp_good.server.execute_dax", return_value=mock_rows):
        result = get_kpi(
            measure="Net Sales",
            dimensions=["Country"],
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
        )
    assert "_note" in result
    assert "truncated" in result["_note"]


def test_get_kpi_no_truncation_note_when_under_cap():
    mock_rows = [{"Country": f"C{i}", "Net Sales": i} for i in range(5)]
    with patch("mcp_good.server.execute_dax", return_value=mock_rows):
        result = get_kpi(
            measure="Net Sales",
            dimensions=["Country"],
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
        )
    assert "_note" not in result


def test_get_kpi_empty_result_note():
    with patch("mcp_good.server.execute_dax", return_value=[]):
        result = get_kpi(
            measure="Net Sales",
            dimensions=["Country"],
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
        )
    assert result["rows"] == []
    assert "_note" in result
    assert "No data" in result["_note"]


def test_get_kpi_invalid_date_format():
    with pytest.raises(ValueError, match="ISO-format"):
        get_kpi(
            measure="Net Sales",
            dimensions=["Country"],
            date_range={"from": "01/01/2024", "to": "2024-12-31"},
        )


def test_get_kpi_date_from_after_to():
    with pytest.raises(ValueError, match="must be <="):
        get_kpi(
            measure="Net Sales",
            dimensions=["Country"],
            date_range={"from": "2024-12-31", "to": "2024-01-01"},
        )


def test_get_kpi_high_cardinality_without_filter():
    # HIGH_CARDINALITY_DIMS = {"ProductName"} — not in model columns, check guard works
    from mcp_good.server import HIGH_CARDINALITY_DIMS
    # Add a temp high-cardinality dim to test the guard independently
    with pytest.raises(ValueError, match="high-cardinality"):
        get_kpi(
            measure="Net Sales",
            dimensions=["ProductName"],
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
        )


def test_get_kpi_with_brand_filter():
    mock_rows = [{"SubCategoryName": "P1", "Net Sales": 100}]
    with patch("mcp_good.server.execute_dax", return_value=mock_rows):
        result = get_kpi(
            measure="Net Sales",
            dimensions=["SubCategoryName"],
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
            filters={"Brand": "Contoso"},
        )
    assert result["rows"] == mock_rows

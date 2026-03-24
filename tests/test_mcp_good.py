import pytest
from unittest.mock import patch
from mcp_good.server import get_kpi, get_top_products


def test_get_kpi_enforces_date_range_for_time_span_measure():
    with pytest.raises(ValueError, match="date_range"):
        get_kpi(measure="Net Sales", dimensions=["Country"])  # missing date_range


def test_get_kpi_raises_on_unknown_measure():
    with pytest.raises(ValueError, match="Unknown measure"):
        get_kpi(measure="Revenue", dimensions=["Country"], date_range={"from": "2024-01-01", "to": "2024-12-31"})


def test_get_kpi_applies_top_n_limit():
    mock_rows = [{"Country": f"C{i}", "Net Sales": i} for i in range(50)]
    with patch("mcp_good.server.execute_measure_query", return_value=mock_rows) as mock:
        get_kpi(
            measure="Net Sales",
            dimensions=["Country"],
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
        )
    mock.assert_called_once()
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["top_n"] == 50
    assert call_kwargs["measure"] == "Net Sales"


def test_get_top_products_returns_limited_results():
    mock_rows = [{"ProductName": f"P{i}", "Net Sales": i} for i in range(10)]
    with patch("mcp_good.server.execute_measure_query", return_value=mock_rows) as mock:
        result = get_top_products(
            measure="Net Sales",
            date_range={"from": "2024-01-01", "to": "2024-12-31"},
            n=10,
        )
    assert len(result) <= 10
    call_kwargs = mock.call_args.kwargs
    assert call_kwargs["top_n"] == 10

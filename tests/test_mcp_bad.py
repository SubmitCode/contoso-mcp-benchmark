import pytest
from unittest.mock import patch
from mcp_bad.server import query_table, run_dax


def test_query_table_returns_rows():
    mock_rows = [{"col": i} for i in range(200)]
    with patch("mcp_bad.server.execute_dax", return_value=mock_rows):
        result = query_table("Sales")
    assert len(result) == 200


def test_query_table_uses_topn_200():
    mock_rows = [{"col": i} for i in range(5)]
    with patch("mcp_bad.server.execute_dax", return_value=mock_rows) as mock:
        result = query_table("Products")
    dax_query = mock.call_args[0][0]
    assert "TOPN(200," in dax_query
    assert "Products" in dax_query


def test_query_table_raises_on_unknown_table():
    with pytest.raises(ValueError, match="Unknown table"):
        query_table("UnknownTable")


def test_run_dax_passes_through():
    mock_rows = [{"Net Sales": 9999}]
    with patch("mcp_bad.server.execute_dax", return_value=mock_rows) as mock:
        result = run_dax("EVALUATE ROW(\"Net Sales\", [Net Sales])")
    assert result == mock_rows
    assert mock.call_args[0][0] == "EVALUATE ROW(\"Net Sales\", [Net Sales])"

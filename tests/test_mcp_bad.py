import pytest
from unittest.mock import patch
from mcp_bad.server import query_raw_table, run_sql


def test_query_raw_table_returns_rows():
    mock_rows = [{"col": i} for i in range(200)]
    with patch("mcp_bad.server.execute_sql", return_value=mock_rows):
        result = query_raw_table("Sales")
    assert len(result) == 200


def test_query_raw_table_non_sales():
    mock_rows = [{"col": i} for i in range(5)]
    with patch("mcp_bad.server.execute_sql", return_value=mock_rows) as mock:
        result = query_raw_table("Products")
    assert "TOP 200" in mock.call_args[0][0]
    assert "dbo.product" in mock.call_args[0][0]


def test_run_sql_passes_through():
    mock_rows = [{"Revenue": 9999}]
    with patch("mcp_bad.server.execute_sql", return_value=mock_rows):
        result = run_sql("SELECT SUM(Quantity) FROM dbo.sales")
    assert result == mock_rows

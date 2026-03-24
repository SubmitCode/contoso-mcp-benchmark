import pytest
from unittest.mock import patch
from mcp_bad.server import query_raw_table, execute_dax_query


def test_query_raw_table_returns_rows():
    mock_rows = [{"col": i} for i in range(50)]
    with patch("mcp_bad.server.execute_sql", return_value=mock_rows):
        result = query_raw_table("Sales")
    assert len(result) == 50


def test_execute_dax_query_passes_through():
    mock_rows = [{"Revenue": 9999}]
    with patch("mcp_bad.server.execute_dax", return_value=mock_rows):
        result = execute_dax_query("EVALUATE SUMMARIZE(Sales, Sales[Year])")
    assert result == mock_rows

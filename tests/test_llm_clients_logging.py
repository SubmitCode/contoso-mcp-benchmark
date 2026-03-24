from benchmark.llm_clients import _fmt_tool_call


def test_fmt_tool_call_basic():
    assert _fmt_tool_call("get_kpi", {"measure": "Revenue"}) == "  → get_kpi(measure=Revenue)"


def test_fmt_tool_call_no_args():
    assert _fmt_tool_call("get_data_model", {}) == "  → get_data_model()"


def test_fmt_tool_call_truncates_long_string():
    long_val = "x" * 100
    result = _fmt_tool_call("execute_dax_query", {"dax_query": long_val})
    assert len(result) < 200
    assert "..." in result


def test_fmt_tool_call_non_string_value():
    result = _fmt_tool_call("get_kpi", {"dimensions": ["ProductCategory", "Year"]})
    assert "dimensions=" in result


def test_fmt_tool_call_truncates_long_non_string():
    long_list = list(range(50))
    result = _fmt_tool_call("tool", {"items": long_list})
    assert "..." in result

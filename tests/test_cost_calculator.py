import pytest
from benchmark.cost_calculator import calculate_cost, PRICING


def test_calculate_cost_gpt4o():
    cost = calculate_cost("gpt-4o", input_tokens=1000, output_tokens=500)
    expected = (1000 / 1_000_000) * PRICING["gpt-4o"]["input"] + \
               (500 / 1_000_000) * PRICING["gpt-4o"]["output"]
    assert abs(cost - expected) < 1e-8


def test_calculate_cost_unknown_model():
    with pytest.raises(KeyError):
        calculate_cost("unknown-model", input_tokens=100, output_tokens=50)


def test_pricing_table_has_required_models():
    required = {"gpt-4o", "claude-sonnet-4-6", "gemini-1.5-pro"}
    assert required.issubset(PRICING.keys())

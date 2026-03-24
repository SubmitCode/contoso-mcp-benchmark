import pytest
from benchmark.cost_calculator import calculate_cost, PRICING


def test_calculate_cost_gpt53():
    cost = calculate_cost("gpt-5.3-chat-latest", input_tokens=1000, output_tokens=500)
    expected = (1000 / 1_000_000) * PRICING["gpt-5.3-chat-latest"]["input"] + \
               (500 / 1_000_000) * PRICING["gpt-5.3-chat-latest"]["output"]
    assert abs(cost - expected) < 1e-8


def test_calculate_cost_unknown_model():
    with pytest.raises(KeyError):
        calculate_cost("unknown-model", input_tokens=100, output_tokens=50)


def test_pricing_table_has_required_models():
    required = {"gpt-5.3-chat-latest", "claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5"}
    assert required.issubset(PRICING.keys())

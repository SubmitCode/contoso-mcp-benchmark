# Pricing in USD per 1M tokens (sourced 2026-03, verify before publishing results)
PRICING = {
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "claude-sonnet-4-6": {"input": 3.00, "output": 15.00},
    "gemini-1.5-pro": {"input": 3.50, "output": 10.50},
}


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Return total cost in USD for a given model and token counts.

    Raises KeyError if the model is not in the PRICING table.
    """
    p = PRICING[model]
    return (input_tokens / 1_000_000) * p["input"] + (output_tokens / 1_000_000) * p["output"]

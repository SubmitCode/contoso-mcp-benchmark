# Pricing in USD per 1M tokens (sourced 2026-03, verify before publishing results)
# OpenAI: https://openai.com/api/pricing/
# Anthropic: https://platform.claude.com/docs/en/about-claude/pricing
PRICING = {
    "gpt-5.4":             {"input": 2.50,  "output": 15.00},
    "gpt-5.4-mini":        {"input": 0.75,  "output": 4.50},
    "claude-opus-4-6":     {"input": 5.00,  "output": 25.00},
    "claude-sonnet-4-6":   {"input": 3.00,  "output": 15.00},
    "claude-haiku-4-5":    {"input": 1.00,  "output": 5.00},
}


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Return total cost in USD for a given model and token counts.

    Raises KeyError if the model is not in the PRICING table.
    """
    p = PRICING[model]
    return (input_tokens / 1_000_000) * p["input"] + (output_tokens / 1_000_000) * p["output"]

import json
from pathlib import Path
from benchmark.ground_truth import _SCALAR_QUERIES, _RANKED_QUERIES, _CUSTOM_KEYS

_PROMPTS = json.loads((Path(__file__).parent.parent / "benchmark" / "prompts.json").read_text())


def test_all_answer_keys_are_declared():
    """Every prompt answer_key must have a matching query/key in ground_truth.py."""
    declared = set(_SCALAR_QUERIES) | set(_RANKED_QUERIES) | _CUSTOM_KEYS
    for p in _PROMPTS:
        key = p.get("answer_key")
        if key:
            assert key in declared, f"Prompt {p['id']} answer_key '{key}' not declared in ground_truth.py"

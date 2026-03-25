import json
from pathlib import Path

_PROMPTS = json.loads((Path(__file__).parent.parent / "benchmark" / "prompts.json").read_text())
_GT_PATH = Path(__file__).parent.parent / "benchmark" / "ground_truth.json"


def test_all_answer_keys_exist_in_ground_truth():
    """Every prompt answer_key must have a matching entry in ground_truth.json."""
    if not _GT_PATH.exists():
        import pytest
        pytest.skip("ground_truth.json not yet generated — run: python -m benchmark.ground_truth")
    gt = json.loads(_GT_PATH.read_text())
    for p in _PROMPTS:
        key = p.get("answer_key")
        if key:
            assert key in gt, f"Prompt {p['id']} answer_key '{key}' not found in ground_truth.json"

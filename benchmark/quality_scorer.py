"""Score LLM final answers against pre-computed ground truth."""
import json
import logging
import os
import re
from pathlib import Path

import openai

_GT_PATH = Path(__file__).parent / "ground_truth.json"

MONTH_NAMES = {
    1: ["january", "jan"],
    2: ["february", "feb"],
    3: ["march", "mar"],
    4: ["april", "apr"],
    5: ["may"],
    6: ["june", "jun"],
    7: ["july", "jul"],
    8: ["august", "aug"],
    9: ["september", "sep", "sept"],
    10: ["october", "oct"],
    11: ["november", "nov"],
    12: ["december", "dec"],
}

UP_WORDS = {"increase", "increased", "grew", "growth", "higher", "up", "rose", "risen", "more"}
DOWN_WORDS = {"decrease", "decreased", "fell", "fall", "declined", "lower", "down", "dropped", "less"}


def load_ground_truth() -> dict:
    """Load ground_truth.json. Returns empty dict if file does not exist yet."""
    if not _GT_PATH.exists():
        return {}
    return json.loads(_GT_PATH.read_text())


def _extract_numbers(text: str) -> list[float]:
    """Extract all numeric values from text, handling commas and % signs."""
    # Remove commas from numbers like 1,234,567
    cleaned = text.replace(",", "")
    return [float(m) for m in re.findall(r"-?\d+(?:\.\d+)?", cleaned)]


def _score_scalar(answer: str, expected_value: float) -> float:
    nums = _extract_numbers(answer)
    if not nums:
        return 0.0
    closest = min(nums, key=lambda n: abs(n - expected_value))
    if expected_value == 0:
        return 1.0 if closest == 0 else 0.0
    rel_err = abs(closest - expected_value) / abs(expected_value)
    if rel_err <= 0.05:
        return 1.0
    if rel_err <= 0.20:
        return 0.5
    return 0.0


def _score_top_1(answer: str, expected_name: str) -> float:
    return 1.0 if expected_name.lower() in answer.lower() else 0.0


def _score_ranked_list(answer: str, expected_list: list[str]) -> float:
    """Partial credit: each item scores 1.0 at correct position, 0.5 if mentioned anywhere."""
    if not expected_list:
        return 0.0
    answer_lower = answer.lower()
    total = 0.0
    for i, name in enumerate(expected_list):
        name_lower = name.lower()
        if name_lower not in answer_lower:
            continue
        # Try to detect position by finding the index of this name relative to others
        pos = answer_lower.find(name_lower)
        # Check how many expected items appear before this one in the answer
        items_before = sum(
            1 for j, other in enumerate(expected_list)
            if j < i and other.lower() in answer_lower and answer_lower.find(other.lower()) < pos
        )
        if items_before == i:
            total += 1.0  # correct rank
        else:
            total += 0.5  # mentioned but wrong rank
    return total / len(expected_list)


_COMPARISON_ALIASES: dict[str, list[str]] = {
    "h1": ["h1", "first half", "first-half", "jan-jun", "jan–jun", "january-june", "january–june", "1st half"],
    "h2": ["h2", "second half", "second-half", "jul-dec", "jul–dec", "july-december", "july–december", "2nd half"],
}


def _score_comparison(answer: str, winner: str) -> float:
    answer_lower = answer.lower()
    aliases = _COMPARISON_ALIASES.get(winner.lower(), [winner.lower()])
    return 1.0 if any(a in answer_lower for a in aliases) else 0.0


def _score_trend(answer: str, direction: str) -> float:
    answer_lower = answer.lower()
    words = set(re.findall(r"\w+", answer_lower))
    if direction == "up":
        if words & UP_WORDS:
            return 1.0
        if words & DOWN_WORDS:
            return 0.0
    elif direction == "down":
        if words & DOWN_WORDS:
            return 1.0
        if words & UP_WORDS:
            return 0.0
    return 0.0


def _score_month(answer: str, expected_month: int) -> float:
    answer_lower = answer.lower()
    names = MONTH_NAMES.get(expected_month, [])
    if any(name in answer_lower for name in names):
        return 1.0
    # Also accept the month number, but only if it looks like a standalone month reference
    if re.search(rf"\b{expected_month}\b", answer):
        return 1.0
    return 0.0


def score(answer: str, expected: dict | None) -> float | None:
    """Score a final answer string against an expected ground truth dict.

    Returns a float 0.0–1.0, or None if expected is None (no ground truth available).

    expected dict format:
        {"type": "scalar", "value": 12345678.9}
        {"type": "top_1", "value": "United States"}
        {"type": "ranked_list", "value": ["Computers", "Cell phones", "TV and Video"]}
        {"type": "comparison", "value": "United States"}
        {"type": "trend", "value": "up"}
        {"type": "month", "value": 7}
    """
    if expected is None or not answer:
        return None

    t = expected.get("type")
    v = expected.get("value")

    if t == "scalar":
        return _score_scalar(answer, float(v))
    elif t == "top_1":
        return _score_top_1(answer, str(v))
    elif t == "ranked_list":
        return _score_ranked_list(answer, list(v))
    elif t == "comparison":
        return _score_comparison(answer, str(v))
    elif t == "trend":
        return _score_trend(answer, str(v))
    elif t == "month":
        return _score_month(answer, int(v))
    else:
        return None


# ---------------------------------------------------------------------------
# LLM-as-a-judge scorer
# ---------------------------------------------------------------------------

_JUDGE_MODEL = "gpt-5.4-mini"

_JUDGE_PROMPT = """\
You are a strict but fair judge evaluating whether an AI assistant correctly \
answered a data analytics question.

Question: {question}
Expected answer: {expected}
Assistant's answer: {answer}

Score the assistant's answer on a scale from 0.0 to 1.0:
- 1.0 = correct answer, even if wording differs from expected
- 0.5 = partially correct (right direction but wrong numbers, or incomplete list)
- 0.0 = wrong, refused to answer, or answered a different question

Respond with ONLY a JSON object: {{"score": <float>, "rationale": "<one sentence>"}}
"""


def _get_judge_client() -> openai.OpenAI | None:
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        return None
    return openai.OpenAI(api_key=key)


def llm_judge(question: str, answer: str, expected: dict | None) -> dict | None:
    """Score an answer using an LLM judge.

    Returns {"score": float, "rationale": str} or None if judging is unavailable.
    """
    if expected is None or not answer:
        return None

    client = _get_judge_client()
    if client is None:
        return None

    # Format the expected answer for the judge
    v = expected.get("value")
    t = expected.get("type")
    if t == "scalar":
        expected_str = f"{v:,.2f}" if isinstance(v, (int, float)) else str(v)
    elif t == "ranked_list":
        expected_str = ", ".join(f"{i+1}. {item}" for i, item in enumerate(v))
    elif t == "month":
        names = MONTH_NAMES.get(int(v), [])
        expected_str = names[0].title() if names else str(v)
    elif t == "trend":
        expected_str = f"The trend is {v} ({'increasing' if v == 'up' else 'decreasing'})"
    elif t == "comparison":
        expected_str = f"The winner is {v}"
    else:
        expected_str = str(v)

    prompt = _JUDGE_PROMPT.format(
        question=question,
        expected=expected_str,
        answer=answer,
    )

    try:
        resp = client.chat.completions.create(
            model=_JUDGE_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=256,
        )
        text = resp.choices[0].message.content.strip()
        # Parse JSON from response, handling markdown code fences
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        result = json.loads(text)
        return {
            "score": max(0.0, min(1.0, float(result["score"]))),
            "rationale": result.get("rationale", ""),
        }
    except Exception as exc:
        logging.warning("LLM judge failed: %s", exc)
        return None

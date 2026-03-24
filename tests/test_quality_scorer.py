from benchmark.quality_scorer import score


# --- scalar ---

def test_scalar_exact():
    assert score("Total net sales were $1,234,567.89", {"type": "scalar", "value": 1234567.89}) == 1.0


def test_scalar_within_5_pct():
    assert score("Approximately 1,200,000", {"type": "scalar", "value": 1234567.0}) == 1.0


def test_scalar_within_20_pct():
    result = score("Around 1,050,000", {"type": "scalar", "value": 1234567.0})
    assert result == 0.5


def test_scalar_too_far():
    assert score("About 500,000", {"type": "scalar", "value": 1234567.0}) == 0.0


def test_scalar_no_numbers():
    assert score("I don't know the answer.", {"type": "scalar", "value": 1000.0}) == 0.0


# --- top_1 ---

def test_top_1_found_exact():
    assert score("The top country was United States.", {"type": "top_1", "value": "United States"}) == 1.0


def test_top_1_found_case_insensitive():
    assert score("the top country was united states.", {"type": "top_1", "value": "United States"}) == 1.0


def test_top_1_not_found():
    assert score("Germany had the highest net sales.", {"type": "top_1", "value": "United States"}) == 0.0


# --- ranked_list ---

def test_ranked_list_perfect():
    answer = "1. Computers 2. Cell phones 3. TV and Video"
    expected = {"type": "ranked_list", "value": ["Computers", "Cell phones", "TV and Video"]}
    result = score(answer, expected)
    assert result == 1.0


def test_ranked_list_partial():
    answer = "The categories are: Cell phones, Computers, TV and Video"
    expected = {"type": "ranked_list", "value": ["Computers", "Cell phones", "TV and Video"]}
    result = score(answer, expected)
    # Computers at position 2 (wrong), Cell phones at position 1 (wrong), TV and Video at position 3 (correct)
    assert 0.0 < result < 1.0


def test_ranked_list_missing_items():
    answer = "1. Computers"
    expected = {"type": "ranked_list", "value": ["Computers", "Cell phones", "TV and Video"]}
    result = score(answer, expected)
    assert 0.0 < result < 1.0


# --- comparison ---

def test_comparison_correct_winner():
    assert score("United States had higher net sales than Germany.", {"type": "comparison", "value": "United States"}) == 1.0


def test_comparison_wrong_winner():
    assert score("Germany had higher net sales.", {"type": "comparison", "value": "United States"}) == 0.0


# --- trend ---

def test_trend_up_increased():
    assert score("Net sales increased significantly from 2022 to 2024.", {"type": "trend", "value": "up"}) == 1.0


def test_trend_up_grew():
    assert score("Sales grew by 15% over the period.", {"type": "trend", "value": "up"}) == 1.0


def test_trend_wrong_direction():
    assert score("Sales decreased from 2022 to 2024.", {"type": "trend", "value": "up"}) == 0.0


def test_trend_down_correct():
    assert score("Net sales fell from 2022 to 2024.", {"type": "trend", "value": "down"}) == 1.0


# --- month ---

def test_month_by_name():
    assert score("July had the highest net sales in 2024.", {"type": "month", "value": 7}) == 1.0


def test_month_by_number():
    assert score("Month 7 was the best performing month.", {"type": "month", "value": 7}) == 1.0


def test_month_wrong():
    assert score("January had the highest net sales.", {"type": "month", "value": 7}) == 0.0


def test_month_abbreviated():
    assert score("Dec was the top month.", {"type": "month", "value": 12}) == 1.0


# --- edge cases ---

def test_none_expected_returns_none():
    assert score("Some answer", None) is None


def test_empty_answer_returns_none():
    assert score("", {"type": "scalar", "value": 1000.0}) is None


def test_unknown_type_returns_none():
    assert score("Some answer", {"type": "unknown_type", "value": "foo"}) is None

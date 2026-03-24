from unittest.mock import patch, MagicMock
from benchmark.runner import run_benchmark, LLM_RUNNERS, SERVERS


def _make_mock_llm(answer="42"):
    def _run(prompt, tools, call_tool):
        from benchmark.llm_clients import RunResult
        return RunResult(
            model="mock", prompt_id="", server="",
            input_tokens=10, output_tokens=5,
            tool_calls=1, final_answer=answer,
        )
    return _run


def test_run_benchmark_filters_models():
    mock_fn = _make_mock_llm()
    with patch.dict("benchmark.runner.LLM_RUNNERS", {"model-a": mock_fn, "model-b": mock_fn}, clear=True), \
         patch.dict("benchmark.runner.SERVERS", {"good": list(SERVERS.values())[0]}, clear=True), \
         patch("benchmark.runner.PROMPTS", [{"id": "P01", "question": "q?"}]), \
         patch("benchmark.runner._write_csv"), patch("benchmark.runner._print_summary"):
        results = run_benchmark(models=["model-a"])
    assert all(r["model"] == "model-a" for r in results)
    assert len(results) == 1


def test_run_benchmark_filters_servers():
    mock_fn = _make_mock_llm()
    with patch.dict("benchmark.runner.LLM_RUNNERS", {"model-a": mock_fn}, clear=True), \
         patch.dict("benchmark.runner.SERVERS", {
             "bad": list(SERVERS.values())[0],
             "good": list(SERVERS.values())[0],
         }, clear=True), \
         patch("benchmark.runner.PROMPTS", [{"id": "P01", "question": "q?"}]), \
         patch("benchmark.runner._write_csv"), patch("benchmark.runner._print_summary"):
        results = run_benchmark(servers=["good"])
    assert all(r["server"] == "good" for r in results)
    assert len(results) == 1


def test_run_benchmark_no_filter_runs_all():
    mock_fn = _make_mock_llm()
    with patch.dict("benchmark.runner.LLM_RUNNERS", {"m1": mock_fn, "m2": mock_fn}, clear=True), \
         patch.dict("benchmark.runner.SERVERS", {
             "bad": list(SERVERS.values())[0],
             "good": list(SERVERS.values())[0],
         }, clear=True), \
         patch("benchmark.runner.PROMPTS", [{"id": "P01", "question": "q?"}]), \
         patch("benchmark.runner._write_csv"), patch("benchmark.runner._print_summary"):
        results = run_benchmark()
    assert len(results) == 4  # 1 prompt × 2 servers × 2 models

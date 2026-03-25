from unittest.mock import patch, MagicMock
from benchmark.runner import run_benchmark

_FAKE_SERVER = {
    "tools": [],
    "call": lambda name, args: {},
}


def _make_mock_llm(answer="42"):
    def _run(prompt, tools, call_tool):
        from benchmark.llm_clients import RunResult
        return RunResult(
            model="mock", prompt_id="", server="",
            input_tokens=10, output_tokens=5,
            tool_calls=1, final_answer=answer,
        )
    return _run


def _run_with_patches(tmp_path, **kwargs):
    """Run benchmark writing to a temp dir, bypassing resume and file logging."""
    with patch.dict("benchmark.runner.LLM_RUNNERS", kwargs.get("runners"), clear=True), \
         patch.dict("benchmark.runner.SERVERS", kwargs.get("servers"), clear=True), \
         patch("benchmark.runner.PROMPTS", kwargs.get("prompts", [{"id": "P01", "question": "q?"}])), \
         patch("benchmark.runner._find_latest_csv", return_value=None), \
         patch("benchmark.runner._add_file_handler"), \
         patch("benchmark.runner._GROUND_TRUTH", {}), \
         patch("benchmark.runner.Path") as MockPath:
        # Route all file writes to tmp_path
        mock_results_dir = MagicMock()
        mock_results_dir.__truediv__ = lambda self, name: tmp_path / name
        MockPath.return_value = mock_results_dir
        MockPath.__call__ = lambda self, x: tmp_path if x == "results" else MagicMock()
        # Make Path("results") / f"benchmark_..." resolve to tmp_path
        import benchmark.runner as mod
        original_path = mod.Path
        with patch.object(mod, "Path", side_effect=lambda p: tmp_path / "out.csv" if "results" in str(p) else original_path(p)):
            return run_benchmark(
                models=kwargs.get("model_filter"),
                servers=kwargs.get("server_filter"),
                fresh=True,
            )


def test_run_benchmark_filters_models(tmp_path):
    mock_fn = _make_mock_llm()
    results = _run_with_patches(
        tmp_path,
        runners={"model-a": mock_fn, "model-b": mock_fn},
        servers={"good": _FAKE_SERVER},
        model_filter=["model-a"],
    )
    assert all(r["model"] == "model-a" for r in results)
    assert len(results) == 1


def test_run_benchmark_filters_servers(tmp_path):
    mock_fn = _make_mock_llm()
    results = _run_with_patches(
        tmp_path,
        runners={"model-a": mock_fn},
        servers={"bad": _FAKE_SERVER, "good": _FAKE_SERVER},
        server_filter=["good"],
    )
    assert all(r["server"] == "good" for r in results)
    assert len(results) == 1


def test_run_benchmark_no_filter_runs_all(tmp_path):
    mock_fn = _make_mock_llm()
    results = _run_with_patches(
        tmp_path,
        runners={"m1": mock_fn, "m2": mock_fn},
        servers={"bad": _FAKE_SERVER, "good": _FAKE_SERVER},
    )
    assert len(results) == 4  # 1 prompt × 2 servers × 2 models

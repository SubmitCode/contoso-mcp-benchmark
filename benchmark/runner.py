import csv
import json
import os
from datetime import datetime
from pathlib import Path

from benchmark.llm_clients import run_openai, run_anthropic, RunResult
from benchmark.cost_calculator import calculate_cost
from benchmark.quality_scorer import score as quality_score, load_ground_truth

# Import tool functions directly (demo mode — no subprocess MCP)
import mcp_bad.server as bad_server
import mcp_good.server as good_server

PROMPTS = json.loads((Path(__file__).parent / "prompts.json").read_text())
_GROUND_TRUTH = load_ground_truth()

SERVERS = {
    "bad": {
        "tools": [
            {
                "name": "query_raw_table",
                "description": "Query a table and return all rows. Available tables: Sales, Products, Customers, Stores, Date.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"table_name": {"type": "string"}},
                    "required": ["table_name"],
                },
            },
            {
                "name": "execute_dax_query",
                "description": "Execute any DAX query against the Contoso dataset.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"dax_query": {"type": "string"}},
                    "required": ["dax_query"],
                },
            },
        ],
        "call": lambda name, args: getattr(bad_server, name)(**args),
    },
    "good": {
        "tools": [
            {
                "name": "get_data_model",
                "description": good_server.get_data_model.__doc__,
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            },
            {
                "name": "get_kpi",
                "description": good_server.get_kpi.__doc__,
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "measure": {"type": "string"},
                        "dimensions": {"type": "array", "items": {"type": "string"}},
                        "date_range": {"type": "object"},
                        "filters": {"type": "object"},
                    },
                    "required": ["measure", "dimensions"],
                },
            },
            {
                "name": "get_top_products",
                "description": good_server.get_top_products.__doc__,
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "measure": {"type": "string"},
                        "date_range": {"type": "object"},
                        "n": {"type": "integer"},
                        "category": {"type": "string"},
                    },
                    "required": ["measure", "date_range"],
                },
            },
            {
                "name": "get_dimension_values",
                "description": good_server.get_dimension_values.__doc__,
                "inputSchema": {
                    "type": "object",
                    "properties": {"dimension_column": {"type": "string"}},
                    "required": ["dimension_column"],
                },
            },
        ],
        "call": lambda name, args: getattr(good_server, name)(**args),
    },
}

LLM_RUNNERS = {
    "gpt-5.3-chat-latest": lambda p, t, c: run_openai(p, t, c, model="gpt-5.3-chat-latest"),
    "claude-opus-4-6":     lambda p, t, c: run_anthropic(p, t, c, model="claude-opus-4-6"),
    "claude-sonnet-4-6":   lambda p, t, c: run_anthropic(p, t, c, model="claude-sonnet-4-6"),
    "claude-haiku-4-5":    lambda p, t, c: run_anthropic(p, t, c, model="claude-haiku-4-5-20251001"),
}


def run_benchmark(models: list[str] | None = None, servers: list[str] | None = None) -> list[dict]:
    active_llms = {k: v for k, v in LLM_RUNNERS.items() if models is None or k in models}
    active_servers = {k: v for k, v in SERVERS.items() if servers is None or k in servers}
    results = []

    for prompt in PROMPTS:
        for server_name, server in active_servers.items():
            for llm_name, llm_fn in active_llms.items():
                print(f"Running {prompt['id']} | {server_name:4} | {llm_name}...")
                try:
                    result = llm_fn(prompt["question"], server["tools"], server["call"])
                    result.prompt_id = prompt["id"]
                    result.server = server_name
                    result.model = llm_name
                    cost = calculate_cost(llm_name, result.input_tokens, result.output_tokens)
                except Exception as exc:
                    result = RunResult(
                        model=llm_name,
                        prompt_id=prompt["id"],
                        server=server_name,
                        input_tokens=0,
                        output_tokens=0,
                        tool_calls=0,
                        final_answer="",
                        error=str(exc),
                    )
                    cost = 0.0

                expected = _GROUND_TRUTH.get(prompt.get("answer_key"))
                q_score = quality_score(result.final_answer, expected)

                if result.error:
                    print(f"  ✗  error: {result.error}")
                else:
                    total_tok = result.input_tokens + result.output_tokens
                    q_str = f"{q_score:.2f}" if q_score is not None else "n/a"
                    print(f"  ✓  {result.tool_calls} calls | {total_tok:,} tok | ${cost:.4f} | quality={q_str}")

                results.append({
                    "prompt_id": result.prompt_id,
                    "question": prompt["question"],
                    "complexity": prompt.get("complexity"),
                    "answer_type": prompt.get("answer_type"),
                    "server": result.server,
                    "model": result.model,
                    "input_tokens": result.input_tokens,
                    "output_tokens": result.output_tokens,
                    "total_tokens": result.input_tokens + result.output_tokens,
                    "tool_calls": result.tool_calls,
                    "cost_usd": round(cost, 6),
                    "quality_score": q_score,
                    "error": result.error or "",
                })

    _write_csv(results)
    _print_summary(results)
    return results


def _write_csv(results: list[dict]) -> None:
    if not results:
        return
    out_path = Path("results") / f"benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults saved to {out_path}")


def _print_summary(results: list[dict]) -> None:
    from collections import defaultdict
    totals: dict = defaultdict(lambda: {"cost": 0.0, "tokens": 0, "tool_calls": 0, "quality": 0.0, "quality_n": 0, "runs": 0})
    for r in results:
        key = f"{r['server']:4} | {r['model']}"
        totals[key]["cost"] += r["cost_usd"]
        totals[key]["tokens"] += r["total_tokens"]
        totals[key]["tool_calls"] += r["tool_calls"]
        totals[key]["runs"] += 1
        if r["quality_score"] is not None:
            totals[key]["quality"] += r["quality_score"]
            totals[key]["quality_n"] += 1

    print(f"\n{'Server | Model':<40} {'Total Cost':>12} {'Avg Tokens':>12} {'Avg Tools':>10} {'Avg Quality':>12}")
    print("-" * 90)
    for key, v in sorted(totals.items()):
        avg_tokens = v["tokens"] // v["runs"] if v["runs"] else 0
        avg_calls = v["tool_calls"] // v["runs"] if v["runs"] else 0
        avg_quality = v["quality"] / v["quality_n"] if v["quality_n"] else float("nan")
        print(f"{key:<40} ${v['cost']:>11.4f} {avg_tokens:>12} {avg_calls:>10} {avg_quality:>11.2f}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Contoso MCP benchmark")
    parser.add_argument("--models", nargs="+", choices=list(LLM_RUNNERS), default=None,
                        help="Models to run (default: all)")
    parser.add_argument("--servers", nargs="+", choices=list(SERVERS), default=None,
                        help="Servers to run (default: all)")
    args = parser.parse_args()
    run_benchmark(models=args.models, servers=args.servers)

# Contoso MCP Benchmark

Benchmarks a **"bad" vs "good" MCP server** on top of a Power BI semantic model (Contoso on Microsoft Fabric) across multiple LLMs — measuring token usage, tool calls, and cost per run.

## What This Shows

| Server | Design |
|--------|--------|
| Bad | 2 raw tools, no limits, no semantic context — LLM must crunch data |
| Good | Focused semantic tools, TOP 50 limit, date filter enforcement, KPI descriptions |

The benchmark runs 8 business questions against both servers across GPT-4o, Claude Sonnet, and Gemini Pro. Results reveal the real cost of poor MCP server design.

## Dataset

[Contoso Data Generator V2](https://github.com/sql-bi/Contoso-Data-Generator-V2) — 1M orders. Download from [releases](https://github.com/sql-bi/Contoso-Data-Generator-V2-Data/releases).

## Prerequisites

- Python 3.10+
- Microsoft Fabric workspace with F2+ capacity
- Service principal with `Contributor` role on the workspace
- API keys for OpenAI, Anthropic, Google

## Setup

**1. Install dependencies**
```bash
pip install -r requirements.txt
pip install -r requirements-setup.txt   # fabric-cli, one-time setup only
cp .env.example .env                    # fill in credentials
```

**2. Authenticate with Fabric**
```bash
fab auth login
# or service principal: fab auth login -u <client_id> -p <client_secret> --tenant <tenant_id>
```

**3. Provision Fabric resources (one-time)**
```bash
./setup/01_setup_workspace.sh           # creates workspace + Lakehouse
./setup/02_upload_data.sh               # downloads Contoso 1M + uploads Delta tables
./setup/03_deploy_model.sh              # deploys TMDL semantic model
```

**4. Copy the deployed model's dataset ID into `.env`**
```bash
fab ls /contoso-benchmark | grep ContosoModel
# copy the ID into FABRIC_DATASET_ID in .env
```

## Run the Benchmark

```bash
python -m benchmark.runner
```

Results are written to `results/benchmark_<timestamp>.csv` and a summary is printed to the console.

## Results

> Fill in after running against live Fabric data.

| Server | Model | Avg Total Tokens | Avg Tool Calls | Total Cost (8 prompts) |
|--------|-------|-----------------|----------------|----------------------|
| bad | gpt-4o | - | - | - |
| good | gpt-4o | - | - | - |
| bad | claude-sonnet-4-6 | - | - | - |
| good | claude-sonnet-4-6 | - | - | - |
| bad | gemini-1.5-pro | - | - | - |
| good | gemini-1.5-pro | - | - | - |

## Project Structure

```
contoso-mcp-benchmark/
├── setup/                  # One-time Fabric provisioning scripts
├── semantic_model/         # TMDL semantic model definition
├── fabric_client/          # Fabric auth + DAX query client
├── mcp_bad/                # Naive MCP server (anti-patterns)
├── mcp_good/               # Guardrailed semantic MCP server
├── benchmark/              # Prompts, LLM clients, runner, cost calculator
├── tests/                  # Unit tests
└── results/                # Benchmark output (gitignored)
```

## Extending

- **Add measures:** Edit `mcp_good/tool_config.json` — no code changes needed
- **Add LLMs:** Add an entry to `LLM_RUNNERS` in `benchmark/runner.py`
- **Add prompts:** Append to `benchmark/prompts.json`

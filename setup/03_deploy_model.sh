#!/usr/bin/env bash
# Deploys the TMDL semantic model to the Fabric workspace.
# Usage: WORKSPACE_NAME=contoso-benchmark MODEL_NAME=ContosoModel ./setup/03_deploy_model.sh
# Requires fab CLI on PATH (activate venv first: source .venv/bin/activate)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
WORKSPACE_NAME="${WORKSPACE_NAME:-contoso-benchmark}"
MODEL_NAME="${MODEL_NAME:-ContosoModel}"
FAB="${FAB:-$(command -v fab 2>/dev/null || echo "$REPO_ROOT/.venv/bin/fab")}"

echo "==> Deploying semantic model: $MODEL_NAME"
"$FAB" import "/$WORKSPACE_NAME/$MODEL_NAME.semanticmodel" \
    --definition "$REPO_ROOT/semantic_model/"

echo "==> Done."
echo "    Find the model ID with:"
echo "      fab ls -l /$WORKSPACE_NAME | grep $MODEL_NAME"
echo "    Then set FABRIC_DATASET_ID in .env"

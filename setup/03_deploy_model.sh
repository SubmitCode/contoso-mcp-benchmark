#!/usr/bin/env bash
# Deploys the TMDL semantic model to the Fabric workspace.
# Usage: WORKSPACE_NAME=contoso-benchmark MODEL_NAME=ContosoModel ./setup/03_deploy_model.sh

set -euo pipefail

WORKSPACE_NAME="${WORKSPACE_NAME:-contoso-benchmark}"
MODEL_NAME="${MODEL_NAME:-ContosoModel}"

echo "==> Deploying semantic model: $MODEL_NAME"
fab import "/$WORKSPACE_NAME/$MODEL_NAME.semanticmodel" \
    --definition ./semantic_model/

echo "==> Done. Set FABRIC_DATASET_ID to the deployed model's ID."
echo "    Find it with: fab ls /$WORKSPACE_NAME | grep -F \"$MODEL_NAME\""

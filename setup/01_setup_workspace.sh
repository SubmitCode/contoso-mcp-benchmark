#!/usr/bin/env bash
# Prerequisites: pip install ms-fabric-cli, fab auth login
# Usage: WORKSPACE_NAME=contoso-benchmark ./setup/01_setup_workspace.sh
# Note: this script is not idempotent. If the workspace already exists, fab mkdir will error.
# Delete the workspace in Fabric before re-running.

set -euo pipefail

WORKSPACE_NAME="${WORKSPACE_NAME:-contoso-benchmark}"
LAKEHOUSE_NAME="${LAKEHOUSE_NAME:-ContosoLakehouse}"

echo "==> Creating workspace: $WORKSPACE_NAME"
fab mkdir "/$WORKSPACE_NAME"

echo "==> Creating Lakehouse: $LAKEHOUSE_NAME"
fab mkdir "/$WORKSPACE_NAME/$LAKEHOUSE_NAME.lakehouse"

echo "==> Done. Set FABRIC_WORKSPACE=$WORKSPACE_NAME in your .env"

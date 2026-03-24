#!/usr/bin/env bash
# Downloads Contoso 1M dataset and uploads Delta files to the Fabric Lakehouse.
# Usage: WORKSPACE_NAME=contoso-benchmark LAKEHOUSE_NAME=ContosoLakehouse ./setup/02_upload_data.sh
# Requires fab CLI on PATH (activate venv first: source .venv/bin/activate)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
WORKSPACE_NAME="${WORKSPACE_NAME:-contoso-benchmark}"
LAKEHOUSE_NAME="${LAKEHOUSE_NAME:-ContosoLakehouse}"
DATA_DIR="$REPO_ROOT/data/contoso-1m"
CONTOSO_TAG="${CONTOSO_TAG:-ready-to-use-data-2024}"
RELEASE_URL="https://github.com/sql-bi/Contoso-Data-Generator-V2-Data/releases/download/${CONTOSO_TAG}/delta-1m.7z"
FAB="${FAB:-$(command -v fab 2>/dev/null || echo "$REPO_ROOT/.venv/bin/fab")}"

if [[ ! -f "$DATA_DIR/delta-1m.7z" ]]; then
    echo "==> Downloading Contoso 1M dataset..."
    mkdir -p "$DATA_DIR"
    curl -L "$RELEASE_URL" -o "$DATA_DIR/delta-1m.7z"
else
    echo "==> Skipping download (delta-1m.7z already exists)"
fi

echo "==> Extracting Delta tables..."
7z x -o"$DATA_DIR" -y "$DATA_DIR/delta-1m.7z"

echo "==> Uploading Delta tables to OneLake..."
TABLES_PATH="/$WORKSPACE_NAME/$LAKEHOUSE_NAME.lakehouse/Tables"

shopt -s nullglob
delta_tables=()
for d in "$DATA_DIR"/*/; do
    [[ -d "$d/_delta_log" ]] && delta_tables+=("$d")
done

if [[ ${#delta_tables[@]} -eq 0 ]]; then
    echo "ERROR: No Delta table directories found in $DATA_DIR/" >&2
    exit 1
fi

for table_dir in "${delta_tables[@]}"; do
  table_name=$(basename "$table_dir")
  echo "  Uploading $table_name..."
  "$FAB" cp -r "$table_dir" "$TABLES_PATH/$table_name"
done

echo "==> Done. Tables uploaded to $TABLES_PATH"

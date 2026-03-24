#!/usr/bin/env bash
# Downloads Contoso 1M dataset and uploads Delta files to the Fabric Lakehouse.
# Usage: WORKSPACE_NAME=contoso-benchmark LAKEHOUSE_NAME=ContosoLakehouse ./setup/02_upload_data.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
WORKSPACE_NAME="${WORKSPACE_NAME:-contoso-benchmark}"
LAKEHOUSE_NAME="${LAKEHOUSE_NAME:-ContosoLakehouse}"
DATA_DIR="$REPO_ROOT/data/contoso-1m"
CONTOSO_VERSION="${CONTOSO_VERSION:-v2.0}"
RELEASE_URL="https://github.com/sql-bi/Contoso-Data-Generator-V2-Data/releases/download/${CONTOSO_VERSION}/contoso-1m-delta.zip"

echo "==> Downloading Contoso 1M dataset..."
mkdir -p "$DATA_DIR"
curl -L "$RELEASE_URL" -o "$DATA_DIR/contoso-1m-delta.zip"
unzip -o "$DATA_DIR/contoso-1m-delta.zip" -d "$DATA_DIR"

echo "==> Uploading Delta tables to OneLake..."
TABLES_PATH="/$WORKSPACE_NAME/$LAKEHOUSE_NAME.lakehouse/Tables"

shopt -s nullglob
tables=("$DATA_DIR"/Tables/*/)
if [[ ${#tables[@]} -eq 0 ]]; then
    echo "ERROR: No table directories found in $DATA_DIR/Tables/" >&2
    exit 1
fi

for table_dir in "${tables[@]}"; do
  table_name=$(basename "$table_dir")
  echo "  Uploading $table_name..."
  fab cp -r "$table_dir" "$TABLES_PATH/$table_name"
done

echo "==> Done. Tables uploaded to $TABLES_PATH"

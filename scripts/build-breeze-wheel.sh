#!/usr/bin/env bash
#
# Build a Breeze-installable provider wheel from the standalone `sql-as-dag` package.
#
# Why this script exists
# ----------------------
# Breeze installs provider wheels from the repo's `dist/` folder via
# `--use-distributions-from-dist`, but its in-container installer
# (scripts/in_container/install_airflow_and_providers.py) ONLY globs filenames
# matching `apache_airflow_providers_*`. A wheel named `sql_as_dag-*.whl` is
# silently ignored.
#
# You cannot just rename the built wheel: a wheel's distribution name is baked
# into its internal metadata (`.dist-info/` directory name, `METADATA` `Name:`,
# and the `RECORD` hashes). pip/uv reject a mismatch, e.g.:
#
#   The .dist-info directory sql_as_dag-0.0.1 does not start with the
#   normalized package name: apache-airflow-providers-sql-as-dag
#
# So we must BUILD under the target distribution name. This script temporarily
# overrides the name (distribution + provider-info package-name), builds, and
# ALWAYS restores the original files (even on failure). The importable module
# stays `sql_as_dag`; only the distribution/wheel name changes.
#
# Usage
# -----
#   scripts/build-breeze-wheel.sh [DEST_DIR]
#
# DEST_DIR defaults to ./dist. Pass your Airflow monorepo's dist/ to stage it
# directly, e.g.:
#   scripts/build-breeze-wheel.sh /path/to/airflow/dist
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="${1:-$REPO_ROOT/dist}"

PYPROJECT="$REPO_ROOT/pyproject.toml"
PROVIDER_INFO="$REPO_ROOT/src/sql_as_dag/get_provider_info.py"

REAL_NAME="sql-as-dag"
BUILD_NAME="apache-airflow-providers-sql-as-dag"

# Back up the files we mutate and restore them no matter how we exit.
cp "$PYPROJECT" "$PYPROJECT.bak"
cp "$PROVIDER_INFO" "$PROVIDER_INFO.bak"
restore() {
  mv -f "$PYPROJECT.bak" "$PYPROJECT" 2>/dev/null || true
  mv -f "$PROVIDER_INFO.bak" "$PROVIDER_INFO" 2>/dev/null || true
}
trap restore EXIT

# Apply the temporary name override (exactly one occurrence each).
python3 - "$PYPROJECT" "$PROVIDER_INFO" "$REAL_NAME" "$BUILD_NAME" <<'PY'
import pathlib
import sys

pyproject, provider_info, real, build = sys.argv[1:5]
edits = [
    (pyproject, f'name = "{real}"', f'name = "{build}"'),
    (provider_info, f'"package-name": "{real}"', f'"package-name": "{build}"'),
]
for path, old, new in edits:
    p = pathlib.Path(path)
    text = p.read_text()
    if old not in text:
        raise SystemExit(f"[build-breeze-wheel] expected {old!r} in {path}; aborting")
    p.write_text(text.replace(old, new, 1))
PY

echo "[build-breeze-wheel] building wheel as '$BUILD_NAME' (module stays 'sql_as_dag')..."
rm -f "$REPO_ROOT"/dist/apache_airflow_providers_sql_as_dag-*.whl
uv build --wheel --out-dir "$REPO_ROOT/dist"

WHEEL="$(ls -t "$REPO_ROOT"/dist/apache_airflow_providers_sql_as_dag-*.whl | head -1)"
if [ -z "${WHEEL:-}" ]; then
  echo "[build-breeze-wheel] ERROR: wheel not produced" >&2
  exit 1
fi

if [ "$(cd "$DEST_DIR" 2>/dev/null && pwd || true)" != "$(dirname "$WHEEL")" ]; then
  mkdir -p "$DEST_DIR"
  cp "$WHEEL" "$DEST_DIR/"
fi

echo "[build-breeze-wheel] staged: $DEST_DIR/$(basename "$WHEEL")"
echo "[build-breeze-wheel] install in Breeze with:"
echo "    breeze start-airflow --use-distributions-from-dist --distribution-format wheel --providers-skip-constraints"

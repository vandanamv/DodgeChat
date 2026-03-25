#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python was not found on PATH." >&2
  exit 1
fi

if [ -f "requirements.txt" ] && grep -q '[^[:space:]#]' requirements.txt; then
  "$PYTHON_BIN" -m pip install -r requirements.txt
fi

exec "$PYTHON_BIN" graph_chat_app.py

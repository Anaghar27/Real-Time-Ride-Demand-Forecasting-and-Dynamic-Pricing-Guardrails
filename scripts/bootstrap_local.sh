#!/usr/bin/env bash
set -euo pipefail

REQUIRED_PYTHON="3.11"

if command -v python3.11 >/dev/null 2>&1; then
  PYTHON_BIN="python3.11"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "python3.11 (or python3) is required but not installed." >&2
  exit 1
fi

PYTHON_VERSION=$($PYTHON_BIN -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [ "$PYTHON_VERSION" != "$REQUIRED_PYTHON" ]; then
  echo "Python ${REQUIRED_PYTHON} is required. Found ${PYTHON_VERSION} via ${PYTHON_BIN}." >&2
  exit 1
fi

if [ -d ".venv" ]; then
  VENV_VERSION=$(.venv/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "missing")
  if [ "$VENV_VERSION" != "$REQUIRED_PYTHON" ]; then
    rm -rf .venv
  fi
fi

if [ ! -d ".venv" ]; then
  $PYTHON_BIN -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "Local bootstrap complete."
echo "Next steps:"
echo "  1) make up"
echo "  2) make smoke"
echo "  3) make api  (optional, if running API outside Docker)"

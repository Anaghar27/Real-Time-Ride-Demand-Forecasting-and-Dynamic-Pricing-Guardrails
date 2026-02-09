#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-${POSTGRES_HOST:-localhost}}"
PORT="${2:-${POSTGRES_PORT:-5432}}"
TIMEOUT="${3:-30}"

start_ts=$(date +%s)

while true; do
  if pg_isready -h "$HOST" -p "$PORT" >/dev/null 2>&1; then
    echo "Postgres is reachable at ${HOST}:${PORT}"
    exit 0
  fi

  now_ts=$(date +%s)
  elapsed=$((now_ts - start_ts))
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "Timed out waiting for Postgres at ${HOST}:${PORT} after ${TIMEOUT}s" >&2
    exit 1
  fi

  sleep 1
done

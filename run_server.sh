#!/usr/bin/env bash
# Launch the banking server on the server VM.
set -euo pipefail
cd "$(dirname "$0")"

HOST="${BANK_HOST:-0.0.0.0}"
PORT="${BANK_PORT:-8080}"
MAX_CLIENTS="${BANK_MAX_CLIENTS:-50}"
DB="${BANK_DB:-server/bank.db}"

exec python3 server/bank_server.py \
    --host "$HOST" --port "$PORT" \
    --max-clients "$MAX_CLIENTS" --db "$DB"

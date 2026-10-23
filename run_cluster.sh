#!/usr/bin/env bash
# Launch a fault-tolerant cluster node (Server A or Server B).
#
#   ./run_cluster.sh A primary 192.168.64.131:8081
#   ./run_cluster.sh B backup  192.168.64.130:8080
#
# Arguments: <node> <role> <peer host:port>
set -euo pipefail
cd "$(dirname "$0")"

NODE="${1:-A}"
ROLE="${2:-primary}"
PEER="${3:-}"

PORT="${BANK_CLUSTER_PORT:-8080}"
if [ "$NODE" = "B" ] && [ -z "${BANK_CLUSTER_PORT:-}" ]; then
    PORT=8081
fi
MAX_CLIENTS="${BANK_MAX_CLIENTS:-50}"
DB="${BANK_DB:-server/${NODE}.db}"
LOG="${BANK_LOG:-server/${NODE}.log}"

PEER="${PEER#http://}"          # accept "host:port" and "http://host:port"
PEER_URL=""
if [ -n "$PEER" ]; then PEER_URL="http://$PEER"; fi

exec python3 server/cluster_server.py \
    --node "$NODE" --role "$ROLE" \
    --host 0.0.0.0 --port "$PORT" \
    --peer "$PEER_URL" \
    --max-clients "$MAX_CLIENTS" \
    --db "$DB" --log "$LOG"

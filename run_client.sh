#!/usr/bin/env bash
# Launch the banking client on the client VM.
# Usage: ./run_client.sh <server-ip> [balance|deposit|withdraw <acct> [amt]]
set -euo pipefail
cd "$(dirname "$0")"

SERVER_IP="${1:-192.168.64.130}"
shift || true
exec python3 client/bank_client.py --host "$SERVER_IP" "$@"

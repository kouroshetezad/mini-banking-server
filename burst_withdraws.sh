#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Run SEVERAL clients in parallel from one machine (the demo the assignment
# asks for: "several clients working at the same time").
#
#   ./burst_withdraws.sh <server-ip> [port] [clients] [ops-per-client] [account] [amount]
#
# Examples:
#   ./burst_withdraws.sh 192.168.64.130
#   ./burst_withdraws.sh 192.168.64.130 8080 3 20 A1001 10
#   ./burst_withdraws.sh 192.168.64.130 8080 5 40 A1001 5
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")"

HOST="${1:-192.168.64.130}"
PORT="${2:-8080}"
CLIENTS="${3:-3}"
OPS="${4:-20}"
ACCOUNT="${5:-A1001}"
AMOUNT="${6:-10}"

CLI=(python3 client/bank_client.py --host "$HOST" --port "$PORT")

echo "server   : $HOST:$PORT"
echo "clients  : $CLIENTS  (parallel)"
echo "ops each : $OPS"
echo "action   : $CLIENTS x $OPS x withdraw $ACCOUNT $AMOUNT"
echo

echo "balance BEFORE :"
"${CLI[@]}" balance "$ACCOUNT"
echo

start=$(date +%s)

# Launch every client in the background at the same time.
pids=()
for c in $(seq 1 "$CLIENTS"); do
  (
    ok=0; err=0
    for i in $(seq 1 "$OPS"); do
      if "${CLI[@]}" withdraw "$ACCOUNT" "$AMOUNT" >/dev/null 2>&1; then
        ok=$((ok + 1))
      else
        err=$((err + 1))
      fi
    done
    echo "client $c : ok=$ok  err=$err"
  ) &
  pids+=("$!")
done

# Wait for all clients to finish.
for pid in "${pids[@]}"; do
  wait "$pid"
done

end=$(date +%s)
echo
echo "elapsed: $((end - start))s"
echo "balance AFTER  :"
"${CLI[@]}" balance "$ACCOUNT"
echo
echo "Expected final balance = before - ($CLIENTS x $OPS x $AMOUNT)"
echo "If the number matches, concurrent operations were handled correctly."

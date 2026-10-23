#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Demo driver for the fault-tolerant cluster (Advanced Bonus).
#
# Run this on a CLIENT machine. Both node addresses are given, so the client
# keeps working when the primary dies.
#
#   ./demo_cluster.sh 192.168.64.130:8080 192.168.64.131:8081
#
# The bonus video needs, in order:
#   1. clients sending requests,
#   2. Server A being stopped while they are active,
#   3. Server B taking over,
#   4. the final balances.
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")"

A="${1:-192.168.64.130:8080}"
B="${2:-192.168.64.131:8081}"
CLI=(python3 client/bank_client.py --hosts "$A,$B")

hr() { printf '\n===== %s =====\n' "$1"; }

hr "1. normal operations (served by the primary)"
"${CLI[@]}" balance A1001
"${CLI[@]}" deposit A1001 200
"${CLI[@]}" withdraw A1001 50
"${CLI[@]}" transfer A1001 A1002 25
"${CLI[@]}" balance A1001

hr "2. error cases (server must keep running)"
"${CLI[@]}" balance Z9999                 # unknown account
"${CLI[@]}" withdraw A1003 5000           # insufficient funds
"${CLI[@]}" withdraw A1001 -50            # negative amount
"${CLI[@]}" deposit  A1001 0              # zero amount
"${CLI[@]}" withdraw A1001 abc            # non-numeric amount
"${CLI[@]}" raw '{"action":"withdraw","account":"A1001"}'   # missing field
"${CLI[@]}" raw '{not json'               # malformed
"${CLI[@]}" balance A1001                 # still alive

hr "3. several clients at once"
for i in 1 2 3; do
  ( for j in $(seq 1 10); do
      "${CLI[@]}" withdraw A1001 5 >/dev/null && echo "client $i op $j ok"
    done ) &
done
wait

hr "4. fail-over"
echo "Now STOP Server A (Ctrl+C in its terminal, or: pkill -f 'cluster_server.py.*node A')."
read -r -p "Press Enter once Server A is down... " _

echo "The client still has both addresses and retries with the same request id."
for attempt in $(seq 1 30); do
  if "${CLI[@]}" balance A1001 >/dev/null 2>&1; then
    echo "cluster is serving again (attempt $attempt)"
    break
  fi
  sleep 1
done

hr "5. final balances (must be correct, nothing lost or doubled)"
"${CLI[@]}" balance A1001
"${CLI[@]}" balance A1002
"${CLI[@]}" raw '{"action":"balance","account":"A1001"}' >/dev/null || true

hr "6. node status"
for addr in "$A" "$B"; do
  echo "--- $addr ---"
  curl -s "http://$addr/cluster/status" || echo "(node down)"
  echo
done

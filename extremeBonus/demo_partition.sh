#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Extreme Bonus demo driver: network partition and split brain.
#
# Run this on the CLIENT VM (VM 3).  It walks the whole scenario the assignment
# asks for and pauses so you can cut / restore the network at the right moment.
#
#   ./extremeBonus/demo_partition.sh 192.168.64.130:8080 192.168.64.131:8081 \
#                                    192.168.64.132:8099
#
# All four machines are plain Linux VMs:
#   VM 1  Server A     VM 2  Server B     VM 3  client     VM 4  witness
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/.."

A="${1:-192.168.64.130:8080}"
B="${2:-192.168.64.131:8081}"
W="${3:-192.168.64.132:8099}"

CLI=(python3 client/bank_client.py --hosts "$A,$B")
# For the split-brain proof we must talk to ONE specific node, never fail over.
# With --hosts the client would silently hop to the healthy side and the 503 we
# are trying to show would be masked by a 200.  --retries 1 keeps it snappy.
A_HOST="${A%%:*}"; A_PORT="${A##*:}"
B_HOST="${B%%:*}"; B_PORT="${B##*:}"
CLI_A=(python3 client/bank_client.py --host "$A_HOST" --port "$A_PORT" --retries 1)
CLI_B=(python3 client/bank_client.py --host "$B_HOST" --port "$B_PORT" --retries 1)
hr() { printf '\n===== %s =====\n' "$1"; }
pause() { read -r -p "$1 " _; }

hr "0. witness state (the arbiter on VM 4)"
curl -s "http://$W/witness/state" || echo "(witness not reachable)"
echo

hr "1. normal operation: A leads, writes replicate to B"
"${CLI[@]}" balance  A1001
"${CLI[@]}" deposit  A1001 100
"${CLI[@]}" withdraw A1001 40
for addr in "$A" "$B"; do
  echo "--- $addr ---"
  curl -s "http://$addr/cluster/status" | python3 -c \
    'import sys,json; d=json.load(sys.stdin); print("leader=%s solo=%s term=%s idx=%s bal=%s" % (d["leader"], d["solo"], d["term"], d["last_index"], d["accounts"]["A1001"]))'
done
curl -s "http://$W/witness/state"
echo

hr "2. CUT THE NETWORK between the two servers"
cat <<'NOTE'
On the two server VMs, block the other side and keep the processes running.
Use BOTH commands, one per VM (this is the real partition):

    # on VM 1 (A): block traffic to B
    sudo iptables -A OUTPUT -d 192.168.64.131 -j DROP

    # on VM 2 (B): block traffic to A
    sudo iptables -A OUTPUT -d 192.168.64.130 -j DROP

Both servers stay alive.  The witness on the host is NOT blocked.
NOTE
pause "Press Enter once the link is cut..."

echo "waiting 8s so both nodes notice the partition and B tries to take over..."
sleep 8

hr "3. during the split: exactly one side may write"
echo "--- A ---"
curl -s "http://$A/cluster/status"
echo
echo "--- B ---"
curl -s "http://$B/cluster/status"
echo
curl -s "http://$W/witness/state"
echo

echo "Now try to WRITE on both sides -- only one must succeed:"
echo "  write to A:"; "${CLI_A[@]}" deposit A1001 7
echo "  write to B:"; "${CLI_B[@]}" deposit A1001 9

hr "4. HEAL the network"
cat <<'NOTE'
On both server VMs, remove the block:

    sudo iptables -D OUTPUT -d 192.168.64.131 -j DROP   # on VM 1
    sudo iptables -D OUTPUT -d 192.168.64.130 -j DROP   # on VM 2
NOTE
pause "Press Enter once the link is restored..."
echo "waiting 8s for the nodes to re-sync..."
sleep 8

hr "5. after healing: the logs converge and writes work again"
for addr in "$A" "$B"; do
  echo "--- $addr ---"
  curl -s "http://$addr/cluster/status" | python3 -c \
    'import sys,json; d=json.load(sys.stdin); print("leader=%s idx=%s hash=%s A1001=%s" % (d["leader"], d["last_index"], d["log_hash"], d["accounts"]["A1001"]))'
done
"${CLI[@]}" deposit A1002 11
"${CLI[@]}" balance A1001
"${CLI[@]}" balance A1002

hr "done"
echo "Both sides must now report the SAME hash and the SAME A1001 balance."

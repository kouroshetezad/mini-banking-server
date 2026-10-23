#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Extreme Bonus: run the witness-fenced cluster on four Linux VMs.
#
#   VM 1  (192.168.64.130)  Server A
#   VM 2  (192.168.64.131)  Server B
#   VM 3  (192.168.64.129)  client
#   VM 4  (192.168.64.132)  witness   <-- a THIRD, INDEPENDENT machine
#
# The witness must NOT run on VM 1 or VM 2.  If it did, cutting the link
# between the servers would also cut the arbiter from one side, and the
# safety argument collapses.  Any Linux box that is not A or B will do.
#
# Start order matters: witness first, then A, then B.
#
# All four roles run on plain Linux.  The witness is pure-stdlib Python and
# has no OS-specific code, so `python3 extremeBonus/witness_server.py` works
# on Ubuntu, Debian, Alpine, macOS, or Windows alike.
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."          # repository root

# Where is the witness?  Overridable so you can point at whichever machine
# you actually used without editing this file.
WITNESS="${WITNESS:-http://192.168.64.132:8099}"

# Which interface should the witness bind to?  0.0.0.0 = every interface,
# which is what you want when the servers are on other machines.
WITNESS_BIND="${WITNESS_BIND:-0.0.0.0}"
WITNESS_PORT="${WITNESS_PORT:-8099}"

case "${1:-}" in
  witness)
    # Run this on VM 4 (or any machine that is not Server A / Server B).
    echo "witness binding to ${WITNESS_BIND}:${WITNESS_PORT}"
    echo "servers should be configured with --witness http://<this-host-ip>:${WITNESS_PORT}"
    exec python3 extremeBonus/witness_server.py \
        --host "$WITNESS_BIND" --port "$WITNESS_PORT" \
        --log extremeBonus/witness.log
    ;;
  A)
    exec python3 extremeBonus/quorum_server.py \
        --node A --role primary \
        --host 0.0.0.0 --port 8080 \
        --peer "${2:-http://192.168.64.131:8081}" \
        --witness "$WITNESS" \
        --max-clients 50 \
        --db extremeBonus/nodeA.db --log extremeBonus/nodeA.log
    ;;
  B)
    exec python3 extremeBonus/quorum_server.py \
        --node B --role backup \
        --host 0.0.0.0 --port 8081 \
        --peer "${2:-http://192.168.64.130:8080}" \
        --witness "$WITNESS" \
        --max-clients 50 \
        --db extremeBonus/nodeB.db --log extremeBonus/nodeB.log
    ;;
  *)
    cat <<'USAGE'
usage: ./extremeBonus/run_quorum.sh <witness|A|B> [peer-url]

  witness   run the arbiter      (on VM 4 - any machine that is NOT A or B)
  A         run Server A         (on VM 1)
  B         run Server B         (on VM 2)

environment:
  WITNESS        witness URL  (default http://192.168.64.132:8099)
  WITNESS_BIND   witness bind (default 0.0.0.0)
  WITNESS_PORT   witness port (default 8099)

examples:
  # on VM 4:
  ./extremeBonus/run_quorum.sh witness

  # on VM 1 (pointing at the witness on VM 4):
  ./extremeBonus/run_quorum.sh A

  # if your witness lives elsewhere, override it:
  WITNESS=http://10.0.0.7:8099 ./extremeBonus/run_quorum.sh A
USAGE
    exit 2
    ;;
esac


#!/usr/bin/env bash
# Run the whole test suite and print a one-line verdict per test.
#
#   ./run_tests.sh
#
# Every test starts its own servers on localhost and cleans up afterwards, so
# this can be run on one machine (it is also the fastest way to check the
# submission before recording the video).
set -uo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python

TESTS=(
  test_errors
  test_atomicity
  test_max_clients
  test_concurrency
  test_durability
  test_failover
  test_exactly_once
  test_client_failover
  test_failover_under_load
)

failed=0
for t in "${TESTS[@]}"; do
  printf '%-28s ' "$t"
  if timeout 240 "$PY" "tests/$t.py" >"/tmp/$t.out" 2>&1; then
    echo "PASS"
  else
    echo "FAIL"
    tail -n 12 "/tmp/$t.out" | sed 's/^/    /'
    failed=$((failed + 1))
  fi
done

echo
if [ "$failed" -eq 0 ]; then
  echo "all ${#TESTS[@]} tests passed"
else
  echo "$failed of ${#TESTS[@]} tests failed"
fi
exit "$failed"

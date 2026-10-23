"""The client must follow a fail-over by itself.

A client that only knows one address can never reach the new primary after the
old one dies -- which would make the demo impossible.  ``--hosts a,b`` makes the
client try both nodes, retrying with the same ``request_id``.

Run:  python3 tests/test_client_failover.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

import bank_client  # noqa: E402

A_PORT, B_PORT = 8111, 8112


def spawn(args):
    return subprocess.Popen([sys.executable] + args,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_for(predicate, timeout: float, interval: float = 0.25) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(interval)
    return False


def leader(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/cluster/status",
                                    timeout=1) as r:
            return bool(__import__("json").loads(r.read()).get("leader"))
    except Exception:
        return False


def main() -> int:
    tmp = tempfile.mkdtemp()
    failures = 0

    def check(ok, label, detail=""):
        nonlocal failures
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] {label:32s} {detail}")

    a = spawn([str(ROOT / "server" / "cluster_server.py"),
               "--node", "A", "--role", "primary", "--port", str(A_PORT),
               "--peer", f"http://127.0.0.1:{B_PORT}",
               "--db", os.path.join(tmp, "a.db"),
               "--log", os.path.join(tmp, "a.log")])
    b = spawn([str(ROOT / "server" / "cluster_server.py"),
               "--node", "B", "--role", "backup", "--port", str(B_PORT),
               "--peer", f"http://127.0.0.1:{A_PORT}",
               "--db", os.path.join(tmp, "b.db"),
               "--log", os.path.join(tmp, "b.log")])
    try:
        assert wait_for(lambda: leader(A_PORT), 20), "A never became leader"

        client = bank_client.ClusterClient(
            [("127.0.0.1", A_PORT), ("127.0.0.1", B_PORT)], timeout=3.0)

        code, rid, node, raw, attempts = client.request(
            {"action": "deposit", "account": "A1001", "amount": 100,
             "request_id": "c1"})
        check(code == 200 and node == "A", "client reaches the primary",
              f"node={node} attempts={attempts}")

        # Kill the primary; the client keeps the same host list.
        a.kill()
        a.wait(timeout=5)
        assert wait_for(lambda: leader(B_PORT), 25), "B never took over"

        code, rid, node, raw, attempts = client.request(
            {"action": "deposit", "account": "A1001", "amount": 100,
             "request_id": "c2"})
        check(code == 200 and node == "B", "client follows the fail-over",
              f"node={node} attempts={attempts}")
        check(attempts > 1, "it needed retries (as expected)",
              f"attempts={attempts}")

        # Idempotent retry: same id must not apply twice.
        code, rid, node, raw, attempts = client.request(
            {"action": "deposit", "account": "A1001", "amount": 100,
             "request_id": "c2"})
        import json
        balance = json.loads(raw).get("balance")
        check(code == 200 and balance == 1200.0, "same request id is a no-op",
              f"balance={balance}")

        print("PASS: client fail-over verified" if not failures
              else f"{failures} failure(s)")
        return 1 if failures else 0
    finally:
        for p in (a, b):
            try:
                p.kill()
                p.wait(timeout=5)
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())

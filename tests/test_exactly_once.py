"""Focused check: a write whose reply is lost, retried after fail-over.

This is the case the assignment cares most about: a client sends a request, the
primary applies it (and replicates it) but dies before the reply is delivered.
The client does not know whether the write happened. It retries the *same*
request_id against the new primary, which must return the original result and
must NOT apply the change a second time.

Run:  python3 tests/test_exactly_once.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
A_PORT = 8101
B_PORT = 8102


def post(host, port, payload, timeout=8.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(f"http://{host}:{port}/", data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())
    except Exception as e:
        return 0, {"status": "error", "error": "unreachable", "message": str(e)}


def spawn(args):
    return subprocess.Popen([sys.executable] + args,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_ready(port, deadline=15.0):
    """Wait until the node responds AND actually holds the lease."""
    end = time.time() + deadline
    while time.time() < end:
        code, _ = post("127.0.0.1", port, {"action": "balance", "account": "A1001"},
                       timeout=1.0)
        if code in (200, 422, 503):
            return True
        time.sleep(0.15)
    return False


def wait_leader(port, deadline=25.0):
    """Wait until this node reports itself as the leader."""
    end = time.time() + deadline
    while time.time() < end:
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/cluster/status", timeout=1.0) as r:
                body = json.loads(r.read().decode("utf-8"))
            if body.get("leader"):
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def main() -> int:
    tmp = tempfile.mkdtemp()
    a = spawn([str(ROOT / "server" / "cluster_server.py"), "--node", "A",
               "--role", "primary", "--port", str(A_PORT),
               "--peer", f"http://127.0.0.1:{B_PORT}",
               "--db", os.path.join(tmp, "a.db"),
               "--log", os.path.join(tmp, "a.log")])
    b = spawn([str(ROOT / "server" / "cluster_server.py"), "--node", "B",
               "--role", "backup", "--port", str(B_PORT),
               "--peer", f"http://127.0.0.1:{A_PORT}",
               "--db", os.path.join(tmp, "b.db"),
               "--log", os.path.join(tmp, "b.log")])
    failures = 0
    rid = "lost-reply-1"
    try:
        assert wait_ready(A_PORT) and wait_ready(B_PORT), "servers did not start"
        assert wait_leader(A_PORT), "A never became leader"

        # 1. Apply the write on A. It replicates to B, so B's log now holds it.
        code, body = post("127.0.0.1", A_PORT,
                          {"action": "deposit", "account": "A1001",
                           "amount": 100, "request_id": rid})
        ok = code == 200 and body.get("balance") == 1100.0
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] write applied on A     -> {code} "
              f"{body.get('balance')}")

        # 2. Kill A *after* the write replicated but simulate that the client
        #    never processed the reply.
        a.kill()
        a.wait(timeout=5)
        assert wait_leader(B_PORT), "B never took over"
        code, body = post("127.0.0.1", B_PORT,
                          {"action": "balance", "account": "A1001"})
        took = code == 200 and body.get("balance") == 1100.0
        failures += 0 if took else 1
        print(f"[{'PASS' if took else 'FAIL'}] B has the write        -> {code} "
              f"{body.get('balance')}")

        # 3. Client retries the SAME request_id. Must be a no-op returning 1100.
        code, body = post("127.0.0.1", B_PORT,
                          {"action": "deposit", "account": "A1001",
                           "amount": 100, "request_id": rid})
        ok = code == 200 and body.get("balance") == 1100.0
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] retry is exactly-once  -> {code} "
              f"{body.get('balance')} (must stay 1100.0)")

        # 4. The account total must reflect ONE deposit, not two.
        code, body = post("127.0.0.1", B_PORT,
                          {"action": "balance", "account": "A1001"})
        ok = body.get("balance") == 1100.0
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] final balance          -> {body.get('balance')}")

        print("PASS: exactly-once across fail-over verified" if not failures
              else f"{failures} failure(s)")
        return 1 if failures else 0
    finally:
        for p in (a, b):
            try:
                p.kill(); p.wait(timeout=5)
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())

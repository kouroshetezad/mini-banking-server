"""Advanced Bonus demo: primary/backup fail-over.

Starts Server A (primary) and Server B (backup) locally on different ports,
drives traffic, kills A, and checks that B takes over automatically with **no
lost and no duplicated** transaction.

Run:  python3 tests/test_failover.py
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
A_HOST, A_PORT = "127.0.0.1", 8091
B_HOST, B_PORT = "127.0.0.1", 8092


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


def get(host, port, path, timeout=8.0):
    try:
        with urllib.request.urlopen(f"http://{host}:{port}{path}",
                                    timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except Exception as e:
        return 0, {"status": "error", "message": str(e)}


def role_of(host, port):
    _, body = get(host, port, "/cluster/status")
    return body.get("role"), body.get("leader"), body.get("last_index")


def spawn(args):
    return subprocess.Popen([sys.executable] + args,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_ready(host, port, deadline=15.0):
    end = time.time() + deadline
    while time.time() < end:
        code, _ = get(host, port, "/health", timeout=1.0)
        if code == 200:
            return True
        time.sleep(0.15)
    return False


def wait_leader(host, port, deadline=25.0):
    """Wait until the node actually reports itself leader (not merely up)."""
    end = time.time() + deadline
    while time.time() < end:
        _, leader, _ = role_of(host, port)
        if leader:
            return True
        time.sleep(0.3)
    return False


def main() -> int:
    tmp = tempfile.mkdtemp()
    a = spawn([str(ROOT / "server" / "cluster_server.py"),
               "--node", "A", "--role", "primary", "--port", str(A_PORT),
               "--peer", f"http://{B_HOST}:{B_PORT}",
               "--db", os.path.join(tmp, "a.db"),
               "--log", os.path.join(tmp, "a.log")])
    b = spawn([str(ROOT / "server" / "cluster_server.py"),
               "--node", "B", "--role", "backup", "--port", str(B_PORT),
               "--peer", f"http://{A_HOST}:{A_PORT}",
               "--db", os.path.join(tmp, "b.db"),
               "--log", os.path.join(tmp, "b.log")])
    failures = 0
    try:
        assert wait_ready(A_HOST, A_PORT), "A did not start"
        assert wait_ready(B_HOST, B_PORT), "B did not start"
        assert wait_leader(A_HOST, A_PORT), "A never became leader"
        print(f"     roles: A={role_of(A_HOST, A_PORT)} "
              f"B={role_of(B_HOST, B_PORT)}")

        # --- 1. normal traffic through the primary ---------------------------
        rid = "t1-deposit"
        code, body = post(A_HOST, A_PORT,
                          {"action": "deposit", "account": "A1001",
                           "amount": 100, "request_id": rid})
        ok = code == 200 and body.get("balance") == 1100.0
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] primary deposit        -> {code} "
              f"{body.get('balance')}")

        # --- 2. backup rejects writes (only primary may write) ----------------
        code, body = post(B_HOST, B_PORT,
                          {"action": "deposit", "account": "A1001",
                           "amount": 50, "request_id": "t2"})
        ok = code == 503 and body.get("error") == "not_primary"
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] backup refuses write   -> {code} "
              f"{body.get('error')}")

        # --- 3. exact-once: replay the same request_id ------------------------
        code, body = post(A_HOST, A_PORT,
                          {"action": "deposit", "account": "A1001",
                           "amount": 100, "request_id": rid})
        ok = code == 200 and body.get("balance") == 1100.0
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] duplicate id is no-op  -> {code} "
              f"{body.get('balance')}")

        # --- 4. kill the primary, expect B to take over -----------------------
        a.kill()
        a.wait(timeout=5)
        print("     (Server A killed; waiting for B to take over ...)")
        took = wait_leader(B_HOST, B_PORT, deadline=25.0)
        failures += 0 if took else 1
        print(f"[{'PASS' if took else 'FAIL'}] B took over as primary")

        # --- 5. no lost transaction: balance survived the fail-over -----------
        code, body = post(B_HOST, B_PORT,
                          {"action": "balance", "account": "A1001"})
        ok = code == 200 and body.get("balance") == 1100.0
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] no lost write          -> {code} "
              f"{body.get('balance')}")

        # --- 6. new writes work on the new primary ----------------------------
        code, body = post(B_HOST, B_PORT,
                          {"action": "withdraw", "account": "A1001",
                           "amount": 100, "request_id": "t6"})
        ok = code == 200 and body.get("balance") == 1000.0
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] write on new primary   -> {code} "
              f"{body.get('balance')}")

        print("PASS: fail-over verified" if not failures
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

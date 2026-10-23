"""Advanced Bonus under load: kill the primary while clients are writing.

The assignment's demo is not "kill the primary between two requests", it is
"kill the primary while clients are still sending".  Some requests will be in
flight at that instant; the client does not know whether they were executed.

This test runs several concurrent writers, kills Server A mid-stream, lets the
clients retry against Server B with the same request_id, and then checks:

* every client eventually got an answer,
* the final balance equals start + (successful operations x amount),
* each request_id appears exactly once in the replicated log,
* the backup really did take over (A is gone, B is the leader).

Run:  python3 tests/test_failover_under_load.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "client"))

import bank_client  # noqa: E402

A_PORT, B_PORT = 8121, 8122
WRITERS = 6
OPS_PER_WRITER = 5
AMOUNT = 10
START_BALANCE = 1000.0


def spawn(args):
    return subprocess.Popen([sys.executable] + args,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get(port, path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}",
                                    timeout=3) as r:
            return json.loads(r.read())
    except Exception:
        return {}


def wait_for(predicate, timeout, interval=0.25):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(interval)
    return False


def main() -> int:
    tmp = tempfile.mkdtemp()
    failures = 0

    def check(ok, label, detail=""):
        nonlocal failures
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] {label:38s} {detail}")

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

    applied: list[str] = []
    errors: list[str] = []
    lock = threading.Lock()
    started = threading.Event()
    killed_at = [0.0]

    def writer(idx: int):
        client = bank_client.ClusterClient(
            [("127.0.0.1", A_PORT), ("127.0.0.1", B_PORT)], timeout=3.0)
        started.wait()
        for op in range(OPS_PER_WRITER):
            rid = f"w{idx}-{op}"
            code, _, node, raw, attempts = client.request(
                {"action": "deposit", "account": "A1001", "amount": AMOUNT,
                 "request_id": rid}, retries=30, backoff=0.5)
            if code == 200:
                with lock:
                    applied.append(rid)
            else:
                with lock:
                    errors.append(f"{rid}:{code}:{raw[:80]}")

    try:
        assert wait_for(lambda: get(A_PORT, "/cluster/status").get("leader"), 20), \
            "A never became leader"

        threads = [threading.Thread(target=writer, args=(i,), daemon=True)
                   for i in range(WRITERS)]
        for t in threads:
            t.start()
        started.set()

        # Let the writers get going, then kill the primary mid-stream.
        time.sleep(1.2)
        killed_at[0] = time.time()
        a.kill()
        a.wait(timeout=5)
        print(f"     (killed Server A after {WRITERS} writers had been running "
              f"for 1.2s)")

        for t in threads:
            t.join(timeout=60)

        total = WRITERS * OPS_PER_WRITER
        check(len(applied) + len(errors) == total, "every client got an answer",
              f"ok={len(applied)} failed={len(errors)} of {total}")
        check(len(errors) == 0, "no request was lost",
              f"{errors[:2]}" if errors else "all retries succeeded")

        check(wait_for(lambda: get(B_PORT, "/cluster/status").get("leader"), 30),
              "B took over automatically")

        final = get(B_PORT, "/cluster/status").get("accounts", {}).get("A1001")
        expected = START_BALANCE + AMOUNT * len(applied)
        check(final == expected, "final balance is exactly right",
              f"final={final} expected={expected}")

        log = get(B_PORT, "/cluster/log").get("entries", [])
        ids = [e["request_id"] for e in log]
        check(len(ids) == len(set(ids)), "no request applied twice",
              f"{len(ids)} log entries, {len(set(ids))} distinct ids")
        check(set(applied) <= set(ids), "every acknowledged write is in the log",
              f"acked={len(applied)} logged={len(ids)}")

        print("PASS: fail-over under load is exactly-once" if not failures
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

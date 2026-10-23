"""Concurrency demo: many simultaneous withdrawals on the same account.

Starts the server in a subprocess on a temp database, fires N concurrent withdraw
requests of the same amount at one account, then checks that the final balance is
exactly what a serial execution would produce. Also demonstrates that no request
ever drives the balance negative.

Run:  python3 tests/test_concurrency.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOST, PORT = "127.0.0.1", 8099

THREADS = 60
WITHDRAW = 10.00
START_BALANCE = 1000.00  # A1001 seed


def post(payload: dict):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(f"http://{HOST}:{PORT}/", data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def wait_ready(deadline=15.0):
    end = time.time() + deadline
    while time.time() < end:
        try:
            with urllib.request.urlopen(f"http://{HOST}:{PORT}/health", timeout=1):
                return True
        except Exception:
            time.sleep(0.1)
    return False


def main() -> int:
    tmp = tempfile.mkdtemp()
    db = os.path.join(tmp, "bank.db")
    log = os.path.join(tmp, "requests.log")
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "server" / "bank_server.py"),
         "--host", HOST, "--port", str(PORT), "--db", db, "--log", log,
         "--max-clients", str(THREADS + 10)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        assert wait_ready(), "server did not start"
        results = []
        lock = threading.Lock()

        def worker():
            code, body = post({"action": "withdraw", "account": "A1001",
                               "amount": WITHDRAW})
            with lock:
                results.append((code, body))

        threads = [threading.Thread(target=worker) for _ in range(THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        ok = sum(1 for c, b in results if b.get("status") == "success")
        insufficient = sum(1 for c, b in results
                           if b.get("error") == "insufficient_funds")
        _, final = post({"action": "balance", "account": "A1001"})
        final_balance = final["balance"]

        expected = round(START_BALANCE - ok * WITHDRAW, 2)
        print(f"threads={THREADS} succeeded={ok} insufficient={insufficient}")
        print(f"final balance = {final_balance} (expected {expected})")

        assert final_balance == expected, "LOST UPDATE DETECTED"
        assert final_balance >= 0, "NEGATIVE BALANCE"
        assert ok + insufficient == THREADS, "unexpected errors"
        print("PASS: concurrent withdrawals are serialisable, no lost updates")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())

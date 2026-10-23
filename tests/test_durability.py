"""Durability demo: confirm committed transactions survive a restart.

Runs a fresh server on a temp DB, performs deposits/withdrawals, kills the server,
restarts it on the same DB, and checks the balance is preserved.

Run:  python3 tests/test_durability.py
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOST, PORT = "127.0.0.1", 8097


def post(payload, port=PORT):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(f"http://{HOST}:{port}/", data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def wait_ready(deadline=15.0, port=PORT):
    end = time.time() + deadline
    while time.time() < end:
        try:
            with urllib.request.urlopen(f"http://{HOST}:{port}/health", timeout=1):
                return True
        except Exception:
            time.sleep(0.1)
    return False


def start(db, log):
    return subprocess.Popen(
        [sys.executable, str(ROOT / "server" / "bank_server.py"),
         "--host", HOST, "--port", str(PORT), "--db", db, "--log", log],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def hard_kill(proc):
    """Kill the server without any chance to shut down cleanly.

    On POSIX we send SIGKILL (a true hard crash, matching the report). On
    Windows there is no SIGKILL, so we fall back to TerminateProcess via
    ``proc.kill()``, which likewise runs no Python cleanup code.
    """
    if os.name == "posix":
        proc.send_signal(signal.SIGKILL)
    else:
        proc.kill()
    proc.wait(timeout=5)


def main() -> int:
    tmp = tempfile.mkdtemp()
    db = os.path.join(tmp, "bank.db")
    log = os.path.join(tmp, "requests.log")

    proc = start(db, log)
    try:
        assert wait_ready(), "server did not start"
        post({"action": "deposit", "account": "A1002", "amount": 123.45})
        post({"action": "withdraw", "account": "A1002", "amount": 23.45})
        _, body = post({"action": "balance", "account": "A1002"})
        before = body["balance"]
        print(f"balance before crash = {before}")
    finally:
        hard_kill(proc)  # simulate a hard crash
        proc.wait(timeout=5)

    proc = start(db, log)
    try:
        assert wait_ready(), "server did not restart"
        _, body = post({"action": "balance", "account": "A1002"})
        after = body["balance"]
        print(f"balance after restart = {after}")
        assert after == before, "DURABILITY FAILED"
        print("PASS: committed transactions survived the restart")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())

"""Error-handling demo: exercise every required error case.

Run:  python3 tests/test_errors.py
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
HOST, PORT = "127.0.0.1", 8098


def raw_post(raw: bytes):
    req = urllib.request.Request(f"http://{HOST}:{PORT}/", data=raw,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def post(obj):
    return raw_post(json.dumps(obj).encode())


def wait_ready(deadline=15.0):
    end = time.time() + deadline
    while time.time() < end:
        try:
            with urllib.request.urlopen(f"http://{HOST}:{PORT}/health", timeout=1):
                return True
        except Exception:
            time.sleep(0.1)
    return False


CASES = [
    ("unknown account", {"action": "balance", "account": "Z9999"},
     "unknown_account"),
    ("insufficient funds", {"action": "withdraw", "account": "A1003", "amount": 5000},
     "insufficient_funds"),
    ("negative amount", {"action": "withdraw", "account": "A1001", "amount": -50},
     "invalid_amount"),
    ("zero amount", {"action": "deposit", "account": "A1001", "amount": 0},
     "invalid_amount"),
    ("non-numeric amount", {"action": "withdraw", "account": "A1001", "amount": "abc"},
     "invalid_amount"),
    ("unsupported action", {"action": "frobnicate", "account": "A1001", "amount": 10},
     "unsupported_action"),
    ("missing amount", {"action": "withdraw", "account": "A1001"},
     "missing_field"),
    ("missing action", {"account": "A1001"},
     "missing_field"),
]


def main() -> int:
    tmp = tempfile.mkdtemp()
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "server" / "bank_server.py"),
         "--host", HOST, "--port", str(PORT),
         "--db", os.path.join(tmp, "bank.db"),
         "--log", os.path.join(tmp, "requests.log")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    failures = 0
    try:
        assert wait_ready(), "server did not start"
        for name, payload, expected in CASES:
            code, body = post(payload)
            got = body.get("error")
            ok = (got == expected)
            failures += 0 if ok else 1
            print(f"[{'PASS' if ok else 'FAIL'}] {name:20s} "
                  f"-> {code} {got} ({body.get('message')})")

        # Malformed JSON.
        code, body = raw_post(b"{not valid json")
        ok = body.get("error") == "malformed_request"
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] malformed JSON       -> {code} "
              f"{body.get('error')}")

        # Random bytes.
        code, body = raw_post(os.urandom(32))
        ok = body.get("error") == "malformed_request"
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] random bytes         -> {code} "
              f"{body.get('error')}")

        # Server still alive after all the bad input.
        code, _ = post({"action": "balance", "account": "A1001"})
        ok = code == 200
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] server still alive   -> {code}")

        print("PASS: all error cases handled" if not failures
              else f"{failures} failure(s)")
        return 1 if failures else 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())

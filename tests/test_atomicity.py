"""Atomicity demo: a transfer either happens completely or not at all.

Covers the "fails halfway" case the assignment asks to demonstrate: the debit
and the credit must be in one transaction, so a rejected transfer leaves *both*
accounts exactly as they were.

Run:  python3 tests/test_atomicity.py
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
HOST, PORT = "127.0.0.1", 8096


def post(payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(f"http://{HOST}:{PORT}/", data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
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


def balances():
    _, a = post({"action": "balance", "account": "A1001"})
    _, b = post({"action": "balance", "account": "A1002"})
    return a["balance"], b["balance"]


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
        before = balances()          # A1001 = 1000.00, A1002 = 2500.00
        print(f"start          A1001={before[0]} A1002={before[1]}")

        # 1. a valid transfer moves money and keeps the total constant
        code, body = post({"action": "transfer", "account": "A1001",
                           "to": "A1002", "amount": 250})
        ok = (code == 200 and body.get("balance") == 750.0
              and body.get("to_balance") == 2750.0)
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] transfer 250      -> {code} "
              f"{body.get('balance')} / {body.get('to_balance')}")
        ok = sum(balances()) == sum(before)
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] total unchanged     -> {sum(balances())}")

        # 2. insufficient funds: the source must not be debited at all
        mid = balances()
        code, body = post({"action": "transfer", "account": "A1001",
                           "to": "A1002", "amount": 5000})
        ok = code == 422 and body.get("error") == "insufficient_funds"
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] transfer 5000       -> {code} "
              f"{body.get('error')}")
        ok = balances() == mid
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] both unchanged      -> {balances()}")

        # 3. unknown destination: source must not be debited either
        code, body = post({"action": "transfer", "account": "A1001",
                           "to": "Z9999", "amount": 100})
        ok = code == 422 and body.get("error") == "unknown_account"
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] bad destination     -> {code} "
              f"{body.get('error')}")
        ok = balances() == mid
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] both unchanged      -> {balances()}")

        # 4. transfer to self is rejected
        code, body = post({"action": "transfer", "account": "A1001",
                           "to": "A1001", "amount": 10})
        ok = code == 422 and body.get("error") == "invalid_transfer"
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] transfer to self    -> {code} "
              f"{body.get('error')}")

        # 5. a rejected withdrawal leaves the balance untouched
        mid = balances()
        code, body = post({"action": "withdraw", "account": "A1001",
                           "amount": 999999})
        ok = code == 422 and balances() == mid
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] failed withdraw     -> {code} "
              f"{body.get('error')}, unchanged={balances() == mid}")

        print("PASS: atomicity verified" if not failures
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

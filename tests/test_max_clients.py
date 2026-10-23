"""The max-simultaneous-clients limit is configurable and actually enforced.

The limit is applied when the connection is accepted, so the (max_clients+1)-th
*connected* client is refused immediately with 503 instead of being queued or
spawned anyway.

Run:  python3 tests/test_max_clients.py
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOST, PORT = "127.0.0.1", 8095
MAX_CLIENTS = 2


def wait_ready(deadline=15.0):
    end = time.time() + deadline
    while time.time() < end:
        try:
            with urllib.request.urlopen(f"http://{HOST}:{PORT}/health", timeout=1):
                return True
        except Exception:
            time.sleep(0.1)
    return False


def request():
    payload = json.dumps({"action": "balance", "account": "A1001"}).encode()
    req = urllib.request.Request(f"http://{HOST}:{PORT}/", data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())
    except Exception as e:
        return 0, {"error": "connection_error", "message": str(e)}


def wait_status(wanted: int, timeout: float = 8.0):
    """Poll until the server answers with ``wanted`` (slots are freed lazily)."""
    end = time.time() + timeout
    last = None
    while time.time() < end:
        last = request()
        if last[0] == wanted:
            return last
        time.sleep(0.2)
    return last


def main() -> int:
    tmp = tempfile.mkdtemp()
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "server" / "bank_server.py"),
         "--host", HOST, "--port", str(PORT),
         "--max-clients", str(MAX_CLIENTS),
         "--db", os.path.join(tmp, "bank.db"),
         "--log", os.path.join(tmp, "requests.log")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    held = []
    failures = 0
    try:
        assert wait_ready(), "server did not start"

        # Occupy every slot with connections that stay open and send nothing.
        for _ in range(MAX_CLIENTS):
            s = socket.create_connection((HOST, PORT), timeout=3)
            held.append(s)
        time.sleep(0.5)

        code, body = request()
        ok = code == 503 and body.get("error") == "server_busy"
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] {MAX_CLIENTS + 1}th client is refused "
              f"-> {code} {body.get('error')}")

        # Free one slot: service must resume (the handler thread releases it).
        held.pop().close()
        code, body = wait_status(200)
        ok = code == 200
        failures += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] slot freed, service resumes -> {code}")

        print("PASS: max_clients enforced at connection level" if not failures
              else f"{failures} failure(s)")
        return 1 if failures else 0
    finally:
        for s in held:
            try:
                s.close()
            except OSError:
                pass
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())

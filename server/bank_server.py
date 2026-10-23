"""Mini Banking Transaction Server.

An HTTP server that exposes a small banking API and logs every request/response.

Concurrency model:
  * ``LimitedThreadingHTTPServer`` runs one thread per client connection.
  * A ``BoundedSemaphore`` enforces the configured maximum number of *simultaneous
    clients*; connections beyond the limit are rejected with HTTP 503 rather than
    silently queued forever.
  * Shared state (the :class:`~bank.Bank`) is safe for concurrent handlers.

Run:
    python3 server/bank_server.py --host 0.0.0.0 --port 8080 --max-clients 50
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler
from pathlib import Path

# Allow running as a script from the repository root or from server/.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from bank import Bank, BankError  # noqa: E402
from limits import LimitedThreadingHTTPServer  # noqa: E402

DEFAULT_CONFIG = {
    "host": "0.0.0.0",
    "port": 8080,
    "max_clients": 50,
    "db_path": "server/bank.db",
    "log_path": "server/requests.log",
}

SUPPORTED_ACTIONS = {"balance", "deposit", "withdraw", "transfer"}

# Populated in main().
BANK: Bank
CONFIG: dict
REQ_LOG = logging.getLogger("bank.requests")


def load_config(argv=None) -> dict:
    parser = argparse.ArgumentParser(description="Mini Banking Transaction Server")
    parser.add_argument("--config", default=None, help="path to a JSON config file")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--max-clients", type=int, default=None)
    parser.add_argument("--db", dest="db_path", default=None)
    parser.add_argument("--log", dest="log_path", default=None)
    args = parser.parse_args(argv)

    cfg = dict(DEFAULT_CONFIG)
    if args.config and Path(args.config).exists():
        with open(args.config, "r", encoding="utf-8") as fh:
            cfg.update(json.load(fh))

    # Precedence: defaults < config file < environment < CLI flags.
    env_map = {
        "BANK_HOST": "host",
        "BANK_PORT": "port",
        "BANK_MAX_CLIENTS": "max_clients",
        "BANK_DB": "db_path",
        "BANK_LOG": "log_path",
    }
    for env_key, cfg_key in env_map.items():
        if env_key in os.environ:
            cfg[cfg_key] = os.environ[env_key]

    for key in ("host", "port", "max_clients", "db_path", "log_path"):
        val = getattr(args, key)
        if val is not None:
            cfg[key] = val

    cfg["port"] = int(cfg["port"])
    cfg["max_clients"] = int(cfg["max_clients"])
    return cfg


def setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    REQ_LOG.setLevel(logging.INFO)
    REQ_LOG.handlers.clear()
    REQ_LOG.addHandler(handler)
    # Also echo to stderr so the demo video shows live traffic.
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(logging.Formatter("%(message)s"))
    REQ_LOG.addHandler(stream)
    REQ_LOG.propagate = False


def iso_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + \
        f".{int((time.time() % 1) * 1000):03d}Z"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MiniBank/1.0"
    timeout = 30          # an idle connection must not hold a client slot forever

    # -- logging ---------------------------------------------------------------

    def log_message(self, fmt, *args):  # silence default stderr access log
        pass

    # -- helpers ---------------------------------------------------------------

    def _send_json(self, code: int, payload: dict, request_id: str) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Request-Id", request_id)
        self.end_headers()
        self.wfile.write(body)

    # -- request entry points --------------------------------------------------

    def do_GET(self):  # noqa: N802
        request_id = uuid.uuid4().hex[:8]
        client_ip = self.client_address[0]
        started = time.perf_counter()
        if self.path.rstrip("/") in ("/health", "/healthz"):
            self._send_json(200, {"status": "ok"}, request_id)
            self._log(request_id, client_ip, "health", "-", "success",
                      started, 200)
            return
        self._send_json(404, {"status": "error", "error": "not_found",
                              "message": "use POST / with a JSON body"}, request_id)
        self._log(request_id, client_ip, "unknown", "-", "error:not_found",
                  started, 404)

    def do_POST(self):  # noqa: N802
        request_id = uuid.uuid4().hex[:8]
        client_ip = self.client_address[0]
        started = time.perf_counter()
        # The max-simultaneous-clients limit is enforced by the server when the
        # connection is accepted (see limits.py), so a client that gets this far
        # already owns a slot.
        self._handle_post(request_id, client_ip, started)

    def _handle_post(self, request_id: str, client_ip: str, started: float) -> None:
        # Read the body (malformed input never crashes the server).
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""

        try:
            request = json.loads(raw.decode("utf-8")) if raw else None
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json(400, {"status": "error", "error": "malformed_request",
                                  "message": "body is not valid JSON"}, request_id)
            self._log(request_id, client_ip, "-", "-", "error:malformed_request",
                      started, 400)
            return

        if not isinstance(request, dict):
            self._send_json(400, {"status": "error", "error": "malformed_request",
                                  "message": "request must be a JSON object"},
                            request_id)
            self._log(request_id, client_ip, "-", "-", "error:malformed_request",
                      started, 400)
            return

        action = request.get("action")
        account = request.get("account", "-")
        if not isinstance(account, str):
            account = "-"

        try:
            code, response = self._dispatch(request)
        except BankError as exc:
            code, response = 422, {"status": "error", "error": exc.code,
                                   "message": exc.message, **exc.extra}
        except Exception as exc:  # never let a bug kill the worker
            logging.exception("unhandled error")
            code, response = 500, {"status": "error", "error": "internal_error",
                                   "message": str(exc)}

        self._send_json(code, response, request_id)
        result = response.get("status", "error")
        if result == "error":
            result = f"error:{response.get('error', 'unknown')}"
        self._log(request_id, client_ip, action or "-", account, result,
                  started, code, response.get("balance"))

    def _dispatch(self, request: dict):
        action = request.get("action")
        if not action or not isinstance(action, str):
            raise BankError("missing_field", "action is required")
        if action not in SUPPORTED_ACTIONS:
            raise BankError("unsupported_action", f"unsupported action: {action}")
        if "account" not in request or not request.get("account"):
            raise BankError("missing_field", "account is required")
        account = request["account"]

        if action == "balance":
            res = BANK.balance(account)
        elif action == "deposit":
            if "amount" not in request:
                raise BankError("missing_field", "amount is required")
            res = BANK.deposit(account, request["amount"])
        elif action == "withdraw":
            if "amount" not in request:
                raise BankError("missing_field", "amount is required")
            res = BANK.withdraw(account, request["amount"])
        elif action == "transfer":
            if "amount" not in request:
                raise BankError("missing_field", "amount is required")
            if "to" not in request:
                raise BankError("missing_field", "'to' is required for a transfer")
            moved = BANK.transfer(account, request["to"], request["amount"])
            return 200, {"status": "success", "action": "transfer",
                         "account": moved.account, "balance": moved.balance,
                         "to": moved.to, "to_balance": moved.to_balance}
        else:  # pragma: no cover - guarded above
            raise BankError("unsupported_action", f"unsupported action: {action}")

        return 200, {"status": "success", "account": res.account,
                     "balance": res.balance}

    def _log(self, request_id, client_ip, action, account, result, started,
             code, balance=None):
        latency_ms = (time.perf_counter() - started) * 1000
        entry = {
            "ts": iso_timestamp(),
            "req": request_id,
            "ip": client_ip,
            "action": action,
            "account": account,
            "result": result,
            "http": code,
            "latency_ms": round(latency_ms, 3),
        }
        if balance is not None:
            entry["balance"] = balance
        REQ_LOG.info(json.dumps(entry))


def main(argv=None) -> int:
    global BANK, CONFIG

    CONFIG = load_config(argv)
    setup_logging(CONFIG["log_path"])

    BANK = Bank(CONFIG["db_path"])

    httpd = LimitedThreadingHTTPServer(
        (CONFIG["host"], CONFIG["port"]), Handler, CONFIG["max_clients"]
    )

    REQ_LOG.info(json.dumps({
        "ts": iso_timestamp(),
        "event": "server_start",
        "host": CONFIG["host"],
        "port": CONFIG["port"],
        "max_clients": CONFIG["max_clients"],
        "db": CONFIG["db_path"],
        "accounts": BANK.snapshot(),
    }))

    stop = threading.Event()

    def shutdown(signum, frame):
        if stop.is_set():
            return
        stop.set()
        REQ_LOG.info(json.dumps({"ts": iso_timestamp(), "event": "server_stop",
                                 "signal": signum}))
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
        BANK.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

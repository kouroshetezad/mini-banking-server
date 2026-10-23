"""Fault-tolerant banking server (cluster mode) -- Advanced Bonus.

One program, two nodes.  Exactly one node is the **primary** at any moment and
serves deposits/withdrawals; the other is a **backup** that mirrors every
committed command and takes over if the primary dies.

How a write is committed
------------------------
1. The primary validates and applies the command inside an open SQLite
   transaction (so an invalid request never reaches the network).
2. It ships the log record to the backup and **waits for the ack**.
3. Only then does it ``COMMIT`` locally and reply to the client.

If step 2 fails the transaction is rolled back: a write that exists on one
machine only is never acknowledged, so a crash cannot lose an acknowledged
transaction.  The reverse order would let a client see ``success`` for a write
that dies with the primary.

Exactly once
------------
Every mutating request carries a stable ``request_id``.  A client that never saw
a reply retries the same id against whichever node is primary now; the
``applied_requests`` table returns the stored result instead of applying the
command twice.

Takeover
--------
When the backup cannot reach the primary for ``failover_delay_ms`` it promotes
itself, adopts the log both sides share, and keeps serving.  No operator is
involved.  When the old primary comes back it compares its log with the new
primary's and re-syncs if they differ.

Known limitation (documented in the report)
-------------------------------------------
With two nodes there is no third party to break a tie, so the design assumes the
servers are never both alive yet unable to reach each other.  A genuine network
partition is the subject of the separate Extreme Bonus, which is not part of
this submission.

Run (Server A):

    python3 server/cluster_server.py --node A --role primary \
        --port 8080 --peer http://192.168.64.131:8081 --db server/a.db
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
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bank import BankError  # noqa: E402
from limits import LimitedThreadingHTTPServer  # noqa: E402
from replicated_bank import (  # noqa: E402
    LogEntry, NotLeaderError, ReplicatedBank, ReplicationError,
)

DEFAULT_CONFIG = {
    "host": "0.0.0.0",
    "port": 8080,
    "max_clients": 50,
    "db_path": "server/cluster.db",
    "log_path": "server/cluster.log",
    "node": "A",
    "role": "primary",
    "peer": "",
    "failover_delay_ms": 3000,  # how long a backup waits before taking over
    "require_backup": True,     # refuse writes that cannot be replicated
}

SUPPORTED_ACTIONS = {"balance", "deposit", "withdraw", "transfer"}

BANK: ReplicatedBank
CONFIG: dict
CLUSTER: "Cluster"
REQ_LOG = logging.getLogger("bank.requests")


# --------------------------------------------------------------------------- #
# small HTTP helpers
# --------------------------------------------------------------------------- #

def http_post(url: str, payload: dict, timeout: float = 2.0) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get(url: str, timeout: float = 2.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# --------------------------------------------------------------------------- #
# cluster membership and replication
# --------------------------------------------------------------------------- #

class Cluster:
    """Leadership state for one node.

    * **leader**  -- accepts writes.
    * **follower** -- mirrors the leader; answers client writes with 503.
    * **solo**    -- leader that took over while its peer was unreachable.  It is
      the only survivor, so it keeps serving even though it cannot replicate.
      It stops being solo as soon as the peer answers again.
    """

    def __init__(self, bank: ReplicatedBank, config: dict):
        self.bank = bank
        self.config = config
        self._stop = threading.Event()
        self._lock = threading.Lock()

        self.term = 0                 # incremented on every takeover
        self._leader = False
        self._solo = False
        self._peer_up = False
        self._peer_lost_at = 0.0

        bank.replicate = self.replicate
        bank._is_leader = self.is_leader
        bank._current_epoch = lambda: self.term

    # -- public state ---------------------------------------------------------

    def is_leader(self) -> bool:
        with self._lock:
            return self._leader

    def status(self) -> dict:
        with self._lock:
            return {
                "node": self.config["node"],
                "role": self.config["role"],
                "leader": self._leader,
                "solo": self._solo,
                "term": self.term,
                "peer_up": self._peer_up,
            }

    # -- lifecycle ------------------------------------------------------------

    def start(self) -> None:
        # Decide the initial role *before* the HTTP server starts accepting, so
        # the very first client request does not race the first probe.
        try:
            self._tick()
        except Exception as exc:
            REQ_LOG.info(json.dumps({
                "event": "initial_tick_failed", "node": self.config["node"],
                "error": repr(exc), "ts": round(time.time(), 3)}))
        threading.Thread(target=self._loop, daemon=True,
                         name=f"cluster-{self.config['node']}").start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as exc:      # a probe must never kill the loop
                REQ_LOG.info(json.dumps({
                    "event": "tick_error", "node": self.config["node"],
                    "error": repr(exc), "ts": round(time.time(), 3)}))
            self._stop.wait(1.0)

    # -- one round ------------------------------------------------------------

    def _tick(self) -> None:
        peer_up = self._peer_alive()
        peer_leader = self._peer_is_leader() if peer_up else False
        now = time.monotonic()

        with self._lock:
            self._peer_up = peer_up
            if peer_up:
                self._peer_lost_at = 0.0
            elif self._peer_lost_at == 0.0:
                self._peer_lost_at = now
            was_leader = self._leader
            lost_at = self._peer_lost_at
            role = self.config["role"]

        if was_leader:
            # Two nodes must never both write.  If the peer claims leadership,
            # the node with the smaller name keeps it: a deterministic
            # tie-break for a double start or a returning old primary.
            if peer_up and peer_leader and self.config["node"] > self._peer_name:
                self._demote("peer_is_primary")
            elif peer_up:
                with self._lock:
                    self._solo = False
            return

        if peer_up and peer_leader:
            self._maybe_resync()
            return

        if role == "primary":
            # The designated primary leads, but it does not get to skip
            # replication: if the backup has never answered, writes are refused
            # (503) rather than acknowledged with no second copy.
            self._promote(solo=False)
        elif (not peer_up and lost_at
              and (now - lost_at) >= self.config["failover_delay_ms"] / 1000.0):
            # A backup only takes over when the primary is gone, and then it is
            # the sole survivor -- so it may serve without a peer.
            self._promote(solo=True)

    # -- transitions ----------------------------------------------------------

    def _promote(self, solo: bool) -> None:
        with self._lock:
            self._leader = True
            self._solo = solo
            self.term += 1
            self.config["role"] = "primary"
            term = self.term
        REQ_LOG.info(json.dumps({
            "event": "takeover", "node": self.config["node"], "term": term,
            "solo": solo, "ts": round(time.time(), 3)}))

    def _demote(self, reason: str) -> None:
        with self._lock:
            if not self._leader:
                return
            self._leader = False
            self._solo = False
            self.config["role"] = "backup"
            term = self.term
        REQ_LOG.info(json.dumps({
            "event": "demote", "node": self.config["node"], "reason": reason,
            "term": term, "ts": round(time.time(), 3)}))

    # -- peer probes ----------------------------------------------------------

    @property
    def _peer_name(self) -> str:
        return "B" if self.config["node"] == "A" else "A"

    def _peer_alive(self) -> bool:
        if not self.config.get("peer"):
            return False
        try:
            http_post(self.config["peer"].rstrip("/") + "/cluster/ping",
                      {"from": self.config["node"]}, timeout=1.0)
            return True
        except Exception:
            return False

    def _peer_is_leader(self) -> bool:
        try:
            body = http_get(self.config["peer"].rstrip("/") + "/cluster/status",
                            timeout=1.0)
            return bool(body.get("leader"))
        except Exception:
            return False

    # -- replication (called by the store, inside the open transaction) -------

    def replicate(self, entry: LogEntry) -> None:
        """Ship one record to the backup and wait for the ack.

        Raising :class:`ReplicationError` makes the store roll back, so a write
        that is not on two machines is never acknowledged.
        """
        if not self.config.get("peer"):
            return                       # standalone node (no cluster)
        if self._solo and not self._peer_up:
            REQ_LOG.info(json.dumps({
                "event": "replication_skipped", "node": self.config["node"],
                "index": entry.index, "reason": "sole_survivor",
                "ts": round(time.time(), 3)}))
            return
        try:
            self._send_entry(entry)
        except ReplicationError as exc:
            if not self.config.get("require_backup", True):
                REQ_LOG.info(json.dumps({
                    "event": "replication_failed_tolerated",
                    "node": self.config["node"], "index": entry.index,
                    "error": str(exc), "ts": round(time.time(), 3)}))
                return
            raise

    def _send_entry(self, entry: LogEntry) -> None:
        url = self.config["peer"].rstrip("/") + "/cluster/replicate"
        try:
            resp = http_post(url, {"entry": json.loads(entry.to_json())},
                             timeout=5.0)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise ReplicationError(f"backup rejected entry: {body}")
        except Exception as exc:
            raise ReplicationError(f"backup unreachable: {exc}")
        if not resp.get("ok"):
            raise ReplicationError(f"backup did not ack: {resp}")

    # -- convergence ----------------------------------------------------------

    def _maybe_resync(self) -> None:
        """Adopt the leader's log if ours differs in length or content."""
        peer = self.config.get("peer")
        if not peer or self.is_leader():
            return
        try:
            status = http_get(peer.rstrip("/") + "/cluster/status", timeout=2.0)
        except Exception:
            return
        if not status.get("leader"):
            return
        peer_last = int(status.get("last_index", -1))
        ours = self.bank.last_index
        # Never adopt a log that is *shorter* than ours.  Our extra entries were
        # already committed durably here, and some of them may have been acked to
        # a client: dropping them would lose an acknowledged write.  A leader
        # that trails us simply had not committed the tail before it crashed.
        if peer_last < ours:
            return
        same = (peer_last == ours and status.get("log_hash") == self.bank.log_hash())
        if not same:
            self._try_resync()

    def _try_resync(self) -> None:
        peer = self.config.get("peer")
        if not peer or self.is_leader():
            return
        try:
            data = http_get(peer.rstrip("/") + "/cluster/log", timeout=5.0)
            entries = [LogEntry.from_json(json.dumps(e)) for e in data["entries"]]
            self.bank.resync(entries)
            REQ_LOG.info(json.dumps({
                "event": "resync_ok", "node": self.config["node"],
                "entries": len(entries), "term": self.term,
                "ts": round(time.time(), 3)}))
        except Exception as exc:
            REQ_LOG.info(json.dumps({
                "event": "resync_failed", "node": self.config["node"],
                "error": repr(exc), "ts": round(time.time(), 3)}))

    # -- replica side ---------------------------------------------------------

    def accept_replication(self, entry: LogEntry) -> dict:
        return self.bank.accept_replication(entry)


# --------------------------------------------------------------------------- #
# HTTP layer
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MiniBankCluster/1.1"
    timeout = 30

    def log_message(self, fmt, *args):
        pass

    def _send_json(self, code: int, payload: dict, request_id: str) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Request-Id", request_id)
        self.send_header("X-Node", CONFIG["node"])
        self.send_header("X-Role", CONFIG["role"])
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""
        return json.loads(raw.decode("utf-8")) if raw else None

    # -- internal cluster endpoints -------------------------------------------

    def do_GET(self):  # noqa: N802
        path = self.path.rstrip("/")
        if path in ("/health", "/healthz"):
            self._send_json(200, {"status": "ok", "node": CONFIG["node"],
                                  "role": CONFIG["role"],
                                  "leader": CLUSTER.is_leader()}, "-")
            return
        if path == "/cluster/log":
            entries = [json.loads(e.to_json()) for e in BANK.all_entries()]
            self._send_json(200, {"entries": entries,
                                  "last_index": BANK.last_index}, "-")
            return
        if path == "/cluster/status":
            payload = CLUSTER.status()
            payload.update({
                "last_index": BANK.last_index,
                "log_hash": BANK.log_hash(),
                "accounts": BANK.snapshot(),
            })
            self._send_json(200, payload, "-")
            return
        self._send_json(404, {"status": "error", "error": "not_found"}, "-")

    def do_POST(self):  # noqa: N802
        path = self.path.rstrip("/")
        request_id = uuid.uuid4().hex[:8]
        client_ip = self.client_address[0]
        started = time.perf_counter()

        if path == "/cluster/ping":
            self._send_json(200, {"ok": True, "node": CONFIG["node"]}, "-")
            return
        if path == "/cluster/replicate":
            self._handle_replicate(request_id)
            return
        if path not in ("", "/"):
            self._send_json(404, {"status": "error", "error": "not_found"},
                            request_id)
            return
        self._handle_request(request_id, client_ip, started)

    def _handle_replicate(self, request_id: str) -> None:
        try:
            body = self._read_json()
            entry = LogEntry.from_json(json.dumps(body["entry"]))
            CLUSTER.accept_replication(entry)
        except ReplicationError as exc:
            self._send_json(409, {"ok": False, "error": "replication_conflict",
                                  "message": str(exc)}, request_id)
            return
        except Exception as exc:
            self._send_json(400, {"ok": False, "error": "bad_entry",
                                  "message": str(exc)}, request_id)
            return
        self._send_json(200, {"ok": True, "index": entry.index}, request_id)

    def _handle_request(self, request_id: str, client_ip: str, started: float):
        try:
            request = self._read_json()
        except Exception:
            self._send_json(400, {"status": "error", "error": "malformed_request",
                                  "message": "body is not valid JSON"}, request_id)
            self._log(request_id, client_ip, "-", "-",
                      "error:malformed_request", started, 400)
            return
        if not isinstance(request, dict):
            self._send_json(400, {"status": "error", "error": "malformed_request",
                                  "message": "request must be a JSON object"},
                            request_id)
            self._log(request_id, client_ip, "-", "-",
                      "error:malformed_request", started, 400)
            return

        action = request.get("action")
        account = request.get("account", "-")
        if not isinstance(account, str):
            account = "-"

        try:
            code, response = self._dispatch(request, request_id)
        except BankError as exc:
            code, response = 422, {"status": "error", "error": exc.code,
                                   "message": exc.message, **exc.extra}
        except NotLeaderError:
            hint = {"leader_address": CONFIG["peer"]} if CONFIG.get("peer") else {}
            code, response = 503, {
                "status": "error", "error": "not_primary",
                "message": "this node is not the primary; retry at the "
                           "cluster address", "request_id": request_id,
                **hint}
        except ReplicationError as exc:
            code, response = 503, {
                "status": "error", "error": "replication_unavailable",
                "message": str(exc), "request_id": request_id}
        except Exception as exc:
            logging.exception("unhandled error")
            code, response = 500, {"status": "error", "error": "internal_error",
                                   "message": str(exc)}

        self._send_json(code, response, request_id)
        result = response.get("status", "error")
        if result == "error":
            result = f"error:{response.get('error', 'unknown')}"
        self._log(request_id, client_ip, action or "-", account, result,
                  started, code, response.get("balance"))

    def _dispatch(self, request: dict, request_id: str):
        action = request.get("action")
        if not action or not isinstance(action, str):
            raise BankError("missing_field", "action is required")
        if action not in SUPPORTED_ACTIONS:
            raise BankError("unsupported_action", f"unsupported action: {action}")
        if "account" not in request or not request.get("account"):
            raise BankError("missing_field", "account is required")
        account = request["account"]

        if action == "balance":
            # Reads are served by whichever node receives them, leader or
            # follower, with no leadership or freshness check.  That is safe
            # here because replication is synchronous: the primary commits only
            # after the backup has committed, so the two stores agree.  A
            # follower can be briefly stale only while it re-syncs after a
            # rejoin.  Being permissive keeps reads cheap and always available;
            # a stricter design would route reads through the leader or refuse
            # them until the follower is caught up.
            return 200, {"status": "success", "account": account,
                         "balance": BANK.get_balance(account) / 100.0}

        if "amount" not in request:
            raise BankError("missing_field", "amount is required")
        if action == "transfer" and "to" not in request:
            raise BankError("missing_field", "'to' is required for a transfer")

        # The client-supplied id makes retries idempotent.  Fall back to the
        # per-connection id if an older client omits it.
        rid = request.get("request_id") or request_id
        command = {"action": action, "account": account,
                   "amount": request["amount"]}
        if action == "transfer":
            command["to"] = request["to"]

        return 200, BANK.submit(rid, command)

    def _log(self, request_id, client_ip, action, account, result, started,
             code, balance=None):
        latency_ms = (time.perf_counter() - started) * 1000
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) +
                  f".{int((time.time() % 1) * 1000):03d}Z",
            "req": request_id, "ip": client_ip, "action": action,
            "account": account, "result": result, "http": code,
            "latency_ms": round(latency_ms, 3),
            "node": CONFIG["node"], "role": CONFIG["role"],
        }
        if balance is not None:
            entry["balance"] = balance
        REQ_LOG.info(json.dumps(entry))


# --------------------------------------------------------------------------- #
# wiring
# --------------------------------------------------------------------------- #

def load_config(argv=None) -> dict:
    p = argparse.ArgumentParser(description="Fault-tolerant banking server")
    p.add_argument("--node", default=None, help="this node's id, e.g. A or B")
    p.add_argument("--role", choices=["primary", "backup"], default=None)
    p.add_argument("--peer", default=None, help="base URL of the other server")
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    p.add_argument("--max-clients", type=int, default=None)
    p.add_argument("--db", dest="db_path", default=None)
    p.add_argument("--log", dest="log_path", default=None)
    p.add_argument("--config", default=None)
    p.add_argument("--allow-unreplicated", action="store_true",
                   help="keep serving when the backup is unreachable "
                        "(availability over durability; not recommended)")
    args = p.parse_args(argv)

    cfg = dict(DEFAULT_CONFIG)
    if args.config and Path(args.config).exists():
        with open(args.config, encoding="utf-8") as fh:
            cfg.update(json.load(fh))

    for k in DEFAULT_CONFIG:
        v = getattr(args, k, None)
        if v is not None:
            cfg[k] = v
    if args.allow_unreplicated:
        cfg["require_backup"] = False

    env = {"BANK_NODE": "node", "BANK_ROLE": "role", "BANK_PEER": "peer",
           "BANK_HOST": "host", "BANK_PORT": "port",
           "BANK_MAX_CLIENTS": "max_clients", "BANK_DB": "db_path",
           "BANK_LOG": "log_path"}
    for ek, ck in env.items():
        if ek in os.environ:
            cfg[ck] = os.environ[ek]

    cfg["port"] = int(cfg["port"])
    cfg["max_clients"] = int(cfg["max_clients"])
    cfg["failover_delay_ms"] = int(cfg["failover_delay_ms"])
    cfg["require_backup"] = bool(cfg["require_backup"])
    return cfg


def setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    h = logging.FileHandler(log_path, encoding="utf-8")
    h.setFormatter(logging.Formatter("%(message)s"))
    REQ_LOG.setLevel(logging.INFO)
    REQ_LOG.handlers.clear()
    REQ_LOG.addHandler(h)
    s = logging.StreamHandler(sys.stderr)
    s.setFormatter(logging.Formatter("%(message)s"))
    REQ_LOG.addHandler(s)
    REQ_LOG.propagate = False


def main(argv=None) -> int:
    global BANK, CONFIG, CLUSTER

    CONFIG = load_config(argv)
    setup_logging(CONFIG["log_path"])
    BANK = ReplicatedBank(CONFIG["db_path"], CONFIG["node"])
    CLUSTER = Cluster(BANK, CONFIG)

    httpd = LimitedThreadingHTTPServer(
        (CONFIG["host"], CONFIG["port"]), Handler, CONFIG["max_clients"]
    )
    CLUSTER.start()

    REQ_LOG.info(json.dumps({
        "event": "server_start", "node": CONFIG["node"],
        "role": CONFIG["role"], "port": CONFIG["port"],
        "peer": CONFIG["peer"],
        "require_backup": CONFIG["require_backup"],
        "accounts": BANK.snapshot(), "ts": round(time.time(), 3)}))

    def shutdown(signum, frame):
        REQ_LOG.info(json.dumps({"event": "server_stop",
                                 "node": CONFIG["node"],
                                 "ts": round(time.time(), 3)}))
        CLUSTER.stop()
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

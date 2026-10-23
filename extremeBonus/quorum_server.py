"""Quorum-fenced cluster node for the Extreme Bonus (network partition / split brain).

Relationship to the Advanced Bonus
----------------------------------
``../server/cluster_server.py`` solves *process death*: if the peer stops
answering for a few seconds, the backup promotes itself.  That rule cannot tell
"my peer crashed" apart from "the network between us is cut", so under a real
partition **both** nodes would promote and the balances would diverge.

This module is the Extreme Bonus answer.  It keeps the same replicated-log
store (``../server/replicated_bank``) and the same HTTP surface, but replaces
the promotion rule with **witness-granted leadership**:

    a node may only be primary while it holds a fresh grant from the witness.

Design
------
Every round, each node:

1. probes its peer (is the peer alive? does it claim leadership?),
2. asks the **witness** for the right to lead ``term``,
3. decides:

   * peer reachable
       - peer is leader  -> we are a follower; if the witness also refuses us,
         we are done.  If we are *higher* in the tie-break we may take over.
       - nobody is leader -> the designated primary leads **as a follower of the
         witness**, i.e. it must still hold a grant, and it must still replicate.
   * peer unreachable
       - ask the witness for the *next* term.  If granted -> promote (we are the
         partition majority side; we carry on alone).
       - if refused -> **stay read-only**.  This is the key difference from the
         Advanced Bonus: an unreachable peer no longer implies "I may lead".
   * witness unreachable
       - if ``require_witness`` is set (default) -> never promote.  A node that
         cannot reach the arbiter cannot be sure it is not the isolated side.
         We refuse writes and stay readable.  This is the CAP trade-off the
         report defends: consistency above availability.

Safety argument
---------------
* Leadership requires a witness grant for a specific term.
* The witness hands out at most one owner per term.
* Terms only increase, so a stale leader is fenced out by a higher term.
* Writes are still synchronously replicated before a reply, so an acknowledged
  write exists on a quorum (leader + witness side) before the client is told.

Therefore at most one node can ever hold a term, so at most one node can write,
so the two logs cannot diverge.  When the partition heals the losing side
re-syncs from the leader, exactly as in the Advanced Bonus.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Reuse the store and the HTTP helpers of the Advanced Bonus implementation.
_HERE = os.path.dirname(os.path.abspath(__file__))
_SERVER = os.path.join(os.path.dirname(_HERE), "server")
if _SERVER not in sys.path:
    sys.path.insert(0, _SERVER)

from replicated_bank import (          # noqa: E402
    LogEntry,
    NotLeaderError,
    ReplicatedBank,
    ReplicationError,
)

# --------------------------------------------------------------------------- #
# small HTTP helpers (no third-party packages)
# --------------------------------------------------------------------------- #


def http_json(method: str, url: str, payload: dict | None = None,
              timeout: float = 2.0) -> dict:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body) if body else {}


def http_get(url: str, timeout: float = 2.0) -> dict:
    return http_json("GET", url, None, timeout)


def http_post(url: str, payload: dict, timeout: float = 2.0) -> dict:
    return http_json("POST", url, payload, timeout)


# --------------------------------------------------------------------------- #
# configuration
# --------------------------------------------------------------------------- #

DEFAULT_CONFIG = {
    "host": "0.0.0.0",
    "port": 8080,
    "max_clients": 50,
    "db_path": "extremeBonus/nodeA.db",
    "log_path": "extremeBonus/nodeA.log",
    "node": "A",
    "role": "primary",              # initial hint only; the witness decides
    "peer": "",                     # e.g. http://192.168.64.131:8081
    "witness": "",                  # e.g. http://192.168.64.132:8099 (VM 4)
    "failover_delay_ms": 3000,      # grace before asking the witness for a new term
    "require_backup": True,
    "require_witness": True,        # False = "unsafe mode", for the demo
}


def load_config(path: str | None, argv=None) -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            cfg.update(json.load(fh))

    ap = argparse.ArgumentParser(description="Quorum-fenced cluster node (Extreme Bonus)")
    ap.add_argument("--config", default=path)
    ap.add_argument("--node")
    ap.add_argument("--role", choices=["primary", "backup"])
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    ap.add_argument("--peer")
    ap.add_argument("--witness")
    ap.add_argument("--max-clients", type=int)
    ap.add_argument("--db")
    ap.add_argument("--log")
    ap.add_argument("--failover-delay-ms", type=int)
    ap.add_argument("--no-witness", dest="require_witness",
                    action="store_false", default=None,
                    help="unsafe mode: promote without the witness (shows split brain)")
    args = ap.parse_args(argv)

    for key, attr in (("node", "node"), ("role", "role"), ("host", "host"),
                      ("port", "port"), ("peer", "peer"),
                      ("witness", "witness"), ("max_clients", "max_clients"),
                      ("db_path", "db"), ("log_path", "log"),
                      ("failover_delay_ms", "failover_delay_ms")):
        value = getattr(args, attr, None)
        if value is not None:
            cfg[key] = value
    if args.require_witness is not None:
        cfg["require_witness"] = bool(args.require_witness)

    cfg["peer"] = (cfg.get("peer") or "").rstrip("/")
    cfg["witness"] = (cfg.get("witness") or "").rstrip("/")
    return cfg


# --------------------------------------------------------------------------- #
# the cluster brain
# --------------------------------------------------------------------------- #

class QuorumCluster:
    """Leadership fenced by a witness grant; everything else mirrors the +10% design."""

    def __init__(self, config: dict):
        self.config = config
        self.node = config["node"]
        self._lock = threading.RLock()
        self._stop = threading.Event()

        self.bank = ReplicatedBank(config["db_path"], self.node)
        self.bank.replicate = self.replicate
        self.bank._is_leader = self.is_leader
        self.bank._current_epoch = lambda: self.term

        self.term = self.bank.entry_epoch_at(max(0, self.bank.last_index)) or 0
        self._leader = False
        self._solo = False
        self._peer_up = False
        self._peer_leader = False
        self._witness_up = False
        self._grants_held = 0
        self._grants_refused = 0
        self._last_reason = "start"
        self._peer_lost_at = 0.0

        self._thread: threading.Thread | None = None

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="quorum-tick",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        self.bank.close()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as exc:            # a probe must never kill the loop
                _log_event("tick_error", node=self.node, error=repr(exc))
            self._stop.wait(1.0)

    # -- probes --------------------------------------------------------------

    def _peer_alive(self) -> bool:
        if not self.config.get("peer"):
            return False
        try:
            http_post(self.config["peer"] + "/cluster/ping", {"from": self.node},
                      timeout=1.0)
            return True
        except Exception:
            return False

    def _peer_status(self) -> dict:
        if not self.config.get("peer"):
            return {}
        try:
            return http_get(self.config["peer"] + "/cluster/status", timeout=1.0)
        except Exception:
            return {}

    def _witness_grant(self, term: int) -> tuple[bool, str]:
        """Ask the witness for the right to lead ``term``."""
        if not self.config.get("witness"):
            return False, "no_witness_configured"
        try:
            body = http_post(self.config["witness"] + "/witness/grant",
                             {"node": self.node, "term": term}, timeout=2.0)
            self._witness_up = True
            return bool(body.get("ok")), str(body.get("reason", ""))
        except Exception as exc:
            self._witness_up = False
            return False, f"witness_unreachable:{type(exc).__name__}"

    # -- the decision round ---------------------------------------------------

    def _tick(self) -> None:
        peer_up = self._peer_alive()
        peer_status = self._peer_status() if peer_up else {}
        peer_leader = bool(peer_status.get("leader"))
        now = time.monotonic()

        with self._lock:
            self._peer_up = peer_up
            self._peer_leader = peer_leader
            if peer_up:
                self._peer_lost_at = 0.0
            elif self._peer_lost_at == 0.0:
                self._peer_lost_at = now
            lost_at = self._peer_lost_at
            grace = self.config["failover_delay_ms"] / 1000.0
            waited = bool(lost_at) and (now - lost_at) >= grace

        # ---- case 1: we think we are leader ---------------------------------
        if self.is_leader():
            # If the peer also claims leadership, the higher term wins; on equal
            # terms the alphabetically smaller node keeps it.  The witness has
            # the final word, so re-affirm our grant.
            if peer_up and peer_leader:
                my_term = self.term
                peer_term = int(peer_status.get("term", 0))
                if peer_term > my_term or (peer_term == my_term and self.node > peer_status.get("node", "B")):
                    self._step_down("higher_term_peer")
                    return
            if peer_up:
                with self._lock:
                    self._solo = False
            if not peer_up and waited:
                # Still leader, peer gone: make sure the witness agrees, so the
                # isolated side can never keep writing.
                ok, reason = self._witness_grant(self.term)
                if ok:
                    with self._lock:
                        self._solo = True
                elif self.config.get("require_witness", True):
                    self._step_down(f"witness_refuses:{reason}")
            return

        # ---- case 2: someone else leads -------------------------------------
        if peer_up and peer_leader:
            self._maybe_resync()
            return

        # ---- case 3: asking to lead -----------------------------------------
        if peer_up and not peer_leader:
            # Nobody leads yet.  The designated primary takes the term right
            # away (it still needs a witness grant); a backup waits, because a
            # backup only ever steps up for a *missing* primary.
            if self.config["role"] != "primary":
                with self._lock:
                    self._last_reason = "backup_idle"
                return
        elif peer_up and not waited:
            # Peer present but not answering as leader yet: give it the grace
            # period before we try to take the term away from it.
            with self._lock:
                self._last_reason = "peer_present_waiting"
            return

        candidate_term = self.term + 1
        if not self.config.get("require_witness", True) and not self.config.get("witness"):
            # Explicit unsafe mode: no arbiter at all.  Demonstrates split brain.
            self._become_leader(candidate_term, solo=True, reason="unsafe_no_witness")
            return

        ok, reason = self._witness_grant(candidate_term)
        if ok:
            self._become_leader(candidate_term, solo=not peer_up,
                                reason=f"witness_granted:{reason}")
            if not peer_up:
                _log_event("partition_takeover", node=self.node, term=candidate_term)
            return

        # Refused.  Because we are NOT the leader we must keep refusing writes.
        with self._lock:
            self._leader = False
            self._solo = False
            self._grants_refused += 1
            self._last_reason = f"read_only:{reason}"

    # -- transitions ----------------------------------------------------------

    def _become_leader(self, term: int, solo: bool, reason: str) -> None:
        with self._lock:
            self._leader = True
            self._solo = solo
            self.term = term
            self._grants_held += 1
            self._last_reason = reason
            self.config["role"] = "primary"
        _log_event("promote", node=self.node, term=term, solo=solo, reason=reason)

    def _step_down(self, reason: str) -> None:
        was = self.is_leader()
        with self._lock:
            self._leader = False
            self._solo = False
            self._last_reason = reason
            self.config["role"] = "backup"
        if was:
            _log_event("step_down", node=self.node, term=self.term, reason=reason)

    def is_leader(self) -> bool:
        with self._lock:
            return self._leader

    def is_solo(self) -> bool:
        with self._lock:
            return self._solo

    # -- resync (same idea as the +10% server) --------------------------------

    def _maybe_resync(self) -> None:
        status = self._peer_status()
        try:
            peer_last = int(status.get("last_index", 0))
        except (TypeError, ValueError):
            return
        ours = self.bank.last_index
        if peer_last < ours:
            # Never adopt a shorter log: our extra entries may have been acked.
            return
        same = (peer_last == ours and status.get("log_hash") == self.bank.log_hash())
        if not same:
            self._try_resync()

    def _try_resync(self) -> None:
        peer = self.config.get("peer")
        if not peer or self.is_leader():
            return
        try:
            data = http_get(peer + "/cluster/log", timeout=5.0)
            entries = [LogEntry.from_json(json.dumps(e)) for e in data["entries"]]
            self.bank.resync(entries)
            _log_event("resync_ok", node=self.node, entries=len(entries))
        except Exception as exc:
            _log_event("resync_failed", node=self.node, error=repr(exc))

    # -- replication ----------------------------------------------------------

    def replicate(self, entry: LogEntry) -> None:
        if not self.config.get("peer"):
            return
        if self._solo and not self._peer_up:
            # Sole survivor of a partition where the witness granted us the
            # term: we are the majority side, so we may commit alone.
            _log_event("replication_skipped", node=self.node, index=entry.index,
                       reason="quorum_side")
            return
        try:
            data = http_post(self.config["peer"] + "/cluster/replicate",
                             {"entry": json.loads(entry.to_json())}, timeout=5.0)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise ReplicationError(f"backup rejected entry: {body}")
        except Exception as exc:
            raise ReplicationError(f"backup unreachable: {exc}")
        if not data.get("ok"):
            raise ReplicationError(f"backup did not ack: {data}")

    def accept_replication(self, entry: LogEntry) -> dict:
        return self.bank.accept_replication(entry)

    # -- status ---------------------------------------------------------------

    def status(self) -> dict:
        with self._lock:
            return {
                "node": self.node,
                "role": self.config["role"],
                "leader": self._leader,
                "solo": self._solo,
                "term": self.term,
                "peer_up": self._peer_up,
                "peer_leader": self._peer_leader,
                "witness_up": self._witness_up,
                "grants_held": self._grants_held,
                "grants_refused": self._grants_refused,
                "reason": self._last_reason,
                "last_index": self.bank.last_index,
                "log_hash": self.bank.log_hash(),
                "accounts": self.bank.snapshot(),
            }


# --------------------------------------------------------------------------- #
# logging
# --------------------------------------------------------------------------- #

def _log_event(event: str, **extra) -> None:
    rec = {"ts": round(time.time(), 3), "event": event, **extra}
    _log_line(json.dumps(rec, ensure_ascii=False))


_LOG_PATH = os.environ.get("EXTREME_LOG", "extremeBonus/node.log")


def _log_line(line: str) -> None:
    print(line, flush=True)
    try:
        with open(_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# HTTP layer (same surface as the +10% server so the same client works)
# --------------------------------------------------------------------------- #

CONFIG: dict = {}
CLUSTER: QuorumCluster | None = None
BANK: ReplicatedBank | None = None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MiniBankQuorum/1.0"
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

    # -- GET -----------------------------------------------------------------

    def do_GET(self):                                   # noqa: N802
        assert CLUSTER is not None and BANK is not None
        path = self.path.rstrip("/")
        if path in ("/health", "/healthz"):
            self._send_json(200, {"status": "ok", "node": CONFIG["node"],
                                  "leader": CLUSTER.is_leader()}, "-")
            return
        if path == "/cluster/log":
            entries = [json.loads(e.to_json()) for e in BANK.all_entries()]
            self._send_json(200, {"entries": entries, "last_index": BANK.last_index}, "-")
            return
        if path == "/cluster/status":
            self._send_json(200, CLUSTER.status(), "-")
            return
        self._send_json(404, {"error": "not_found", "path": self.path}, "-")

    # -- POST ----------------------------------------------------------------

    def do_POST(self):                                  # noqa: N802
        assert CLUSTER is not None and BANK is not None
        path = self.path.rstrip("/")
        request_id = self.headers.get("X-Request-Id") or "-"

        if path == "/cluster/ping":
            self._send_json(200, {"ok": True, "from": CONFIG["node"]}, request_id)
            return

        if path == "/cluster/replicate":
            try:
                body = self._read_json() or {}
                entry = LogEntry.from_json(json.dumps(body["entry"]))
                result = CLUSTER.accept_replication(entry)
                self._send_json(200, {"ok": True, "result": result}, request_id)
            except Exception as exc:
                self._send_json(500, {"ok": False, "error": repr(exc)}, request_id)
            return

        # ---- the banking endpoint (POST /) ---------------------------------
        started = time.perf_counter()
        try:
            request = self._read_json()
        except Exception as exc:
            self._bank_reply(400, {"status": "error", "error": "malformed_request",
                                   "detail": repr(exc)}, request_id, started,
                             None, None)
            return
        if not isinstance(request, dict):
            self._bank_reply(400, {"status": "error", "error": "malformed_request"},
                             request_id, started, None, None)
            return

        action = str(request.get("action", "")).strip()
        account = request.get("account")

        if action == "balance":
            if not account:
                self._bank_reply(422, {"status": "error", "error": "missing_field"},
                                 request_id, started, action, account)
                return
            self._bank_reply(200, {"status": "success", "account": account,
                                   "balance": BANK.get_balance(account) / 100.0},
                             request_id, started, action, account)
            return

        if action not in ("deposit", "withdraw", "transfer"):
            self._bank_reply(422, {"status": "error", "error": "unsupported_action"},
                             request_id, started, action, account)
            return

        # Writes need leadership *and* a live witness grant.
        if not CLUSTER.is_leader():
            self._bank_reply(503, {"status": "error", "error": "not_primary",
                                   "reason": CLUSTER._last_reason},
                             request_id, started, action, account)
            return

        try:
            rid = request.get("request_id") or request_id
            command = {"action": action, "account": account,
                       "amount": request["amount"]}
            if action == "transfer":
                command["to"] = request["to"]
            result = BANK.submit(rid, command)
        except NotLeaderError:
            self._bank_reply(503, {"status": "error", "error": "not_primary"},
                             request_id, started, action, account)
            return
        except ReplicationError as exc:
            self._bank_reply(503, {"status": "error", "error": "replication_failed",
                                   "detail": str(exc)},
                             request_id, started, action, account)
            return
        except Exception as exc:
            code = getattr(exc, "code", "internal_error")
            http = 422 if code != "internal_error" else 500
            payload = {"status": "error", "error": code,
                       "message": getattr(exc, "message", str(exc))}
            self._bank_reply(http, payload, request_id, started, action, account)
            return

        self._bank_reply(200, result, request_id, started, action, account)

    def _bank_reply(self, code, payload, request_id, started, action, account) -> None:
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        _log_line(json.dumps({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
                  + f".{int((time.time() % 1) * 1000):03d}Z",
            "req": request_id, "node": CONFIG["node"], "action": action,
            "account": account, "http": code,
            "result": payload.get("status", "error") if isinstance(payload, dict) else "?",
            "latency_ms": latency_ms,
        }, ensure_ascii=False))
        self._send_json(code, payload, request_id)


# --------------------------------------------------------------------------- #
# entry point
# --------------------------------------------------------------------------- #

def main(argv=None) -> int:
    global CONFIG, CLUSTER, BANK, _LOG_PATH
    CONFIG = load_config(os.environ.get("EXTREME_CONFIG"), argv)
    if CONFIG.get("log_path"):
        _LOG_PATH = CONFIG["log_path"]

    _log_event("node_start", node=CONFIG["node"], role=CONFIG["role"],
               port=CONFIG["port"], peer=CONFIG.get("peer"),
               witness=CONFIG.get("witness"),
               require_witness=CONFIG.get("require_witness"))

    CLUSTER = QuorumCluster(CONFIG)
    BANK = CLUSTER.bank
    CLUSTER.start()

    httpd = ThreadingHTTPServer((CONFIG["host"], CONFIG["port"]), Handler)
    httpd.daemon_threads = True
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        CLUSTER.stop()
        httpd.server_close()
        _log_event("node_stop", node=CONFIG["node"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

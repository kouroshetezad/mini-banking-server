"""Witness (arbiter) service for the Extreme Bonus: network partition / split brain.

Why a witness exists
--------------------
Two servers alone can never distinguish "my peer crashed" from "the network
between us is cut".  With only two voters, both sides can reason "the other one
must be gone" and both can decide to become primary.  That is split brain: two
divergent versions of the balances.

A witness breaks the tie.  It is a third, independent machine that stores
**no account data**.  Its only job is to answer one question::

    "Am I (node X, term T) allowed to act as primary?"

It grants leadership to **at most one node per term**.  During a partition the
two sides each ask the witness; whichever asks first for a given term wins and
the other is refused.  The refused side then refuses writes (returns 503) and
downgrades itself to read-only.  When the partition heals, the loser re-syncs
from the winner.

Why this is safe (and honest)
-----------------------------
This is the classic quorum/arbiter pattern.  It is safe because:

* **The witness is a third machine.**  It runs on the host (or a separate VM),
  not on Server A or Server B, so a partition between A and B does not take the
  witness down with either side.
* **One grant per term.**  Two nodes asking for the same term cannot both be
  granted.  Two nodes asking for *different* terms is fine, because the higher
  term wins and the lower-term node must yield.
* **Fencing by term.**  The term number only ever increases.  A node that was
  granted term 3 will refuse to accept replication from a stale leader claiming
  term 2.

What it is NOT
--------------
The witness does not store balances and does not participate in replication.
It cannot tell you the correct balance; it only decides who is allowed to write.
That keeps it tiny and makes the safety argument easy to state.

The witness is also honest about its own failure mode: if the witness is
unreachable, a node **must not** assume it is allowed to lead.  The safe default
is to refuse writes.  That trade-off (availability lost, consistency kept) is
what the assignment asks us to defend, and it is configurable so we can show
both sides.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #

_lock = threading.RLock()

# term -> node_id that owns that term
_grants: dict[int, str] = {}
# The highest term we have ever handed out.  A grant request for a term lower
# than this is always refused: it belongs to a superseded leader.
_last_term = 0

START = time.time()


def _now() -> float:
    return round(time.time(), 3)


def grant(node: str, term: int) -> tuple[bool, str]:
    """Try to give ``node`` the right to lead ``term``.

    Returns ``(ok, reason)``.  ``ok`` is True only if this node owns the term
    after the call.  The call is idempotent: the same node asking twice for the
    same term is still the owner.
    """
    global _last_term
    with _lock:
        if term < _last_term:
            owner = _grants.get(term, "unknown")
            return False, (f"stale_grant (this node holds term {term} for {owner}, "
                           f"but the cluster has moved on to term {_last_term})")

        owner = _grants.get(term)
        if owner is None:
            _grants[term] = node
            _last_term = max(_last_term, term)
            return True, "granted"
        if owner == node:
            _last_term = max(_last_term, term)
            return True, "already_owner"
        return False, f"term_owned_by_{owner}"


def yield_term(node: str, term: int) -> None:
    """A node voluntarily gives up a term (e.g. it lost its own peer anyway)."""
    global _last_term
    with _lock:
        if _grants.get(term) == node:
            del _grants[term]
        _last_term = max(_last_term, term)


def state() -> dict:
    with _lock:
        return {
            "role": "witness",
            "last_term": _last_term,
            "grants": {str(k): v for k, v in sorted(_grants.items())},
            "uptime_s": round(time.time() - START, 1),
            "ts": _now(),
        }


# --------------------------------------------------------------------------- #
# HTTP layer
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MiniBankWitness/1.0"
    timeout = 10

    def log_message(self, fmt, *args):    # keep the console clean
        pass

    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""
        return json.loads(raw.decode("utf-8")) if raw else None

    def _log(self, event: str, **extra) -> None:
        rec = {"ts": _now(), "event": event, **extra}
        line = json.dumps(rec, ensure_ascii=False)
        print(line, flush=True)
        path = getattr(self.server, "log_path", None)   # type: ignore[attr-defined]
        if path:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    # -- endpoints -----------------------------------------------------------

    def do_GET(self):                                   # noqa: N802
        path = self.path.rstrip("/")
        if path in ("/health", "/healthz"):
            self._send_json(200, {"status": "ok", **state()})
            return
        if path == "/witness/state":
            self._send_json(200, state())
            return
        self._send_json(404, {"error": "not_found", "path": self.path})

    def do_POST(self):                                  # noqa: N802
        path = self.path.rstrip("/")
        try:
            body = self._read_json() or {}
        except Exception as exc:
            self._send_json(400, {"error": "malformed_request", "detail": repr(exc)})
            return

        if path == "/witness/grant":
            node = str(body.get("node", "")).strip()
            try:
                term = int(body.get("term", 0))
            except (TypeError, ValueError):
                self._send_json(400, {"error": "invalid_term"})
                return
            if not node:
                self._send_json(400, {"error": "missing_node"})
                return
            ok, reason = grant(node, term)
            self._log("grant_request", node=node, term=term, ok=ok, reason=reason)
            self._send_json(200, {"ok": ok, "reason": reason, "node": node,
                                  "term": term, "last_term": _last_term})
            return

        if path == "/witness/yield":
            node = str(body.get("node", "")).strip()
            try:
                term = int(body.get("term", 0))
            except (TypeError, ValueError):
                self._send_json(400, {"error": "invalid_term"})
                return
            yield_term(node, term)
            self._log("yield", node=node, term=term)
            self._send_json(200, {"ok": True})
            return

        self._send_json(404, {"error": "not_found", "path": self.path})


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def build_server(host: str, port: int, log_path: str | None) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    httpd.log_path = log_path                            # type: ignore[attr-defined]
    return httpd


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Bank witness / arbiter (Extreme Bonus)")
    ap.add_argument("--host", default=os.environ.get("WITNESS_HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int,
                    default=int(os.environ.get("WITNESS_PORT", "8099")))
    ap.add_argument("--log", default=os.environ.get("WITNESS_LOG", "witness.log"))
    args = ap.parse_args(argv)

    httpd = build_server(args.host, args.port, args.log)
    print(json.dumps({"ts": _now(), "event": "witness_start",
                      "host": args.host, "port": args.port,
                      "log": args.log}), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print(json.dumps({"ts": _now(), "event": "witness_stop"}), flush=True)
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

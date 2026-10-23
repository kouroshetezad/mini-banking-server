"""Interactive and batch client for the Mini Banking Transaction Server.

Examples
--------
    # single node
    python3 client/bank_client.py --host 192.168.64.130 balance A1001
    python3 client/bank_client.py --host 192.168.64.130 deposit A1001 200
    python3 client/bank_client.py --host 192.168.64.130 withdraw A1001 50
    python3 client/bank_client.py --host 192.168.64.130 transfer A1001 A1002 50

    # cluster: give BOTH nodes, the client follows a fail-over by itself
    python3 client/bank_client.py --hosts 192.168.64.130:8080,192.168.64.131:8081 \
        deposit A1001 200

    # interactive shell
    python3 client/bank_client.py --hosts 192.168.64.130:8080,192.168.64.131:8081

    # send a raw body (for malformed/edge-case demos)
    python3 client/bank_client.py --host 192.168.64.130 raw '{"action":"frob"}'

Only the standard library is used.

Why ``--hosts`` matters: after a fail-over the old primary is dead, so a client
that only knows one address can never reach the new primary.  With a host list
the client tries the nodes in turn, always retrying with the **same**
``request_id``, which is what makes the retry exactly-once.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid

RETRIABLE_ERRORS = {"not_primary", "replication_unavailable", "server_busy"}


def send(host: str, port: int, payload: bytes, timeout: float = 10.0):
    """One HTTP round trip. Returns (status, request_id, node, body)."""
    url = f"http://{host}:{port}/"
    req = urllib.request.Request(url, data=payload,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return (resp.status, resp.headers.get("X-Request-Id"),
                    resp.headers.get("X-Node"), resp.read())
    except urllib.error.HTTPError as exc:
        return (exc.code, exc.headers.get("X-Request-Id"),
                exc.headers.get("X-Node"), exc.read())


def parse_hosts(spec: str) -> list[tuple[str, int]]:
    hosts = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            host, port = part.rsplit(":", 1)
            hosts.append((host, int(port)))
        else:
            hosts.append((part, 8080))
    if not hosts:
        raise SystemExit("no hosts given")
    return hosts


class ClusterClient:
    """Client that survives a fail-over.

    It keeps a preferred node (the one that answered last) and, on a connection
    error or a retriable 503, moves to the next node in the list.  The request
    body -- including ``request_id`` -- is byte-identical on every attempt, so
    the server can recognise the retry and return the stored result instead of
    applying the operation twice.
    """

    def __init__(self, hosts: list[tuple[str, int]], timeout: float = 10.0):
        self.hosts = hosts
        self.timeout = timeout
        self.prefer = 0

    def _order(self):
        return list(enumerate(self.hosts))[self.prefer:] + \
               list(enumerate(self.hosts))[:self.prefer]

    def request(self, payload: dict, retries: int = 12,
                backoff: float = 0.6) -> tuple[int, str | None, str | None, bytes, int]:
        body = json.dumps(payload).encode("utf-8")
        last = (0, None, None,
                json.dumps({"status": "error", "error": "unreachable",
                            "message": "no node answered"}).encode())
        attempts = 0

        for attempt in range(retries):
            for index, (host, port) in self._order():
                attempts += 1
                try:
                    code, rid, node, raw = send(host, port, body, self.timeout)
                except Exception as exc:
                    last = (0, None, None, json.dumps({
                        "status": "error", "error": "unreachable",
                        "message": f"{host}:{port}: {exc}"}).encode())
                    continue

                try:
                    parsed = json.loads(raw.decode("utf-8"))
                except Exception:
                    parsed = {}

                error = parsed.get("error")
                if error == "not_primary" or (code >= 500 and error in RETRIABLE_ERRORS):
                    # Another node may be the primary now: try it immediately.
                    last = (code, rid, node, raw)
                    continue
                if code >= 500 and attempt < retries - 1:
                    last = (code, rid, node, raw)
                    continue

                self.prefer = index
                return code, rid, node, raw, attempts

            time.sleep(backoff * (attempt + 1))

        code, rid, node, raw = last
        return code, rid, node, raw, attempts

    def raw(self, payload: bytes, retries: int = 12,
            backoff: float = 0.6) -> tuple[int, str | None, str | None, bytes, int]:
        """Send a pre-built body, retrying across the whole host list.

        The ``raw`` command exists to demo malformed input, so the *server* may
        legitimately answer 400.  The point here is only to survive a node that
        is down (connection error) by moving to the next host -- the bytes are
        sent unchanged so a malformed body still arrives malformed.
        """
        last = (0, None, None,
                json.dumps({"status": "error", "error": "unreachable",
                            "message": "no node answered"}).encode())
        attempts = 0

        for attempt in range(retries):
            for index, (host, port) in self._order():
                attempts += 1
                try:
                    code, rid, node, raw = send(host, port, payload, self.timeout)
                except Exception as exc:
                    last = (0, None, None, json.dumps({
                        "status": "error", "error": "unreachable",
                        "message": f"{host}:{port}: {exc}"}).encode())
                    continue
                self.prefer = index
                return code, rid, node, raw, attempts
            time.sleep(backoff * (attempt + 1))

        code, rid, node, raw = last
        return code, rid, node, raw, attempts


def pretty(code, request_id, node, body) -> None:
    try:
        parsed = json.loads(body.decode("utf-8"))
    except Exception:
        parsed = body.decode("utf-8", "replace")
    out = {"http": code, "req": request_id}
    if node:
        out["node"] = node
    out["response"] = parsed
    print(json.dumps(out, indent=2))


USAGE = ("usage: balance <acct> | deposit <acct> <amt> | withdraw <acct> <amt> | "
         "transfer <from> <to> <amt> | raw <json> | quit")


def build_payload(argv: list[str]) -> dict:
    """Turn a command line into a request object, with a fresh request id."""
    action = argv[0]
    payload: dict = {"action": action, "request_id": uuid.uuid4().hex}

    if action in ("balance", "deposit", "withdraw"):
        if len(argv) < 2:
            raise SystemExit(f"'{action}' needs an account\n{USAGE}")
        payload["account"] = argv[1]
        if action != "balance":
            if len(argv) < 3:
                raise SystemExit(f"'{action}' needs an amount\n{USAGE}")
            payload["amount"] = _amount(argv[2])
    elif action == "transfer":
        if len(argv) < 4:
            raise SystemExit(f"'transfer' needs <from> <to> <amount>\n{USAGE}")
        payload["account"], payload["to"] = argv[1], argv[2]
        payload["amount"] = _amount(argv[3])
    else:
        raise SystemExit(f"unknown command {action!r}\n{USAGE}")
    return payload


def _amount(raw: str):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw          # keep it as a string; the server validates it


def interactive(client: ClusterClient) -> int:
    targets = ", ".join(f"{h}:{p}" for h, p in client.hosts)
    print(f"Connected interactive client -> {targets}")
    print(USAGE)
    while True:
        try:
            line = input("bank> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        if line in ("quit", "exit"):
            return 0
        if line.startswith("raw"):
            payload = line[len("raw"):].strip().encode("utf-8")
            code, rid, node, body, attempts = client.raw(payload)
            if attempts > 1:
                print(f"(answered after {attempts} attempt(s))")
            pretty(code, rid, node, body)
            continue
        try:
            obj = build_payload(line.split())
        except SystemExit as exc:
            print(exc)
            continue
        code, rid, node, body, attempts = client.request(obj)
        if attempts > 1:
            print(f"(answered after {attempts} attempt(s))")
        pretty(code, rid, node, body)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Mini Banking client")
    parser.add_argument("--host", default=None,
                        help="server IP address (use the real VM IP, not localhost)")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--hosts", default=None,
                        help="comma separated host:port list for fail-over, "
                             "e.g. 192.168.64.130:8080,192.168.64.131:8081")
    parser.add_argument("--cluster", action="store_true",
                        help="retry with a stable request id (kept for "
                             "compatibility; --hosts implies it)")
    parser.add_argument("--retries", type=int, default=12,
                        help="how many rounds to retry across the host list")
    parser.add_argument("command", nargs="*", help=USAGE)
    args = parser.parse_args(argv)

    if args.hosts:
        hosts = parse_hosts(args.hosts)
    else:
        hosts = [(args.host or "127.0.0.1", args.port)]
    client = ClusterClient(hosts)

    if not args.command:
        return interactive(client)

    if args.command[0] == "raw":
        payload = " ".join(args.command[1:]).encode("utf-8")
        code, rid, node, body, attempts = client.raw(payload, retries=args.retries)
        if attempts > 1:
            print(f"(answered after {attempts} attempt(s))")
        pretty(code, rid, node, body)
        return 0 if code == 200 else 1

    obj = build_payload(args.command)
    code, rid, node, body, attempts = client.request(obj, retries=args.retries)
    if attempts > 1:
        print(f"(answered after {attempts} attempt(s))")
    pretty(code, rid, node, body)
    return 0 if code == 200 else 1


if __name__ == "__main__":
    raise SystemExit(main())

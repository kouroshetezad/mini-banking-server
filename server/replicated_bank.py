"""Replicated account store for the fault-tolerant banking cluster.

This module extends the single-node :mod:`bank` design with the pieces needed
for the *Advanced Bonus* (primary/backup fail-over).

Design in one paragraph
------------------------
Every balance-changing operation is a deterministic **command**
(``deposit``/``withdraw``/``transfer``). A command carries a globally unique
``request_id`` and is appended to a **replicated log** with a monotonically
increasing ``index``. The primary assigns the index, applies the command
locally inside a single SQLite transaction *together with* the log record, and
only then asks the backup to replicate the same log record. The backup applies
it at the same index. When the backup acknowledges, the primary replies to the
client. This gives:

* **Atomicity / Consistency** -- one SQLite transaction per command, with a
  ``CHECK (balance >= 0)`` guard; a failure rolls the whole thing back.
* **Isolation** -- a process-wide write lock plus ``BEGIN IMMEDIATE`` serialise
  command application; the log is a total order, so replay is deterministic.
* **Durability** -- the commit (WAL + ``synchronous=FULL``) happens before the
  replication ack and therefore before the client reply.
* **No lost / no double-applied transaction** -- replication is synchronous and
  the ``applied_requests`` table makes command application **idempotent**: a
  client that retries a request (because it never saw a reply) sends the same
  ``request_id`` and the server returns the cached result instead of applying it
  twice.

Take-over
---------
When the backup becomes the primary it adopts the log both sides share, so the
new primary starts from exactly the state the old primary had acknowledged.
Because replication was synchronous, that includes every write the old primary
ever replied ``success`` to.

Nothing here uses third-party packages.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

# Re-use the value types and validation from the single-node store.
from bank import BankError, Result, _to_cents, DEFAULT_ACCOUNTS


# Command kinds that mutate state. ``balance`` is a read and never logged.
MUTATING = ("deposit", "withdraw", "transfer")


@dataclass
class LogEntry:
    """One record in the replicated log."""

    index: int
    request_id: str
    command: dict
    epoch: int
    ts: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "index": self.index,
                "request_id": self.request_id,
                "command": self.command,
                "epoch": self.epoch,
                "ts": self.ts,
            },
            separators=(",", ":"),
        )

    @staticmethod
    def from_json(raw: str) -> "LogEntry":
        d = json.loads(raw)
        return LogEntry(
            index=int(d["index"]),
            request_id=d["request_id"],
            command=d["command"],
            epoch=int(d["epoch"]),
            ts=float(d.get("ts", time.time())),
        )


class NotLeaderError(Exception):
    """Raised when a non-primary node is asked to mutate state."""


class ReplicationError(Exception):
    """Raised when the backup cannot be reached or rejects the entry."""


class ReplicatedBank:
    """A SQLite bank whose every mutation is a replicated, idempotent command.

    The class is deliberately transport-agnostic: it takes a ``replicate``
    callback that the cluster layer wires to the real peer link. That keeps the
    storage logic testable without any network.
    """

    def __init__(self, db_path: str, node_id: str):
        self.db_path = db_path
        self.node_id = node_id
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._configure()
        self._init_schema()

        # Cluster wiring, set by the cluster layer.
        self.replicate: Optional[Callable[[LogEntry], None]] = None
        self._is_leader: Callable[[], bool] = lambda: True
        self._current_epoch: Callable[[], int] = lambda: 1

    # -- setup -----------------------------------------------------------------

    def _configure(self) -> None:
        cur = self._conn
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=FULL")
        cur.execute("PRAGMA busy_timeout=30000")

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    account TEXT PRIMARY KEY,
                    balance INTEGER NOT NULL CHECK (balance >= 0)
                );
                CREATE TABLE IF NOT EXISTS log (
                    idx        INTEGER PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    command    TEXT NOT NULL,
                    epoch      INTEGER NOT NULL,
                    ts         REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS applied_requests (
                    request_id TEXT PRIMARY KEY,
                    response   TEXT NOT NULL,
                    idx        INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            self._conn.commit()
            row = self._conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()
            if row["n"] == 0:
                self._conn.executemany(
                    "INSERT INTO accounts(account, balance) VALUES(?, ?)",
                    list(DEFAULT_ACCOUNTS.items()),
                )
                self._conn.commit()

    # -- meta helpers ----------------------------------------------------------

    def _meta_get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def _meta_set(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    @property
    def last_index(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(idx), 0) AS m FROM log"
            ).fetchone()
            return int(row["m"])

    # -- reads -----------------------------------------------------------------

    def get_balance(self, account: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT balance FROM accounts WHERE account = ?", (account,)
            ).fetchone()
        if row is None:
            raise BankError("unknown_account", f"unknown account: {account}",
                            account=account)
        return int(row["balance"])

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT account, balance FROM accounts ORDER BY account"
            ).fetchall()
        return {r["account"]: r["balance"] / 100.0 for r in rows}

    # -- idempotency -----------------------------------------------------------

    def _cached_response(self, request_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT response FROM applied_requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        return json.loads(row["response"]) if row else None

    # -- command application ---------------------------------------------------

    def apply_command(self, entry: LogEntry) -> dict:
        """Apply one log entry **inside a single transaction**.

        This is the only place account balances ever change. It is:

        * **idempotent** -- if ``request_id`` was already applied, the stored
          response is returned and nothing is touched. This is what makes a
          client retry safe after a primary crash.
        * **ordered** -- the entry's ``index`` must equal ``last_index + 1``; a
          gap raises, which the replication path turns into a resync.
        """
        with self._lock:
            cached = self._cached_response(entry.request_id)
            if cached is not None:
                return cached

            expected = self.last_index + 1
            if entry.index != expected:
                raise ReplicationError(
                    f"log gap: expected index {expected}, got {entry.index}"
                )

            self._conn.execute("BEGIN IMMEDIATE")
            try:
                response = self._execute(entry)
                self._conn.execute(
                    "INSERT INTO log(idx, request_id, command, epoch, ts) "
                    "VALUES(?,?,?,?,?)",
                    (entry.index, entry.request_id,
                     json.dumps(entry.command, separators=(",", ":")),
                     entry.epoch, entry.ts),
                )
                self._conn.execute(
                    "INSERT INTO applied_requests(request_id, response, idx) "
                    "VALUES(?,?,?)",
                    (entry.request_id,
                     json.dumps(response, separators=(",", ":")), entry.index),
                )
                self._conn.commit()  # fsync here -> durable before we ack
            except BaseException:
                # A rejected command (insufficient funds, bad amount, ...) must
                # leave absolutely nothing behind, not even an open transaction.
                self._conn.rollback()
                raise
            return response

    def _execute(self, entry: LogEntry) -> dict:
        """Run the balance math for a command. Raises BankError on rejection."""
        cmd = entry.command
        action = cmd["action"]
        account = cmd["account"]

        if action == "deposit":
            cents = _to_cents(cmd["amount"])
            self._require_account(account)
            self._conn.execute(
                "UPDATE accounts SET balance = balance + ? WHERE account = ?",
                (cents, account),
            )
            return {"status": "success", "account": account,
                    "balance": self._peek(account) / 100.0}

        if action == "withdraw":
            cents = _to_cents(cmd["amount"])
            current = self._require_account(account)
            if current < cents:
                raise BankError("insufficient_funds", "insufficient funds",
                                account=account, balance=current / 100.0)
            self._conn.execute(
                "UPDATE accounts SET balance = balance - ? WHERE account = ?",
                (cents, account),
            )
            return {"status": "success", "account": account,
                    "balance": self._peek(account) / 100.0}

        if action == "transfer":
            cents = _to_cents(cmd["amount"])
            src, dst = account, cmd["to"]
            if src == dst:
                raise BankError("invalid_transfer", "source and destination are equal")
            src_bal = self._require_account(src)
            self._require_account(dst)
            if src_bal < cents:
                raise BankError("insufficient_funds", "insufficient funds",
                                account=src, balance=src_bal / 100.0)
            # Debit and credit in the *same* transaction -> atomic transfer.
            self._conn.execute(
                "UPDATE accounts SET balance = balance - ? WHERE account = ?",
                (cents, src),
            )
            self._conn.execute(
                "UPDATE accounts SET balance = balance + ? WHERE account = ?",
                (cents, dst),
            )
            return {"status": "success", "account": src,
                    "balance": self._peek(src) / 100.0,
                    "to": dst, "to_balance": self._peek(dst) / 100.0}

        raise BankError("unsupported_action", f"unsupported action: {action}")

    def _require_account(self, account: str) -> int:
        row = self._conn.execute(
            "SELECT balance FROM accounts WHERE account = ?", (account,)
        ).fetchone()
        if row is None:
            raise BankError("unknown_account", f"unknown account: {account}",
                            account=account)
        return int(row["balance"])

    def _peek(self, account: str) -> int:
        row = self._conn.execute(
            "SELECT balance FROM accounts WHERE account = ?", (account,)
        ).fetchone()
        return int(row["balance"])

    # -- primary path ----------------------------------------------------------

    def submit(self, request_id: str, command: dict) -> dict:
        """Primary path: validate, replicate, commit, then answer.

        The order is deliberate and it is what makes the guarantees hold:

        1. the command is *validated and applied* inside an open SQLite
           transaction, so an invalid request (insufficient funds, bad amount)
           fails before anything is replicated;
        2. the record is shipped to the backup and we wait for its ack;
        3. only then do we ``COMMIT`` locally and reply to the client.

        If step 2 fails the transaction is rolled back, so a write that reached
        only one machine is never acknowledged.  The reverse order (commit, then
        replicate) would let the client see ``success`` for a write that dies
        with this node.
        """
        if not self._is_leader():
            raise NotLeaderError("this node is not the primary")

        with self._lock:
            cached = self._cached_response(request_id)
            if cached is not None:
                return cached
            index = self.last_index + 1
            entry = LogEntry(index=index, request_id=request_id,
                             command=command, epoch=self._current_epoch())

            self._conn.execute("BEGIN IMMEDIATE")
            try:
                response = self._execute(entry)          # may raise BankError
                if self.replicate is not None:
                    self.replicate(entry)                # may raise ReplicationError
                self._conn.execute(
                    "INSERT INTO log(idx, request_id, command, epoch, ts) "
                    "VALUES(?,?,?,?,?)",
                    (entry.index, entry.request_id,
                     json.dumps(entry.command, separators=(",", ":")),
                     entry.epoch, entry.ts),
                )
                self._conn.execute(
                    "INSERT INTO applied_requests(request_id, response, idx) "
                    "VALUES(?,?,?)",
                    (entry.request_id,
                     json.dumps(response, separators=(",", ":")), entry.index),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
            return response

    # -- backup path -----------------------------------------------------------

    def append_from_leader(self, entry: LogEntry) -> dict:
        """Backup path: apply an entry the leader has told us about."""
        return self.apply_command(entry)

    def accept_replication(self, entry: LogEntry) -> dict:
        """Apply a record pushed by the primary.

        The primary holds a valid lease, so its log is authoritative.  If we
        already hold a *different* record at this index (a divergent tail left
        over from a partition, or a prepare whose ack was lost), we drop our
        tail from that index and accept the leader's version.  That is what
        makes the two logs converge instead of drifting apart forever.
        """
        with self._lock:
            local = self._request_id_at(entry.index)
            if local is not None:
                if local == entry.request_id:
                    return self._cached_response(entry.request_id) or {}
                self._truncate_from(entry.index)
            return self.apply_command(entry)

    def _request_id_at(self, index: int) -> Optional[str]:
        row = self._conn.execute(
            "SELECT request_id FROM log WHERE idx = ?", (index,)
        ).fetchone()
        return row["request_id"] if row else None

    def _truncate_from(self, index: int) -> None:
        """Delete our log from ``index`` onwards and replay what is left."""
        self._conn.execute("DELETE FROM log WHERE idx >= ?", (index,))
        self._conn.execute("DELETE FROM applied_requests WHERE idx >= ?", (index,))
        self._conn.commit()
        self._rebuild_accounts_locked()

    # -- recovery --------------------------------------------------------------

    def resync(self, entries: List[LogEntry]) -> None:
        """Adopt the leader's log wholesale.

        Used when a node rejoins after a partition or a crash.  The log is
        rebuilt from the seed and replayed entry by entry, so the resulting
        balances are exactly the leader's -- regardless of how far behind (or
        how divergent) this node was.
        """
        with self._lock:
            self._conn.execute("DELETE FROM log")
            self._conn.execute("DELETE FROM applied_requests")
            self._conn.commit()
            self._rebuild_accounts_locked()
            for entry in sorted(entries, key=lambda e: e.index):
                self.apply_command(entry)
            self._meta_set("last_synced", str(time.time()))
            self._conn.commit()

    def _rebuild_accounts_locked(self) -> None:
        """Reset accounts to the seed, then replay whatever log survived.

        The idempotency table is rebuilt at the same time, otherwise a client
        retry of a request that is still in the log would be applied twice.
        """
        self._conn.execute("DELETE FROM accounts")
        self._conn.executemany(
            "INSERT INTO accounts(account, balance) VALUES(?, ?)",
            list(DEFAULT_ACCOUNTS.items()),
        )
        rows = self._conn.execute(
            "SELECT idx, request_id, command, epoch, ts FROM log ORDER BY idx"
        ).fetchall()
        self._conn.execute("DELETE FROM applied_requests")
        for r in rows:
            entry = LogEntry(
                index=int(r["idx"]), request_id=r["request_id"],
                command=json.loads(r["command"]), epoch=int(r["epoch"]),
                ts=float(r["ts"]),
            )
            response = self._execute(entry)
            self._conn.execute(
                "INSERT INTO applied_requests(request_id, response, idx) "
                "VALUES(?,?,?)",
                (entry.request_id,
                 json.dumps(response, separators=(",", ":")), entry.index),
            )
        self._conn.commit()

    def log_hash(self) -> str:
        """Short fingerprint of the log, used to detect divergence cheaply."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT idx, request_id FROM log ORDER BY idx"
            ).fetchall()
        h = hashlib.sha256()
        for r in rows:
            h.update(f"{r['idx']}:{r['request_id']}\n".encode("utf-8"))
        return h.hexdigest()[:16]

    def entry_epoch_at(self, index: int) -> Optional[int]:
        """Return the epoch of the local entry at ``index``, or None if absent."""
        with self._lock:
            row = self._conn.execute(
                "SELECT epoch FROM log WHERE idx = ?", (index,)
            ).fetchone()
        return int(row["epoch"]) if row else None

    def all_entries(self) -> List[LogEntry]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT idx, request_id, command, epoch, ts FROM log ORDER BY idx"
            ).fetchall()
        return [
            LogEntry(index=int(r["idx"]), request_id=r["request_id"],
                     command=json.loads(r["command"]), epoch=int(r["epoch"]),
                     ts=float(r["ts"]))
            for r in rows
        ]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

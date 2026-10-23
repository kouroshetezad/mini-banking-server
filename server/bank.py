"""Account store for the Mini Banking Transaction Server.

Durability + ACID are delegated to SQLite:

* Durability  -> ``PRAGMA journal_mode=WAL`` + ``PRAGMA synchronous=FULL`` and an
  explicit ``COMMIT`` before the caller sends a reply.
* Atomicity   -> each operation runs inside a single ``BEGIN IMMEDIATE`` transaction;
  any failure rolls back, leaving the previous balance untouched.
* Consistency -> the balance is checked/written in the same transaction, and a
  ``CHECK (balance >= 0)`` constraint is a last line of defence.
* Isolation   -> ``BEGIN IMMEDIATE`` takes the write lock up front, so a read-modify-write
  on an account is serialised against every other writer. Because all balance-changing
  operations are single-row updates, the total money changes only through
  deposits/withdrawals, and running them in parallel is equivalent to some serial order.

Money is stored as integer **cents** to avoid floating point drift.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from typing import Dict, Optional

# Seed accounts used when the database is created for the first time.
DEFAULT_ACCOUNTS: Dict[str, int] = {
    "A1001": 100000,  # 1000.00
    "A1002": 250000,  # 2500.00
    "A1003": 5000,    # 50.00
}


class BankError(Exception):
    """An error that should be reported to the client (not a server bug)."""

    def __init__(self, code: str, message: str, **extra):
        super().__init__(message)
        self.code = code
        self.message = message
        self.extra = extra


@dataclass
class Result:
    account: str
    balance_cents: int

    @property
    def balance(self) -> float:
        return self.balance_cents / 100.0


@dataclass
class TransferResult:
    account: str
    balance_cents: int
    to: str
    to_balance_cents: int

    @property
    def balance(self) -> float:
        return self.balance_cents / 100.0

    @property
    def to_balance(self) -> float:
        return self.to_balance_cents / 100.0


MAX_AMOUNT = 1_000_000_000


def _to_cents(amount) -> int:
    """Validate an amount and convert it to a positive integer number of cents.

    Accepts int/float and numeric strings. Rejects booleans, zero, negatives,
    NaN/inf, more than two decimal places and anything non-numeric.
    """
    if isinstance(amount, bool):
        raise BankError("invalid_amount", "amount must be a number")
    if isinstance(amount, str):
        try:
            amount = float(amount)
        except ValueError:
            raise BankError("invalid_amount", f"amount is not a number: {amount!r}")
    if not isinstance(amount, (int, float)):
        raise BankError("invalid_amount", "amount must be a number")
    if amount != amount or amount in (float("inf"), float("-inf")):
        raise BankError("invalid_amount", "amount must be a finite number")
    if amount > MAX_AMOUNT:
        raise BankError("invalid_amount", f"amount must not exceed {MAX_AMOUNT}")
    scaled = amount * 100
    if abs(scaled - round(scaled)) > 1e-6:
        raise BankError("invalid_amount", "amount may have at most 2 decimal places")
    cents = round(scaled)
    if cents <= 0:
        raise BankError("invalid_amount", "amount must be greater than zero")
    return cents


class Bank:
    """Thread-safe, process-safe SQLite-backed account store."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        # A single connection shared by all handler threads. sqlite3 serialises access
        # at the C level by default, and we additionally guard writes with _write_lock so
        # a transaction cannot be interleaved with another on the same connection.
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._write_lock = threading.RLock()
        self._configure()
        self._init_schema()

    # -- setup -----------------------------------------------------------------

    def _configure(self) -> None:
        cur = self._conn
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=FULL")     # fsync on commit -> durability
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA busy_timeout=30000")

    def _init_schema(self) -> None:
        with self._write_lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    account TEXT PRIMARY KEY,
                    balance INTEGER NOT NULL CHECK (balance >= 0)
                )
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

    # -- helpers ---------------------------------------------------------------

    def _get_balance_locked(self, account: str) -> int:
        row = self._conn.execute(
            "SELECT balance FROM accounts WHERE account = ?", (account,)
        ).fetchone()
        if row is None:
            raise BankError("unknown_account", f"unknown account: {account}",
                            account=account)
        return int(row["balance"])

    # -- operations ------------------------------------------------------------

    def balance(self, account: str) -> Result:
        if not account:
            raise BankError("missing_field", "account is required")
        with self._write_lock:
            cents = self._get_balance_locked(account)
        return Result(account, cents)

    def deposit(self, account: str, amount) -> Result:
        if not account:
            raise BankError("missing_field", "account is required")
        cents = _to_cents(amount)
        with self._write_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self._get_balance_locked(account)  # unknown-account check + lock
                self._conn.execute(
                    "UPDATE accounts SET balance = balance + ? WHERE account = ?",
                    (cents, account),
                )
                self._conn.commit()                # fsync -> durable before reply
            except BankError:
                self._conn.rollback()
                raise
            except Exception:
                self._conn.rollback()
                raise
            return Result(account, self._get_balance_locked(account))

    def withdraw(self, account: str, amount) -> Result:
        if not account:
            raise BankError("missing_field", "account is required")
        cents = _to_cents(amount)
        with self._write_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                current = self._get_balance_locked(account)
                if current < cents:
                    raise BankError(
                        "insufficient_funds",
                        "insufficient funds",
                        account=account,
                        balance=current / 100.0,
                    )
                self._conn.execute(
                    "UPDATE accounts SET balance = balance - ? WHERE account = ?",
                    (cents, account),
                )
                self._conn.commit()
            except BankError:
                self._conn.rollback()
                raise
            except Exception:
                self._conn.rollback()
                raise
            return Result(account, self._get_balance_locked(account))

    def transfer(self, src: str, dst: str, amount) -> TransferResult:
        """Move money between two accounts as one atomic transaction.

        Both rows are updated inside the same ``BEGIN IMMEDIATE`` transaction,
        so the debit and the credit happen together or not at all: a crash, an
        exception or a failed check between the two updates rolls both back.
        """
        if not src:
            raise BankError("missing_field", "account is required")
        if not dst:
            raise BankError("missing_field", "'to' is required for a transfer")
        if src == dst:
            raise BankError("invalid_transfer",
                            "source and destination accounts must differ")
        cents = _to_cents(amount)
        with self._write_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                src_balance = self._get_balance_locked(src)
                self._get_balance_locked(dst)      # destination must exist
                if src_balance < cents:
                    raise BankError(
                        "insufficient_funds", "insufficient funds",
                        account=src, balance=src_balance / 100.0,
                    )
                self._conn.execute(
                    "UPDATE accounts SET balance = balance - ? WHERE account = ?",
                    (cents, src),
                )
                self._conn.execute(
                    "UPDATE accounts SET balance = balance + ? WHERE account = ?",
                    (cents, dst),
                )
                self._conn.commit()
            except BankError:
                self._conn.rollback()
                raise
            except Exception:
                self._conn.rollback()
                raise
            return TransferResult(src, self._get_balance_locked(src),
                                  dst, self._get_balance_locked(dst))

    def snapshot(self) -> Dict[str, float]:
        with self._write_lock:
            rows = self._conn.execute(
                "SELECT account, balance FROM accounts ORDER BY account"
            ).fetchall()
        return {r["account"]: r["balance"] / 100.0 for r in rows}

    def close(self) -> None:
        with self._write_lock:
            self._conn.close()

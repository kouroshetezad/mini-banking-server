# Mini Banking Transaction Server — Assignment 1 (Distributed Systems)

A small client–server banking system. The server keeps simulated accounts in SQLite and
serves concurrent clients over HTTP. All data is fake — no real banking information.

## Layout

```
server/
  bank_server.py       HTTP server: routing, config, logging, graceful shutdown
  bank.py              Account store + ACID transactions on SQLite (WAL)
  limits.py            hard limit on simultaneous client connections
  config.json          Server configuration (host, port, max clients, db path)
  -- advanced bonus --
  cluster_server.py    Fault-tolerant server (primary/backup fail-over)
  replicated_bank.py   Replicated, idempotent, log-based account store
  cluster_config.json  Sample cluster configuration
client/
  bank_client.py       CLI client; --hosts A,B follows a fail-over
tests/
  test_errors.py              every required error case
  test_atomicity.py           transfer / failed operation leaves both accounts unchanged
  test_max_clients.py         max_clients enforced at connection level
  test_concurrency.py         many concurrent withdrawals, correct final balance
  test_durability.py          kill and restart, balance survives
  test_failover.py            bonus: primary/backup fail-over
  test_exactly_once.py        bonus: no lost / no duplicated write across fail-over
  test_client_failover.py     bonus: the client follows the new primary
  test_failover_under_load.py bonus: kill the primary while clients are writing
run_server.sh          Convenience launcher (single node)
run_client.sh          Convenience launcher
run_cluster.sh         Launch a cluster node (A or B)
demo_cluster.sh        Guided fail-over demo for the video
burst_withdraws.sh     Fire N concurrent withdrawals at a running server
run_tests.sh           Run the whole suite, one verdict per test
requirements.txt       Dependencies

extremeBonus/          Extreme Bonus: network partition / split-brain (+15%)
  witness_server.py      arbiter: grants leadership for one term to one node only
  quorum_server.py       witness-fenced node (reuses replicated_bank.py)
  partition_proxy.py     TCP relay used to cut/restore the link in tests
  test_partition.py      self-contained end-to-end partition scenario
  run_quorum.sh          launcher: witness | A | B
  demo_partition.sh      guided partition demo for the video

report-fa.pdf          Built Persian report (5 pages)
```

## Requirements

- Python 3.9+ (uses only the standard library: `http.server`, `sqlite3`, `json`,
  `threading`, `argparse`). No third-party packages needed to run.

## VMware network setup

The two VMs must be able to reach each other. In VMware use a **Host-only**
(`VMnet1`) or **Bridged** (`VMnet0`) adapter, and make sure both VMs are on the
same VMnet.

| VM         | IP              |
|------------|-----------------|
| **server** | `192.168.64.130` |
| **client** | `192.168.64.129` |

Verify inside each guest with `ip addr`, and from the client VM confirm reachability:

```bash
ping 192.168.64.130
```

Use the server VM's **real IP, not localhost**. If the client cannot connect,
allow the port through the firewall on the server VM:

```bash
sudo ufw allow 8080/tcp          # Ubuntu
```

## Run

Server (on the **server VM**):

```bash
python3 server/bank_server.py --host 0.0.0.0 --port 8080 --max-clients 50 \
    --db server/bank.db
# or: ./run_server.sh
```

Client (on the **client VM**), connecting to the server VM:

```bash
python3 client/bank_client.py --host 192.168.64.130 balance A1001
python3 client/bank_client.py --host 192.168.64.130 deposit A1001 200
python3 client/bank_client.py --host 192.168.64.130 withdraw A1001 50
python3 client/bank_client.py --host 192.168.64.130 transfer A1001 A1002 50
# interactive shell:  python3 client/bank_client.py --host 192.168.64.130
```

## Message format

Requests are JSON over HTTP `POST /` (or `GET /health` for a liveness check).

```json
{ "action": "withdraw", "account": "A1001", "amount": 200 }
```

Success response:

```json
{ "status": "success", "account": "A1001", "balance": 800 }
```

Transfer response (both sides of the move):

```json
{ "status": "success", "action": "transfer", "account": "A1001", "balance": 750,
  "to": "A1002", "to_balance": 2750 }
```

Error response:

```json
{ "status": "error", "error": "insufficient_funds",
  "message": "insufficient funds", "account": "A1001", "balance": 300 }
```

Every response also carries an `X-Request-Id` header matching the log line.

Status codes:

- `400` — the request could not be parsed at all (malformed JSON / random bytes).
- `422` — the JSON was valid but the request was rejected (unknown account,
  insufficient funds, invalid amount, unsupported action, missing field).
- `503` — the maximum number of simultaneous clients has been reached
  (`server_busy`), or the node is not the primary (`not_primary`).

## Configuration

`server/config.json` (overridable by CLI flags / env vars):

| key          | meaning                          |
|--------------|----------------------------------|
| `host`       | bind address                     |
| `port`       | TCP port                         |
| `max_clients`| max simultaneous client connections |
| `db_path`    | SQLite database file             |
| `log_path`   | request log file                 |

`max_clients` is enforced when the connection is accepted: the (max+1)-th concurrent
client gets `503 server_busy` and is disconnected immediately.

## Design notes

See `report-fa.pdf` for the ACID / concurrency discussion.

---

# Fault-Tolerant Banking Cluster

The bonus extends the single-node server into a **primary/backup cluster** so that when the
primary crashes, the backup takes over automatically and no transaction is lost or applied
twice.

## Roles and topology

| VM | Role | Address |
|----|------|---------|
| Server A | banking server (primary) | `192.168.64.130:8080` |
| Server B | banking server (backup)  | `192.168.64.131:8081` |
| Client(s) | send requests           | `192.168.64.129` |

## How a write is committed

1. The client sends a JSON request **with a stable `request_id`** (the bundled client
   generates one automatically).
2. The primary checks its `applied_requests` table: if that id was already applied, it
   returns the **stored** result and does nothing (idempotency).
3. Otherwise it opens one SQLite transaction and applies the command to the balances.
4. It replicates the log record to the backup and **waits for the ack**.
5. Only then does it `COMMIT` locally and reply to the client.

If step 4 fails, the transaction is rolled back — a write that exists on one machine only
is never acknowledged. That ordering is what makes "no transaction is lost" true rather
than merely likely.

Because the commit is durable and the idempotency table is part of the same transaction, a
client that never saw a reply can retry safely after a fail-over: the new primary returns
the original result instead of applying the change twice.

## Fail-over

A node is the leader while it is the designated primary and its peer cannot act as one.
When Server A dies:

- B notices its peer is unreachable and, after a short grace period (default 3 s),
  promotes itself to primary with a **higher epoch**.
- B already holds every record A acknowledged (replication was synchronous), so it serves
  exactly the state A had confirmed.
- Clients retry; the retry is idempotent, so nothing is lost or duplicated. No operator
  steps in.

When a node rejoins it compares its log length and a `(index, request_id)` hash with the
leader's, and adopts the leader's log if they differ.

**Scope note.** With only two nodes there is no third party to break ties, so this design
assumes the two servers are never both alive yet unable to reach each other (a true network
partition). It targets the Advanced Bonus, which is about one server *dying*. A real
partition is handled separately by the **Extreme Bonus** in `extremeBonus/` (see below),
which adds a witness so that at most one side can ever accept a write.

## Running the bonus

```bash
# Server A (primary)  -- on 192.168.64.130
./run_cluster.sh A primary 192.168.64.131:8081

# Server B (backup)   -- on 192.168.64.131
./run_cluster.sh B backup  192.168.64.130:8080

# Client: give BOTH addresses so it can follow the fail-over
python3 client/bank_client.py --hosts 192.168.64.130:8080,192.168.64.131:8081 balance A1001
python3 client/bank_client.py --hosts 192.168.64.130:8080,192.168.64.131:8081 deposit A1001 200
python3 client/bank_client.py --hosts 192.168.64.130:8080,192.168.64.131:8081 transfer A1001 A1002 50
```

Cluster endpoints (useful in the demo): `GET /health`, `GET /cluster/status` (role, leader
flag, term, log hash, balances), `GET /cluster/log`, `POST /cluster/ping`,
`POST /cluster/replicate`.

## Automating the demo

```bash
python3 tests/test_failover.py             # kill A, B takes over, no lost write
python3 tests/test_exactly_once.py         # lost reply, retried, applied once
python3 tests/test_client_failover.py      # the client itself follows the new primary
python3 tests/test_failover_under_load.py  # kill A while 6 clients are writing
```

`demo_cluster.sh` walks through the video scenario (normal ops, error cases, concurrent
clients, then a fail-over) using both node addresses:

```bash
./demo_cluster.sh 192.168.64.130:8080 192.168.64.131:8081
```

A Persian step-by-step operating guide is described in the Extreme Bonus section below.

---

# Extreme Bonus: Network Partition and Split Brain (+15%)

The Advanced Bonus only distinguishes "peer is dead" — and from inside a node, a *dead peer*
and a *cut network* look identical. So under a real partition both sides promote themselves
and the balances diverge (**split brain**). Two servers simply cannot break that tie; a third
opinion is required.

`extremeBonus/` adds a **witness** on a third machine (VM 4). It holds no balances and
processes no transactions — it only answers "may this node lead in this term?" and grants
**one owner per term**. A node that cannot get a grant stays read-only and rejects writes
with `503 not_primary`. If the witness is unreachable, nodes never self-promote: consistency
is preferred over availability, which is the deliberate CAP choice.

This is the **arbiter / quorum** pattern, not full Raft or Paxos — no claim is made otherwise.

Layout: the witness runs on VM 4 (`192.168.64.132:8099`); Servers A and B keep their Advanced
Bonus addresses. See the commands below for the 4-VM run steps and the exact
`iptables` cut/heal commands.

```bash
# VM 4
./extremeBonus/run_quorum.sh witness
# VM 1 / VM 2
./extremeBonus/run_quorum.sh A 192.168.64.131:8081
./extremeBonus/run_quorum.sh B 192.168.64.130:8080
# any VM: full automated partition scenario
python3 extremeBonus/test_partition.py
```

---

# Testing

```bash
./run_tests.sh          # whole suite, one PASS/FAIL per test (LF-only, for the Linux VMs)
```

The individual scripts under `tests/` each start their own servers on high ports and clean
up after themselves, so the suite runs on a single machine.




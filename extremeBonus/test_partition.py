#!/usr/bin/env python3
"""Self-test for the Extreme Bonus: witness-fenced leadership across a partition.

This script starts a witness, two quorum nodes and two partition proxies on
loopback, then walks the full scenario the assignment asks for:

  1. normal operation                    (A leads, writes replicate to B)
  2. cut the link between the servers    (both processes stay alive)
  3. assert NO SPLIT BRAIN               (at most one node may accept a write)
  4. heal the link                       (the nodes converge again)
  5. final balances match on both sides

Run from the repository root:

    python3 extremeBonus/test_partition.py      # or `python` on Windows
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PY = sys.executable

WITNESS = 9400
A_PORT = 9401
B_PORT = 9402
A_PROXY = 9411          # A's view of B
B_PROXY = 9412          # B's view of A

WITNESS_URL = f"http://127.0.0.1:{WITNESS}"
A_URL = f"http://127.0.0.1:{A_PORT}"
B_URL = f"http://127.0.0.1:{B_PORT}"

procs: list[subprocess.Popen] = []


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def post(url: str, payload: dict, timeout: float = 3.0) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def get(url: str, timeout: float = 3.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


def try_post(url: str, payload: dict, timeout: float = 3.0):
    """Return (http_code, body).  Never raises for an HTTP error status."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return None, {"error": repr(e)}


def spawn(args: list[str], env_extra: dict | None = None) -> subprocess.Popen:
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    p = subprocess.Popen([PY] + args, cwd=ROOT, env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    procs.append(p)
    return p


def cleanup() -> None:
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass
    for p in procs:
        try:
            p.wait(timeout=3)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass
    procs.clear()


def start_stack() -> None:
    """Bring up witness, both nodes and both proxies."""
    spawn(["extremeBonus/witness_server.py", "--host", "127.0.0.1",
           "--port", str(WITNESS), "--log", "extremeBonus/_t_witness.log"])
    time.sleep(1.0)

    spawn(["extremeBonus/partition_proxy.py",
           "--listen", f"127.0.0.1:{A_PROXY}", "--target", f"127.0.0.1:{B_PORT}"])
    spawn(["extremeBonus/partition_proxy.py",
           "--listen", f"127.0.0.1:{B_PROXY}", "--target", f"127.0.0.1:{A_PORT}"])
    time.sleep(0.6)

    common = ["--witness", WITNESS_URL]
    spawn(["extremeBonus/quorum_server.py", "--node", "A", "--role", "primary",
           "--host", "127.0.0.1", "--port", str(A_PORT),
           "--peer", f"http://127.0.0.1:{A_PROXY}",
           "--db", "extremeBonus/_t_a.db", "--log", "extremeBonus/_t_a.log",
           *common])
    spawn(["extremeBonus/quorum_server.py", "--node", "B", "--role", "backup",
           "--host", "127.0.0.1", "--port", str(B_PORT),
           "--peer", f"http://127.0.0.1:{B_PROXY}",
           "--db", "extremeBonus/_t_b.db", "--log", "extremeBonus/_t_b.log",
           *common])
    time.sleep(4.5)


def cut_link() -> None:
    """Stop both proxies: each node now sees the peer vanish. Processes live."""
    for p in list(procs):
        cmd = " ".join(p.args) if isinstance(p.args, list) else str(p.args)
        if "partition_proxy.py" in cmd:
            p.terminate()
            try:
                p.wait(timeout=3)
            except Exception:
                p.kill()


def wait_for(pred, seconds: float, label: str) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(0.5)
    print(f"    (timed out waiting for {label})")
    return False


def banner(msg: str) -> None:
    print(f"\n===== {msg} =====")


# --------------------------------------------------------------------------- #
# the scenario
# --------------------------------------------------------------------------- #

def main() -> int:
    for f in ("_t_a.db", "_t_a.db-wal", "_t_a.db-shm",
              "_t_b.db", "_t_b.db-wal", "_t_b.db-shm",
              "_t_a.log", "_t_b.log", "_t_witness.log"):
        try:
            os.remove(os.path.join(HERE, f))
        except OSError:
            pass

    failures = 0
    try:
        banner("1. normal operation")
        start_stack()
        a = get(A_URL + "/cluster/status")
        b = get(B_URL + "/cluster/status")
        print(f"    A leader={a['leader']} term={a['term']} witness_up={a['witness_up']}")
        print(f"    B leader={b['leader']} term={b['term']} reason={b['reason']}")
        if not a["leader"] or b["leader"]:
            print("    FAIL: expected exactly A to be leader")
            failures += 1

        code, body = try_post(A_URL + "/", {"action": "deposit", "account": "A1001",
                                            "amount": 100, "request_id": "t-1"})
        print(f"    write via A -> http {code}, balance {body.get('balance')}")
        if code != 200:
            print("    FAIL: normal write should succeed")
            failures += 1
        a = get(A_URL + "/cluster/status")
        b = get(B_URL + "/cluster/status")
        if a["log_hash"] != b["log_hash"]:
            print("    FAIL: logs should match before the partition")
            failures += 1
        else:
            print(f"    replication ok (both at index {a['last_index']})")

        banner("2. cut the network between the servers (processes stay alive)")
        cut_link()
        print("    proxies stopped: each node can no longer reach its peer")
        wait_for(lambda: not get(A_URL + "/cluster/status")["peer_up"] and
                         not get(B_URL + "/cluster/status")["peer_up"],
                 8, "both nodes to notice the partition")
        # give the backup time to *try* to take over
        time.sleep(5)

        banner("3. during the split: at most one side may accept a write")
        a = get(A_URL + "/cluster/status")
        b = get(B_URL + "/cluster/status")
        print(f"    A leader={a['leader']} solo={a['solo']} term={a['term']} "
              f"reason={a['reason']}")
        print(f"    B leader={b['leader']} solo={b['solo']} term={b['term']} "
              f"reason={b['reason']}")

        writers = [u for u, s in ((A_URL, a), (B_URL, b)) if s["leader"]]
        print(f"    nodes claiming leadership: {len(writers)}  ({writers})")
        if len(writers) > 1:
            print("    FAIL: SPLIT BRAIN - both nodes think they are primary")
            failures += 1
        else:
            print("    OK: no split brain")

        # the decisive test: try a write on BOTH sides
        ca, ba = try_post(A_URL + "/", {"action": "deposit", "account": "A1001",
                                        "amount": 7, "request_id": "split-A"})
        cb, bb = try_post(B_URL + "/", {"action": "deposit", "account": "A1001",
                                        "amount": 9, "request_id": "split-B"})
        print(f"    write to A -> http {ca} ({ba.get('error', ba.get('status'))})")
        print(f"    write to B -> http {cb} ({bb.get('error', bb.get('status'))})")
        accepted = [c for c in (ca, cb) if c == 200]
        if len(accepted) > 1:
            print("    FAIL: both sides accepted a write during the split")
            failures += 1
        else:
            print(f"    OK: exactly {len(accepted)} side accepted a write")

        a = get(A_URL + "/cluster/status")
        b = get(B_URL + "/cluster/status")
        print(f"    balances now: A={a['accounts'].get('A1001')} "
              f"B={b['accounts'].get('A1001')}")

        banner("4. heal the link")
        spawn(["extremeBonus/partition_proxy.py",
               "--listen", f"127.0.0.1:{A_PROXY}", "--target", f"127.0.0.1:{B_PORT}"])
        spawn(["extremeBonus/partition_proxy.py",
               "--listen", f"127.0.0.1:{B_PROXY}", "--target", f"127.0.0.1:{A_PORT}"])
        time.sleep(6)
        a = get(A_URL + "/cluster/status")
        b = get(B_URL + "/cluster/status")
        print(f"    A peer_up={a['peer_up']} leader={a['leader']} idx={a['last_index']}")
        print(f"    B peer_up={b['peer_up']} leader={b['leader']} idx={b['last_index']}")

        banner("5. convergence")
        ok = wait_for(
            lambda: (get(A_URL + "/cluster/status")["log_hash"] ==
                     get(B_URL + "/cluster/status")["log_hash"]),
            15, "the two logs to converge")
        a = get(A_URL + "/cluster/status")
        b = get(B_URL + "/cluster/status")
        print(f"    A hash={a['log_hash']} A1001={a['accounts'].get('A1001')}")
        print(f"    B hash={b['log_hash']} A1001={b['accounts'].get('A1001')}")
        if not ok:
            print("    FAIL: logs did not converge after healing")
            failures += 1
        else:
            print("    OK: logs converged")

        # a write after healing must work again
        leaders = A_URL if a["leader"] else B_URL
        code, body = try_post(leaders + "/", {"action": "deposit", "account": "A1002",
                                              "amount": 11, "request_id": "post-heal"})
        print(f"    write after healing -> http {code}, balance {body.get('balance')}")
        if code != 200:
            print("    FAIL: cluster should serve writes again after healing")
            failures += 1

    finally:
        cleanup()

    banner("RESULT")
    if failures:
        print(f"    {failures} check(s) FAILED")
        return 1
    print("    PARTITION TEST: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

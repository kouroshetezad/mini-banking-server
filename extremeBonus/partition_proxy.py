"""Controllable TCP proxy: used to *simulate* a network partition on one machine.

On the real VMs the partition is a firewall rule or an unplugged link.  For a
reproducible self-test we put this proxy between the two nodes: while it runs,
A<->B traffic flows; when it is stopped, the two nodes see exactly what a real
partition looks like -- the peer simply stops answering, while both processes
keep running.

One proxy is started per direction, so we can sever the link **both ways**
without touching either server process.

Usage:
    python partition_proxy.py --listen 127.0.0.1:9301 --target 127.0.0.1:9300
"""

from __future__ import annotations

import argparse
import socket
import threading


def pump(src: socket.socket, dst: socket.socket) -> None:
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def handle(client: socket.socket, target_host: str, target_port: int) -> None:
    try:
        upstream = socket.create_connection((target_host, target_port), timeout=5)
    except OSError:
        client.close()
        return
    threading.Thread(target=pump, args=(client, upstream), daemon=True).start()
    threading.Thread(target=pump, args=(upstream, client), daemon=True).start()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", required=True, help="host:port to listen on")
    ap.add_argument("--target", required=True, help="host:port to forward to")
    args = ap.parse_args()

    lh, lp = args.listen.rsplit(":", 1)
    th, tp = args.target.rsplit(":", 1)
    lp, tp = int(lp), int(tp)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((lh, lp))
    srv.listen(64)
    print(f"proxy {args.listen} -> {args.target} UP", flush=True)
    try:
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=handle, args=(conn, th, tp), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

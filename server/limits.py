"""A ThreadingHTTPServer that enforces a hard limit on simultaneous clients.

``ThreadingHTTPServer`` happily spawns one thread per accepted connection, so a
``BoundedSemaphore`` checked inside the request handler only limits *requests in
flight*, not *connected clients*.  The assignment asks for the maximum number of
simultaneous clients to be configurable, so the limit is applied where the
connection is accepted: the (max_clients + 1)-th concurrent connection is
answered with ``503 server_busy`` and closed immediately.

The limit comes from the config file / environment / CLI, never from a constant.
"""

from __future__ import annotations

import json
import socket
import threading
from http.server import ThreadingHTTPServer


class LimitedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address, handler, max_clients: int):
        self.max_clients = max(1, int(max_clients))
        self._slots = threading.BoundedSemaphore(self.max_clients)
        self.rejected_connections = 0
        super().__init__(server_address, handler)

    # -- accept path ----------------------------------------------------------

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            self.rejected_connections += 1
            # Answer on a short-lived thread: sending the 503 and draining the
            # request must not stall the accept loop, otherwise a burst of
            # rejected clients would delay every other connection.
            threading.Thread(target=self._reject_busy, args=(request,),
                             daemon=True).start()
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()

    def _reject_busy(self, request) -> None:
        body = json.dumps({
            "status": "error",
            "error": "server_busy",
            "message": f"max simultaneous clients reached ({self.max_clients})",
        }).encode("utf-8")
        response = (
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
            b"Connection: close\r\n\r\n" + body
        )
        try:
            request.sendall(response)
            # Half-close and drain whatever the client already sent: closing a
            # socket that still has unread data makes the kernel send an RST,
            # which can destroy the 503 before the client reads it.
            request.shutdown(socket.SHUT_WR)
            request.settimeout(1.0)
            while request.recv(4096):
                pass
        except OSError:
            pass
        finally:
            try:
                request.close()
            except OSError:
                pass

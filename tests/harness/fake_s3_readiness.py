"""Read-only S3-shaped peer for Compose wiring smoke tests."""

from __future__ import annotations

from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


class Handler(SimpleHTTPRequestHandler):
    def do_HEAD(self) -> None:  # noqa: N802 - stdlib callback name
        if self.path == "/fake-bucket":
            self.send_response(200)
            self.end_headers()
            return
        super().do_HEAD()


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8081), partial(Handler, directory="/srv")).serve_forever()

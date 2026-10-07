from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from http_api_contracts.control import ControlBudgetExhausted, control_budget
from http_api_contracts.metadata_contact import BoundedMetadataContacts, MetadataContactDeferred


def test_unresponsive_contact_returns_within_total_budget_and_holds_actual_capacity():
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    contacts = BoundedMetadataContacts(maximum_contacts=2)

    def held():
        entered.set()
        release.wait(5)
        finished.set()
        return "old response"

    try:
        started = time.monotonic()
        with pytest.raises(ControlBudgetExhausted), control_budget(0.05):
            contacts.call("withheld", held)
        assert time.monotonic() - started < 0.5 and entered.is_set()
        with pytest.raises(MetadataContactDeferred):
            contacts.call("withheld", lambda: "incorrect premature release")
        assert (
            contacts.call("healthy", lambda: "healthy current response")
            == "healthy current response"
        )
    finally:
        release.set()
        assert finished.wait(2)


def test_actual_trickled_http_body_cannot_reset_the_callers_total_allowance():
    contacts = BoundedMetadataContacts()
    stopped = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "30")
            self.end_headers()
            try:
                for _ in range(30):
                    self.wfile.write(b"a")
                    self.wfile.flush()
                    time.sleep(0.025)
            except OSError:
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"

    def read():
        try:
            with httpx.Client(timeout=0.15) as client:
                return client.get(url)
        finally:
            stopped.set()

    try:
        started = time.monotonic()
        with pytest.raises(ControlBudgetExhausted):
            contacts.call(url, read, maximum_seconds=0.15)
        assert time.monotonic() - started < 0.5
        assert contacts.call("independent-registration", lambda: "available") == "available"
    finally:
        assert stopped.wait(3)
        server.shutdown()
        server.server_close()
        thread.join(2)

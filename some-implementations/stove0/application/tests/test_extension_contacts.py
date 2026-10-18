from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from stove0_core.control_contacts import ContactDeferred, ControlContacts


def test_unavailable_component_backoff_does_not_block_another_component(monkeypatch) -> None:
    clock = [0.0]
    monkeypatch.setattr("stove0_core.control_contacts.time.monotonic", lambda: clock[0])
    contacts = ControlContacts(("unavailable", "healthy"))
    calls = []

    def unavailable():
        calls.append("unavailable")
        raise httpx.ConnectTimeout("offline")

    with pytest.raises(httpx.ConnectTimeout):
        contacts.call("unavailable", unavailable)
    for _ in range(10):
        with pytest.raises(ContactDeferred):
            contacts.call("unavailable", unavailable)
        assert contacts.call("healthy", lambda: "accepted") == "accepted"
    assert calls == ["unavailable"]
    clock[0] = 2.0
    assert contacts.call("unavailable", lambda: "recovered") == "recovered"


def test_concurrent_component_contact_defers_without_waiting_for_inflight_call() -> None:
    contacts = ControlContacts(("busy", "healthy"))
    entered, release = threading.Event(), threading.Event()

    def wait():
        entered.set()
        assert release.wait(2)
        return "accepted"

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(contacts.call, "busy", wait)
        assert entered.wait(1)
        try:
            with pytest.raises(ContactDeferred):
                contacts.call("busy", lambda: pytest.fail("duplicate contact"))
            assert contacts.call("healthy", lambda: "accepted") == "accepted"
        finally:
            release.set()
        assert pending.result(timeout=1) == "accepted"

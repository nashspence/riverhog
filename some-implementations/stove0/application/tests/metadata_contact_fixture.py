"""Hold real metadata IO capacity while testing scheduler continuation semantics."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from http_api_contracts.control import ControlBudgetExhausted, control_budget
from http_api_contracts.metadata_contact import BoundedMetadataContacts


@contextmanager
def occupied_metadata_contact(origin: str) -> Iterator[BoundedMetadataContacts]:
    contacts = BoundedMetadataContacts()
    entered, release = threading.Event(), threading.Event()
    workers: list[threading.Thread] = []

    def held() -> None:
        workers.append(threading.current_thread())
        entered.set()
        assert release.wait(5)

    try:
        with pytest.raises(ControlBudgetExhausted), control_budget(0.05):
            contacts.call(origin, held)
        assert entered.wait(2)
        yield contacts
    finally:
        release.set()
        for worker in workers:
            worker.join(2)
            assert not worker.is_alive()

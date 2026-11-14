from __future__ import annotations

import sys

import pytest

from tests.support.retained_resource import fault_native as fault_native


@pytest.fixture(autouse=True)
def actual_fault_owner(request, monkeypatch):
    if sys.platform not in {"linux", "darwin", "win32"}:
        return
    if request.module.__name__.endswith(("test_receiving", "test_construction_receiving")):
        import retained_resource

        native, path = request.getfixturevalue("fault_native")
        monkeypatch.setattr(request.module, "Resource", native.Resource)
        monkeypatch.setattr(request.module, "Borrow", native.Borrow)
        monkeypatch.setattr(retained_resource, "Resource", native.Resource)
        monkeypatch.setattr(retained_resource, "Borrow", native.Borrow)
        monkeypatch.setattr(request.module, "FAULT_EXTENSION", path)

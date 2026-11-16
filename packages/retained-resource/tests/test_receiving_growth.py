from __future__ import annotations

import os
import sys

import pytest
from retained_resource import Borrow, Resource

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"}, reason="native Linux/Darwin POSIX owner backend"
)


def test_normal_native_build_exposes_no_fault_hook():
    resource = Resource("file")
    assert not hasattr(resource, "_test_fault")
    assert not resource.received
    resource.close()
    assert resource.received


def test_original_borrow_and_child_are_independent_receiving(tmp_path):
    parent = Resource("directory")
    child = Resource("file")
    borrowed = Borrow(parent)
    data = tmp_path / "data"
    data.write_bytes(b"x")
    try:
        parent.acquire(os.fsencode(tmp_path))
        borrowed.acquire()
        child.acquire(b"data", parent=parent)
        borrowed.close()
        with pytest.raises(BlockingIOError):
            parent.close()
        child.close()
        parent.close()
        assert parent.received and child.received
    finally:
        borrowed.close()
        child.close()
        parent.close()

from __future__ import annotations

import os
import sys

import pytest
from retained_posix import Borrow, Resource
from retained_posix.growth import acquire, receive
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.support.workspace_reclamation import execution_sample

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"}, reason="native Linux/Darwin POSIX owner backend"
)


@pytest.mark.growth_witness(
    "retained-posix.native-resource-work",
    gates=("unit", "client-platform-qualification"),
)
def test_native_receiving_has_one_lifetime_dispatch_and_complete_control_work(
    tmp_path, record_property
):
    bindings = tuple(growth_binding(operation) for operation in (acquire, receive))
    projection = project_bindings(bindings)
    samples = []
    widths = set()
    for backlog in (1, 17, 1025):
        root = tmp_path / str(backlog)
        root.mkdir(mode=0o700)
        for index in range(backlog):
            (root / str(index)).touch()
        for width in (1, 17, 129):
            path = root / ("x" * width)
            for extent in (0, 17, 65537, 8 * 1024 * 1024):
                with path.open("wb") as data:
                    data.truncate(extent)
                for kind in ("file", "directory"):
                    resource = Resource(kind)
                    target = os.fsencode(path if kind == "file" else root)
                    for operation in (acquire, receive, *([receive] * 17)):
                        before = resource.metrics
                        if operation is acquire:
                            operation(resource, target)
                        else:
                            operation(resource)
                        after = resource.metrics
                        metrics = {
                            name: after[name] - before[name]
                            for name in (
                                "open_calls",
                                "dup_calls",
                                "fstat_calls",
                                "close_calls",
                                "control_octets",
                            )
                        }
                        binding = growth_binding(operation)
                        sample = execution_sample(
                            binding,
                            projection,
                            {"unrelated_files": backlog, "path_octets": len(target)},
                            metrics,
                            {},
                        )
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                        samples.append(sample)
                    assert resource.received and resource.metrics["close_calls"] == 1
                    widths.add(resource.metrics["owner_storage_bytes"])
    assert len(widths) == 1
    record_property("growth_samples", samples)


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

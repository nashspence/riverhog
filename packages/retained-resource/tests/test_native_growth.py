"""Actual native growth observations for each supported backend's resource kinds."""

from __future__ import annotations

import os
import sys

import pytest
from retained_resource import Resource
from retained_resource.growth import acquire, receive
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from scripts.validation_numeric import numeric_policy_from_environment
from tests.support.native_growth import record_native_growth
from tests.support.workspace_reclamation import execution_sample

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin", "win32"}, reason="supported native owner backend"
)


def encoded_path(path):
    return str(path) if sys.platform == "win32" else os.fsencode(path)


def path_octets(path):
    return len(path.encode("utf-16-le")) if isinstance(path, str) else len(path)


@pytest.mark.growth_witness(
    "retained-resource.native-creation-work",
    gates=("unit", "client-platform-qualification"),
)
def test_creation_measures_actual_native_entries_and_full_control_operands(
    tmp_path, record_property
):
    bindings = tuple(growth_binding(operation) for operation in (acquire, receive))
    projection = project_bindings(
        bindings, numeric_policy=numeric_policy_from_environment(os.environ)
    )
    samples = []
    for backlog in (1, 17, 1025):
        root = tmp_path / str(backlog)
        root.mkdir(mode=0o700)
        for index in range(backlog):
            (root / str(index)).touch()
        parent = None
        if sys.platform != "win32":
            parent = Resource("directory")
            parent.acquire(os.fsencode(root))
        try:
            for width in (1, 17, 129):
                for relative in (False,) if sys.platform == "win32" else (False, True):
                    for exclusive in (False, True):
                        name = "x" * width
                        target = root / name
                        path = encoded_path(name if relative else target)
                        owner = Resource("file")
                        for operation in (acquire, receive, *([receive] * 17)):
                            before = owner.metrics
                            if operation is acquire:
                                operation(
                                    owner,
                                    path,
                                    parent=parent if relative else None,
                                    writable=True,
                                    create=True,
                                    exclusive=exclusive,
                                )
                            else:
                                operation(owner)
                            after = owner.metrics
                            metrics = {
                                key: after[key] - before[key]
                                for key in (
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
                                {"unrelated_files": backlog, "path_octets": path_octets(path)},
                                metrics,
                                {},
                            )
                            validate_execution_samples(
                                projection,
                                binding.declaration.id,
                                [sample],
                                numeric_policy=numeric_policy_from_environment(os.environ),
                            )
                            samples.append(sample)
                        assert owner.received and owner.metrics["close_calls"] == 1
                        target.unlink()
                if parent is not None:
                    assert parent.metrics["children"] == 0
        finally:
            if parent is not None:
                parent.close()
    record_native_growth(record_property, samples)


@pytest.mark.growth_witness(
    "retained-resource.native-resource-work",
    gates=("unit", "client-platform-qualification"),
)
def test_native_receiving_has_one_lifetime_dispatch_and_complete_control_work(
    tmp_path, record_property
):
    bindings = tuple(growth_binding(operation) for operation in (acquire, receive))
    projection = project_bindings(
        bindings, numeric_policy=numeric_policy_from_environment(os.environ)
    )
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
                for kind in ("file",) if sys.platform == "win32" else ("file", "directory"):
                    resource = Resource(kind)
                    target = encoded_path(path if kind == "file" else root)
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
                            {"unrelated_files": backlog, "path_octets": path_octets(target)},
                            metrics,
                            {},
                        )
                        validate_execution_samples(
                            projection,
                            binding.declaration.id,
                            [sample],
                            numeric_policy=numeric_policy_from_environment(os.environ),
                        )
                        samples.append(sample)
                    assert resource.received and resource.metrics["close_calls"] == 1
                    widths.add(resource.metrics["owner_storage_bytes"])
    assert len(widths) == 1
    record_native_growth(record_property, samples)

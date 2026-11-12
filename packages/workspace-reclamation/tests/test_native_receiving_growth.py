"""Inclusive native extent and receiving work over independent populations."""

from __future__ import annotations

import os
import sys

import pytest
import workspace_reclamation as receiving
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.support.retained_resource import fault_native as fault_native
from tests.support.workspace_reclamation import execution_sample, observe_reclamation

pytestmark = pytest.mark.skipif(
    sys.platform not in {"linux", "darwin"}, reason="native POSIX owner backend"
)


def _dimensions(path, names, present, extent, budget):
    encoded = tuple(os.fsencode(name) for name in names)
    return {
        "candidate_files": len(names),
        "name_octets": sum(map(len, encoded)),
        "maximum_name_octets": max(map(len, encoded), default=0),
        "retained_files": len(present),
        "retained_octets": len(present) * extent,
        "octet_budget": budget,
        "directory_octets": len(os.fsencode(path)),
    }


def _file(path, extent):
    with path.open("xb") as stream:
        stream.truncate(extent)
    path.chmod(0o600)


@pytest.mark.growth_witness(
    "native-private-file-extent-attempt-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="workspace_reclamation.FileReclamationOwner.reclaim",
)
def test_actual_native_and_python_work_includes_complete_control_widths(
    tmp_path, monkeypatch, record_property
):
    binding = growth_binding(receiving.FileReclamationOwner.reclaim)
    projection = project_bindings((binding,))
    samples = []
    for population in (0, 17, 257):
        for candidates in (0, 1, 17, 129):
            for width in (4, 190):
                for directory_width in (1, 129):
                    for extent in (0, 17, 8 * 1024 * 1024):
                        path = (
                            tmp_path
                            / f"{population}-{candidates}-{width}-{extent}"
                            / ("p" * directory_width)
                        )
                        path.mkdir(parents=True, mode=0o700)
                        for index in range(population):
                            _file(path / f"unclassified-{index}", 0)
                        names = tuple(
                            "x" * width + f"{index:03d}" for index in reversed(range(candidates))
                        )
                        present = names[::2]
                        for name in present:
                            _file(path / name, extent)
                        dimensions = _dimensions(path, names, present, extent, 3)
                        owner = receiving.FileReclamationOwner()
                        with observe_reclamation(
                            monkeypatch, receiving.FileReclamationOwner.reclaim
                        ) as metrics:
                            result = owner.reclaim(path, names=names, octet_budget=3)
                        assert owner.received
                        assert result.removed_octets == metrics["reclaimed_extent_octets"]
                        assert (
                            result.remaining_octets == len(present) * extent - result.removed_octets
                        )
                        assert metrics["open_calls"] == metrics["close_calls"] == 1 + bool(present)
                        assert metrics["encoded_directory_octets"] == dimensions["directory_octets"]
                        sample = execution_sample(binding, projection, dimensions, metrics, {})
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                        samples.append(sample)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "native-private-file-original-receiving-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="workspace_reclamation.FileReclamationOwner.close",
)
def test_original_receiving_does_no_extent_or_namespace_work_on_retries(
    tmp_path, monkeypatch, fault_native, record_property
):
    native, _path = fault_native
    monkeypatch.setattr(receiving, "Resource", native.Resource)
    monkeypatch.setattr(receiving, "Borrow", native.Borrow)
    binding = growth_binding(receiving.FileReclamationOwner.close)
    projection = project_bindings((binding,))
    samples = []
    for width in (1, 17, 129):
        for slot in ("_directory", "_file"):
            for fault in ("before_close", "after_close"):
                path = tmp_path / ("p" * width) / (slot + fault)
                path.mkdir(parents=True, mode=0o700)
                _file(path / "data", 17)
                names = ("data",)
                dimensions = _dimensions(path, names, names, 17, 3)
                owner = receiving.FileReclamationOwner()
                getattr(owner, slot)._test_fault(fault)
                with pytest.raises(RuntimeError):
                    owner.reclaim(path, names=names, octet_budget=3)
                for _retry in range(5):
                    with observe_reclamation(
                        monkeypatch, receiving.FileReclamationOwner.close
                    ) as metrics:
                        owner.close()
                    assert owner.received
                    sample = execution_sample(binding, projection, dimensions, metrics, {})
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                assert (path / "data").stat().st_size == 14
                assert (
                    owner._file.metrics["close_calls"]
                    == owner._directory.metrics["close_calls"]
                    == 1
                )
    record_property("growth_samples", samples)

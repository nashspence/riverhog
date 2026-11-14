from __future__ import annotations

import errno
import os

import pytest
import workspace_reclamation
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from workspace_reclamation import (
    InvalidWorkspaceFiles,
    WorkspaceReclamationCapacity,
)
from workspace_reclamation import (
    reclaim_retired_files as reclaim,
)

from tests.support.workspace_reclamation import (
    execution_sample,
    observe_reclamation,
)
from tests.support.workspace_reclamation import (
    retained_file_receivers as retained_file_receivers,
)

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retained owner backend")


def private_file(path, name, extent):
    target = path / name
    with target.open("xb") as stream:
        stream.truncate(extent)
    target.chmod(0o600)
    return target


@pytest.mark.parametrize(
    "names",
    [
        ["data"],
        ("",),
        (".",),
        ("..",),
        ("../data",),
        ("/data",),
        ("a/b",),
        ("a\x00b",),
        ("data", "data"),
        (1,),
    ],
)
def test_invalid_named_selection_is_rejected_before_opening_custody(
    tmp_path, monkeypatch, names, retained_file_receivers
):
    def opened(*_args, **_kwargs):
        pytest.fail("invalid selection acquired or changed filesystem custody")

    monkeypatch.setattr(os, "open", opened)
    with pytest.raises((TypeError, ValueError)):
        reclaim(tmp_path, names=names, octet_budget=1, receiver=retained_file_receivers())


@pytest.mark.parametrize("budget", [True, False, 0, -1, 1.5])
def test_positive_integer_byte_admission_is_required(tmp_path, budget, retained_file_receivers):
    with pytest.raises(ValueError):
        reclaim(tmp_path, names=(), octet_budget=budget, receiver=retained_file_receivers())


def test_uniqueness_validation_preserves_caller_disposal_order_and_unselected_bytes(
    tmp_path, retained_file_receivers
):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    first = private_file(path, "z", 11)
    first.write_bytes(b"abcdefghijk")
    second = private_file(path, "a", 17)
    unrelated = private_file(path, ".lease", 5)
    progress = reclaim(path, names=("z", "a"), octet_budget=3, receiver=retained_file_receivers())
    assert first.read_bytes() == b"abcdefgh"
    assert second.stat().st_size == 17 and unrelated.stat().st_size == 5
    assert (progress.removed_octets, progress.remaining_octets) == (3, 25)
    assert not progress.files_reclaimed


def test_empty_selection_retains_custody_and_unknown_namespace(tmp_path, retained_file_receivers):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    unknown = private_file(path, "unclassified", 7)
    assert reclaim(
        path, names=(), octet_budget=1, receiver=retained_file_receivers()
    ).files_reclaimed
    assert unknown.stat().st_size == 7
    path.chmod(0o755)
    with pytest.raises(InvalidWorkspaceFiles):
        reclaim(path, names=(), octet_budget=1, receiver=retained_file_receivers())


def test_absent_directory_is_not_recreated(tmp_path, retained_file_receivers):
    path = tmp_path / "absent"
    with pytest.raises(FileNotFoundError):
        reclaim(path, names=("data",), octet_budget=1, receiver=retained_file_receivers())
    assert not path.exists()


def test_replaced_inode_is_refused_before_extent_mutation(
    tmp_path, monkeypatch, retained_file_receivers
):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    target = private_file(path, "data", 17)
    original = workspace_reclamation._files
    calls = 0

    def replaced(descriptor, names):
        nonlocal calls
        selected = original(descriptor, names)
        calls += 1
        if calls == 1:
            target.rename(path / "original")
            private_file(path, "data", 19)
        return selected

    with monkeypatch.context() as patch:
        patch.setattr(workspace_reclamation, "_files", replaced)
        with pytest.raises(InvalidWorkspaceFiles, match="replaced"):
            reclaim(path, names=("data",), octet_budget=3, receiver=retained_file_receivers())
    assert target.stat().st_size == 19 and (path / "original").stat().st_size == 17


@pytest.mark.parametrize("replace_directory", [False, True])
def test_held_directory_pin_precedes_every_named_file_probe(
    tmp_path, monkeypatch, retained_file_receivers, replace_directory
):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    target = private_file(path, "data", 7)
    target.write_bytes(b"original")
    held = path.stat()
    receiver = retained_file_receivers()
    actual = workspace_reclamation._encode_directory

    def encode(directory):
        encoded = actual(directory)
        path.rename(tmp_path / "original")
        path.mkdir(mode=0o700)
        private_file(path, "data", 11).write_bytes(b"replacement")
        return encoded

    def forbidden(*_args):
        pytest.fail("replacement directory reached named-file probes")

    if replace_directory:
        monkeypatch.setattr(workspace_reclamation, "_encode_directory", encode)
        monkeypatch.setattr(workspace_reclamation, "_files", forbidden)
        with pytest.raises(InvalidWorkspaceFiles, match="directory was replaced"):
            reclaim(
                path,
                names=("data",),
                octet_budget=3,
                receiver=receiver,
                expected_directory_identity=(held.st_dev, held.st_ino),
            )
        assert not receiver.received
        receiver.close()
        assert receiver.received
        assert target.read_bytes() == b"replacement"
        assert (tmp_path / "original" / "data").read_bytes() == b"original"
    else:
        result = reclaim(
            path,
            names=("data",),
            octet_budget=3,
            receiver=receiver,
            expected_directory_identity=(held.st_dev, held.st_ino),
        )
        assert result.removed_octets == 3 and receiver.received
        assert target.read_bytes() == b"origi"


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
def test_uncertain_unlink_sync_reconciles_actual_absence_without_reconstruction(
    tmp_path, monkeypatch, code, retained_file_receivers
):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    target = private_file(path, "data", 7)

    def failed(_descriptor):
        raise OSError(code, "injected directory sync capacity")

    original_receiver = retained_file_receivers()
    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", failed)
        with pytest.raises(WorkspaceReclamationCapacity):
            reclaim(path, names=("data",), octet_budget=7, receiver=original_receiver)
    assert not target.exists()
    original_receiver.close()
    assert original_receiver.received
    progress = reclaim(path, names=("data",), octet_budget=7, receiver=retained_file_receivers())
    assert progress.files_reclaimed and progress.removed_octets == 0


@pytest.mark.growth_witness(
    "private-workspace-file-extent-reclamation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="workspace_reclamation.reclaim_retired_files",
)
def test_actual_filename_work_and_extents_vary_independently(
    tmp_path, monkeypatch, record_property, retained_file_receivers
):
    binding = growth_binding(reclaim)
    projection = project_bindings((binding,))
    samples = []
    for candidates in (0, 1, 4, 17):
        for width in (4, 190):
            for extent in (0, 17, 65537):
                for budget in (1, 65536):
                    path = tmp_path / f"{candidates}-{width}-{extent}-{budget}"
                    path.mkdir(mode=0o700)
                    names = tuple(
                        "x" * width + f"{index:03d}" for index in reversed(range(candidates))
                    )
                    present = names[::2]
                    for name in present:
                        private_file(path, name, extent)
                    encoded = tuple(os.fsencode(name) for name in names)
                    dimensions = {
                        "candidate_files": candidates,
                        "name_octets": sum(map(len, encoded)),
                        "maximum_name_octets": max(map(len, encoded), default=0),
                        "retained_files": len(present),
                        "retained_octets": len(present) * extent,
                        "octet_budget": budget,
                        "directory_octets": len(os.fsencode(path)),
                    }
                    with observe_reclamation(monkeypatch, reclaim) as metrics:
                        progress = reclaim(
                            path,
                            names=names,
                            octet_budget=budget,
                            receiver=retained_file_receivers(),
                        )
                    assert progress.removed_octets == metrics["reclaimed_extent_octets"]
                    assert (
                        progress.remaining_octets == len(present) * extent - progress.removed_octets
                    )
                    sample = execution_sample(binding, projection, dimensions, metrics, {})
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
    record_property("growth_samples", samples)

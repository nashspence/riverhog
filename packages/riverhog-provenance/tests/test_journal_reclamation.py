from __future__ import annotations

import errno
import os
import subprocess
import sys

import pytest
import riverhog_provenance.journal_reclamation as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import reclaim_retired_journal_workspace as reclaim
from riverhog_provenance.errors import JournalValidationCapacity
from riverhog_provenance.journal_store import InvalidJournalWorkspace

from tests.support.workspace_reclamation import (
    execution_sample,
    observe_file_delegate,
    observe_reclamation,
)
from tests.support.workspace_reclamation import (
    retained_file_receivers as retained_file_receivers,
)

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retained owner backend")


def directory(tmp_path):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    return path


def file(path, name, octets):
    target = path / name
    with target.open("xb") as stream:
        stream.write(bytes(n % 251 for n in range(octets)))
    target.chmod(0o600)
    return target


@pytest.mark.parametrize(
    "budget,extents", [(1, (17, 3, 29)), (4096, (65537, 17, 131073)), (65536, (0, 17, 131073))]
)
def test_each_unit_reclaims_at_most_its_actual_byte_admission(
    tmp_path, budget, extents, retained_file_receivers
):
    path = directory(tmp_path)
    sizes = dict(
        zip(
            ("snapshot.sqlite3-journal", "snapshot.sqlite3-wal", "snapshot.sqlite3"),
            extents,
            strict=True,
        )
    )
    for name, octets in sizes.items():
        file(path, name, octets)
    calls = removed = unlinked = 0
    while True:
        progress = reclaim(path, octet_budget=budget, receiver=retained_file_receivers())
        assert progress.removed_octets <= budget and progress.removed_files <= 1
        removed += progress.removed_octets
        unlinked += progress.removed_files
        calls += 1
        if progress.files_reclaimed:
            break
    assert removed == sum(sizes.values()) and unlinked == len(sizes)
    assert calls == sum(max(1, (n + budget - 1) // budget) for n in sizes.values())
    assert path.is_dir() and reclaim(path, receiver=retained_file_receivers()).files_reclaimed


def test_truncation_preserves_the_remaining_prefix_and_owned_non_sqlite_files(
    tmp_path, retained_file_receivers
):
    path = directory(tmp_path)
    database = file(path, "snapshot.sqlite3", 65537)
    before = database.read_bytes()
    lease = file(path, ".lease", 19)
    other = file(path, "unclassified", 29)
    progress = reclaim(path, octet_budget=4096, receiver=retained_file_receivers())
    assert progress.removed_octets == 4096 and progress.remaining_octets == 65537 - 4096
    assert database.read_bytes() == before[:-4096]
    assert lease.read_bytes() == bytes(range(19)) and other.read_bytes() == bytes(range(29))


@pytest.mark.parametrize("fault", ["symlink", "hardlink", "directory", "mode"])
def test_foreign_or_replaced_file_custody_is_refused_without_mutation(
    tmp_path, fault, retained_file_receivers
):
    path = directory(tmp_path)
    target = path / "snapshot.sqlite3"
    foreign = tmp_path / "foreign"
    foreign.write_bytes(b"unrelated")
    foreign.chmod(0o600)
    if fault == "symlink":
        target.symlink_to(foreign)
    elif fault == "hardlink":
        os.link(foreign, target)
    elif fault == "directory":
        target.mkdir()
    else:
        target.write_bytes(b"owned fixture")
        target.chmod(0o644)
    with pytest.raises(InvalidJournalWorkspace):
        reclaim(path, receiver=retained_file_receivers())
    assert foreign.read_bytes() == b"unrelated" and target.exists()


def test_real_worker_exit_resumes_from_the_already_reclaimed_extent(
    tmp_path, retained_file_receivers
):
    path = directory(tmp_path)
    database = file(path, "snapshot.sqlite3", 65537)
    program = """import os,sys
from pathlib import Path
from riverhog_provenance import reclaim_retired_journal_workspace
from workspace_reclamation import FileReclamationOwner
receiver=FileReclamationOwner()
progress=reclaim_retired_journal_workspace(Path(sys.argv[1]),receiver=receiver,octet_budget=4096)
assert progress.removed_octets==4096
assert receiver.received
os._exit(81)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            str(path),
        ],
        capture_output=True,
    )
    assert result.returncode == 81, result.stderr.decode()
    assert database.stat().st_size == 65537 - 4096
    progress = reclaim(path, octet_budget=4096, receiver=retained_file_receivers())
    assert progress.remaining_octets == 65537 - 8192


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
def test_capacity_failure_after_tail_removal_is_reconciled_without_reconstruction(
    tmp_path, monkeypatch, code, retained_file_receivers
):
    path = directory(tmp_path)
    database = file(path, "snapshot.sqlite3", 65537)

    def failed(_descriptor):
        raise OSError(code, "injected sync capacity")

    original_receiver = retained_file_receivers()
    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", failed)
        with pytest.raises(JournalValidationCapacity):
            reclaim(path, octet_budget=4096, receiver=original_receiver)
    assert database.stat().st_size == 65537 - 4096
    original_receiver.close()
    assert original_receiver.received
    assert (
        reclaim(path, octet_budget=4096, receiver=retained_file_receivers()).remaining_octets
        == 65537 - 8192
    )


@pytest.mark.growth_witness(
    "riverhog-journal-workspace-reclamation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_reclamation.reclaim_retired_journal_workspace",
)
def test_native_reclamation_records_actual_file_work(
    tmp_path, monkeypatch, record_property, retained_file_receivers
):
    binding = growth_binding(reclaim)
    projection = project_bindings((binding,))
    names = (
        "snapshot.sqlite3-journal",
        "snapshot.sqlite3-wal",
        "snapshot.sqlite3-shm",
        "snapshot.sqlite3",
    )
    samples = []
    for files in (0, 1, 4):
        for extent in (0, 17, 65537, 1048577):
            for budget in (1, 65536):
                path = tmp_path / f"{files}-{extent}-{budget}"
                path.mkdir(mode=0o700)
                for name in names[:files]:
                    file(path, name, extent)
                dimensions = {
                    "retained_octets": files * extent,
                    "retained_files": files,
                    "octet_budget": budget,
                    "directory_octets": len(os.fsencode(path)),
                }
                with observe_reclamation(monkeypatch, reclaim) as metrics:
                    with observe_file_delegate(
                        monkeypatch,
                        owner,
                        projection,
                        dimensions,
                        metrics,
                    ) as children:
                        progress = reclaim(
                            path, octet_budget=budget, receiver=retained_file_receivers()
                        )
                assert progress.removed_octets == metrics["reclaimed_extent_octets"]
                assert progress.remaining_octets == files * extent - progress.removed_octets
                assert len(children) == 1
                sample = execution_sample(
                    binding,
                    projection,
                    dimensions,
                    metrics,
                    {"files": children},
                )
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    record_property("growth_samples", samples)

from __future__ import annotations

import errno
import os
import subprocess
import sys

import pytest
import riverhog_provenance.corpus_reclamation as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    CorpusReclamationProgress,
    JournalValidationCapacity,
    StaleCorpusWorkspace,
)
from riverhog_provenance import (
    reclaim_retired_corpus_workspace as reclaim,
)

from tests.support.workspace_reclamation import (
    execution_sample,
    observe_file_delegate,
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


def test_only_corpus_owned_files_are_reclaimed_across_all_admitted_turns(
    tmp_path, retained_file_receivers
):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    names = tuple("corpus.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))
    for name, extent in zip(names, (0, 3, 7, 17), strict=True):
        private_file(path, name, extent)
    others = (".lease", "discovery.sqlite3", "snapshot.sqlite3", "order.sqlite3", "unclassified")
    for name in others:
        private_file(path, name, 19)
    removed = unlinked = 0
    for _ in range(20):
        progress = reclaim(path, octet_budget=3, receiver=retained_file_receivers())
        assert isinstance(progress, CorpusReclamationProgress)
        removed += progress.removed_octets
        unlinked += progress.removed_files
        assert progress.removed_octets <= 3 and progress.removed_files <= 1
        if progress.files_reclaimed:
            break
    assert progress.files_reclaimed and removed == 27 and unlinked == 4
    assert all((path / name).stat().st_size == 19 for name in others)
    assert all(not (path / name).exists() for name in names)


@pytest.mark.parametrize("fault", ["symlink", "hardlink", "mode"])
def test_corpus_custody_fault_preserves_foreign_bytes(tmp_path, fault, retained_file_receivers):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    foreign = private_file(tmp_path, "foreign", 17)
    target = path / "corpus.sqlite3"
    if fault == "symlink":
        target.symlink_to(foreign)
    elif fault == "hardlink":
        os.link(foreign, target)
    else:
        target = private_file(path, "corpus.sqlite3", 19)
        target.chmod(0o644)
    with pytest.raises(StaleCorpusWorkspace):
        reclaim(path, receiver=retained_file_receivers())
    assert foreign.stat().st_size == 17 and target.exists()


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
def test_uncertain_corpus_tail_is_reconciled_under_existing_capacity_error(
    tmp_path, monkeypatch, code, retained_file_receivers
):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    target = private_file(path, "corpus.sqlite3", 17)

    def failed(_descriptor):
        raise OSError(code, "injected file sync capacity")

    original_receiver = retained_file_receivers()
    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", failed)
        with pytest.raises(JournalValidationCapacity):
            reclaim(path, octet_budget=3, receiver=original_receiver)
    assert target.stat().st_size == 14
    original_receiver.close()
    assert original_receiver.received
    assert reclaim(path, octet_budget=3, receiver=retained_file_receivers()).remaining_octets == 11


def test_corpus_process_exit_preserves_actual_progress_for_next_owner(
    tmp_path, retained_file_receivers
):
    path = tmp_path / "retired"
    path.mkdir(mode=0o700)
    target = private_file(path, "corpus.sqlite3", 17)
    program = """import os,sys
from pathlib import Path
from riverhog_provenance import reclaim_retired_corpus_workspace
from workspace_reclamation import FileReclamationOwner
receiver=FileReclamationOwner()
progress=reclaim_retired_corpus_workspace(Path(sys.argv[1]),receiver=receiver,octet_budget=3)
assert progress.removed_octets==3
assert receiver.received
os._exit(81)
"""
    result = subprocess.run([sys.executable, "-c", program, str(path)], capture_output=True)
    assert result.returncode == 81, result.stderr.decode()
    assert target.stat().st_size == 14
    assert reclaim(path, octet_budget=3, receiver=retained_file_receivers()).remaining_octets == 11


@pytest.mark.growth_witness(
    "riverhog-corpus-workspace-reclamation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.corpus_reclamation.reclaim_retired_corpus_workspace",
)
def test_native_corpus_records_actual_delegated_file_work(
    tmp_path, monkeypatch, record_property, retained_file_receivers
):
    binding = growth_binding(reclaim)
    projection = project_bindings((binding,))
    names = tuple("corpus.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))
    samples = []
    for files in (0, 1, 4):
        for extent in (0, 17, 65537):
            for budget in (1, 65536):
                path = tmp_path / f"{files}-{extent}-{budget}"
                path.mkdir(mode=0o700)
                for name in names[:files]:
                    private_file(path, name, extent)
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

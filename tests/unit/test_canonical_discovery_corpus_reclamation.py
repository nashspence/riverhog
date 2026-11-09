from __future__ import annotations

import errno
import os
import subprocess
import sys

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_corpus_reclamation as owner
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.journal_validation_retirement import lease_retiring_journal_workspace
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import JournalValidationCapacity
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from tests.unit.test_canonical_discovery_child_reclamation import current_job
from tests.unit.test_canonical_discovery_metadata import owned
from tests.unit.test_canonical_discovery_retired_metadata import case as case
from tests.unit.test_canonical_discovery_retired_metadata import (
    child_binding,
    register_children,
    retired,
)
from tests.unit.test_canonical_discovery_retired_metadata import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_retired_metadata import original as original
from tests.unit.test_canonical_discovery_retired_metadata import qualified as qualified

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original corpus reclamation")


def private_file(path, name, extent):
    value = path / name
    value.write_bytes(b"x" * extent)
    value.chmod(0o600)
    return value


def prepare_corpus(case, *, pinned=True, extent=17, child=False):
    with owned(case) as (metadata, path):
        metadata.open()
        if pinned:
            job = jobs.pin_discovery_corpus(case.factory, metadata.job, case.frozen)
            assert job is not None
            metadata.job = job
        if child:
            with metadata.transaction() as (db, _):
                register_children(db, metadata.job, [child_binding(metadata.job)])
        if extent is not None:
            private_file(path, "corpus.sqlite3", extent)
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, metadata.job)
            assert job is not None
            job = jobs.advance_discovery_reclamation(session, job, "corpus")
        assert job is not None
    return job, path


def step(case, job, *, budget=3):
    with retired(case, job) as metadata:
        metadata.open()
        return owner.reclaim_discovery_corpus(metadata, octet_budget=budget)


def finish(case, job, *, budget=65536):
    for _ in range(128):
        result = step(case, job, budget=budget)
        assert result.advanced
        job = result.job
        if job.reclamation_phase == "journals_root":
            return job
    pytest.fail("small exact original corpus did not complete its bounded extents")


def test_only_original_corpus_extents_progress_to_root_while_other_owners_remain(case):
    job, path = prepare_corpus(case)
    original = job.binding
    others = ("order.sqlite3", "snapshot.sqlite3", "unclassified")
    for name in others:
        private_file(path, name, 19)
    before = (path / "discovery.sqlite3").read_bytes()
    lease = (path / ".lease").stat().st_ino
    remaining = 17
    while remaining:
        result = step(case, job)
        assert result.advanced and result.files is not None
        assert result.files.removed_octets <= 3
        remaining -= result.files.removed_octets
        if remaining:
            assert result.job == job
            assert (path / "corpus.sqlite3").stat().st_size == remaining
        else:
            assert result.job.reclamation_phase == "journals_root"
            assert result.job.binding == original
            assert current_job(case, job.workspace_id) == result.job
        assert (path / "discovery.sqlite3").read_bytes() == before
        assert (path / ".lease").stat().st_ino == lease
        assert all((path / name).stat().st_size == 19 for name in others)
    assert path.is_dir()


def test_pre_pin_absence_is_synchronized_without_inventing_native_ownership(case, monkeypatch):
    job, path = prepare_corpus(case, pinned=False, extent=None)

    def forbidden(*_args, **_kwargs):
        pytest.fail("pre-pin absence acquired Native disposal authority")

    with monkeypatch.context() as patch:
        patch.setattr(owner, "reclaim_retired_corpus_workspace", forbidden)
        result = step(case, job)
    assert result.advanced and result.files is None
    assert result.job.reclamation_phase == "journals_root"
    assert result.job.source_manifest_sha256 is None and path.is_dir()


@pytest.mark.parametrize(
    "name", ("corpus.sqlite3", "corpus.sqlite3-journal", "corpus.sqlite3-wal", "corpus.sqlite3-shm")
)
def test_unpinned_existing_corpus_name_is_a_fault_and_preserves_bytes(case, name):
    job, path = prepare_corpus(case, pinned=False, extent=None)
    file = private_file(path, name, 17)
    with pytest.raises(owner.LostJournalValidationWorkspace, match="unpinned"):
        step(case, job)
    assert file.stat().st_size == 17 and current_job(case, job.workspace_id) == job


@pytest.mark.parametrize(
    ("values", "constraint"),
    (
        ({"source_manifest_sha256": None}, "pinned_inputs"),
        ({"source_count": None}, "pinned_inputs"),
        ({"selection_manifest_sha256": None}, "pinned_inputs"),
        ({"selection_count": None}, "pinned_inputs"),
        ({"selection_manifest_sha256": "invalid"}, "selection_manifest_sha256_hex"),
    ),
)
def test_catalog_refuses_invalid_pinning_without_disposing_corpus(case, values, constraint):
    job, path = prepare_corpus(case)
    with pytest.raises(IntegrityError, match=constraint):
        with case.factory() as session, session.begin():
            session.execute(
                update(Job).where(Job.workspace_id == job.workspace_id).values(**values)
            )
    assert current_job(case, job.workspace_id) == job
    assert (path / "corpus.sqlite3").stat().st_size == 17


@pytest.mark.parametrize(
    "values",
    (
        {"source_count": "00"},
        {"selection_count": "-1"},
    ),
)
def test_malformed_original_binding_does_not_dispose_corpus(case, values):
    job, path = prepare_corpus(case)
    with case.factory() as session, session.begin():
        session.execute(update(Job).where(Job.workspace_id == job.workspace_id).values(**values))
    job = current_job(case, job.workspace_id)
    with pytest.raises(owner.StaleArchiveTask):
        step(case, job)
    assert (path / "corpus.sqlite3").stat().st_size == 17
    assert current_job(case, job.workspace_id) == job


def test_nonreceived_registered_child_prevents_corpus_disposal(case):
    job, path = prepare_corpus(case, child=True)
    with pytest.raises(owner.LostJournalValidationWorkspace, match="children still require"):
        step(case, job)
    assert (path / "corpus.sqlite3").stat().st_size == 17
    assert current_job(case, job.workspace_id) == job


@pytest.mark.parametrize("damage", ("wrong-phase", "stale-parent", "closed"))
def test_corpus_units_reject_wrong_or_lost_original_custody(case, damage):
    job, path = prepare_corpus(case)
    if damage == "wrong-phase":
        with case.factory() as session, session.begin():
            job = jobs.advance_discovery_reclamation(session, job, "journals_root")
        assert job is not None
    with retired(case, job) as metadata:
        metadata.open()
        if damage == "stale-parent":
            metadata._custody._held = False
        elif damage == "closed":
            metadata.close()
        try:
            with pytest.raises(owner.StaleArchiveTask):
                owner.reclaim_discovery_corpus(metadata, octet_budget=3)
        finally:
            metadata._custody._held = True
    assert (path / "corpus.sqlite3").stat().st_size == 17


@pytest.mark.parametrize("budget", (0, -1, True))
def test_corpus_extent_requires_positive_explicit_admission(case, budget):
    job, path = prepare_corpus(case)
    with pytest.raises(ValueError, match="positive octet budget"):
        step(case, job, budget=budget)
    assert (path / "corpus.sqlite3").stat().st_size == 17


@pytest.mark.parametrize("after", (False, True))
def test_uncertain_last_unlink_is_received_before_corpus_phase_advances(case, monkeypatch, after):
    job, path = prepare_corpus(case, extent=2)
    actual = os.fsync

    def uncertain(descriptor):
        if after:
            actual(descriptor)
        raise OSError(errno.ENOSPC, "actual corpus sync capacity uncertain")

    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", uncertain)
        with pytest.raises(JournalValidationCapacity):
            step(case, job, budget=64)
    assert not (path / "corpus.sqlite3").exists()
    assert current_job(case, job.workspace_id) == job
    result = step(case, job, budget=64)
    assert result.files.files_reclaimed and result.job.reclamation_phase == "journals_root"


@pytest.mark.parametrize("committed", (False, True))
def test_original_corpus_phase_rollback_or_committed_lost_reply(case, monkeypatch, committed):
    job, path = prepare_corpus(case, extent=2)
    actual = jobs.advance_discovery_reclamation
    actual_advance = owner._advance

    def rolled_back(session, before, phase, **kwargs):
        assert actual(session, before, phase, **kwargs) is not None
        raise OSError("corpus phase reply uncertain")

    def lost(metadata):
        actual_advance(metadata)
        raise OSError("corpus phase reply uncertain")

    with monkeypatch.context() as patch:
        if committed:
            patch.setattr(owner, "_advance", lost)
        else:
            patch.setattr(jobs, "advance_discovery_reclamation", rolled_back)
        with pytest.raises(OSError, match="phase reply uncertain"):
            step(case, job, budget=64)
    assert not (path / "corpus.sqlite3").exists()
    current = current_job(case, job.workspace_id)
    if committed:
        assert current.reclamation_phase == "journals_root"
    else:
        assert current == job
        assert step(case, current).job.reclamation_phase == "journals_root"


@pytest.mark.parametrize("storage", ("native", "core"))
@pytest.mark.parametrize("lost_reply", (False, True))
def test_actual_uncertain_owner_close_prevents_parent_disposal_until_received(
    case, qualified, storage, lost_reply
):
    from tests.unit.test_canonical_discovery_corpus_source import seed
    from tests.unit.test_discovery_validation_owner import UncertainClose, unit

    actor = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(actor, case) as work:
            assert work is not None
            selection, _ = seed(work.metadata, qualified)
            native = work.corpus(selection)
            original = work.metadata.job
            if storage == "native":
                native._corpus._db = UncertainClose(native._db, lost_reply=lost_reply)
            else:
                work.metadata._db = UncertainClose(work.metadata._db, lost_reply=lost_reply)
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, original)
            assert job is not None
            job = jobs.advance_discovery_reclamation(session, job, "corpus")
        assert job is not None
        with pytest.raises(OSError, match="unconfirmed"):
            actor.close()
        with lease_retiring_journal_workspace(case.root, job.workspace_id) as busy:
            assert busy is None
        assert actor.active_build_ids() == (case.build_id,)
        actor.close()
        assert actor.active_build_ids() == ()
        assert finish(case, job).reclamation_phase == "journals_root"
    finally:
        actor.close()


def test_actual_process_exit_after_corpus_extent_reconciles_same_original_job(case):
    job, path = prepare_corpus(case)
    script = """import os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.canonical_discovery_corpus_reclamation import reclaim_discovery_corpus
from riverhog_core.journal_validation_retirement import lease_retiring_journal_workspace
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
with factory() as session:
    job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
with lease_retiring_journal_workspace(Path(sys.argv[2]),job.workspace_id) as parent:
    metadata=RetiredDiscoveryMetadata(factory,job,parent)
    metadata.open()
    result=reclaim_discovery_corpus(metadata,octet_budget=3)
    assert result.files.removed_octets==3 and result.job==job
    os._exit(81)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(case.engine.url), str(case.root), job.workspace_id],
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 81, result.stderr.decode()[-1000:]
    assert (path / "corpus.sqlite3").stat().st_size == 14
    assert current_job(case, job.workspace_id) == job
    assert finish(case, job).reclamation_phase == "journals_root"

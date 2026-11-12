from __future__ import annotations

import errno
import os
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_metadata as live
from riverhog_core import canonical_discovery_metadata_reclamation as owner
from riverhog_core.retained_workspace_handles import RetainedWorkspaceHandles
from workspace_reclamation import FileReclamationOwner

from tests.support.retirement_namespace import lease_retiring_journal_workspace
from tests.unit.test_canonical_discovery_metadata import owned
from tests.unit.test_canonical_discovery_root_reclamation import case as case
from tests.unit.test_canonical_discovery_root_reclamation import (
    current_job,
    prepare_root,
    private_file,
)
from tests.unit.test_canonical_discovery_root_reclamation import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_root_reclamation import original as original
from tests.unit.test_canonical_discovery_root_reclamation import qualified as qualified
from tests.unit.test_canonical_discovery_root_reclamation import step as root_step

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original metadata extents")


def prepare_metadata(case):
    job, path = prepare_root(case)
    result = root_step(case, job)
    assert result.metadata_closed and result.job.reclamation_phase == "metadata"
    return result.job, path


@contextmanager
def metadata_lease(case, job):
    receiver = FileReclamationOwner()
    handles = RetainedWorkspaceHandles()
    with lease_retiring_journal_workspace(
        case.root, job.workspace_id, handles=handles
    ) as retirement:
        handles.callback(receiver.close)
        yield retirement, receiver


def step(case, job, *, budget=1024 * 1024):
    with metadata_lease(case, job) as (retirement, receiver):
        assert retirement is not None and retirement.directory is not None
        return owner.reclaim_discovery_metadata(
            case.factory, job, retirement, receiver=receiver, octet_budget=budget
        )


def finish(case, job, *, budget=1024 * 1024):
    for _ in range(8):
        result = step(case, job, budget=budget)
        assert result.advanced
        job = result.job
        if job.reclamation_phase == "directory":
            return job
    pytest.fail("small original metadata did not finish its admitted extents")


def test_actual_core_database_is_truncated_without_reopen_and_other_owners_remain(
    case, monkeypatch
):
    job, path = prepare_metadata(case)
    file = path / "discovery.sqlite3"
    before = file.stat().st_size
    assert before > 3
    lease = (path / ".lease").stat().st_ino
    for name in ("order.sqlite3", "snapshot.sqlite3", "unclassified"):
        private_file(path, name, 17)
    actual = sqlite3.connect

    def forbidden(database, *args, **kwargs):
        if "discovery.sqlite3" in str(database):
            pytest.fail("irreversibly retired Core metadata was reopened")
        return actual(database, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", forbidden)
        result = step(case, job, budget=3)
        assert result.job == job and result.files.removed_octets == 3
        assert file.stat().st_size == before - 3
        result = finish(case, job)
    assert result.state == result.reclamation_phase == "directory"
    assert result.storage_key == job.storage_key
    assert current_job(case, job.workspace_id) == result
    assert not file.exists() and path.is_dir() and (path / ".lease").stat().st_ino == lease
    assert all(
        (path / name).stat().st_size == 17
        for name in ("order.sqlite3", "snapshot.sqlite3", "unclassified")
    )


def test_each_original_core_sidecar_is_admitted_without_touching_native_names(case):
    job, path = prepare_metadata(case)
    for suffix in ("-journal", "-wal", "-shm"):
        private_file(path, "discovery.sqlite3" + suffix, 2)
    result = step(case, job, budget=1)
    assert result.job == job and result.files.removed_octets == 1
    assert (path / "discovery.sqlite3-journal").stat().st_size == 1
    assert finish(case, job).reclamation_phase == "directory"
    assert all(not os.path.lexists(path / name) for name in owner._FILES)


def test_actual_unconfirmed_partial_core_creation_needs_no_header_reconstruction(case, monkeypatch):
    def interrupted(_db):
        raise OSError("actual Core creation stopped before schema completion")

    with owned(case) as (metadata, path):
        with monkeypatch.context() as patch:
            patch.setattr(live, "initialize_archive_framing", interrupted)
            with pytest.raises(OSError, match="schema completion"):
                metadata.open()
        assert metadata._db is not None and not metadata._initialized
        assert metadata.job.storage_state == "unconfirmed"
        metadata.close()
        with case.factory() as session, session.begin():
            job = jobs.retire_discovery_job(session, metadata.job)
        assert job is not None and job.reclamation_phase == "metadata"
        with lease_retiring_journal_workspace(case.root, job.workspace_id) as busy:
            assert busy is None
    assert (path / "discovery.sqlite3").is_file()
    assert finish(case, job).reclamation_phase == "directory"


@pytest.mark.parametrize(
    "prior",
    (
        "journals",
        "corpus.sqlite3",
        "corpus.sqlite3-journal",
        "corpus.sqlite3-wal",
        "corpus.sqlite3-shm",
    ),
)
def test_prior_native_or_root_names_block_metadata_disposal(case, prior):
    job, path = prepare_metadata(case)
    before = (path / "discovery.sqlite3").stat().st_size
    if prior == "journals":
        (path / prior).mkdir(mode=0o700)
    else:
        private_file(path, prior, 17)
    with pytest.raises(owner.LostJournalValidationWorkspace, match="still require receiving"):
        step(case, job)
    assert os.path.lexists(path / prior)
    assert (path / "discovery.sqlite3").stat().st_size == before
    assert current_job(case, job.workspace_id) == job


@pytest.mark.parametrize("damage", ("mode", "hardlink", "symlink"))
def test_unsafe_metadata_file_is_preserved_without_catalog_completion(case, damage):
    job, path = prepare_metadata(case)
    file = path / "discovery.sqlite3"
    if damage == "mode":
        file.chmod(0o644)
    elif damage == "hardlink":
        os.link(file, path / "retained-alias")
    else:
        file.rename(path / "retained-original")
        private_file(path, "foreign", 17)
        file.symlink_to(path / "foreign")
    with pytest.raises(owner.LostJournalValidationWorkspace):
        step(case, job)
    assert os.path.lexists(file) and current_job(case, job.workspace_id) == job


@pytest.mark.parametrize("damage", ("phase", "stale-custody", "original-identity"))
def test_wrong_phase_stale_custody_or_original_identity_never_acquires_extent_authority(
    case, monkeypatch, damage
):
    job, path = prepare_metadata(case)

    def forbidden(*_args, **_kwargs):
        pytest.fail("invalid original metadata acquired neutral extent authority")

    with metadata_lease(case, job) as (retirement, receiver):
        assert retirement is not None
        if damage == "phase":
            value = replace(job, reclamation_phase="journals_root")
        elif damage == "original-identity":
            value = replace(job, policy_sha256="f" * 64)
        else:
            value = job
            retirement.custody._held = False
        try:
            with monkeypatch.context() as patch:
                patch.setattr(owner, "reclaim_retired_files", forbidden)
                with pytest.raises(owner.StaleArchiveTask):
                    owner.reclaim_discovery_metadata(
                        case.factory, value, retirement, receiver=receiver, octet_budget=3
                    )
        finally:
            retirement.custody._held = True
    assert (path / "discovery.sqlite3").is_file()
    assert current_job(case, job.workspace_id) == job


@pytest.mark.parametrize("budget", (0, -1, True))
def test_metadata_extents_require_explicit_positive_byte_admission(case, budget):
    job, path = prepare_metadata(case)
    with pytest.raises(ValueError, match="positive octet budget"):
        step(case, job, budget=budget)
    assert (path / "discovery.sqlite3").is_file()


@pytest.mark.parametrize("stage", ("extent", "final-parent"))
@pytest.mark.parametrize("after", (False, True))
def test_uncertain_sync_never_advances_before_positive_absence_receiving(
    case, monkeypatch, stage, after
):
    job, path = prepare_metadata(case)
    actual = os.fsync
    with metadata_lease(case, job) as (retirement, receiver):
        assert retirement is not None
        parent = retirement.custody._owner

        def uncertain(descriptor):
            selected = stage == "extent" or descriptor == parent
            if selected:
                if after:
                    actual(descriptor)
                raise OSError(errno.ENOSPC, "actual metadata sync uncertain")
            return actual(descriptor)

        with monkeypatch.context() as patch:
            patch.setattr(os, "fsync", uncertain)
            with pytest.raises(owner.JournalValidationCapacity):
                owner.reclaim_discovery_metadata(
                    case.factory, job, retirement, receiver=receiver, octet_budget=1024 * 1024
                )
    assert not (path / "discovery.sqlite3").exists()
    assert current_job(case, job.workspace_id) == job
    assert step(case, job).job.reclamation_phase == "directory"


@pytest.mark.parametrize("committed", (False, True))
def test_metadata_phase_uncertain_reply_preserves_directory_receiving(case, monkeypatch, committed):
    job, path = prepare_metadata(case)
    actual = jobs.advance_discovery_reclamation
    advance = owner._advance

    def rolled_back(session, before, phase, **kwargs):
        assert actual(session, before, phase, **kwargs) is not None
        raise OSError("metadata phase reply uncertain")

    def lost(factory, before, retirement):
        advance(factory, before, retirement)
        raise OSError("metadata phase reply uncertain")

    with monkeypatch.context() as patch:
        if committed:
            patch.setattr(owner, "_advance", lost)
        else:
            patch.setattr(jobs, "advance_discovery_reclamation", rolled_back)
        with pytest.raises(OSError, match="phase reply uncertain"):
            step(case, job)
    assert not (path / "discovery.sqlite3").exists() and path.is_dir()
    current = current_job(case, job.workspace_id)
    if committed:
        assert current.state == current.reclamation_phase == "directory"
    else:
        assert current == job
        assert step(case, current).job.reclamation_phase == "directory"


def test_actual_process_exit_after_metadata_extent_resumes_without_sqlite_reopen(case):
    job, path = prepare_metadata(case)
    before = (path / "discovery.sqlite3").stat().st_size
    script = """import os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from riverhog_core.canonical_discovery_metadata_reclamation import reclaim_discovery_metadata
from tests.support.retirement_namespace import lease_retiring_journal_workspace
from workspace_reclamation import FileReclamationOwner
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
with factory() as session:job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
receiver=FileReclamationOwner()
with lease_retiring_journal_workspace(Path(sys.argv[2]),job.workspace_id) as parent:
    result=reclaim_discovery_metadata(factory,job,parent,receiver=receiver,octet_budget=3)
    assert receiver.received
    assert result.files.removed_octets==3 and result.job==job
    os._exit(84)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(case.engine.url), str(case.root), job.workspace_id],
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 84, result.stderr.decode()[-1000:]
    assert (path / "discovery.sqlite3").stat().st_size == before - 3
    assert current_job(case, job.workspace_id) == job
    assert finish(case, job).reclamation_phase == "directory"

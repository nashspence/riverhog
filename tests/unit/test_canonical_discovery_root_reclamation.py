from __future__ import annotations

import errno
import fcntl
import os
import subprocess
import sys

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_root_reclamation as owner
from riverhog_core.journal_validation_retirement import lease_retiring_journal_workspace
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace

from tests.unit.test_canonical_discovery_corpus_reclamation import case as case
from tests.unit.test_canonical_discovery_corpus_reclamation import (
    current_job,
    prepare_corpus,
    private_file,
)
from tests.unit.test_canonical_discovery_corpus_reclamation import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_reclamation import original as original
from tests.unit.test_canonical_discovery_corpus_reclamation import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_reclamation import step as corpus_step
from tests.unit.test_canonical_discovery_retired_metadata import retired

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX original journals-root receiving")


def prepare_root(case, *, variant="control"):
    job, path = prepare_corpus(case, pinned=False, extent=None)
    job = corpus_step(case, job).job
    assert job.reclamation_phase == "journals_root"
    if variant != "absent":
        root = path / "journals"
        root.mkdir(mode=0o700)
        if variant != "prefix":
            private_file(root, ".coordinator", 0)
        if variant == "unknown":
            private_file(root, "unclassified", 17)
    return job, path


def step(case, job):
    with retired(case, job) as metadata:
        metadata.open()
        result = owner.reclaim_discovery_journals_root(metadata)
        if result.advanced:
            assert metadata._closed and metadata._db is None and metadata._descriptor is None
            assert metadata._child_root is None and metadata._root_coordinator is None
        return result


@pytest.mark.parametrize("variant", ("absent", "prefix", "control"))
def test_root_absence_and_actual_core_close_precede_metadata_phase(case, variant):
    job, path = prepare_root(case, variant=variant)
    before = (path / "discovery.sqlite3").read_bytes()
    lease = (path / ".lease").stat().st_ino
    result = step(case, job)
    assert result.advanced and result.metadata_closed
    assert result.job.reclamation_phase == "metadata"
    assert result.job.storage_key == job.storage_key
    assert current_job(case, job.workspace_id) == result.job
    assert not (path / "journals").exists()
    assert (path / "discovery.sqlite3").read_bytes() == before
    assert (path / ".lease").stat().st_ino == lease and path.is_dir()


@pytest.mark.parametrize("before_control", (False, True))
def test_actual_root_creation_interrupt_or_stale_callback_needs_no_repair(
    case, monkeypatch, before_control
):
    job, path = prepare_root(case, variant="absent")
    actual = os.open

    def interrupted(name, *args, **kwargs):
        if name == ".coordinator":
            raise OSError("actual root creation interrupted before control")
        return actual(name, *args, **kwargs)

    with retired(case, job) as metadata:
        metadata.open()
        with monkeypatch.context() as patch:
            if before_control:
                patch.setattr(os, "open", interrupted)
                with pytest.raises(OSError, match="creation interrupted"):
                    with lease_journal_validation_workspace(
                        path / "journals", "f" * 32, create=True, creation_current=lambda: False
                    ):
                        pytest.fail("interrupted creation entered its body")
            else:
                with lease_journal_validation_workspace(
                    path / "journals", "f" * 32, create=True, creation_current=lambda: False
                ) as stale:
                    assert stale is None
        assert (path / "journals").is_dir()
        assert not (path / "journals" / ("f" * 32)).exists()
        assert metadata.verify_current()
        result = owner.reclaim_discovery_journals_root(metadata)
        assert result.metadata_closed and result.job.reclamation_phase == "metadata"


def test_unknown_root_entry_is_preserved_and_blocks_phase(case):
    job, path = prepare_root(case, variant="unknown")
    with pytest.raises(owner.LostJournalValidationWorkspace, match="unclassified"):
        step(case, job)
    assert (path / "journals" / "unclassified").read_bytes() == b"x" * 17
    assert current_job(case, job.workspace_id) == job
    assert (path / "discovery.sqlite3").is_file()


def test_busy_actual_root_coordinator_retains_core_and_catalog_phase(case):
    job, path = prepare_root(case)
    descriptor = os.open(path / "journals" / ".coordinator", os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with retired(case, job) as metadata:
            metadata.open()
            result = owner.reclaim_discovery_journals_root(metadata)
            assert not result.advanced and not result.metadata_closed and result.job == job
            assert metadata.verify_current()
            assert metadata._child_root is None and metadata._root_coordinator is None
        assert current_job(case, job.workspace_id) == job
        assert (path / "journals" / ".coordinator").is_file()
    finally:
        os.close(descriptor)
    assert step(case, job).job.reclamation_phase == "metadata"


@pytest.mark.parametrize("damage", ("mode", "hardlink", "symlink", "root-file", "root-symlink"))
def test_unsafe_root_or_control_never_grants_namespace_disposal(case, damage):
    job, path = prepare_root(case, variant="absent" if damage.startswith("root-") else "control")
    root = path / "journals"
    if damage == "root-file":
        private_file(path, "journals", 17)
    elif damage == "root-symlink":
        target = path / "other-root"
        target.mkdir(mode=0o700)
        root.symlink_to(target, target_is_directory=True)
    elif damage == "mode":
        (root / ".coordinator").chmod(0o644)
    elif damage == "hardlink":
        os.link(root / ".coordinator", path / "retained-link")
    else:
        (root / ".coordinator").unlink()
        private_file(path, "retained-target", 17)
        (root / ".coordinator").symlink_to(path / "retained-target")
    with pytest.raises(owner.LostJournalValidationWorkspace):
        step(case, job)
    assert os.path.lexists(root)
    assert current_job(case, job.workspace_id) == job


def test_replaced_root_descriptor_preserves_both_namespaces(case, monkeypatch):
    job, path = prepare_root(case)
    actual = os.open
    original = (path / "journals").stat()

    def replaced(name, *args, **kwargs):
        descriptor = actual(name, *args, **kwargs)
        parent = os.fstat(kwargs["dir_fd"]) if name == ".coordinator" else None
        if parent is not None and (parent.st_dev, parent.st_ino) == (
            original.st_dev,
            original.st_ino,
        ):
            (path / "journals").rename(path / "original-journals")
            (path / "journals").mkdir(mode=0o700)
        return descriptor

    with monkeypatch.context() as patch:
        patch.setattr(os, "open", replaced)
        with pytest.raises(owner.LostJournalValidationWorkspace, match="replaced"):
            step(case, job)
    assert (path / "original-journals" / ".coordinator").is_file()
    assert (path / "journals").is_dir()
    assert current_job(case, job.workspace_id) == job


@pytest.mark.parametrize("remaining", ("child", "corpus"))
def test_remaining_child_or_corpus_preserves_root_and_original_phase(case, remaining):
    if remaining == "child":
        job, path = prepare_corpus(case, child=True, extent=None)
        with case.factory() as session, session.begin():
            job = jobs.advance_discovery_reclamation(session, job, "journals_root")
        assert job is not None
    else:
        job, path = prepare_root(case)
        private_file(path, "corpus.sqlite3", 17)
    with pytest.raises(owner.LostJournalValidationWorkspace, match="still require"):
        step(case, job)
    assert current_job(case, job.workspace_id) == job
    assert (path / "discovery.sqlite3").is_file()


@pytest.mark.parametrize("damage", ("phase", "stale", "closed"))
def test_root_receiving_requires_current_original_phase_and_custody(case, damage):
    if damage == "phase":
        job, path = prepare_corpus(case, pinned=False, extent=None)
    else:
        job, path = prepare_root(case)
    with retired(case, job) as metadata:
        metadata.open()
        if damage == "stale":
            metadata._custody._held = False
        elif damage == "closed":
            metadata.close()
        try:
            with pytest.raises(owner.StaleArchiveTask):
                owner.reclaim_discovery_journals_root(metadata)
        finally:
            metadata._custody._held = True
    assert current_job(case, job.workspace_id) == job
    assert (path / "discovery.sqlite3").is_file()


@pytest.mark.parametrize("operation", ("unlink", "rmdir", "fsync"))
@pytest.mark.parametrize("after", (False, True))
def test_root_mutation_or_sync_lost_reply_reconciles_without_control_repair(
    case, monkeypatch, operation, after
):
    job, path = prepare_root(case)
    actual = getattr(os, operation)

    def uncertain(*args, **kwargs):
        if after:
            actual(*args, **kwargs)
        raise OSError(errno.ENOSPC, "actual root operation uncertain")

    with monkeypatch.context() as patch:
        patch.setattr(os, operation, uncertain)
        with pytest.raises(owner.JournalValidationCapacity):
            step(case, job)
    assert current_job(case, job.workspace_id) == job
    assert step(case, job).job.reclamation_phase == "metadata"
    assert not (path / "journals").exists()


@pytest.mark.parametrize("committed", (False, True))
def test_root_phase_rollback_or_committed_lost_reply_preserves_metadata_bytes(
    case, monkeypatch, committed
):
    job, path = prepare_root(case)
    actual = jobs.advance_discovery_reclamation
    advance = owner._advance

    def rolled_back(session, before, phase, **kwargs):
        assert actual(session, before, phase, **kwargs) is not None
        raise OSError("root phase reply uncertain")

    def lost(metadata):
        advance(metadata)
        raise OSError("root phase reply uncertain")

    with monkeypatch.context() as patch:
        if committed:
            patch.setattr(owner, "_advance", lost)
        else:
            patch.setattr(jobs, "advance_discovery_reclamation", rolled_back)
        with pytest.raises(OSError, match="phase reply uncertain"):
            step(case, job)
    assert not (path / "journals").exists()
    assert (path / "discovery.sqlite3").is_file()
    current = current_job(case, job.workspace_id)
    if committed:
        assert current.reclamation_phase == "metadata"
    else:
        assert current == job
        assert step(case, current).job.reclamation_phase == "metadata"


@pytest.mark.parametrize("lost_reply", (False, True))
def test_actual_core_close_uncertainty_retains_parent_and_forbids_phase_advance(case, lost_reply):
    from tests.unit.test_discovery_validation_owner import UncertainClose

    job, path = prepare_root(case)
    with retired(case, job) as metadata:
        metadata.open()
        metadata._db = UncertainClose(metadata._db, lost_reply=lost_reply)
        with pytest.raises(OSError, match="unconfirmed"):
            owner.reclaim_discovery_journals_root(metadata)
        assert not metadata.verify_current() and metadata._db is not None
        assert metadata._descriptor is not None and not metadata._closed
        assert current_job(case, job.workspace_id) == job
        assert not (path / "journals").exists()
        with lease_retiring_journal_workspace(case.root, job.workspace_id) as busy:
            assert busy is None
        metadata.close()
        assert metadata._closed and metadata._db is None and metadata._descriptor is None
    assert step(case, job).job.reclamation_phase == "metadata"


@pytest.mark.parametrize("field", ("_root_coordinator", "_child_root"))
@pytest.mark.parametrize("lost_reply", (False, True))
def test_uncertain_namespace_close_retains_selected_handle_and_parent(
    case, monkeypatch, field, lost_reply
):
    job, path = prepare_root(case)
    actual = os.close
    held = []
    with retired(case, job) as metadata:
        metadata.open()

        def uncertain(descriptor):
            if descriptor == getattr(metadata, field):
                held.append(descriptor)
                if lost_reply:
                    actual(descriptor)
                raise OSError("root descriptor close uncertain")
            actual(descriptor)

        with monkeypatch.context() as patch:
            patch.setattr(os, "close", uncertain)
            with pytest.raises(OSError, match="close uncertain"):
                owner.reclaim_discovery_journals_root(metadata)
        assert held == [getattr(metadata, field)] and not metadata.verify_current()
        assert current_job(case, job.workspace_id) == job
        with lease_retiring_journal_workspace(case.root, job.workspace_id) as busy:
            assert busy is None
        if lost_reply:
            # Receive the selected repeat-close explicitly, without risking a
            # reused descriptor. The first lost reply supplied no completion.
            with monkeypatch.context() as patch:
                patch.setattr(os, "close", lambda fd: None if fd == held[0] else actual(fd))
                metadata.close()
        else:
            metadata.close()
        assert metadata._closed
    assert step(case, job).job.reclamation_phase == "metadata"


@pytest.mark.parametrize("boundary", ("unlink", "rmdir"))
def test_actual_process_exit_receives_same_root_phase_and_original_identity(case, boundary):
    job, path = prepare_root(case)
    script = """import os,sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.canonical_discovery_root_reclamation import reclaim_discovery_journals_root
from riverhog_core.journal_validation_retirement import lease_retiring_journal_workspace
engine=create_catalog_engine(sys.argv[1]);factory=sessionmaker(engine,expire_on_commit=False)
with factory() as session:job=DiscoveryJob.read(session.get(Row,sys.argv[3]))
with lease_retiring_journal_workspace(Path(sys.argv[2]),job.workspace_id) as parent:
    metadata=RetiredDiscoveryMetadata(factory,job,parent);metadata.open()
    actual=getattr(os,sys.argv[4])
    def interrupted(*args,**kwargs):
        actual(*args,**kwargs);os._exit(83)
    setattr(os,sys.argv[4],interrupted)
    reclaim_discovery_journals_root(metadata)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(case.engine.url),
            str(case.root),
            job.workspace_id,
            boundary,
        ],
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 83, result.stderr.decode()[-1000:]
    assert current_job(case, job.workspace_id) == job
    assert step(case, job).job.reclamation_phase == "metadata"
    assert not (path / "journals").exists()

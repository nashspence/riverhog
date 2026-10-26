from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import (
    StaleArchiveTask,
    apply_archive_descriptor,
    archive_metadata_progress,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as State,
)
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)
from sqlalchemy import update

from tests.unit.test_canonical_discovery_corpus_jobs import case as case
from tests.unit.test_canonical_discovery_corpus_jobs import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_jobs import original as original
from tests.unit.test_canonical_discovery_corpus_jobs import qualified as qualified


@contextmanager
def owned(case):
    selected = jobs.select_discovery_job(
        case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
    )
    assert selected is not None
    captured = []
    with lease_journal_validation_workspace(
        case.root,
        selected.workspace_id,
        create=selected.state == "preparing",
        creation_current=lambda: jobs.discovery_worker_current(case.factory, selected),
        on_custody=captured.append,
    ) as path:
        assert path is not None
        job = jobs.claim_discovery_worker(case.factory, selected)
        assert job is not None
        metadata = DiscoveryMetadata(case.factory, job, case.admission, captured[0])
        try:
            yield metadata, path
        finally:
            metadata.close()


def test_owned_metadata_initializes_once_and_resumes_stable_archive_work(case, qualified):
    with owned(case) as (first, path):
        assert not first.verify_current()
        first.open()
        assert first.job.state == "seeding"
        assert first.verify_current()
        with first.read() as (db, _):
            assert db.execute("SELECT format FROM discovery_workspace").fetchone() == (
                "riverhog-discovery-metadata/v13",
            )
            assert db.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='discovery_corpus_replay'"
            ).fetchone() == ("discovery_corpus_replay",)
            assert db.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='discovery_endpoint_tasks'"
            ).fetchone() == ("discovery_endpoint_tasks",)
            assert db.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name='discovery_endpoint_inverse'"
            ).fetchone() == ("discovery_endpoint_inverse",)
        first.open()
        reader = qualified[1][2]
        with first.read() as (db, key):
            prepared = prepare_archive_descriptor(db, key, case.admission, reader)
        with first.transaction() as (db, key):
            assert apply_archive_descriptor(db, key, case.admission, prepared).version == 1
        inode = (path / "discovery.sqlite3").stat().st_ino
        key = first.key
        epoch = first.job.worker_epoch
    assert not first.verify_current()
    with owned(case) as (second, path):
        second.open()
        assert second.key == key
        assert second.job.worker_epoch == epoch + 1
        assert (path / "discovery.sqlite3").stat().st_ino == inode
        with second.read() as (db, current):
            assert current == key
            assert archive_metadata_progress(db, current).version == 1


def test_storage_confirmation_lost_reply_resumes_the_actual_durable_file(case, monkeypatch):
    confirm = jobs.confirm_discovery_storage
    with owned(case) as (first, path):

        def uncertain(factory, job):
            assert confirm(factory, job) is not None
            raise TimeoutError("confirmation reply lost")

        with monkeypatch.context() as patch:
            patch.setattr(jobs, "confirm_discovery_storage", uncertain)
            with pytest.raises(TimeoutError, match="reply lost"):
                first.open()
        assert not first.verify_current()
        inode = (path / "discovery.sqlite3").stat().st_ino
    with owned(case) as (resumed, path):
        resumed.open()
        assert resumed.job.state == "seeding"
        assert (path / "discovery.sqlite3").stat().st_ino == inode
        assert resumed.verify_current()


@pytest.mark.parametrize("damage", ["file", "lease", "header", "schema", "worker"])
def test_changed_owned_metadata_cannot_read_or_commit_and_is_never_repaired(case, damage):
    with owned(case) as (metadata, path):
        metadata.open()
        file = path / "discovery.sqlite3"
        if damage == "file":
            original = file.with_suffix(".held")
            file.rename(original)
            file.write_bytes(original.read_bytes())
            file.chmod(0o600)
        elif damage == "lease":
            (path / ".lease").unlink()
        elif damage == "worker":
            with case.factory() as session, session.begin():
                session.execute(update(State).values(epoch=2))
        else:
            with sqlite3.connect(file) as db:
                if damage == "header":
                    db.execute("UPDATE discovery_workspace SET attempt_id=?", ("e" * 32,))
                else:
                    db.execute("CREATE TABLE incompatible(value TEXT)")
        before = file.read_bytes()
        assert not metadata.verify_current()
        for operation in (metadata.read, metadata.transaction):
            with pytest.raises(StaleArchiveTask):
                with operation():
                    pytest.fail("changed custody was admitted")
        assert file.read_bytes() == before
        if damage == "lease":
            with pytest.raises(LostJournalValidationWorkspace, match="lease was lost"):
                with lease_journal_validation_workspace(
                    case.root, metadata.job.workspace_id, create=True
                ):
                    pytest.fail("Core progress allowed a replacement lease")


def test_stale_fence_rolls_back_actual_bounded_source_registration(case, qualified):
    with owned(case) as (metadata, path):
        metadata.open()
        with metadata.read() as (db, key):
            prepared = prepare_archive_descriptor(db, key, case.admission, qualified[1][2])
        with pytest.raises(StaleArchiveTask):
            with metadata.transaction() as (db, key):
                apply_archive_descriptor(db, key, case.admission, prepared)
                with case.factory() as session, session.begin():
                    session.execute(update(State).values(epoch=2))
        with sqlite3.connect(path / "discovery.sqlite3") as db:
            assert archive_metadata_progress(db, metadata.key).version == 0


def test_missing_confirmed_core_storage_cannot_be_recreated(case):
    with owned(case) as (metadata, path):
        metadata.open()
    (path / "discovery.sqlite3").unlink()
    with owned(case) as (metadata, path):
        with pytest.raises(FileNotFoundError):
            metadata.open()
        assert not (path / "discovery.sqlite3").exists()


def test_existing_partial_core_file_cannot_be_silently_initialized(case):
    with owned(case) as (metadata, path):
        file = path / "discovery.sqlite3"
        file.touch(mode=0o600)
        with pytest.raises(LostJournalValidationWorkspace, match="format"):
            metadata.open()
        assert file.read_bytes() == b""
        assert metadata.job.state == "preparing"


@pytest.mark.parametrize("lost_reply", [False, True])
def test_uncertain_core_close_retains_kernel_custody_until_confirmed(case, lost_reply):
    with owned(case) as (metadata, _):
        metadata.open()
        db = metadata._db

        class UncertainClose:
            attempted = False

            def close(self):
                if not self.attempted:
                    self.attempted = True
                    if lost_reply:
                        db.close()
                    raise OSError("Core close unconfirmed")
                db.close()

        metadata._db = UncertainClose()
        with pytest.raises(OSError, match="unconfirmed"):
            metadata.close()
        assert not metadata.verify_current()
        with lease_journal_validation_workspace(case.root, metadata.job.workspace_id) as busy:
            assert busy is None
        metadata.close()
        with pytest.raises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")
    with lease_journal_validation_workspace(case.root, case.job.workspace_id) as available:
        assert available is not None


def test_owned_metadata_retains_the_actual_authenticated_admission(case, monkeypatch):
    with owned(case) as (metadata, _):
        metadata.open()
        with monkeypatch.context() as patch:
            patch.setattr(
                case.admission,
                "root",
                replace(case.admission.root, journal_count=case.admission.root.journal_count + 1),
            )
            assert not metadata.verify_current()
        assert metadata.verify_current()
        with monkeypatch.context() as patch:
            patch.setattr(case.admission, "archive_sha256", "e" * 64)
            assert not metadata.verify_current()
        assert metadata.verify_current()


def test_stale_preparing_snapshot_cannot_recreate_confirmed_missing_storage(case):
    with owned(case) as (first, path):
        preparing = first.job
        first.open()
        assert first.job.state == "seeding"
        first.close()
        file = path / "discovery.sqlite3"
        file.unlink()
        stale = DiscoveryMetadata(case.factory, preparing, case.admission, first.custody)
        try:
            with pytest.raises(FileNotFoundError):
                stale.open()
            assert not file.exists()
        finally:
            stale.close()


def test_serialized_core_metadata_can_move_between_worker_threads(case):
    from concurrent.futures import ThreadPoolExecutor

    with owned(case) as (metadata, _):
        metadata.open()

        def advance():
            with metadata.transaction() as (db, key):
                return archive_metadata_progress(db, key)

        with ThreadPoolExecutor(max_workers=1) as worker:
            assert worker.submit(advance).result(timeout=10).version == 0
            assert worker.submit(metadata.verify_current).result(timeout=10)
            worker.submit(metadata.close).result(timeout=10)


def test_new_claim_record_cannot_reauthorize_old_core_handles(case):
    with owned(case) as (first, _):
        first.open()
        replacement = jobs.claim_discovery_worker(case.factory, first.job)
        assert replacement is not None
        first.job = replacement
        assert not first.verify_current()
        with pytest.raises(StaleArchiveTask):
            with first.read():
                pass
        second = DiscoveryMetadata(case.factory, replacement, case.admission, first.custody)
        try:
            second.open()
            assert second.verify_current()
            with second.read() as (db, key):
                assert archive_metadata_progress(db, key).version == 0
        finally:
            second.close()


@pytest.mark.parametrize("damage", ["format", "required_table"])
def test_reopen_rejects_incompatible_owned_metadata_without_recreation(case, damage):
    with owned(case) as (metadata, path):
        metadata.open()
    file = path / "discovery.sqlite3"
    with sqlite3.connect(file) as db:
        if damage == "format":
            db.execute("UPDATE discovery_workspace SET format='unrecognized'")
        else:
            db.execute("DROP TABLE discovery_corpus_replay")
    before = file.read_bytes()
    with owned(case) as (metadata, _):
        with pytest.raises(LostJournalValidationWorkspace, match="missing or changed"):
            metadata.open()
    assert file.read_bytes() == before

from __future__ import annotations

import errno
import hashlib
import os
import subprocess
import sys
from contextlib import ExitStack

import pytest
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.journal_validation_retirement import (
    remove_retired_journal_workspace_directory,
)
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)
from riverhog_core.services import collection_uploads as source_owner
from riverhog_core.services import upload_journal_validation_jobs as jobs
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_core.services.upload_journal_validation_retirement import (
    UploadJournalValidationRetirement,
)
from riverhog_provenance.journal_store import JournalStore
from sqlalchemy import delete, event, select

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_staged_journal_range import JOURNAL, Journal
from tests.unit.test_upload_journal_certificates import ready

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX retained owner backend")
ONE, TWO = "a" * 32, "b" * 32


def selected(factory, identity=None):
    with factory() as session:
        query = select(Job)
        if identity is not None:
            query = query.where(Job.workspace_id == identity)
        row = session.scalar(query)
        return None if row is None else jobs.ValidationJob.read(row)


def detached_job(identity, *, state="retiring"):
    return Job(
        workspace_id=identity,
        collection_id=None,
        journal_id=None,
        attempt_id="owned-test-attempt",
        source_binding_json="{}",
        certificate_binding_json="{}",
        worker_epoch=0,
        version="0" * 64,
        state=state,
    )


def finish(current, factory, identity):
    for _ in range(1000):
        if selected(factory, identity) is None:
            return
        assert current.advance(limit=1, octet_budget=65536) == 1
    pytest.fail("small retired workspace did not finish")


def test_source_deletion_queues_bounded_files_then_directory_then_catalog_removal(tmp_path):
    root = tmp_path / "native"
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        work, _ = ready(factory)
        current = UploadJournalValidationOwner(factory, root)
        current.advance(work, catalog=source_owner.admission_provenance_catalog())
        job = selected(factory)
        identity = job.workspace_id
        database = root / job.workspace_id / "snapshot.sqlite3"
        current.close()
        before = hashlib.sha256(database.read_bytes()).hexdigest()
        with factory() as session, session.begin():
            session.execute(delete(Journal))
        orphan = selected(factory)
        assert orphan.workspace_id == job.workspace_id and orphan.collection_id is None
        assert orphan.progress_json == job.progress_json
        cleanup = UploadJournalValidationRetirement(factory, root)
        assert cleanup.advance() == 1 and selected(factory).state == "retiring"
        assert hashlib.sha256(database.read_bytes()).hexdigest() == before
        observed_directory_phase = False
        for _ in range(1000):
            job = selected(factory)
            if job is None:
                break
            old = database.stat().st_size if database.exists() else 0
            if job.state == "directory":
                observed_directory_phase = True
                assert not database.exists() and (root / job.workspace_id / ".lease").exists()
            assert cleanup.advance(octet_budget=65536) == 1
            remaining = database.stat().st_size if database.exists() else 0
            assert 0 <= old - remaining <= 65536
            assert engine.pool.checkedout() == 0
        else:
            pytest.fail("retirement stopped making bounded progress")
        assert observed_directory_phase and not (root / identity).exists()
        assert (root / ".coordinator").is_file() and cleanup.advance() == 0


def test_live_source_remains_ineligible_and_native_bytes_are_preserved(tmp_path):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, _ = ready(factory)
        root = tmp_path / "native"
        current = UploadJournalValidationOwner(factory, root)
        try:
            current.advance(work, catalog=source_owner.admission_provenance_catalog())
            job = selected(factory)
            database = root / job.workspace_id / "snapshot.sqlite3"
            before = database.read_bytes()
            assert UploadJournalValidationRetirement(factory, root).advance(limit=3) == 0
            assert selected(factory) == job and database.read_bytes() == before
        finally:
            current.close()


def test_native_rejection_retires_only_the_derived_workspace_after_publication(tmp_path):
    from riverhog_core.catalog_models import CollectionUploadProvenanceJournalChunkRecord as Chunk

    raw = source(journal_id=JOURNAL) + b"malformed late framing"
    root = tmp_path / "native"
    with retained(tmp_path, raw) as (_, factory):
        work, _ = ready(factory)
        current = UploadJournalValidationOwner(factory, root)
        try:
            for _ in range(10000):
                step = current.advance(work, catalog=source_owner.admission_provenance_catalog())
                if step.outcome is not None:
                    break
            else:
                pytest.fail("small native rejection did not complete")
            assert step.outcome.failure
            job = selected(factory)
            cleanup = UploadJournalValidationRetirement(factory, root)
            assert cleanup.advance() == 0 and job.state == "failed"
            assert source_owner._commit_upload_journal_validation(factory, work, step.outcome)
            assert selected(factory).state == "retiring"
            finish(cleanup, factory, job.workspace_id)
            with factory() as session:
                assert session.get(Journal, (1, JOURNAL)).state == "failed"
                assert (
                    b"".join(session.scalars(select(Chunk.content).order_by(Chunk.ordinal))) == raw
                )
        finally:
            current.close()


def test_old_selected_creator_cannot_recreate_a_removed_job(tmp_path):
    from riverhog_core.services.upload_journal_replay import _binding

    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        work, certificates = ready(factory)
        catalog = source_owner.admission_provenance_catalog()
        job = jobs.select_job(
            factory,
            work,
            source_binding=_binding(work, catalog).to_json(),
            certificate_binding=certificates.binding.to_json(),
        )
        root = tmp_path / "native"
        with lease_journal_validation_workspace(root, TWO, create=True):
            pass
        assert jobs.creation_current(factory, work, job)
        with factory() as session, session.begin():
            session.execute(delete(Journal))
        cleanup = UploadJournalValidationRetirement(factory, root)
        finish(cleanup, factory, job.workspace_id)
        with lease_journal_validation_workspace(
            root,
            job.workspace_id,
            create=True,
            creation_current=lambda: jobs.creation_current(factory, work, job),
        ) as path:
            assert path is None
        assert not (root / job.workspace_id).exists() and (root / TWO).is_dir()


def test_borrowed_reader_blocks_only_its_retirement_and_other_job_finishes(tmp_path):
    root = tmp_path / "native"
    resources = ExitStack()
    path = resources.enter_context(lease_journal_validation_workspace(root, ONE, create=True))
    store = JournalStore.open_retained(path, create=True, on_storage_close=resources.close)
    store.freeze()
    borrowed = store.borrow()
    store.close()
    with lease_journal_validation_workspace(root, TWO, create=True) as second:
        JournalStore.open_retained(second, create=True).close()
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        with factory() as session, session.begin():
            session.add_all([detached_job(ONE), detached_job(TWO)])
        cleanup = UploadJournalValidationRetirement(factory, root)
        try:
            finish(cleanup, factory, TWO)
            assert selected(factory, ONE).state == "retiring"
            assert cleanup.advance() == 0 and (root / ONE / "snapshot.sqlite3").exists()
            assert borrowed.db.execute("SELECT 1").fetchone() == (1,)
        finally:
            borrowed.close()
        finish(cleanup, factory, ONE)
        assert not (root / ONE).exists() and not (root / TWO).exists()


def test_unknown_files_are_preserved_and_do_not_create_a_hot_retry_loop(tmp_path):
    root = tmp_path / "native"
    with lease_journal_validation_workspace(root, ONE, create=True) as directory:
        unknown = directory / "unclassified"
        unknown.write_bytes(b"preserve this owned fixture")
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        with factory() as session, session.begin():
            session.add(detached_job(ONE, state="directory"))
        cleanup = UploadJournalValidationRetirement(factory, root)
        assert cleanup.advance() == 1
        blocked = selected(factory)
        assert blocked.state == "directory" and "unclassified" in blocked.failure
        assert cleanup.advance() == 0 and selected(factory) == blocked
        assert unknown.read_bytes() == b"preserve this owned fixture"
        unknown.unlink()
        assert cleanup.advance() == 1 and selected(factory) is None


@pytest.mark.parametrize("fault", ["lease-unlink", "directory-remove"])
def test_real_worker_exit_finishes_the_durable_directory_phase(tmp_path, fault):
    root = tmp_path / "native"
    with lease_journal_validation_workspace(root, ONE, create=True):
        pass
    program = """import os,sys
from pathlib import Path
from riverhog_core.journal_validation_retirement import remove_retired_journal_workspace_directory
name='unlink' if sys.argv[3]=='lease-unlink' else 'rmdir'
original=getattr(os,name)
def interrupted(*args,**kwargs):
 result=original(*args,**kwargs)
 os._exit(81)
 return result
setattr(os,name,interrupted)
remove_retired_journal_workspace_directory(Path(sys.argv[1]),sys.argv[2])
raise AssertionError('interruption was not reached')
"""
    process = subprocess.run(
        [sys.executable, "-c", program, str(root), ONE, fault], capture_output=True
    )
    assert process.returncode == 81, process.stderr.decode()
    assert remove_retired_journal_workspace_directory(root, ONE)
    assert not (root / ONE).exists() and (root / ".coordinator").is_file()


def test_capacity_after_directory_removal_does_not_lose_the_catalog_pointer(tmp_path, monkeypatch):
    root = tmp_path / "native"
    with lease_journal_validation_workspace(root, ONE, create=True):
        pass
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        with factory() as session, session.begin():
            session.add(detached_job(ONE, state="directory"))
        sync = os.fsync

        def failed(descriptor):
            if not (root / ONE).exists():
                raise OSError(errno.ENOSPC, "injected root synchronization capacity")
            return sync(descriptor)

        cleanup = UploadJournalValidationRetirement(factory, root)
        with monkeypatch.context() as patch:
            patch.setattr(os, "fsync", failed)
            assert cleanup.advance() == 1
        assert selected(factory).state == "directory" and not (root / ONE).exists()
        assert cleanup.advance() == 1 and selected(factory) is None


@pytest.mark.parametrize("fault", ["root", "coordinator", "lease"])
def test_missing_storage_custody_preserves_the_pending_job(tmp_path, fault):
    root = tmp_path / "native"
    with lease_journal_validation_workspace(root, ONE, create=True):
        pass
    if fault == "root":
        root.rename(tmp_path / "unavailable")
    elif fault == "coordinator":
        (root / ".coordinator").unlink()
    else:
        (root / ONE / ".lease").unlink()
    with retained(tmp_path, source(journal_id=JOURNAL)) as (_, factory):
        with factory() as session, session.begin():
            session.add(detached_job(ONE))
        cleanup = UploadJournalValidationRetirement(factory, root)
        assert cleanup.advance() == 1
        pending = selected(factory)
        assert pending.state == "retiring" and pending.failure
        assert cleanup.advance() == 0 and selected(factory) == pending


def test_directory_phase_refuses_a_remaining_owned_database(tmp_path):
    root = tmp_path / "native"
    with lease_journal_validation_workspace(root, ONE, create=True) as directory:
        JournalStore.open_retained(directory, create=True).close()
    with pytest.raises(LostJournalValidationWorkspace, match="still has data"):
        remove_retired_journal_workspace_directory(root, ONE)
    assert (root / ONE / "snapshot.sqlite3").is_file() and (root / ONE / ".lease").is_file()


@pytest.mark.parametrize("count", [1, 17, 129, 1025])
def test_orphan_selection_uses_its_partial_index_without_a_pending_job_census(tmp_path, count):
    with retained(tmp_path, source(journal_id=JOURNAL)) as (engine, factory):
        with factory() as session, session.begin():
            session.add_all(detached_job(f"{number:032x}") for number in range(count))
            orphan = detached_job(f"{count + 1:032x}", state="opening")
            session.add(orphan)
        observed = []

        def record(_connection, _cursor, sql, parameters, _context, _many):
            if "state NOT IN ('retiring','directory')" in sql:
                observed.append((sql, parameters))

        event.listen(engine, "before_cursor_execute", record)
        try:
            cleanup = UploadJournalValidationRetirement(factory, tmp_path / "native")
            first = cleanup._select(())
            assert first.state == "retiring"
            second = cleanup._select(())
            assert second.state == "opening" and second.workspace_id == f"{count + 1:032x}"
        finally:
            event.remove(engine, "before_cursor_execute", record)
        assert observed and engine.pool.checkedout() == 0
        for sql, parameters in observed:
            with engine.connect() as connection:
                with connection.exec_driver_sql("EXPLAIN QUERY PLAN " + sql, parameters) as rows:
                    plan = " ".join(row[3] for row in rows)
            assert "SEARCH" in plan and "ix_journal_validation_orphan_workspace" in plan
            assert "SCAN" not in plan and "USE TEMP" not in plan

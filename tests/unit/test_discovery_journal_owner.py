from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_journal_registration as registration
from riverhog_core.canonical_discovery_archive_framing import ArchiveJournalFramingWork
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import (
    IncompleteSourceError,
    InvalidJournalWorkspace,
    JournalValidationBudget,
    JournalValidationSession,
    iter_journal_frames,
)

from tests.support.canonical_discovery_work import corrected_journal_fixture
from tests.unit.test_canonical_discovery_journal_source import source_case
from tests.unit.test_discovery_validation_owner import UncertainClose


@contextmanager
def prepared(path, *, retained=1):
    fixture, raw, _ = corrected_journal_fixture()
    journal_id = tuple(iter_journal_frames((raw,)))[0].document["journal_id"]
    with source_case(
        path, raw, segments=1, retained=retained, catalog=fixture.catalog, journal_id=journal_id
    ) as (
        original,
        framing,
        _source,
    ):
        case = SimpleNamespace(
            factory=original.factory,
            engine=original.engine,
            admission=original.admission,
            catalog=fixture.catalog,
            build_id=original.metadata.job.build_id,
            workspace_id=original.metadata.job.workspace_id,
            root=original.metadata.custody.directory.parent,
            reader=framing.source.reader,
            journal_id=journal_id,
        )
    case.pool = DiscoveryValidationOwner(case.factory, case.root)
    try:
        yield case
    finally:
        case.pool.close()


@contextmanager
def quantum(case):
    with case.pool.work(
        build_id=case.build_id, admission=case.admission, catalog=case.catalog
    ) as work:
        assert work is not None
        framing = ArchiveJournalFramingWork(
            ArchiveJournalSource(work.metadata, case.reader, case.journal_id)
        )
        yield work, framing


def access(root, identity, *, busy):
    with lease_journal_validation_workspace(root, identity) as path:
        assert (path is None) is busy


def finish(session):
    for _ in range(100000):
        if session.advance(JournalValidationBudget(rows=64)).complete:
            return
    pytest.fail("small actual Native journal did not complete")


def test_source_is_registered_before_native_directory_creation_and_stays_warm(tmp_path):
    with prepared(tmp_path / "warm") as case:
        with quantum(case) as (work, framing):
            source = work.journal_source(framing)
            child_root = work.metadata.custody.directory / "journals"
            assert not child_root.exists()
            with work.metadata.read() as (db, key):
                selected = registration.find_discovery_journal(db, key, framing)
            assert selected.binding == source.binding and not selected.confirmed
            native = work.journal(framing)
            assert native is not None
            before = native.advance(JournalValidationBudget(rows=1))
            assert not before.complete
            epoch = work.metadata.job.worker_epoch
            identity = (child_root / source.binding.directory_id / "snapshot.sqlite3").stat().st_ino
            with work.metadata.read() as (db, key):
                assert registration.find_discovery_journal(db, key, framing).confirmed
        with quantum(case) as (work, framing):
            assert work.metadata.job.worker_epoch == epoch
            assert work.journal_source(framing).binding == source.binding
            assert work.journal(framing) is native
            after = native.advance(JournalValidationBudget(rows=1))
            assert after.version >= before.version
            assert (
                child_root / source.binding.directory_id / "snapshot.sqlite3"
            ).stat().st_ino == identity


def test_real_worker_handoff_resumes_same_child_inode_and_checkpoint(tmp_path):
    with prepared(tmp_path / "replacement") as case:
        with quantum(case) as (work, framing):
            native = work.journal(framing)
            before = native.advance(JournalValidationBudget(rows=1))
            epoch = work.metadata.job.worker_epoch
            binding = work.journal_source(framing).binding
            directory = work.metadata.custody.directory / "journals" / binding.directory_id
            inode = (directory / "snapshot.sqlite3").stat().st_ino
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (work, framing):
            assert work.metadata.job.worker_epoch == epoch + 1
            assert work.journal_source(framing).binding == binding
            resumed = work.journal(framing)
            assert resumed is not native
            assert resumed.progress().accepted_frames == before.accepted_frames
            assert (directory / "snapshot.sqlite3").stat().st_ino == inode
            assert resumed.advance(JournalValidationBudget(rows=1)) is not None


@pytest.mark.parametrize("after", [False, True])
def test_actual_native_close_fault_keeps_both_lease_domains_until_retry(tmp_path, after):
    with prepared(tmp_path / "uncertain") as case:
        with quantum(case) as (work, framing):
            native = work.journal(framing)
            binding = work.journal_source(framing).binding
            child_root = work.metadata.custody.directory / "journals"
            native._store._storage.connection = UncertainClose(native._store.db, lost_reply=after)
        with pytest.raises(OSError, match="unconfirmed"):
            case.pool.close()
        assert case.pool.active_build_ids() == (case.build_id,)
        access(case.root, case.workspace_id, busy=True)
        access(child_root, binding.directory_id, busy=True)
        with pytest.raises(RuntimeError, match="closed"):
            native.advance(JournalValidationBudget(rows=1))
        case.pool.close()
        assert case.pool.active_build_ids() == ()
        access(case.root, case.workspace_id, busy=False)
        access(child_root, binding.directory_id, busy=False)


def test_failed_native_constructor_retains_partial_storage_and_retries_cleanup(
    tmp_path, monkeypatch
):
    initialize = JournalValidationSession._initialize

    def interrupted(session, *args, **kwargs):
        initialize(session, *args, **kwargs)
        session._store._storage.connection = UncertainClose(
            session._store.db, lost_reply=False, failures=2
        )
        raise InterruptedError("actual Native initialization interrupted")

    with prepared(tmp_path / "partial") as case:
        with monkeypatch.context() as patch:
            patch.setattr(JournalValidationSession, "_initialize", interrupted)
            with pytest.raises(OSError, match="unconfirmed"):
                with quantum(case) as (work, framing):
                    binding = work.journal_source(framing).binding
                    child_root = work.metadata.custody.directory / "journals"
                    work.journal(framing)
        assert case.pool.active_build_ids() == (case.build_id,)
        access(case.root, case.workspace_id, busy=True)
        access(child_root, binding.directory_id, busy=True)
        case.pool.release_stale()
        assert case.pool.active_build_ids() == ()
        access(case.root, case.workspace_id, busy=False)
        with quantum(case) as (work, framing):
            assert work.journal_source(framing).binding == binding
            resumed = work.journal(framing)
            assert resumed.advance(JournalValidationBudget(rows=1)) is not None


def test_actual_frozen_borrower_outlives_owner_and_keeps_both_leases(tmp_path):
    results = []
    try:
        with prepared(tmp_path / "borrower") as case:
            with quantum(case) as (work, framing):
                native = work.journal(framing)
                finish(native)
                first = native.result()
                last = first.borrow()
                results.extend((first, last))
                binding = work.journal_source(framing).binding
                child_root = work.metadata.custody.directory / "journals"
            case.pool.close()
            assert case.pool.active_build_ids() == ()
            first.close()
            access(case.root, case.workspace_id, busy=True)
            access(child_root, binding.directory_id, busy=True)
            assert last.journal_sha256 == binding.source_sha256
            last.close()
            access(case.root, case.workspace_id, busy=False)
            access(child_root, binding.directory_id, busy=False)
    finally:
        for result in results:
            result.close()


@pytest.mark.parametrize("committed", [False, True])
def test_lost_registration_confirmation_reopens_actual_file_without_reset(
    tmp_path, monkeypatch, committed
):
    confirm = registration.confirm_discovery_journal

    def uncertain(db, key, framing, selected):
        assert confirm(db, key, framing, selected)
        if committed:
            # A lost reply after the transaction is separately tested by making
            # the following owner quantum fail after successful confirmation.
            return True
        raise TimeoutError("registration confirmation lost")

    with prepared(tmp_path / "lost-reply") as case:
        with monkeypatch.context() as patch:
            patch.setattr(registration, "confirm_discovery_journal", uncertain)
            with pytest.raises(TimeoutError, match="confirmation lost"):
                with quantum(case) as (work, framing):
                    binding = work.journal_source(framing).binding
                    directory = work.metadata.custody.directory / "journals" / binding.directory_id
                    work.journal(framing)
                    raise TimeoutError("registration confirmation lost")
        inode = (directory / "snapshot.sqlite3").stat().st_ino
        with quantum(case) as (work, framing):
            assert work.journal_source(framing).binding == binding
            resumed = work.journal(framing)
            assert (directory / "snapshot.sqlite3").stat().st_ino == inode
            assert resumed.advance(JournalValidationBudget(rows=1)) is not None


def test_busy_child_returns_without_replacing_registration_or_leaking_references(tmp_path):
    with prepared(tmp_path / "busy") as case:
        with quantum(case) as (work, framing):
            binding = work.journal_source(framing).binding
            root = work.metadata.custody.directory / "journals"
            with lease_journal_validation_workspace(
                root,
                binding.directory_id,
                create=True,
                creation_current=work.metadata.verify_current,
            ) as held:
                assert held is not None
                assert work.journal(framing) is None
            assert work.journal_source(framing).binding == binding
            assert work.journal(framing) is not None


def test_confirmed_missing_native_file_marks_job_lost_without_recreation(tmp_path):
    with prepared(tmp_path / "missing") as case:
        with quantum(case) as (work, framing):
            binding = work.journal_source(framing).binding
            assert work.journal(framing) is not None
            directory = work.metadata.custody.directory / "journals" / binding.directory_id
        case.pool.close()
        (directory / "snapshot.sqlite3").unlink()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with pytest.raises((LostJournalValidationWorkspace, InvalidJournalWorkspace)):
            with quantum(case) as (work, framing):
                work.journal(framing)
        selected = jobs.select_discovery_job(
            case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
        )
        assert selected.state == "lost"
        assert not (directory / "snapshot.sqlite3").exists()


@pytest.mark.parametrize("domain", ["child", "parent"])
@pytest.mark.parametrize("after", [False, True])
def test_transferred_reference_close_fault_retains_actual_slot_for_retry(tmp_path, domain, after):
    from tests.unit.test_journal_validation_workspace_reference import CloseFault

    with prepared(tmp_path / "reference-fault") as case:
        with quantum(case) as (work, framing):
            native = work.journal(framing)
            binding = work.journal_source(framing).binding
            child_root = work.metadata.custody.directory / "journals"
            references = work._slot.journal.references
            reference = getattr(references, domain)
            reference._stream = CloseFault(reference._stream, after=after)
        with pytest.raises(OSError, match="close unconfirmed"):
            case.pool.close()
        assert case.pool.active_build_ids() == (case.build_id,)
        assert not reference.closed
        access(case.root, case.workspace_id, busy=True)
        access(child_root, binding.directory_id, busy=True)
        with pytest.raises(RuntimeError, match="closed"):
            native.progress()
        case.pool.close()
        assert case.pool.active_build_ids() == ()
        assert reference.closed
        access(case.root, case.workspace_id, busy=False)
        access(child_root, binding.directory_id, busy=False)


def test_changed_actual_child_custody_rejects_warm_native_advance_without_reset(tmp_path):
    with prepared(tmp_path / "changed-custody") as case:
        with quantum(case) as (work, framing):
            native = work.journal(framing)
            binding = work.journal_source(framing).binding
            directory = work.metadata.custody.directory / "journals" / binding.directory_id
            before = (directory / "snapshot.sqlite3").read_bytes()
            (directory / ".lease").rename(directory / ".retained-original-lease")
            with pytest.raises(IncompleteSourceError, match="no longer current"):
                native.advance(JournalValidationBudget(rows=1))
            assert (directory / "snapshot.sqlite3").read_bytes() == before
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with pytest.raises(LostJournalValidationWorkspace):
            with quantum(case) as (work, framing):
                work.journal(framing)
        assert (directory / "snapshot.sqlite3").read_bytes() == before


def test_confirmed_corrupt_native_file_is_preserved_and_marks_job_lost(tmp_path):
    with prepared(tmp_path / "corrupt-storage") as case:
        with quantum(case) as (work, framing):
            binding = work.journal_source(framing).binding
            assert work.journal(framing) is not None
            directory = work.metadata.custody.directory / "journals" / binding.directory_id
        case.pool.close()
        path = directory / "snapshot.sqlite3"
        before = b"unrecognized owned retained Native format"
        path.write_bytes(before)
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with pytest.raises((InvalidJournalWorkspace, sqlite3.DatabaseError)):
            with quantum(case) as (work, framing):
                work.journal(framing)
        selected = jobs.select_discovery_job(
            case.factory, build_id=case.build_id, admission=case.admission, catalog=case.catalog
        )
        assert selected.state == "lost"
        assert path.read_bytes() == before

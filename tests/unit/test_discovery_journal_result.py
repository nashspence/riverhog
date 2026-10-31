from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_archive_work as archive
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.canonical_discovery_index import begin_index_build
from riverhog_core.catalog_models import CollectionRecord
from riverhog_core.services import discovery_journal_result as result_owner
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import InvalidJournalWorkspace, JournalValidationSession
from riverhog_provenance.journal_store import JournalStore
from sqlalchemy import update

from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission
from tests.unit.test_canonical_discovery_archive_work import root_for
from tests.unit.test_canonical_discovery_index import _collection
from tests.unit.test_discovery_journal_owner import access
from tests.unit.test_discovery_validation_owner import UncertainClose
from tests.unit.test_upload_journal_result import JOURNAL, Job, Journal, sealed, source_owner


@contextmanager
def prepared(path, *, archive_raw=None):
    with sealed(path / "upload") as (engine, factory, native_root, job, raw):
        reader, objects, _, _ = archive_reader_fixture(
            raw if archive_raw is None else archive_raw, journal_id=JOURNAL, segment_ends=()
        )
        admission = discovery_archive_admission(root_for(objects))
        catalog = source_owner.admission_provenance_catalog()
        with factory() as session, session.begin():
            collection = _collection()
            collection.archive_generation = admission.root.archive_generation
            collection.archive_root_sha256 = admission.archive_sha256
            collection.provenance_identity = admission.provenance_sha256
            collection.artifact_set_identity = admission.root.artifact_set_sha256
            session.add(collection)
            session.flush()
            build_id = begin_index_build(session, collection_id=1)
        case = SimpleNamespace(
            engine=engine,
            factory=factory,
            native_root=native_root,
            job=job,
            raw=raw,
            reader=reader,
            admission=admission,
            catalog=catalog,
            build_id=build_id,
            root=path / "discovery",
        )
        case.pool = DiscoveryValidationOwner(factory, case.root, journal_root=native_root)
        try:
            with quantum(case) as (work, _source):
                metadata = work.metadata
                while True:
                    with metadata.read() as (db, key):
                        if archive.archive_metadata_progress(db, key).complete:
                            break
                        page = archive.prepare_archive_descriptor(db, key, admission, reader)
                    with metadata.transaction() as (db, key):
                        archive.apply_archive_descriptor(db, key, admission, page)
                case.workspace_id = metadata.job.workspace_id
            yield case
        finally:
            case.pool.close()


@contextmanager
def quantum(case):
    with case.pool.work(
        build_id=case.build_id, admission=case.admission, catalog=case.catalog
    ) as work:
        assert work is not None
        with work.metadata.read() as (db, key):
            ready = archive.archive_metadata_progress(db, key).complete
        source = ArchiveJournalSource(work.metadata, case.reader, JOURNAL) if ready else None
        yield work, source


def open_result(work, source):
    return work.sealed_journal(source, collection_id=1)


def both(case, *, busy):
    access(case.root, case.workspace_id, busy=busy)
    access(case.native_root, case.job.workspace_id, busy=busy)


def test_actual_sealed_result_is_warm_without_catalog_transaction_or_replay(tmp_path, monkeypatch):
    with prepared(tmp_path) as case:
        native_open = result_owner.open_retained_journal_result
        calls = []

        def admitted(*args, **kwargs):
            assert case.engine.pool.checkedout() == 0
            calls.append(kwargs)
            result = native_open(*args, **kwargs)
            assert case.engine.pool.checkedout() == 0
            return result

        def forbidden(*args, **kwargs):
            pytest.fail("sealed result replayed Native validation or read archive payload")

        with monkeypatch.context() as patch:
            patch.setattr(result_owner, "open_retained_journal_result", admitted)
            patch.setattr(JournalValidationSession, "advance", forbidden)
            patch.setattr(ArchiveJournalSource, "read_window", forbidden)
            patch.setattr(ArchiveJournalSource, "read_exact", forbidden)
            with quantum(case) as (work, source):
                with open_result(work, source) as result:
                    retained = work._slot.sealed
                    binding = retained.admission.binding
                    anchor = result.anchor
                    assert binding.to_json() == case.job.source_binding_json
                    assert retained.admission.confirmed.worker_epoch == case.job.worker_epoch
                    assert retained.opening.closed
                    assert result.journal_bytes == len(case.raw)
                    assert retained.native_source.binding is binding
            with quantum(case) as (work, source):
                with open_result(work, source) as result:
                    assert work._slot.sealed is retained
                    assert result.anchor == anchor
        assert len(calls) == 1 and case.engine.pool.checkedout() == 0


def test_final_independent_borrower_keeps_both_actual_leases_after_owner_closes(tmp_path):
    with prepared(tmp_path) as case:
        with quantum(case) as (work, source):
            first = open_result(work, source)
            last = first.borrow()
            expected = first.anchor
        try:
            case.pool.close()
            assert case.pool.active_build_ids() == ()
            first.close()
            both(case, busy=True)
            assert last.anchor == expected
            last.close()
            both(case, busy=False)
        finally:
            first.close()
            last.close()


def test_busy_actual_upload_workspace_returns_pending_without_replacing_it(tmp_path):
    from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace

    with prepared(tmp_path) as case:
        with lease_journal_validation_workspace(case.native_root, case.job.workspace_id) as held:
            assert held is not None
            with quantum(case) as (work, source):
                assert open_result(work, source) is None
                assert work._slot.sealed is None
        with quantum(case) as (work, source):
            with open_result(work, source) as result:
                assert result.journal_bytes == len(case.raw)


@pytest.mark.parametrize("changed", ["source", "job", "archive", "lease"])
def test_live_fences_reject_warm_result_before_another_borrow(tmp_path, changed):
    with prepared(tmp_path) as case:
        with quantum(case) as (work, source):
            open_result(work, source).close()
            retained = work._slot.sealed
        if changed in {"source", "job", "archive"}:
            with case.factory() as session, session.begin():
                if changed == "source":
                    session.execute(update(Journal).values(capture_version=0))
                elif changed == "job":
                    session.execute(update(Job).values(version="f" * 64))
                else:
                    session.execute(update(CollectionRecord).values(archive_root_sha256="e" * 64))
        else:
            directory = case.native_root / case.job.workspace_id
            (directory / ".lease").rename(directory / ".original-lease")
        assert not retained.native_source.verify_current()
        if changed == "archive":
            with case.pool.work(
                build_id=case.build_id, admission=case.admission, catalog=case.catalog
            ) as work:
                assert work is None
        else:
            with pytest.raises(archive.StaleArchiveTask, match="current custody"):
                with quantum(case) as (work, source):
                    open_result(work, source)
        assert case.pool.active_build_ids() == ()


@pytest.mark.parametrize("after", [False, True])
def test_actual_native_close_fault_retains_selected_slot_and_both_locks(tmp_path, after):
    with prepared(tmp_path) as case:
        with quantum(case) as (work, source):
            open_result(work, source).close()
            retained = work._slot.sealed
            store = retained.result._store
            store._storage.connection = UncertainClose(store.db, lost_reply=after)
        with pytest.raises(OSError, match="unconfirmed"):
            case.pool.close()
        assert case.pool.active_build_ids() == (case.build_id,)
        assert retained.result is not None
        both(case, busy=True)
        case.pool.close()
        assert case.pool.active_build_ids() == ()
        both(case, busy=False)


@pytest.mark.parametrize("after", [False, True])
def test_partial_native_constructor_cleanup_remains_reachable_in_actual_slot(
    tmp_path, monkeypatch, after
):
    restore = JournalStore._restore_workspace

    def interrupted(store):
        restore(store)
        store._storage.connection = UncertainClose(store.db, lost_reply=after, failures=2)
        raise InterruptedError("actual sealed opening interrupted")

    with prepared(tmp_path) as case:
        path = case.native_root / case.job.workspace_id / "snapshot.sqlite3"
        before, inode = path.read_bytes(), path.stat().st_ino
        with monkeypatch.context() as patch:
            patch.setattr(JournalStore, "_restore_workspace", interrupted)
            with pytest.raises(OSError, match="unconfirmed"):
                with quantum(case) as (work, source):
                    open_result(work, source)
        retained = case.pool._slots[case.build_id].sealed
        assert retained.opening is not None and not retained.opening.closed
        assert not retained.transferred and retained.result is None
        both(case, busy=True)
        case.pool.release_stale()
        assert case.pool.active_build_ids() == ()
        both(case, busy=False)
        assert path.read_bytes() == before and path.stat().st_ino == inode


@pytest.mark.parametrize("domain", ["child", "parent"])
def test_actual_reference_release_failure_keeps_slot_for_ordered_retry(tmp_path, domain):
    from tests.unit.test_journal_validation_workspace_reference import CloseFault

    with prepared(tmp_path) as case:
        with quantum(case) as (work, source):
            open_result(work, source).close()
            references = work._slot.sealed.references
            reference = getattr(references, domain)
            reference._stream = CloseFault(reference._stream, after=False)
        with pytest.raises(OSError, match="close unconfirmed"):
            case.pool.close()
        assert not reference.closed and case.pool.active_build_ids() == (case.build_id,)
        both(case, busy=True)
        case.pool.close()
        assert reference.closed
        both(case, busy=False)


def test_another_collection_is_rejected_before_native_opening(tmp_path, monkeypatch):
    with prepared(tmp_path) as case:

        def forbidden(*args, **kwargs):
            pytest.fail("another collection acquired Native storage")

        monkeypatch.setattr(result_owner, "open_retained_journal_result", forbidden)
        with pytest.raises(archive.StaleArchiveTask, match="another collection"):
            with quantum(case) as (work, source):
                work.sealed_journal(source, collection_id=2)


def test_original_full_archive_commitment_must_match_before_native_admission(tmp_path, monkeypatch):
    with prepared(tmp_path, archive_raw=b"different archived journal\n") as case:

        def forbidden(*args, **kwargs):
            pytest.fail("a different original source opened Native storage")

        monkeypatch.setattr(result_owner, "open_retained_journal_result", forbidden)
        with pytest.raises(archive.StaleArchiveTask, match="full original"):
            with quantum(case) as (work, source):
                open_result(work, source)


def test_unsealed_source_has_no_frozen_fast_path(tmp_path):
    with prepared(tmp_path) as case:
        with case.factory() as session, session.begin():
            session.execute(update(Journal).values(state="validating"))
        with quantum(case) as (work, source):
            assert open_result(work, source) is None
            assert work._slot.sealed is None


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_confirmed_missing_or_corrupt_storage_is_never_recreated(tmp_path, damage):
    with prepared(tmp_path) as case:
        path = case.native_root / case.job.workspace_id / "snapshot.sqlite3"
        before = b"unrecognized owned Native storage"
        if damage == "missing":
            path.unlink()
        else:
            path.write_bytes(before)
        with pytest.raises((FileNotFoundError, InvalidJournalWorkspace, sqlite3.DatabaseError)):
            with quantum(case) as (work, source):
                open_result(work, source)
        assert case.pool.active_build_ids() == ()
        if damage == "missing":
            assert not path.exists()
        else:
            assert path.read_bytes() == before

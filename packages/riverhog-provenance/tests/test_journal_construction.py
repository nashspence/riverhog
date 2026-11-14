"""Actual retained Native constructors retain pre-return resource obligations."""

from __future__ import annotations

import os
from threading import Event, Thread

import pytest
import retained_resource
import sqlite_admission
from riverhog_provenance import (
    JournalConstruction,
    JournalStore,
    JournalValidationSession,
    journal_store,
)
from sqlite_admission import SqliteCapacity, SqliteConnectionAdmission

from tests.support.retained_resource import fault_native as fault_native


def pool(*, verify=lambda: None, connections=4):
    return SqliteConnectionAdmission(
        connection_limit=connections,
        cursor_limit=128,
        operation_limit=4,
        verify_process=verify,
    )


def frame(admission):
    return JournalConstruction(admission.connection_scope(), resource_limit=2)


@pytest.fixture(autouse=True)
def native(monkeypatch, fault_native):
    module, _path = fault_native
    monkeypatch.setattr(retained_resource, "Resource", module.Resource)
    return module


@pytest.fixture
def directory(tmp_path):
    path = tmp_path / "journal"
    path.mkdir(mode=0o700)
    return path


def test_all_original_records_are_passive_and_missing_frame_refuses_before_effects(directory):
    calls = []
    admission = pool(verify=lambda: calls.append("verify"))
    original = frame(admission)
    assert not calls and admission.status.connections == 0
    assert original.creator.metrics["open_calls"] == 0
    assert original.directory is None or original.directory.metrics["open_calls"] == 0
    with pytest.raises(TypeError):
        JournalStore.open_retained(directory, create=True)
    with pytest.raises(TypeError, match="original construction"):
        JournalStore.open_retained(directory, construction=object(), create=True)
    assert not calls and not list(directory.iterdir())
    assert original.close_step() and original.received


@pytest.mark.parametrize("limit", [0, -1, False, 0.5])
def test_explicit_file_grant_refuses_without_process_or_files(directory, limit):
    calls = []
    admission = pool(verify=lambda: calls.append("verify"))
    with pytest.raises(ValueError):
        JournalConstruction(admission.connection_scope(), resource_limit=limit)
    assert not calls and not list(directory.iterdir()) and admission.status.connections == 0
    with pytest.raises(TypeError):
        JournalConstruction(object(), resource_limit=2)


def test_failed_shared_reservation_releases_constructor_gate_and_never_creates(directory):
    admission = pool(connections=1)
    occupied = admission.connect(":memory:")
    original = frame(admission)
    with pytest.raises(SqliteCapacity):
        JournalStore.open_retained(directory, construction=original, create=True)
    assert not list(directory.iterdir()) and original.storage is None
    assert original.creator.metrics["open_calls"] == 0
    assert original.close_step() and original.received
    assert admission.status.connections == 1
    occupied.close()
    assert admission.status.connections == 0


def test_process_refusal_precedes_creator_and_same_frame_remains_receivable(directory):
    def refuse():
        raise RuntimeError("process owner refused")

    admission = pool(verify=refuse)
    original = frame(admission)
    with pytest.raises(RuntimeError, match="process owner refused"):
        JournalStore.open_retained(directory, construction=original, create=True)
    assert not list(directory.iterdir()) and original.storage is None
    assert original.close_step() and admission.status.connections == 0


@pytest.mark.parametrize(
    "fault", ["after_open", "signal_after_open", "stat_refusal", "before_close"]
)
def test_creator_fault_before_native_return_keeps_original_scope_and_file(directory, native, fault):
    admission = pool()
    original = frame(admission)
    original.creator._test_fault(fault)
    handles = []
    with pytest.raises((RuntimeError, KeyboardInterrupt, OSError)):
        JournalStore.open_retained(
            directory, construction=original, create=True, on_storage_open=handles.append
        )
    assert not handles and original.storage is None and not original.received
    assert admission.status.connections == 1
    assert original.creator.state in {"live", "rejected"}
    assert original.creator.metrics["open_calls"] == 1
    if fault != "stat_refusal":
        borrowed = native.Borrow(original.creator)
        borrowed.acquire()
        os.fstat(borrowed.fileno())
        borrowed.close()
    assert original.close_step() and original.received
    assert original.creator.metrics["close_calls"] == 1
    assert admission.status.connections == 0
    with pytest.raises(RuntimeError, match="single-attempt"):
        JournalStore.open_retained(directory, construction=original)


@pytest.mark.skipif(os.name != "posix", reason="Native directory sync is POSIX")
@pytest.mark.parametrize("fault", ["after_open", "before_sync", "after_sync", "before_close"])
def test_original_directory_sync_receiving_precedes_external_callback(directory, fault):
    admission = pool()
    original = frame(admission)
    original.directory._test_fault(fault)
    released = []
    with pytest.raises(RuntimeError):
        JournalStore.open_retained(
            directory,
            construction=original,
            create=True,
            on_storage_close=lambda: released.append("parent"),
        )
    assert not released and not original.received
    assert original.close_step() and not released and admission.status.connections == 0


def test_actual_connection_return_loss_uses_its_exact_scope_without_scanning_others(
    directory, monkeypatch
):
    admission = pool()
    original = frame(admission)
    driver = sqlite_admission._connect

    def lose(*args, **options):
        driver(*args, **options)
        raise InterruptedError("original factory return lost")

    with monkeypatch.context() as patch:
        patch.setattr(sqlite_admission, "_connect", lose)
        with pytest.raises(InterruptedError):
            JournalStore.open_retained(directory, construction=original, create=True)
    independent = admission.connect(":memory:")
    monkeypatch.setattr(
        type(admission),
        "pending_connections",
        property(lambda _self: pytest.fail("scanned shared connections")),
    )
    assert original.storage is None and admission.status.connections == 2
    assert original.close_step() and admission.status.connections == 1
    cursor = independent.execute("SELECT 17")
    assert cursor.fetchone() == (17,)
    cursor.close()
    independent.close()


def test_storage_wrapper_failure_retains_original_actual_sqlite(directory, monkeypatch):
    admission = pool()
    original = frame(admission)

    def fail(*_args, **_options):
        raise MemoryError("Native wrapper allocation failed")

    monkeypatch.setattr(journal_store, "_SnapshotStorage", fail)
    with pytest.raises(MemoryError):
        JournalStore.open_retained(directory, construction=original, create=True)
    assert original.storage is None and admission.status.connections == 1
    assert original.close_step() and admission.status.connections == 0


@pytest.mark.parametrize("phase", ["early_callback", "schema"])
def test_original_store_is_retained_before_callback_and_schema_failure(
    directory, monkeypatch, phase
):
    admission = pool()
    original = frame(admission)

    def fail(*_args):
        assert original.storage is not None
        raise InterruptedError("initialization refused")

    options = {}
    if phase == "schema":
        monkeypatch.setattr(JournalStore, "_initialize_schema", fail)
    else:
        options["on_storage_open"] = fail
    with pytest.raises(InterruptedError):
        JournalStore.open_retained(directory, construction=original, create=True, **options)
    assert original.storage is not None
    assert original.close_step() and admission.status.connections == 0


def test_actual_borrower_prevents_scope_emergency_receiving(directory, monkeypatch):
    admission = pool()
    original = frame(admission)
    store = JournalStore.open_retained(directory, construction=original, create=True)
    store.freeze()
    reader = store.borrow()
    close = original.scope.close
    calls = []
    monkeypatch.setattr(original.scope, "close", lambda: calls.append("close"))
    assert not original.close_step()
    assert not calls and not original.received and admission.status.connections == 1
    cursor = reader.db.execute("SELECT 23")
    assert cursor.fetchone() == (23,)
    cursor.close()
    reader.close()
    monkeypatch.setattr(original.scope, "close", close)
    assert original.close_step() and original.received and admission.status.connections == 0


@pytest.mark.parametrize("callback_failure", [False, True])
def test_final_callback_checks_dependents_without_storage_marker_cycle(directory, callback_failure):
    admission = pool()
    original = frame(admission)
    calls = []

    def receive():
        assert original.dependents_received and not store.storage_received
        calls.append("parent")
        if callback_failure and len(calls) == 1:
            raise InterruptedError("parent receiving not confirmed")

    store = JournalStore.open_retained(
        directory, construction=original, create=True, on_storage_close=receive
    )
    if callback_failure:
        with pytest.raises(InterruptedError):
            original.close_step()
        assert not original.received and not store.storage_received
        assert original.dependents_received
    assert original.close_step() and store.storage_received and original.received
    assert len(calls) == (2 if callback_failure else 1)


def test_constructor_gate_tracks_actual_worker_return_and_independent_progress(directory):
    admission = pool()
    original = frame(admission)
    entered, release = Event(), Event()
    errors, returned = [], []

    def opening(_store):
        entered.set()
        assert release.wait(10)

    def run():
        try:
            returned.append(
                JournalStore.open_retained(
                    directory, construction=original, create=True, on_storage_open=opening
                )
            )
        except BaseException as error:
            errors.append(error)

    worker = Thread(target=run)
    worker.start()
    try:
        assert entered.wait(10)
        with pytest.raises(BlockingIOError):
            original.close_step()
        other_path = directory.parent / "independent"
        other_path.mkdir(mode=0o700)
        independent = frame(admission)
        other = JournalStore.open_retained(other_path, construction=independent, create=True)
        other.close()
        assert independent.close_step()
        assert admission.status.connections == 1 and not original.received
    finally:
        release.set()
        worker.join(10)
    assert not worker.is_alive() and not errors and len(returned) == 1
    assert original.close_step() and admission.status.connections == 0


def test_exclusive_existing_bytes_and_readonly_current_format_are_preserved(directory):
    admission = pool()
    original = frame(admission)
    store = JournalStore.open_retained(directory, construction=original, create=True)
    store.close()
    assert original.close_step()
    before = (directory / "snapshot.sqlite3").read_bytes()
    duplicate = frame(admission)
    with pytest.raises(FileExistsError):
        JournalStore.open_retained(directory, construction=duplicate, create=True)
    assert duplicate.close_step()
    readonly = frame(admission)
    reader = JournalStore.open_retained(directory, construction=readonly, read_only=True)
    with pytest.raises(sqlite_admission.sqlite3.OperationalError, match="readonly"):
        reader.db.execute("CREATE TABLE forbidden(value INTEGER)")
    reader.close()
    assert readonly.close_step()
    assert (directory / "snapshot.sqlite3").read_bytes() == before


@pytest.mark.parametrize("phase", ["source", "post_store"])
def test_whole_session_constructor_keeps_original_gate_until_actual_return(
    directory, journal, catalog, monkeypatch, phase
):
    from test_retained_journal_session import OwnedSource

    admission = pool()
    original = frame(admission)
    source = OwnedSource(journal, catalog)
    entered, release = Event(), Event()
    returned, errors = [], []

    def hold():
        entered.set()
        assert release.wait(10)

    if phase == "source":
        verify = source.verify_current

        def verifying():
            hold()
            return verify()

        monkeypatch.setattr(source, "verify_current", verifying)
    else:
        initialize = JournalValidationSession._initialize

        def initializing(current, *, retained=False):
            assert original.storage is not None and original.phase == "ready"
            hold()
            return initialize(current, retained=retained)

        monkeypatch.setattr(JournalValidationSession, "_initialize", initializing)

    def run():
        try:
            returned.append(
                JournalValidationSession.open_retained(
                    directory,
                    source,
                    construction=original,
                    attempt_id="c" * 32,
                    worker_epoch=0,
                    create=True,
                    catalog=catalog,
                )
            )
        except BaseException as error:
            errors.append(error)

    worker = Thread(target=run)
    worker.start()
    try:
        assert entered.wait(10)
        with pytest.raises(BlockingIOError):
            original.close_step()
        assert not returned and admission.status.connections == 1
        assert not original.received
    finally:
        release.set()
        worker.join(10)
    assert not worker.is_alive() and not errors and len(returned) == 1
    returned[0].close()
    assert original.close_step() and admission.status.connections == 0


def test_result_constructor_gate_covers_validation_after_original_store_open(
    directory, journal, catalog, monkeypatch
):
    from riverhog_provenance import journal_result, open_retained_journal_result
    from test_retained_journal_session import OwnedSource, finish

    admission = pool()
    writer = frame(admission)
    source = OwnedSource(journal, catalog)
    session = JournalValidationSession.open_retained(
        directory,
        source,
        construction=writer,
        attempt_id="d" * 32,
        worker_epoch=0,
        create=True,
        catalog=catalog,
    )
    confirmed = finish(session)
    session.close()
    assert writer.close_step()
    original = frame(admission)
    entered, release = Event(), Event()
    progress = journal_result._checkpoint_progress
    returned, errors = [], []

    def checking(*args, **kwargs):
        assert original.storage is not None
        entered.set()
        assert release.wait(10)
        return progress(*args, **kwargs)

    monkeypatch.setattr(journal_result, "_checkpoint_progress", checking)

    def run():
        try:
            returned.append(
                open_retained_journal_result(
                    directory, source, construction=original, confirmed=confirmed, catalog=catalog
                )
            )
        except BaseException as error:
            errors.append(error)

    worker = Thread(target=run)
    worker.start()
    try:
        assert entered.wait(10)
        with pytest.raises(BlockingIOError):
            original.close_step()
        assert not returned and admission.status.connections == 1
    finally:
        release.set()
        worker.join(10)
    assert not worker.is_alive() and not errors and len(returned) == 1
    returned[0].close()
    assert original.close_step() and admission.status.connections == 0

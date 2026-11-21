from __future__ import annotations

import hashlib

import pytest
from riverhog_provenance import JournalStore, JournalValidationSession
from sqlite_admission import SqliteProgressConnection, observe_sqlite_execution


def _finish(session):
    while not session.advance().complete:
        pass
    return session.result()


def test_native_semantic_queries_share_execution_check_and_vm_observation(journal, catalog):
    instructions = []
    checks = []
    original = []

    def opened(session):
        original.append(session)
        assert isinstance(session._store.db, SqliteProgressConnection)
        session._store.db.set_progress_handler(lambda: instructions.append(None), 1)

    with JournalValidationSession(
        (journal,), attempt_id="e" * 32, catalog=catalog, on_storage_open=opened
    ) as session:
        assert original == [session]
        with observe_sqlite_execution(session._store.db, lambda: checks.append(None)):
            result = _finish(session)
    with result:
        assert result.journal_sha256 == hashlib.sha256(journal).hexdigest()
        assert result.journal_bytes == len(journal)
        assert result.materialize()
    assert len(instructions) > 1024 and len(checks) > 1
    assert session._closed and session._store.cleanup_complete


def test_native_sql_interrupt_keeps_original_deadline_cause_and_receives_storage(journal, catalog):
    original = TimeoutError("original Native execution deadline")
    calls = []

    def check():
        calls.append(None)
        if len(calls) > 1:
            raise original

    with JournalValidationSession((journal,), attempt_id="f" * 32, catalog=catalog) as session:
        session._store.db.set_progress_handler(lambda: None, 1)
        with pytest.raises(TimeoutError) as caught:
            with observe_sqlite_execution(session._store.db, check):
                _finish(session)
        assert caught.value is original
    assert session._closed and session._store.cleanup_complete


@pytest.mark.parametrize("retained", [False, True])
def test_native_store_dispatcher_exists_at_early_handle_retention(
    tmp_path, journal_constructions, retained
):
    originals = []

    def opened(store):
        assert isinstance(store.db, SqliteProgressConnection)
        originals.append(store)

    if retained:
        path = tmp_path / "owned"
        path.mkdir(mode=0o700)
        construction = journal_constructions.retain()
        store = JournalStore.open_retained(
            path, construction=construction, create=True, on_storage_open=opened
        )
    else:
        construction = None
        store = JournalStore(on_storage_open=opened)
    try:
        assert originals == [store]
        with observe_sqlite_execution(store.db, lambda: None):
            store.db.execute("SELECT count(*) FROM assertions").close()
    finally:
        store.close()
    assert store.cleanup_complete
    if construction is not None:
        assert construction.close_step() and construction.received

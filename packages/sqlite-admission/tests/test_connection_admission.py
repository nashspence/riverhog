from __future__ import annotations

import gc
import sqlite3
import weakref
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest
import sqlite_admission as owner
from sqlite_admission import SqliteCapacity, SqliteCleanupPending, SqliteConnectionAdmission


def admitted(**options):
    return SqliteConnectionAdmission(
        connection_limit=options.get("connections", 1),
        cursor_limit=options.get("cursors", 8),
        verify_process=options.get("verify", lambda: None),
    )


def populate(db):
    with closing(db.execute("CREATE TABLE items (value INTEGER PRIMARY KEY)")):
        pass
    with closing(db.executemany("INSERT INTO items VALUES (?)", [(n,) for n in range(128)])):
        pass
    db.commit()


def test_connection_capacity_precedes_driver_and_refills_only_after_close(tmp_path, monkeypatch):
    events = []
    pool = admitted(verify=lambda: events.append("process"))
    make = owner._connect

    def connect(*args, **options):
        events.append("driver")
        return make(*args, **options)

    monkeypatch.setattr(owner, "_connect", connect)
    first = pool.connect(tmp_path / "one.sqlite3")
    with pytest.raises(SqliteCapacity):
        pool.connect(tmp_path / "two.sqlite3")
    assert events == ["process", "driver"]
    assert pool.status.connections == pool.status.connection_peak == 1
    first.close()
    second = pool.connect(tmp_path / "two.sqlite3")
    assert events == ["process", "driver", "process", "driver"]
    second.close()
    assert pool.status.connections == pool.status.cursors == 0


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
@pytest.mark.parametrize("grant", ["connection_limit", "cursor_limit"])
def test_invalid_grants_do_not_run_process_admission(grant, value):
    events = []
    options = {
        "connection_limit": 1,
        "cursor_limit": 2,
        "verify_process": lambda: events.append(True),
    }
    options[grant] = value
    with pytest.raises(ValueError, match="positive integers"):
        SqliteConnectionAdmission(**options)
    assert not events


def test_process_refusal_precedes_driver_and_does_not_consume_connection_slot(
    tmp_path, monkeypatch
):
    def refuse():
        raise RuntimeError("process policy changed")

    pool = admitted(verify=refuse)
    monkeypatch.setattr(owner, "_connect", lambda *_a, **_k: pytest.fail("driver opened"))
    with pytest.raises(RuntimeError, match="process policy changed"):
        pool.connect(tmp_path / "not-opened.sqlite3")
    assert pool.status.connections == 0 and not list(tmp_path.iterdir())


@pytest.mark.parametrize("returned_handle", [False, True])
def test_unreturned_construction_remains_process_retained(tmp_path, monkeypatch, returned_handle):
    pool = admitted()
    make = owner._connect

    def interrupted(*args, **options):
        if returned_handle:
            make(*args, **options)
        raise OSError("open outcome unconfirmed")

    monkeypatch.setattr(owner, "_connect", interrupted)
    with pytest.raises(OSError, match="open outcome unconfirmed"):
        pool.connect(tmp_path / "unreturned.sqlite3")
    assert pool.status.connections == pool.status.unreturned_connections == 1
    reference = weakref.ref(pool)
    del pool
    gc.collect()
    pool = reference()
    assert pool is not None and pool.status.connections == 1
    assert len(pool.pending_connections) == int(returned_handle)
    if returned_handle:
        pool.pending_connections[0].close()
        assert pool.status.connections == 0 and pool not in owner._admissions


def test_cursor_capacity_precedes_sql_and_replenishes_after_confirmed_release(tmp_path):
    pool = admitted(cursors=1)
    db = pool.connect(tmp_path / "bounded.sqlite3")
    first = db.execute("CREATE TABLE first (value)")
    with pytest.raises(SqliteCapacity, match="cursor"):
        db.execute("CREATE TABLE forbidden (value)")
    first.close()
    with closing(db.execute("SELECT name FROM sqlite_master WHERE type='table'")) as rows:
        assert rows.fetchall() == [("first",)]
    assert pool.status.cursor_peak == 1
    db.close()
    assert pool.status.connections == pool.status.cursors == 0


def test_actual_suspended_cursor_closes_before_driver_and_database_file_release(
    tmp_path, monkeypatch
):
    pool = admitted()
    path = tmp_path / "owned.sqlite3"
    db = pool.connect(path)
    populate(db)
    rows = db.execute("SELECT value FROM items ORDER BY value")
    assert next(rows) == (0,)
    close = owner._Connection._driver_close
    observations = []

    def ordered(connection):
        assert rows._cleanup_complete and not connection._cursors
        observations.append(True)
        close(connection)

    monkeypatch.setattr(owner._Connection, "_driver_close", ordered)
    db.close()
    assert observations == [True] and db.cleanup_complete
    assert pool.status.connections == pool.status.cursors == 0
    with pytest.raises(SqliteCleanupPending):
        next(rows)
    path.unlink()


@pytest.mark.parametrize("after", [False, True])
def test_uncertain_connection_close_retains_slot_without_context_retry(
    tmp_path, monkeypatch, after
):
    pool = admitted()
    db = pool.connect(tmp_path / "uncertain.sqlite3")
    close, calls = owner._Connection._driver_close, []

    def interrupted(connection):
        calls.append(True)
        if len(calls) == 1:
            if after:
                close(connection)
            raise OSError("connection close unconfirmed")
        return close(connection)

    monkeypatch.setattr(owner._Connection, "_driver_close", interrupted)
    with pytest.raises(OSError, match="connection close unconfirmed"):
        with db:
            db.close()
    for _ in range(3):
        assert pool.status.connections == pool.status.closing_connections == 1
        assert not db.cleanup_complete
    assert len(calls) == 1
    with pytest.raises(SqliteCapacity):
        pool.connect(tmp_path / "blocked.sqlite3")
    db.close()
    assert len(calls) == 2 and db.cleanup_complete and pool.status.connections == 0


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("finalized", [False, True])
def test_uncertain_cursor_close_remains_reachable_and_charged(
    tmp_path, monkeypatch, after, finalized
):
    pool = admitted()
    db = pool.connect(tmp_path / "uncertain-cursor.sqlite3")
    rows = db.execute("SELECT 1")
    close, calls = owner._Cursor._driver_close, []

    def interrupted(cursor):
        calls.append(True)
        if len(calls) == 1:
            if after:
                close(cursor)
            raise OSError("cursor close unconfirmed")
        return close(cursor)

    monkeypatch.setattr(owner._Cursor, "_driver_close", interrupted)
    reference = weakref.ref(rows)
    if finalized:
        del rows
        gc.collect()
    else:
        with pytest.raises(OSError, match="cursor close unconfirmed"):
            rows.close()
    assert reference() is not None
    for _ in range(3):
        assert pool.status.connections == pool.status.cursors == 1
    assert len(calls) == 1
    with pytest.raises(SqliteCleanupPending):
        db.execute("SELECT 2")
    if finalized:
        with pytest.raises(SqliteCleanupPending, match="quarantined"):
            db.close()
        assert len(calls) == 1
        assert pool.status.connections == pool.status.cursors == 1
    else:
        with pytest.raises(SqliteCleanupPending, match="remains unconfirmed"):
            db.close()
        assert len(calls) == 1
        db.close()
        assert len(calls) == 2 and pool.status.connections == pool.status.cursors == 0


def test_unreferenced_normal_cursor_confirms_close_before_grant_retirement(tmp_path, monkeypatch):
    pool = admitted(cursors=1)
    db = pool.connect(tmp_path / "temporary.sqlite3")
    close, counts = owner._Cursor._driver_close, []

    def observe(cursor):
        counts.append(pool.status.cursors)
        return close(cursor)

    monkeypatch.setattr(owner._Cursor, "_driver_close", observe)
    db.execute("SELECT 1").fetchone()
    gc.collect()
    assert counts == [1] and pool.status.cursors == 0
    db.close()


@pytest.mark.parametrize("stage", ["cursor", "query", "fetch", "backup"])
def test_reentrant_close_waits_for_actual_driver_work_and_blocks_handoff(
    tmp_path, monkeypatch, stage
):
    pool = admitted(connections=2)
    db = pool.connect(tmp_path / "source.sqlite3")
    populate(db)
    calls = []

    def requested():
        with pytest.raises(SqliteCleanupPending, match="still active"):
            db.close()
        calls.append(True)

    if stage == "cursor":
        initialize = owner._Cursor.__init__

        def interrupted(cursor, *args):
            initialize(cursor, *args)
            requested()

        monkeypatch.setattr(owner._Cursor, "__init__", interrupted)
        run = db.cursor
    elif stage in ("query", "fetch"):
        db.create_function("close_requested", 0, lambda: requested() or 1)
        if stage == "query":

            def run():
                return db.execute("SELECT close_requested()")
        else:
            rows = db.execute(
                "SELECT CASE WHEN value=1 THEN close_requested() ELSE 1 END FROM items"
            )
            run = rows.fetchone
    else:
        target = pool.connect(tmp_path / "copy.sqlite3")

        def run():
            return db.backup(target, pages=1, progress=lambda *_args: requested())

    with pytest.raises(SqliteCleanupPending):
        run()
    assert calls and pool.status.connections == (2 if stage == "backup" else 1)
    db.close()
    if stage == "backup":
        target.close()
    assert pool.status.connections == pool.status.cursors == 0


def test_thread_rejection_does_not_release_connection_grant(tmp_path):
    pool = admitted()
    db = pool.connect(tmp_path / "thread.sqlite3")
    with ThreadPoolExecutor(max_workers=1) as worker:
        with pytest.raises(sqlite3.ProgrammingError):
            worker.submit(db.close).result(timeout=5)
    assert pool.status.connections == pool.status.closing_connections == 1
    db.close()
    assert pool.status.connections == 0


def test_owner_row_factory_transactions_and_backup_keep_sqlite_semantics(tmp_path):
    pool = admitted(connections=2)
    db = pool.connect(tmp_path / "source.sqlite3", isolation_level=None)
    db.row_factory = sqlite3.Row
    populate(db)
    with closing(db.execute("SELECT value FROM items ORDER BY value")) as rows:
        assert rows.fetchone()["value"] == 0
        assert len(rows.fetchmany(3)) == 3
        assert len(rows.fetchall()) == 124
    target = pool.connect(tmp_path / "target.sqlite3")
    db.backup(target, pages=1)
    with closing(target.execute("SELECT count(*) FROM items")) as rows:
        assert rows.fetchone() == (128,)
    db.close()
    target.close()
    assert pool.status.connections == pool.status.cursors == 0


def test_factory_reinitialization_and_unadmitted_handles_are_refused(tmp_path):
    pool = admitted()
    db = pool.connect(tmp_path / "source.sqlite3")
    with pytest.raises(RuntimeError, match="reinitialize"):
        db.__init__(tmp_path / "other.sqlite3")
    with pytest.raises(ValueError, match="cursor factory"):
        db.cursor(factory=sqlite3.Cursor)
    with pytest.raises(ValueError, match="blob handles"):
        db.blobopen("items", "value", 1)
    with sqlite3.connect(":memory:") as unadmitted:
        with pytest.raises(ValueError, match="admitted target"):
            db.backup(unadmitted)
    unadmitted.close()
    db.close()
    assert not (tmp_path / "other.sqlite3").exists()


@pytest.mark.parametrize("kind", ["journal", "corpus", "selection", "completion", "tags"])
def test_admission_composes_actual_storage_owners_without_counting_handles_as_connections(
    tmp_path, kind
):
    from riverhog_archive_contracts import (
        HistoryJournalAnchor,
        MemberHistoryBuilder,
        MemberHistoryPrimary,
    )
    from riverhog_client import CompletionRecords
    from riverhog_client.initial_tags import prepare_initial_collection_tags
    from riverhog_provenance import CanonicalCorpusValidator, JournalStore

    checks, retained = [], []
    pool = admitted(connections=3, cursors=128, verify=lambda: checks.append(True))
    if kind == "journal":
        storage = JournalStore(connection_factory=pool.connect, on_storage_open=retained.append)
        storage.freeze()
        first, last = storage.borrow(), storage.borrow()
        storage.close()
        first.close()
        assert storage.cleanup_complete and first.cleanup_complete
        assert pool.status.connections == 1
        last.close()
    elif kind == "corpus":
        storage = CanonicalCorpusValidator(
            connection_factory=pool.connect, on_storage_open=retained.append
        )
        assert pool.status.connections == 1
        storage.close()
    elif kind == "selection":
        primary = MemberHistoryPrimary(
            HistoryJournalAnchor(
                "urn:uuid:11111111-1111-4111-8111-111111111111",
                "urn:uuid:22222222-2222-4222-8222-222222222222",
                0,
                "a" * 64,
                "d" * 64,
                1,
            ),
            "urn:uuid:33333333-3333-4333-8333-333333333333",
        )
        storage = MemberHistoryBuilder(
            artifact_id="b" * 64,
            bytes=4,
            sha256="c" * 64,
            primary=primary,
            connection_factory=pool.connect,
            on_storage_open=retained.append,
            verification_connection_factory=pool.connect,
            on_verification_storage_open=retained.append,
        )
        storage.seal()
        assert pool.status.connections == 1 and pool.status.connection_peak == 2
        storage.close()
    elif kind == "completion":
        storage = CompletionRecords(
            connection_factory=pool.connect, on_storage_open=retained.append
        )
        record = storage.add("a", [b"x" * (256 * 1024)])
        reader = record.read()
        assert len(next(reader)) == 128 * 1024
        storage.close()
        reader.close()
    else:
        storage = prepare_initial_collection_tags(
            [],
            connection_factory=pool.connect,
            on_storage_open=retained.append,
        )
        assert pool.status.connections == 1
        storage.close()
    assert checks and retained
    assert pool.status.connections == pool.status.cursors == 0
    assert pool not in owner._admissions


def test_journal_context_reports_unknown_cursor_cleanup_without_retry(tmp_path, monkeypatch):
    from riverhog_provenance import JournalStore

    pool = admitted()
    store = JournalStore(connection_factory=pool.connect)
    cursor = store.db.execute("SELECT 1")
    original, calls = owner._Cursor._driver_close, []

    def interrupted(actual):
        calls.append(True)
        if len(calls) == 1:
            raise OSError("cursor close unconfirmed")
        return original(actual)

    monkeypatch.setattr(owner._Cursor, "_driver_close", interrupted)
    with pytest.raises(SqliteCleanupPending, match="remains unconfirmed"):
        with closing(store):
            with pytest.raises(OSError, match="cursor close unconfirmed"):
                cursor.close()
    assert len(calls) == 1 and pool.status.connections == pool.status.cursors == 1
    assert not store.cleanup_complete
    store.close()
    assert len(calls) == 2 and store.cleanup_complete
    assert pool.status.connections == pool.status.cursors == 0


def test_failed_sql_shortcut_settles_its_unreturned_cursor_and_keeps_connection_usable(tmp_path):
    pool = admitted(cursors=1)
    db = pool.connect(tmp_path / "failed-statement.sqlite3")
    with closing(db.execute("CREATE TABLE unique_ (value PRIMARY KEY)")):
        pass
    with closing(db.execute("INSERT INTO unique_ VALUES (1)")):
        pass
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("INSERT INTO unique_ VALUES (1)")
    assert pool.status.cursors == 0
    with closing(db.execute("SELECT count(*) FROM unique_")) as rows:
        assert rows.fetchone() == (1,)
    db.close()
    assert pool.status.connections == 0


def test_connection_slot_remains_charged_while_actual_open_is_in_progress(tmp_path, monkeypatch):
    from threading import Event

    pool = admitted()
    entered, release = Event(), Event()
    make = owner._connect

    def waiting(*args, **options):
        entered.set()
        assert release.wait(5)
        return make(*args, **options)

    monkeypatch.setattr(owner, "_connect", waiting)
    with ThreadPoolExecutor(max_workers=1) as worker:
        opened = worker.submit(
            pool.connect, tmp_path / "active-open.sqlite3", check_same_thread=False
        )
        try:
            assert entered.wait(5)
            assert pool.status.connections == pool.status.unreturned_connections == 1
            with pytest.raises(SqliteCapacity):
                pool.connect(tmp_path / "refused.sqlite3")
        finally:
            release.set()
        db = opened.result(timeout=5)
    assert pool.status.connections == 1 and not (tmp_path / "refused.sqlite3").exists()
    db.close()
    assert pool.status.connections == 0


def test_actual_cursor_close_must_finish_before_connection_or_grant_release(tmp_path, monkeypatch):
    from threading import Event

    pool = admitted()
    db = pool.connect(tmp_path / "close-active.sqlite3", check_same_thread=False)
    rows = db.execute("SELECT 1")
    entered, release = Event(), Event()
    close = owner._Cursor._driver_close

    def waiting(cursor):
        entered.set()
        assert release.wait(5)
        return close(cursor)

    monkeypatch.setattr(owner._Cursor, "_driver_close", waiting)
    with ThreadPoolExecutor(max_workers=1) as worker:
        stopped = worker.submit(rows.close)
        try:
            assert entered.wait(5)
            assert pool.status.connections == pool.status.cursors == 1
            with pytest.raises(SqliteCleanupPending, match="still active"):
                db.close()
            with pytest.raises(SqliteCleanupPending):
                db.execute("SELECT 2")
        finally:
            release.set()
        stopped.result(timeout=5)
    assert pool.status.connections == 1 and pool.status.cursors == 0
    db.close()
    assert pool.status.connections == 0


def test_explicit_larger_grants_retain_existing_cursor_identity_and_use(tmp_path):
    pool = admitted(cursors=1)
    db = pool.connect(tmp_path / "first.sqlite3")
    rows = db.execute("SELECT 1 UNION ALL SELECT 2")
    assert rows.fetchone() == (1,)
    pool.increase(connection_limit=2, cursor_limit=2)
    assert pool.status.connections == pool.status.cursors == 1
    assert pool.status.connection_limit == pool.status.cursor_limit == 2
    other = pool.connect(tmp_path / "second.sqlite3")
    with closing(other.execute("SELECT 3")) as result:
        assert result.fetchone() == (3,)
    assert rows.fetchone() == (2,)
    assert pool.status.connection_peak == pool.status.cursor_peak == 2
    db.close()
    other.close()
    assert pool.status.connections == pool.status.cursors == 0


@pytest.mark.parametrize("connections,cursors", [(1, 1), (0, 2), (2, 0), (True, 2), (2, 1.5)])
def test_invalid_or_unchanged_larger_offer_does_not_reset_use(tmp_path, connections, cursors):
    pool = admitted(cursors=1)
    db = pool.connect(tmp_path / "retained.sqlite3")
    rows = db.execute("SELECT 1")
    before = pool.status
    with pytest.raises(ValueError):
        pool.increase(connection_limit=connections, cursor_limit=cursors)
    assert pool.status == before and rows.fetchone() == (1,)
    db.close()


@pytest.mark.parametrize("kind", ["cursor", "connection"])
@pytest.mark.parametrize("after", [False, True])
def test_metadata_acknowledgement_loss_does_not_repeat_confirmed_driver_close(
    tmp_path, monkeypatch, kind, after
):
    pool = admitted()
    db = pool.connect(tmp_path / "retirement.sqlite3")
    rows = db.execute("SELECT 1")
    cursor_close, connection_close = owner._Cursor._driver_close, owner._Connection._driver_close
    driver_calls = []

    def observed_cursor(cursor):
        driver_calls.append("cursor")
        return cursor_close(cursor)

    def observed_connection(connection):
        driver_calls.append("connection")
        return connection_close(connection)

    name = "_release_" + kind
    retire, calls = getattr(pool, name), []

    def interrupted(lease):
        calls.append(True)
        if len(calls) == 1:
            if after:
                retire(lease)
            raise OSError("retirement acknowledgement lost")
        return retire(lease)

    monkeypatch.setattr(owner._Cursor, "_driver_close", observed_cursor)
    monkeypatch.setattr(owner._Connection, "_driver_close", observed_connection)
    monkeypatch.setattr(pool, name, interrupted)
    with pytest.raises(OSError, match="retirement acknowledgement lost"):
        rows.close() if kind == "cursor" else db.close()
    if kind == "cursor":
        rows.close()
    db.close()
    assert driver_calls == ["cursor", "connection"]
    assert pool.status.connections == pool.status.cursors == 0

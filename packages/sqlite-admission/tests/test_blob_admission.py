from __future__ import annotations

import gc
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Event

import pytest
import sqlite_admission as owner
from sqlite_admission import SqliteCapacity, SqliteCleanupPending, SqliteConnectionAdmission


def opened(*, blobs=1, **options):
    pool = SqliteConnectionAdmission(
        operation_limit=8,
        connection_limit=1,
        cursor_limit=8,
        blob_limit=blobs,
        verify_process=lambda: None,
    )
    db = pool.connect(":memory:", **options)
    with closing(db.execute("CREATE TABLE items(value BLOB)")):
        pass
    with closing(db.execute("INSERT INTO items VALUES (zeroblob(8)), (zeroblob(8))")):
        pass
    db.commit()
    return pool, db


@pytest.mark.parametrize("grant", [True, -1, 1.5, "1", None])
def test_invalid_blob_grant_never_reaches_process_or_driver(grant):
    def forbidden():
        pytest.fail("invalid blob grant reached process admission")

    with pytest.raises(ValueError, match="blob grant"):
        SqliteConnectionAdmission(
            operation_limit=8,
            connection_limit=1,
            cursor_limit=1,
            blob_limit=grant,
            verify_process=forbidden,
        )


def test_blob_capacity_is_explicit_and_increase_keeps_original_handle_and_cursor():
    pool, db = opened(blobs=0)
    with pytest.raises(SqliteCapacity, match="blob grant"):
        db.blobopen("items", "value", 1)
    assert not db.cleanup_complete and pool.status.blobs == 0
    pool.increase(connection_limit=1, cursor_limit=8, blob_limit=1)
    first = db.blobopen("items", "value", 1)
    first.write(b"original")
    first.seek(3)
    with pytest.raises(SqliteCapacity, match="blob grant"):
        db.blobopen("items", "value", 2)
    pool.increase(connection_limit=1, cursor_limit=8, blob_limit=2)
    with db.blobopen("items", "value", 2) as second:
        assert first.tell() == 3 and first.read() == b"ginal"
        second[:] = first[:]
    assert pool.status.blobs == 1 and pool.status.blob_peak == 2
    db.close()
    assert pool.status.connections == pool.status.blobs == 0
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        first._actual.read()


@pytest.mark.parametrize("grant", [0, True, -1, 1.5])
def test_blob_offer_cannot_reset_retained_use(grant):
    pool, db = opened()
    blob = db.blobopen("items", "value", 1)
    before = pool.status
    with pytest.raises(ValueError):
        pool.increase(connection_limit=1, cursor_limit=8, blob_limit=grant)
    assert pool.status == before and len(blob) == 8
    db.close()


def test_blob_preserves_sqlite_read_write_index_seek_and_context_semantics():
    pool, db = opened()
    with db.blobopen("items", "value", 1) as blob:
        assert len(blob) == 8 and blob.tell() == 0
        blob.write(b"original")
        blob.seek(-3, 2)
        assert blob.read(2) == b"na" and blob.tell() == 7
        assert blob[0] == ord("o") and blob[:3] == b"ori"
        blob[0] = ord("O")
        blob[1:3] = b"RI"
        blob.seek(0)
        assert blob.read() == b"ORIginal"
        with pytest.raises(ValueError):
            blob.write(b"overflow")
    assert pool.status.blobs == 0
    with pytest.raises(sqlite3.ProgrammingError, match="closed blob"):
        blob.__enter__()
    with closing(db.execute("SELECT value FROM items WHERE rowid=1")) as cursor:
        assert cursor.fetchone() == (b"ORIginal",)
    db.close()


@pytest.mark.parametrize("after", [False, True])
def test_unknown_blob_close_stays_charged_without_context_or_owner_retry(monkeypatch, after):
    pool, db = opened()
    blob = db.blobopen("items", "value", 1)
    close = owner._Blob._driver_close
    attempts = []

    def uncertain(actual):
        attempts.append(True)
        if len(attempts) == 1:
            if after:
                close(actual)
            raise OSError("blob close unconfirmed")
        close(actual)

    monkeypatch.setattr(owner._Blob, "_driver_close", uncertain)
    with pytest.raises(OSError, match="blob close unconfirmed"):
        with blob:
            blob.close()
    with pytest.raises(SqliteCleanupPending, match="unconfirmed"):
        db.close()
    assert len(attempts) == 1 and pool.status.blobs == pool.status.connections == 1
    with pytest.raises(SqliteCleanupPending):
        blob.read()
    db.close()
    assert len(attempts) == 2 and pool.status.blobs == pool.status.connections == 0


def test_blob_construction_close_attempt_prevents_handoff_and_retains_actual_handle(monkeypatch):
    pool, db = opened()
    create = owner._Connection._driver_blobopen

    def reentered(actual, *args, **options):
        blob = create(actual, *args, **options)
        with pytest.raises(SqliteCleanupPending, match="active"):
            actual.close()
        return blob

    monkeypatch.setattr(owner._Connection, "_driver_blobopen", reentered)
    with pytest.raises(SqliteCleanupPending, match="closing"):
        db.blobopen("items", "value", 1)
    assert pool.status.blobs == pool.status.unreturned_blobs == pool.status.connections == 1
    db.close()
    assert pool.status.blobs == pool.status.connections == 0


def test_unknown_unreturned_blob_is_not_reopened_or_refunded(monkeypatch):
    pool, db = opened()
    attempts = []

    def unavailable(*args, **options):
        attempts.append(True)
        raise OSError("blob construction unconfirmed")

    monkeypatch.setattr(owner._Connection, "_driver_blobopen", unavailable)
    with pytest.raises(OSError, match="construction unconfirmed"):
        db.blobopen("items", "value", 1)
    for _ in range(3):
        with pytest.raises(SqliteCleanupPending, match="unconfirmed"):
            db.close()
    assert len(attempts) == 1
    assert pool.status.connections == pool.status.blobs == pool.status.unreturned_blobs == 1


def test_close_during_actual_blob_read_prevents_result_handoff():
    pool, db = opened()
    blob = db.blobopen("items", "value", 1)
    actual = blob._actual

    class Reentered:
        def read(self, length):
            result = actual.read(length)
            with pytest.raises(SqliteCleanupPending, match="active"):
                db.close()
            return result

        def close(self):
            actual.close()

    blob._actual = Reentered()
    with pytest.raises(SqliteCleanupPending, match="closing"):
        blob.read()
    assert pool.status.blobs == pool.status.connections == 1
    db.close()
    assert pool.status.blobs == pool.status.connections == 0


def test_actual_blob_close_finishes_before_connection_or_grant_release(monkeypatch):
    pool, db = opened(check_same_thread=False)
    blob = db.blobopen("items", "value", 1)
    entered, release = Event(), Event()
    close = owner._Blob._driver_close

    def blocked(actual):
        entered.set()
        assert release.wait(10)
        close(actual)

    monkeypatch.setattr(owner._Blob, "_driver_close", blocked)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(blob.close)
        try:
            assert entered.wait(10)
            with pytest.raises(SqliteCleanupPending, match="active"):
                db.close()
            assert pool.status.blobs == pool.status.connections == 1
        finally:
            release.set()
        pending.result(10)
    db.close()
    assert pool.status.blobs == pool.status.connections == 0


def test_blob_thread_rejection_retains_actual_slot_until_correct_thread_retry():
    pool, db = opened()
    blob = db.blobopen("items", "value", 1)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with pytest.raises(sqlite3.ProgrammingError, match="thread"):
            executor.submit(blob.close).result(10)
    with pytest.raises(SqliteCleanupPending, match="unconfirmed"):
        db.close()
    assert pool.status.connections == pool.status.blobs == 1
    db.close()
    assert pool.status.connections == pool.status.blobs == 0


@pytest.mark.parametrize("after", [False, True])
def test_blob_metadata_ack_loss_never_repeats_confirmed_driver_close(monkeypatch, after):
    pool, db = opened()
    blob = db.blobopen("items", "value", 1)
    retire, close = pool._release_blob, owner._Blob._driver_close
    closes, acknowledgements = [], []

    def confirmed(actual):
        closes.append(True)
        close(actual)

    def uncertain(lease):
        acknowledgements.append(True)
        if len(acknowledgements) == 1:
            if after:
                retire(lease)
            raise OSError("metadata acknowledgement unconfirmed")
        retire(lease)

    monkeypatch.setattr(pool, "_release_blob", uncertain)
    monkeypatch.setattr(owner._Blob, "_driver_close", confirmed)
    with pytest.raises(OSError, match="acknowledgement"):
        blob.close()
    blob.close()
    db.close()
    assert closes == [True] and pool.status.blobs == pool.status.connections == 0


def test_normal_blob_finalization_confirms_actual_close_before_retirement(monkeypatch):
    pool, db = opened()
    close = owner._Blob._driver_close
    observed = []

    def confirmed(actual):
        observed.append((pool.status.connections, pool.status.blobs))
        close(actual)

    monkeypatch.setattr(owner._Blob, "_driver_close", confirmed)
    blob = db.blobopen("items", "value", 1)
    del blob
    gc.collect()
    assert observed == [(1, 1)] and pool.status.blobs == 0
    db.close()


def test_unknown_blob_finalization_keeps_actual_obligation_until_process_exit(monkeypatch):
    pool, db = opened()
    attempts = []

    def uncertain(actual):
        attempts.append(True)
        raise OSError("actual finalizer close unconfirmed")

    monkeypatch.setattr(owner._Blob, "_driver_close", uncertain)
    blob = db.blobopen("items", "value", 1)
    del blob
    gc.collect()
    with pytest.raises(SqliteCleanupPending, match="quarantined"):
        db.close()
    assert attempts == [True] and pool.status.blobs == pool.status.connections == 1


def test_actual_blob_is_closed_before_the_actual_database_driver(monkeypatch):
    pool, db = opened()
    blob = db.blobopen("items", "value", 1)
    close = owner._Connection._driver_close
    observed = []

    def database(actual):
        with pytest.raises(sqlite3.ProgrammingError, match="closed blob"):
            blob._actual.read()
        observed.append((pool.status.connections, pool.status.blobs))
        close(actual)

    monkeypatch.setattr(owner._Connection, "_driver_close", database)
    db.close()
    assert observed == [(1, 0)]
    assert pool.status.connections == pool.status.blobs == 0

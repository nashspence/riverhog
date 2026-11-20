from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from threading import Event

import pytest
import sqlite_admission as owner
from sqlite_admission import SqliteCapacity, SqliteCleanupPending, SqliteConnectionAdmission


def admitted(*, operations=1, connections=3, verify=lambda: None):
    return SqliteConnectionAdmission(
        connection_limit=connections,
        cursor_limit=8,
        blob_limit=1,
        operation_limit=operations,
        verify_process=verify,
    )


@contextmanager
def executing(db):
    entered, release = Event(), Event()

    def blocked():
        entered.set()
        if not release.wait(5):
            raise RuntimeError("test did not release actual SQLite work")
        return 7

    db.create_function("blocked", 0, blocked)
    with ThreadPoolExecutor(max_workers=1) as worker:
        result = worker.submit(db.execute, "SELECT blocked()")
        assert entered.wait(5)
        try:
            yield result
        finally:
            release.set()


@pytest.mark.parametrize("grant", [0, -1, True, None, 1.5])
def test_invalid_operation_grant_precedes_process_or_driver(grant, monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("invalid operation grant reached process or driver")

    monkeypatch.setattr(owner, "_connect", forbidden)
    with pytest.raises(ValueError, match="operation grant"):
        admitted(operations=grant, verify=forbidden)


def test_actual_sql_work_refuses_another_connection_and_new_open_before_driver(tmp_path):
    observations = []
    pool = admitted(verify=lambda: observations.append("verified"))
    with closing(pool.connect(":memory:", check_same_thread=False)) as first:
        with closing(pool.connect(":memory:")) as second:
            traced = []
            second.set_trace_callback(traced.append)
            with executing(first) as result:
                assert pool.status.operations == pool.status.operation_peak == 1
                before = pool.status
                with pytest.raises(SqliteCapacity, match="operation grant"):
                    second.execute("CREATE TABLE forbidden(value)")
                with pytest.raises(SqliteCapacity, match="operation grant"):
                    pool.connect(tmp_path / "forbidden.sqlite3")
                assert pool.status == before
                assert observations == ["verified", "verified"]
                assert traced == [] and not list(tmp_path.iterdir())
            with closing(result.result(timeout=5)) as rows:
                assert rows.fetchone() == (7,)
            assert pool.status.operations == 0
    assert pool.status.connections == pool.status.cursors == pool.status.operations == 0


def test_refused_cursor_and_blob_work_preserves_original_positions():
    pool = admitted()
    with closing(pool.connect(":memory:", check_same_thread=False)) as first:
        with closing(pool.connect(":memory:")) as second:
            with closing(second.execute("CREATE TABLE items(value BLOB)")):
                pass
            with closing(second.execute("INSERT INTO items VALUES (zeroblob(8))")):
                pass
            second.commit()
            with closing(second.execute("SELECT 1 UNION ALL SELECT 2")) as rows:
                assert rows.fetchone() == (1,)
                with closing(second.blobopen("items", "value", 1)) as blob:
                    blob.write(b"original")
                    blob.seek(3)
                    with executing(first) as result:
                        with pytest.raises(SqliteCapacity, match="operation grant"):
                            rows.fetchone()
                        with pytest.raises(SqliteCapacity, match="operation grant"):
                            blob.read(2)
                        assert pool.status.operations == 1
                    result.result(timeout=5).close()
                    assert rows.fetchone() == (2,)
                    assert blob.tell() == 3 and blob.read() == b"ginal"
                    assert pool.status.operations == 0


def test_explicit_operation_increase_preserves_live_work_and_existing_handles():
    pool = admitted()
    with closing(pool.connect(":memory:", check_same_thread=False)) as first:
        with closing(pool.connect(":memory:")) as second:
            with executing(first) as result:
                pool.increase(connection_limit=3, cursor_limit=8, operation_limit=2)
                assert pool.status.operations == 1 and pool.status.operation_limit == 2
                with closing(second.execute("SELECT 11")) as rows:
                    assert rows.fetchone() == (11,)
                    assert pool.status.operations == 1
                assert pool.status.operation_peak == 2
            with closing(result.result(timeout=5)) as rows:
                assert rows.fetchone() == (7,)
            assert pool.status.operations == 0


@pytest.mark.parametrize("grant", [0, -1, True, 1, 1.5])
def test_invalid_or_unchanged_operation_offer_preserves_live_use(grant):
    pool = admitted()
    with closing(pool.connect(":memory:", check_same_thread=False)) as db:
        with executing(db) as result:
            before = pool.status
            with pytest.raises(ValueError):
                pool.increase(connection_limit=3, cursor_limit=8, operation_limit=grant)
            assert pool.status == before
        result.result(timeout=5).close()


def test_independent_cleanup_is_not_blocked_by_a_full_work_grant():
    pool = admitted()
    first = pool.connect(":memory:", check_same_thread=False)
    second = pool.connect(":memory:")
    with closing(second.execute("CREATE TABLE items(value BLOB)")):
        pass
    with closing(second.execute("INSERT INTO items VALUES (zeroblob(8))")):
        pass
    second.commit()
    blob = second.blobopen("items", "value", 1)
    rows = second.execute("SELECT 1")
    with executing(first) as result:
        blob.close()
        rows.close()
        second.close()
        assert pool.status.blobs == 0
        assert second.cleanup_complete
        assert pool.status.operations == pool.status.connections == 1
        assert pool.status.cursors == 1
    result.result(timeout=5).close()
    first.close()
    assert pool.status.connections == pool.status.operations == pool.status.cursors == 0


def test_one_actual_backup_call_retains_both_connections_under_one_work_grant():
    pool = admitted(connections=2)
    with closing(pool.connect(":memory:")) as first:
        with closing(pool.connect(":memory:")) as second:
            with closing(first.execute("CREATE TABLE items(value)")):
                pass
            with closing(first.execute("INSERT INTO items VALUES (17)")):
                pass
            first.commit()
            observed = []

            def progress(*_args):
                observed.append(pool.status.operations)
                assert first._calls == second._calls == 1
                with pytest.raises(SqliteCapacity, match="operation grant"):
                    second.execute("CREATE TABLE forbidden(value)")

            first.backup(second, pages=1, progress=progress, sleep=0)
            assert observed and set(observed) == {1}
            assert pool.status.operation_peak == 1 and pool.status.operations == 0
            with closing(second.execute("SELECT value FROM items")) as rows:
                assert rows.fetchall() == [(17,)]


@pytest.mark.parametrize("returned_handle", [False, True])
def test_open_operation_ends_on_driver_return_while_unknown_construction_stays_charged(
    monkeypatch, returned_handle
):
    pool = admitted()
    original = owner._connect

    def unknown(*args, **kwargs):
        assert pool.status.operations == 1
        if returned_handle:
            original(*args, **kwargs)
        raise OSError("construction acknowledgement was lost")

    monkeypatch.setattr(owner, "_connect", unknown)
    with pytest.raises(OSError, match="acknowledgement"):
        pool.connect(":memory:")
    assert pool.status.operations == 0
    assert pool.status.connections == pool.status.unreturned_connections == 1
    if returned_handle:
        pool.pending_connections[0].close()
        assert pool.status.connections == 0


def test_returned_sql_failure_and_cursor_cleanup_do_not_leak_operation_slots():
    pool = admitted()
    with closing(pool.connect(":memory:")) as db:
        with pytest.raises(sqlite3.OperationalError):
            db.execute("SELECT missing_column")
        assert pool.status.operations == pool.status.cursors == 0
        with closing(db.execute("SELECT 19")) as rows:
            assert rows.fetchone() == (19,)


def test_concurrent_close_keeps_work_charged_until_actual_sql_has_returned():
    pool = admitted()
    db = pool.connect(":memory:", check_same_thread=False)
    with executing(db) as result:
        with pytest.raises(SqliteCleanupPending, match="still active"):
            db.close()
        assert pool.status.operations == pool.status.connections == 1
    with pytest.raises(SqliteCleanupPending):
        result.result(timeout=5)
    assert pool.status.operations == 0 and pool.status.connections == 1
    db.close()
    assert pool.status.operations == pool.status.connections == 0


def test_positive_operation_offer_cannot_lower_a_live_aggregate_grant():
    pool = admitted(operations=2)
    with closing(pool.connect(":memory:", check_same_thread=False)) as first:
        with closing(pool.connect(":memory:", check_same_thread=False)) as second:
            with executing(first) as first_result:
                with executing(second) as second_result:
                    before = pool.status
                    assert before.operations == before.operation_limit == 2
                    with pytest.raises(ValueError, match="no decrease"):
                        pool.increase(connection_limit=3, cursor_limit=8, operation_limit=1)
                    assert pool.status == before
                with closing(second_result.result(timeout=5)) as rows:
                    assert rows.fetchone() == (7,)
            with closing(first_result.result(timeout=5)) as rows:
                assert rows.fetchone() == (7,)
            assert pool.status.operations == 0


@pytest.mark.parametrize("operations", [1, 2])
@pytest.mark.parametrize("same_connection", [False, True])
def test_actual_row_callback_preserves_outer_result_and_obeys_shared_operation_grant(
    operations, same_connection
):
    pool = admitted(operations=operations)
    with closing(pool.connect(":memory:")) as first:
        with closing(pool.connect(":memory:")) as second:
            target = first if same_connection else second
            observed = []

            def row_factory(_cursor, row):
                if row == (99,):
                    return row
                observed.append(row)
                assert pool.status.operations == 1
                if operations == 1:
                    with pytest.raises(SqliteCapacity, match="operation grant"):
                        target.execute("SELECT 99")
                else:
                    with closing(target.execute("SELECT 99")) as nested:
                        assert nested.fetchone() == (99,)
                assert pool.status.operations == 1
                return row

            first.row_factory = row_factory
            with closing(first.execute("SELECT 1 UNION ALL SELECT 2")) as rows:
                assert rows.fetchall() == [(1,), (2,)]
            assert observed == [(1,), (2,)]
            assert pool.status.operation_peak == operations
            assert pool.status.operations == pool.status.cursors == 0

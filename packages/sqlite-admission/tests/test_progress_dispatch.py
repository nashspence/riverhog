from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
import sqlite_admission.progress as progress_owner
from sqlite_admission import (
    SqliteConnectionAdmission,
    SqliteProgressConnection,
    observe_sqlite_execution,
)

QUERY = (
    "WITH RECURSIVE work(n) AS (VALUES(0) UNION ALL "
    "SELECT n+1 FROM work WHERE n<4096) SELECT sum(n) FROM work"
)


def _result(db):
    with closing(db.execute(QUERY)) as rows:
        return rows.fetchone()


def _pool():
    return SqliteConnectionAdmission(
        connection_limit=1,
        cursor_limit=2,
        operation_limit=1,
        verify_process=lambda: None,
    )


@pytest.fixture(params=["ordinary", "admitted"])
def connection(request):
    pool = None
    if request.param == "ordinary":
        db = sqlite3.connect(":memory:", factory=SqliteProgressConnection, cached_statements=0)
    else:
        pool = _pool()
        db = pool.connect(":memory:", cached_statements=0)
    try:
        yield db
    finally:
        db.close()
        if pool is not None:
            assert pool.status.connections == pool.status.cursors == pool.status.operations == 0


def test_actual_vm_observer_keeps_every_instruction_across_execution_registration(connection):
    observed = []
    connection.set_progress_handler(lambda: observed.append(None), 1)
    expected = _result(connection)
    baseline = len(observed)
    assert baseline > 1024
    checks = []
    with observe_sqlite_execution(connection, lambda: checks.append(None)):
        observed.clear()
        assert _result(connection) == expected
        assert len(observed) == baseline
    assert len(checks) >= 2
    observed.clear()
    assert _result(connection) == expected
    assert len(observed) == baseline


def test_original_cancellation_cause_interrupts_actual_vm_and_unregisters(connection):
    class Cancelled(RuntimeError):
        pass

    original = Cancelled("owner cancelled original execution")
    calls = []

    def check():
        calls.append(None)
        if len(calls) == 3:
            raise original

    with pytest.raises(Cancelled) as caught:
        with observe_sqlite_execution(connection, check):
            _result(connection)
    assert caught.value is original
    assert isinstance(caught.value.__cause__, sqlite3.OperationalError)
    assert caught.value.__cause__.sqlite_errorcode == sqlite3.SQLITE_INTERRUPT
    assert len(calls) == 3
    assert _result(connection) == (4096 * 4097 // 2,)
    with observe_sqlite_execution(connection, lambda: None):
        assert _result(connection) == (4096 * 4097 // 2,)


def test_observer_removal_cannot_remove_execution_deadline(connection):
    calls = []
    observed = []
    connection.set_progress_handler(lambda: observed.append(None), 1)

    def deadline():
        calls.append(None)
        if len(calls) > 1:
            raise TimeoutError("original execution deadline")

    with pytest.raises(TimeoutError, match="original execution deadline"):
        with observe_sqlite_execution(connection, deadline):
            connection.set_progress_handler(None, 0)
            _result(connection)
    assert calls == [None, None]
    assert observed == []
    assert _result(connection) == (4096 * 4097 // 2,)


def test_original_growth_observer_can_interrupt_without_becoming_owner_cancellation(connection):
    connection.set_progress_handler(lambda: 1, 1)
    try:
        with pytest.raises(sqlite3.OperationalError) as caught:
            with observe_sqlite_execution(connection, lambda: None):
                _result(connection)
        assert caught.value.sqlite_errorcode == sqlite3.SQLITE_INTERRUPT
        assert caught.value.__cause__ is None
    finally:
        connection.set_progress_handler(None, 0)


def test_nested_registration_cannot_replace_the_original_execution_check(connection):
    calls = []
    with observe_sqlite_execution(connection, lambda: calls.append(None)):
        with pytest.raises(RuntimeError, match="original.*still registered"):
            with observe_sqlite_execution(connection, lambda: pytest.fail("replacement ran")):
                pytest.fail("replacement was admitted")
        _result(connection)
    assert len(calls) >= 2


def test_unrelated_sql_error_keeps_its_original_error_and_receives_registration(connection):
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        with observe_sqlite_execution(connection, lambda: None):
            connection.execute("SELECT * FROM absent").close()
    with observe_sqlite_execution(connection, lambda: None):
        assert _result(connection) == (4096 * 4097 // 2,)


def test_already_expired_admission_precedes_actual_sql_work(connection):
    statements = []
    connection.set_trace_callback(statements.append)

    def expired():
        raise TimeoutError("expired before SQL")

    with pytest.raises(TimeoutError, match="expired before SQL"):
        with observe_sqlite_execution(connection, expired):
            _result(connection)
    assert statements == []


def test_original_registration_is_received_on_non_sql_body_failure(connection):
    class Interrupted(BaseException):
        pass

    original = Interrupted()
    with pytest.raises(Interrupted) as caught:
        with observe_sqlite_execution(connection, lambda: None):
            raise original
    assert caught.value is original
    with observe_sqlite_execution(connection, lambda: None):
        _result(connection)


def test_close_with_registered_check_preserves_actual_connection_receiving(connection):
    with observe_sqlite_execution(connection, lambda: None):
        connection.close()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        with observe_sqlite_execution(connection, lambda: None):
            pytest.fail("closed connection admitted execution")


def test_wrong_check_result_is_an_explicit_owner_error(connection):
    with pytest.raises(TypeError, match="return None or raise"):
        with observe_sqlite_execution(connection, lambda: True):
            pytest.fail("invalid check admitted")


def test_arbitrary_existing_hook_is_refused_without_replacement():
    calls = []
    with closing(sqlite3.connect(":memory:")) as db:
        db.set_progress_handler(lambda: calls.append(None), 1)
        with pytest.raises(TypeError, match="owned from connection creation"):
            with observe_sqlite_execution(db, lambda: None):
                pytest.fail("arbitrary driver hook was accepted")
        _result(db)
        assert len(calls) > 1024


@pytest.mark.parametrize("observer_period", [None, 1])
def test_cancelled_private_transaction_rolls_back_before_another_epoch(connection, observer_period):
    if observer_period is not None:
        connection.set_progress_handler(lambda: None, observer_period)
    connection.execute("CREATE TABLE effects(value)").close()
    connection.commit()
    calls = []

    def check():
        calls.append(None)
        if len(calls) > 1:
            raise TimeoutError("private epoch deadline")

    with pytest.raises(TimeoutError, match="private epoch deadline"):
        try:
            with observe_sqlite_execution(connection, check):
                connection.execute("BEGIN").close()
                connection.execute("INSERT INTO effects VALUES ('unfinished')").close()
                _result(connection)
                connection.commit()
        except BaseException:
            connection.rollback()
            raise
    with closing(connection.execute("SELECT count(*) FROM effects")) as rows:
        assert rows.fetchone() == (0,)
    assert not connection.in_transaction


def test_no_growth_observer_does_not_create_an_instruction_trace(connection):
    calls = []
    with observe_sqlite_execution(connection, lambda: calls.append(None)):
        _result(connection)
    # This bounds the owned control calls in this fixed VM workload; it is not
    # a timing target or an archive extent limit.
    assert 2 <= len(calls) < 1000


def test_observer_only_work_has_no_extra_per_instruction_dispatch(monkeypatch):
    dispatches = []
    observed = []
    original = progress_owner._Progress.dispatch

    def dispatch(self):
        dispatches.append(None)
        return original(self)

    monkeypatch.setattr(progress_owner._Progress, "dispatch", dispatch)
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        db.set_progress_handler(lambda: observed.append(None), 1)
        assert _result(db) == (4096 * 4097 // 2,)
        assert observed and dispatches == []
        with observe_sqlite_execution(db, lambda: None):
            _result(db)
        assert dispatches
        dispatches.clear()
        _result(db)
        assert dispatches == []


@pytest.mark.parametrize("period", [2**100, -(2**100)])
def test_invalid_observer_period_cannot_bypass_driver_validation_or_replace_hook(
    connection, period
):
    observed = 0

    def tick():
        nonlocal observed
        observed += 1
        return 0

    connection.set_progress_handler(tick, 1)
    with closing(sqlite3.connect(":memory:")) as raw:
        with pytest.raises(OverflowError):
            raw.set_progress_handler(lambda: None, period)
    checks = []
    with observe_sqlite_execution(connection, lambda: checks.append(None)):
        with pytest.raises(OverflowError):
            connection.set_progress_handler(lambda: 1, period)
        assert _result(connection) == (4096 * 4097 // 2,)
    assert observed > 1024 and len(checks) > 1

from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from riverhog_provenance._native_epoch import EpochContext
from sqlite_admission import SqliteProgressConnection


class _Connection(SqliteProgressConnection):
    commit_failure = None
    rollback_failure = None

    def commit(self):
        if self.commit_failure == "before":
            raise OSError("original commit failed before publication")
        super().commit()
        if self.commit_failure == "after":
            raise OSError("original commit response was lost")

    def rollback(self):
        if self.rollback_failure is not None:
            raise self.rollback_failure
        super().rollback()


@pytest.fixture
def db():
    with closing(sqlite3.connect(":memory:", factory=_Connection)) as connection:
        connection.execute("CREATE TABLE semantic(value TEXT NOT NULL)").close()
        yield connection


def _rows(db):
    with closing(db.execute("SELECT value FROM semantic")) as rows:
        return rows.fetchall()


def _mutation(db, value):
    def apply(epoch):
        epoch.require(db)
        db.execute("INSERT INTO semantic VALUES(?)", (value,)).close()
        return value

    return apply


def test_pending_live_state_is_installed_only_after_actual_private_commit(db):
    owner = EpochContext(db)
    current = ["before"]
    assert owner.quiescent

    def mutate(epoch):
        assert current == ["before"] and not owner.quiescent
        return _mutation(db, "after")(epoch)

    def receive(value):
        assert not db.in_transaction and not owner.quiescent
        assert _rows(db) == [("after",)]
        current[:] = [value]

    assert owner.apply(mutate, receive=receive) == "after"
    assert current == ["after"] and owner.quiescent


@pytest.mark.parametrize("failure", ["before", "after"])
def test_ambiguous_original_commit_never_publishes_live_state_or_reuses_executor(db, failure):
    owner = EpochContext(db)
    received = []
    db.commit_failure = failure
    with pytest.raises(OSError, match="original commit") as caught:
        owner.apply(_mutation(db, "candidate"), receive=received.append)
    assert received == [] and not owner.quiescent
    assert _rows(db) == ([] if failure == "before" else [("candidate",)])
    db.commit_failure = None
    with pytest.raises(RuntimeError, match="requires reconstruction") as reused:
        owner.apply(_mutation(db, "replacement"), receive=received.append)
    assert reused.value.__cause__ is caught.value
    assert received == []


def test_precommit_base_exception_receives_rollback_without_publishing_pending_state(db):
    owner = EpochContext(db)
    received = []

    class Cancelled(BaseException):
        pass

    original = Cancelled()

    def mutate(epoch):
        _mutation(db, "rolled-back")(epoch)
        raise original

    with pytest.raises(Cancelled) as caught:
        owner.apply(mutate, receive=received.append)
    assert caught.value is original
    assert _rows(db) == [] and received == [] and not owner.quiescent


def test_postcommit_live_receiving_failure_requires_reconstruction(db):
    owner = EpochContext(db)
    original = RuntimeError("live graph receiving failed")

    def receive(value):
        assert value == "committed"
        raise original

    with pytest.raises(RuntimeError) as caught:
        owner.apply(_mutation(db, "committed"), receive=receive)
    assert caught.value is original and _rows(db) == [("committed",)]
    assert not owner.quiescent
    with pytest.raises(RuntimeError, match="requires reconstruction"):
        owner.apply(_mutation(db, "later"), receive=lambda _: None)


def test_nested_epoch_refusal_preserves_the_original_active_writer(db):
    owner = EpochContext(db)

    def mutate(epoch):
        with pytest.raises(RuntimeError, match="already active"):
            owner.apply(_mutation(db, "nested"), receive=lambda _: None)
        epoch.require(db)
        return _mutation(db, "original")(epoch)

    assert owner.apply(mutate, receive=lambda _: None) == "original"
    assert owner.quiescent and _rows(db) == [("original",)]


def test_foreign_writer_and_connection_cannot_be_borrowed_as_an_epoch(db):
    owner = EpochContext(db)
    db.execute("BEGIN").close()
    with pytest.raises(RuntimeError, match="foreign writer"):
        owner.apply(_mutation(db, "refused"), receive=lambda _: None)
    assert db.in_transaction and _rows(db) == []
    db.rollback()
    with closing(sqlite3.connect(":memory:")) as foreign:
        with pytest.raises(RuntimeError, match="original active"):
            owner.apply(lambda epoch: epoch.require(foreign), receive=lambda _: None)
    assert not owner.quiescent


def test_child_cannot_commit_behind_the_original_epoch(db):
    owner = EpochContext(db)
    received = []

    def mutate(epoch):
        _mutation(db, "unexpected-commit")(epoch)
        db.commit()

    with pytest.raises(RuntimeError, match="original active"):
        owner.apply(mutate, receive=received.append)
    assert received == [] and not owner.quiescent
    assert _rows(db) == [("unexpected-commit",)]


def test_failed_rollback_keeps_original_failure_as_diagnostic_cause(db):
    owner = EpochContext(db)
    original = RuntimeError("original application failed")
    cleanup = OSError("original rollback failed")
    db.rollback_failure = cleanup

    def mutate(epoch):
        _mutation(db, "unreceived")(epoch)
        raise original

    with pytest.raises(OSError) as caught:
        owner.apply(mutate, receive=lambda _: None)
    assert caught.value is cleanup and caught.value.__context__ is original
    assert not owner.quiescent and db.in_transaction
    db.rollback_failure = None
    db.rollback()
    with pytest.raises(RuntimeError, match="requires reconstruction") as reused:
        owner.apply(_mutation(db, "later"), receive=lambda _: None)
    assert reused.value.__cause__ is original


@pytest.mark.parametrize("observe_every_instruction", [False, True])
def test_actual_vm_cancellation_receives_registration_before_rollback(
    db, observe_every_instruction
):
    owner = EpochContext(db)
    original = TimeoutError("original private epoch expired")
    checks = 0
    received = []
    if observe_every_instruction:
        db.set_progress_handler(lambda: None, 1)

    def check():
        nonlocal checks
        checks += 1
        if checks > 1:
            raise original

    def mutate(epoch):
        _mutation(db, "unfinished")(epoch)
        with closing(
            db.execute(
                "WITH RECURSIVE work(n) AS (VALUES(0) UNION ALL "
                "SELECT n+1 FROM work WHERE n<4096) SELECT sum(n) FROM work"
            )
        ) as rows:
            rows.fetchone()

    with pytest.raises(TimeoutError) as caught:
        owner.apply(mutate, receive=received.append, check=check)
    assert caught.value is original
    assert _rows(db) == [] and not db.in_transaction and received == []
    assert not owner.quiescent


def test_short_epoch_checks_execution_before_commit_even_without_a_vm_sample(db):
    owner = EpochContext(db)
    original = TimeoutError("original short epoch expired")
    checks = []
    received = []

    def check():
        checks.append(1)
        if len(checks) == 2:
            raise original

    with pytest.raises(TimeoutError) as caught:
        owner.apply(_mutation(db, "unfinished"), receive=received.append, check=check)
    assert caught.value is original
    assert checks == [1, 1]
    assert _rows(db) == [] and received == [] and not owner.quiescent


def test_expiry_after_commit_does_not_skip_original_live_receiving(db):
    owner = EpochContext(db)
    received = []
    expired = False
    commit = db.commit

    def publish():
        nonlocal expired
        commit()
        expired = True

    db.commit = publish

    def check():
        if expired:
            raise TimeoutError("deadline expired after the original commit")

    def receive(value):
        received.append(value)
        assert expired and not owner.quiescent

    owner.apply(_mutation(db, "committed"), receive=receive, check=check)
    assert received == ["committed"] and owner.quiescent

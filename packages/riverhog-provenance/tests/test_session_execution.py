from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager
from copy import deepcopy

import pytest
from riverhog_provenance import append_correction, assertion_reference, new_id, validate_journal
from riverhog_provenance._entry_preparation import EntryPreparation
from riverhog_provenance._native_epoch import EpochContext
from riverhog_provenance.journal_session import JournalValidationSession
from sqlite_admission import SqliteProgressConnection
from test_native_epoch_integration import _reach

ATTEMPT = "a" * 32
QUERY = (
    "WITH RECURSIVE work(n) AS (VALUES(0) UNION ALL "
    "SELECT n+1 FROM work WHERE n<4096) SELECT sum(n) FROM work"
)


class _Connection(SqliteProgressConnection):
    expire_after_commit = False
    expired = False

    def commit(self):
        super().commit()
        if self.expire_after_commit:
            self.expired = True


@pytest.mark.parametrize("correction", [False, True])
def test_actual_native_children_share_the_original_checked_execution(
    journal, catalog, who, monkeypatch, correction
):
    if correction:
        with validate_journal(journal, catalog=catalog) as initial:
            original = initial.graph["artifacts"][0]
            replacement = deepcopy(original)
            replacement["assertion_id"] = new_id()
            target = assertion_reference(initial, original["assertion_id"])
        journal = append_correction(
            journal,
            [target],
            reason="Replace its documentary assertion.",
            recorded_by_agent_id=who,
            assertions={"artifacts": [replacement]},
            catalog=catalog,
        )
    with validate_journal(journal, catalog=catalog) as expected:
        with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
            original = EpochContext.preparing
            preparations, checks = [], []

            @contextmanager
            def preparing(epoch):
                assert epoch is session._ledger.epoch
                assert epoch._execution_active
                preparations.append(1)
                with original(epoch):
                    yield

            def check():
                checks.append(1)

            monkeypatch.setattr(EpochContext, "preparing", preparing)
            for _ in range(4096):
                progress = session.advance(check=check)
                if progress.complete:
                    break
            else:
                pytest.fail("small Native execution did not finish")
            assert preparations and len(checks) >= len(preparations)
            result = session.result()
        with result:
            assert result.materialize() == expected.materialize()
    assert session.storage_received


@pytest.mark.parametrize("observe_every_instruction", [False, True])
def test_actual_detached_child_query_observes_deadline_and_receives_registration(
    journal, catalog, monkeypatch, observe_every_instruction
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        _reach(session, "entry-admission")
        before = session._ledger.load()
        original = EntryPreparation._admission
        expired = False
        failure = TimeoutError("original Native preparation expired")
        observed = []
        if observe_every_instruction:
            session._store.db.set_progress_handler(lambda: observed.append(1), 1)

        def prepare(child):
            nonlocal expired
            assert child.epoch is session._ledger.epoch and not child.db.in_transaction
            expired = True
            with closing(child.db.execute(QUERY)) as rows:
                rows.fetchone()
            return original(child)

        def check():
            if expired:
                raise failure

        monkeypatch.setattr(EntryPreparation, "_admission", prepare)
        with pytest.raises(TimeoutError) as caught:
            session.advance(check=check)
        assert caught.value is failure
        assert session._ledger.epoch.quiescent
        assert session._ledger.load() is before
        if observe_every_instruction:
            assert len(observed) >= 1024
        # The same connection can read after the exact cancelled registration exits.
        with closing(session._store.db.execute(QUERY)) as rows:
            assert rows.fetchone() == (4096 * 4097 // 2,)
        with pytest.raises(RuntimeError, match="session failed"):
            session.advance()
    assert session.storage_received


def test_actual_native_cancellation_before_commit_rolls_back_without_live_publication(
    journal, catalog
):
    with JournalValidationSession((journal,), attempt_id=ATTEMPT, catalog=catalog) as session:
        session.advance()
        before = session._ledger.load()
        cancelled = False
        failure = RuntimeError("original Native execution cancelled")

        def trace(sql):
            nonlocal cancelled
            if sql.startswith("INSERT INTO journal_validation_frame"):
                cancelled = True

        def check():
            if cancelled:
                raise failure

        session._store.db.set_trace_callback(trace)
        try:
            with pytest.raises(RuntimeError) as caught:
                session.advance(check=check)
        finally:
            session._store.db.set_trace_callback(None)
        assert caught.value is failure
        assert session._ledger._received is before
        assert session._replay._current is None
        assert not session._store.db.in_transaction
        with closing(
            session._store.db.execute("SELECT count(*) FROM journal_validation_frame")
        ) as rows:
            assert rows.fetchone() == (0,)
        with pytest.raises(RuntimeError, match="requires reconstruction"):
            session.progress()
    assert session.storage_received


def test_actual_expiry_after_commit_finishes_original_live_receiving(journal, catalog):
    def connect(*args, **options):
        return sqlite3.connect(*args, factory=_Connection, **options)

    with JournalValidationSession(
        (journal,), attempt_id=ATTEMPT, catalog=catalog, connection_factory=connect
    ) as session:
        session.advance()
        db = session._store.db
        failure = TimeoutError("original deadline expired after commit")

        def check():
            if db.expired:
                raise failure

        db.expire_after_commit = True
        progress = session.advance(check=check)
        assert db.expired and progress.phase == "entry-admission"
        assert session._replay._current is not None and not session._input.pending
        assert session._ledger.epoch.quiescent
        with pytest.raises(TimeoutError) as caught:
            session.advance(check=check)
        assert caught.value is failure
    assert session.storage_received


def test_actual_execution_check_refuses_an_unowned_sqlite_progress_hook(journal, catalog):
    with JournalValidationSession(
        (journal,), attempt_id=ATTEMPT, catalog=catalog, connection_factory=sqlite3.connect
    ) as session:
        before = session._ledger.load()
        with pytest.raises(TypeError, match="owned from connection creation"):
            session.advance(check=lambda: None)
        assert session._ledger.load() is before
    assert session.storage_received


def test_execution_scope_cannot_be_reentered_or_replace_its_original_check():
    with closing(sqlite3.connect(":memory:", factory=SqliteProgressConnection)) as db:
        epoch = EpochContext(db)

        def original():
            return None

        with epoch.execution(original):
            with pytest.raises(RuntimeError, match="already active"):
                with epoch.execution(original):
                    pytest.fail("nested execution admitted")
            with epoch.preparing():
                with epoch.preparing():
                    with closing(db.execute("SELECT 1")) as rows:
                        assert rows.fetchone() == (1,)
                with pytest.raises(RuntimeError, match="preparation must be received"):
                    epoch.apply(lambda _owner: None, receive=lambda _pending: None)
            with pytest.raises(RuntimeError, match="cannot replace"):
                epoch.apply(lambda _owner: None, receive=lambda _pending: None, check=lambda: None)
            epoch.apply(lambda _owner: None, receive=lambda _pending: None)
        assert epoch.quiescent

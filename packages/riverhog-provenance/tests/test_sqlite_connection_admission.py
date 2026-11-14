from __future__ import annotations

import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest
import sqlite_admission
from riverhog_provenance import (
    CorpusConstruction,
    JournalConstruction,
    RetainedCorpusValidator,
    corpus_workspace,
    journal_store,
)
from riverhog_provenance.journal_session import JournalValidationSession
from riverhog_provenance.journal_store import JournalStore
from sqlite_admission import SqliteConnectionAdmission
from test_retained_corpus_faults import EmptySource
from test_retained_journal_session import ATTEMPT, OwnedSource, finish

from tests.support.sqlite_admission import Capacity

_CONNECT = sqlite3.connect
_POSIX = pytest.mark.skipif(os.name != "posix", reason="retained corpus uses POSIX custody")


class Admission:
    """One actual connection slot, released only after a confirmed close."""

    def __init__(self):
        self.active = 0
        self.peak = 0
        self.attempts = []
        self.connections = []
        self.close_attempts = 0
        self.reject = False
        self.fail_close = False
        self.close_before_failure = False
        self.fail_construction = False

    def __call__(self, database, **options):
        self.attempts.append((database, options))
        if self.reject or self.active:
            raise Capacity("connection population exhausted")
        self.active += 1
        self.peak = max(self.peak, self.active)
        owner = self

        class Connection(sqlite3.Connection):
            released = False

            def close(self):
                owner.close_attempts += 1
                if owner.fail_close:
                    owner.fail_close = False
                    if owner.close_before_failure:
                        super().close()
                    raise OSError("connection close unconfirmed")
                super().close()
                if not self.released:
                    owner.active -= 1
                    self.released = True

        # Admission precedes the real engine open, not just its schema work.
        assert self.active == 1
        db = _CONNECT(database, factory=Connection, **options)
        self.connections.append(db)
        if self.fail_construction:
            raise OSError("connection construction unconfirmed")
        db.execute("PRAGMA mmap_size=0").close()
        return db


def private_directory(tmp_path, name="retained"):
    directory = tmp_path / name
    directory.mkdir(mode=0o700)
    return directory


def no_default_connection(*_args, **_kwargs):
    raise AssertionError("selected connection factory was bypassed")


class RetainedAdmission:
    """Observe and fault the actual original admitted connection, never replace it."""

    def __init__(self, monkeypatch):
        self.reject = self.fail_close = self.close_before_failure = False
        self.fail_construction = False
        self.attempts, self.connections, self.frames = [], [], []
        self.close_attempts = 0
        self.owner = SqliteConnectionAdmission(
            connection_limit=1,
            cursor_limit=128,
            operation_limit=4,
            verify_process=self.verify,
        )
        driver, driver_close = sqlite_admission._connect, sqlite_admission._Connection._driver_close

        def connect(database, **options):
            lease = getattr(options.get("factory"), "_reservation", None)
            if lease is None or lease.admission is not self.owner:
                return driver(database, **options)
            self.attempts.append((database, {k: v for k, v in options.items() if k != "factory"}))
            assert self.active == 1
            actual = driver(database, **options)
            self.connections.append(actual)
            if self.fail_construction:
                raise OSError("connection construction unconfirmed")
            return actual

        def close(connection):
            if connection._lease.admission is not self.owner:
                return driver_close(connection)
            self.close_attempts += 1
            if self.fail_close:
                self.fail_close = False
                if self.close_before_failure:
                    driver_close(connection)
                raise OSError("connection close unconfirmed")
            return driver_close(connection)

        monkeypatch.setattr(sqlite_admission, "_connect", connect)
        monkeypatch.setattr(sqlite_admission._Connection, "_driver_close", close)

    def verify(self):
        if self.reject:
            raise Capacity("connection population exhausted")

    @property
    def active(self):
        return self.owner.status.connections

    @property
    def peak(self):
        return self.owner.status.connection_peak

    def retain(self, *, corpus=False):
        cls, count = (CorpusConstruction, 3) if corpus else (JournalConstruction, 2)
        original = cls(self.owner.connection_scope(), resource_limit=count)
        self.frames.append(original)
        return original

    def close(self):
        while self.frames:
            assert self.frames[-1].close_step()
            self.frames.pop()
        status = self.owner.status
        assert not (status.connections or status.cursors or status.blobs or status.operations)


@pytest.fixture
def retained_admission(monkeypatch):
    original = RetainedAdmission(monkeypatch)
    try:
        yield original
    finally:
        original.close()


def scalar(db, query):
    with closing(db.execute(query)) as rows:
        return rows.fetchone()


@pytest.mark.parametrize("after", [False, True])
def test_retained_journal_factory_keeps_one_slot_through_shared_readers_and_uncertain_close(
    tmp_path, monkeypatch, after, retained_admission
):
    path = private_directory(tmp_path)
    admission = retained_admission
    monkeypatch.setattr(journal_store.sqlite3, "connect", no_default_connection)
    handles = []
    current = JournalStore.open_retained(
        path, construction=admission.retain(), create=True, on_storage_open=handles.append
    )
    assert handles == [current] and admission.active == admission.peak == 1
    uri, options = admission.attempts[0]
    assert uri == (path / "snapshot.sqlite3").resolve().as_uri() + "?mode=rw"
    assert options == {"uri": True, "check_same_thread": False, "cached_statements": 0}
    assert current.db is admission.connections[0]
    assert scalar(current.db, "PRAGMA cache_size") == (-512,)
    assert scalar(current.db, "PRAGMA temp_store") == (1,)
    current.freeze()
    first, last = current.borrow(), current.borrow()
    try:
        current.close()
        first.close()
        assert admission.active == 1 and admission.close_attempts == 0
        with ThreadPoolExecutor(max_workers=1) as worker:
            assert worker.submit(scalar, last.db, "SELECT 1").result(10) == (1,)
        admission.fail_close, admission.close_before_failure = True, after
        with pytest.raises(OSError, match="close unconfirmed"):
            last.close()
        assert admission.active == 1 and not last._lease.closed
        last.close()
        assert admission.active == 0 and admission.close_attempts == 2
        last.close()
        assert admission.close_attempts == 2
    finally:
        first.close()
        last.close()
        current.close()
    with closing(
        JournalStore.open_retained(path, construction=admission.retain(), read_only=True)
    ) as cold:
        assert admission.active == 1 and cold.db is admission.connections[-1]
        assert admission.attempts[-1][0].endswith("?mode=ro")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            cold.db.execute("DELETE FROM findings")
    assert admission.active == 0 and admission.peak == 1


def test_retained_journal_factory_refuses_before_schema_or_recovery_and_preserves_bytes(
    tmp_path, retained_admission
):
    path = private_directory(tmp_path)
    admission = retained_admission
    with closing(JournalStore.open_retained(path, construction=admission.retain(), create=True)):
        pass
    before = (path / "snapshot.sqlite3").read_bytes()
    admission.reject = True
    handles = []
    with pytest.raises(Capacity):
        JournalStore.open_retained(
            path, construction=admission.retain(), on_storage_open=handles.append
        )
    assert admission.active == 0 and not handles and len(admission.connections) == 1
    assert (path / "snapshot.sqlite3").read_bytes() == before
    admission.reject = False
    with closing(JournalStore.open_retained(path, construction=admission.retain())):
        pass
    assert admission.active == 0


@pytest.mark.parametrize("after", [False, True])
def test_retained_journal_factory_initialization_failure_keeps_unconfirmed_close_reachable(
    tmp_path, monkeypatch, after, retained_admission
):
    path = private_directory(tmp_path)
    admission = retained_admission
    admission.fail_close, admission.close_before_failure = True, after
    handles = []

    def fail_schema(_store):
        raise InterruptedError("schema execution interrupted")

    monkeypatch.setattr(JournalStore, "_initialize_schema", fail_schema)
    original = admission.retain()
    with pytest.raises(OSError, match="close unconfirmed"):
        JournalStore.open_retained(
            path, construction=original, create=True, on_storage_open=handles.append
        )
    assert len(handles) == 1 and admission.active == 1 and path.is_dir()
    handles[0].close()
    assert original.close_step()
    assert admission.active == 0 and admission.close_attempts == 2


@pytest.mark.parametrize("corpus", [False, True])
def test_factory_retains_its_own_unconfirmed_construction_without_owner_reopening(
    tmp_path, catalog, corpus, retained_admission
):
    if corpus and os.name != "posix":
        pytest.skip("retained corpus uses POSIX custody")
    path = private_directory(tmp_path)
    admission = retained_admission
    admission.fail_construction = True
    handles = []
    original = admission.retain(corpus=corpus)
    with pytest.raises(OSError, match="construction unconfirmed"):
        if corpus:
            RetainedCorpusValidator.open_retained(
                path,
                EmptySource(catalog),
                construction=original,
                worker_epoch=0,
                create=True,
                on_storage_open=handles.append,
            )
        else:
            JournalStore.open_retained(
                path, construction=original, create=True, on_storage_open=handles.append
            )
    assert not handles and len(admission.attempts) == len(admission.connections) == 1
    assert admission.active == int(not corpus) and admission.close_attempts == int(corpus)
    # Corpus receives its database internally on failure, but retains its live guard.
    # Journal receiving starts from the same unreturned connection's exact scope.
    assert not original.received
    if corpus:
        assert original.borrow.active
    assert original.close_step()
    assert admission.active == 0 and admission.close_attempts == 1


def test_retained_session_delegates_factory_before_source_and_keeps_frozen_result_slot(
    tmp_path, journal, catalog, monkeypatch, retained_admission
):
    path = private_directory(tmp_path)
    source = OwnedSource(journal, catalog)
    admission = retained_admission
    admission.reject = True
    with pytest.raises(Capacity):
        JournalValidationSession.open_retained(
            path,
            source,
            construction=admission.retain(),
            attempt_id=ATTEMPT,
            worker_epoch=0,
            create=True,
            catalog=catalog,
        )
    assert not source.calls and not admission.connections
    assert not (path / "snapshot.sqlite3").exists()
    path = private_directory(tmp_path, "accepted")
    admission.reject = False
    monkeypatch.setattr(journal_store.sqlite3, "connect", no_default_connection)
    with JournalValidationSession.open_retained(
        path,
        source,
        construction=admission.retain(),
        attempt_id=ATTEMPT,
        worker_epoch=0,
        create=True,
        catalog=catalog,
    ) as session:
        finish(session)
        result = session.result()
    try:
        assert admission.active == 1 and len(admission.connections) == 1
        assert result.journal_sha256 == source.binding.source_sha256
    finally:
        result.close()
    assert admission.active == 0 and admission.close_attempts == 1


@_POSIX
def test_retained_corpus_factory_admits_one_connection_before_schema_and_worker_replacement(
    tmp_path, catalog, monkeypatch, retained_admission
):
    path = private_directory(tmp_path)
    admission = retained_admission
    source = EmptySource(catalog)
    monkeypatch.setattr(corpus_workspace.sqlite3, "connect", no_default_connection)
    with RetainedCorpusValidator.open_retained(
        path, source, construction=admission.retain(corpus=True), worker_epoch=0, create=True
    ) as current:
        uri, options = admission.attempts[0]
        assert uri == (path / "corpus.sqlite3").absolute().as_uri() + "?mode=rw"
        assert options == {"uri": True, "check_same_thread": False}
        assert current._db is admission.connections[0] and admission.active == 1
        assert scalar(current._db, "PRAGMA cache_size") == (-512,)
        with ThreadPoolExecutor(max_workers=1) as worker:
            assert worker.submit(current.advance_semantics).result(10).complete
    assert admission.active == 0
    before = (path / "corpus.sqlite3").read_bytes()
    admission.reject = True
    with pytest.raises(Capacity):
        RetainedCorpusValidator.open_retained(
            path, source, construction=admission.retain(corpus=True), worker_epoch=1
        )
    assert admission.active == 0 and len(admission.connections) == 1
    assert (path / "corpus.sqlite3").read_bytes() == before
    admission.reject = False
    with RetainedCorpusValidator.open_retained(
        path, source, construction=admission.retain(corpus=True), worker_epoch=1
    ) as cold:
        assert cold.advance_semantics().complete
    assert admission.active == 0 and admission.peak == 1


@_POSIX
@pytest.mark.parametrize("after", [False, True])
def test_retained_corpus_factory_close_failure_preserves_slot_and_reachable_workspace(
    tmp_path, catalog, monkeypatch, after, retained_admission
):
    path = private_directory(tmp_path)
    admission = retained_admission
    admission.fail_close, admission.close_before_failure = True, after
    handles = []

    def fail_schema(_workspace):
        raise InterruptedError("schema execution interrupted")

    monkeypatch.setattr(RetainedCorpusValidator, "_initialize", fail_schema)
    original = admission.retain(corpus=True)
    with pytest.raises(OSError, match="close unconfirmed"):
        RetainedCorpusValidator.open_retained(
            path,
            EmptySource(catalog),
            construction=original,
            worker_epoch=0,
            create=True,
            on_storage_open=handles.append,
        )
    assert len(handles) == 1 and admission.active == 1 and original.borrow.active
    handles[0].close()
    assert original.received and original.borrow.received and admission.active == 0
    assert admission.close_attempts == 2

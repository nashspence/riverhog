from __future__ import annotations

import os
import sqlite3
from contextlib import closing

import pytest
from riverhog_provenance import journal_store as owner
from riverhog_provenance.journal_session import JournalValidationSession
from riverhog_provenance.journal_store import JournalStore
from test_retained_journal_session import ATTEMPT, OwnedSource, finish


def fault_connection(monkeypatch, *, fail=False, after=False):
    actual_connect = sqlite3.connect
    state = {"fail": fail, "closes": 0, "connections": []}

    class Connection(sqlite3.Connection):
        def close(self):
            state["closes"] += 1
            if state["fail"]:
                state["fail"] = False
                if after:
                    super().close()
                raise OSError("database close unconfirmed")
            return super().close()

    def connect(*args, **kwargs):
        result = actual_connect(*args, factory=Connection, **kwargs)
        state["connections"].append(result)
        return result

    monkeypatch.setattr(owner.sqlite3, "connect", connect)
    return state


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("after", [False, True])
def test_last_reader_retries_uncertain_database_close_without_losing_its_lease(
    tmp_path, monkeypatch, retained, after
):
    state = fault_connection(monkeypatch, after=after)
    path = tmp_path / "retained"
    path.mkdir()
    released = []
    if retained:
        current = JournalStore.open_retained(
            path, create=True, on_storage_close=lambda: released.append("released")
        )
    else:
        current = JournalStore()
        path = current._storage.directory
    current.freeze()
    first = current.borrow()
    last = first.borrow()
    storage = current._storage
    current.close()
    first.close()
    state["fail"] = True
    with pytest.raises(OSError, match="unconfirmed"):
        last.close()
    assert state["closes"] == 1 and storage._readers == 1 and not storage._closed
    assert not last._lease.closed and last._cleanup.alive and not released
    with pytest.raises(RuntimeError, match="closed"):
        _ = last.db
    with pytest.raises(RuntimeError, match="closed"):
        last.borrow()
    last.close()
    assert state["closes"] == 2 and storage._readers == 0 and storage._closed
    assert released == (["released"] if retained else [])
    assert path.exists() == retained
    current.close()
    first.close()
    last.close()
    assert state["closes"] == 2


def test_owner_release_failure_keeps_real_kernel_custody_until_confirmed_retry(
    tmp_path, monkeypatch
):
    fcntl = pytest.importorskip("fcntl")
    state = fault_connection(monkeypatch)
    lease_path = tmp_path / "owner.lease"
    descriptor = os.open(lease_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    attempts = []

    def release():
        with pytest.raises(sqlite3.ProgrammingError):
            state["connections"][0].execute("SELECT 1")
        attempts.append(1)
        if len(attempts) == 1:
            raise OSError("owner release unconfirmed")
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    path = tmp_path / "retained"
    path.mkdir()
    current = JournalStore.open_retained(path, create=True, on_storage_close=release)
    current.freeze()
    last = current.borrow()
    current.close()
    try:
        with pytest.raises(OSError, match="owner release"):
            last.close()
        assert state["closes"] == 1 and current._storage.on_close is release
        assert current._storage._readers == 1 and not last._lease.closed
        with closing(lease_path.open("r+b")) as contender:
            with pytest.raises(BlockingIOError):
                fcntl.flock(contender.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            last.close()
            fcntl.flock(contender.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert attempts == [1, 1] and state["closes"] == 1
        assert current._storage.on_close is None and current._storage._readers == 0
        last.close()
        assert attempts == [1, 1]
    finally:
        last.close()


@pytest.mark.parametrize("after", [False, True])
def test_disposable_directory_cleanup_is_retryable_after_database_close(monkeypatch, after):
    state = fault_connection(monkeypatch)
    current = JournalStore()
    directory = current._storage.directory
    actual = owner.shutil.rmtree
    attempts = []

    def remove(path, *args, **kwargs):
        attempts.append(path)
        if len(attempts) == 1:
            if after:
                actual(path, *args, **kwargs)
            raise OSError("directory removal unconfirmed")
        return actual(path, *args, **kwargs)

    monkeypatch.setattr(owner.shutil, "rmtree", remove)
    with pytest.raises(OSError, match="directory removal"):
        current.close()
    assert state["closes"] == 1 and current._storage._readers == 1
    assert not current._storage._closed
    current.close()
    assert not directory.exists() and state["closes"] == 1
    assert current._storage._readers == 0 and current._storage._closed


def test_explicit_close_can_retry_even_after_an_uncertain_finalizer_attempt(tmp_path, monkeypatch):
    state = fault_connection(monkeypatch)
    path = tmp_path / "retained"
    path.mkdir()
    current = JournalStore.open_retained(path, create=True)
    state["fail"] = True
    with pytest.raises(OSError, match="unconfirmed"):
        current._cleanup()
    assert not current._cleanup.alive and not current._lease.closed
    current.close()
    assert current._lease.closed and state["closes"] == 2


@pytest.mark.parametrize("failure", ["store", "prefix", "retention"])
def test_early_session_retention_preserves_failed_initialization_and_close_for_retry(
    tmp_path, journal, catalog, monkeypatch, failure
):
    state = fault_connection(monkeypatch, fail=True)
    retained, released = [], []
    path = tmp_path / "retained"
    path.mkdir()

    def retain(current):
        retained.append(current)
        assert not hasattr(current, "_prefix")
        if failure == "retention":
            raise RuntimeError("retention callback interrupted")

    def interrupted(*args, **kwargs):
        raise RuntimeError("initialization interrupted")

    if failure == "store":
        monkeypatch.setattr(JournalStore, "_initialize_schema", interrupted)
    elif failure == "prefix":
        monkeypatch.setattr(JournalValidationSession, "_initialize", interrupted)
    with pytest.raises(OSError, match="close unconfirmed"):
        JournalValidationSession.open_retained(
            path,
            OwnedSource(journal, catalog),
            attempt_id=ATTEMPT,
            worker_epoch=0,
            create=True,
            catalog=catalog,
            on_storage_open=retain,
            on_storage_close=lambda: released.append(1),
        )
    assert len(retained) == 1 and state["closes"] == 1 and not released
    current = retained[0]
    assert current._closing and not current._closed and (path / "snapshot.sqlite3").exists()
    with pytest.raises(RuntimeError, match="closed"):
        current.advance()
    current.close()
    current.close()
    assert current._closed and state["closes"] == 2 and not released
    assert (path / "snapshot.sqlite3").is_file()


@pytest.mark.parametrize("completed", [False, True])
def test_session_or_frozen_result_retries_a_failed_last_database_close(
    tmp_path, journal, catalog, monkeypatch, completed
):
    state = fault_connection(monkeypatch)
    path = tmp_path / "retained"
    path.mkdir()
    retained, released = [], []
    session = JournalValidationSession.open_retained(
        path,
        OwnedSource(journal, catalog),
        attempt_id=ATTEMPT,
        worker_epoch=0,
        create=True,
        catalog=catalog,
        on_storage_open=retained.append,
        on_storage_close=lambda: released.append(1),
    )
    assert retained == [session]
    if completed:
        finish(session)
        target = session.result()
        session.close()
        assert not released
    else:
        target = session
    state["fail"] = True
    with pytest.raises(OSError, match="close unconfirmed"):
        target.close()
    assert not released and state["closes"] == 1
    if completed:
        with pytest.raises(RuntimeError, match="closed"):
            target.borrow()
    else:
        assert not session._closed
        with pytest.raises(RuntimeError, match="closed"):
            session.advance()
    target.close()
    assert released == [1] and state["closes"] == 2
    target.close()
    session.close()
    assert released == [1] and state["closes"] == 2

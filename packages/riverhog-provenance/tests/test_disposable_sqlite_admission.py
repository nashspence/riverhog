from __future__ import annotations

import gc
import shutil
import sqlite3
from pathlib import Path

import pytest
from riverhog_provenance import journal_store, validate_journal_chunks
from riverhog_provenance.journal_session import JournalValidationSession
from riverhog_provenance.journal_store import JournalStore
from test_sqlite_connection_admission import Admission, Capacity, no_default_connection


def directories_under(tmp_path, monkeypatch):
    make = journal_store.mkdtemp
    paths = []

    def create(**options):
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(journal_store, "mkdtemp", create)
    return paths


class UnreadChunks:
    def __iter__(self):
        raise AssertionError("input was consumed before connection admission")


def test_disposable_admission_refusal_precedes_source_and_leaves_factory_cleanup_owned(
    tmp_path, monkeypatch
):
    paths = directories_under(tmp_path, monkeypatch)
    admission = Admission()
    admission.reject = True
    handles = []
    with pytest.raises(Capacity):
        validate_journal_chunks(
            UnreadChunks(), connection_factory=admission, on_storage_open=handles.append
        )
    assert len(paths) == len(admission.attempts) == 1 and not handles
    assert admission.active == 0 and not admission.connections
    assert admission.attempts[0] == (
        paths[0] / "snapshot.sqlite3",
        {"check_same_thread": False, "cached_statements": 0},
    )
    assert paths[0].is_dir() and not list(paths[0].iterdir())
    # A supplied factory owns its failed call and the received private path.
    shutil.rmtree(paths[0])


@pytest.mark.parametrize("after", [False, True])
def test_disposable_unreturned_connection_keeps_live_files_and_slot_until_factory_settles(
    tmp_path, monkeypatch, after
):
    paths = directories_under(tmp_path, monkeypatch)
    admission = Admission()
    admission.fail_construction = True
    with pytest.raises(OSError, match="construction unconfirmed"):
        JournalStore(connection_factory=admission)
    gc.collect()
    db = admission.connections[0]
    db.execute("CREATE TABLE owned(value TEXT)").close()
    db.execute("INSERT INTO owned VALUES ('exact original')").close()
    db.commit()
    assert admission.active == 1 and paths[0].is_dir()
    assert db.execute("SELECT value FROM owned").fetchone() == ("exact original",)
    admission.fail_close, admission.close_before_failure = True, after
    with pytest.raises(OSError, match="close unconfirmed"):
        db.close()
    assert admission.active == 1 and paths[0].is_dir()
    db.close()
    assert admission.active == 0 and admission.close_attempts == 2
    shutil.rmtree(paths[0])


@pytest.mark.parametrize("after", [False, True])
def test_disposable_schema_failure_retains_closeable_store_before_sql_execution(
    tmp_path, monkeypatch, after
):
    paths = directories_under(tmp_path, monkeypatch)
    admission = Admission()
    admission.fail_close, admission.close_before_failure = True, after
    handles = []

    def retain(store):
        handles.append(store)
        assert store.db.execute("SELECT count(*) FROM sqlite_master").fetchone() == (0,)

    def interrupted(_store):
        raise InterruptedError("schema initialization interrupted")

    monkeypatch.setattr(JournalStore, "_initialize_schema", interrupted)
    with pytest.raises(OSError, match="close unconfirmed"):
        JournalStore(connection_factory=admission, on_storage_open=retain)
    assert len(handles) == 1 and admission.active == 1 and paths[0].is_dir()
    handles[0].close()
    assert admission.active == 0 and not paths[0].exists()
    assert admission.close_attempts == 2


@pytest.mark.parametrize("helper", [False, True])
@pytest.mark.parametrize("after", [False, True])
def test_disposable_session_or_helper_retains_failed_open_until_explicit_close_retry(
    tmp_path, monkeypatch, helper, after
):
    paths = directories_under(tmp_path, monkeypatch)
    admission = Admission()
    admission.fail_close, admission.close_before_failure = True, after
    handles = []

    def interrupted(_session, *, retained):
        assert not retained
        raise InterruptedError("prefix initialization interrupted")

    monkeypatch.setattr(JournalValidationSession, "_initialize", interrupted)
    with pytest.raises(OSError, match="close unconfirmed"):
        if helper:
            validate_journal_chunks(
                UnreadChunks(), connection_factory=admission, on_storage_open=handles.append
            )
        else:
            JournalValidationSession(
                UnreadChunks(),
                attempt_id="a" * 32,
                connection_factory=admission,
                on_storage_open=handles.append,
            )
    assert len(handles) == 1 and admission.active == 1 and paths[0].is_dir()
    handles[0].close()
    assert admission.active == 0 and not paths[0].exists()
    assert admission.close_attempts == 2


@pytest.mark.parametrize("after", [False, True])
def test_disposable_validated_summary_keeps_one_real_connection_until_final_reader(
    tmp_path, journal, catalog, monkeypatch, after
):
    paths = directories_under(tmp_path, monkeypatch)
    admission = Admission()
    monkeypatch.setattr(journal_store.sqlite3, "connect", no_default_connection)
    handles = []
    summary = validate_journal_chunks(
        (journal,), catalog=catalog, connection_factory=admission, on_storage_open=handles.append
    )
    assert len(admission.connections) == len(handles) == 1
    expected = summary.anchor
    last = summary.borrow()
    summary.close()
    assert admission.active == admission.peak == 1 and admission.close_attempts == 0
    assert last.anchor == expected and last.journal_bytes == len(journal)
    admission.fail_close, admission.close_before_failure = True, after
    with pytest.raises(OSError, match="close unconfirmed"):
        last.close()
    assert admission.active == 1 and paths[0].exists()
    last.close()
    assert admission.active == 0 and not paths[0].exists()
    handles[0].close()
    assert admission.close_attempts == 2


def test_disposable_directory_cleanup_retry_never_releases_connection_capacity_twice(
    tmp_path, monkeypatch
):
    paths = directories_under(tmp_path, monkeypatch)
    admission = Admission()
    current = JournalStore(connection_factory=admission)
    actual = journal_store.shutil.rmtree
    attempts = []

    def remove(path, *args, **kwargs):
        attempts.append(path)
        if len(attempts) == 1:
            raise OSError("directory release unconfirmed")
        return actual(path, *args, **kwargs)

    monkeypatch.setattr(journal_store.shutil, "rmtree", remove)
    with pytest.raises(OSError, match="directory release"):
        current.close()
    assert admission.active == 0 and admission.close_attempts == 1 and paths[0].is_dir()
    current.close()
    assert not paths[0].exists() and admission.close_attempts == 1


def test_default_disposable_connect_failure_keeps_its_existing_directory_cleanup(
    tmp_path, monkeypatch
):
    paths = directories_under(tmp_path, monkeypatch)

    def refuse(*_args, **_kwargs):
        raise sqlite3.OperationalError("cannot open")

    monkeypatch.setattr(journal_store.sqlite3, "connect", refuse)
    with pytest.raises(sqlite3.OperationalError, match="cannot open"):
        JournalStore()
    assert len(paths) == 1 and not paths[0].exists()

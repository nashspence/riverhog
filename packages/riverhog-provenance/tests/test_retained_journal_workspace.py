from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from riverhog_provenance import iter_journal_frames
from riverhog_provenance import journal_store as owner
from riverhog_provenance._entry_replay import ReplayBudget
from riverhog_provenance.journal_store import InvalidJournalWorkspace, JournalStore
from test_entry_replay import initialize, make, owned_summary


def directory(tmp_path):
    path = tmp_path / "retained"
    path.mkdir(mode=0o700)
    return path


def test_retained_connections_use_synchronized_rollback_journals_and_keep_files(tmp_path):
    path = directory(tmp_path)
    current = JournalStore.open_retained(path, create=True)
    db = current.db
    runs = dict(current._dag_runs)
    assert db.execute("PRAGMA journal_mode").fetchone() == ("delete",)
    assert db.execute("PRAGMA synchronous").fetchone() == (2,)
    assert db.execute("PRAGMA auto_vacuum").fetchone() == (2,)
    assert db.execute("PRAGMA fullfsync").fetchone() == (1,)
    db.execute("INSERT INTO findings VALUES('retained','complete owned fixture')").close()
    db.commit()
    current.close()
    assert path.is_dir() and (path / "snapshot.sqlite3").is_file()
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute("SELECT 1")
    with closing(JournalStore.open_retained(path)) as reopened:
        assert reopened._dag_runs == runs
        assert reopened.db.execute(
            "SELECT value FROM findings WHERE key='retained'"
        ).fetchone() == ("complete owned fixture",)
        assert reopened.db.execute("PRAGMA synchronous").fetchone() == (2,)


def test_disposable_store_keeps_its_existing_cleanup_contract():
    current = JournalStore()
    path = current._storage.directory
    assert current.db.execute("PRAGMA synchronous").fetchone() == (0,)
    current.close()
    assert not path.exists()


def test_frozen_retained_borrows_release_connections_without_removing_owned_state(tmp_path):
    path = directory(tmp_path)
    current = JournalStore.open_retained(path, create=True)
    db = current.db
    current.freeze()
    borrow = current.borrow()
    current.close()
    assert borrow.db.execute("SELECT 1").fetchone() == (1,)
    borrow.close()
    assert (path / "snapshot.sqlite3").is_file()
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute("SELECT 1")


@pytest.mark.parametrize("fault", ["format", "marker", "file", "mode"])
def test_lost_or_foreign_state_fails_without_repair_or_recreation(tmp_path, fault):
    path = directory(tmp_path)
    with closing(JournalStore.open_retained(path, create=True)) as current:
        if fault == "format":
            current.db.execute("UPDATE journal_workspace SET format='foreign'").close()
        elif fault == "marker":
            current.db.execute("DELETE FROM journal_workspace").close()
        current.db.commit()
    file = path / "snapshot.sqlite3"
    if fault == "file":
        file.unlink()
    elif fault == "mode":
        with closing(sqlite3.connect(file)) as db:
            assert db.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    before = None if not file.exists() else hashlib.sha256(file.read_bytes()).hexdigest()
    with pytest.raises(InvalidJournalWorkspace):
        JournalStore.open_retained(path)
    assert path.is_dir()
    assert (None if not file.exists() else hashlib.sha256(file.read_bytes()).hexdigest()) == before


def test_creation_is_exclusive_and_never_replaces_an_existing_owner(tmp_path):
    path = directory(tmp_path)
    with closing(JournalStore.open_retained(path, create=True)) as current:
        current.db.execute("INSERT INTO findings VALUES('owned','retained')").close()
        current.db.commit()
    before = (path / "snapshot.sqlite3").read_bytes()
    with pytest.raises(FileExistsError):
        JournalStore.open_retained(path, create=True)
    assert (path / "snapshot.sqlite3").read_bytes() == before


def test_reopen_and_resume_never_read_accepted_frame_or_assertion_payloads(
    tmp_path, journal, catalog, monkeypatch
):
    path = directory(tmp_path)
    with closing(JournalStore.open_retained(path, create=True)) as store:
        replay = initialize(store, catalog)
        for frame in iter_journal_frames((journal,)):
            replay.admit(frame)
            while replay.ledger.load().checkpoint.phase != "frame":
                replay.advance()
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        db = real_connect(*args, **kwargs)

        def authorize(action, table, column, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ
                and table in {"frames", "assertions"}
                and column == "value"
                else sqlite3.SQLITE_OK
            )

        db.set_authorizer(authorize)
        return db

    monkeypatch.setattr(owner.sqlite3, "connect", connect)
    with closing(JournalStore.open_retained(path)) as store:
        replay = make(store, catalog)
        state = replay.prefix.finish()
        assert store.frame_count == state.frame_count == 1
        assert state.hexdigest == hashlib.sha256(journal).hexdigest()


@pytest.mark.parametrize("phase", ["entry", "staging", "generic", "row", "lifecycle", "publish"])
def test_actual_worker_exit_retains_the_exact_next_parent_unit(tmp_path, journal, catalog, phase):
    path = directory(tmp_path)
    with closing(JournalStore.open_retained(path, create=True)) as store:
        replay = initialize(store, catalog)
        replay.admit(next(iter_journal_frames((journal,))))
        for _ in range(100000):
            before = replay.ledger.load().checkpoint
            if before.phase == phase:
                break
            replay.advance(ReplayBudget(rows=1))
        else:
            raise AssertionError("the small fixture did not reach its selected phase")
    code = """import os,sys
sys.path.insert(0,sys.argv[1])
from pathlib import Path
from riverhog_provenance.journal_store import JournalStore
from riverhog_provenance_contracts import ContractCatalog
from riverhog_provenance._entry_replay import ReplayBudget
from test_entry_replay import make
store=JournalStore.open_retained(Path(sys.argv[2]))
current=make(store,ContractCatalog())
current.advance(ReplayBudget(rows=1))
os._exit(81)
"""
    process = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).parent), str(path)], capture_output=True
    )
    assert process.returncode == 81, process.stderr.decode()
    with closing(JournalStore.open_retained(path)) as store:
        replay = make(store, catalog)
        after = replay.ledger.load().checkpoint
        assert after.version == before.version + 1
        assert after.frame_ordinal == before.frame_ordinal + (1 if phase == "publish" else 0)
        while replay.ledger.load().checkpoint.phase != "frame":
            replay.advance(ReplayBudget(rows=1))
        summary = owned_summary(store, replay.prefix.finish())
        with summary:
            assert summary.journal_sha256 == hashlib.sha256(journal).hexdigest()
    assert (path / "snapshot.sqlite3").is_file()


@pytest.mark.skipif(os.name != "posix", reason="POSIX fixture symlinks")
def test_retained_workspace_rejects_database_and_directory_symlinks(tmp_path):
    path = directory(tmp_path)
    file = tmp_path / "foreign.sqlite3"
    file.write_bytes(b"untrusted fixture")
    (path / "snapshot.sqlite3").symlink_to(file)
    with pytest.raises(InvalidJournalWorkspace):
        JournalStore.open_retained(path)
    link = tmp_path / "directory-link"
    link.symlink_to(path, target_is_directory=True)
    with pytest.raises(InvalidJournalWorkspace):
        JournalStore.open_retained(link)
    assert file.read_bytes() == b"untrusted fixture"


def test_owned_release_waits_for_the_final_reader_and_closed_database(tmp_path):
    released = []

    def release():
        with pytest.raises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")
        released.append("closed")

    current = JournalStore.open_retained(directory(tmp_path), create=True, on_storage_close=release)
    db = current.db
    current.freeze()
    first = current.borrow()
    last = first.borrow()
    current.close()
    first.close()
    first.close()
    assert not released and last.db.execute("SELECT 1").fetchone() == (1,)
    last.close()
    last.close()
    assert released == ["closed"]
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute("SELECT 1")


def test_failed_retained_store_construction_keeps_owner_resource_with_caller(tmp_path):
    path = directory(tmp_path)
    with closing(JournalStore.open_retained(path, create=True)) as current:
        current.db.execute("DELETE FROM journal_workspace").close()
        current.db.commit()
    released = []
    with pytest.raises(InvalidJournalWorkspace):
        JournalStore.open_retained(path, on_storage_close=lambda: released.append("closed"))
    assert not released

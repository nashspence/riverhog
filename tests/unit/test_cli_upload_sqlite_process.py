from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import closing

import pytest
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy, UploadSqliteResources
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from riverhog_provenance.journal_store import JournalStore


class _Process:
    def __init__(self):
        self.checks = 0
        self.accepted = True

    def __bool__(self):
        return False

    def verify(self):
        self.checks += 1
        if not self.accepted:
            raise RuntimeError("selected process is no longer admitted")


@pytest.mark.parametrize("temporary_storage", ["FILE", "MEMORY"])
def test_upload_consumers_use_the_selected_process_and_temporary_storage(temporary_storage):
    process = _Process()
    with UploadSqliteResources(
        UploadSqlitePolicy(connections=2, cache_kib=13),
        process=process,
        temporary_storage=temporary_storage,
    ) as resources:
        before = process.checks
        tags = PreparedInitialCollectionTags(
            ["z", "a"], connection_factory=resources.connect, on_storage_open=resources.retain
        )
        journal = JournalStore(
            connection_factory=resources.connect, on_storage_open=resources.retain
        )
        assert process.checks > before
        assert resources.status.connections == 2
        assert list(tags.iter_batches()) == [("a", "z")]
        for statement, expected in (
            ("PRAGMA temp_store", 1 if temporary_storage == "FILE" else 2),
            ("PRAGMA main.cache_size", -13),
            ("PRAGMA mmap_size", 0),
        ):
            with closing(journal.db.execute(statement)) as cursor:
                assert cursor.fetchone() == (expected,)
        directories = resources.pending_directories
    assert resources.status.connections == 0
    assert all(not directory.exists() for directory in directories)
    # The application owner did not replace or retire the process policy.
    with UploadSqliteResources(UploadSqlitePolicy(), process=process) as replacement:
        assert replacement.status.connections == 0


def test_memory_temporary_storage_requires_process_admission_before_resources():
    with pytest.raises(ValueError, match="requires the admitted SQLite process"):
        UploadSqliteResources(UploadSqlitePolicy(), temporary_storage="MEMORY")
    with UploadSqliteResources(UploadSqlitePolicy()) as resources:
        assert resources.pending_directories == ()


def test_failed_process_startup_does_not_acquire_upload_custody():
    process = _Process()
    process.accepted = False
    with pytest.raises(RuntimeError, match="no longer admitted"):
        UploadSqliteResources(UploadSqlitePolicy(), process=process)
    assert process.checks == 1
    with UploadSqliteResources(UploadSqlitePolicy()) as resources:
        assert resources.pending_directories == ()


def test_process_loss_refuses_another_consumer_and_still_permits_exact_cleanup():
    process = _Process()
    resources = UploadSqliteResources(UploadSqlitePolicy(), process=process)
    journal = JournalStore(connection_factory=resources.connect, on_storage_open=resources.retain)
    (directory,) = resources.pending_directories
    try:
        with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
            original = cursor.fetchone()
        process.accepted = False
        with pytest.raises(RuntimeError, match="no longer admitted"):
            resources.connect(directory / "unopened.sqlite3")
        assert resources.pending_directories == (directory,)
        assert resources.status.connections == 1
        assert not (directory / "unopened.sqlite3").exists()
        with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == original
    finally:
        resources.close()
    assert journal.cleanup_complete and not directory.exists()
    assert resources.status.connections == 0


def test_projection_cleanup_removes_only_its_dedicated_scratch(tmp_path, monkeypatch):
    import tempfile

    sentinel = tmp_path / "independent-state"
    sentinel.write_bytes(b"independently retained")
    before = set(tmp_path.iterdir())
    scratch = tmp_path / "projection-scratch"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    with UploadSqliteResources(
        UploadSqlitePolicy(), process=_Process(), temporary_storage="MEMORY"
    ) as resources:
        tags = PreparedInitialCollectionTags(
            ["z", "a"], connection_factory=resources.connect, on_storage_open=resources.retain
        )
        journal = JournalStore(
            connection_factory=resources.connect, on_storage_open=resources.retain
        )
        assert list(tags.iter_batches()) == [("a", "z")]
        assert len(resources.pending_directories) == 2
        assert all(path.parent == scratch for path in resources.pending_directories)
        with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == (0,)
    assert not list(scratch.iterdir())
    assert sentinel.read_bytes() == b"independently retained"
    assert set(tmp_path.iterdir()) == before | {scratch}


@pytest.mark.skipif(os.name != "nt", reason="qualified loaded Windows SQLite engine")
def test_actual_upload_consumers_share_one_heap_and_preserve_original_on_nomem(tmp_path):
    scratch = tmp_path / "projection-scratch"
    scratch.mkdir()
    code = r"""
import json, sqlite3, sys, tempfile
from pathlib import Path
from contextlib import closing
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy, UploadSqliteResources
from riverhog_client import WindowsSqliteProcess
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from riverhog_provenance.journal_store import JournalStore

tempfile.tempdir=sys.argv[1]
limit=8*1024**2
process=WindowsSqliteProcess(heap_bytes=limit,native_limits=(2,8))
resources=UploadSqliteResources(UploadSqlitePolicy(connections=2,cache_kib=13),
    process=process,temporary_storage="MEMORY")
try:
    tags=PreparedInitialCollectionTags(["z","a"],
        connection_factory=resources.connect,on_storage_open=resources.retain)
    journal=JournalStore(connection_factory=resources.connect,on_storage_open=resources.retain)
    assert resources.status.connections==2
    assert list(tags.iter_batches())==[("a","z")]
    with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
        original=cursor.fetchone()
    for sql,value in (
        ("PRAGMA temp_store",2),("PRAGMA main.cache_size",-13),("PRAGMA mmap_size",0)
    ):
        with closing(journal.db.execute(sql)) as cursor:
            assert cursor.fetchone()==(value,)
    with closing(journal.db.cursor()) as cursor:
        try:
            cursor.execute("SELECT randomblob(?)",(limit,)).fetchone()
        except (MemoryError,sqlite3.Error) as error:
            if isinstance(error,sqlite3.Error):
                assert error.sqlite_errorcode&255==sqlite3.SQLITE_NOMEM
        else:
            raise AssertionError("actual application consumers escaped their aggregate heap")
    assert process.report()["sqlite_heap_peak_bytes"]<=limit
    with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
        assert cursor.fetchone()==original
    assert list(tags.iter_batches())==[("a","z")]
    assert resources.status.connections==2
    try:
        WindowsSqliteProcess(heap_bytes=limit*2,native_limits=(2,8))
    except RuntimeError:
        pass
    else:
        raise AssertionError("a second upload process replaced the active heap owner")
    process.increase_heap(limit*2)
    with closing(journal.db.execute("SELECT length(randomblob(?))",(limit,))) as cursor:
        assert cursor.fetchone()==(limit,)
    assert process.report()["sqlite_heap_peak_bytes"]<=limit*2
    directories=resources.pending_directories
finally:
    resources.close()
assert resources.status.connections==resources.status.cursors==resources.status.operations==0
assert all(not directory.exists() for directory in directories)
assert process.report()["heap_limit_bytes"]==limit*2
assert process.native_report()["owners"]==process.native_report()["files"]==0
with UploadSqliteResources(
    UploadSqlitePolicy(),process=process,temporary_storage="MEMORY"
) as next_owner:
    with closing(JournalStore(
        connection_factory=next_owner.connect,on_storage_open=next_owner.retain
    )) as next_journal:
        with closing(next_journal.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone()==(0,)
remaining=tuple(sorted(path.name for path in Path(sys.argv[1]).iterdir()))
assert remaining==(), ("projection scratch remains",remaining)
print(json.dumps({"passed":True,"heap_limit_bytes":limit*2,"connections":resources.status.connection_peak}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(scratch)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "passed": True,
        "heap_limit_bytes": 16 * 1024**2,
        "connections": 2,
    }


@pytest.mark.parametrize(
    ("admitted", "temporary_storage", "projection_storage", "message"),
    [
        (False, "MEMORY", "MEMORY", "requires the admitted SQLite process"),
        (True, "FILE", "MEMORY", "MEMORY temporary storage"),
        (False, "FILE", "MEMORY", "admitted SQLite process"),
        (True, "MEMORY", "invalid", "projection storage must be FILE or MEMORY"),
    ],
)
def test_memory_projection_selection_refuses_unadmitted_storage_before_custody(
    admitted, temporary_storage, projection_storage, message
):
    with pytest.raises(ValueError, match=message):
        UploadSqliteResources(
            UploadSqlitePolicy(),
            process=_Process() if admitted else None,
            temporary_storage=temporary_storage,
            projection_storage=projection_storage,
        )
    with UploadSqliteResources(UploadSqlitePolicy()) as resources:
        assert resources.pending_directories == ()


def test_actual_disposable_projections_keep_database_and_journal_in_admitted_memory():
    with UploadSqliteResources(
        UploadSqlitePolicy(connections=2),
        process=_Process(),
        temporary_storage="MEMORY",
        projection_storage="MEMORY",
    ) as resources:
        tags = PreparedInitialCollectionTags(
            ["z", "a"], connection_factory=resources.connect, on_storage_open=resources.retain
        )
        journal = JournalStore(
            connection_factory=resources.connect, on_storage_open=resources.retain
        )
        assert list(tags.iter_batches()) == [("a", "z")]
        for statement, expected in (
            ("PRAGMA database_list", (0, "main", "")),
            ("PRAGMA journal_mode", ("memory",)),
            ("PRAGMA temp_store", (2,)),
        ):
            with closing(journal.db.execute(statement)) as cursor:
                assert cursor.fetchone() == expected
        directories = resources.pending_directories
        assert len(directories) == 2 and all(not list(path.iterdir()) for path in directories)
        assert resources.status.connections == 2
    assert resources.status.connections == 0
    assert all(not path.exists() for path in directories)


def test_memory_projection_refuses_existing_database_and_uri_without_hiding_original(tmp_path):
    original = tmp_path / "original.sqlite3"
    original.write_bytes(b"independently retained original")
    unopened = tmp_path / "unopened.sqlite3"
    with UploadSqliteResources(
        UploadSqlitePolicy(),
        process=_Process(),
        temporary_storage="MEMORY",
        projection_storage="MEMORY",
    ) as resources:
        with pytest.raises(FileExistsError, match="requires an absent database"):
            resources.connect(original)
        with pytest.raises(ValueError, match="cannot open a retained URI"):
            resources.connect(unopened, uri=True)
        assert resources.pending_directories == ()
        assert resources.status.connections == 0
    assert original.read_bytes() == b"independently retained original" and not unopened.exists()


def test_memory_projection_reader_keeps_actual_connection_and_directory_custody():
    from sqlite_admission import SqliteCleanupPending

    resources = UploadSqliteResources(
        UploadSqlitePolicy(connections=1),
        process=_Process(),
        temporary_storage="MEMORY",
        projection_storage="MEMORY",
    )
    journal = JournalStore(connection_factory=resources.connect, on_storage_open=resources.retain)
    journal.freeze()
    reader = journal.borrow()
    (directory,) = resources.pending_directories
    try:
        journal.close()
        assert resources.status.connections == 1 and directory.is_dir()
        with pytest.raises(SqliteCleanupPending, match="cleanup remains unconfirmed"):
            resources.close()
        with closing(reader.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == (0,)
        assert not list(directory.iterdir())
    finally:
        reader.close()
        resources.close()
    assert resources.status.connections == 0 and not directory.exists()


@pytest.mark.skipif(os.name != "nt", reason="qualified loaded Windows SQLite engine")
def test_native_memory_projection_write_refusal_keeps_originals_and_the_same_heap(tmp_path):
    scratch = tmp_path / "memory-projection-scratch"
    scratch.mkdir()
    code = r"""
import json,sqlite3,sys,tempfile
from pathlib import Path
from contextlib import closing
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy,UploadSqliteResources
from riverhog_client import WindowsSqliteProcess
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from riverhog_provenance.journal_store import JournalStore
tempfile.tempdir=sys.argv[1]
limit=8*1024**2
process=WindowsSqliteProcess(heap_bytes=limit,native_limits=(2,8))
resources=UploadSqliteResources(UploadSqlitePolicy(connections=2),process=process,
    temporary_storage="MEMORY",projection_storage="MEMORY")
try:
    tags=PreparedInitialCollectionTags(["z","a"],
        connection_factory=resources.connect,on_storage_open=resources.retain)
    journal=JournalStore(connection_factory=resources.connect,on_storage_open=resources.retain)
    assert resources.status.connections==2
    for statement,expected in (
        ("PRAGMA database_list",(0,"main","")),
        ("PRAGMA journal_mode",("memory",)),
        ("PRAGMA temp_store",(2,)),
    ):
        with closing(journal.db.execute(statement)) as cursor:assert cursor.fetchone()==expected
    journal.db.execute("CREATE TABLE allocation_probe(value BLOB)").close()
    journal.db.execute("INSERT INTO allocation_probe VALUES(?)",(b"original",)).close()
    journal.db.commit()
    try:
        with closing(journal.db.execute(
            "INSERT INTO allocation_probe VALUES(zeroblob(?))",(limit,)
        )):pass
        journal.db.commit()
    except (MemoryError,sqlite3.Error) as error:
        if isinstance(error,sqlite3.Error):assert error.sqlite_errorcode&255==sqlite3.SQLITE_NOMEM
        journal.db.rollback()
    else:raise AssertionError("memory projection writes escaped the admitted aggregate heap")
    with closing(journal.db.execute("SELECT value FROM allocation_probe")) as cursor:
        assert cursor.fetchall()==[(b"original",)]
    with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
        assert cursor.fetchone()==(0,)
    assert list(tags.iter_batches())==[("a","z")]
    assert process.report()["heap_limit_bytes"]==limit
    assert process.report()["sqlite_heap_peak_bytes"]<=limit
    directories=resources.pending_directories
    assert len(directories)==2 and all(not list(path.iterdir()) for path in directories)
finally:resources.close()
assert all(not path.exists() for path in directories)
assert resources.status.connections==resources.status.cursors==resources.status.operations==0
assert process.report()["heap_limit_bytes"]==limit
assert process.native_report()["owners"]==process.native_report()["files"]==0
assert not list(Path(sys.argv[1]).iterdir())
print(json.dumps({"passed":True,"same_heap":True,"committed_original_preserved":True}))
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(scratch)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["passed"]

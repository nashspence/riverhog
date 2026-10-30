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

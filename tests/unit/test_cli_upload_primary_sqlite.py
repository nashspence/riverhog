from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy, UploadSqliteResources
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from riverhog_provenance.journal_store import JournalStore
from sqlite_admission import SqliteCapacity, SqliteCleanupPending

from tests.unit.test_cli_upload_sqlite_process import _Process


def test_primary_open_requires_process_before_driver_or_persistent_mutation(tmp_path, monkeypatch):
    import sqlite_admission

    def forbidden(*args, **kwargs):
        pytest.fail("unadmitted primary reached the driver")

    path = tmp_path / "original.sqlite3"
    with UploadSqliteResources(UploadSqlitePolicy()) as resources:
        monkeypatch.setattr(sqlite_admission, "_connect", forbidden)
        with pytest.raises(ValueError, match="requires the admitted process"):
            resources.connect_primary(path)
        assert resources.status.connections == 0
        assert resources.pending_directories == ()
    assert not path.exists()


def test_primary_and_projections_share_handle_and_blob_grants_without_directory_custody(tmp_path):
    path = tmp_path / "retained.sqlite3"
    resources = UploadSqliteResources(
        UploadSqlitePolicy(connections=3, blobs=1), process=_Process()
    )
    primary = resources.connect_primary(path)
    try:
        primary.execute("CREATE TABLE original(body BLOB)").close()
        primary.execute("INSERT INTO original VALUES (zeroblob(4))").close()
        primary.commit()
        with closing(primary.blobopen("original", "body", 1)) as blob:
            blob.write(b"same")
            blob.seek(1)
            with pytest.raises(SqliteCapacity, match="blob grant"):
                primary.blobopen("original", "body", 1)
            assert resources.status.blobs == resources.status.blob_peak == 1
            assert blob.read() == b"ame"
        tags = PreparedInitialCollectionTags(
            ["z", "a"], connection_factory=resources.connect, on_storage_open=resources.retain
        )
        journal = JournalStore(
            connection_factory=resources.connect, on_storage_open=resources.retain
        )
        assert list(tags.iter_batches()) == [("a", "z")]
        assert resources.status.connections == resources.status.connection_peak == 3
        directories = resources.pending_directories
        assert len(directories) == 2 and path.parent not in directories
        with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == (0,)
    finally:
        primary.close()
        resources.close()
    assert resources.status.connections == resources.status.cursors == resources.status.blobs == 0
    assert all(not directory.exists() for directory in directories)
    assert path.is_file()
    with closing(sqlite3.connect(path)) as recovered:
        with closing(recovered.execute("SELECT body FROM original")) as cursor:
            assert cursor.fetchone() == (b"same",)


def test_application_close_waits_for_primary_handle_and_preserves_persistent_files(tmp_path):
    path = tmp_path / "retained.sqlite3"
    resources = UploadSqliteResources(UploadSqlitePolicy(), process=_Process())
    primary = resources.connect_primary(path)
    try:
        primary.execute("CREATE TABLE original(value)").close()
        primary.execute("INSERT INTO original VALUES ('exact original')").close()
        primary.commit()
        journal = JournalStore(
            connection_factory=resources.connect, on_storage_open=resources.retain
        )
        (directory,) = resources.pending_directories
        with pytest.raises(SqliteCleanupPending, match="cleanup remains unconfirmed"):
            resources.close()
        assert journal.cleanup_complete and not directory.exists()
        assert resources.pending_directories == () and resources.status.connections == 1
        assert path.is_file()
        with pytest.raises(SqliteCleanupPending, match="previous upload"):
            UploadSqliteResources(UploadSqlitePolicy(), process=_Process())
        with closing(primary.execute("SELECT value FROM original")) as cursor:
            assert cursor.fetchone() == ("exact original",)
    finally:
        primary.close()
        resources.close()
    assert path.is_file()
    with UploadSqliteResources(UploadSqlitePolicy(), process=_Process()) as next_owner:
        assert next_owner.status.connections == 0


def test_primary_capacity_refusal_preserves_the_existing_projection_until_explicit_close():
    resources = UploadSqliteResources(UploadSqlitePolicy(connections=1), process=_Process())
    journal = JournalStore(connection_factory=resources.connect, on_storage_open=resources.retain)
    try:
        with pytest.raises(SqliteCapacity, match="connection grant"):
            resources.connect_primary(":memory:")
        assert resources.status.connections == 1
        with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == (0,)
    finally:
        resources.close()
    assert journal.cleanup_complete


def _actual_primary_replay_with_projections(tmp_path, projection_storage):
    from riverhog_client import SqlitePrimaryLimits
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles

    from tests.support.prepared_primary import SqlitePrimaryTestGuard
    from tests.support.prepared_primary_replay import witness

    path = tmp_path / "retained primary.sqlite3"
    resources = UploadSqliteResources(
        UploadSqlitePolicy(connections=3, blobs=1),
        process=_Process(),
        temporary_storage="MEMORY",
        projection_storage=projection_storage,
    )
    try:
        tags = PreparedInitialCollectionTags(
            ["z", "a"], connection_factory=resources.connect, on_storage_open=resources.retain
        )
        journal = JournalStore(
            connection_factory=resources.connect, on_storage_open=resources.retain
        )
        calls = []

        def factory(location, *, allocation_descriptor, initialize):
            if initialize:
                location.touch(exist_ok=False)
            calls.append(initialize)
            return TransactionalPrimaryFiles(
                SqlitePrimaryTestGuard(location),
                allocation_descriptor=allocation_descriptor,
                limits=SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024),
                initialize=initialize,
                connection_factory=resources.connect_primary,
            )

        with closing(journal.db.execute("SELECT count(*) FROM identities")) as suspended:
            witness(path, factory)
            assert suspended.fetchone() == (0,)
        assert calls == [True, False, False, False]
        assert list(tags.iter_batches()) == [("a", "z")]
        assert resources.status.connection_peak == 3 and resources.status.connections == 2
        assert resources.status.blob_peak == 1
        directories = resources.pending_directories
        assert len(directories) == 2 and path.parent not in directories
        if projection_storage == "MEMORY":
            assert all(not list(directory.iterdir()) for directory in directories)
            with closing(journal.db.execute("PRAGMA database_list")) as cursor:
                assert cursor.fetchone() == (0, "main", "")
    finally:
        resources.close()
    assert path.is_file() and all(not directory.exists() for directory in directories)


@pytest.mark.skipif(os.name != "posix", reason="actual primary schema/pager uses Unix VFS fixture")
def test_actual_primary_cold_replay_shares_application_admission_with_live_projections(tmp_path):
    _actual_primary_replay_with_projections(tmp_path, "FILE")


@pytest.mark.skipif(os.name != "posix", reason="actual primary schema/pager uses Unix VFS fixture")
def test_actual_primary_cold_replay_preserves_original_with_memory_projections(tmp_path):
    _actual_primary_replay_with_projections(tmp_path, "MEMORY")


def _actual_windows_primary_replay_with_projections(tmp_path, projection_storage):
    code = r"""
import importlib.util,json,sys,tempfile
from pathlib import Path
from contextlib import closing
root=Path(sys.argv[1]);sys.path.insert(0,str(root))
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy,UploadSqliteResources
from riverhog_client import (
    WindowsSqliteProcess,SqlitePrimaryLimits,SqliteOperationBank,SqliteOperationGrants)
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from riverhog_provenance.journal_store import JournalStore
from tests.support.prepared_primary_replay import DESCRIPTOR,witness
spec=importlib.util.spec_from_file_location("upload_primary_parent",
    root/"tests/platform/windows_primary_process.py")
parents=importlib.util.module_from_spec(spec);sys.modules[spec.name]=parents
spec.loader.exec_module(parents)
scratch=Path(sys.argv[2])/"scratch";scratch.mkdir()
tempfile.tempdir=str(scratch)
parent=Path(sys.argv[2])/"private-primary";parents._parent(parent)
process=WindowsSqliteProcess(heap_bytes=64*1024**2,native_limits=(1,2))
resources=UploadSqliteResources(UploadSqlitePolicy(connections=3,blobs=1),
    process=process,temporary_storage="MEMORY",projection_storage=sys.argv[3])
path=process.primary_location(parent,DESCRIPTOR)
limits=SqlitePrimaryLimits(16*1024**2,32*1024**2,128*1024)
bank=SqliteOperationBank(512,8*1024**2,65536,8*1024**2,65536,
    262144,0,1<<40,1<<60,1<<40)
selected=process.primary_factory(limits=limits,operations=SqliteOperationGrants(bank,bank),
    retry_profile=(2,3),connection_factory=resources.connect_primary)
calls=[];state={}
def factory(location,*,allocation_descriptor,initialize):
    calls.append(initialize)
    files=selected(location,allocation_descriptor=allocation_descriptor,initialize=initialize)
    if initialize:
        # Establish the primary's admitted global retry policy before other file consumers.
        tags=PreparedInitialCollectionTags(["z","a"],
            connection_factory=resources.connect,on_storage_open=resources.retain)
        journal=JournalStore(connection_factory=resources.connect,on_storage_open=resources.retain)
        state.update(tags=tags,journal=journal,
            cursor=journal.db.execute("SELECT count(*) FROM identities"))
    return files
try:
    witness(path,factory)
    with closing(state["cursor"]) as suspended:
        assert suspended.fetchone()==(0,)
    assert calls==[True,False,False,False]
    assert list(state["tags"].iter_batches())==[("a","z")]
    assert resources.status.connection_peak==3 and resources.status.connections==2
    assert resources.status.blob_peak==1 and resources.status.blobs==0
    assert len(resources.pending_directories)==2
    if sys.argv[3]=="MEMORY":
        assert all(not list(directory.iterdir()) for directory in resources.pending_directories)
        with closing(state["journal"].db.execute("PRAGMA database_list")) as cursor:
            assert cursor.fetchone()==(0,"main","")
    assert process.native_report()["owners"]==process.native_report()["files"]==0
    report=process.report()
    assert 0<report["sqlite_heap_peak_bytes"]<=report["heap_limit_bytes"]==64*1024**2
finally:
    resources.close()
assert path.is_file() and not list(scratch.iterdir())
assert resources.status.connections==resources.status.cursors==resources.status.blobs==0
assert process.report()["heap_limit_bytes"]==64*1024**2
print(json.dumps({"passed":True,"initializations":1,"cold_opens":3,
    "connections":3,"blob_peak":1,"persistent_primary_preserved":True}))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            code,
            str(Path(__file__).resolve().parents[2]),
            str(tmp_path),
            projection_storage,
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "passed": True,
        "initializations": 1,
        "cold_opens": 3,
        "connections": 3,
        "blob_peak": 1,
        "persistent_primary_preserved": True,
    }


@pytest.mark.skipif(os.name != "nt", reason="qualified Windows engine and private local NTFS")
def test_actual_windows_primary_replay_and_projections_share_the_application_process(tmp_path):
    _actual_windows_primary_replay_with_projections(tmp_path, "FILE")


@pytest.mark.skipif(os.name != "nt", reason="qualified Windows engine and private local NTFS")
def test_actual_windows_primary_replay_preserves_original_with_memory_projections(tmp_path):
    _actual_windows_primary_replay_with_projections(tmp_path, "MEMORY")

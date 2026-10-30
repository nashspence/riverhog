from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from contextlib import closing

import pytest
from a_riverhog_cli import main
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy, UploadSqliteResources
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from riverhog_provenance.journal_store import JournalStore
from sqlite_admission import SqliteCleanupPending
from typer.testing import CliRunner


def test_actual_tags_and_provenance_share_one_application_admission():
    resources = UploadSqliteResources(UploadSqlitePolicy(connections=2, cursors=8, operations=2))
    try:
        tags = PreparedInitialCollectionTags(
            ["z", "a"], connection_factory=resources.connect, on_storage_open=resources.retain
        )
        journal = JournalStore(
            connection_factory=resources.connect, on_storage_open=resources.retain
        )
        assert resources.status.connections == resources.status.connection_peak == 2
        assert list(tags.iter_batches()) == [("a", "z")]
        with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == (0,)
        assert resources.status.cursor_peak > 0
        assert 0 < resources.status.operation_peak <= 2
        directories = resources.pending_directories
        assert len(directories) == 2 and all(path.is_dir() for path in directories)
        tags.close()
        assert resources.status.connections == 1
        journal.close()
        assert resources.status.connections == 0
        resources.close()
        assert resources.pending_directories == ()
        assert all(not path.exists() for path in directories)
    finally:
        resources.close()


def test_borrowed_journal_keeps_connection_and_directory_after_original_close():
    resources = UploadSqliteResources(UploadSqlitePolicy(connections=1))
    journal = JournalStore(connection_factory=resources.connect, on_storage_open=resources.retain)
    journal.freeze()
    reader = journal.borrow()
    (directory,) = resources.pending_directories
    try:
        journal.close()
        assert journal.cleanup_complete
        assert resources.status.connections == 1 and directory.exists()
        with pytest.raises(SqliteCleanupPending, match="cleanup remains unconfirmed"):
            resources.close()
        with pytest.raises(SqliteCleanupPending, match="previous upload"):
            UploadSqliteResources(UploadSqlitePolicy())
        with closing(reader.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == (0,)
        assert resources.status.connections == 1
    finally:
        reader.close()
        resources.close()
    assert not directory.exists()
    with UploadSqliteResources(UploadSqlitePolicy()) as replacement:
        assert replacement.status.connections == 0


def test_result_reader_retention_survives_unwind_without_implicit_close_retry():
    resources = UploadSqliteResources(UploadSqlitePolicy(connections=1))
    journal = JournalStore(connection_factory=resources.connect, on_storage_open=resources.retain)
    journal.freeze()
    reader = journal.borrow()
    resources.retain(reader)
    journal.close()
    (directory,) = resources.pending_directories
    try:
        with pytest.raises(ValueError, match="original execution failed"):
            with resources:
                raise ValueError("original execution failed")
        assert not reader.cleanup_complete and resources.status.connections == 1
        assert directory.exists()
        with closing(reader.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == (0,)
    finally:
        resources.close()
    assert reader.cleanup_complete and not directory.exists()
    assert resources.status.connections == 0


def test_unconfirmed_directory_observation_keeps_custody_and_original_error(monkeypatch):
    resources = UploadSqliteResources(UploadSqlitePolicy())
    journal = JournalStore(connection_factory=resources.connect, on_storage_open=resources.retain)
    (directory,) = resources.pending_directories
    journal.close()
    actual = type(directory).lstat

    def inaccessible(path, *args, **kwargs):
        if path == directory:
            raise PermissionError("directory absence not acknowledged")
        return actual(path, *args, **kwargs)

    try:
        with monkeypatch.context() as blocked:
            blocked.setattr(type(directory), "lstat", inaccessible)
            with pytest.raises(ValueError, match="original execution failed"):
                with resources:
                    raise ValueError("original execution failed")
            assert resources.status.connections == 0
            assert resources.pending_directories == (directory,)
            with pytest.raises(SqliteCleanupPending, match="cleanup remains unconfirmed"):
                resources.close()
    finally:
        resources.close()
    assert resources.pending_directories == ()


def test_completed_projection_owners_do_not_accumulate_over_members():
    with UploadSqliteResources(UploadSqlitePolicy(connections=1)) as resources:
        directories = []
        for _ in range(24):
            with closing(
                JournalStore(connection_factory=resources.connect, on_storage_open=resources.retain)
            ) as journal:
                directories.extend(resources.pending_directories)
                assert len(resources.pending_directories) == 1
                assert resources.status.connections == 1
                with closing(journal.db.execute("SELECT count(*) FROM identities")) as cursor:
                    assert cursor.fetchone() == (0,)
            assert resources.status.connections == 0
        assert resources.status.connection_peak == 1
        assert all(not directory.exists() for directory in directories)


@pytest.mark.parametrize(
    "payload",
    [
        '{"connections":0}',
        '{"cursors":true}',
        '{"operations":1.5}',
        '{"heap_bytes":1024}',
    ],
)
def test_invalid_cli_sqlite_grants_fail_before_state_or_remote_work(tmp_path, monkeypatch, payload):
    root = tmp_path / "source"
    root.mkdir()
    (root / "member").write_bytes(b"original")
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    monkeypatch.setenv(
        "A_RIVERHOG_CLI_PRIMARY_STATE",
        json.dumps({"scope": {"service": "fixture-service", "account": "fixture-account"}}),
    )

    def forbidden(*args, **kwargs):
        pytest.fail("invalid application admission reached state or remote work")

    monkeypatch.setattr(main, "prepare_primary_upload", forbidden)
    monkeypatch.setattr(main, "client", forbidden)
    result = CliRunner().invoke(
        main.app, ["collection", "upload", "start", str(root), "--sqlite-resources", payload]
    )
    assert result.exit_code == 2
    assert "--sqlite-resources" in result.output
    assert not state.exists()


@pytest.mark.parametrize(
    "fault", ["capacity", "unreturned", "close-before", "close-after", "close-caught"]
)
def test_failed_application_sqlite_ownership_survives_unwind_and_blocks_replacement(
    tmp_path, fault
):
    script = r"""
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy, UploadSqliteResources
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from riverhog_provenance.journal_store import JournalStore
import sqlite_admission as admission
from sqlite_admission import SqliteCapacity, SqliteCleanupPending

fault = sys.argv[1]
tempfile.tempdir = sys.argv[2]
resources = UploadSqliteResources(UploadSqlitePolicy(connections=1))
actual = admission._connect
closes = []
armed = True
def driver(database, **options):
    global armed
    base = options.pop("factory")
    class Probe(base):
        def close(self):
            global armed
            closes.append("close")
            if armed and fault in ("close-before", "close-caught"):
                armed = False
                raise RuntimeError("actual driver close not acknowledged")
            super().close()
            if armed and fault == "close-after":
                armed = False
                raise RuntimeError("actual driver close acknowledged then failed")
    connection = actual(database, factory=Probe, **options)
    if fault == "unreturned":
        raise RuntimeError("actual driver constructed without returning")
    return connection
admission._connect = driver
journal = None
try:
    with resources:
        journal = JournalStore(
            connection_factory=resources.connect, on_storage_open=resources.retain
        )
        if fault == "capacity":
            PreparedInitialCollectionTags(
                ["tag"], connection_factory=resources.connect, on_storage_open=resources.retain
            )
        if fault == "close-caught":
            try:
                resources.close()
            except RuntimeError as error:
                assert str(error) == "actual driver close not acknowledged"
            else:
                raise AssertionError("close failure disappeared")
        else:
            journal.close()
except (RuntimeError, SqliteCapacity) as original:
    if fault == "capacity":
        assert isinstance(original, SqliteCapacity)
    elif fault == "unreturned":
        assert str(original) == "actual driver constructed without returning"
    elif fault == "close-caught":
        assert isinstance(original, SqliteCleanupPending)
    else:
        assert "driver close" in str(original)
else:
    raise AssertionError("expected failure did not escape")
assert resources.pending_directories and all(p.exists() for p in resources.pending_directories)
assert len(closes) == (1 if fault.startswith("close-") else 0)
assert resources.status.connections == (0 if fault == "close-after" else 1)
if fault == "unreturned":
    assert resources.status.unreturned_connections == 1
try:
    UploadSqliteResources(UploadSqlitePolicy(connections=2))
except SqliteCleanupPending:
    pass
else:
    raise AssertionError("replacement discarded prior ownership")
if fault.startswith("close-"):
    resources.close()  # explicit retry, preserving the original owner
    assert len(closes) == 2
    assert resources.pending_directories == () and resources.status.connections == 0
    with UploadSqliteResources(UploadSqlitePolicy()) as replacement:
        assert replacement.status.connections == 0
"""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script), fault, str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
        env=os.environ.copy(),
    )
    assert result.returncode == 0, result.stderr

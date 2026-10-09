from __future__ import annotations

import errno
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import riverhog_core.scratch_workspace as scratch
from riverhog_core.canonical_discovery_relevance import MemberRelevance
from riverhog_core.provenance_custody_set import ProvenanceCustodySet
from riverhog_protocol.errors import ServiceUnavailable

_CHILD = """
import sys
from pathlib import Path
from riverhog_core.scratch_workspace import process_workspace, scratch_directory
with process_workspace(Path(sys.argv[1])) as owner:
    with scratch_directory(prefix="journal-") as operation:
        Path(operation, "metadata").write_bytes(b"live operation")
        print(operation, flush=True)
        sys.stdin.readline()
"""


@pytest.mark.skipif(os.name != "posix", reason="native server uses POSIX kernel leases")
def test_hard_kill_reclaims_abandoned_scratch_and_preserves_live_operations(tmp_path: Path) -> None:
    children = [
        subprocess.Popen(
            [sys.executable, "-c", _CHILD, str(tmp_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    try:
        operations = []
        for child in children:
            assert child.stdout is not None
            operations.append(Path(child.stdout.readline().strip()))
        assert all(
            (operation / "metadata").read_bytes() == b"live operation" for operation in operations
        )
        abandoned, live = operations
        children[0].kill()
        children[0].wait(timeout=5)
        assert abandoned.exists()
        with scratch.process_workspace(tmp_path) as owner:
            assert not abandoned.exists()
            assert (live / "metadata").read_bytes() == b"live operation"
            assert owner.stat().st_mode & 0o777 == 0o700
            with scratch.scratch_directory(prefix="reader-") as operation:
                assert Path(operation).parent == owner
        assert not owner.exists()
        assert live.exists()
    finally:
        for child in children:
            if child.poll() is None:
                child.communicate("\n", timeout=5)
            if child.stdin is not None:
                child.stdin.close()
            if child.stdout is not None:
                child.stdout.close()
    assert not live.exists()
    root = tmp_path / f"riverhog-runtime-scratch-{os.getuid()}"
    assert list(root.iterdir()) == [root / ".coordinator"]


@pytest.mark.skipif(os.name != "posix", reason="native server uses POSIX kernel leases")
def test_reclamation_ignores_unknown_entries_and_symlink_targets(tmp_path: Path) -> None:
    with scratch.process_workspace(tmp_path) as owner:
        root = owner.parent
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_bytes(b"not scratch")
    (root / ("operation-" + "a" * 32)).symlink_to(outside, target_is_directory=True)
    lease_link = root / ("operation-" + "b" * 32)
    lease_link.mkdir(mode=0o700)
    (lease_link / ".lease").symlink_to(outside / "keep")
    unknown = root / "unmanaged"
    unknown.mkdir()
    incomplete = root / ("operation-" + "c" * 32)
    incomplete.mkdir(mode=0o700)
    with scratch.process_workspace(tmp_path):
        assert not incomplete.exists()
        assert unknown.is_dir()
        assert lease_link.is_dir()
        assert (outside / "keep").read_bytes() == b"not scratch"
        assert (root / ("operation-" + "a" * 32)).is_symlink()


@pytest.mark.skipif(os.name != "posix", reason="native server uses POSIX kernel leases")
def test_failed_normal_cleanup_releases_lease_for_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = scratch.shutil.rmtree
    with monkeypatch.context() as patch:

        def fail_cleanup(path, *args, **kwargs):
            if Path(path).name.startswith("operation-"):
                raise OSError(errno.EBUSY, "fixture cleanup failure")
            return original(path, *args, **kwargs)

        patch.setattr(scratch.shutil, "rmtree", fail_cleanup)
        with (
            pytest.raises(OSError, match="fixture cleanup failure"),
            scratch.process_workspace(tmp_path) as abandoned,
        ):
            (abandoned / "metadata").write_bytes(b"abandoned")
    with scratch.process_workspace(tmp_path):
        assert not abandoned.exists()


def test_physical_capacity_rejects_before_allocation_and_can_recover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary_root = tmp_path / "scratch"
    temporary_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporary_root))
    with monkeypatch.context() as patch:
        patch.setattr(scratch.shutil, "disk_usage", lambda path: SimpleNamespace(free=0))
        with pytest.raises(ServiceUnavailable, match="temporary workspace capacity exhausted"):
            with scratch.scratch_directory(prefix="reader-"):
                pytest.fail("must not admit")
    assert list(temporary_root.iterdir()) == []
    with scratch.scratch_directory(prefix="reader-") as operation:
        assert Path(operation).exists()
    assert not Path(operation).exists()


@pytest.mark.parametrize("error_number", [errno.ENOSPC, errno.EDQUOT])
def test_exhaustion_during_operation_is_retryable_and_removes_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_number: int
) -> None:
    temporary_root = tmp_path / "scratch"
    temporary_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporary_root))
    with pytest.raises(ServiceUnavailable) as failure:
        with scratch.scratch_directory(prefix="reader-") as operation:
            raise OSError(error_number, "private pathname must not escape")
    assert str(failure.value) == "temporary workspace capacity exhausted"
    assert not Path(operation).exists()


def test_sqlite_full_is_capacity_failure_and_not_invalid_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary_root = tmp_path / "scratch"
    temporary_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporary_root))
    with pytest.raises(ServiceUnavailable, match="capacity exhausted"):
        with scratch.scratch_directory(prefix="reader-") as operation:
            db = sqlite3.connect(Path(operation) / "index.sqlite3")
            try:
                db.execute("PRAGMA max_page_count=2")
                db.execute("CREATE TABLE records(value BLOB)")
                db.execute("INSERT INTO records VALUES (?)", (bytes(8192),))
            finally:
                db.close()
    assert not Path(operation).exists()


@pytest.mark.parametrize("owner_type", [MemberRelevance, ProvenanceCustodySet])
def test_index_initialization_capacity_failure_closes_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owner_type: type
) -> None:
    temporary_root = tmp_path / "scratch"
    temporary_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporary_root))
    connect = sqlite3.connect

    def constrained(*args, **kwargs):
        db = connect(*args, **kwargs)
        db.execute("PRAGMA max_page_count=1")
        return db

    monkeypatch.setattr(sqlite3, "connect", constrained)
    with pytest.raises(ServiceUnavailable, match="capacity exhausted"):
        owner_type()
    assert list(temporary_root.iterdir()) == []


@pytest.mark.parametrize("owner_type", [MemberRelevance, ProvenanceCustodySet])
def test_index_operation_exhaustion_uses_retryable_capacity_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owner_type: type
) -> None:
    temporary_root = tmp_path / "scratch"
    temporary_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporary_root))
    with pytest.raises(ServiceUnavailable, match="capacity exhausted"):
        with owner_type():
            raise OSError(errno.EDQUOT, "private pathname")
    assert list(temporary_root.iterdir()) == []


def test_non_capacity_failures_keep_their_meaning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary_root = tmp_path / "scratch"
    temporary_root.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temporary_root))
    with pytest.raises(sqlite3.DatabaseError, match="malformed"):
        with scratch.scratch_directory(prefix="reader-"):
            raise sqlite3.DatabaseError("malformed")
    with pytest.raises(OSError) as failure:
        with scratch.scratch_directory(prefix="reader-"):
            raise OSError(errno.EIO, "I/O error")
    assert failure.value.errno == errno.EIO
    assert list(temporary_root.iterdir()) == []

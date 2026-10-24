from __future__ import annotations

import errno
import os
import stat
import subprocess
import sys

import pytest
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    lease_journal_validation_workspace,
)
from riverhog_core.scratch_workspace import process_workspace
from riverhog_provenance import JournalValidationCapacity

pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="server kernel file leases require POSIX"
)
ONE, TWO = "a" * 32, "b" * 32


def test_job_workspace_preserves_files_and_is_independent_of_process_scratch(tmp_path):
    root = tmp_path / "retained"
    with process_workspace(base=tmp_path):
        with lease_journal_validation_workspace(root, ONE, create=True) as directory:
            assert directory == root / ONE
            (directory / "snapshot.sqlite3").write_bytes(b"owned fixture state")
            assert directory.stat().st_mode & 0o777 == 0o700
            assert (directory / ".lease").stat().st_mode & 0o777 == 0o600
    with lease_journal_validation_workspace(root, ONE) as reopened:
        assert (reopened / "snapshot.sqlite3").read_bytes() == b"owned fixture state"


def test_busy_attempt_does_not_block_an_independent_worker(tmp_path):
    root = tmp_path / "retained"
    code = """import sys
from pathlib import Path
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
with lease_journal_validation_workspace(Path(sys.argv[1]),sys.argv[2]) as busy:
 assert busy is None
 with lease_journal_validation_workspace(Path(sys.argv[1]),sys.argv[3],create=True) as free:
  assert free is not None
"""
    with lease_journal_validation_workspace(root, ONE, create=True):
        result = subprocess.run(
            [sys.executable, "-c", code, str(root), ONE, TWO], capture_output=True
        )
        assert result.returncode == 0, result.stderr.decode()
    assert (root / ONE).is_dir() and (root / TWO).is_dir()


def test_actual_worker_exit_releases_only_its_lease_and_retains_progress(tmp_path):
    root = tmp_path / "retained"
    code = """import os,sys
from pathlib import Path
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
with lease_journal_validation_workspace(Path(sys.argv[1]),sys.argv[2],create=True) as directory:
 with (directory/'snapshot.sqlite3').open('wb') as stream:
  stream.write(b'owned committed fixture')
  stream.flush()
  os.fsync(stream.fileno())
 os._exit(83)
"""
    result = subprocess.run([sys.executable, "-c", code, str(root), ONE], capture_output=True)
    assert result.returncode == 83, result.stderr.decode()
    with lease_journal_validation_workspace(root, ONE) as directory:
        assert (directory / "snapshot.sqlite3").read_bytes() == b"owned committed fixture"


def test_missing_retained_directory_is_not_recreated(tmp_path):
    root = tmp_path / "missing-root"
    with pytest.raises(LostJournalValidationWorkspace):
        with lease_journal_validation_workspace(root, ONE):
            pytest.fail("lost workspace was admitted")
    assert not root.exists()


@pytest.mark.parametrize("state_name", ["snapshot.sqlite3", "corpus.sqlite3"])
def test_interrupted_empty_creation_can_finish_but_a_lost_database_lease_cannot(
    tmp_path, state_name
):
    root = tmp_path / "retained"
    directory = root / ONE
    directory.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    with lease_journal_validation_workspace(root, ONE, create=True) as resumed:
        assert resumed == directory
    (directory / state_name).write_bytes(b"retained fixture")
    (directory / ".lease").unlink()
    with pytest.raises(LostJournalValidationWorkspace, match="lease was lost"):
        with lease_journal_validation_workspace(root, ONE, create=True):
            pytest.fail("lost lease was silently recreated")
    assert not (directory / ".lease").exists()


@pytest.mark.parametrize(
    "fault", ["directory-mode", "directory-link", "lease-link", "lease-hardlink"]
)
def test_foreign_or_replaced_files_are_not_admitted_or_repaired(tmp_path, fault):
    root = tmp_path / "retained"
    with lease_journal_validation_workspace(root, ONE, create=True) as directory:
        pass
    if fault == "directory-mode":
        directory.chmod(0o755)
    elif fault == "directory-link":
        (root / TWO).symlink_to(directory, target_is_directory=True)
    else:
        (directory / ".lease").unlink()
        foreign = tmp_path / "foreign-fixture"
        foreign.write_bytes(b"unrelated fixture")
        foreign.chmod(0o600)
        if fault == "lease-link":
            (directory / ".lease").symlink_to(foreign)
        else:
            os.link(foreign, directory / ".lease")
    with pytest.raises(LostJournalValidationWorkspace):
        with lease_journal_validation_workspace(root, TWO if fault == "directory-link" else ONE):
            pytest.fail("foreign workspace state was admitted")
    if fault.startswith("lease"):
        assert foreign.read_bytes() == b"unrelated fixture"


def test_caller_io_failure_keeps_its_own_meaning(tmp_path):
    with pytest.raises(FileNotFoundError, match="caller source unavailable"):
        with lease_journal_validation_workspace(tmp_path / "retained", ONE, create=True):
            raise FileNotFoundError("caller source unavailable")


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
@pytest.mark.parametrize("phase", ["open", "lease-sync"])
def test_workspace_creation_reports_capacity_without_accepting_or_replacing_state(
    tmp_path, monkeypatch, code, phase
):
    original = os.open
    sync = os.fsync
    regular_syncs = []
    failed = []

    def full(path, *args, **kwargs):
        if phase == "open" and path == ".coordinator":
            raise OSError(code, "synthetic storage capacity")
        return original(path, *args, **kwargs)

    def full_sync(descriptor):
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            regular_syncs.append(descriptor)
            if phase == "lease-sync" and len(regular_syncs) == 2:
                failed.append(descriptor)
                raise OSError(code, "synthetic sync capacity")
        return sync(descriptor)

    monkeypatch.setattr(os, "open", full)
    monkeypatch.setattr(os, "fsync", full_sync)
    root = tmp_path / "retained"
    with pytest.raises(JournalValidationCapacity):
        with lease_journal_validation_workspace(root, ONE, create=True):
            pytest.fail("workspace was admitted without capacity")
    if phase == "open":
        assert not (root / ONE).exists()
    else:
        assert not (root / ONE / "snapshot.sqlite3").exists()
        assert len(failed) == 1
        with pytest.raises(OSError) as closed:
            os.fstat(failed[0])
        assert closed.value.errno == errno.EBADF


def test_retained_summary_keeps_kernel_lease_until_its_last_reader_closes(tmp_path):
    from contextlib import ExitStack

    from riverhog_provenance.journal_store import JournalStore

    root = tmp_path / "retained"
    resources = ExitStack()
    try:
        directory = resources.enter_context(
            lease_journal_validation_workspace(root, ONE, create=True)
        )
        current = JournalStore.open_retained(
            directory, create=True, on_storage_close=resources.close
        )
    except BaseException:
        resources.close()
        raise
    current.freeze()
    reader = current.borrow()
    current.close()
    code = """import sys
from pathlib import Path
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
with lease_journal_validation_workspace(Path(sys.argv[1]),sys.argv[2]) as directory:
 assert (directory is None)==(sys.argv[3]=="busy")
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", code, str(root), ONE, "busy"], capture_output=True
        )
        assert result.returncode == 0, result.stderr.decode()
        assert reader.db.execute("SELECT 1").fetchone() == (1,)
    finally:
        reader.close()
    result = subprocess.run(
        [sys.executable, "-c", code, str(root), ONE, "free"], capture_output=True
    )
    assert result.returncode == 0, result.stderr.decode()
    assert (root / ONE / "snapshot.sqlite3").is_file()


def test_shared_frozen_readers_exclude_writers_until_every_independent_reader_closes(tmp_path):
    from contextlib import ExitStack

    root = tmp_path / "retained"
    with lease_journal_validation_workspace(root, ONE, create=True):
        pass
    first, second = ExitStack(), ExitStack()
    try:
        assert (
            first.enter_context(lease_journal_validation_workspace(root, ONE, shared=True))
            == root / ONE
        )
        assert (
            second.enter_context(lease_journal_validation_workspace(root, ONE, shared=True))
            == root / ONE
        )
        code = """import sys
from pathlib import Path
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
root=Path(sys.argv[1])
with lease_journal_validation_workspace(root,sys.argv[2],shared=True) as reader:
 assert reader is not None
 with lease_journal_validation_workspace(root,sys.argv[2]) as writer:
  assert writer is None
 with lease_journal_validation_workspace(root,sys.argv[3],create=True) as independent:
  assert independent is not None
"""
        process = subprocess.run(
            [sys.executable, "-c", code, str(root), ONE, TWO], capture_output=True
        )
        assert process.returncode == 0, process.stderr.decode()
        first.close()
        with lease_journal_validation_workspace(root, ONE) as busy:
            assert busy is None
    finally:
        first.close()
        second.close()
    with lease_journal_validation_workspace(root, ONE) as ready:
        assert ready == root / ONE


def test_shared_database_reader_keeps_its_kernel_lease_through_last_borrow(tmp_path):
    from contextlib import ExitStack

    from riverhog_provenance.journal_store import JournalStore

    root = tmp_path / "retained"
    with lease_journal_validation_workspace(root, ONE, create=True) as path:
        JournalStore.open_retained(path, create=True).close()
    resources = ExitStack()
    try:
        directory = resources.enter_context(
            lease_journal_validation_workspace(root, ONE, shared=True)
        )
        current = JournalStore.open_retained(
            directory, read_only=True, on_storage_close=resources.close
        )
    except BaseException:
        resources.close()
        raise
    current.freeze()
    last = current.borrow()
    current.close()
    try:
        with lease_journal_validation_workspace(root, ONE, shared=True) as another:
            assert another == directory
        with lease_journal_validation_workspace(root, ONE) as busy:
            assert busy is None
    finally:
        last.close()
    with lease_journal_validation_workspace(root, ONE) as ready:
        assert ready == directory


@pytest.mark.parametrize("shared", [False, True])
def test_existing_reader_or_writer_cannot_recreate_a_lost_coordinator(tmp_path, shared):
    root = tmp_path / "retained"
    with lease_journal_validation_workspace(root, ONE, create=True):
        pass
    (root / ".coordinator").unlink()
    with pytest.raises(LostJournalValidationWorkspace):
        with lease_journal_validation_workspace(root, ONE, shared=shared):
            pytest.fail("a lost coordinator was recreated")
    assert not (root / ".coordinator").exists()


def test_shared_readers_never_create_a_workspace(tmp_path):
    root = tmp_path / "absent"
    with pytest.raises(ValueError, match="cannot create"):
        with lease_journal_validation_workspace(root, ONE, create=True, shared=True):
            pytest.fail("a reader created a workspace")
    assert not root.exists()


def test_retained_corpus_holds_the_real_kernel_lease_until_native_close(tmp_path):
    from contextlib import ExitStack

    from riverhog_provenance import CorpusReplayBinding, RetainedCorpusValidator
    from riverhog_provenance_contracts import ContractCatalog

    catalog = ContractCatalog()

    class EmptyCorpusSource:
        binding = CorpusReplayBinding(
            attempt_id=ONE,
            directory_id="owned-corpus",
            source_manifest_sha256="1" * 64,
            source_count=0,
            selection_manifest_sha256="2" * 64,
            selection_count=0,
            policy_sha256="3" * 64,
            catalog_sha256=catalog.checkpoint_sha256(),
        )

        def verify_current(self):
            return True

        def admit_source(self, anchor):
            return False

        def admit_selection(self, anchor):
            return False

    root = tmp_path / "retained"
    resources = ExitStack()
    try:
        directory = resources.enter_context(
            lease_journal_validation_workspace(root, ONE, create=True)
        )
        corpus = RetainedCorpusValidator.open_retained(
            directory,
            EmptyCorpusSource(),
            worker_epoch=0,
            create=True,
            catalog=catalog,
            on_storage_close=resources.close,
        )
    except BaseException:
        resources.close()
        raise
    code = """import sys
from pathlib import Path
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
with lease_journal_validation_workspace(Path(sys.argv[1]),sys.argv[2]) as directory:
 assert (directory is None)==(sys.argv[3]=="busy")
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", code, str(root), ONE, "busy"], capture_output=True
        )
        assert result.returncode == 0, result.stderr.decode()
        assert corpus._db.execute("SELECT sources,selections FROM corpus_workspace").fetchone() == (
            "0",
            "0",
        )
    finally:
        corpus.close()
    result = subprocess.run(
        [sys.executable, "-c", code, str(root), ONE, "free"], capture_output=True
    )
    assert result.returncode == 0, result.stderr.decode()
    assert (directory / "corpus.sqlite3").is_file()
    (directory / ".lease").unlink()
    with pytest.raises(LostJournalValidationWorkspace, match="lease was lost"):
        with lease_journal_validation_workspace(root, ONE, create=True):
            pytest.fail("lost corpus lease was recreated")
    assert not (directory / ".lease").exists()

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from riverhog_client import PrimaryStateError
from riverhog_client._prepared_primary_windows_native import (
    WindowsContainer,
    windows_container_path,
)


def test_windows_container_filename_commits_the_original_framed_descriptor():
    parent = Path("state")
    first = windows_container_path(parent, b"app\0allocation\0scope")
    assert first.parent == parent
    assert first != windows_container_path(parent, b"app\0allocation\0scope\0")
    assert first.name.startswith("primary-") and first.name.endswith(".sqlite3")
    assert first.name.isascii()


@pytest.mark.skipif(os.name != "nt", reason="requires native unprivileged Windows and NTFS")
def test_native_unprivileged_owner_and_transaction_process_boundaries(tmp_path):
    from tests.platform.windows_primary_process import run_unprivileged

    root = Path(__file__).resolve().parents[3]
    helper = root / "tests/platform/windows_primary_process.py"
    evidence_root = Path(os.environ.get("RIVERHOG_CI_TIMING_DIR", str(tmp_path)))
    evidence_root.mkdir(parents=True, exist_ok=True)
    report = evidence_root / "windows-prepared-primary.json"
    try:
        code = run_unprivileged(
            [sys.executable, str(helper), "--witness", str(tmp_path), "--report", str(report)],
            cwd=root,
            timeout_seconds=600,
            writable_directory=tmp_path,
            report=report,
        )
    except BaseException as exc:
        report.write_text(json.dumps({"failed": True, "launcher_error": str(exc)}))
        raise
    raw = report.read_text() if report.is_file() else ""
    evidence = json.loads(raw) if raw else {"missing_report": True, "exit_code": code}
    assert code == 0, evidence
    assert evidence["passed"] == 16 and evidence["elevated"] is False, evidence
    assert evidence["vfs"] == "win32"
    print(json.dumps(evidence, sort_keys=True))


class _JournalAPI:
    """Journal lifecycle fixture; native handle and ACL behavior runs separately."""

    identity = None
    private_error = False

    def open(self, _path, *, journal):
        assert journal
        if self.identity is None:
            raise FileNotFoundError
        return 1

    def information(self, _handle, _path, *, directory):
        assert not directory
        return self.identity

    def private(self, _handle, *, inherited):
        assert inherited
        if self.private_error:
            raise PrimaryStateError("fixture sidecar is not private")

    def close(self, _handle):
        pass


def _journal_guard(monkeypatch):
    guard = object.__new__(WindowsContainer)
    guard.path = Path("state.sqlite3")
    guard._api = _JournalAPI()
    guard._journal_identity = (7, 0, 1)
    monkeypatch.setattr(guard, "check_current", lambda: None)
    return guard


def test_journal_connection_recovery_rebinds_an_absent_old_sidecar(monkeypatch):
    guard = _journal_guard(monkeypatch)
    guard.check_journal(required=False, recovering=True)
    assert guard._journal_identity is None
    guard._api.identity = (7, 0, 2)
    guard.check_journal(required=True)
    assert guard._journal_identity == (7, 0, 2)


def test_journal_replacement_outside_connection_recovery_is_rejected(monkeypatch):
    guard = _journal_guard(monkeypatch)
    guard.check_journal(required=False)
    assert guard._journal_identity == (7, 0, 1)
    guard._api.identity = (7, 0, 2)
    with pytest.raises(PrimaryStateError, match="journal identity changed"):
        guard.check_journal(required=True)
    assert guard._journal_identity == (7, 0, 1)
    guard.check_journal(required=True, recovering=True)
    assert guard._journal_identity == (7, 0, 2)


@pytest.mark.parametrize("failure", ["missing", "unprivate"])
def test_journal_connection_recovery_preserves_presence_and_private_checks(monkeypatch, failure):
    guard = _journal_guard(monkeypatch)
    if failure == "unprivate":
        guard._api.identity = (7, 0, 2)
        guard._api.private_error = True
    with pytest.raises(PrimaryStateError, match="missing|not private"):
        guard.check_journal(required=True, recovering=True)
    assert guard._journal_identity == (7, 0, 1)

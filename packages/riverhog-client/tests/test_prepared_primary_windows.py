from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from riverhog_client._prepared_primary_windows_native import windows_container_path


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
    assert evidence["passed"] == 13 and evidence["elevated"] is False, evidence
    assert evidence["vfs"] == "win32"
    print(json.dumps(evidence, sort_keys=True))

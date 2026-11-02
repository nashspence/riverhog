from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from tests.platform import windows_primary_process as native


def test_launcher_deadline_retains_last_witness_phase_without_exception_payload(
    tmp_path, monkeypatch
):
    report = tmp_path / "witness.json"
    clock = iter((100.0, 100.0, 731.125))
    monkeypatch.setattr(native.time, "monotonic", lambda: next(clock))
    progress = native._WitnessProgress(report)
    progress.enter("resource-vfs", 53)
    original_inode = report.stat().st_ino

    native.record_witness_failure(
        report,
        subprocess.TimeoutExpired(["private-command"], 900, output=b"private-output"),
        origin="launcher",
    )

    result = json.loads(report.read_text())
    assert report.stat().st_ino == original_inode
    assert result == {
        "format": "riverhog-windows-primary-failure/v1",
        "failed": True,
        "origin": "launcher",
        "failure_class": "timeout",
        "progress": {
            "format": "riverhog-windows-primary-progress/v1",
            "phase": "resource-vfs",
            "completed_cases": 53,
            "elapsed_seconds": 631.125,
        },
        "source_frames": [],
    }
    assert len(report.read_bytes()) < 4096
    assert "private" not in report.read_text()
    assert "passed" not in result


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b'{"format":',
        b" " * 4097,
        b"[" * 1100,
        json.dumps(
            {
                "format": native._PROGRESS_FORMAT,
                "phase": "private-phase",
                "completed_cases": 1,
                "elapsed_seconds": 1,
            }
        ).encode(),
        json.dumps(
            {
                "format": native._PROGRESS_FORMAT,
                "phase": "resource-vfs",
                "completed_cases": True,
                "elapsed_seconds": 1,
            }
        ).encode(),
        json.dumps(
            {
                "format": native._PROGRESS_FORMAT,
                "phase": "resource-vfs",
                "completed_cases": 158,
                "elapsed_seconds": 1,
            }
        ).encode(),
        json.dumps(
            {
                "format": native._PROGRESS_FORMAT,
                "phase": "resource-vfs",
                "completed_cases": 1,
                "elapsed_seconds": float("nan"),
            }
        ).encode(),
        json.dumps(
            {
                "format": native._PROGRESS_FORMAT,
                "phase": "resource-vfs",
                "completed_cases": 1,
                "elapsed_seconds": 10**1000,
            }
        ).encode(),
        json.dumps(
            {
                "format": native._PROGRESS_FORMAT,
                "phase": "resource-vfs",
                "completed_cases": 1,
                "elapsed_seconds": 1,
                "extra": "private",
            }
        ).encode(),
    ],
    ids=(
        "empty",
        "partial",
        "oversized",
        "nested",
        "foreign-phase",
        "bool-count",
        "excess-count",
        "nan",
        "huge-time",
        "foreign-field",
    ),
)
def test_launcher_failure_rejects_incomplete_or_foreign_progress(tmp_path, body):
    report = tmp_path / "witness.json"
    report.write_bytes(body)
    native.record_witness_failure(report, TimeoutError("private-message"), origin="launcher")
    result = json.loads(report.read_text())
    assert result["progress"] is None
    assert result["failure_class"] == "timeout"
    assert "private" not in report.read_text()


def test_witness_failure_keeps_owned_source_frames_only(tmp_path):
    report = tmp_path / "witness.json"
    # The real reporter must not retain foreign traceback function names or payloads.
    with pytest.raises(AssertionError) as caught:
        native._WitnessProgress(report).enter("private-message", 0)
    native.record_witness_failure(report, caught.value, origin="witness")
    result = json.loads(report.read_text())
    assert result["failure_class"] == "assertion"
    assert result["progress"]["phase"] == "initialization"
    assert result["source_frames"] == []
    assert "private" not in report.read_text()


def test_failure_evidence_write_does_not_replace_original_failure(tmp_path, monkeypatch):
    report = tmp_path / "witness.json"
    progress = native._WitnessProgress(report)
    original = RuntimeError("original-private-error")

    def refused(*args, **kwargs):
        raise PermissionError("private-write-error")

    monkeypatch.setattr(Path, "write_text", refused)
    progress.enter("physical-space", 155)
    with pytest.raises(RuntimeError) as caught:
        try:
            raise original
        except RuntimeError as error:
            native.record_witness_failure(report, error, origin="launcher")
            raise
    assert caught.value is original


def test_witness_main_records_last_phase_on_failure(tmp_path, monkeypatch):
    report = tmp_path / "witness.json"

    def failed(root, *, progress):
        progress.enter("control-vfs", 139)
        raise AssertionError("private-message")

    monkeypatch.setattr(native, "witness", failed)
    monkeypatch.setattr(
        sys, "argv", ["witness", "--witness", str(tmp_path), "--report", str(report)]
    )
    assert native.main() == 1
    result = json.loads(report.read_text())
    assert result["failed"] is True and result["origin"] == "witness"
    assert result["progress"]["phase"] == "control-vfs"
    assert result["progress"]["completed_cases"] == 139
    assert [frame["function"] for frame in result["source_frames"]] == ["main"]
    assert all(
        type(frame["line"]) is int and frame["line"] > 0 for frame in result["source_frames"]
    )
    assert "private" not in report.read_text()


def test_witness_main_keeps_complete_result_separate_from_progress(tmp_path, monkeypatch):
    report = tmp_path / "witness.json"
    complete = {"passed": 157, "elevated": False, "cases": ["original-cases"]}

    def passed(root, *, progress):
        progress.enter("runtime-attestation", 157)
        return complete

    monkeypatch.setattr(native, "witness", passed)
    monkeypatch.setattr(
        sys, "argv", ["witness", "--witness", str(tmp_path), "--report", str(report)]
    )
    assert native.main() == 0
    assert json.loads(report.read_text()) == complete

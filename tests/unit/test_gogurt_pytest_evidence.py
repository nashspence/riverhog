from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import gogurt_pytest_evidence as evidence


def _report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    failed: bool = True,
    stash: dict | None = None,
) -> SimpleNamespace:
    monkeypatch.setattr(evidence, "ROOT", tmp_path)
    report = SimpleNamespace(failed=failed, when="call", sections=[])
    item = SimpleNamespace(
        path=tmp_path / "test_fixture.py",
        nodeid="test_fixture::test_failure",
        config=SimpleNamespace(stash={} if stash is None else stash),
        funcargs={"tmp_path": tmp_path},
    )
    call = SimpleNamespace(excinfo=SimpleNamespace(value=OSError(5, "PRIVATE fixture detail")))
    hook = evidence.pytest_runtest_makereport(item, call)
    next(hook)
    with pytest.raises(StopIteration) as result:
        hook.send(report)
    assert result.value.value is report
    return report


def test_evidence_is_failure_only_and_opt_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GOGURT_TEST_EVIDENCE_DIR", raising=False)
    monkeypatch.setattr(evidence, "_snapshot", lambda *_args: pytest.fail("unexpected collection"))
    _report(monkeypatch, tmp_path)
    target = tmp_path / "evidence"
    monkeypatch.setenv("GOGURT_TEST_EVIDENCE_DIR", str(target))
    _report(monkeypatch, tmp_path, failed=False)
    assert not target.exists()


def test_failure_captures_owned_state_without_private_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / "heartbeat.json").write_text(
        json.dumps(
            {
                "pid": 123,
                "queue_depth": 0,
                "active_dispatch": "PRIVATE action identity",
                "command": ["PRIVATE"],
                "environment": {"KEY": "PRIVATE"},
                "runtime": {"status": "failed", "diagnostic": "unsettled dispatch worker PRIVATE"},
            }
        )
    )
    connection = sqlite3.connect(state / "listener.sqlite3")
    try:
        connection.execute("CREATE TABLE dispatches (state TEXT, plan TEXT)")
        connection.execute("INSERT INTO dispatches VALUES ('uncertain', 'PRIVATE')")
        connection.commit()
    finally:
        connection.close()
    target = tmp_path / "evidence"
    monkeypatch.setenv("GOGURT_TEST_EVIDENCE_DIR", str(target))
    monkeypatch.setenv("GOGURT_CI_SOURCE_SHA", "a" * 40)
    report = _report(monkeypatch, tmp_path)
    assert report.sections == []
    [path] = target.iterdir()
    text = path.read_text()
    assert "PRIVATE" not in text
    payload = json.loads(text)
    assert payload["source_sha"] == "a" * 40
    assert payload["state"]["dispatch_counts"] == {"uncertain": 1}
    assert payload["state"]["heartbeat"]["shutdown_classification"] == "unsettled dispatch worker"
    assert payload["exception"]["errno"] == 5
    assert len(text.encode()) < evidence.MAX_FILE_BYTES


def test_snapshot_failure_keeps_original_test_failure_and_a_minimal_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(_path: Path) -> dict:
        raise RuntimeError("PRIVATE")

    monkeypatch.setattr(evidence, "_snapshot", broken)
    target = tmp_path / "evidence"
    monkeypatch.setenv("GOGURT_TEST_EVIDENCE_DIR", str(target))
    _report(monkeypatch, tmp_path)
    [path] = target.iterdir()
    assert json.loads(path.read_text())["state"]["unavailable"]["type"] == "RuntimeError"
    assert "PRIVATE" not in path.read_text()


def test_budget_and_write_failure_do_not_replace_test_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "not-a-directory"
    target.write_text("fixture")
    monkeypatch.setenv("GOGURT_TEST_EVIDENCE_DIR", str(target))
    report = _report(monkeypatch, tmp_path)
    assert "retention failed" in report.sections[0][1]
    report = _report(monkeypatch, tmp_path, stash={evidence._REPORT_COUNT: evidence.MAX_REPORTS})
    assert "report limit" in report.sections[0][1]


def test_oversized_sources_are_rejected_and_missing_database_not_created(tmp_path: Path) -> None:
    path = tmp_path / "heartbeat.json"
    path.write_bytes(b"x" * (evidence.MAX_FILE_BYTES + 1))
    with pytest.raises(ValueError, match="byte budget"):
        evidence._read_owned_file(path)
    database = tmp_path / "missing.sqlite3"
    with pytest.raises(FileNotFoundError):
        evidence._dispatch_counts(database)
    assert not database.exists()


def test_incomplete_heartbeat_does_not_hide_database_counts(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    (state / "heartbeat.json").write_text("{")
    connection = sqlite3.connect(state / "listener.sqlite3")
    try:
        connection.execute("CREATE TABLE dispatches (state TEXT)")
        connection.commit()
    finally:
        connection.close()
    snapshot = evidence._snapshot(tmp_path)
    assert snapshot["dispatch_counts"] == {}
    assert snapshot["heartbeat"]["unavailable"]["type"] == "JSONDecodeError"


def test_real_pytest_failure_is_retained_before_fixture_teardown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os
    import subprocess
    import sys

    project = tmp_path / "isolated-pytest"
    project.mkdir()
    (project / "pytest.ini").write_text("[pytest]\naddopts = --strict-markers\n")
    (project / "conftest.py").write_text(
        "from pathlib import Path\nimport shutil,pytest\n"
        "from scripts import gogurt_pytest_evidence as plugin\n"
        "plugin.ROOT = Path(__file__).parent\n"
        "pytest_plugins = ('scripts.gogurt_pytest_evidence',)\n"
        "@pytest.fixture(autouse=True)\n"
        "def remove_state(tmp_path):\n"
        "    yield\n"
        "    shutil.rmtree(tmp_path)\n"
    )
    (project / "test_failure.py").write_text(
        "def test_failure(tmp_path):\n"
        "    state = tmp_path / 'state'\n"
        "    state.mkdir()\n"
        "    (state / 'heartbeat.json').write_text('{\"pid\":123}')\n"
        "    assert False, 'intentional fixture failure'\n"
    )
    target = tmp_path / "evidence"
    environment = os.environ.copy()
    environment["GOGURT_TEST_EVIDENCE_DIR"] = str(target)
    environment["PYTHONPATH"] = str(evidence.ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-c",
            str(project / "pytest.ini"),
            "--confcutdir",
            str(project),
            str(project / "test_failure.py"),
        ],
        cwd=project,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    [path] = target.iterdir()
    payload = json.loads(path.read_text())
    assert payload["phase"] == "call"
    assert payload["state"]["heartbeat"]["pid"] == 123
    assert "intentional fixture failure" not in path.read_text()

"""Opt-in, bounded failure evidence for CI's pytest lanes; no successful-test I/O."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import sys
import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAX_REPORTS = 16  # Per pytest worker: bounded even under a cascading failure.
MAX_FILE_BYTES = 65536
_REPORT_COUNT = pytest.StashKey[int]()


def _error(exc: BaseException) -> dict[str, object]:
    return {
        "type": type(exc).__name__,
        "errno": getattr(exc, "errno", None),
        "winerror": getattr(exc, "winerror", None),
    }


def _read_owned_file(path: Path) -> bytes:
    if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode):
        raise ValueError("not an owned regular evidence file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("not an owned regular evidence file")
        data = b""
        while len(data) <= MAX_FILE_BYTES:
            block = os.read(descriptor, MAX_FILE_BYTES + 1 - len(data))
            if not block:
                break
            data += block
    finally:
        os.close(descriptor)
    if len(data) > MAX_FILE_BYTES:
        raise ValueError("evidence source exceeds byte budget")
    return data


def _heartbeat(path: Path) -> dict[str, object]:
    payload = json.loads(_read_owned_file(path))
    if not isinstance(payload, dict):
        raise ValueError("heartbeat is not an object")
    # Never retain argv, environment, credentials, arbitrary diagnostic text,
    # config paths, or action stdout/stderr from a pytest failure report.
    result: dict[str, object] = {}
    for key in ("pid", "queue_depth"):
        value = payload.get(key)
        if type(value) is int and 0 <= value <= (1 << 63) - 1:
            result[key] = value
    result["active_dispatch_present"] = isinstance(payload.get("active_dispatch"), str)
    runtime = payload.get("runtime")
    if isinstance(runtime, dict):
        if runtime.get("status") in ("running", "failed"):
            result["runtime_status"] = runtime["status"]
        diagnostic = runtime.get("diagnostic")
        result["diagnostic_present"] = bool(diagnostic)
        result["shutdown_classification"] = next(
            (
                kind
                for kind in ("unsettled dispatch worker", "live child process")
                if isinstance(diagnostic, str) and kind in diagnostic
            ),
            None,
        )
    return result


def _dispatch_counts(path: Path) -> dict[str, int]:
    if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode):
        raise ValueError("not an owned regular database")
    # Read the live WAL through SQLite, never copy a potentially incomplete
    # primary database or open it immutable while a writer may still exist.
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.05)
    deadline = time.monotonic() + 0.05
    try:
        connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        return {
            str(state): int(count)
            for state, count in connection.execute(
                "SELECT state, count(*) FROM dispatches GROUP BY state LIMIT 8"
            )
            if state in {"queued", "running", "retry", "uncertain", "completed", "failed"}
        }
    finally:
        connection.close()


def _frame_location(filename: str, name: str, line: int) -> dict[str, object]:
    path = Path(filename)
    try:
        source = path.relative_to(ROOT).as_posix()
    except ValueError:
        source = path.name
    return {"file": source[:256], "function": name[:128], "line": line}


def _gogurt_threads() -> list[dict[str, object]]:
    names = {thread.ident: thread.name for thread in threading.enumerate()}
    result: list[dict[str, object]] = []
    for ident, frame in list(sys._current_frames().items())[:64]:
        frames = []
        relevant = names.get(ident) == "gogurt-dispatch"
        for _ in range(32):
            if frame is None:
                break
            filename = frame.f_code.co_filename
            relevant = relevant or "gogurt_listener_runtime" in Path(filename).parts
            frames.append(_frame_location(filename, frame.f_code.co_name, frame.f_lineno))
            frame = frame.f_back
        if relevant:
            result.append(
                {
                    "role": "gogurt-dispatch"
                    if names.get(ident) == "gogurt-dispatch"
                    else "listener-control",
                    "frames": frames[:16],
                }
            )
    return result[:8]


def _snapshot(tmp_path: Path | None) -> dict[str, object]:
    result: dict[str, object] = {"threads": _gogurt_threads()}
    if tmp_path is None:
        result["state"] = "no fixture state available"
        return result
    state = tmp_path / "state"  # The listener test's explicit owned state, not a recursive scan.
    if state.is_symlink():
        result["state"] = "refused symlink"
        return result
    for name, read in (
        ("heartbeat", lambda: _heartbeat(state / "heartbeat.json")),
        ("dispatch_counts", lambda: _dispatch_counts(state / "listener.sqlite3")),
    ):
        try:
            result[name] = read()
        except (OSError, ValueError, sqlite3.Error) as exc:
            result[name] = {"unavailable": _error(exc)}
    return result


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[Any]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    destination = os.environ.get("GOGURT_TEST_EVIDENCE_DIR")
    if not destination or not report.failed or not item.path.is_relative_to(ROOT):
        return report
    count = item.config.stash.get(_REPORT_COUNT, 0)
    if count >= MAX_REPORTS:
        report.sections.append(("Gogurt evidence", "per-worker report limit reached"))
        return report
    item.config.stash[_REPORT_COUNT] = count + 1
    fixture = getattr(item, "funcargs", {}).get("tmp_path")
    try:
        state = _snapshot(fixture if isinstance(fixture, Path) else None)
    except Exception as exc:
        # Diagnostics must never replace the original failing report.
        state = {"unavailable": _error(exc)}
    payload = {
        "test_id_sha256": hashlib.sha256(item.nodeid.encode()).hexdigest(),
        "test_file": item.path.relative_to(ROOT).as_posix(),
        "phase": report.when,
        "source_sha": os.environ.get("GOGURT_CI_SOURCE_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "exception": _error(call.excinfo.value) if call.excinfo is not None else None,
        "state": state,
    }
    try:
        target = Path(destination)
        target.mkdir(parents=True, exist_ok=True)
        # Exclusive creation protects concurrent xdist workers without locks.
        name = f"{payload['test_id_sha256']}-{report.when}-{os.getpid()}.json"
        with (target / name).open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
    except (OSError, ValueError) as exc:
        report.sections.append(("Gogurt evidence", f"retention failed: {type(exc).__name__}"))
    return report

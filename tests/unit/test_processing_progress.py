from __future__ import annotations

from collections import Counter

import pytest

from scripts import qualify_stove0_processing as processing


def test_processing_wait_inspects_complete_evidence_and_new_work_after_progress(monkeypatch):
    a, b = "a" * 64, "b" * 64
    pages = [
        [{"work_id": a, "phase": "executing"}],
        [{"work_id": a, "phase": "complete"}, {"work_id": b, "phase": "executing"}],
        [
            {"work_id": a, "phase": "complete"},
            {"work_id": b, "phase": "complete", "output": {"collection_id": 2}},
        ],
    ]
    calls = Counter()
    clock = [0.0]

    def stove(path, _payload=None, *, timeout=30):
        calls[path] += 1
        if path.startswith("/v1/work?"):
            return {"work": pages.pop(0)}
        assert timeout == 5
        work_id = path.split("/")[-2]
        return {
            "format": "stove0-work-progress/v1",
            "work_id": work_id,
            "phase": "complete",
            "revision": 7,
        }

    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "10")
    monkeypatch.setattr(processing, "stove", stove)
    monkeypatch.setattr(processing.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        processing.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    processing.wait()
    assert not pages
    assert calls["/v1/work/" + a + "/progress"] == 2
    assert calls["/v1/work/" + b + "/progress"] == 1
    assert sum(value for path, value in calls.items() if path.startswith("/v1/work?")) == 3


def test_processing_wait_fetches_exact_failure_after_terminal_progress(monkeypatch):
    work_id = "a" * 64
    paths = []

    def stove(path, _payload=None, *, timeout=30):
        paths.append(path)
        if path.startswith("/v1/work?"):
            return {"work": [{"work_id": work_id, "phase": "executing"}]}
        if path.endswith("/progress"):
            return {
                "format": "stove0-work-progress/v1",
                "work_id": work_id,
                "phase": "failed",
                "revision": 2,
            }
        assert path == "/v1/work/" + work_id
        return {
            "work_id": work_id,
            "phase": "failed",
            "failure": {"code": "native-target-failure", "message": "exact failure"},
        }

    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "10")
    monkeypatch.setattr(processing, "stove", stove)
    with pytest.raises(RuntimeError, match="native-target-failure"):
        processing.wait()
    assert paths[-1] == "/v1/work/" + work_id

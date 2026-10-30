from __future__ import annotations

import json
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


def test_processing_wait_fetches_exact_failure_after_terminal_progress(monkeypatch, capsys):
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
    annotation = _wait_annotation(capsys.readouterr().err)
    assert annotation["reason"] == "terminal-work"
    assert annotation["latest_progress_phase_counts"]["failed"] == 1
    assert annotation["full_work_phase_counts"]["executing"] == 1
    assert work_id not in json.dumps(annotation)
    assert "native-target-failure" not in json.dumps(annotation)


def test_processing_wait_discovers_a_late_failed_child_before_parent_completion(monkeypatch):
    parent, child = "a" * 64, "b" * 64
    clock = [0.0]
    paths = []
    browses = 0

    def stove(path, _payload=None, *, timeout=30):
        nonlocal browses
        paths.append(path)
        assert timeout == 5
        if path.startswith("/v1/work?"):
            browses += 1
            rows = [{"work_id": parent, "phase": "coordinating"}]
            if browses > 1:
                rows.append({"work_id": child, "phase": "failed"})
            return {"work": rows}
        if path.endswith("/progress"):
            work_id = path.split("/")[-2]
            return {
                "format": "stove0-work-progress/v1",
                "work_id": work_id,
                "phase": "failed" if work_id == child else "coordinating",
                "revision": 3 if work_id == child else 1,
            }
        assert path == "/v1/work/" + child
        return {
            "work_id": child,
            "phase": "failed",
            "failure": {"code": "late-child-failed", "message": "exact child failure"},
        }

    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "300")
    monkeypatch.setattr(processing, "stove", stove)
    monkeypatch.setattr(processing.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(processing.time, "sleep", lambda _: clock.__setitem__(0, clock[0] + 61))
    with pytest.raises(RuntimeError, match="late-child-failed"):
        processing.wait()
    assert clock[0] == 61
    assert browses == 2
    assert paths[-1] == "/v1/work/" + child


def test_processing_metadata_retry_preserves_exact_post_and_original_deadline(monkeypatch):
    import io
    import json
    import urllib.error

    clock, requests = [0.0], []
    monkeypatch.setattr(processing.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        processing.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )

    def contact(request, *, timeout):
        requests.append((request, timeout))
        if len(requests) < 3:
            raise urllib.error.HTTPError(request.full_url, 503, "unavailable", {}, io.BytesIO(b""))
        return io.BytesIO(b'{"job_id":"same-invocation"}')

    monkeypatch.setattr(processing.urllib.request, "urlopen", contact)
    payload = {"invocation_id": "a" * 64, "preview_sha256": "b" * 64}
    assert processing.stove("/v1/work", payload, timeout=2.5) == {"job_id": "same-invocation"}
    assert [timeout for _, timeout in requests] == [2.5, 1.5, 0.5]
    assert all(request is requests[0][0] for request, _ in requests)
    assert all(json.loads(request.data) == payload for request, _ in requests)


def test_processing_metadata_deferral_cannot_extend_the_original_deadline(monkeypatch):
    import io
    import urllib.error

    clock, requests = [0.0], []
    monkeypatch.setattr(processing.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        processing.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )

    def unavailable(request, *, timeout):
        requests.append(timeout)
        raise urllib.error.HTTPError(request.full_url, 503, "unavailable", {}, io.BytesIO(b""))

    monkeypatch.setattr(processing.urllib.request, "urlopen", unavailable)
    with pytest.raises(TimeoutError, match="original deadline"):
        processing.stove("/v1/work", {"invocation_id": "a" * 64}, timeout=2.5)
    assert clock[0] == 2.5 and requests == [2.5, 1.5, 0.5]


@pytest.mark.parametrize("status", [400, 401, 403, 409, 500])
def test_processing_metadata_retry_does_not_hide_nontransient_errors(monkeypatch, status):
    import io
    import urllib.error

    error = urllib.error.HTTPError("https://stove0.invalid", status, "failure", {}, io.BytesIO(b""))

    def failed(*args, **kwargs):
        raise error

    def forbidden(*args):
        pytest.fail("nontransient response was retried")

    monkeypatch.setattr(processing.urllib.request, "urlopen", failed)
    monkeypatch.setattr(processing.time, "sleep", forbidden)
    with pytest.raises(urllib.error.HTTPError) as received:
        processing.stove("/v1/work", {"invocation_id": "a" * 64})
    assert received.value is error
    error.close()


def _wait_annotation(output):
    prefix = "::error title=Processing wait qualification::"
    rows = [
        json.loads(line.removeprefix(prefix))
        for line in output.splitlines()
        if line.startswith(prefix)
    ]
    assert len(rows) == 1
    return rows[0]


@pytest.mark.parametrize(
    "mode", ["stalled-target", "metadata-unavailable", "diagnostic-read-unavailable"]
)
def test_processing_wait_deadline_keeps_failure_and_emits_observed_state_counts(
    monkeypatch, capsys, mode
):
    work_id = "a" * 64
    clock = [0.0]
    paths = []

    def stove(path, payload=None, *, timeout=30):
        paths.append(path)
        assert timeout == 5
        if path == "/v1/admin/scheduler/run":
            if mode == "diagnostic-read-unavailable":
                raise TimeoutError("private scheduler payload")
            return {"private-scheduler-key": "private scheduler payload"}
        if mode == "metadata-unavailable":
            raise TimeoutError("private status payload")
        if path.endswith("/progress"):
            return {
                "format": "stove0-work-progress/v1",
                "work_id": work_id,
                "phase": "output_finalizing",
                "revision": 7,
            }
        return {
            "work": [
                {
                    "work_id": work_id,
                    "phase": "executing",
                    "target_status": {"state": "running", "failure": None},
                    "target_settlement": None,
                    "output": None,
                    "failure": None,
                    "private-key": "private response",
                }
            ]
        }

    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "2")
    monkeypatch.setattr(processing, "stove", stove)
    monkeypatch.setattr(processing.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        processing.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    with pytest.raises(TimeoutError):
        processing.wait()
    assert clock[0] == 2
    assert paths[-1] == "/v1/admin/scheduler/run"
    annotation = _wait_annotation(capsys.readouterr().err)
    assert annotation["format"] == "riverhog-processing-wait-diagnostic/v1"
    assert annotation["reason"] == "completion-deadline"
    assert annotation["elapsed_seconds"] == 2
    if mode == "metadata-unavailable":
        assert annotation["full_work_count"] == annotation["latest_progress_count"] == 0
        assert annotation["full_work_snapshot_age_seconds"] is None
        assert annotation["status_timeouts"] == 4
    else:
        assert annotation["full_work_phase_counts"]["executing"] == 1
        assert annotation["latest_progress_phase_counts"]["output_finalizing"] == 1
        assert annotation["full_work_target_state_counts"]["running"] == 1
        assert annotation["full_work_snapshot_age_seconds"] == 2
        assert annotation["status_timeouts"] == 0
        assert annotation["full_work_with_output"] == 0
        assert annotation["full_work_with_target_settlement"] == 0
    encoded = json.dumps(annotation)
    assert work_id not in encoded and "private" not in encoded


def test_processing_wait_annotation_cannot_copy_unrecognized_state_names_or_payloads(capsys):
    private = "private-response-value"
    processing.wait_failure_annotation(
        "completion-deadline",
        rows=[
            {
                "work_id": private,
                "phase": private,
                "target_status": {"state": private, "failure": {"message": private}},
                "failure": {"message": private},
                "output": {"private": private},
                "target_settlement": {"private": private},
            }
        ],
        progress=[{"work_id": private, "phase": private}],
        elapsed_seconds=7,
        snapshot_age_seconds=3,
        status_timeouts=2,
    )
    annotation = _wait_annotation(capsys.readouterr().err)
    assert private not in json.dumps(annotation)
    assert annotation["full_work_count"] == annotation["latest_progress_count"] == 1
    assert (
        annotation["full_work_failure_count"] == annotation["full_work_target_failure_count"] == 1
    )
    assert (
        annotation["full_work_with_output"] == annotation["full_work_with_target_settlement"] == 1
    )
    assert sum(annotation["full_work_phase_counts"].values()) == 0
    assert sum(annotation["latest_progress_phase_counts"].values()) == 0
    assert sum(annotation["full_work_target_state_counts"].values()) == 0

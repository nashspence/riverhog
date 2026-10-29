from __future__ import annotations

import io
import json
import urllib.error

import pytest

from scripts import qualify_review0_delivery as owner

CANARY = "omitted-diagnostic-content"
WORK = "a" * 64


def annotation(capsys):
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 1
    prefix = "::error title=Review0 qualification::"
    assert lines[0].startswith(prefix)
    return json.loads(
        lines[0][len(prefix) :].replace("%0A", "\n").replace("%0D", "\r").replace("%25", "%")
    )


def test_evaluation_timeout_reports_bounded_work_state_without_payloads_or_messages(
    monkeypatch, capsys
):
    identities = [f"{index:064x}" for index in range(1, 7)]
    evaluation = {
        "evaluation_id": "b" * 64,
        "phase": "running",
        "children": [{"work_id": identities[0], "state": "running"}],
        "definition": CANARY,
    }
    calls = []

    def stove(path, **options):
        calls.append((path, options))
        if path.startswith("/v1/evaluations/"):
            return evaluation
        assert options == {"timeout": 5, "maximum_response_octets": 128 * 1024}
        index = identities.index(path.rsplit("/", 1)[1])
        return {
            "work_id": identities[index],
            "phase": "waiting_target",
            "failure": {"code": "capacity_busy", "message": CANARY},
            "preview_acceptance": {"target_plans": [{"work_id": identities[index + 1]}]},
            "target_status": {
                "phase": "running",
                "failure": {"code": "sample_busy", "message": CANARY},
            },
            "work": CANARY,
        }

    ticks = iter((0, 0, 601))
    sleeps = []
    monkeypatch.setattr(owner.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(owner.time, "sleep", sleeps.append)
    monkeypatch.setattr(owner, "stove", stove)
    with pytest.raises(TimeoutError) as failed:
        owner.await_evaluation("b" * 64)
    result = annotation(capsys)
    assert result == failed.value.args[0]
    assert result["stage"] == "review-evaluation" and result["truncated"]
    assert len(calls) == 5 and len(result["works"]) == 4 and sleeps == [0.5]
    assert [row["work_id"] for row in result["works"]] == identities[:4]
    assert all(row["failure_code"] == "capacity_busy" for row in result["works"])
    assert CANARY not in json.dumps(result) and len(json.dumps(result)) < 6000


@pytest.mark.parametrize("state", ["failed", "canceled", "inapplicable"])
def test_terminal_evaluation_keeps_its_failure_and_emits_control_state(monkeypatch, capsys, state):
    evaluation = {
        "evaluation_id": "b" * 64,
        "phase": state,
        "children": [{"work_id": WORK, "state": state}],
    }
    monkeypatch.setattr(owner.time, "monotonic", lambda: 0)
    monkeypatch.setattr(
        owner,
        "stove",
        lambda path, **_kw: (
            evaluation if path.startswith("/v1/evaluations/") else {"work_id": WORK, "phase": state}
        ),
    )
    with pytest.raises(AssertionError) as failed:
        owner.await_evaluation("b" * 64)
    assert annotation(capsys) == failed.value.args[0]


def test_successful_evaluation_performs_no_diagnostic_reads_or_annotation(monkeypatch, capsys):
    child = {"work_id": WORK, "state": "complete"}
    calls = []

    def stove(path, **options):
        calls.append((path, options))
        return {"phase": "complete", "children": [child]}

    monkeypatch.setattr(owner, "stove", stove)
    assert owner.await_evaluation("b" * 64) is child
    assert calls == [("/v1/evaluations/" + "b" * 64, {})]
    assert not capsys.readouterr().err


def test_diagnostic_deduplicates_cycles_and_ignores_untrusted_identity_or_text(monkeypatch, capsys):
    calls = []

    def stove(path, **options):
        calls.append((path, options))
        return {
            "work_id": WORK,
            "phase": "running\n::warning::" + CANARY,
            "failure": {"code": "bad code " + CANARY, "message": CANARY},
            "preview_acceptance": {
                "target_plans": [{"work_id": WORK}, {"work_id": "../../" + CANARY}]
            },
            "target_status": {"failure": {"code": "known.code/v1", "message": CANARY}},
        }

    monkeypatch.setattr(owner, "stove", stove)
    result = owner._report_failure(
        "review-evaluation", evaluation={"children": [{"work_id": WORK, "state": "running"}]}
    )
    assert annotation(capsys) == result
    assert len(calls) == 1 and len(result["works"]) == 1 and not result["truncated"]
    assert result["works"][0]["phase"] is None
    assert result["works"][0]["failure_code"] is None
    assert result["works"][0]["target_failure_code"] == "known.code/v1"
    assert CANARY not in json.dumps(result)


@pytest.mark.parametrize("body,accepted", [(b"{}", True), (b"{} ", False)])
def test_diagnostic_http_reader_has_explicit_byte_and_timeout_admission(
    monkeypatch, body, accepted
):
    reads = []
    calls = []

    class Response(io.BytesIO):
        def read(self, size=-1):
            reads.append(size)
            return super().read(size)

    def urlopen(_request, *, timeout):
        calls.append(timeout)
        return Response(body)

    monkeypatch.setattr(owner.urllib.request, "urlopen", urlopen)
    if accepted:
        assert owner.stove("/v1/work/" + WORK, timeout=5, maximum_response_octets=2) == {}
    else:
        with pytest.raises(ValueError, match="byte budget"):
            owner.stove("/v1/work/" + WORK, timeout=5, maximum_response_octets=2)
    assert reads == [3] and calls == [5]


def test_http_diagnostic_failure_reports_status_without_reading_error_body(monkeypatch, capsys):
    class UnreadBody(io.BytesIO):
        def read(self, *_args):
            raise AssertionError("diagnostic must not read an HTTP error body")

    error = urllib.error.HTTPError("http://unit.invalid/", 401, CANARY, None, UnreadBody())

    def urlopen(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(owner.urllib.request, "urlopen", urlopen)
    result = owner._report_failure(
        "review-evaluation", evaluation={"children": [{"work_id": WORK, "state": "running"}]}
    )
    assert annotation(capsys) == result
    assert result["works"] == [
        {"work_id": WORK, "diagnostic_error": "HTTPError", "http_status": 401}
    ]
    assert CANARY not in json.dumps(result)


def test_delivery_admission_timeout_emits_failure_instead_of_success(monkeypatch, capsys):
    monkeypatch.setenv("REVIEW_OUTPUT_COLLECTION_ID", "1")
    monkeypatch.setenv("REVIEW_SOURCE_COLLECTION_ID", "2")
    ticks = iter((0, 601))
    monkeypatch.setattr(owner.time, "monotonic", lambda: next(ticks))
    calls = []

    def stove(path, **options):
        calls.append((path, options))
        return {"baseline_mode": "backfill"}

    monkeypatch.setattr(owner, "stove", stove)
    with pytest.raises(AssertionError) as failed:
        owner.delivery()
    result = annotation(capsys)
    assert result == failed.value.args[0] and result["stage"] == "delivery-admission"
    assert len(calls) == 1 and not result["works"]


def test_delivery_failure_reuses_observed_work_and_preserves_the_failure(monkeypatch, capsys):
    monkeypatch.setenv("REVIEW_OUTPUT_COLLECTION_ID", "1")
    monkeypatch.setenv("REVIEW_SOURCE_COLLECTION_ID", "2")
    monkeypatch.setattr(owner.time, "monotonic", lambda: 0)
    calls = []

    def stove(path, **options):
        calls.append((path, options))
        if path.endswith(":backfill"):
            return {"baseline_mode": "backfill"}
        if path.startswith("/v1/admissions?"):
            return {
                "admissions": [
                    {
                        "intent": {
                            "collection": {"collection_id": "1"},
                            "policy_id": "review0-output-delivery",
                        },
                        "state": "work_bound",
                        "work_id": WORK,
                    }
                ]
            }
        return {
            "work_id": WORK,
            "phase": "failed",
            "failure": {"code": "delivery_failed", "message": CANARY},
        }

    monkeypatch.setattr(owner, "stove", stove)
    with pytest.raises(AssertionError) as failed:
        owner.delivery()
    result = annotation(capsys)
    assert result == failed.value.args[0] and result["stage"] == "delivery-work"
    assert len(calls) == 3 and result["works"][0]["failure_code"] == "delivery_failed"
    assert CANARY not in json.dumps(result)

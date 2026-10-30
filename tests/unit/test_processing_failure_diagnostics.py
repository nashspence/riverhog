from __future__ import annotations

import io
import json
import subprocess
import sys
import urllib.error

import pytest

from scripts import qualify_stove0_processing as processing

PREFIX = "::error title=Processing qualification::"


def annotation(output):
    rows = [
        json.loads(line.removeprefix(PREFIX))
        for line in output.splitlines()
        if line.startswith(PREFIX)
    ]
    assert len(rows) == 1
    return rows[0]


@pytest.mark.parametrize(
    "error,category",
    [
        (AssertionError("private fixture payload"), "assertion"),
        (TimeoutError("private fixture payload"), "timeout"),
        (OSError("private fixture payload"), "filesystem"),
        (ValueError("private fixture payload"), "value"),
        (KeyError("private fixture payload"), "key"),
        (TypeError("private fixture payload"), "type"),
        (RuntimeError("private fixture payload"), "runtime"),
        (Exception("private fixture payload"), "unexpected"),
    ],
)
def test_processing_failure_annotation_excludes_exception_payload_and_type_names(
    capsys, error, category
):
    processing.qualification_failure_annotation(error)
    observed = annotation(capsys.readouterr().err)
    assert observed == {
        "format": "riverhog-processing-failure-diagnostic/v1",
        "failure_class": category,
        "source_frames": [],
    }


def test_processing_failure_annotation_keeps_http_status_without_request_or_response(capsys):
    error = urllib.error.HTTPError(
        "https://private.invalid/secret",
        503,
        "private reason",
        {"X-Private": "private header"},
        io.BytesIO(b"private body"),
    )
    try:
        processing.qualification_failure_annotation(error)
    finally:
        error.close()
    observed = annotation(capsys.readouterr().err)
    assert observed == {
        "format": "riverhog-processing-failure-diagnostic/v1",
        "failure_class": "http-status",
        "source_frames": [],
        "http_status": 503,
    }


def test_processing_main_keeps_original_exception_and_excludes_imported_callback_frames(
    monkeypatch, capsys
):
    error = RuntimeError("private callback payload")

    def private_callback_name():
        raise error

    monkeypatch.setattr(processing, "await_admission", private_callback_name)
    monkeypatch.setattr(sys, "argv", ["qualification", "admission"])
    with pytest.raises(RuntimeError) as observed:
        processing.main()
    assert observed.value is error
    payload = annotation(capsys.readouterr().err)
    assert payload["failure_class"] == "runtime"
    assert len(payload["source_frames"]) == 1
    assert payload["source_frames"][0]["function"] == "main"
    assert payload["source_frames"][0]["line"] > processing.main.__code__.co_firstlineno
    assert "private" not in json.dumps(payload)


def test_processing_failure_frame_observation_is_bounded_and_contains_only_owned_source(capsys):
    actual, declared = 0, 1
    for _ in range(30):
        actual, declared = {"private-key": actual}, {"private-key": declared}
    with pytest.raises(AssertionError) as error:
        processing.declared_values(actual, declared)
    processing.qualification_failure_annotation(error.value)
    payload = annotation(capsys.readouterr().err)
    assert payload["failure_class"] == "assertion"
    assert len(payload["source_frames"]) == 3
    assert all(row["function"] == "declared_values" for row in payload["source_frames"])
    assert all(row["line"] > 0 for row in payload["source_frames"])
    assert "private" not in json.dumps(payload)


@pytest.mark.parametrize("argument", [None, "private-argument"])
def test_invalid_processing_command_preserves_failure_without_copying_argument(
    monkeypatch, capsys, argument
):
    monkeypatch.setattr(sys, "argv", ["qualification"] + ([] if argument is None else [argument]))
    with pytest.raises((IndexError, KeyError)):
        processing.main()
    payload = annotation(capsys.readouterr().err)
    assert payload["failure_class"] == ("unexpected" if argument is None else "key")
    assert [row["function"] for row in payload["source_frames"]] == ["main"]
    assert "private" not in json.dumps(payload)


def test_successful_processing_command_emits_no_failure_annotation(monkeypatch, capsys):
    observed = []
    monkeypatch.setattr(processing, "await_admission", lambda: observed.append("called"))
    monkeypatch.setattr(sys, "argv", ["qualification", "admission"])
    processing.main()
    assert observed == ["called"]
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(7)])
def test_processing_cancellation_is_not_caught_or_reclassified(monkeypatch, capsys, error):
    def interrupted():
        raise error

    monkeypatch.setattr(processing, "await_admission", interrupted)
    monkeypatch.setattr(sys, "argv", ["qualification", "admission"])
    with pytest.raises(BaseException) as observed:
        processing.main()
    assert observed.value is error
    assert capsys.readouterr().err == ""


def test_uncaught_processing_failure_still_fails_the_actual_process():
    code = """
import sys
from scripts import qualify_stove0_processing as processing
def private_callback_name():
    raise RuntimeError("private callback payload")
processing.await_admission = private_callback_name
sys.argv = ["qualification", "admission"]
processing.main()
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 1
    payload = annotation(result.stderr)
    assert payload["failure_class"] == "runtime"
    assert [row["function"] for row in payload["source_frames"]] == ["main"]
    assert "private" not in json.dumps(payload)


def test_failure_reporting_cannot_replace_the_original_processing_failure(monkeypatch, capsys):
    original = RuntimeError("original fixture failure")

    def failed():
        raise original

    def unavailable(_error):
        raise OSError("reporting stream unavailable")

    monkeypatch.setattr(processing, "await_admission", failed)
    monkeypatch.setattr(processing, "qualification_failure_annotation", unavailable)
    monkeypatch.setattr(sys, "argv", ["qualification", "admission"])
    with pytest.raises(RuntimeError) as error:
        processing.main()
    assert error.value is original
    assert capsys.readouterr().err == ""

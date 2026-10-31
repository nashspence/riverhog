from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

SCRIPT = Path(__file__).parents[2] / "scripts/ftp_publication_diagnostics.py"
PREFIX = "::error title=FTP publication qualification::"


def load_script():
    spec = importlib.util.spec_from_file_location("ftp_publication_diagnostics", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "error,category",
    [
        (AssertionError("private response body"), "assertion"),
        (httpx.ReadTimeout("private URL"), "http-timeout"),
        (FileNotFoundError("private path"), "filesystem"),
        (RuntimeError("private value"), "unexpected"),
    ],
)
def test_failure_annotation_preserves_original_hook_without_copying_messages(
    monkeypatch, capsys, error, category
):
    module = load_script()
    observed = []
    monkeypatch.setattr(sys, "excepthook", lambda *args: observed.append(args))
    module.install_publication_diagnostics("flush-result")
    module.publication_count("completed", 0)
    module.publication_count("failed", 1)
    sys.excepthook(type(error), error, None)
    diagnostic = json.loads(capsys.readouterr().err.removeprefix(PREFIX))
    assert diagnostic == {
        "format": "riverhog-ftp-publication-diagnostic/v1",
        "stage": "flush-result",
        "counts": {"completed": 0, "failed": 1},
        "failure_class": category,
    }
    assert observed == [(type(error), error, None)]


def test_http_status_observation_excludes_request_and_response_contents(monkeypatch, capsys):
    module = load_script()
    monkeypatch.setattr(sys, "excepthook", lambda *_args: None)
    module.install_publication_diagnostics("spool-flush")
    request = httpx.Request("POST", "https://private.invalid/secret", headers={"X-Key": "secret"})
    response = httpx.Response(503, request=request, text="private response")
    error = httpx.HTTPStatusError("private error", request=request, response=response)
    sys.excepthook(type(error), error, None)
    diagnostic = json.loads(capsys.readouterr().err.removeprefix(PREFIX))
    assert diagnostic == {
        "format": "riverhog-ftp-publication-diagnostic/v1",
        "stage": "spool-flush",
        "counts": {},
        "failure_class": "http-status",
        "http_status": 503,
    }


def test_uncaught_fixture_failure_still_fails_the_process_and_clears_prior_stage_counts():
    code = (
        SCRIPT.read_text()
        + "\n"
        + "\n".join(
            [
                "install_publication_diagnostics('flush-result')",
                "publication_count('completed', 1)",
                "publication_stage('classification')",
                "raise AssertionError('fixture classification failed')",
            ]
        )
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 1
    annotations = [
        line.removeprefix(PREFIX) for line in result.stderr.splitlines() if line.startswith(PREFIX)
    ]
    assert len(annotations) == 1
    assert json.loads(annotations[0]) == {
        "format": "riverhog-ftp-publication-diagnostic/v1",
        "stage": "classification",
        "counts": {},
        "failure_class": "assertion",
    }


def test_operator_client_error_preserves_http_status_without_message_or_code(monkeypatch, capsys):
    from a_riverhog_ftp_spool_client import FtpSpoolApiError

    module = load_script()
    observed = []
    monkeypatch.setattr(sys, "excepthook", lambda *args: observed.append(args))
    module.install_publication_diagnostics("spool-status", api_error_type=FtpSpoolApiError)
    error = FtpSpoolApiError("private response body", code="private code", status=500)
    sys.excepthook(type(error), error, None)
    diagnostic = json.loads(capsys.readouterr().err.removeprefix(PREFIX))
    assert diagnostic == {
        "format": "riverhog-ftp-publication-diagnostic/v1",
        "stage": "spool-status",
        "counts": {},
        "failure_class": "http-status",
        "http_status": 500,
    }
    assert observed == [(type(error), error, None)]


def test_failed_claim_annotation_fingerprints_without_copying_response_contents(
    monkeypatch, capsys
):
    module = load_script()
    observed = []
    monkeypatch.setattr(sys, "excepthook", lambda *args: observed.append(args))
    module.install_publication_diagnostics("flush-result")
    result = {
        "completed": 0,
        "failed": [
            {"source": "private source", "claim": "private path", "error": "private response body"}
        ],
    }
    error = AssertionError(result)
    sys.excepthook(type(error), error, None)
    encoded = capsys.readouterr().err.removeprefix(PREFIX)
    diagnostic = json.loads(encoded)
    assert diagnostic["claim_error_sha256s"] == [
        hashlib.sha256(b"private response body").hexdigest()
    ]
    assert not any(
        value in encoded for value in ["private source", "private path", "private response"]
    )
    assert observed == [(type(error), error, None)]


@pytest.mark.parametrize("count", [0, 1, 8, 9])
def test_claim_error_fingerprint_observation_is_bounded(monkeypatch, capsys, count):
    module = load_script()
    monkeypatch.setattr(sys, "excepthook", lambda *_args: None)
    module.install_publication_diagnostics("flush-result")
    result = {"failed": [{"error": "x" * 1001} for _ in range(count)]}
    error = AssertionError(result)
    sys.excepthook(type(error), error, None)
    diagnostic = json.loads(capsys.readouterr().err.removeprefix(PREFIX))
    fingerprints = diagnostic.get("claim_error_sha256s", [])
    assert fingerprints == [hashlib.sha256(b"x" * 1000).hexdigest()] * min(count, 8)


@pytest.mark.parametrize(
    "result",
    [
        "private response",
        {"failed": "private response"},
        {"failed": [None, {"error": 123}, {"message": "private response"}]},
    ],
)
def test_malformed_claim_diagnostic_preserves_original_failure(monkeypatch, capsys, result):
    module = load_script()
    observed = []
    monkeypatch.setattr(sys, "excepthook", lambda *args: observed.append(args))
    module.install_publication_diagnostics("flush-result")
    error = AssertionError(result)
    sys.excepthook(type(error), error, None)
    diagnostic = json.loads(capsys.readouterr().err.removeprefix(PREFIX))
    assert "claim_error_sha256s" not in diagnostic
    assert diagnostic["failure_class"] == "assertion"
    assert observed == [(type(error), error, None)]


def test_claim_error_fingerprint_is_scoped_to_failed_flush(monkeypatch, capsys):
    module = load_script()
    monkeypatch.setattr(sys, "excepthook", lambda *_args: None)
    module.install_publication_diagnostics("source-cleanup")
    error = AssertionError({"failed": [{"error": "private response"}]})
    sys.excepthook(type(error), error, None)
    diagnostic = json.loads(capsys.readouterr().err.removeprefix(PREFIX))
    assert "claim_error_sha256s" not in diagnostic

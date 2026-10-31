from __future__ import annotations

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

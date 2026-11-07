from __future__ import annotations

import json
import subprocess
import sys
from types import SimpleNamespace

import httpx
import pytest
from riverhog_protocol.errors import RiverhogError

from scripts import qualify_stove0_lineage as lineage

PREFIX = "::error title=Processing lineage qualification::"


def annotation(output):
    rows = [
        json.loads(line.removeprefix(PREFIX))
        for line in output.splitlines()
        if line.startswith(PREFIX)
    ]
    assert len(rows) == 1
    return rows[0]


def _artifact(size=2, digest="a" * 64):
    return SimpleNamespace(model_dump=lambda **kwargs: {"bytes": str(size), "sha256": digest})


class Client:
    def __init__(self):
        self.members = {
            "1": [
                (_artifact(), ("smoke-0000.wav",)),
                (_artifact(), ("smoke-0000.xmp",)),
            ],
            "2": [
                (_artifact(), ("smoke-0000.opus",)),
                (_artifact(), ("smoke-0000.opus.xmp",)),
                (_artifact(), ("smoke-0000.xmp",)),
            ],
        }
        self.format = "riverhog-collection-derivation/v1"
        self.authority = "a" * 64
        self.page_authority = self.authority
        self.root = 1
        self.pages = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def get_collection(self, collection_id):
        return {"id": str(collection_id)}

    def get_collection_derivation(self, collection_id):
        assert collection_id == "2"
        return {
            "derivation": {
                "format": self.format,
                "input_set_sha256": self.authority,
                "claim": {"id": "b" * 64},
            }
        }

    def list_processing_claim_inputs(self, claim_id, *, identity_sha256, start_ordinal):
        assert claim_id == "b" * 64 and identity_sha256 == self.authority
        self.pages.append(start_ordinal)
        return SimpleNamespace(
            identity=SimpleNamespace(sha256=self.page_authority),
            inputs=[SimpleNamespace(collection_id=self.root)] if start_ordinal == 0 else [],
            next_ordinal=1 if start_ordinal == 0 else None,
        )


@pytest.fixture
def qualified(monkeypatch):
    client = Client()
    monkeypatch.setattr(lineage, "ApiClient", lambda: client)
    monkeypatch.setattr(
        lineage, "root_bound_artifacts", lambda current, identity: current.members[identity]
    )
    for name, value in {
        "INPUT_COLLECTION_ID": "1",
        "OUTPUT_COLLECTION_ID": "2",
        "STOVE0_SMOKE_FILE_COUNT": "1",
        "STOVE0_SMOKE_ELAPSED_NS": "1000000000",
        "STOVE0_SMOKE_PROCESSING_LANE": "processing-e2e",
        "STOVE0_SMOKE_RECIPE_ID": "fixture/v1",
    }.items():
        monkeypatch.setenv(name, value)
    return client


def test_lineage_verifies_paged_original_roots_and_preserves_measurement(qualified, capsys):
    lineage.main()
    streams = capsys.readouterr()
    assert streams.err == ""
    measurement = json.loads(streams.out)
    assert measurement["format"] == "stove0-final-image-scale/v1"
    assert measurement["measurement"] == "ftp-ingress-through-opus-derived-publication"
    assert measurement["input_artifacts"] == 2 and measurement["output_artifacts"] == 3
    assert measurement["elapsed_seconds"] == 1
    assert qualified.pages == [0, 1]


@pytest.mark.parametrize(
    "changed",
    [
        "missing-hint",
        "duplicate-hint",
        "input-count",
        "output-names",
        "sidecar-bytes",
        "sidecar-digest",
        "derivation-format",
        "input-authority",
        "input-root",
    ],
)
def test_lineage_still_refuses_incomplete_or_changed_custody(qualified, capsys, changed):
    if changed == "missing-hint":
        qualified.members["1"][0] = (_artifact(), None)
    elif changed == "duplicate-hint":
        qualified.members["1"].append(qualified.members["1"][0])
    elif changed == "input-count":
        qualified.members["1"].append((_artifact(), ("private-member",)))
    elif changed == "output-names":
        qualified.members["2"].pop()
    elif changed == "sidecar-bytes":
        qualified.members["2"][-1] = (_artifact(size=3), ("smoke-0000.xmp",))
    elif changed == "sidecar-digest":
        qualified.members["2"][-1] = (_artifact(digest="b" * 64), ("smoke-0000.xmp",))
    elif changed == "derivation-format":
        qualified.format = "private-format"
    elif changed == "input-authority":
        qualified.page_authority = "b" * 64
    else:
        qualified.root = 3
    with pytest.raises(AssertionError):
        lineage.main()
    streams = capsys.readouterr()
    assert streams.out == ""
    diagnostic = annotation(streams.err)
    assert diagnostic["failure_class"] == "assertion"
    assert 1 <= len(diagnostic["source_frames"]) <= 3
    assert {row["function"] for row in diagnostic["source_frames"]} <= {
        "main",
        "qualify",
        "readouts",
    }
    assert "private" not in json.dumps(diagnostic)


@pytest.mark.parametrize(
    "error,category",
    [
        (AssertionError("private payload"), "assertion"),
        (TimeoutError("private payload"), "timeout"),
        (httpx.ReadTimeout("private payload"), "timeout"),
        (httpx.ConnectError("private payload"), "transport"),
        (OSError("private payload"), "filesystem"),
        (ValueError("private payload"), "value"),
        (KeyError("private payload"), "key"),
        (TypeError("private payload"), "type"),
        (RuntimeError("private payload"), "runtime"),
        (Exception("private payload"), "unexpected"),
    ],
)
def test_lineage_annotation_excludes_exception_payload_and_type(capsys, error, category):
    lineage.qualification_failure_annotation(error)
    assert annotation(capsys.readouterr().err) == {
        "format": "riverhog-processing-lineage-failure-diagnostic/v1",
        "failure_class": category,
        "source_frames": [],
    }


def test_lineage_annotation_keeps_observed_http_status_without_response(capsys):
    error = RiverhogError("private response", observed_status=503, details={"private": "body"})
    lineage.qualification_failure_annotation(error)
    assert annotation(capsys.readouterr().err) == {
        "format": "riverhog-processing-lineage-failure-diagnostic/v1",
        "failure_class": "http-status",
        "http_status": 503,
        "source_frames": [],
    }


def test_lineage_main_keeps_original_error_and_excludes_imported_callback(monkeypatch, capsys):
    error = RuntimeError("private callback payload")

    def private_callback_name():
        raise error

    monkeypatch.setattr(lineage, "qualify", private_callback_name)
    with pytest.raises(RuntimeError) as observed:
        lineage.main()
    assert observed.value is error
    diagnostic = annotation(capsys.readouterr().err)
    assert diagnostic["failure_class"] == "runtime"
    assert [row["function"] for row in diagnostic["source_frames"]] == ["main"]
    assert "private" not in json.dumps(diagnostic)


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(7)])
def test_lineage_cancellation_is_not_reclassified(monkeypatch, capsys, error):
    def interrupted():
        raise error

    monkeypatch.setattr(lineage, "qualify", interrupted)
    with pytest.raises(BaseException) as observed:
        lineage.main()
    assert observed.value is error
    assert capsys.readouterr().err == ""


def test_lineage_reporting_failure_cannot_replace_original_error(monkeypatch):
    original = RuntimeError("original fixture failure")

    def failed():
        raise original

    def unavailable(error):
        raise OSError("reporting stream unavailable")

    monkeypatch.setattr(lineage, "qualify", failed)
    monkeypatch.setattr(lineage, "qualification_failure_annotation", unavailable)
    with pytest.raises(RuntimeError) as observed:
        lineage.main()
    assert observed.value is original


def test_lineage_failure_still_fails_the_actual_process():
    code = """
from scripts import qualify_stove0_lineage as lineage
def private_callback_name():
    raise RuntimeError("private callback payload")
lineage.qualify = private_callback_name
lineage.main()
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 1
    diagnostic = annotation(result.stderr)
    assert [row["function"] for row in diagnostic["source_frames"]] == ["main"]
    assert "private" not in json.dumps(diagnostic)

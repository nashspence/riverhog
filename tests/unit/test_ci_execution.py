from __future__ import annotations

import copy
import subprocess

import pytest
from riverhog_canonical_json import require_canonical_json

from scripts import ci_execution


def test_actual_checkout_is_recorded_before_and_after_owned_work(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "owned.txt").write_text("actual source")
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(["git", "-C", str(source), "add", "owned.txt"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    sha = ci_execution.source_identity(source)["source_sha"]
    for name, value in {
        "GITHUB_REPOSITORY": "owner/repo",
        "GITHUB_WORKFLOW_REF": "owner/repo/.github/workflows/codeql.yml@refs/heads/main",
        "GITHUB_WORKFLOW_SHA": sha,
        "GITHUB_SHA": sha,
        "GITHUB_RUN_ID": "23",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_JOB": "analyze",
    }.items():
        monkeypatch.setenv(name, str(value))
    output = tmp_path / "checkout.json"
    assert ci_execution.record_checkout("start", "codeql-python", output, root=source) == 0
    started = require_canonical_json(output.read_bytes())
    assert started["state"] == "running" and started["source"]["source_sha"] == sha
    marker = tmp_path / "work"
    marker.write_text("performed exactly once")
    assert ci_execution.record_checkout("finish", "codeql-python", output, root=source) == 0
    completed = require_canonical_json(output.read_bytes())
    assert (
        ci_execution.validate_checkout(
            completed, repository="owner/repo", run_id=23, attempt=2, work="codeql-python"
        )
        == sha
    )
    assert marker.read_text() == "performed exactly once"
    with pytest.raises(ValueError, match="already closed"):
        ci_execution.record_checkout("finish", "codeql-python", output, root=source)
    with pytest.raises(ValueError, match="overwrite"):
        ci_execution.record_checkout("start", "codeql-python", output, root=source)
    (source / "owned.txt").write_text("source changed")
    other = tmp_path / "changed.json"
    assert ci_execution.record_checkout("start", "codeql-python", other, root=source) == 2
    assert ci_execution.record_checkout("finish", "codeql-python", other, root=source) == 2
    with pytest.raises(ValueError):
        ci_execution.validate_checkout(
            require_canonical_json(other.read_bytes()),
            repository="owner/repo",
            run_id=23,
            attempt=2,
            work="codeql-python",
        )


@pytest.mark.parametrize(
    "defect", ["partial", "attempt", "run", "source", "dirty", "work", "clock", "time", "extra"]
)
def test_checkout_consumer_rejects_invalid_and_foreign_evidence(defect):
    from tests.unit.test_ci_actions import checkout_record

    row = copy.deepcopy(checkout_record("python"))
    if defect == "partial":
        row["state"] = "running"
    elif defect == "attempt":
        row["execution"]["run_attempt"] = 1
    elif defect == "run":
        row["execution"]["run_id"] = 24
    elif defect == "source":
        row["source_end"]["source_sha"] = "b" * 40
    elif defect == "dirty":
        row["source"]["source_clean"] = False
    elif defect == "work":
        row["execution"]["work"] = "codeql-actions"
    elif defect == "clock":
        row["elapsed_ns"] = "-1"
    elif defect == "time":
        row["finished_at"] = "2026-10-12T00:00:00Z"
    else:
        row["unexpected"] = True
    with pytest.raises(ValueError):
        ci_execution.validate_checkout(
            row, repository="owner/repo", run_id=23, attempt=2, work="codeql-python"
        )

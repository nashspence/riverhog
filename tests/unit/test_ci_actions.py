from __future__ import annotations

import copy
import hashlib
import subprocess
import sys
import zipfile
from io import BytesIO

import pytest
from riverhog_canonical_json import require_canonical_json

from scripts import ci_actions, ci_qualification, ci_timing
from scripts.validation_evidence import ValidationWork


def run_record():
    return {
        "id": 23,
        "run_attempt": 2,
        "workflow_id": 7,
        "repository": {"id": 10, "full_name": "owner/repo"},
        "head_repository": {"id": 10, "full_name": "owner/repo"},
        "head_sha": "a" * 40,
        "head_branch": "main",
        "event": "push",
        "path": ".github/workflows/codeql.yml",
        "status": "completed",
        "conclusion": "success",
        "run_started_at": "2026-10-13T00:00:00Z",
    }


def job_record(identity=30, start=1, finish=10):
    def clock(seconds):
        return f"2026-10-13T00:00:{seconds:02d}Z"

    return {
        "id": identity,
        "run_id": 23,
        "run_attempt": 2,
        "head_sha": "a" * 40,
        "name": "Analyze (python)",
        "status": "completed",
        "conclusion": "success",
        "started_at": clock(start),
        "completed_at": clock(finish),
        "labels": ["ubuntu-24.04"],
        "runner_name": "actual-runner",
        "steps": [
            {
                "number": 1,
                "name": "Set up job",
                "status": "completed",
                "conclusion": "success",
                "started_at": clock(start),
                "completed_at": clock(start + 1),
            },
            {
                "number": 2,
                "name": "Analyze repository",
                "status": "completed",
                "conclusion": "success",
                "started_at": clock(start + 1),
                "completed_at": clock(finish - 1),
            },
            {
                "number": 4,
                "name": "Post Check out repository",
                "status": "completed",
                "conclusion": "success",
                "started_at": clock(finish - 1),
                "completed_at": clock(finish),
            },
        ],
    }


class Remote:
    repository = "owner/repo"

    def __init__(self, run=None, jobs=None, artifact=None, archive=b""):
        self.run = run or run_record()
        self.jobs = jobs or [job_record()]
        if artifact is not None:
            self.jobs[0]["name"] = "actual fixture"
        self.artifacts = [] if artifact is None else [artifact]
        self.archive = archive
        self.downloads = 0
        self.calls = []

    def api(self, endpoint):
        self.calls.append(endpoint)
        if endpoint == "actions/runs/23":
            return copy.deepcopy(self.run)
        if endpoint == "actions/workflows/7":
            return {"id": 7, "path": self.run["path"]}
        if endpoint.startswith("actions/runs/23/attempts/2/jobs?"):
            return {"total_count": len(self.jobs), "jobs": copy.deepcopy(self.jobs)}
        if endpoint.startswith("actions/runs/23/artifacts?"):
            return {"total_count": len(self.artifacts), "artifacts": copy.deepcopy(self.artifacts)}
        raise AssertionError(endpoint)

    def download_workflow_artifact(self, identity, destination):
        assert identity == 40
        self.downloads += 1
        destination.write_bytes(self.archive)


def zipped(files):
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as stream:
        for path, data in files:
            stream.writestr(path, data)
    return output.getvalue()


def artifact_record(archive):
    return {
        "id": 40,
        "name": "validation-fixture-23-attempt-2",
        "expired": False,
        "digest": "sha256:" + hashlib.sha256(archive).hexdigest(),
        "size_in_bytes": len(archive),
        "workflow_run": {
            "id": 23,
            "head_sha": "a" * 40,
            "head_branch": "main",
            "repository_id": 10,
            "head_repository_id": 10,
        },
    }


@pytest.fixture(scope="module")
def actual_execution(tmp_path_factory):
    root = tmp_path_factory.mktemp("actions-execution")
    source, outside = root / "source", root / "count"
    source.mkdir()
    (source / "owned.txt").write_text("actual fixture source")
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
    work = ValidationWork(
        "fixture",
        (
            sys.executable,
            "-c",
            f"from pathlib import Path; p=Path({str(outside)!r}); p.write_text('executed-once')",
        ),
    )
    receipt = root / "receipt.json"
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ci_qualification, "ROOT", source)
        patch.setattr(ci_qualification, "validation_work", lambda name: work)
        patch.setenv("RIVERHOG_CI_TIMING_DIR", str(root / "timings"))
        for name in ("MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "RIVERHOG_VALIDATION_SCOPE"):
            patch.delenv(name, raising=False)
        assert ci_timing.execute_validation("fixture", receipt, "23-2") == 0
    assert outside.read_text() == "executed-once"
    return work, receipt.read_bytes(), ci_timing.source_identity(source)["source_sha"]


def test_consumer_uses_bound_real_execution_and_never_repeats_it(
    actual_execution, tmp_path, monkeypatch
):
    work, receipt, source_sha = actual_execution
    archive = zipped([("receipt.json", receipt)])
    remote = Remote(artifact=artifact_record(archive), archive=archive)
    monkeypatch.setattr(ci_actions, "ci_execution_work", lambda: {"actual fixture": work})
    result = ci_actions.verify_ci_artifacts(remote, remote.run, source_sha, tmp_path)
    assert result["actual fixture"]["result"]["qualification_exit_status"] == 0
    assert remote.downloads == 1
    assert require_canonical_json(receipt)["elapsed_seconds"] >= 0


@pytest.mark.parametrize(
    "defect", ["missing", "duplicate", "attempt", "source", "owner", "digest", "size", "expired"]
)
def test_missing_stale_forged_or_duplicate_artifacts_fail(
    actual_execution, tmp_path, monkeypatch, defect
):
    work, receipt, source_sha = actual_execution
    archive = zipped([("receipt.json", receipt)])
    artifact = artifact_record(archive)
    remote = Remote(artifact=artifact, archive=archive)
    monkeypatch.setattr(ci_actions, "ci_execution_work", lambda: {"actual fixture": work})
    if defect == "missing":
        remote.artifacts = []
    elif defect == "duplicate":
        remote.artifacts.append(copy.deepcopy(artifact))
    elif defect == "attempt":
        artifact["name"] = "validation-fixture-23-attempt-1"
    elif defect == "source":
        artifact["workflow_run"]["head_sha"] = "b" * 40
    elif defect == "owner":
        artifact["workflow_run"]["repository_id"] = 11
    elif defect == "digest":
        artifact["digest"] = "sha256:" + "b" * 64
    elif defect == "size":
        artifact["size_in_bytes"] += 1
    else:
        artifact["expired"] = True
    with pytest.raises(ValueError):
        ci_actions.verify_ci_artifacts(remote, remote.run, source_sha, tmp_path)


@pytest.mark.parametrize("path", ["../escaped", "/absolute", "a/../../escaped", "a\\b", "C:/path"])
def test_artifact_paths_cannot_escape_the_data_only_destination(tmp_path, path):
    archive = zipped([(path, b"data")])
    remote = Remote(artifact=artifact_record(archive), archive=archive)
    with pytest.raises(ValueError):
        ci_actions.extract_artifact(remote, remote.artifacts[0], remote.run, tmp_path / "contents")


def test_parallel_aggregate_uses_actual_wall_and_includes_setup_and_cleanup():
    jobs = [job_record(30, 1, 20), job_record(31, 5, 15)]
    result = ci_actions.measure_jobs(jobs, run_record())
    assert result["elapsed_seconds"] == 19
    assert sum(row["elapsed_seconds"] for row in result["jobs"]) == 29
    assert result["jobs"][0]["steps"][0]["name"] == "Set up job"
    assert result["jobs"][0]["steps"][-1]["name"].startswith("Post ")
    assert result["timestamp_resolution_seconds"] == 1


@pytest.mark.parametrize(
    "defect", ["attempt", "source", "duplicate", "unfinished", "time", "phase"]
)
def test_aggregate_rejects_partial_foreign_and_contradictory_job_data(defect):
    job = job_record()
    jobs = [job]
    if defect == "attempt":
        job["run_attempt"] = 1
    elif defect == "source":
        job["head_sha"] = "b" * 40
    elif defect == "duplicate":
        jobs.append(copy.deepcopy(job))
    elif defect == "unfinished":
        job["status"] = "in_progress"
    elif defect == "time":
        job["completed_at"] = "2026-10-12T00:00:00Z"
    else:
        job["steps"][0]["completed_at"] = "2026-10-13T00:01:00Z"
    with pytest.raises(ValueError):
        ci_actions.measure_jobs(jobs, run_record())


def test_failed_completed_workflow_remains_failure_with_truthful_elapsed():
    run = run_record()
    run["conclusion"] = "failure"
    result = ci_actions.collect_completed(Remote(run=run), 23, 2)
    assert result["qualification_exit_status"] == 1
    assert result["conclusion"] == "failure"
    assert result["measurement"]["elapsed_seconds"] == 10


def test_matrix_coverage_comes_from_the_existing_execution_owners():
    work = ci_actions.ci_execution_work()
    assert len(work) == 23
    assert len({value.name for value in work.values()}) == 23
    assert work["client platform (windows-2025)"].command[-1] == "native"
    assert work["unit unit"].growth_witnesses


def test_unstarted_and_skipped_jobs_are_preserved_without_fabricated_elapsed():
    job = job_record()
    job.update(conclusion="skipped", started_at=None, completed_at=None, steps=[])
    result = ci_actions.measure_jobs([job], run_record())
    assert result["boundary"] == "no-job-started"
    assert result["elapsed_seconds"] is None
    assert result["jobs"][0]["conclusion"] == "skipped"


def test_job_failure_is_retained_beside_successful_command_evidence(
    actual_execution, tmp_path, monkeypatch
):
    work, receipt, source_sha = actual_execution
    archive = zipped([("receipt.json", receipt)])
    remote = Remote(artifact=artifact_record(archive), archive=archive)
    remote.jobs[0]["conclusion"] = "failure"
    monkeypatch.setattr(ci_actions, "ci_execution_work", lambda: {"actual fixture": work})
    result = ci_actions.verify_ci_artifacts(remote, remote.run, source_sha, tmp_path)
    assert result["actual fixture"]["job_conclusion"] == "failure"
    assert result["actual fixture"]["result"]["qualification_exit_status"] == 0


class SourceRemote(Remote):
    def __init__(self, *, pending=False, failed=False):
        super().__init__()
        ci = copy.deepcopy(self.run)
        ci.update(id=24, workflow_id=8, path=".github/workflows/ci.yml")
        if pending:
            ci.update(status="in_progress", conclusion=None)
        elif failed:
            ci["conclusion"] = "failure"
        self.runs = {23: self.run, 24: ci}

    def api(self, endpoint):
        self.calls.append(endpoint)
        if endpoint.startswith("actions/workflows/"):
            path = endpoint.split("/")[2]
            run = self.runs[24 if path in {"ci.yml", "8"} else 23]
            if "/runs?" in endpoint:
                return {"total_count": 1, "workflow_runs": [copy.deepcopy(run)]}
            return {"id": run["workflow_id"], "path": run["path"]}
        if endpoint in {"actions/runs/23", "actions/runs/24"}:
            return copy.deepcopy(self.runs[int(endpoint.split("/")[-1])])
        if "/attempts/2/jobs?" in endpoint:
            identity = int(endpoint.split("/")[2])
            job = job_record(30 if identity == 23 else 31, 1, 10 if identity == 23 else 20)
            job.update(run_id=identity, name="Analyze (python)" if identity == 23 else "CI gate")
            return {"total_count": 1, "jobs": [job]}
        raise AssertionError(endpoint)


def test_complete_required_source_aggregate_uses_parallel_wall_and_preserves_failures():
    remote = SourceRemote()
    result = ci_actions.collect_required_source(remote, ci_actions.attempt_identity(remote.run))
    assert result["state"] == "completed"
    assert result["elapsed_seconds"] == 20
    assert sum(row["measurement"]["elapsed_seconds"] for row in result["workflows"]) == 30
    assert result["qualification_exit_status"] == 0
    remote = SourceRemote(failed=True)
    result = ci_actions.collect_required_source(remote, ci_actions.attempt_identity(remote.run))
    assert result["qualification_exit_status"] == 1


def test_unfinished_required_workflow_is_pending_not_a_completed_source_check():
    remote = SourceRemote(pending=True)
    result = ci_actions.collect_required_source(remote, ci_actions.attempt_identity(remote.run))
    assert result["state"] == "pending"
    assert result["qualification_exit_status"] is None
    assert len(result["completed_workflows"]) == 1


def test_completed_cli_keeps_failure_from_another_required_workflow(tmp_path, monkeypatch):
    monkeypatch.setattr(
        ci_actions, "GitHubPublication", lambda repository: SourceRemote(failed=True)
    )
    output = tmp_path / "observation.json"
    assert (
        ci_actions.main(
            (
                "completed",
                "--repository",
                "owner/repo",
                "--run-id",
                "23",
                "--attempt",
                "2",
                "--output",
                str(output),
            )
        )
        == 1
    )
    result = require_canonical_json(output.read_bytes())
    assert result["state"] == "completed"
    assert result["result"]["qualification_exit_status"] == 0
    assert result["result"]["required_source_aggregate"]["qualification_exit_status"] == 1

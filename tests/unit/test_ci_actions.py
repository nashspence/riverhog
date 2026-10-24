from __future__ import annotations

import base64
import copy
import hashlib
import json
import subprocess
import sys
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
from riverhog_canonical_json import require_canonical_json

from scripts import ci_actions, ci_make, ci_qualification, ci_timing
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
        "runner_id": 100,
        "runner_name": "actual-runner",
        "runner_group_id": 1,
        "runner_group_name": "Default",
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


def checkout_record(language):
    return {
        "format": "riverhog-actions-checkout/v1",
        "execution": {
            "repository": "owner/repo",
            "workflow_ref": "owner/repo/.github/workflows/codeql.yml@refs/heads/main",
            "workflow_sha": "a" * 40,
            "run_id": 23,
            "run_attempt": 2,
            "work": "codeql-" + language,
            "github_sha": "a" * 40,
        },
        "source": {"source_sha": "a" * 40, "source_clean": True},
        "source_end": {"source_sha": "a" * 40, "source_clean": True},
        "state": "completed",
        "started_at": "2026-10-13T00:00:02Z",
        "finished_at": "2026-10-13T00:00:08Z",
        "started_monotonic_ns": "100",
        "elapsed_ns": "6000000000",
    }


class Remote:
    repository = "owner/repo"

    def __init__(self, run=None, jobs=None, artifact=None, archive=b""):
        self.run = run or run_record()
        default_jobs = [job_record(), job_record(31)]
        default_jobs[1]["name"] = "Analyze (actions)"
        self.jobs = default_jobs if jobs is None else jobs
        self.attempt_jobs = None
        if artifact is not None:
            self.jobs[0]["name"] = "actual fixture"
        self.artifacts = [] if artifact is None else [artifact]
        self.archive = archive
        self.archives = {}
        if artifact is None and self.run["path"] == ".github/workflows/codeql.yml":
            for index, language in enumerate(("actions", "python")):
                record = checkout_record(language)
                data = zipped(
                    [
                        (
                            "checkout.json",
                            json.dumps(record, sort_keys=True, separators=(",", ":")).encode(),
                        )
                    ]
                )
                item = artifact_record(data)
                item.update(id=40 + index, name=f"validation-codeql-{language}-23-attempt-2")
                self.artifacts.append(item)
                self.archives[item["id"]] = data
        self.downloads = 0
        self.calls = []

    def api(self, endpoint):
        self.calls.append(endpoint)
        if endpoint.startswith("contents/"):
            path = endpoint.removeprefix("contents/").split("?")[0]
            raw = (ci_actions.ROOT / path).read_bytes()
            return {
                "type": "file",
                "path": path,
                "encoding": "base64",
                "size": len(raw),
                "content": base64.b64encode(raw).decode(),
                "sha": hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest(),
            }
        if endpoint == "actions/runs/23":
            return copy.deepcopy(self.run)
        if endpoint == "actions/workflows/7":
            return {"id": 7, "path": self.run["path"]}
        if endpoint.startswith("actions/runs/23/jobs?filter=all"):
            return {"total_count": len(self.jobs), "jobs": copy.deepcopy(self.jobs)}
        if endpoint.startswith(f"actions/runs/23/attempts/{self.run['run_attempt']}/jobs?"):
            jobs = self.jobs if self.attempt_jobs is None else self.attempt_jobs
            return {"total_count": len(jobs), "jobs": copy.deepcopy(jobs)}
        if endpoint.startswith("actions/runs/23/artifacts?"):
            return {"total_count": len(self.artifacts), "artifacts": copy.deepcopy(self.artifacts)}
        raise AssertionError(endpoint)

    def download_workflow_artifact(self, identity, destination):
        assert identity in {40, 41}
        self.downloads += 1
        destination.write_bytes(self.archives.get(identity, self.archive))


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


def require_fixture_work(work, names):
    assert tuple(names) == (work.name,)
    return (work,)


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
        patch.setattr(
            ci_qualification,
            "require_validation_work",
            lambda names: require_fixture_work(work, names),
        )
        patch.setenv("RIVERHOG_CI_TIMING_DIR", str(root / "timings"))
        for name in ("MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "RIVERHOG_VALIDATION_SCOPE"):
            patch.delenv(name, raising=False)
        assert ci_timing.execute_validation("fixture", receipt, "23-2") == 0
    assert outside.read_text() == "executed-once"
    return work, receipt.read_bytes(), ci_timing.source_identity(source)["source_sha"]


@pytest.fixture(scope="module")
def actual_make_execution(tmp_path_factory):
    root = tmp_path_factory.mktemp("actions-make-execution")
    source, count, timing = root / "source", root / "count", root / "timings"
    source.mkdir()
    makefile = source / "Makefile"
    makefile.write_text(
        "SHELL := bash\n"
        "ifneq ($(RIVERHOG_VALIDATION_MAKE_DIRECTORY),)\n"
        "SHELL := $(RIVERHOG_VALIDATION_MAKE_SHELL)\n"
        ".SHELLFLAGS := -c\n"
        "export RIVERHOG_VALIDATION_RECIPE_TARGET = $@\n"
        "endif\n"
        "# Maintained target recipes.\n"
        ".PHONY: compile\n"
        "compile:\n"
        f"\t@printf 'executed-once' > {str(count)!r}\n"
    )
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(["git", "-C", str(source), "add", "Makefile"], check=True)
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
    work = ValidationWork("fixture", ("make", "compile"))
    receipt = root / "receipt.json"
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(source)
        patch.setattr(ci_make, "ROOT", source)
        patch.setattr(ci_qualification, "ROOT", source)
        patch.setattr(ci_qualification, "validation_work", lambda name: work)
        patch.setattr(
            ci_qualification,
            "require_validation_work",
            lambda names: require_fixture_work(work, names),
        )
        patch.setattr(
            ci_make, "make_work_definitions", lambda goals, parameters: [work.definition()]
        )
        patch.setenv("RIVERHOG_CI_TIMING_DIR", str(timing))
        patch.setenv("RIVERHOG_CI_RUN_ID", "23-2")
        for name in (
            "MAKEFLAGS",
            "MFLAGS",
            "MAKEOVERRIDES",
            "RIVERHOG_VALIDATION_SCOPE",
            "RIVERHOG_VALIDATION_PARENT_SPAN",
            "RIVERHOG_VALIDATION_ACTIVE_SPAN",
        ):
            patch.delenv(name, raising=False)
        patch.setattr(
            ci_timing,
            "run_validation_child",
            lambda command, *, cwd, env: ci_make.observe_make(makefile, command[1:]),
        )
        assert ci_timing.execute_validation("fixture", receipt, "23-2") == 0
    assert count.read_text() == "executed-once"
    (manifest,) = timing.glob("make/23-2/*/manifest.json")
    return work, receipt.read_bytes(), require_canonical_json(manifest.read_bytes()), source


@pytest.mark.parametrize(
    "case", ["owner", "iteration", "descendant-failure", "foreign", "incomplete", "missing-owner"]
)
def test_ci_consumer_requires_the_qualified_owner_and_its_descendants(
    actual_make_execution, tmp_path, monkeypatch, case
):
    work, receipt, owner, source = actual_make_execution
    files = [("receipt.json", receipt)]
    manifests = [copy.deepcopy(owner)]
    if case not in {"owner", "missing-owner"}:
        child = copy.deepcopy(owner)
        child.update(span="child", parent_span=owner["span"])
        for recipe in child["recipes"]:
            recipe["span"] = "child"
        if case == "iteration":
            child.update(qualification_eligible=False, work_definitions=[], source_sha="b" * 40)
        elif case == "descendant-failure":
            child["exit_status"] = 7
            child["recipes"][0]["exit_status"] = 7
        elif case == "foreign":
            child["source_sha"] = "b" * 40
        else:
            child["state"] = "running"
        manifests.append(child)
    if case == "missing-owner":
        manifests = []
    for manifest in manifests:
        files.append(
            (
                f"make/23-2/{manifest['span']}/manifest.json",
                json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(),
            )
        )
    archive = zipped(files)
    remote = Remote(artifact=artifact_record(archive), archive=archive)
    monkeypatch.setattr(ci_actions, "ROOT", source)
    monkeypatch.setattr(ci_actions, "ci_execution_work", lambda: {"actual fixture": work})
    monkeypatch.setattr(
        ci_make, "make_work_definitions", lambda goals, parameters: [work.definition()]
    )
    if case in {"foreign", "incomplete", "missing-owner"}:
        with pytest.raises(ValueError):
            ci_actions.verify_ci_artifacts(remote, remote.run, owner["source_sha"], tmp_path)
    else:
        result = ci_actions.verify_ci_artifacts(remote, remote.run, owner["source_sha"], tmp_path)
        assert result["actual fixture"]["result"]["qualification_exit_status"] == (
            1 if case == "descendant-failure" else 0
        )
        assert remote.downloads == 1


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


@pytest.mark.parametrize("capacity", ["compressed", "expanded", "exact-expanded"])
def test_artifact_capacity_covers_verified_compressed_and_expanded_bytes(
    tmp_path, monkeypatch, capacity
):
    payload = b"complete-evidence" * 1024
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as stream:
        stream.writestr("evidence.json", payload)
    archive = output.getvalue()
    remote = Remote(artifact=artifact_record(archive), archive=archive)
    budget = len(archive) - 1 if capacity == "compressed" else len(payload)
    if capacity == "expanded":
        budget -= 1
    monkeypatch.setenv("RIVERHOG_VALIDATION_MAX_BYTES", str(budget))
    destination = tmp_path / "contents"
    if capacity == "exact-expanded":
        assert ci_actions.extract_artifact(
            remote, remote.artifacts[0], remote.run, destination
        ) == len(payload)
        assert (destination / "evidence.json").read_bytes() == payload
    else:
        message = "trusted execution" if capacity == "compressed" else "evidence budget"
        with pytest.raises(ValueError, match=message):
            ci_actions.extract_artifact(remote, remote.artifacts[0], remote.run, destination)
        assert not (destination / "evidence.json").exists()
    assert remote.downloads == (0 if capacity == "compressed" else 1)


@pytest.mark.parametrize("budget", ["0", "-1", "", "invalid", "1.5"])
def test_invalid_evidence_capacity_rejects_before_downloading(tmp_path, monkeypatch, budget):
    archive = zipped([("evidence.json", b"complete evidence")])
    remote = Remote(artifact=artifact_record(archive), archive=archive)
    monkeypatch.setenv("RIVERHOG_VALIDATION_MAX_BYTES", budget)
    with pytest.raises(ValueError, match="storage budget.*positive integer"):
        ci_actions.extract_artifact(remote, remote.artifacts[0], remote.run, tmp_path / "contents")
    assert remote.downloads == 0
    assert not (tmp_path / "contents").exists()


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
        if endpoint.startswith("contents/"):
            path = endpoint.removeprefix("contents/").split("?")[0]
            raw = (ci_actions.ROOT / path).read_bytes()
            return {
                "type": "file",
                "path": path,
                "encoding": "base64",
                "size": len(raw),
                "content": base64.b64encode(raw).decode(),
                "sha": hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest(),
            }
        if endpoint.startswith("actions/workflows/"):
            path = endpoint.split("/")[2]
            run = self.runs[24 if path in {"ci.yml", "8"} else 23]
            if "/runs?" in endpoint:
                return {"total_count": 1, "workflow_runs": [copy.deepcopy(run)]}
            return {"id": run["workflow_id"], "path": run["path"]}
        if endpoint.startswith("actions/runs/23/artifacts?"):
            return {"total_count": len(self.artifacts), "artifacts": copy.deepcopy(self.artifacts)}
        if endpoint in {"actions/runs/23", "actions/runs/24"}:
            return copy.deepcopy(self.runs[int(endpoint.split("/")[-1])])
        if "/attempts/2/jobs?" in endpoint or "/jobs?filter=all" in endpoint:
            identity = int(endpoint.split("/")[2])
            job = job_record(30 if identity == 23 else 31, 1, 10 if identity == 23 else 20)
            job.update(run_id=identity, name="Analyze (python)" if identity == 23 else "CI gate")
            jobs = [job]
            if identity == 23:
                actions = copy.deepcopy(job)
                actions.update(id=32, name="Analyze (actions)")
                jobs.append(actions)
            return {"total_count": len(jobs), "jobs": jobs}
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


def test_source_definitions_are_bound_to_the_observed_revision_not_the_observer_checkout(
    tmp_path, monkeypatch
):
    remote = SourceRemote()
    paths, identities = ci_actions.required_workflow_paths(remote, "a" * 40)
    assert paths == ("ci.yml", "codeql.yml")
    assert {row["source_sha"] for row in identities} == {"a" * 40}
    assert all("ref=" + "a" * 40 in call for call in remote.calls if call.startswith("contents/"))
    monkeypatch.setattr(ci_actions, "ROOT", tmp_path)
    # The source adapter reads GitHub data, never the observer's different worktree.
    documents = {
        identity["path"]: (Path(__file__).resolve().parents[2] / identity["path"]).read_bytes()
        for identity in identities
    }
    original_api = remote.api

    def api(endpoint):
        if endpoint.startswith("contents/"):
            path = endpoint.removeprefix("contents/").split("?")[0]
            raw = documents[path]
            return {
                "type": "file",
                "path": path,
                "encoding": "base64",
                "size": len(raw),
                "content": base64.b64encode(raw).decode(),
                "sha": hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest(),
            }
        return original_api(endpoint)

    monkeypatch.setattr(remote, "api", api)
    assert ci_actions.required_workflow_paths(remote, "a" * 40)[0] == paths


def test_ci_gate_rejects_artifacts_from_another_workflow_before_consuming_them(monkeypatch):
    remote = Remote()
    monkeypatch.setattr(
        ci_actions, "source_identity", lambda root: {"source_sha": "a" * 40, "source_clean": True}
    )
    with pytest.raises(ValueError, match="maintained CI workflow"):
        ci_actions.collect_gate(remote, 23, 2)
    assert remote.downloads == 0


def test_native_work_cannot_be_qualified_under_another_platform(actual_execution):
    from scripts.validation_evidence import validate_evidence_records

    work, raw, sha = actual_execution
    row = require_canonical_json(raw)
    wrong = ValidationWork(work.name, work.command, conditions=(("runner_image", "windows-2025"),))
    row["definition"] = wrong.definition()
    row["observed_conditions"]["platform"] = "linux"
    with pytest.raises(ValueError, match="required work conditions"):
        validate_evidence_records([row], expected=[wrong], source_sha=sha, run_id="23-2")


@pytest.mark.parametrize("snapshot", ["original", "carried", "chain"])
def test_partial_retry_consumes_the_original_completed_receipt_without_reexecution(
    actual_execution, tmp_path, monkeypatch, snapshot
):
    work, raw, sha = actual_execution
    row = require_canonical_json(raw)
    row["run_id"] = "23-1"
    archive = zipped(
        [("receipt.json", json.dumps(row, sort_keys=True, separators=(",", ":")).encode())]
    )
    artifact = artifact_record(archive)
    artifact["name"] = "validation-fixture-23-attempt-1"
    remote = Remote(artifact=artifact, archive=archive)
    remote.jobs[0]["run_attempt"] = 1
    remote.jobs = remote.jobs[:1]
    remote.run["run_started_at"] = "2026-10-13T00:00:20Z"
    if snapshot != "original":
        carried = copy.deepcopy(remote.jobs[0])
        carried.update(id=32, run_attempt=2)
        remote.jobs.append(carried)
        for item in (carried, *carried["steps"]):
            for key in ("started_at", "completed_at"):
                item[key] = item[key].replace("Z", "+00:00")
    if snapshot == "chain":
        remote.run["run_attempt"] = 3
        carried = copy.deepcopy(remote.jobs[-1])
        carried.update(id=33, run_attempt=3)
        remote.jobs.append(carried)
    monkeypatch.setattr(ci_actions, "ci_execution_work", lambda: {"actual fixture": work})
    result = ci_actions.verify_ci_artifacts(remote, remote.run, sha, tmp_path)
    assert result["actual fixture"]["origin_attempt"] == 1
    assert result["actual fixture"]["reused_completion"] is True
    assert result["actual fixture"]["result"]["qualification_exit_status"] == 0
    assert remote.downloads == 1
    assert result["actual fixture"]["job_measurement"]["jobs"][0]["job_id"] == 30


def test_retry_does_not_reuse_an_old_success_when_the_new_execution_failed():
    remote = Remote()
    old = copy.deepcopy(remote.jobs[0])
    old.update(id=20, run_attempt=1, conclusion="success")
    remote.jobs[0]["conclusion"] = "failure"
    remote.jobs.append(old)
    chosen = ci_actions.latest_execution_jobs(remote, remote.run)
    owner = next(row for row in chosen if row["name"] == "Analyze (python)")
    assert owner["run_attempt"] == 2 and owner["conclusion"] == "failure"


def retry_remote(*, include_inherited=True):
    run = run_record()
    run.update(path=".github/workflows/ci.yml", run_started_at="2026-10-13T00:00:20Z")
    original = job_record(20)
    original["run_attempt"] = 1
    original["runner_group_id"] = 0
    original["steps"].append(
        {"number": 3, "name": "Optional work", "status": "completed", "conclusion": "skipped"}
    )
    carried = copy.deepcopy(original)
    carried.update(id=30, run_attempt=2, runner_group_id=None)
    for row in (carried, *carried["steps"]):
        for key in ("started_at", "completed_at"):
            if key in row:
                row[key] = row[key].replace("Z", "+00:00")
    gate = job_record(31, 21, 30)
    gate["name"] = "CI gate"
    remote = Remote(run=run, jobs=[original, carried, gate])
    remote.attempt_jobs = [carried, gate] if include_inherited else [gate]
    return remote


@pytest.mark.parametrize("include_inherited", [False, True])
def test_retry_measures_fresh_work_separately_from_retained_completions(include_inherited):
    remote = retry_remote(include_inherited=include_inherited)
    result = ci_actions.collect_completed(remote, 23, 2)
    assert result["qualification_exit_status"] == 0
    assert result["measurement"]["elapsed_seconds"] == 10
    assert [row["job_id"] for row in result["measurement"]["jobs"]] == [31]
    (retained,) = result["reused_dependency_completions"]
    assert retained["origin_attempt"] == 1
    assert retained["measurement"]["elapsed_seconds"] == 9
    assert retained["measurement"]["jobs"][0]["job_id"] == 20


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "failed",
        "runner",
        "runner-id",
        "runner-group-id",
        "runner-group-name",
        "labels",
        "step-name",
        "step-result",
        "time",
        "ambiguous",
    ],
)
def test_inherited_snapshot_requires_unique_matching_original_completion(defect):
    remote = retry_remote()
    original = remote.jobs[0]
    if defect == "missing":
        remote.jobs.pop(0)
    elif defect == "failed":
        original["conclusion"] = "failure"
    elif defect == "runner":
        original["runner_name"] = "another-runner"
    elif defect == "runner-id":
        original["runner_id"] = 101
    elif defect == "runner-group-id":
        original["runner_group_id"] = 2
    elif defect == "runner-group-name":
        original["runner_group_name"] = "Another group"
    elif defect == "labels":
        original["labels"] = ["another-image"]
    elif defect == "step-name":
        original["steps"][1]["name"] = "Different work"
    elif defect == "step-result":
        original["steps"][1]["conclusion"] = "failure"
    elif defect == "time":
        original["steps"][1]["started_at"] = "2026-10-13T00:00:03Z"
    else:
        duplicate = copy.deepcopy(original)
        duplicate["id"] = 21
        remote.jobs.append(duplicate)
    with pytest.raises(ValueError, match="original execution|ambiguous dependency"):
        ci_actions.latest_execution_jobs(remote, remote.run)


@pytest.mark.parametrize("conclusion", ["success", "failure"])
def test_fresh_retry_execution_keeps_its_own_identity_and_result(conclusion):
    remote = retry_remote()
    fresh = job_record(30, 21, 29)
    fresh["conclusion"] = conclusion
    remote.jobs[1] = fresh
    remote.attempt_jobs[0] = fresh
    remote.run["conclusion"] = conclusion
    chosen = ci_actions.latest_execution_jobs(remote, remote.run)
    assert chosen[0]["id"] == 30 and chosen[0]["run_attempt"] == 2
    result = ci_actions.collect_completed(remote, 23, 2)
    assert result["qualification_exit_status"] == int(conclusion != "success")
    assert result["reused_dependency_completions"] == []
    assert {row["job_id"] for row in result["measurement"]["jobs"]} == {30, 31}


@pytest.mark.parametrize(
    "defect", ["missing", "unknown", "runner", "duplicate", "source", "attempt"]
)
def test_current_attempt_must_agree_with_complete_execution_history(defect):
    remote = retry_remote()
    remote.attempt_jobs = copy.deepcopy(remote.attempt_jobs)
    if defect == "missing":
        remote.attempt_jobs.pop()
    elif defect == "unknown":
        remote.attempt_jobs[-1]["id"] = 99
    elif defect == "runner":
        remote.attempt_jobs[0]["runner_name"] = "another-runner"
    elif defect == "duplicate":
        remote.attempt_jobs.append(copy.deepcopy(remote.attempt_jobs[-1]))
    elif defect == "source":
        remote.attempt_jobs[0]["head_sha"] = "b" * 40
    else:
        remote.attempt_jobs[0]["run_attempt"] = 1
    with pytest.raises(ValueError):
        ci_actions.collect_completed(remote, 23, 2)


def test_retained_snapshot_chain_resolves_original_when_a_later_retry_omits_it():
    remote = retry_remote(include_inherited=False)
    remote.run.update(run_attempt=3, run_started_at="2026-10-13T00:00:40Z")
    gate = job_record(32, 41, 50)
    gate.update(name="CI gate", run_attempt=3)
    remote.jobs.append(gate)
    remote.attempt_jobs = [gate]
    result = ci_actions.collect_completed(remote, 23, 3)
    (retained,) = result["reused_dependency_completions"]
    assert retained["origin_attempt"] == 1
    assert retained["measurement"]["jobs"][0]["job_id"] == 20
    assert result["measurement"]["elapsed_seconds"] == 10


def test_codeql_retry_binds_retained_checkout_to_its_actual_origin():
    run = run_record()
    run["run_started_at"] = "2026-10-13T00:00:20Z"
    remote = Remote(run=run)
    fresh = job_record(30, 21, 29)
    original = job_record(21)
    original.update(name="Analyze (actions)", run_attempt=1)
    carried = copy.deepcopy(original)
    carried.update(id=31, run_attempt=2)
    remote.jobs = [original, carried, fresh]
    remote.attempt_jobs = [carried, fresh]
    checkout = checkout_record("actions")
    checkout["execution"]["run_attempt"] = 1
    archive = zipped(
        [("checkout.json", json.dumps(checkout, sort_keys=True, separators=(",", ":")).encode())]
    )
    artifact = artifact_record(archive)
    artifact["name"] = "validation-codeql-actions-23-attempt-1"
    remote.artifacts[0] = artifact
    remote.archives[40] = archive
    result = ci_actions.collect_completed(remote, 23, 2)
    assert result["qualification_exit_status"] == 0
    records = result["source_evidence"]["records"]
    assert records["Analyze (actions)"]["origin_attempt"] == 1
    assert records["Analyze (actions)"]["reused_completion"] is True
    assert records["Analyze (python)"]["origin_attempt"] == 2
    assert records["Analyze (python)"]["reused_completion"] is False
    assert [row["job_id"] for row in result["measurement"]["jobs"]] == [30]

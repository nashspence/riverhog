"""Bind Actions evidence to a trusted workflow revision, run, attempt and artifact."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from .github_publication import GitHubPublication
from .model import ContractAtlasError, canonical_bytes
from .publication import DEFAULT_SITE_BUDGET, file_sha256, safe_relative

WORKFLOWS = {
    "qualification": "release-qualification.yml",
    "preparation": "release-preparation.yml",
}


def execution_record(kind: str, source_sha: str, version: str) -> dict[str, Any]:
    workflow = ".github/workflows/" + WORKFLOWS[kind]
    repository = os.environ["GITHUB_REPOSITORY"]
    authority = os.environ["GITHUB_WORKFLOW_SHA"]
    ref = repository + "/" + workflow + "@refs/heads/main"
    event = os.environ["GITHUB_EVENT_NAME"]
    if (
        os.environ.get("GITHUB_WORKFLOW_REF") != ref
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
        or event
        not in (
            {"workflow_dispatch", "schedule"} if kind == "qualification" else {"workflow_dispatch"}
        )
        or not re.fullmatch(r"[0-9a-f]{40}", authority)
        or not re.fullmatch(r"[0-9a-f]{40}", source_sha)
    ):
        raise ContractAtlasError("evidence must be produced by the trusted main workflow")
    return {
        "format": "riverhog-workflow-execution/v1",
        "repository": repository,
        "workflow": workflow,
        "workflow_ref": ref,
        "workflow_sha": authority,
        "event": event,
        "run_id": int(os.environ["GITHUB_RUN_ID"]),
        "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
        "source_sha": source_sha,
        "version": version,
    }


def trusted_run(
    remote: GitHubPublication, run_id: int, kind: str, authority_sha: str
) -> dict[str, Any]:
    """Workflow authority is the approved main revision, separate from tested source."""

    run = remote.api(f"actions/runs/{run_id}")
    if not isinstance(run, dict):
        raise ContractAtlasError("selected workflow run must be an object")
    workflow = remote.api("actions/workflows/" + WORKFLOWS[kind])
    path = ".github/workflows/" + WORKFLOWS[kind]
    repository = run.get("repository", {})
    head = run.get("head_repository", {})
    if (
        run.get("id") != run_id
        or run.get("workflow_id") != workflow.get("id")
        or workflow.get("path") != path
        or run.get("path") not in {path, path + "@main", path + "@refs/heads/main"}
        or repository.get("full_name") != remote.repository
        or head.get("full_name") != remote.repository
        or not repository.get("id")
        or repository.get("id") != head.get("id")
        or run.get("head_branch") != "main"
        or run.get("head_sha") != authority_sha
        or run.get("event")
        not in (
            {"workflow_dispatch", "schedule"} if kind == "qualification" else {"workflow_dispatch"}
        )
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
        or not isinstance(run.get("run_attempt"), int)
        or run["run_attempt"] < 1
    ):
        raise ContractAtlasError(f"selected {kind} run is not trusted successful main evidence")
    return run


def collect_workflow_artifact(
    remote: GitHubPublication,
    run_id: int,
    kind: str,
    source_sha: str,
    version: str,
    destination: Path,
    *,
    authority_sha: str | None = None,
    budget: int = DEFAULT_SITE_BUDGET,
) -> dict[str, Any]:
    authority = authority_sha or remote.api("git/ref/heads/main")["object"]["sha"]
    run = trusted_run(remote, run_id, kind, authority)
    name = f"release-{kind}-{source_sha}-attempt-{run['run_attempt']}"
    rows = remote.api(f"actions/runs/{run_id}/artifacts?name={name}&per_page=100")
    artifacts = [item for item in rows["artifacts"] if item.get("name") == name]
    if rows.get("total_count") != 1 or len(artifacts) != 1:
        raise ContractAtlasError("trusted workflow attempt must have one exact artifact")
    artifact = artifacts[0]
    owner = artifact.get("workflow_run", {})
    if (
        artifact.get("expired") is not False
        or not isinstance(artifact.get("id"), int)
        or owner.get("id") != run_id
        or owner.get("repository_id") != run["repository"]["id"]
        or owner.get("head_repository_id") != run["repository"]["id"]
        or owner.get("head_branch") != "main"
        or owner.get("head_sha") != authority
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(artifact.get("digest")))
        or not isinstance(artifact.get("size_in_bytes"), int)
        or not 0 < artifact["size_in_bytes"] <= budget
    ):
        raise ContractAtlasError("workflow artifact lacks exact trusted run identity")
    if destination.exists():
        raise ContractAtlasError("workflow artifact destination must not exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".workflow-evidence-", dir=destination.parent) as temp:
        archive, stage = Path(temp) / "artifact.zip", Path(temp) / "contents"
        remote.download_workflow_artifact(artifact["id"], archive)
        if (
            archive.stat().st_size != artifact["size_in_bytes"]
            or "sha256:" + file_sha256(archive) != artifact["digest"]
        ):
            raise ContractAtlasError("downloaded workflow artifact identity differs")
        stage.mkdir()
        seen: set[str] = set()
        expanded = 0
        with zipfile.ZipFile(archive) as stream:
            for item in stream.infolist():
                relative = safe_relative(
                    item.filename.rstrip("/") if item.is_dir() else item.filename
                )
                mode = (item.external_attr >> 16) & 0o170000
                if relative in seen or mode not in {0, 0o040000, 0o100000}:
                    raise ContractAtlasError(
                        "workflow artifact repeats a path or contains a special file"
                    )
                seen.add(relative)
                expanded += item.file_size
                if expanded > budget:
                    raise ContractAtlasError(
                        "workflow artifact expansion exceeds its operational budget"
                    )
                target = stage / relative
                if item.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with stream.open(item) as source, target.open("xb") as output:
                        shutil.copyfileobj(source, output, length=1_048_576)
        raw = (stage / "execution.json").read_bytes()
        record = json.loads(raw)
        expected = {
            "format": "riverhog-workflow-execution/v1",
            "repository": remote.repository,
            "workflow": ".github/workflows/" + WORKFLOWS[kind],
            "workflow_ref": remote.repository
            + "/.github/workflows/"
            + WORKFLOWS[kind]
            + "@refs/heads/main",
            "workflow_sha": authority,
            "event": run["event"],
            "run_id": run_id,
            "run_attempt": run["run_attempt"],
            "source_sha": source_sha,
            "version": version,
        }
        if raw != canonical_bytes(record) or record != expected:
            raise ContractAtlasError(
                "workflow artifact execution differs from trusted run/source/attempt"
            )
        if trusted_run(remote, run_id, kind, authority) != run:
            raise ContractAtlasError("workflow attempt changed during evidence collection")
        stage.rename(destination)
    return {
        "execution": expected,
        "artifact_id": artifact["id"],
        "artifact_sha256": artifact["digest"].removeprefix("sha256:"),
    }


def source_workflow_checks(
    remote: GitHubPublication, source_sha: str, branch: str, workflows: tuple[str, ...]
) -> bool:
    """Select the latest trusted push run and its current attempt, never global check names."""

    ready = True
    for name in workflows:
        workflow = remote.api("actions/workflows/" + name)
        rows = remote.api(
            f"actions/workflows/{name}/runs?head_sha={source_sha}&branch={branch}&event=push&per_page=100"
        )["workflow_runs"]
        matching = [
            run
            for run in rows
            if run.get("event") == "push"
            and run.get("head_sha") == source_sha
            and run.get("head_branch") == branch
            and run.get("workflow_id") == workflow["id"]
            and run.get("path")
            in {
                workflow["path"],
                workflow["path"] + "@refs/heads/" + branch,
                workflow["path"] + "@" + branch,
            }
            and run.get("repository", {}).get("full_name") == remote.repository
            and run.get("head_repository", {}).get("full_name") == remote.repository
            and run["repository"].get("id") == run["head_repository"].get("id")
        ]
        if not matching:
            ready = False
            continue
        run = max(matching, key=lambda item: item["id"])
        selected = remote.api(f"actions/runs/{run['id']}")
        if any(
            selected.get(key) != run.get(key)
            for key in (
                "head_sha",
                "head_branch",
                "workflow_id",
                "run_attempt",
                "check_suite_id",
                "event",
                "repository",
                "head_repository",
            )
        ):
            raise ContractAtlasError("selected source workflow attempt changed")
        if selected.get("status") != "completed" or selected.get("conclusion") != "success":
            ready = False
            continue
        if name == "codeql.yml":
            checks = remote.api(
                f"check-suites/{selected['check_suite_id']}/check-runs?per_page=100"
            )
            for context in ("Analyze (actions)", "Analyze (python)"):
                owned = [
                    item
                    for item in checks["check_runs"]
                    if item.get("name") == context and item.get("app", {}).get("id") == 15368
                ]
                if (
                    len(owned) != 1
                    or owned[0].get("status") != "completed"
                    or owned[0].get("conclusion") != "success"
                ):
                    ready = False
        if remote.api(f"actions/runs/{run['id']}") != selected:
            raise ContractAtlasError("source workflow attempt changed during readiness check")
    return ready

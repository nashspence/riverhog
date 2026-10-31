"""Consume exact-attempt Actions artifacts and measure actual job/step boundaries."""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import re
import subprocess
import sys
import time
import tomllib
import zipfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlencode

import yaml
from riverhog_canonical_json import require_canonical_json

sys.path.insert(0, str(Path(__file__).resolve().parent))

from contract_atlas.github_publication import GitHubPublication
from contract_atlas.model import ContractAtlasError
from contract_atlas.publication import file_sha256

if TYPE_CHECKING:
    from ci_execution import validate_checkout
    from ci_qualification import (
        COMPOSE_LANES,
        IMAGE_GROUPS,
        REPOSITORY_TARGETS,
        ROOT,
        validation_workloads,
    )
    from ci_timing import source_identity, write_record
else:
    from scripts.ci_execution import validate_checkout
    from scripts.ci_qualification import (
        COMPOSE_LANES,
        IMAGE_GROUPS,
        REPOSITORY_TARGETS,
        ROOT,
        validation_workloads,
    )
    from scripts.ci_timing import source_identity, write_record

from scripts.ci_make import owned_execution_spans, validate_make_receipt
from scripts.ci_unit import CI_SHARDS, UNIT_PARTITIONS
from scripts.validation_evidence import ValidationWork, runner_platform, validate_evidence_records
from scripts.validation_resources import validation_storage_budget
from scripts.validation_witnesses import collect_test_witnesses

MAX_ROWS = 512
MAX_FILES = 4096
TIMESTAMP_RESOLUTION_SECONDS = 1


def ci_execution_work() -> dict[str, ValidationWork]:
    """Project the actual matrix owners and their maintained workload definitions."""
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    cases: dict[str, ValidationWork] = {}
    definitions = {work.name: work for work in validation_workloads()}
    for owner, parameter, values, prefix in (
        ("repository", "target", REPOSITORY_TARGETS, ""),
        ("units", "shard", CI_SHARDS, "unit-"),
        ("compose", "lane", COMPOSE_LANES, "compose-"),
        ("images", "group", IMAGE_GROUPS, "image-"),
        (
            "client-platforms",
            "os",
            jobs["client-platforms"]["strategy"]["matrix"]["os"],
            "native-",
        ),
    ):
        template = jobs[owner]["name"]
        placeholder = "${{ matrix." + parameter + " }}"
        if template.count(placeholder) != 1:
            raise ValueError("matrix display identity does not match its execution owner")
        for value in values:
            name = template.replace(placeholder, value)
            if name in cases:
                raise ValueError("duplicate matrix execution identity")
            cases[name] = definitions[prefix + value]
    unit_witnesses = [
        identity
        for part in UNIT_PARTITIONS
        for identity in cases[f"unit unit-{part}"].growth_witnesses
    ]
    if len(unit_witnesses) != len(set(unit_witnesses)) or set(unit_witnesses) != set(
        definitions["unit-unit"].growth_witnesses
    ):
        raise ValueError("CI unit partitions omit or duplicate required growth witnesses")
    if set(jobs["gate"]["needs"]) != set(jobs) - {"gate"}:
        raise ValueError("CI gate omits a maintained execution owner")
    if set(jobs) != {
        "plan",
        "gate",
        "repository",
        "units",
        "compose",
        "images",
        "client-platforms",
    }:
        raise ValueError("CI execution owner has no required-evidence adapter")
    return cases


def bounded_rows(remote: GitHubPublication, endpoint: str, key: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for page in range(1, MAX_ROWS // 100 + 2):
        separator = "&" if "?" in endpoint else "?"
        response = remote.api(endpoint + separator + f"per_page=100&page={page}")
        rows = response[key]
        if (
            not isinstance(rows, list)
            or any(not isinstance(row, dict) for row in rows)
            or type(response["total_count"]) is not int
            or response["total_count"] > MAX_ROWS
        ):
            raise ValueError("Actions evidence exceeds its bounded collection budget")
        result.extend(rows)
        if len(result) == response["total_count"]:
            return result
        if not rows or len(result) > response["total_count"]:
            break
    raise ValueError("Actions evidence pagination is incomplete")


def _execution_jobs(
    remote: GitHubPublication, run: Mapping[str, Any]
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Pair each latest API snapshot with its proven actual execution origin."""
    rows = bounded_rows(remote, f"actions/runs/{run['id']}/jobs?filter=all", "jobs")
    grouped: dict[str, list[dict[str, Any]]] = {}
    seen: set[int] = set()
    for row in rows:
        origin = row.get("run_attempt")
        identity, name = row.get("id"), row.get("name")
        if (
            type(origin) is not int
            or not 1 <= origin <= run["run_attempt"]
            or type(identity) is not int
            or identity in seen
            or not isinstance(name, str)
            or not name
            or row.get("run_id") != run["id"]
            or row.get("head_sha") != run["head_sha"]
        ):
            raise ValueError("execution history has duplicate or foreign job provenance")
        seen.add(identity)
        grouped.setdefault(name, []).append(row)
    result = []
    for candidates in grouped.values():
        if len({row["run_attempt"] for row in candidates}) != len(candidates):
            raise ValueError("execution history has ambiguous dependency ownership")
        newest = max(row["run_attempt"] for row in candidates)
        selected = next(row for row in candidates if row["run_attempt"] == newest)
        original = selected
        if selected.get("status") == "completed" and selected.get("conclusion") == "success":
            completion = _job_completion(selected, run)
            start = timestamp(run["run_started_at"])
            # Actions may relabel an inherited completion with a new ID and attempt.
            # An actual current execution must start within the current attempt.
            current = newest == run["run_attempt"]
            if current and timestamp(selected["started_at"]) >= start:
                result.append((selected, original))
                continue
            matches = [
                row
                for row in candidates
                if row["run_attempt"] < newest
                and row.get("status") == "completed"
                and row.get("conclusion") == "success"
                and _job_completion(row, run) == completion
            ]
            if current and (timestamp(selected["completed_at"]) >= start or not matches):
                raise ValueError("inherited completion lacks an exact original execution")
            if matches:
                original = min(matches, key=lambda row: row["run_attempt"])
        result.append((selected, original))
    return result


def latest_execution_jobs(
    remote: GitHubPublication, run: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Use the most recent actual execution of each dependency, retaining its origin."""
    return [original for _, original in _execution_jobs(remote, run)]


def trusted_attempt(remote: GitHubPublication, run_id: int, attempt: int) -> dict[str, Any]:
    if type(run_id) is not int or run_id < 1 or type(attempt) is not int or attempt < 1:
        raise ValueError("Actions run and attempt must be positive integers")
    run = remote.api(f"actions/runs/{run_id}")
    repository = run.get("repository", {})
    head = run.get("head_repository", {})
    if (
        type(run_id) is not int
        or run_id < 1
        or type(attempt) is not int
        or attempt < 1
        or run.get("id") != run_id
        or run.get("run_attempt") != attempt
        or repository.get("full_name") != remote.repository
        or type(repository.get("id")) is not int
        or repository["id"] < 1
        or type(head.get("id")) is not int
        or head["id"] < 1
        or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", str(head.get("full_name"))) is None
        or (repository["id"] != head["id"] and run.get("event") != "pull_request")
        or re.fullmatch(r"[0-9a-f]{40}", str(run.get("head_sha"))) is None
    ):
        raise ValueError("Actions attempt lacks exact repository/source identity")
    return cast(dict[str, Any], run)


def attempt_identity(run: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: run[key]
        for key in (
            "id",
            "run_attempt",
            "head_sha",
            "head_branch",
            "event",
            "path",
            "workflow_id",
            "repository",
            "head_repository",
        )
    }


def extract_artifact(
    remote: GitHubPublication,
    artifact: Mapping[str, Any],
    run: Mapping[str, Any],
    destination: Path,
) -> int:
    budget = validation_storage_budget()
    owner = artifact.get("workflow_run", {})
    size = artifact.get("size_in_bytes")
    if (
        artifact.get("expired") is not False
        or type(artifact.get("id")) is not int
        or type(size) is not int
        or not 0 < size <= budget
        or owner.get("id") != run["id"]
        or owner.get("head_sha") != run["head_sha"]
        or owner.get("head_branch") != run["head_branch"]
        or owner.get("repository_id") != run["repository"]["id"]
        or owner.get("head_repository_id") != run["head_repository"]["id"]
        or re.fullmatch(r"sha256:[0-9a-f]{64}", str(artifact.get("digest"))) is None
    ):
        raise ValueError("artifact is not owned by the selected trusted execution")
    archive = destination.with_suffix(".zip")
    remote.download_workflow_artifact(artifact["id"], archive)
    if archive.stat().st_size != size or ("sha256:" + file_sha256(archive) != artifact["digest"]):
        raise ValueError("artifact bytes differ from GitHub's exact digest")
    destination.mkdir()
    expanded = 0
    seen: set[str] = set()
    with zipfile.ZipFile(archive) as stream:
        members = stream.infolist()
        if len(members) > MAX_FILES:
            raise ValueError("artifact has too many entries")
        for item in members:
            relative = PurePosixPath(item.filename.rstrip("/"))
            mode = (item.external_attr >> 16) & 0o170000
            if (
                not relative.parts
                or relative.is_absolute()
                or any(part in {".", ".."} for part in item.filename.split("/"))
                or "\\" in item.filename
                or ":" in item.filename
                or str(relative) in seen
                or mode not in {0, 0o040000, 0o100000}
            ):
                raise ValueError("artifact has an unsafe, repeated or special path")
            seen.add(str(relative))
            expanded += item.file_size
            if expanded > budget:
                raise ValueError("artifact expansion exceeds the evidence budget")
            path = destination / relative
            if item.is_dir():
                path.mkdir(parents=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                with stream.open(item) as source, path.open("xb") as target:
                    while data := source.read(65536):
                        target.write(data)
    return expanded


def verify_ci_artifacts(
    remote: GitHubPublication, run: Mapping[str, Any], source_sha: str, directory: Path
) -> dict[str, object]:
    """Consume actual owner artifacts, never an unbound pass string in a summary."""
    budget = validation_storage_budget()
    cases = ci_execution_work()
    artifacts = bounded_rows(remote, f"actions/runs/{run['id']}/artifacts", "artifacts")
    attempt = run["run_attempt"]
    jobs = latest_execution_jobs(remote, run)
    results: dict[str, object] = {}
    total = 0
    expanded_total = 0
    for job, work in cases.items():
        owned_jobs = [
            row
            for row in jobs
            if row.get("name") == job or str(row.get("name")).endswith(" / " + job)
        ]
        if len(owned_jobs) != 1:
            raise ValueError("required execution job is missing or duplicated: " + job)
        origin = owned_jobs[0]["run_attempt"]
        origin_run = {**run, "run_attempt": origin}
        run_identity = f"{run['id']}-{origin}"
        job_measurement = measure_jobs(owned_jobs, origin_run)
        runner = dict(work.conditions).get("runner_image")
        if runner is not None and runner not in owned_jobs[0]["labels"]:
            raise ValueError("execution ran under a different runner condition")
        name = f"validation-{work.name}-{run['id']}-attempt-{origin}"
        matches = [row for row in artifacts if row.get("name") == name]
        if len(matches) != 1:
            raise ValueError("required execution artifact is missing or duplicated: " + work.name)
        artifact = matches[0]
        size = artifact.get("size_in_bytes")
        if type(size) is not int:
            raise ValueError("artifact size is missing")
        total += size
        if total > budget:
            raise ValueError("complete execution artifacts exceed the collection budget")
        owned = directory / work.name
        expanded_total += extract_artifact(remote, artifact, run, owned)
        if expanded_total > budget:
            raise ValueError("complete expanded artifacts exceed the collection budget")
        receipt = require_canonical_json((owned / "receipt.json").read_bytes())
        report = validate_evidence_records(
            [receipt], expected=[work], source_sha=source_sha, run_id=run_identity
        )
        assert isinstance(receipt, dict)
        if runner is not None:
            expected_platform = runner_platform(runner)
            conditions = receipt["observed_conditions"]
            if not isinstance(conditions, dict) or conditions.get("platform") != expected_platform:
                raise ValueError("native evidence contradicts the actual platform condition")
        if work.command[0] == "make":
            manifests = [
                require_canonical_json(path.read_bytes())
                for path in (owned / "make" / run_identity).glob("*/manifest.json")
            ]
            roots = [
                row for row in manifests if isinstance(row, dict) and row.get("parent_span") is None
            ]
            if len(roots) != 1 or roots[0].get("work_definitions") != [work.definition()]:
                raise ValueError("required command has no exact owning Make execution")
            spans = owned_execution_spans(owned, run_identity, str(roots[0]["span"]), source_sha)
            for row in manifests:
                if isinstance(row, dict) and row.get("span") not in spans:
                    continue
                if not isinstance(row, dict):
                    raise ValueError("invalid owned Make execution")
                goals, parameters = row["goals"], row["parameters"]
                if (
                    not isinstance(goals, list)
                    or any(not isinstance(value, str) for value in goals)
                    or not isinstance(parameters, dict)
                    or any(not isinstance(value, str) for value in parameters.values())
                ):
                    raise ValueError("invalid owned Make selectors")
                make_report = validate_make_receipt(
                    row,
                    makefile=ROOT / "Makefile",
                    goals=cast(list[str], goals),
                    parameters=cast(dict[str, str], parameters),
                    source={"source_sha": source_sha, "source_clean": True},
                    run_id=run_identity,
                    require_qualification=True,
                )
                report["qualification_exit_status"] = (
                    report["qualification_exit_status"] or make_report["qualification_exit_status"]
                )
        if work.growth_witnesses:
            actual = collect_test_witnesses(
                tuple((owned / "pytest").glob(f"*/{run_identity}/*/*.json")),
                root=ROOT,
                expected=work.growth_witnesses,
                source_sha=source_sha,
                run_id=run_identity,
            )
            if actual != receipt["growth_results"]:
                raise ValueError("summary contradicts actual owner evidence")
        results[job] = {
            "work": work.definition(),
            "receipt": receipt,
            "result": report,
            "origin_attempt": origin,
            "reused_completion": origin != attempt,
            "job_measurement": job_measurement,
            "job_conclusion": owned_jobs[0]["conclusion"],
            "artifact_id": artifact["id"],
            "artifact_sha256": artifact["digest"],
        }
    return results


def timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("missing execution timestamp")
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError("execution timestamp has no timezone")
    return result


def measure_jobs(jobs: Sequence[Mapping[str, Any]], run: Mapping[str, Any]) -> dict[str, object]:
    """Job and step costs include setup and post-action cleanup at GitHub's resolution."""
    seen: set[int] = set()
    measurements = []
    starts, finishes = [], []
    for job in jobs:
        identity = job.get("id")
        if (
            type(identity) is not int
            or identity in seen
            or job.get("run_id") != run["id"]
            or job.get("run_attempt") != run["run_attempt"]
            or job.get("head_sha") != run["head_sha"]
            or job.get("status") != "completed"
        ):
            raise ValueError("job is duplicated, incomplete or from another execution")
        seen.add(identity)
        if job.get("conclusion") == "skipped" or (
            job.get("conclusion") == "cancelled" and job.get("started_at") is None
        ):
            measurements.append(
                {
                    "job_id": identity,
                    "name": job["name"],
                    "conclusion": job["conclusion"],
                    "started_at": None,
                    "finished_at": None,
                    "elapsed_seconds": None,
                    "runner_labels": job["labels"],
                    "runner_name": job["runner_name"],
                    "steps": [],
                }
            )
            continue
        start, finish = timestamp(job.get("started_at")), timestamp(job.get("completed_at"))
        if finish < start:
            raise ValueError("job completion predates its start")
        starts.append(start)
        finishes.append(finish)
        steps = []
        numbers: set[int] = set()
        for step in job["steps"]:
            number = step.get("number")
            if type(number) is not int or number in numbers or step.get("status") != "completed":
                raise ValueError("step is duplicated or incomplete")
            numbers.add(number)
            # GitHub's skipped steps sometimes have no clock. They did no measured work.
            seconds = None
            if step.get("conclusion") != "skipped":
                left, right = timestamp(step.get("started_at")), timestamp(step.get("completed_at"))
                # Independent job and step snapshots can straddle one reported second.
                # Retain both clocks verbatim; only the containment comparison is tolerant.
                resolution = timedelta(seconds=TIMESTAMP_RESOLUTION_SECONDS)
                if not start - resolution <= left <= right <= finish + resolution:
                    raise ValueError("step is outside its owning job's completion boundary")
                seconds = (right - left).total_seconds()
            steps.append(
                {
                    "number": number,
                    "name": step["name"],
                    "conclusion": step["conclusion"],
                    "started_at": step.get("started_at"),
                    "finished_at": step.get("completed_at"),
                    "elapsed_seconds": seconds,
                }
            )
        measurements.append(
            {
                "job_id": identity,
                "name": job["name"],
                "conclusion": job["conclusion"],
                "started_at": start.isoformat(),
                "finished_at": finish.isoformat(),
                "elapsed_seconds": (finish - start).total_seconds(),
                "runner_labels": job["labels"],
                "runner_name": job["runner_name"],
                "steps": steps,
            }
        )
    if not starts:
        return {
            "boundary": "no-job-started",
            "timestamp_resolution_seconds": TIMESTAMP_RESOLUTION_SECONDS,
            "timestamp_boundary_tolerance_seconds": TIMESTAMP_RESOLUTION_SECONDS,
            "started_at": None,
            "finished_at": None,
            "elapsed_seconds": None,
            "jobs": measurements,
        }
    return {
        "boundary": "first-job-start-through-last-job-cleanup-including-between-job-gaps",
        "timestamp_resolution_seconds": TIMESTAMP_RESOLUTION_SECONDS,
        "timestamp_boundary_tolerance_seconds": TIMESTAMP_RESOLUTION_SECONDS,
        "started_at": min(starts).isoformat(),
        "finished_at": max(finishes).isoformat(),
        "elapsed_seconds": (max(finishes) - min(starts)).total_seconds(),
        "jobs": measurements,
    }


def _job_completion(job: Mapping[str, Any], run: Mapping[str, Any]) -> dict[str, Any]:
    """Compare complete measured work, ignoring only reassigned snapshot identity."""
    measurement = measure_jobs([job], {**run, "run_attempt": job["run_attempt"]})
    result = cast(list[dict[str, Any]], measurement["jobs"])[0]
    del result["job_id"]
    for key in ("runner_id", "runner_group_id", "runner_group_name"):
        result[key] = job.get(key)
    # Actions represents an unassigned runner group as zero or null in snapshots.
    group = result["runner_group_id"]
    if group is not None and (type(group) is not int or group < 0):
        raise ValueError("completion has an invalid runner group identity")
    result["runner_group_id"] = group or None
    for row in (result, *result["steps"]):
        for key in ("started_at", "finished_at"):
            if row[key] is not None:
                row[key] = timestamp(row[key]).astimezone(UTC).isoformat()
    return result


def collect_gate(remote: GitHubPublication, run_id: int, attempt: int) -> dict[str, object]:
    source = source_identity(ROOT)
    if source["source_clean"] is not True:
        raise ValueError("gate source must be exact and clean")
    run = trusted_attempt(remote, run_id, attempt)
    owners = {".github/workflows/ci.yml"}
    for path in (ROOT / ".github/workflows").glob("*.yml"):
        workflow = yaml.safe_load(path.read_text())
        if any(
            job.get("uses") == "./.github/workflows/ci.yml" for job in workflow["jobs"].values()
        ):
            owners.add(path.relative_to(ROOT).as_posix())
    if run["path"] not in owners:
        raise ValueError("execution artifacts are not owned by a maintained CI workflow")
    with TemporaryDirectory(prefix="riverhog-ci-evidence-") as temporary:
        results = verify_ci_artifacts(remote, run, str(source["source_sha"]), Path(temporary))
    after = trusted_attempt(remote, run_id, attempt)
    if attempt_identity(after) != attempt_identity(run) or source_identity(ROOT) != source:
        raise ValueError("execution identity changed during consumption")
    statuses = []
    for row in results.values():
        if not isinstance(row, dict) or not isinstance(row.get("result"), dict):
            raise ValueError("missing consumed qualification result")
        statuses.append(
            row["result"]["qualification_exit_status"] != 0 or row["job_conclusion"] != "success"
        )
    failed = any(statuses)
    return {
        "format": "riverhog-actions-validation-gate/v1",
        "execution": attempt_identity(run),
        "source_sha": source["source_sha"],
        "evidence_complete": True,
        "qualification_exit_status": int(failed),
        "work": results,
    }


def collect_codeql_checkout(
    remote: GitHubPublication, run: Mapping[str, Any], jobs: Sequence[Mapping[str, Any]]
) -> dict[str, object]:
    budget = validation_storage_budget()
    text, document = source_document(remote, run["head_sha"], run["path"])
    workflow = yaml.safe_load(text)
    job_definition = workflow["jobs"]["analyze"]
    languages = job_definition["strategy"]["matrix"]["language"]
    if not languages or len(set(languages)) != len(languages):
        raise ValueError("CodeQL source matrix is empty or duplicated")
    artifacts = bounded_rows(remote, f"actions/runs/{run['id']}/artifacts", "artifacts")
    records = {}
    total = 0
    with TemporaryDirectory(prefix="riverhog-codeql-source-") as temporary:
        for language in languages:
            context = job_definition["name"].replace("${{ matrix.language }}", language)
            owned = [job for job in jobs if job.get("name") == context]
            if len(owned) != 1:
                raise ValueError("CodeQL execution owner is missing or duplicated")
            work = "codeql-" + language
            origin = owned[0]["run_attempt"]
            name = f"validation-{work}-{run['id']}-attempt-{origin}"
            matches = [row for row in artifacts if row.get("name") == name]
            if len(matches) != 1:
                raise ValueError("CodeQL checkout artifact is missing or duplicated")
            artifact = matches[0]
            if type(artifact.get("size_in_bytes")) is not int:
                raise ValueError("CodeQL checkout artifact has no bounded size")
            total += artifact["size_in_bytes"]
            if total > budget:
                raise ValueError("CodeQL checkout artifacts exceed the collection budget")
            destination = Path(temporary) / work
            extract_artifact(remote, artifact, run, destination)
            row = require_canonical_json((destination / "checkout.json").read_bytes())
            source = validate_checkout(
                row,
                repository=remote.repository,
                run_id=run["id"],
                attempt=origin,
                work=work,
            )
            if source != run["head_sha"]:
                if run["event"] != "pull_request":
                    raise ValueError("CodeQL checkout differs from its exact non-PR source")
                commit = remote.api("git/commits/" + source)
                if commit.get("sha") != source or run["head_sha"] not in {
                    parent["sha"] for parent in commit["parents"]
                }:
                    raise ValueError("CodeQL merge checkout is not bound to the measured PR head")
            records[context] = {
                "checkout": row,
                "origin_attempt": origin,
                "reused_completion": origin != run["run_attempt"],
                "source_sha": source,
                "artifact_id": artifact["id"],
                "artifact_sha256": artifact["digest"],
            }
    if len({row["source_sha"] for row in records.values()}) != 1:
        raise ValueError("CodeQL matrix analyzed different checkouts")
    return {
        "state": "verified",
        "source_sha": next(iter(records.values()))["source_sha"],
        "workflow_document": document,
        "records": records,
    }


def collect_completed(remote: GitHubPublication, run_id: int, attempt: int) -> dict[str, object]:
    run = trusted_attempt(remote, run_id, attempt)
    if run["status"] != "completed" or run["path"] not in {
        ".github/workflows/ci.yml",
        ".github/workflows/codeql.yml",
    }:
        raise ValueError("observer requires a completed required-check workflow")
    workflow = remote.api(f"actions/workflows/{run['workflow_id']}")
    if workflow["path"] != run["path"] or workflow["id"] != run["workflow_id"]:
        raise ValueError("workflow identity differs from the selected run")
    jobs = bounded_rows(remote, f"actions/runs/{run_id}/attempts/{attempt}/jobs", "jobs")
    measure_jobs(jobs, run)
    executions = _execution_jobs(remote, run)
    snapshots = {snapshot["id"]: (snapshot, original) for snapshot, original in executions}
    current = []
    for job in jobs:
        pair = snapshots.get(job["id"])
        if pair is None or _job_completion(job, run) != _job_completion(pair[0], run):
            raise ValueError("current attempt contradicts its execution history")
        if pair[1]["run_attempt"] == attempt:
            current.append(pair[1])
    if {job["id"] for job in current} != {
        original["id"] for _, original in executions if original["run_attempt"] == attempt
    }:
        raise ValueError("current attempt omits an actual execution")
    measurement = measure_jobs(current, run)
    dependencies = [original for _, original in executions]
    reused = []
    for job in dependencies:
        if job["run_attempt"] != attempt:
            reused.append(
                {
                    "origin_attempt": job["run_attempt"],
                    "measurement": measure_jobs([job], {**run, "run_attempt": job["run_attempt"]}),
                }
            )
    # The whole workflow includes initial scheduling/preparation and between-job gaps.
    if measurement["finished_at"] is not None:
        start = timestamp(run["run_started_at"])
        finish = timestamp(measurement["finished_at"])
        if finish < start or start > timestamp(measurement["started_at"]):
            raise ValueError("workflow start contradicts its actual job boundaries")
        measurement.update(
            boundary="workflow-start-through-last-job-cleanup-including-scheduling-gaps",
            started_at=start.isoformat(),
            elapsed_seconds=(finish - start).total_seconds(),
        )
    if trusted_attempt(remote, run_id, attempt) != run:
        raise ValueError("completed attempt changed during observation")
    result: dict[str, object] = {
        "format": "riverhog-actions-complete-run/v1",
        "execution": attempt_identity(run),
        "conclusion": run["conclusion"],
        "measurement": measurement,
        "reused_dependency_completions": reused,
        "functional_exit_status": int(run["conclusion"] != "success"),
        "qualification_exit_status": int(run["conclusion"] != "success"),
    }
    if run["path"] == ".github/workflows/codeql.yml":
        try:
            result["source_evidence"] = collect_codeql_checkout(remote, run, dependencies)
        except (
            ValueError,
            OSError,
            KeyError,
            TypeError,
            zipfile.BadZipFile,
            yaml.YAMLError,
            subprocess.CalledProcessError,
            ContractAtlasError,
        ):
            result["source_evidence"] = {"state": "invalid", "reason": "required-checkout-evidence"}
            result["qualification_exit_status"] = result["qualification_exit_status"] or 2
    return result


def source_document(
    remote: GitHubPublication, source_sha: str, path: str
) -> tuple[str, dict[str, str]]:
    """Read bounded data from an exact Git revision without executing its source."""
    if re.fullmatch(r"[0-9a-f]{40}", source_sha) is None:
        raise ValueError("source document requires an exact commit")
    row = remote.api("contents/" + path + "?" + urlencode({"ref": source_sha}))
    if (
        row.get("type") != "file"
        or row.get("path") != path
        or row.get("encoding") != "base64"
        or type(row.get("size")) is not int
        or not 0 < row["size"] <= 1024 * 1024
        or not isinstance(row.get("content"), str)
        or re.fullmatch(r"[0-9a-f]{40}", str(row.get("sha"))) is None
    ):
        raise ValueError("source document lacks bounded exact-file identity")
    content = row["content"]
    if len(content) > 2 * 1024 * 1024:
        raise ValueError("encoded source document exceeds the collection budget")
    raw = base64.b64decode("".join(content.splitlines()), validate=True)
    if (
        len(raw) != row["size"]
        or hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest() != row["sha"]
    ):
        raise ValueError("source document bytes differ from their Git blob identity")
    return raw.decode(), {"path": path, "source_sha": source_sha, "git_blob_sha": row["sha"]}


def required_workflow_paths(
    remote: GitHubPublication, source_sha: str
) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    """Account for mandatory contexts declared by the actual measured source."""
    documents = []
    text, identity = source_document(remote, source_sha, "release.toml")
    configuration = tomllib.loads(text)["governance"]
    documents.append(identity)
    text, identity = source_document(remote, source_sha, ".github/workflows/ci.yml")
    ci = yaml.safe_load(text)
    documents.append(identity)
    text, identity = source_document(remote, source_sha, ".github/workflows/codeql.yml")
    codeql = yaml.safe_load(text)
    documents.append(identity)
    analyze = codeql["jobs"]["analyze"]
    contexts = {ci["jobs"]["gate"]["name"]} | {
        analyze["name"].replace("${{ matrix.language }}", language)
        for language in analyze["strategy"]["matrix"]["language"]
    }
    if contexts != set(configuration["required_checks"]):
        raise ValueError("mandatory check has no complete-workflow measurement adapter")
    return ("ci.yml", "codeql.yml"), documents


def collect_required_source(
    remote: GitHubPublication, execution: Mapping[str, Any]
) -> dict[str, object]:
    """Latest exact-source push attempts form the maintained source check-in aggregate."""
    if execution["event"] != "push":
        return {"state": "not-applicable", "boundary": "required-push-checks-at-one-source"}
    measurements = []
    pending: list[dict[str, object]] = []
    paths, documents = required_workflow_paths(remote, execution["head_sha"])
    for path in paths:
        workflow = remote.api("actions/workflows/" + path)
        query = urlencode(
            {
                "head_sha": execution["head_sha"],
                "branch": execution["head_branch"],
                "event": "push",
            }
        )
        rows = bounded_rows(remote, "actions/workflows/" + path + "/runs?" + query, "workflow_runs")
        matching = [
            row
            for row in rows
            if row.get("workflow_id") == workflow["id"]
            and row.get("head_sha") == execution["head_sha"]
            and row.get("head_branch") == execution["head_branch"]
            and row.get("event") == "push"
            and row.get("repository", {}).get("full_name") == remote.repository
        ]
        if not matching:
            pending.append({"workflow": path, "state": "not-started"})
            continue
        latest = max(matching, key=lambda row: row["id"])
        run = trusted_attempt(remote, latest["id"], latest["run_attempt"])
        if (
            run["head_sha"] != execution["head_sha"]
            or run["event"] != "push"
            or run["workflow_id"] != workflow["id"]
            or run["head_branch"] != execution["head_branch"]
            or run["path"] != workflow["path"]
        ):
            raise ValueError("required source workflow selection changed")
        if run["status"] != "completed":
            pending.append(
                {"workflow": path, "state": run["status"], "execution": attempt_identity(run)}
            )
            continue
        measurements.append(collect_completed(remote, run["id"], run["run_attempt"]))
    if pending:
        return {
            "state": "pending",
            "source_sha": execution["head_sha"],
            "pending": pending,
            "completed_workflows": measurements,
            "source_documents": documents,
            "qualification_exit_status": None,
        }
    spans = [row["measurement"] for row in measurements]
    starts = [
        timestamp(row["started_at"])
        for row in spans
        if isinstance(row, dict) and row["started_at"] is not None
    ]
    finishes = [
        timestamp(row["finished_at"])
        for row in spans
        if isinstance(row, dict) and row["finished_at"] is not None
    ]
    return {
        "state": "completed",
        "source_sha": execution["head_sha"],
        "workflows": measurements,
        "source_documents": documents,
        "boundary": "first-required-workflow-start-through-last-required-job-cleanup",
        "started_at": min(starts).isoformat() if starts else None,
        "finished_at": max(finishes).isoformat() if finishes else None,
        "elapsed_seconds": (max(finishes) - min(starts)).total_seconds() if starts else None,
        "qualification_exit_status": int(
            any(row["qualification_exit_status"] != 0 for row in measurements)
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("gate", "completed"))
    parser.add_argument("--run-id", type=int, default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--attempt", type=int, default=os.environ.get("GITHUB_RUN_ATTEMPT"))
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    started = time.monotonic()
    record: dict[str, object] = {
        "format": "riverhog-actions-evidence-collection/v1",
        "state": "running",
        "run_id": args.run_id,
        "run_attempt": args.attempt,
        "repository": args.repository,
        "collector_source": source_identity(ROOT),
        "started_at": datetime.now(UTC).isoformat(),
    }
    if args.output.exists():
        parser.error("evidence output must not overwrite another attempt")
    write_record(args.output, record)
    status = 2
    try:
        remote = GitHubPublication(args.repository)
        record["result"] = (
            collect_gate(remote, args.run_id, args.attempt)
            if args.command == "gate"
            else collect_completed(remote, args.run_id, args.attempt)
        )
        record["state"] = "completed"
        result = record["result"]
        assert isinstance(result, dict)
        if args.command == "completed":
            result["required_source_aggregate"] = collect_required_source(
                remote, result["execution"]
            )
        status = result["qualification_exit_status"]
        assert isinstance(status, int)
        if args.command == "completed":
            aggregate = result["required_source_aggregate"]
            assert isinstance(aggregate, dict)
            if aggregate["state"] == "completed":
                status = status or int(aggregate["qualification_exit_status"])
    except KeyboardInterrupt:
        record["state"] = "interrupted"
        status = 130
    except (
        ValueError,
        OSError,
        KeyError,
        TypeError,
        zipfile.BadZipFile,
        yaml.YAMLError,
        subprocess.CalledProcessError,
        ContractAtlasError,
    ):
        record["state"] = "evidence-failed"
        print("Actions validation evidence is invalid or incomplete.")
    finally:
        record.update(
            elapsed_seconds=time.monotonic() - started,
            finished_at=datetime.now(UTC).isoformat(),
            exit_status=status,
        )
        write_record(args.output, record)
    return status


if __name__ == "__main__":
    raise SystemExit(main())

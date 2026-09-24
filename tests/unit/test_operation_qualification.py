from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from tests import operation_observer

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/operation_qualification.py"


def load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("operation_qualification", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_generated_operation_matrix_is_complete_and_fail_closed() -> None:
    module = load_script()

    matrix = module.operation_matrix()

    identities = {(item.application, item.operation_id, item.method, item.path) for item in matrix}
    assert len(identities) == len(matrix)
    assert {item.application for item in matrix} == {
        "riverhog",
        "a-riverhog-ftp-spool",
        "stove0",
    }
    assert {item.classification for item in matrix} == {
        "human-cli+json",
        "client-only-primitive",
        "standard-tool/protocol",
    }
    assert {item.response_authority for item in matrix} == {
        "canonical-document",
        "http-json",
        "operator-projection",
        "stream-or-empty",
    }
    assert all(
        item.client is not None
        for item in matrix
        if item.classification in {"human-cli+json", "client-only-primitive"}
    )
    assert all(item.cli_commands for item in matrix if item.classification == "human-cli+json")


def test_operation_audiences_distinguish_commands_wires_and_protocols() -> None:
    module = load_script()
    by_identity = {
        (item.application, item.operation_id): item for item in module.operation_matrix()
    }

    assert by_identity[("riverhog", "list_collections")].classification == "human-cli+json"
    assert "collection list" in by_identity[("riverhog", "list_collections")].cli_commands
    assert (
        by_identity[("riverhog", "put_collection_upload_session_unit")].classification
        == "client-only-primitive"
    )
    assert (
        by_identity[("riverhog", "get_portable_collection_inventory")].classification
        == "standard-tool/protocol"
    )
    assert (
        by_identity[("riverhog", "head_retrieval_file")].classification == "standard-tool/protocol"
    )
    assert by_identity[("stove0", "list_work")].classification == "human-cli+json"
    assert "work list" in by_identity[("stove0", "list_work")].cli_commands
    assert (
        by_identity[("a-riverhog-ftp-spool", "get_ftp_spool_status")].classification
        == "human-cli+json"
    )


@pytest.fixture
def observed_operation_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[ModuleType, dict[str, Any], Path]:
    module = load_script()
    source_sha = "a" * 40
    source_state = {"head": source_sha, "clean": True}
    monkeypatch.setattr(module.qualification_source, "checkout_state", lambda: source_state)
    monkeypatch.setattr(
        module,
        "_cold_cli_timings",
        lambda: {"riverhog": {"trials": 3, "median_ms": 1.0}},
    )

    matrix = module.operation_matrix()
    timings = tmp_path / "timings.json"
    timing_rows = []
    for item in matrix:
        row = {
            "application": item.application,
            "operation_id": item.operation_id,
            "server_wall": {
                "samples": 1,
                "minimum_ms": 1.0,
                "median_ms": 1.0,
                "maximum_ms": 1.0,
            },
        }
        if item.classification in {"human-cli+json", "client-only-primitive"}:
            row["client_wall"] = {
                "samples": 1,
                "minimum_ms": 2.0,
                "median_ms": 2.0,
                "maximum_ms": 2.0,
            }
        timing_rows.append(row)
    timings.write_text(
        json.dumps(
            {
                "format": "riverhog-operation-timings/v1",
                "source_sha": source_sha,
                "source_checkout": {"start": source_state, "finish": source_state},
                "pytest_exit_status": 0,
                "operations": timing_rows,
            }
        )
    )

    return module, module.evidence(source_sha=source_sha, timings=timings), timings


def test_exact_sha_evidence_contains_only_generated_current_rows(
    observed_operation_evidence: tuple[ModuleType, dict[str, Any], Path],
    monkeypatch,
) -> None:
    module, payload, timings = observed_operation_evidence
    source_sha = payload["source_sha"]
    matrix = module.operation_matrix()
    operations = payload["operations"]

    assert payload["format"] == "riverhog-operation-qualification/v1"
    assert payload["source_sha"] == source_sha
    assert payload["summary"]["operations"] == len(operations)
    assert payload["summary"]["matrix_sha256"] == module._matrix_sha256(module.operation_matrix())
    assert payload["provider_evidence"]["issue"] == 442
    assert payload["provider_evidence"]["required_for"]
    assert payload["performance"]["cold_cli_startup"]["riverhog"]["median_ms"] == 1.0
    assert payload["performance"]["local_api"]["operations"]
    assert payload["qualification"]["positive_local_lifecycles"]["status"] == "not_established"
    assert payload["qualification"]["positive_local_lifecycles"][
        "operations_with_successful_responses"
    ] == len(payload["performance"]["local_api"]["operations"])
    assert payload["qualification"]["positive_local_lifecycles"][
        "locally_required_operations"
    ] == sum(item.provider_evidence is None for item in matrix)
    assert len(payload["performance"]["local_api"]["operations"]) > sum(
        item.provider_evidence is None for item in matrix
    )
    extent = payload["qualification"]["extent_contract"]
    assert extent["status"] == "passed"
    assert extent["format"] == "riverhog-contract-machine-closure/v1"
    assert extent["extent_format"] == "riverhog-extent-contract/v1"
    assert extent["extent_decisions"] > 0
    assert len(extent["projection_sha256"]) == 64
    assert len(extent["extent_sha256"]) == 64
    assert payload["qualification"]["cli_human_json_projection"]["status"] == "not_established"
    assert payload["qualification"]["bounded_state_access"]["status"] == "not_established"
    assert payload["qualification"]["event_cursor_restart_resume"]["status"] == "not_established"
    assert all(set(item) == set(module.Operation.__dataclass_fields__) for item in operations)

    output = timings.parent / "operations.json"
    monkeypatch.setattr(module, "evidence", lambda **kwargs: payload)
    assert (
        module.main(
            [
                "evidence",
                "--source-sha",
                source_sha,
                "--timings",
                str(timings),
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert json.loads(output.read_text()) == json.loads(json.dumps(payload))
    markdown = output.with_name(output.name + ".md").read_text()
    assert markdown == module.evidence_markdown(payload)
    assert "Required witness did not pass in this run." in markdown
    assert "**not established**" in markdown

    incomplete = json.loads(timings.read_text())
    required_clients = {
        (item.application, item.operation_id)
        for item in matrix
        if item.provider_evidence is None
        and item.classification in {"human-cli+json", "client-only-primitive"}
    }
    client_row = next(
        item
        for item in incomplete["operations"]
        if (item["application"], item["operation_id"]) in required_clients
    )
    del client_row["client_wall"]
    timings.write_text(json.dumps(incomplete))
    with pytest.raises(module.QualificationError, match="lack client wall timings"):
        module._load_operation_timings(timings, source_sha=source_sha, matrix=matrix)


def _release_qualification_step(name: str) -> str:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github/workflows/release-qualification.yml").read_text()
    )
    return next(
        step["run"] for step in workflow["jobs"]["release-audit"]["steps"] if step["name"] == name
    )


def test_release_disposable_selection_satisfies_current_observation_requirements(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = load_script()
    source_sha = module.qualification_source.checkout_state()["head"]
    timings = tmp_path / "timings.json"
    selected = subprocess.run(
        [
            "bash",
            "--noprofile",
            "--norc",
            "-e",
            "-o",
            "pipefail",
            "-c",
            _release_qualification_step(
                "Exercise disposable operation lifecycles and record timings"
            ),
        ],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "RIVERHOG_OPERATION_SOURCE_SHA": source_sha,
            "RIVERHOG_OPERATION_TIMINGS": str(timings),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert selected.returncode == 0, selected.stdout + selected.stderr

    # Timing observations and passed restart assertions are separate inputs.
    matrix = module.operation_matrix()
    observations = json.loads(timings.read_text())
    assert observations["operations"]
    surfaces = module.application_surfaces()
    monkeypatch.setattr(module, "application_surfaces", lambda: surfaces)
    witnesses = observations["event_cursor_restarts"]
    claim = module._event_cursor_restart_claim(witnesses, matrix=matrix, source_sha=source_sha)
    assert claim["status"] == "not_established"
    local = claim["local_api_process_restart"]
    assert local["status"] == "passed"
    assert {(item["application"], item["operation_id"]) for item in local["operations"]} == {
        ("riverhog", "list_lifecycle_events"),
        ("stove0", "list_events"),
    }
    assert all(item["status"] == "passed" for item in local["operations"])
    assert all(f"/blob/{source_sha}/" in item["assertion_source"] for item in local["operations"])
    if all(
        module.qualification_source.matches_source(state, source_sha)
        for state in observations["source_checkout"].values()
    ):
        # A clean committed run exercises the actual accepted producer path.
        payload = module.evidence(source_sha=source_sha, timings=timings)
        assert payload["qualification"]["event_cursor_restart_resume"] == claim
        markdown = module.evidence_markdown(payload)
        for item in local["operations"]:
            assert (
                f"[Restart assertions ({item['application']})]({item['assertion_source']})"
                in markdown
            )
        assert local["scope"] in markdown
        assert local["limitations"] in markdown
        assert "| event cursor restart resume | **not established** |" in markdown
        assert "Checkout verified clean at test start and finish" in markdown
    else:
        # Development still exercises the same tests and records observations,
        # but these cannot become qualification evidence for the committed SHA.
        with pytest.raises(module.QualificationError, match="clean checkout"):
            module._load_operation_timings(timings, source_sha=source_sha, matrix=matrix)
        with pytest.raises(module.QualificationError, match="clean checkout"):
            module.evidence(source_sha=source_sha, timings=timings)

    for missing in ([], witnesses[:1]):
        partial = module._event_cursor_restart_claim(missing, matrix=matrix, source_sha=source_sha)
        assert partial["local_api_process_restart"]["status"] == "not_established"
    invalid_witnesses = [None, [*witnesses, witnesses[0]]]
    for field, value in (
        ("application", "a-riverhog-ftp-spool"),
        ("operation_id", "unknown"),
        ("test_nodeid", "tests/unit/test_operation_lifecycle_api.py::unrelated_test"),
    ):
        invalid = deepcopy(witnesses)
        invalid[0][field] = value
        invalid_witnesses.append(invalid)
    for invalid in invalid_witnesses:
        with pytest.raises(module.QualificationError, match="restart witness"):
            module._event_cursor_restart_claim(invalid, matrix=matrix, source_sha=source_sha)

    # Discovery must leave a newly exposed feed unqualified until its assertions run.
    riverhog = next(surface for surface in surfaces if surface.name == "riverhog")
    event_route = next(
        route for route, path in module._application_routes(riverhog.app) if path == "/v1/events"
    )
    riverhog.app.add_api_route(
        "/v1/additional-events",
        lambda: {},
        operation_id="additional_events",
        response_model=event_route.response_model,
        openapi_extra=event_route.openapi_extra,
    )
    riverhog.app.openapi_schema = None
    existing = next(item for item in matrix if item.operation_id == "list_lifecycle_events")
    expanded = (
        *matrix,
        replace(existing, operation_id="additional_events", path="/v1/additional-events"),
    )
    missing_new_feed = module._event_cursor_restart_claim(
        witnesses,
        matrix=expanded,
        source_sha=source_sha,
    )["local_api_process_restart"]
    assert missing_new_feed["status"] == "not_established"
    assert (
        next(
            item
            for item in missing_new_feed["operations"]
            if item["operation_id"] == "additional_events"
        )["status"]
        == "not_established"
    )


@pytest.mark.parametrize(
    ("outcome", "exit_status", "recorded"),
    [
        ("pass", 0, True),
        ("fail", 1, False),
        ("skip", 0, False),
        ("xfail", 0, False),
        ("xpass", 0, False),
        ("teardown_failure", 1, True),
    ],
)
def test_restart_attribution_requires_successful_test_outcome(
    tmp_path,
    outcome,
    exit_status,
    recorded,
):
    source = tmp_path / "test_outcome.py"
    action = {
        "fail": "assert False",
        "skip": "pytest.skip('skipped')",
        "xfail": "pytest.xfail('expected failure')",
    }.get(outcome, "pass")
    marker = "@pytest.mark.xfail(reason='expected failure')" if outcome == "xpass" else ""
    source.write_text(
        "import pytest\n"
        "@pytest.fixture(autouse=True)\n"
        "def teardown():\n"
        "    yield\n"
        f"    assert {outcome!r} != 'teardown_failure'\n"
        f"{marker}\n"
        "def test_witness(record_property):\n"
        "    record_property('event_cursor_restart', "
        "{'application': 'riverhog', 'operation_id': 'list_lifecycle_events'})\n"
        f"    {action}\n"
    )
    timing_path = tmp_path / "timings.json"
    source_sha = "a" * 40
    selected = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "tests.operation_observer", str(source)],
        cwd=REPO_ROOT,
        env={
            **os.environ,
            "RIVERHOG_OPERATION_SOURCE_SHA": source_sha,
            "RIVERHOG_OPERATION_TIMINGS": str(timing_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert selected.returncode == exit_status, selected.stdout + selected.stderr
    observed = json.loads(timing_path.read_text())
    assert observed["pytest_exit_status"] == exit_status
    assert bool(observed["event_cursor_restarts"]) is recorded
    if exit_status:
        module = load_script()
        with pytest.raises(module.QualificationError, match="identity or test result"):
            module._load_operation_timings(timing_path, source_sha=source_sha, matrix=())


def test_release_operation_predicate_consumes_current_evidence_and_rejects_missing_proof(
    observed_operation_evidence: tuple[ModuleType, dict[str, Any], Path],
    tmp_path: Path,
) -> None:
    module, observed, _ = observed_operation_evidence
    artifact = tmp_path / "qualification/contracts/riverhog-v1.json"
    artifact.parent.mkdir(parents=True)
    shutil.copyfile(module.CONTRACT_FREEZE, artifact)
    evidence_path = tmp_path / "operations.json"
    step = _release_qualification_step("Verify exact-SHA operation evidence")

    def verify(payload: dict[str, Any]) -> subprocess.CompletedProcess[str]:
        evidence_path.write_text(json.dumps(payload))
        return subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step],
            cwd=tmp_path,
            env={
                **os.environ,
                "SOURCE_SHA": observed["source_sha"],
                "OPERATIONS_SUMMARY": str(evidence_path),
            },
            capture_output=True,
            text=True,
            check=False,
        )

    rejected = verify(observed)
    assert rejected.returncode == 1, rejected.stderr
    assert rejected.stdout.strip() == "false"

    # Counterfactual statuses isolate consumer wiring from the producer's known
    # proof gaps. This test does not establish any behavioral qualification.
    qualified = deepcopy(observed)
    unestablished = [
        name
        for name, claim in qualified["qualification"].items()
        if claim["status"] == "not_established"
    ]
    assert unestablished
    for name in unestablished:
        qualified["qualification"][name]["status"] = "passed"
    accepted = verify(qualified)
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert accepted.stdout.strip() == "true"

    mutations = [
        (("source_sha",), "b" * 40),
        (("performance", "local_api", "source_sha"), "b" * 40),
        (("qualification", "extent_contract", "projection_sha256"), "0" * 64),
        (("qualification", "extent_contract", "extent_sha256"), "0" * 64),
        (("summary", "operations"), len(observed["operations"]) + 1),
        (("summary", "applications"), {"unexpected-application": 1}),
        (("summary", "classifications"), {"unexpected-classification": 1}),
        *[
            (("qualification", name, "status"), status)
            for name in unestablished
            for status in ("not_established", None)
        ],
    ]
    for path, value in mutations:
        invalid = deepcopy(qualified)
        target = invalid
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        rejected = verify(invalid)
        assert rejected.returncode == 1, (path, value, rejected.stdout, rejected.stderr)
        assert rejected.stdout.strip() == "false", path

    frozen = json.loads(artifact.read_bytes())
    del frozen["projection"]["external_contract"]["extents"]["sha256"]
    artifact.write_text(json.dumps(frozen))
    invalid = deepcopy(qualified)
    invalid["qualification"]["extent_contract"].update(
        projection_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
        extent_sha256=None,
    )
    assert verify(invalid).returncode != 0


def test_release_qualification_record_uses_current_artifact_identities(tmp_path: Path) -> None:
    artifact = tmp_path / "qualification/contracts/riverhog-v1.json"
    artifact.parent.mkdir(parents=True)
    shutil.copyfile(REPO_ROOT / "qualification/contracts/riverhog-v1.json", artifact)
    frozen = json.loads(artifact.read_bytes())
    summaries = {}
    for name in ("operations", "database", "release"):
        summary = tmp_path / f"{name}.json"
        summary.write_text(json.dumps({"fixture": name}))
        summaries[name] = summary
    step = _release_qualification_step("Record the completed qualification")
    environment = {
        **os.environ,
        "SOURCE_REF": "refs/heads/release/v1",
        "QUALIFICATION_MODE": "prospective",
        "SOURCE_SHA": "a" * 40,
        "RELEASE_VERSION": "1.0.0",
        "QUALIFICATION_DIR": str(tmp_path),
        **{f"{name.upper()}_SUMMARY": str(path) for name, path in summaries.items()},
    }

    def record() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step],
            cwd=tmp_path,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    completed = record()
    assert completed.returncode == 0, completed.stdout + completed.stderr
    output = tmp_path / "qualification.json"
    qualification = json.loads(output.read_text())
    assert (
        qualification["contract_projection_sha256"]
        == hashlib.sha256(artifact.read_bytes()).hexdigest()
    )
    assert qualification["contract_trace_sha256"] == frozen["identities"]["trace_sha256"]
    assert (
        qualification["extent_contract_sha256"]
        == frozen["projection"]["external_contract"]["extents"]["sha256"]
    )
    assert qualification["source_sha"] == environment["SOURCE_SHA"]
    assert qualification["published"] is False
    for name, path in summaries.items():
        assert (
            qualification[f"{name.removesuffix('s')}_evidence_sha256"]
            == hashlib.sha256(path.read_bytes()).hexdigest()
        )

    for identity_path in (
        ("identities", "trace_sha256"),
        ("projection", "external_contract", "extents", "sha256"),
    ):
        invalid = deepcopy(frozen)
        target = invalid
        for key in identity_path[:-1]:
            target = target[key]
        del target[identity_path[-1]]
        artifact.write_text(json.dumps(invalid))
        assert record().returncode != 0, identity_path
        assert output.read_text() == "", identity_path


def test_operation_evidence_rejects_an_incomplete_extent_authority(tmp_path: Path) -> None:
    module = load_script()
    authority = tmp_path / "contract.json"
    authority.write_text(
        json.dumps(
            {
                "format": "riverhog-contract-machine-closure/v1",
                "atlas": {"directory": "riverhog-v1", "documents": []},
            }
        )
    )

    try:
        module._contract_freeze_identity(authority)
    except module.QualificationError as exc:
        assert "extent authority is unavailable" in str(exc)
    else:
        raise AssertionError("incomplete extent authority must fail runtime qualification")


def test_operation_timings_record_successful_responses(monkeypatch) -> None:
    monkeypatch.setattr(operation_observer, "_OBSERVERS", [])
    app = FastAPI()

    @app.get("/items/{item_id}", operation_id="get_item")
    def get_item(item_id: int) -> dict[str, int]:
        if item_id != 1:
            raise HTTPException(status_code=404)
        return {"id": item_id}

    observer = operation_observer.OperationObserver.install(app, application="example")
    with TestClient(app) as client:
        observed = operation_observer.TimeoutNeutralTestClient(client, observer=observer)
        assert observed.get("/items/1").json() == {"id": 1}
        assert observed.get("/items/2").status_code == 404

    payload = operation_observer.timing_evidence(source_sha="a" * 40, exit_status=0)
    assert payload["source_sha"] == "a" * 40
    assert payload["pytest_exit_status"] == 0
    [row] = payload["operations"]
    assert row["application"] == "example"
    assert row["operation_id"] == "get_item"
    assert row["server_wall"]["samples"] == 1
    assert row["client_wall"]["samples"] == 1


def test_timing_evidence_fails_closed_on_missing_local_operation(tmp_path: Path) -> None:
    module = load_script()
    source_sha = "b" * 40
    timings = tmp_path / "timings.json"
    timings.write_text(
        json.dumps(
            {
                "format": "riverhog-operation-timings/v1",
                "source_sha": source_sha,
                "source_checkout": {
                    "start": {"head": source_sha, "clean": True},
                    "finish": {"head": source_sha, "clean": True},
                },
                "pytest_exit_status": 0,
                "operations": [],
            }
        )
    )

    try:
        module._load_operation_timings(
            timings,
            source_sha=source_sha,
            matrix=module.operation_matrix(),
        )
    except module.QualificationError as exc:
        assert "lack positive local timing witnesses" in str(exc)
    else:
        raise AssertionError("missing local operation witnesses must fail qualification")

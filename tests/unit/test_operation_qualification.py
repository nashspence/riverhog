from __future__ import annotations

import importlib.util
import json
import sys
from collections import defaultdict
from pathlib import Path
from types import ModuleType

import pytest

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
        "riverhog-ftp-adapter",
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
        by_identity[("riverhog-ftp-adapter", "get_ftp_adapter_status")].classification
        == "human-cli+json"
    )


def test_exact_sha_evidence_contains_only_generated_current_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    module = load_script()
    source_sha = "a" * 40
    monkeypatch.setattr(module, "_git_head", lambda: source_sha)
    monkeypatch.setattr(
        module,
        "_cold_cli_timings",
        lambda: {"riverhog": {"trials": 3, "median_ms": 1.0}},
    )

    matrix = module.operation_matrix()
    timings = tmp_path / "timings.json"
    timing_rows = []
    for item in matrix:
        if item.provider_evidence is not None:
            continue
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
                "schema": "riverhog-operation-timings/v1",
                "source_sha": source_sha,
                "pytest_exit_status": 0,
                "operations": timing_rows,
                "cli_callback_entries": [
                    {
                        "application": application,
                        "command": command,
                        "human_entries": 1,
                        "json_entries": 1,
                    }
                    for application, command in sorted(
                        {
                            (item.application, command)
                            for item in matrix
                            if item.classification == "human-cli+json"
                            for command in item.cli_commands
                        }
                    )
                ],
            }
        )
    )

    payload = module.evidence(source_sha=source_sha, timings=timings)
    operations = payload["operations"]

    assert payload["schema"] == "riverhog-operation-qualification/v1"
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
    ] == sum(item.provider_evidence is None for item in matrix)
    extent = payload["qualification"]["extent_contract"]
    assert extent["status"] == "passed"
    assert extent["schema"] == "riverhog-contract-machine-closure/v1"
    assert extent["extent_schema"] == "riverhog-extent-contract/v1"
    assert extent["extent_decisions"] > 0
    assert len(extent["projection_sha256"]) == 64
    assert len(extent["extent_sha256"]) == 64
    assert payload["qualification"]["cli_human_json_projection"]["status"] == "not_established"
    assert payload["qualification"]["bounded_state_access"]["status"] == "not_established"
    assert payload["qualification"]["event_cursor_restart_resume"]["status"] == "not_established"
    assert all(set(item) == set(module.Operation.__dataclass_fields__) for item in operations)

    incomplete = json.loads(timings.read_text())
    projection = next(
        item
        for item in incomplete["cli_callback_entries"]
        if item["application"] == "riverhog" and item["command"] == "retrieval cache status"
    )
    projection["json_entries"] = 0
    timings.write_text(json.dumps(incomplete))
    try:
        module._load_operation_timings(timings, source_sha=source_sha, matrix=matrix)
    except module.QualificationError as exc:
        assert "commands lack human/JSON callback entries" in str(exc)
    else:
        raise AssertionError("missing CLI callback coverage must fail observation validation")


def test_operation_evidence_rejects_an_incomplete_extent_authority(tmp_path: Path) -> None:
    module = load_script()
    authority = tmp_path / "contract.json"
    authority.write_text(
        json.dumps(
            {
                "schema": "riverhog-contract-machine-closure/v1",
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


def test_failed_cli_callbacks_are_only_reported_as_entries(monkeypatch) -> None:
    def failed_command(*, json_mode: bool) -> None:
        raise RuntimeError("command failed before producing output")

    monkeypatch.setattr(operation_observer, "_OBSERVERS", [])
    monkeypatch.setattr(
        operation_observer,
        "_CLI_CALLBACKS",
        {failed_command.__code__: [("riverhog", "collection list")]},
    )
    monkeypatch.setattr(
        operation_observer, "_CLI_CALLBACK_ENTRIES", defaultdict(lambda: defaultdict(int))
    )
    previous = sys.getprofile()
    try:
        sys.setprofile(operation_observer._observe_cli_callback_entry)
        for mode in (False, True):
            with pytest.raises(RuntimeError, match="before producing output"):
                failed_command(json_mode=mode)
    finally:
        sys.setprofile(previous)

    payload = operation_observer.timing_evidence(source_sha="a" * 40, exit_status=0)
    assert payload["cli_callback_entries"] == [
        {
            "application": "riverhog",
            "command": "collection list",
            "human_entries": 1,
            "json_entries": 1,
        }
    ]


def test_timing_evidence_fails_closed_on_missing_local_operation(tmp_path: Path) -> None:
    module = load_script()
    source_sha = "b" * 40
    timings = tmp_path / "timings.json"
    timings.write_text(
        json.dumps(
            {
                "schema": "riverhog-operation-timings/v1",
                "source_sha": source_sha,
                "pytest_exit_status": 0,
                "operations": [],
                "cli_callback_entries": [],
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

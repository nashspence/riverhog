from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest
import rfc8785

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/contract_freeze.py"
ARTIFACT = REPO_ROOT / "qualification/contracts/riverhog-v1.json"


def load_script() -> ModuleType:
    if str(SCRIPT.parent) not in sys.path:
        sys.path.insert(0, str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location("riverhog_contract_freeze", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_checked_contract_freeze_matches_every_executable_authority() -> None:
    module = load_script()
    audit_bundle = importlib.import_module("contract_audit_bundle")
    projection, trace, generated = module._generated_bundle()
    checked = module.load_bundle(ARTIFACT)

    assert ARTIFACT.read_bytes() == module.canonical_bytes(generated.root)
    assert checked.root == generated.root
    assert checked.files == generated.files
    assert module.reassemble_projection(checked) == json.loads(json.dumps(projection))
    assert module.reassemble_trace(checked) == json.loads(json.dumps(trace))
    assert projection["schema"] == "riverhog-contract-freeze/v1"
    assert set(projection) == {"schema", "series", "boundaries", "external_contract"}
    boundaries = projection["boundaries"]
    assert set(boundaries) == {
        "components",
        "entry_point_extensions",
        "process_extensions",
        "reference_policy",
        "runtime_images",
    }
    components = boundaries["components"]
    assert len(components) == 71
    roles = {component["distribution"]: component["role"] for component in components}
    extension_points = boundaries["entry_point_extensions"]
    assert {point["group"] for point in extension_points} == {
        "gogurt.listener-host-providers",
        "gogurt.mounted-volume-providers",
        "riverhog.provenance-contracts",
        "riverhog.provenance-observers",
        "stove0.observer-semantic-validators",
    }
    assert all(roles[point["owner"]] == "reusable_library" for point in extension_points)
    assert all(
        roles[provider["distribution"]] == "reference_component"
        for point in extension_points
        for provider in point["providers"]
    )
    process_extensions = boundaries["process_extensions"]
    assert {protocol for point in process_extensions for protocol in point["protocols"]} == {
        "riverhog-storage-adapter/v1",
        "stove0-content-observer/v1",
        "stove0-effect-target/v1",
        "stove0-review-sampler/v1",
        "stove0-transform-target/v1",
    }
    assert all(point["providers"] for point in process_extensions)
    assert all(
        roles[provider["distribution"]] == "reference_component"
        for point in process_extensions
        for provider in point["providers"]
    )
    external = projection["external_contract"]
    assert set(external) == {
        "cli",
        "configuration_documents",
        "configuration_environment",
        "configuration_environment_patterns",
        "durable_state",
        "extents",
        "http_openapi",
        "operations",
        "protocol_schemas",
        "python",
        "release",
    }
    assert set(external["cli"]) == {
        "gogurt",
        "mango-fish",
        "piggity",
        "riverhog-ftp-adapter",
        "riverhog-recover",
        "riverhog-storage-adapter-conformance",
        "riverhog-storage-adapter-filesystem-materialize",
        "riverhog-storage-adapter-schemas",
        "stove0",
        "stove0-observer-conformance",
        "stove0-observer-schemas",
        "stove0-review-planning",
        "stove0-review-sampler-conformance",
        "stove0-review-sampler-schemas",
        "stove0-target-conformance",
        "stove0-target-schemas",
    }
    assert set(external["cli"]["piggity"]["commands"]) == {
        "app",
        "archive",
        "catalog-sync",
        "collection",
        "event",
        "find",
        "local",
        "retrieval",
        "tag",
    }
    assert "list" in external["cli"]["piggity"]["commands"]["collection"]["commands"]
    assert set(external["http_openapi"]) == {
        "riverhog",
        "riverhog-ftp-adapter",
        "stove0",
    }
    assert len(external["operations"]) == 147
    assert any(
        operation["operation_id"] == "replace_collection_description"
        and operation["classification"] == "human-cli+json"
        for operation in external["operations"]
    )
    assert len(external["python"]) == 25
    riverhog_client_modules = {
        surface["module"]
        for surface in external["python"]
        if surface["distribution"] == "riverhog-client"
    }
    assert riverhog_client_modules == {"riverhog_client", "riverhog_client.transform"}
    assert len(external["durable_state"]["owners"]) == 8
    extents = external["extents"]
    assert extents["schema"] == "riverhog-extent-contract/v1"
    assert extents["coverage"]["classified"] == extents["coverage"]["discovered"]
    assert extents["coverage"]["missing"] == 0
    assert extents["coverage"]["duplicate"] == 0
    assert extents["coverage"]["stale"] == 0
    assert extents["coverage"]["undecided"] == 0
    assert trace["schema"] == "riverhog-contract-trace/v1"
    assert set(trace) == {
        "boundary_canonical_sha256",
        "contract_canonical_sha256",
        "contract_projection_sha256",
        "contract_schema",
        "coverage",
        "extent_sources",
        "schema",
        "segmented_extent_witnesses",
        "sources",
    }
    boundary_payload = json.dumps(boundaries, separators=(",", ":"), sort_keys=True).encode()
    boundary_sha256 = hashlib.sha256(boundary_payload).hexdigest()
    assert trace["boundary_canonical_sha256"] == boundary_sha256
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    assert release["governance"]["boundary_freeze"] == {
        "status": "frozen",
        "boundary_canonical_sha256": boundary_sha256,
    }
    root = checked.root
    assert root["schema"] == "riverhog-contract-audit-bundle/v1"
    assert len(ARTIFACT.read_bytes()) <= 64 * 1024
    assert root["boundary"]["legacy_canonical_sha256"] == boundary_sha256
    assert root["boundary"]["canonical_sha256"] == boundary_sha256
    contexts = module.context_descriptors(root)
    assert len(contexts) == root["coverage"]["contexts"]
    assert all(
        reference["bytes"] <= 32 * 1024
        for context in contexts
        for reference in (context.get("normative"), context["trace"])
        if reference is not None
    )
    assert root["coverage"]["anomalies"] == {
        "duplicate": 0,
        "missing": 0,
        "multiply_disposed": 0,
        "multiply_represented": 0,
        "multiply_routed": 0,
        "stale": 0,
        "undecided": 0,
    }
    assert root["detector_meta_closure"]["coverage"]["missing"] == 0
    assert root["detector_meta_closure"]["coverage"]["duplicate"] == 0
    assert root["detector_meta_closure"]["coverage"]["stale"] == 0
    assert root["detector_meta_closure"]["coverage"]["undecided"] == 0
    assert trace["coverage"]["source_authorities"] == len(trace["sources"])
    assert trace["coverage"]["source_kinds"] == {
        "cli": 16,
        "configuration": 6,
        "configuration-environment": 119,
        "openapi": 3,
        "protocol": 35,
        "python": 25,
        "release": 1,
        "state": 8,
    }
    assert trace["coverage"]["extent_decisions"] == len(extents["decisions"])
    assert trace["coverage"]["extent_source_links"] == len(extents["decisions"])
    segmented = [
        decision
        for decision in extents["decisions"]
        if decision["policy"] == "segmented_no_total_max"
    ]
    assert trace["coverage"]["segmented_decisions"] == len(segmented)
    assert trace["coverage"]["segmented_extent_witness_links"] >= len(segmented)
    assert trace["coverage"]["segmented_extent_witnesses"] == len(
        trace["segmented_extent_witnesses"]
    )

    contract_facts = []
    candidates = []
    candidate_sources = []
    trace_facts = []
    for context in contexts:
        normative = context.get("normative")
        if normative is not None:
            document = json.loads(checked.files[f"{root['context_directory']}/{normative['path']}"])
            if document["scope"] == "external-contract":
                contract_facts.extend(document["facts"])
        traced = json.loads(
            checked.files[f"{root['context_directory']}/{context['trace']['path']}"]
        )
        candidates.extend(traced["candidates"])
        candidate_sources.extend(traced["candidate_sources"])
        trace_facts.extend(traced["projection_trace_facts"])
    contract_identity = {
        "schema": audit_bundle.CONTRACT_IDENTITY_SCHEMA,
        "policies": list(audit_bundle.CONTRACT_POLICIES),
        "facts": sorted(contract_facts, key=lambda item: item["id"]),
    }
    coverage_identity = {
        "schema": audit_bundle.COVERAGE_IDENTITY_SCHEMA,
        "exclusion_policies": list(audit_bundle.EXCLUSION_POLICIES),
        "detectors": list(audit_bundle.DETECTORS),
        "meta_closure": root["detector_meta_closure"],
        "candidates": sorted(candidates, key=lambda item: item["id"]),
    }
    trace_identity = {
        "schema": audit_bundle.TRACE_IDENTITY_SCHEMA,
        "candidate_sources": sorted(candidate_sources, key=lambda item: item["candidate_id"]),
        "projection_trace_facts": sorted(trace_facts, key=lambda item: item["id"]),
        "qualification_routes": {
            key: list(value) for key, value in sorted(audit_bundle.QUALIFICATION_ROUTES.items())
        },
    }
    assert (
        root["identities"]["external_contract_sha256"]
        == hashlib.sha256(rfc8785.dumps(contract_identity)).hexdigest()
    )
    assert (
        root["identities"]["coverage_sha256"]
        == hashlib.sha256(rfc8785.dumps(coverage_identity)).hexdigest()
    )
    assert (
        root["identities"]["trace_sha256"]
        == hashlib.sha256(rfc8785.dumps(trace_identity)).hexdigest()
    )


def test_contract_regeneration_cannot_bless_undeclared_boundary_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    component_boundaries = module._component_boundaries

    def changed_component_boundaries(projects: object) -> list[dict[str, object]]:
        components = component_boundaries(projects)
        components[0] = {
            **components[0],
            "role": f"{components[0]['role']}-changed",
        }
        return components

    monkeypatch.setattr(module, "_component_boundaries", changed_component_boundaries)

    with pytest.raises(module.ContractFreezeError, match="maintainer-declared freeze"):
        module.contract_projection()


def test_extent_semantic_diff_is_grouped_by_owning_boundary() -> None:
    module = load_script()
    previous = {
        "external_contract": {
            "extents": {
                "decisions": [
                    {"id": "kept", "owner": "riverhog", "policy": "fixed"},
                    {"id": "changed", "owner": "stove0", "policy": "fixed"},
                    {"id": "removed", "owner": "stove0", "policy": "fixed"},
                ]
            }
        }
    }
    current = {
        "external_contract": {
            "extents": {
                "decisions": [
                    {"id": "kept", "owner": "riverhog", "policy": "fixed"},
                    {"id": "changed", "owner": "stove0", "policy": "contract_max"},
                    {"id": "added", "owner": "riverhog", "policy": "fixed"},
                ]
            }
        }
    }

    assert module._extent_diff(previous, current) == {
        "riverhog": {"added": 1, "changed": 0, "removed": 0},
        "stove0": {"added": 0, "changed": 1, "removed": 1},
    }


def test_audit_commands_expose_summary_filters_and_one_complete_unit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()

    assert module.main(["summary"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["schema"] == "riverhog-contract-audit-bundle/v1"
    assert summary["coverage"]["anomalies"]["missing"] == 0

    assert module.main(["list", "--kind", "http", "--disposition", "contractual"]) == 0
    contexts = json.loads(capsys.readouterr().out)
    assert contexts
    assert {context["kind"] for context in contexts} == {"http"}

    assert module.main(["show", contexts[0]["id"]]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["context"] == contexts[0]
    assert shown["normative"]["facts"]
    assert shown["trace"]["candidates"]
    assert shown["trace"]["qualification_routes"]

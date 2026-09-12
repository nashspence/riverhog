from __future__ import annotations

import importlib.util
import json
import sys
import tomllib
from pathlib import Path
from types import ModuleType

import pytest

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
    projection, trace, generated = module._generated_atlas()
    checked = module.load_atlas(ARTIFACT)

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
    assert set(external["http_openapi"]) == {"riverhog", "riverhog-ftp-adapter", "stove0"}
    assert len(external["operations"]) == 147
    assert len(external["python"]) == 25
    assert len(external["durable_state"]["owners"]) == 8
    extents = external["extents"]
    assert extents["coverage"]["classified"] == extents["coverage"]["discovered"]
    assert all(
        extents["coverage"][key] == 0 for key in ("missing", "duplicate", "stale", "undecided")
    )
    assert trace["schema"] == "riverhog-contract-trace/v1"
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

    root = checked.root
    assert root["schema"] == "riverhog-contract-machine-closure/v1"
    assert root["projection"]
    assert root["trace"]
    assert root["counts"]["contract_elements"] == len(root["elements"])
    assert root["counts"]["extent_decisions"] == len(extents["decisions"])
    assert root["counts"]["atlas_documents"] == len(root["atlas"]["documents"])
    assert root["discovery"]["anomalies"] == {
        "duplicate": 0,
        "missing": 0,
        "multiply_disposed": 0,
        "multiply_represented": 0,
        "stale": 0,
        "undecided": 0,
    }
    assert all(path.endswith(".md") for path in checked.files)
    assert not any(path.endswith(".json") for path in checked.files)
    assert root["identities"]["boundary_legacy_sha256"] == trace["boundary_canonical_sha256"]
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    assert release["governance"]["boundary_freeze"] == {
        "status": "frozen",
        "boundary_canonical_sha256": root["identities"]["boundary_legacy_sha256"],
    }


def test_contract_regeneration_cannot_bless_undeclared_boundary_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    component_boundaries = module._component_boundaries

    def changed_component_boundaries(projects: object) -> list[dict[str, object]]:
        components = component_boundaries(projects)
        components[0] = {**components[0], "role": f"{components[0]['role']}-changed"}
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


def test_audit_commands_route_by_authority_interface_and_dossier(
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_script()

    assert module.main(["summary"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["schema"] == "riverhog-contract-machine-closure/v1"
    assert summary["discovery_anomalies"]["missing"] == 0
    assert summary["atlas_root"] == "riverhog-v1/index.md"

    assert module.main(["list", "--authority", "riverhog", "--interface", "http"]) == 0
    elements = json.loads(capsys.readouterr().out)
    assert elements
    assert {item["authority"] for item in elements} == {"riverhog"}
    assert {item["interface"] for item in elements} == {"http"}

    assert module.main(["show", elements[0]["id"]]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["element"] == elements[0]
    assert shown["values"]
    assert shown["sources"]
    assert shown["trace_schema"] == "riverhog-contract-trace/v1"

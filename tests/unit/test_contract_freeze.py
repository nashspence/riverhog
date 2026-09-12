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
        "configuration-environment": 127,
        "configuration-environment-pattern": 1,
        "openapi": 3,
        "protocol": 35,
        "python": 25,
        "release": 1,
        "state": 8,
    }
    assert trace["coverage"]["extent_decisions"] == len(extents["decisions"])
    authority_registry = trace["authority_registry"]
    assert authority_registry["schema"] == "riverhog-contract-authority-registry/v1"
    assert {item["id"] for item in authority_registry["declared_authorities"]} == {
        "extent-contract",
        "release",
        "repository",
        "riverhog",
        "stove0",
    }
    assert {item["id"] for item in authority_registry["noncontractual_projection"]} == {
        "contract-projection-envelope",
        "durable-state-registry-envelope",
        "extent-projection-envelope",
    }
    sources = {item["id"]: item for item in trace["sources"]}
    assert sources["cli:stove0"]["owner"] == "stove0-client"
    assert (
        sources["cli:riverhog-storage-adapter-conformance"]["owner"]
        == "riverhog-storage-adapter-support"
    )
    assert sources["configuration:gogurt-routes"]["owner"] == "gogurt-core"
    assert sources["configuration:stove0-recipes"]["owner"] == "stove0-recipe-config"
    configuration = trace["configuration_registry"]
    assert configuration["counts"] == {
        "contracts": 127,
        "patterns": 1,
        "unique_environment_names": 119,
        "by_owner": {
            "gogurt": 2,
            "piggity": 7,
            "riverhog-client": 12,
            "riverhog-ftp-adapter": 3,
            "riverhog-ftp-adapter-api-client": 5,
            "riverhog-provenance": 1,
            "riverhog-server": 50,
            "stove0-api-client": 5,
            "stove0-exiftool-observer": 8,
            "stove0-ffprobe-sampling-observer": 8,
            "stove0-nvenc-av1-opus-review-sampler": 1,
            "stove0-nvenc-av1-opus-target": 2,
            "stove0-opus-review-sampler": 1,
            "stove0-opus-target": 1,
            "stove0-server": 20,
            "stove0-target-support": 1,
        },
        "by_classification": {"credential": 16, "identity": 42, "runtime": 69},
        "by_disposition": {"contractual": 127},
    }
    assert set(configuration["coverage"].values()) == {0}
    components = {item["distribution"] for item in projection["boundaries"]["components"]}
    assert {item["owner"] for item in configuration["records"]} <= components
    assert {consumer for item in configuration["records"] for consumer in item["consumers"]} <= (
        components
    )
    assert {
        item["owner"] for item in configuration["records"] if item["name"] == "RIVERHOG_BASE_URL"
    } == {"riverhog-client", "riverhog-ftp-adapter", "stove0-server"}
    assert not any(item["authority"] == "configuration" for item in checked.root["elements"])
    assert not {
        "durable-state",
        "gogurt-routes",
        "stove0-recipes",
        "stove0-review-target",
        "stove0-review-target-sampler",
    } & {item["authority"] for item in checked.root["elements"]}

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


def test_configuration_contract_fails_closed_on_an_owner_outside_the_frozen_topology(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_script()
    changed = (
        (REPO_ROOT / "qualification/configuration-contract.toml")
        .read_text(encoding="utf-8")
        .replace('owner = "gogurt"', 'owner = "unowned-setting"', 1)
    )
    contract = tmp_path / "configuration-contract.toml"
    contract.write_text(changed, encoding="utf-8")
    monkeypatch.setattr(module, "CONFIGURATION_CONTRACT", contract)

    with pytest.raises(module.ContractFreezeError, match="not an existing authority"):
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

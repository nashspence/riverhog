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
        "http_route_supplements",
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
    assert len(external["http_route_supplements"]) == 2
    assert len(trace["operation_qualification"]["records"]) == 147
    assert isinstance(external["python"], dict)
    assert len(external["python"]) == trace["python_registry"]["coverage"]["protected"]
    assert len(external["python"]) > len(trace["python_registry"]["detections"])
    assert len(external["durable_state"]["owners"]) == 8
    extents = external["extents"]
    assert extents["coverage"]["classified"] == extents["coverage"]["discovered"]
    assert all(
        extents["coverage"][key] == 0 for key in ("missing", "duplicate", "stale", "undecided")
    )
    assert trace["schema"] == "riverhog-contract-trace/v1"
    assert trace["coverage"]["source_kinds"] == {
        "audit": 1,
        "cli": 16,
        "configuration": 7,
        "configuration-environment": 250,
        "configuration-environment-pattern": 2,
        "openapi": 3,
        "protocol": 35,
        "python": 84,
        "release": 1,
        "state": 8,
    }
    assert trace["coverage"]["extent_decisions"] == len(extents["decisions"])
    assert trace["coverage"]["operation_qualification_records"] == 147
    assert trace["python_registry"]["coverage"] == {
        "detected": 84,
        "resolved": len(trace["python_registry"]["resolutions"]),
        "protected": len(external["python"]),
        "excluded": 22,
        "unresolved": 0,
        "undispositioned": 0,
        "stale_exceptions": 0,
    }
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
        "boundary-projection",
        "durable-state-registry-envelope",
        "extent-projection-envelope",
    }
    sources = {item["id"]: item for item in trace["sources"]}
    assert sources["cli:stove0"]["owner"] == "stove0-client"
    assert (
        sources["cli:riverhog-storage-adapter-conformance"]["owner"]
        == "riverhog-storage-adapter-support"
    )
    assert (
        sources["configuration:gogurt-core:configuration:gogurt-routes-schema"]["owner"]
        == "gogurt-core"
    )
    assert (
        sources["configuration:stove0-recipe-config:configuration:recipe-catalog"]["owner"]
        == "stove0-recipe-config"
    )
    configuration = trace["configuration_registry"]
    configuration_documents = trace["configuration_document_registry"]
    assert configuration_documents["counts"] == {
        "contracts": 7,
        "detections": 7,
        "resolved_detections": 7,
    }
    assert set(configuration_documents["coverage"].values()) == {0, 7}
    assert {item["id"] for item in configuration_documents["candidates"]} == set(
        external["configuration_documents"]
    )
    assert configuration["counts"] == {
        "contracts": 250,
        "detections": 199,
        "patterns": 2,
        "resolution_exceptions": 6,
        "resolved_detections": 199,
        "unique_environment_names": 240,
        "by_owner": {
            "gogurt-linux-listener-host": 2,
            "gogurt-windows-listener-host": 3,
            "piggity": 7,
            "riverhog-client": 12,
            "riverhog-ftp-adapter": 3,
            "riverhog-ftp-adapter-api-client": 5,
            "riverhog-provenance": 3,
            "riverhog-server": 50,
            "riverhog-storage-adapter-aws": 30,
            "riverhog-storage-adapter-backblaze": 20,
            "riverhog-storage-adapter-filesystem": 8,
            "stove0-api-client": 5,
            "stove0-exiftool-observer": 8,
            "stove0-ffprobe-sampling-observer": 8,
            "stove0-nvenc-av1-opus-review-sampler": 8,
            "stove0-nvenc-av1-opus-target": 10,
            "stove0-opus-review-sampler": 8,
            "stove0-opus-target": 9,
            "stove0-review-materialize-target": 10,
            "stove0-review-rclone-effect-target": 15,
            "stove0-server": 25,
            "stove0-target-support": 1,
        },
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
    assert not any(item["interface"] == "boundary" for item in checked.root["elements"])
    assert not any(
        pointer.startswith("/boundaries")
        for item in checked.root["elements"]
        for pointer in item["pointers"]
    )
    assert not {
        "durable-state",
        "gogurt-core:configuration:gogurt-routes-schema",
        "stove0-recipe-config:configuration:recipe-catalog",
        "stove0-review-target-support:configuration:review-target-config",
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


def test_configuration_resolution_fails_closed_on_an_owner_outside_the_frozen_topology(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_script()
    changed = (
        (REPO_ROOT / "qualification/contract-freeze-exceptions.toml")
        .read_text(encoding="utf-8")
        .replace(
            'source_authority_id = "configuration:mango-fish:configuration:mango-fish-config"',
            'source_authority_id = "configuration:unowned-setting"',
            1,
        )
    )
    contract = tmp_path / "contract-freeze-exceptions.toml"
    contract.write_text(changed, encoding="utf-8")
    monkeypatch.setattr(module, "CONTRACT_FREEZE_EXCEPTIONS", contract)

    projection = module.contract_projection()
    with pytest.raises(module.ContractFreezeError, match="unknown authorities"):
        module.trace_projection(projection)


def test_disposition_is_independent_of_detection_and_resolution() -> None:
    module = load_script()
    projects = module.release_contract.validate_release_contract(REPO_ROOT)
    projection = module.contract_projection()
    complete = module._release_surface_registries(projects, projection)
    undispositioned = module._release_surface_registries(
        projects, projection, include_dispositions=False
    )

    for name in complete:
        assert undispositioned[name]["detections"] == complete[name]["detections"]
        assert undispositioned[name]["resolutions"] == complete[name]["resolutions"]
        assert undispositioned[name]["candidates"] == complete[name]["candidates"]
        assert undispositioned[name]["dispositions"] == []
        assert undispositioned[name]["coverage"]["undispositioned"] == len(
            complete[name]["candidates"]
        )


def test_python_class_members_assign_structure_to_the_smallest_public_unit() -> None:
    module = load_script()

    class Example:
        @property
        def status(self) -> str:
            return "ready"

        def run(self, value: int) -> int:
            return value

        def __call__(self, value: int) -> int:
            return value

        def __iter__(self) -> object:
            return iter(())

        def _private(self) -> None:
            return None

    class_surface = module._class_surface(Example)
    members = module._public_class_members(Example)

    assert "members" not in class_surface
    assert set(members) == {"__call__", "__iter__", "run", "status"}
    assert members["__call__"] == {
        "kind": "method",
        "signature": module._signature(Example.__call__),
    }
    assert members["status"] == {
        "kind": "property",
        "signature": module._signature(Example.status.fget),
    }


def test_python_public_import_paths_and_special_methods_are_exact_units() -> None:
    module = load_script()
    projects = module.release_contract.validate_release_contract(REPO_ROOT)
    surfaces = module._python_surfaces(projects)

    assert all(public_identity.count(".") >= 1 for public_identity in surfaces)
    assert all(surface["unit"] in {"export", "member"} for surface in surfaces.values())
    lifecycle_enter = surfaces["lifecycle_events.LifecycleEventClient.__enter__"]
    assert lifecycle_enter["distribution"] == "lifecycle-events"
    assert lifecycle_enter["module"] == "lifecycle_events"
    assert lifecycle_enter["owner"] == "lifecycle_events.LifecycleEventClient"
    assert lifecycle_enter["unit"] == "member"
    assert lifecycle_enter["contract"]["kind"] == "method"
    assert "self" in lifecycle_enter["contract"]["signature"]
    assert "riverhog_client.transform.CapabilityApiClient.__getattr__" in surfaces
    assert "riverhog_storage_adapter_support.FramedContent.__iter__" in surfaces
    assert "stove0_operator_contracts.Stove0EventData.__getitem__" in surfaces
    assert "enum_values" in surfaces["riverhog_provenance.LargeValueDisposition"]["contract"]
    assert "riverhog_provenance.LargeValueDisposition.__str__" in surfaces


def test_python_surface_discovery_detects_reexports_and_member_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    shared = object()
    first = ModuleType("first_api")
    second = ModuleType("second_api")

    class Worker:
        def run(self) -> None:
            return None

        def __call__(self) -> None:
            return None

    first.__all__ = ["Shared", "Worker"]
    first.Shared = shared
    first.Worker = Worker
    second.__all__ = ["Shared"]
    second.Shared = shared
    detections = [
        {"distribution": "first-dist", "module": "first_api"},
        {"distribution": "second-dist", "module": "second_api"},
    ]
    monkeypatch.setattr(module, "python_package_detections", lambda *_: detections)
    monkeypatch.setattr(module, "load_exceptions", lambda *_: {"exclusion": []})
    monkeypatch.setattr(
        module.importlib,
        "import_module",
        lambda name: {"first_api": first, "second_api": second}[name],
    )

    before = module._python_surfaces([])
    assert "first_api.Shared" in before
    assert "second_api.Shared" in before
    assert "first_api.Worker.run" in before
    assert "first_api.Worker.__call__" in before

    def renamed(self: object) -> None:
        return None

    del Worker.run
    Worker.renamed = renamed
    after = module._python_surfaces([])
    assert "first_api.Worker.run" not in after
    assert "first_api.Worker.renamed" in after


def test_exception_overlay_cannot_create_or_describe_a_candidate(tmp_path: Path) -> None:
    module = load_script()
    path = tmp_path / "contract-freeze-exceptions.toml"
    path.write_text(
        "\n".join(
            (
                'schema = "riverhog-contract-freeze-exceptions/v1"',
                "resolution = []",
                "[[exclusion]]",
                'candidate_id = "not-detected"',
                'policy_id = "exclusion/example/v1"',
                'reason = "example"',
                'name = "RIVERHOG_INVENTED"',
            )
        ),
        encoding="utf-8",
    )
    with pytest.raises(module.DiscoveryError, match="unexpected|incomplete"):
        module.load_exceptions(path)


def test_exception_overlay_cannot_create_an_undetected_candidate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_script()
    changed = (
        (REPO_ROOT / "qualification/contract-freeze-exceptions.toml").read_text(encoding="utf-8")
        + "\n[[exclusion]]\n"
        + 'candidate_id = "console-script:missing:missing"\n'
        + 'policy_id = "exclusion/process-launcher-not-cli/v1"\n'
        + 'reason = "not actually detected"\n'
    )
    path = tmp_path / "contract-freeze-exceptions.toml"
    path.write_text(changed, encoding="utf-8")
    monkeypatch.setattr(module, "CONTRACT_FREEZE_EXCEPTIONS", path)
    projection = module.contract_projection()

    with pytest.raises(module.ContractFreezeError, match="exclusions are stale"):
        module._console_script_registry(projection)


def test_removing_resolution_hints_preserves_detection_and_fails_unresolved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = load_script()
    projects = module.release_contract.validate_release_contract(REPO_ROOT)
    before = module._environment_detections(projects)
    path = tmp_path / "contract-freeze-exceptions.toml"
    path.write_text(
        'schema = "riverhog-contract-freeze-exceptions/v1"\nresolution = []\nexclusion = []\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "CONTRACT_FREEZE_EXCEPTIONS", path)

    assert module._environment_detections(projects) == before
    with pytest.raises(module.ContractFreezeError, match="configuration reads are unresolved"):
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

    assert module.main(["list", "--authority", "riverhog", "--interface", "http-operations"]) == 0
    elements = json.loads(capsys.readouterr().out)
    assert elements
    assert {item["authority"] for item in elements} == {"riverhog"}
    assert {item["interface"] for item in elements} == {"http-operations"}

    assert module.main(["show", elements[0]["id"]]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["element"] == elements[0]
    assert shown["values"]
    assert shown["sources"]
    assert shown["trace_schema"] == "riverhog-contract-trace/v1"

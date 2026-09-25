from __future__ import annotations

import argparse
import copy
import importlib.util
import inspect
import json
import sys
import tomllib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import typer
from pydantic import BaseModel, Field
from riverhog_client import ApiClient
from typer.main import get_command

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


def test_decorated_client_source_resolves_to_its_repository_definition() -> None:
    module = load_script()
    method = ApiClient.stream_collection_provenance_journal
    definition = inspect.unwrap(method)
    assert definition is not method

    source = module._linked_source_ref(method)

    assert source["path"] == "packages/riverhog-client/src/riverhog_client/client.py"
    assert source["symbol"] == "ApiClient.stream_collection_provenance_journal"
    assert source["line"] == inspect.getsourcelines(definition)[1]


@pytest.mark.parametrize("parser_kind", ("argparse", "typer"))
def test_operation_trace_binds_the_actual_parser_callback_and_rejects_ambiguity(
    monkeypatch: pytest.MonkeyPatch,
    parser_kind: str,
) -> None:
    module = load_script()

    def callback() -> None:
        pass

    if parser_kind == "argparse":
        parser = argparse.ArgumentParser(prog="tool")
        parser.add_subparsers().add_parser("inspect").set_defaults(func=callback)
    else:
        app = typer.Typer()
        app.callback()(lambda: None)
        app.command("inspect")(callback)
        parser = get_command(app)
    parsers = {"tool": parser}
    monkeypatch.setattr(module, "_cli_parsers", lambda: parsers)
    surface = module.operation_qualification.ApplicationSurface(
        "example",
        module.operation_qualification.FastAPI(),
        (),
        (("inspect", callback, False),),
    )
    operation = module.operation_qualification.Operation(
        "example",
        "inspect",
        "GET",
        "/inspect",
        "http-only",
        "http-json",
        None,
        ("inspect",),
        None,
        None,
    )
    monkeypatch.setattr(module.operation_qualification, "application_surfaces", lambda: (surface,))
    monkeypatch.setattr(module.operation_qualification, "operation_matrix", lambda: (operation,))
    external = {
        "python": {},
        "cli": {
            "tool": {
                "commands": {"inspect": {"result_contract": {"identity": "tool-result/inspect/v1"}}}
            },
        },
    }
    binding = module._operation_trace(external)[0]["cli_bindings"][0]
    assert binding == {
        "command": "inspect",
        "executable": "tool",
        "result_identity": "tool-result/inspect/v1",
        "source": module._linked_source_ref(callback),
    }
    parsers["other"] = parser
    external["cli"]["other"] = external["cli"]["tool"]
    with pytest.raises(module.ContractFreezeError, match="no unique installed CLI identity"):
        module._operation_trace(external)


def test_prior_read_authority_links_remain_contract_side_audit_context() -> None:
    trace = json.loads(ARTIFACT.read_text(encoding="utf-8"))["trace"]
    witnesses = {item["id"]: item for item in trace["read_authority_witnesses"]}
    assert set(witnesses) == {
        "catalog-sync-read-authority-lifetime/v1",
        "exact-tag-revision-lifetime/v1",
    }
    assert all(item["status"] == "candidate-association" for item in witnesses.values())
    assert {node for item in witnesses.values() for node in item["test_node_ids"]} == {
        "tests/unit/test_catalog_sync.py::test_catalog_sync_cursor_fails_closed_for_authority_changes",
        "tests/unit/test_catalog_sync.py::test_catalog_sync_cursor_expiry_is_explicit",
        "tests/unit/test_catalog_sync.py::test_catalog_sync_history_reaping_has_an_explicit_gap_error",
        "tests/unit/test_runtime_config.py::test_catalog_sync_cursor_lifetimes_fit_the_retained_history",
        "tests/unit/test_collection_tags.py::test_exact_tag_revisions_expire_with_the_catalog_history_that_names_them",
    }
    assert all(
        pointer.startswith("/external_contract/")
        for item in witnesses.values()
        for pointer in item["subject_pointers"]
    )


def test_schema_discovery_does_not_consume_guidance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_script()
    source = tmp_path / "packages/protocol/example.schema.json"
    source.parent.mkdir(parents=True)
    source.write_text('{"$id":"example"}', encoding="utf-8")
    context = tmp_path / "guidance/unrelated.schema.json"
    context.parent.mkdir()
    context.write_text('{"$id":"unrelated"}', encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    assert module._repository_schema_paths() == [source]


def test_checked_contract_freeze_matches_every_executable_authority(
    checked_contract_closure: dict[str, Any],
) -> None:
    module = load_script()
    projection, trace, generated = module._generated_atlas()
    checked = checked_contract_closure["atlas"]

    assert ARTIFACT.read_bytes() == module.canonical_bytes(generated.root)
    assert checked.root == generated.root
    assert checked.files == generated.files
    assert module.reassemble_projection(checked) == json.loads(json.dumps(projection))
    assert module.reassemble_trace(checked) == json.loads(json.dumps(trace))
    assert projection["format"] == "riverhog-contract-freeze/v1"
    assert set(projection) == {"format", "series", "boundaries", "external_contract"}
    boundaries = projection["boundaries"]
    assert set(boundaries) == {
        "components",
        "contract_authorities",
        "entry_point_extensions",
        "process_extensions",
        "role_kinds",
        "runtime_images",
    }
    components = boundaries["components"]
    assert len(components) == 75
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
        roles[provider["distribution"]] == "component"
        for point in extension_points
        for provider in point["providers"]
    )
    process_extensions = boundaries["process_extensions"]
    assert {protocol for point in process_extensions for protocol in point["protocols"]} == {
        "riverhog-storage-adapter/v1",
        "stove0-content-observer/v1",
        "stove0-effect-target/v1",
        "review0-sampler/v1",
        "stove0-transform-target/v1",
    }
    external = projection["external_contract"]
    assert set(external) == {
        "cli",
        "configuration_documents",
        "configuration_environment",
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
        "a-riverhog-event-relay",
        "a-riverhog-minisign-witness",
        "a-riverhog-opentimestamps-witness",
        "a-riverhog-cli",
        "riverhog-api",
        "a-riverhog-ftp-spool",
        "a-riverhog-recovery-tool",
        "a-riverhog-aws-store",
        "a-riverhog-b2-store",
        "riverhog-storage-adapter-conformance",
        "a-riverhog-filesystem-store",
        "a-riverhog-filesystem-store-materialize",
        "riverhog-storage-adapter-schemas",
        "stove0",
        "a-stove0-exiftool-observer",
        "a-stove0-ffprobe-sampling-observer",
        "a-review0-nvenc-av1-opus-sampler",
        "a-stove0-nvenc-av1-opus-target",
        "stove0-observer-conformance",
        "stove0-observer-schemas",
        "a-review0-opus-sampler",
        "a-stove0-opus-target",
        "a-review0-materializer",
        "review0-planner",
        "a-review0-rclone-target",
        "review0-sampler-conformance",
        "review0-sampler-schemas",
        "stove0-server",
        "stove0-target-conformance",
        "stove0-target-schemas",
    }
    assert set(external["cli"]["a-riverhog-cli"]["commands"]) == {
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
    assert set(external["http_openapi"]) == {"riverhog", "a-riverhog-ftp-spool", "stove0"}
    assert len(external["http_route_supplements"]) == 2
    assert len(trace["operation_qualification"]["records"]) == 153
    assert isinstance(external["python"], dict)
    assert len(external["python"]) == trace["python_registry"]["coverage"]["protected"]
    assert len(external["python"]) > len(trace["python_registry"]["detections"])
    assert len(external["durable_state"]["owners"]) == 10
    assert all(
        "structure" in owner and "fixture_sha256s" not in owner
        for owner in external["durable_state"]["owners"]
    )
    release = external["release"]
    assert set(release) == {"compatibility", "publication"}
    publication = release["publication"]
    assert publication["format"] == "riverhog-release-publication/v1"
    assert len(publication["distributions"]) == 75
    assert len(publication["runtime_images"]) == 15
    assert len(publication["installation_roots"]) == 4
    assert "test" not in publication["runtime_images"]
    assert all(
        unit["requires_python"] == ">=3.12" for unit in publication["distributions"].values()
    )
    assert all(
        unit["platforms"] == ["linux/amd64"] for unit in publication["runtime_images"].values()
    )
    assert all(
        unit["platforms"] == ["linux-x64", "macos-arm64", "windows-x64"]
        for unit in publication["installation_roots"].values()
    )
    extents = external["extents"]
    assert extents["coverage"]["classified"] == extents["coverage"]["discovered"]
    assert all(
        extents["coverage"][key] == 0 for key in ("missing", "duplicate", "stale", "undecided")
    )
    assert trace["format"] == "riverhog-contract-trace/v1"
    assert trace["coverage"]["source_kinds"] == {
        "audit": 1,
        "cli": 31,
        "configuration": 11,
        "configuration-environment": 121,
        "openapi": 3,
        "protocol": 43,
        "python": 65,
        "release": 1,
        "release-distribution": 75,
        "release-images": 1,
        "release-installation": 1,
        "release-publication": 1,
        "state": 10,
    }
    assert trace["coverage"]["extent_decisions"] == len(extents["decisions"])
    assert trace["coverage"]["operation_qualification_records"] == 153
    assert trace["python_registry"]["coverage"] == {
        "detected": 65,
        "resolved": len(trace["python_registry"]["resolutions"]),
        "protected": len(external["python"]),
        "unresolved": 0,
        "undispositioned": 0,
    }
    authority_registry = trace["authority_registry"]
    assert authority_registry["format"] == "riverhog-contract-authority-registry/v1"
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
        "release-publication-envelope",
    }
    sources = {item["id"]: item for item in trace["sources"]}
    assert all(
        sources[f"state:{owner['id']}"]["declarations"]
        and sources[f"state:{owner['id']}"]["fixtures"]
        for owner in external["durable_state"]["owners"]
    )
    assert sources["cli:stove0"]["owner"] == "a-stove0-cli"
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
        "contracts": 11,
        "detections": 12,
        "resolved_detections": 12,
    }
    assert set(configuration_documents["coverage"].values()) == {0, 11, 12}
    assert {item["id"] for item in configuration_documents["candidates"]} == set(
        external["configuration_documents"]
    )
    assert configuration["counts"] == {
        "contracts": 121,
        "detections": 125,
        "patterns": 0,
        "resolution_exceptions": 0,
        "resolved_detections": 125,
        "unique_environment_names": 116,
        "by_owner": {
            "a-gogurt-linux-listener": 2,
            "a-gogurt-windows-listener": 3,
            "a-riverhog-cli": 7,
            "riverhog-client": 12,
            "a-riverhog-ftp-spool": 1,
            "a-riverhog-ftp-spool-client": 5,
            "riverhog-provenance": 3,
            "riverhog-server": 1,
            "a-riverhog-aws-store": 3,
            "a-riverhog-b2-store": 3,
            "a-riverhog-filesystem-store": 8,
            "stove0-api-client": 5,
            "a-stove0-exiftool-observer": 8,
            "a-stove0-ffprobe-sampling-observer": 8,
            "a-review0-nvenc-av1-opus-sampler": 8,
            "a-stove0-nvenc-av1-opus-target": 10,
            "a-review0-opus-sampler": 8,
            "a-stove0-opus-target": 9,
            "a-review0-materializer": 7,
            "a-review0-rclone-target": 8,
            "stove0-server": 1,
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
    } == {"riverhog-client"}
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
        "review0-target-lib:configuration:review-target-config",
    } & {item["authority"] for item in checked.root["elements"]}

    root = checked.root
    assert root["format"] == "riverhog-contract-machine-closure/v1"
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
    assert root["identities"]["boundary_canonical_sha256"] == trace["boundary_canonical_sha256"]
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    freeze = release["governance"]["boundary_freeze"]
    protected_boundaries = module.reassemble_projection(checked)["boundaries"]
    assert freeze["status"] == "frozen"
    assert freeze["protected_boundary_sha256"] == module._boundary_canonical_sha256(
        module._protected_boundary(protected_boundaries, freeze)
    )
    assert set(freeze["protected_components"]) <= {
        item["distribution"] for item in protected_boundaries["components"]
    }
    assert set(freeze["protected_runtime_images"]) <= set(
        protected_boundaries["runtime_images"]["runtime"]
    )


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


def test_boundary_freeze_allows_new_coordinates_but_protects_existing_topology() -> None:
    module = load_script()
    freeze = {
        "protected_components": ["owner", "provider"],
        "protected_runtime_images": ["existing-image"],
    }
    boundaries: dict[str, Any] = {
        "contract_authorities": {"repository": "repository authority"},
        "role_kinds": ["reusable_library", "component"],
        "components": [
            {
                "distribution": "owner",
                "role": "reusable_library",
                "dependencies": [],
                "optional_dependencies": {},
            },
            {
                "distribution": "provider",
                "role": "component",
                "dependencies": ["owner"],
                "optional_dependencies": {"feature": ["owner"]},
            },
        ],
        "runtime_images": {
            "platforms": ["linux/amd64"],
            "runtime": {
                "existing-image": {"role": "implementation", "distributions": ["provider"]}
            },
            "test_only": {},
        },
        "entry_point_extensions": [
            {
                "group": "example.providers",
                "owner": "owner",
                "providers": [
                    {"distribution": "provider", "name": "existing", "value": "provider:load"}
                ],
            }
        ],
        "process_extensions": [
            {
                "name": "example-process",
                "contract_owner": "owner",
                "providers": [{"distribution": "provider", "images": ["existing-image"]}],
            }
        ],
    }
    protected = module._protected_boundary(boundaries, freeze)
    baseline = module._boundary_canonical_sha256(protected)

    additive = copy.deepcopy(boundaries)
    additive["components"].append(
        {"distribution": "new-provider", "role": "component", "dependencies": ["owner"]}
    )
    additive["components"][1]["dependencies"].append("new-provider")
    additive["components"][1]["optional_dependencies"]["feature"].append("new-provider")
    additive["runtime_images"]["runtime"]["new-image"] = {
        "role": "implementation",
        "distributions": ["new-provider"],
    }
    additive["runtime_images"]["runtime"]["existing-image"]["distributions"].append("new-provider")
    additive["entry_point_extensions"][0]["providers"].append(
        {"distribution": "new-provider", "name": "new", "value": "new_provider:load"}
    )
    additive["process_extensions"][0]["providers"].append(
        {"distribution": "new-provider", "images": ["new-image"]}
    )
    assert (
        module._boundary_canonical_sha256(module._protected_boundary(additive, freeze)) == baseline
    )
    assert module._boundary_canonical_sha256(additive) != module._boundary_canonical_sha256(
        boundaries
    )

    changed_role = copy.deepcopy(additive)
    changed_role["components"][1]["role"] = "application"
    assert (
        module._boundary_canonical_sha256(module._protected_boundary(changed_role, freeze))
        != baseline
    )
    removed_existing_dependency = copy.deepcopy(additive)
    removed_existing_dependency["components"][1]["dependencies"].remove("owner")
    assert (
        module._boundary_canonical_sha256(
            module._protected_boundary(removed_existing_dependency, freeze)
        )
        != baseline
    )
    new_authority = copy.deepcopy(additive)
    new_authority["contract_authorities"]["other"] = "another authority"
    assert (
        module._boundary_canonical_sha256(module._protected_boundary(new_authority, freeze))
        != baseline
    )
    new_extension = copy.deepcopy(additive)
    new_extension["entry_point_extensions"].append(
        {"group": "new.group", "owner": "owner", "providers": []}
    )
    assert (
        module._boundary_canonical_sha256(module._protected_boundary(new_extension, freeze))
        != baseline
    )
    removed = copy.deepcopy(additive)
    removed["components"].pop(1)
    with pytest.raises(
        module.ContractFreezeError, match="protected component coordinates disappeared"
    ):
        module._protected_boundary(removed, freeze)


def test_configuration_resolution_fails_closed_on_an_owner_outside_the_frozen_topology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    original = module._configuration_registry

    def unowned_resolution(*args: Any, **kwargs: Any) -> dict[str, object]:
        registry = original(*args, **kwargs)
        registry["resolution_exceptions"] = [
            {"source_authority_id": "configuration:unowned-setting"}
        ]
        return registry

    monkeypatch.setattr(module, "_configuration_registry", unowned_resolution)

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


def test_python_class_surface_preserves_selected_enum_model_and_dataclass_structure() -> None:
    module = load_script()

    class Mode(StrEnum):
        READY = "ready"

    class Payload(BaseModel):
        name: str = Field(min_length=1)

    @dataclass(frozen=True)
    class Record:
        name: str
        count: int = 1

    assert module._class_surface(Mode)["enum_values"] == {"READY": "ready"}
    payload = module._class_surface(Payload)
    assert "schema_sha256" not in payload
    assert payload["schema"]["properties"]["name"]["minLength"] == 1
    assert module._class_surface(Record)["fields"] == [
        {"name": "name", "type": "'str'", "default": "required"},
        {"name": "count", "type": "'int'", "default": "1"},
    ]


def test_python_members_follow_effective_mro_and_preserve_descriptor_kinds() -> None:
    module = load_script()

    class Common:
        def run(self, value: int) -> int:
            return value

        def masked(self) -> None:
            pass

    class Left(Common):
        pass

    class Right(Common):
        def run(self, value: str) -> str:  # type: ignore[override]
            return value

        @property
        def status(self) -> str:
            raise AssertionError("discovery must not execute a property")

        @classmethod
        def create(cls, value: int) -> int:
            return value

        @staticmethod
        def normalize(value: str) -> str:
            return value

        def __enter__(self) -> object:
            return self

        def _private(self) -> None:
            pass

    class Exported(Left, Right):
        masked = None  # type: ignore[assignment]

    members = module._public_class_members(
        Exported, inherited_source_roots=(Path(__file__).parent,)
    )

    assert set(members) == {"run", "status", "create", "normalize", "__enter__"}
    assert members["run"]["signature"] == module._signature(Right.run)
    assert members["status"] == {
        "kind": "property",
        "signature": module._signature(Right.status.fget),
    }
    assert members["create"] == {
        "kind": "classmethod",
        "signature": module._signature(vars(Right)["create"].__func__),
    }
    assert members["normalize"]["kind"] == "staticmethod"


def test_python_inheritance_stays_within_release_owned_packages() -> None:
    module = load_script()

    class OwnedModel(BaseModel):
        def describe(self) -> str:
            return "owned behavior"

    class Exported(OwnedModel):
        pass

    members = module._public_class_members(
        Exported, inherited_source_roots=(Path(__file__).parent,)
    )

    assert members == {
        "describe": {"kind": "method", "signature": module._signature(OwnedModel.describe)}
    }


def test_python_surface_preserves_methods_moved_into_unexported_mixins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    public = ModuleType("example_api")

    class Behavior:
        def run(self, value: int) -> int:
            return value

    class Direct:
        run = Behavior.run

    class Inherited(Behavior):
        pass

    public.__all__ = ["Worker"]
    public.Worker = Direct
    monkeypatch.setattr(
        module,
        "python_package_detections",
        lambda *_: [
            {
                "distribution": "example-dist",
                "module": "example_api",
                "path": Path(__file__).relative_to(REPO_ROOT).as_posix(),
            }
        ],
    )
    monkeypatch.setattr(module.importlib, "import_module", lambda _: public)

    before = module._python_surfaces([])
    public.Worker = Inherited
    after = module._python_surfaces([])

    assert set(after) == {"example_api.Worker", "example_api.Worker.run"}
    assert after == before


def test_python_external_discovery_uses_public_exports_and_includes_them_automatically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    package = ModuleType("example_api")

    def exposed(value: str) -> str:
        return value

    package.exposed = exposed
    detection = {
        "id": "python-package:example-dist:example_api",
        "kind": "python-package",
        "distribution": "example-dist",
        "module": "example_api",
        "path": Path(__file__).relative_to(REPO_ROOT).as_posix(),
    }
    monkeypatch.setattr(module, "python_package_detections", lambda *_: [detection])
    monkeypatch.setattr(module.importlib, "import_module", lambda _: package)
    assert module._python_registry([])["candidates"] == []
    package.__all__ = ["exposed"]
    registry = module._python_registry([])
    assert registry["detections"] == [detection]
    assert {item["id"] for item in registry["candidates"]} == {
        "python:example-dist:example_api.exposed"
    }
    assert {item["disposition"] for item in registry["dispositions"]} == {"protected"}


def test_installed_entry_points_all_resolve_to_included_cli_trees(
    checked_contract_closure: dict[str, Any],
) -> None:
    checked = checked_contract_closure["atlas"]
    projection, trace = checked.root["projection"], checked.root["trace"]
    installed = {
        name
        for component in projection["boundaries"]["components"]
        for name in component["console_scripts"]
    }
    cli = projection["external_contract"]["cli"]
    registry = trace["console_script_registry"]
    assert set(cli) == installed
    assert {item["name"] for item in registry["candidates"]} == installed
    assert {item["disposition"] for item in registry["dispositions"]} == {"protected"}
    assert {item["candidate_id"] for item in registry["dispositions"]} == {
        item["id"] for item in registry["candidates"]
    }
    for authority in ("riverhog-api", "stove0-server"):
        commands = cli[authority]["commands"]["state"]["commands"]
        assert set(commands) == {"status", "upgrade", "verify"}
        assert all(
            item["result_contract"]["structured_output"] == "optional-json"
            for item in commands.values()
        )
    port = next(
        item
        for item in cli["stove0-server"]["commands"]["serve"]["parameters"]
        if item["dest"] == "port"
    )
    assert port["default"] == 8080


def test_python_model_schema_ignores_only_schema_prose_annotations() -> None:
    module = load_script()

    class First(BaseModel):
        """First documentation-only model description."""

        value: str = Field(description="First documentation-only field description.")

    class Second(BaseModel):
        """Second documentation-only model description."""

        value: str = Field(description="Second documentation-only field description.")

    assert module._class_surface(First)["schema"] == module._class_surface(Second)["schema"]

    raw = {
        "title": "Presentation only",
        "description": "Presentation only",
        "type": "object",
        "properties": {
            "title": {
                "title": "Generated field title",
                "description": "Generated field prose",
                "type": "string",
                "default": "kept",
            },
            "description": {
                "title": "Generated field title",
                "type": "integer",
                "minimum": 1,
            },
        },
        "default": {"title": "literal value", "description": "literal value"},
    }
    normalized = module.structural_json_schema(raw)

    assert set(normalized["properties"]) == {"title", "description"}
    assert normalized["properties"]["title"] == {"type": "string", "default": "kept"}
    assert normalized["properties"]["description"] == {"type": "integer", "minimum": 1}
    assert normalized["default"] == {
        "title": "literal value",
        "description": "literal value",
    }


def test_python_model_schema_retains_defaults_and_validation_structure() -> None:
    module = load_script()

    class Baseline(BaseModel):
        value: int = Field(default=1, ge=1)

    class ChangedDefault(BaseModel):
        value: int = Field(default=2, ge=1)

    class ChangedConstraint(BaseModel):
        value: int = Field(default=1, ge=2)

    baseline = module._class_surface(Baseline)["schema"]
    assert baseline != module._class_surface(ChangedDefault)["schema"]
    assert baseline != module._class_surface(ChangedConstraint)["schema"]


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
    assert "riverhog_client.processing.CapabilityApiClient.__getattr__" in surfaces
    assert "riverhog_storage_adapter_support.FramedContent.__iter__" in surfaces
    assert "stove0_operator_contracts.Stove0EventData.__getitem__" in surfaces
    assert "enum_values" in surfaces["riverhog_provenance.LargeValueDisposition"]["contract"]
    assert "riverhog_provenance.LargeValueDisposition.__str__" in surfaces


@pytest.mark.parametrize(
    "missing_identity",
    ("riverhog_client.ApiClient.list_processing_claims", "riverhog_client.ApiClient"),
)
def test_operation_clients_require_exported_owners_and_member_units(
    missing_identity: str, checked_contract_closure: dict[str, Any]
) -> None:
    module = load_script()
    projects = module.release_contract.validate_release_contract(REPO_ROOT)
    surfaces = module._python_surfaces(projects)
    external = {
        "python": surfaces,
        "cli": checked_contract_closure["projection"]["external_contract"]["cli"],
    }
    records = module._operation_trace(external)

    for record in records:
        if record["client"] is not None:
            assert record["client_bindings"]
        for binding in record["client_bindings"]:
            assert surfaces[binding["public_identity"]]["unit"] == "member"

    del surfaces[missing_identity]
    with pytest.raises(module.ContractFreezeError, match="maintained operation client lacks"):
        module._operation_trace(external)


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
        {
            "distribution": distribution,
            "module": name,
            "path": Path(__file__).relative_to(REPO_ROOT).as_posix(),
        }
        for distribution, name in (("first-dist", "first_api"), ("second-dist", "second_api"))
    ]
    monkeypatch.setattr(module, "python_package_detections", lambda *_: detections)
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
                'format = "riverhog-contract-freeze-exceptions/v1"',
                "[[resolution]]",
                'detection_id = "not-detected"',
                'source_authority_id = "configuration:example"',
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
        (REPO_ROOT / "qualification/contract-freeze-exceptions.toml")
        .read_text(encoding="utf-8")
        .replace("resolution = []\n", "")
        + "\n[[resolution]]\n"
        + 'detection_id = "configuration-read:missing:missing"\n'
        + 'source_authority_id = "configuration:missing"\n'
        + 'reason = "not actually detected"\n'
    )
    path = tmp_path / "contract-freeze-exceptions.toml"
    path.write_text(changed, encoding="utf-8")
    monkeypatch.setattr(module, "CONTRACT_FREEZE_EXCEPTIONS", path)
    projects = module.release_contract.validate_release_contract(REPO_ROOT)
    with pytest.raises(module.ContractFreezeError, match="resolution exception is stale"):
        module._environment_resolutions(projects)


def test_unresolved_configuration_read_fails_closed_even_when_detected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    projects = module.release_contract.validate_release_contract(REPO_ROOT)
    before = module._environment_detections(projects)
    unresolved = copy.deepcopy(before)
    unresolved[0]["resolved_names"] = []
    monkeypatch.setattr(module, "_environment_detections", lambda _projects: unresolved)

    assert [item["id"] for item in unresolved] == [item["id"] for item in before]
    with pytest.raises(module.ContractFreezeError, match="configuration reads are unresolved"):
        module._environment_inventory(projects)


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
    monkeypatch: pytest.MonkeyPatch,
    checked_contract_closure: dict[str, Any],
) -> None:
    module = load_script()
    monkeypatch.setattr(module, "load_atlas", lambda _path: checked_contract_closure["atlas"])

    assert module.main(["summary"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["format"] == "riverhog-contract-machine-closure/v1"
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
    assert shown["trace_format"] == "riverhog-contract-trace/v1"

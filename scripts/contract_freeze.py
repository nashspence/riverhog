#!/usr/bin/env python3
"""Generate or verify the canonical Riverhog v1 exposed-contract projection."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import inspect
import json
import re
import sys
import tomllib
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import MISSING, asdict, fields, is_dataclass
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Any, cast

import extent_contract
import extent_witnesses
import operation_qualification
import release as release_contract
import state_contract
from contract_atlas import (
    ContractAtlas,
    ContractAtlasError,
    build_atlas,
    canonical_bytes,
    load_atlas,
    pointer_value,
    reassemble_projection,
    reassemble_trace,
    structural_json_schema,
)
from contract_discovery import (
    DiscoveryError,
    discover_configuration_documents,
    discover_environment_reads,
    load_exceptions,
    python_package_detections,
)
from gogurt.cli import app as gogurt_app
from jsonschema import Draft202012Validator
from mango_fish.cli import parser as mango_fish_parser
from piggity.main import app as piggity_app
from pydantic import BaseModel
from riverhog_core.runtime_config import (
    ARCHIVE_STORE_ENVIRONMENT_SETTINGS,
    ARCHIVE_STORE_ENVIRONMENT_TEMPLATE,
    RETRIEVAL_CACHE_STORE_ENVIRONMENT_SETTINGS,
    RETRIEVAL_CACHE_STORE_ENVIRONMENT_TEMPLATE,
)
from riverhog_ftp_adapter.app import build_parser as ftp_adapter_parser
from riverhog_recover.cli import _parser as recovery_parser
from riverhog_storage_adapter_filesystem.materialize_cli import (
    build_parser as filesystem_materialize_parser,
)
from riverhog_storage_adapter_support import storage_adapter_schema_bundle
from riverhog_storage_adapter_support.conformance import _parser as storage_conformance_parser
from riverhog_storage_adapter_support.schemas import _parser as storage_schemas_parser
from stove0_cli.main import app as stove0_app
from stove0_observer_support import observer_schema_bundle
from stove0_observer_support.conformance import _parser as observer_conformance_parser
from stove0_observer_support.schemas import _parser as observer_schemas_parser
from stove0_review_planning.conformance import _parser as review_planning_parser
from stove0_review_sampler_support import sampler_schema_bundle
from stove0_review_sampler_support.conformance import _parser as sampler_conformance_parser
from stove0_review_sampler_support.schemas import _parser as sampler_schemas_parser
from stove0_target_support import target_schema_bundle
from stove0_target_support.conformance import _parser as target_conformance_parser
from stove0_target_support.schemas import _parser as target_schemas_parser
from typer.main import get_command

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "qualification/contracts/riverhog-v1.json"
ATLAS_DIRECTORY = ROOT / "qualification/contracts/riverhog-v1"
LEGACY_TRACE_OUTPUT = ROOT / "qualification/contracts/riverhog-v1-trace.json"
CONTRACT_FREEZE_EXCEPTIONS = ROOT / "qualification/contract-freeze-exceptions.toml"
SCHEMA = "riverhog-contract-freeze/v1"
TRACE_SCHEMA = "riverhog-contract-trace/v1"
CONFIGURATION_DISCOVERY_SCHEMA = "riverhog-configuration-discovery/v1"
AUTHORITY_REGISTRY_SCHEMA = "riverhog-contract-authority-registry/v1"
NONCONTRACTUAL_PROJECTION_AUTHORITIES: tuple[dict[str, object], ...] = (
    {
        "id": "boundary-projection",
        "pointers": ["/boundaries"],
        "reason": (
            "Frozen authority and extension topology used to attribute and navigate semantic "
            "contracts; component existence is not itself an external semantic promise."
        ),
    },
    {
        "id": "contract-projection-envelope",
        "pointers": ["/schema", "/series"],
        "reason": "Machine projection identity, not an external product promise.",
    },
    {
        "id": "durable-state-registry-envelope",
        "pointers": ["/external_contract/durable_state/schema"],
        "reason": (
            "Registry format identity; each durable-state promise belongs to its named owner."
        ),
    },
    {
        "id": "extent-projection-envelope",
        "pointers": [
            "/external_contract/extents/coverage",
            "/external_contract/extents/schema",
            "/external_contract/extents/sha256",
        ],
        "reason": "Generated coverage and identity metadata, not external extent semantics.",
    },
    {
        "id": "release-publication-envelope",
        "pointers": ["/external_contract/release/publication/schema"],
        "reason": (
            "Release generator and evidence format metadata; the exact publication promises "
            "belong to the meaningful release interfaces."
        ),
    },
)
PROCESS_SCHEMA_BUNDLES: dict[str, Callable[[], dict[str, Any]]] = {
    "riverhog-storage-adapter": storage_adapter_schema_bundle,
    "stove0-observer": observer_schema_bundle,
    "stove0-review-sampler": sampler_schema_bundle,
    "stove0-target": target_schema_bundle,
}
PUBLIC_DATA_MODEL_METHODS = frozenset(
    {
        "__aenter__",
        "__aexit__",
        "__aiter__",
        "__anext__",
        "__await__",
        "__bytes__",
        "__call__",
        "__contains__",
        "__delitem__",
        "__enter__",
        "__exit__",
        "__format__",
        "__getattr__",
        "__getitem__",
        "__iter__",
        "__len__",
        "__next__",
        "__reversed__",
        "__setitem__",
        "__str__",
    }
)


class ContractFreezeError(RuntimeError):
    """The generated contract projection differs from its authority."""


def _boundary_canonical_sha256(boundaries: Mapping[str, object]) -> str:
    payload = json.dumps(boundaries, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


def _semantic_symbol_id(value: str) -> str:
    words = re.sub(r"(.)([A-Z][a-z]+)", r"\1-\2", value)
    words = re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", words)
    return words.replace("_", "-").replace(".", "-").lower()


def _require_declared_boundary_freeze(
    config: Mapping[str, object], boundaries: Mapping[str, object]
) -> None:
    governance = cast(Mapping[str, object], config["governance"])
    freeze = cast(Mapping[str, object], governance["boundary_freeze"])
    expected = str(freeze["boundary_canonical_sha256"])
    observed = _boundary_canonical_sha256(boundaries)
    if observed != expected:
        raise ContractFreezeError(
            "the executable v1 authority boundary differs from the maintainer-declared "
            f"freeze: expected {expected}, observed {observed}; changing the frozen "
            "boundary requires an explicit maintainer decision and release.toml update"
        )


def _json_value(value: object) -> object | None:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return _json_value(value.value)
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            return None
        converted = {str(key): _json_value(item) for key, item in value.items()}
        return converted if all(item is not None for item in converted.values()) else None
    if isinstance(value, (list, tuple)):
        sequence_values = [_json_value(item) for item in value]
        return sequence_values if all(item is not None for item in sequence_values) else None
    if isinstance(value, (set, frozenset)):
        set_values = [_json_value(item) for item in value]
        if any(item is None for item in set_values):
            return None
        return sorted(set_values, key=lambda item: json.dumps(item, sort_keys=True))
    return None


def _signature(value: object) -> str:
    try:
        return _stable_repr(str(inspect.signature(cast(Callable[..., Any], value), eval_str=False)))
    except (TypeError, ValueError):
        return "unavailable"


def _stable_repr(value: object) -> str:
    return re.sub(r" at 0x[0-9a-fA-F]+", "", repr(value))


def _class_surface(value: type[object]) -> dict[str, object]:
    surface: dict[str, object] = {
        "kind": "class",
        "signature": _signature(value),
    }
    if issubclass(value, Enum):
        surface["enum_values"] = {
            name: _json_value(member.value) for name, member in value.__members__.items()
        }
    if issubclass(value, BaseModel):
        schema = cast(
            dict[str, object],
            structural_json_schema(value.model_json_schema(mode="validation")),
        )
        Draft202012Validator.check_schema(schema)
        surface["schema"] = schema
    if is_dataclass(value):
        surface["fields"] = [
            {
                "name": field.name,
                "type": _stable_repr(field.type),
                "default": (
                    _stable_repr(field.default)
                    if field.default is not MISSING
                    else "factory"
                    if field.default_factory is not MISSING
                    else "required"
                ),
            }
            for field in fields(value)
        ]
    return surface


def _public_class_members(value: type[object]) -> dict[str, dict[str, str]]:
    """Return directly declared callable/property units in one exported class."""

    members: dict[str, dict[str, str]] = {}
    for name, member in value.__dict__.items():
        if name.startswith("_") and name not in PUBLIC_DATA_MODEL_METHODS:
            continue
        candidate: object = member
        kind = "method"
        if isinstance(member, classmethod):
            candidate = member.__func__
            kind = "classmethod"
        elif isinstance(member, staticmethod):
            candidate = member.__func__
            kind = "staticmethod"
        elif isinstance(member, property):
            candidate = member.fget
            kind = "property"
        if callable(candidate):
            members[name] = {"kind": kind, "signature": _signature(candidate)}
    return members


def _python_export(value: object) -> dict[str, object]:
    if inspect.isclass(value):
        return _class_surface(value)
    if inspect.isfunction(value) or inspect.isbuiltin(value):
        return {"kind": "function", "signature": _signature(value)}
    if type(value).__name__ == "TypeAliasType":
        return {
            "kind": "type-alias",
            "value": _stable_repr(getattr(value, "__value__", value)),
        }
    serializable = _json_value(value)
    if serializable is not None:
        return {"kind": "constant", "value": serializable}
    return {
        "kind": "object",
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
    }


def _python_surfaces(
    projects: list[release_contract.Project],
) -> dict[str, dict[str, object]]:
    exceptions = load_exceptions(CONTRACT_FREEZE_EXCEPTIONS)
    excluded = {
        item["candidate_id"]
        for item in exceptions["exclusion"]
        if item["candidate_id"].startswith("python:")
    }
    result: dict[str, dict[str, object]] = {}
    for detection in python_package_detections(ROOT, projects):
        project = str(detection["distribution"])
        package = str(detection["module"])
        candidate_id = f"python:{project}:{package}"
        module = importlib.import_module(package)
        exports = getattr(module, "__all__", None)
        if exports is None:
            continue
        if candidate_id in excluded:
            raise ContractFreezeError(
                f"declared public Python API cannot be hidden by an exclusion: {candidate_id}"
            )
        if (
            not isinstance(exports, list)
            or not exports
            or any(
                not isinstance(name, str) or not name or name.startswith("_") for name in exports
            )
            or len(exports) != len(set(exports))
        ):
            raise ContractFreezeError(f"invalid public __all__ for {project}:{package}")
        missing = sorted(name for name in exports if not hasattr(module, name))
        if missing:
            raise ContractFreezeError(f"missing public exports for {project}:{package}: {missing}")
        for name in sorted(exports):
            value = getattr(module, name)
            public_identity = f"{package}.{name}"
            if public_identity in result:
                raise ContractFreezeError(
                    f"public Python import identity is provided by multiple distributions: "
                    f"{public_identity}"
                )
            result[public_identity] = {
                "distribution": project,
                "module": package,
                "name": name,
                "unit": "export",
                "contract": _python_export(value),
            }
            if inspect.isclass(value):
                for member_name, member_contract in sorted(_public_class_members(value).items()):
                    member_identity = f"{public_identity}.{member_name}"
                    if member_identity in result:
                        raise ContractFreezeError(
                            f"public Python identity is duplicated: {member_identity}"
                        )
                    result[member_identity] = {
                        "distribution": project,
                        "module": package,
                        "name": member_name,
                        "owner": public_identity,
                        "unit": "member",
                        "contract": member_contract,
                    }
    return dict(sorted(result.items()))


def _python_registry(
    projects: list[release_contract.Project],
    *,
    include_dispositions: bool = True,
) -> dict[str, object]:
    exceptions = load_exceptions(CONTRACT_FREEZE_EXCEPTIONS)
    detections = python_package_detections(ROOT, projects)
    excluded = {
        item["candidate_id"]: item
        for item in exceptions["exclusion"]
        if item["candidate_id"].startswith("python:")
    }
    candidate_ids = {f"python:{item['distribution']}:{item['module']}" for item in detections}
    stale = sorted(set(excluded) - candidate_ids) if include_dispositions else []
    if stale:
        raise ContractFreezeError(f"contract-freeze exclusions are stale: {stale}")
    protected_surfaces = _python_surfaces(projects)
    resolutions: list[dict[str, object]] = []
    candidates: list[dict[str, object]] = []
    dispositions: list[dict[str, str]] = []
    for detection in detections:
        distribution = str(detection["distribution"])
        module_name = str(detection["module"])
        package_candidate_id = f"python:{distribution}:{module_name}"
        module = importlib.import_module(module_name)
        exports = getattr(module, "__all__", None)
        declared = exports is not None
        if declared:
            if package_candidate_id in excluded:
                raise ContractFreezeError(
                    "declared public Python API cannot be hidden by an exclusion: "
                    f"{package_candidate_id}"
                )
            module_surfaces = {
                public_identity: surface
                for public_identity, surface in protected_surfaces.items()
                if surface["distribution"] == distribution and surface["module"] == module_name
            }
            for public_identity, surface in module_surfaces.items():
                candidate_id = f"python:{distribution}:{public_identity}"
                resolution = {
                    "detection_id": detection["id"],
                    "candidate_id": candidate_id,
                    "authority": distribution,
                    "module": module_name,
                    "public_identity": public_identity,
                    "unit": surface["unit"],
                }
                resolutions.append(resolution)
                candidates.append(
                    {
                        "id": candidate_id,
                        "authority": distribution,
                        "module": module_name,
                        "public_identity": public_identity,
                        "unit": surface["unit"],
                    }
                )
                if include_dispositions:
                    dispositions.append(
                        {
                            "candidate_id": candidate_id,
                            "disposition": "protected",
                            "policy_id": "compatibility/python-api/v1",
                            "reason": (
                                "The release package explicitly exports this exact Python unit."
                            ),
                        }
                    )
            continue

        resolutions.append(
            {
                "detection_id": detection["id"],
                "candidate_id": package_candidate_id,
                "authority": distribution,
                "module": module_name,
                "declared_exports": False,
            }
        )
        candidates.append(
            {
                "id": package_candidate_id,
                "authority": distribution,
                "module": module_name,
                "unit": "package",
            }
        )
        if include_dispositions:
            exception = excluded.get(package_candidate_id)
            dispositions.append(
                {
                    "candidate_id": package_candidate_id,
                    "disposition": "excluded",
                    "policy_id": (
                        exception["policy_id"]
                        if exception is not None
                        else "exclusion/python-package-no-declared-api/v1"
                    ),
                    "reason": (
                        exception["reason"]
                        if exception is not None
                        else "The importable package declares no public Python export surface."
                    ),
                }
            )
    return {
        "detector": "release-wheel-package",
        "detections": detections,
        "resolutions": resolutions,
        "candidates": candidates,
        "dispositions": dispositions,
        "coverage": {
            "detected": len(detections),
            "resolved": len(resolutions),
            "protected": sum(item["disposition"] == "protected" for item in dispositions),
            "excluded": sum(item["disposition"] == "excluded" for item in dispositions),
            "unresolved": 0,
            "undispositioned": len(candidates) - len(dispositions),
            "stale_exceptions": 0,
        },
    }


def _project_config(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _component_boundaries(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    names = {project.name for project in projects}
    components: list[dict[str, object]] = []
    for project in projects:
        config = _project_config(ROOT / project.path / "pyproject.toml")
        metadata = config["project"]
        dependencies = sorted(
            release_contract._dependency_name(str(item))
            for item in metadata.get("dependencies", [])
            if release_contract._dependency_name(str(item)) in names
        )
        optional_dependencies = {
            extra: sorted(
                release_contract._dependency_name(str(item))
                for item in values
                if release_contract._dependency_name(str(item)) in names
            )
            for extra, values in sorted(metadata.get("optional-dependencies", {}).items())
        }
        scripts = {name: value for name, value in sorted(metadata.get("scripts", {}).items())}
        components.append(
            {
                "distribution": project.name,
                "path": project.path,
                "role": project.role,
                "dependencies": dependencies,
                "optional_dependencies": optional_dependencies,
                "console_scripts": scripts,
            }
        )
    return components


def _extension_points(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    roles = {project.name: project.role for project in projects}
    paths = {project.name: project.path for project in projects}
    owners: dict[str, tuple[str, str]] = {}
    for surface in _python_surfaces(projects).values():
        name = str(surface["name"])
        contract = cast(Mapping[str, object], surface["contract"])
        value = contract.get("value")
        if (
            surface["unit"] == "export"
            and name.endswith("_ENTRY_POINT_GROUP")
            and isinstance(value, str)
        ):
            existing = owners.get(value)
            owner_binding = (str(surface["distribution"]), name)
            if existing is not None and existing != owner_binding:
                raise ContractFreezeError(f"entry-point group has multiple public owners: {value}")
            owners[value] = owner_binding

    providers: dict[str, list[dict[str, str]]] = defaultdict(list)
    for project in projects:
        metadata = _project_config(ROOT / project.path / "pyproject.toml")["project"]
        for group, entries in metadata.get("entry-points", {}).items():
            for name, value in entries.items():
                providers[group].append(
                    {
                        "distribution": project.name,
                        "name": name,
                        "value": value,
                    }
                )
    unknown = sorted(set(providers) - set(owners))
    if unknown:
        raise ContractFreezeError(f"extension groups lack a public owner: {unknown}")
    points: list[dict[str, object]] = []
    for group, (owner_distribution, constant) in sorted(owners.items()):
        if roles[owner_distribution] != "reusable_library":
            raise ContractFreezeError(f"extension owner is not reusable: {owner_distribution}")
        group_providers = sorted(
            providers.get(group, []),
            key=lambda item: (item["distribution"], item["name"]),
        )
        invalid = sorted(
            provider["distribution"]
            for provider in group_providers
            if roles[provider["distribution"]] != "reference_component"
        )
        if invalid:
            raise ContractFreezeError(
                f"checked-in extension providers are outside the reference role: {invalid}"
            )
        points.append(
            {
                "group": group,
                "owner": owner_distribution,
                "owner_path": paths[owner_distribution],
                "owner_constant": constant,
                "providers": group_providers,
            }
        )
    return points


def _process_extensions(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    roles = {project.name: project.role for project in projects}
    internal, _direct, _licenses = release_contract._project_dependency_graph(ROOT, projects)
    distributions_by_module: dict[str, str] = {}
    for project in projects:
        config = _project_config(ROOT / project.path / "pyproject.toml")
        packages = (
            config.get("tool", {})
            .get("hatch", {})
            .get("build", {})
            .get("targets", {})
            .get("wheel", {})
            .get("packages", [])
        )
        for package in packages:
            module = Path(str(package)).name
            existing = distributions_by_module.get(module)
            if existing is not None and existing != project.name:
                raise ContractFreezeError(f"Python package has multiple owners: {module}")
            distributions_by_module[module] = project.name

    images_by_distribution: dict[str, set[str]] = defaultdict(set)
    images = _project_config(ROOT / "release.toml")["images"]["runtime"]
    for image, config in images.items():
        for distribution in config["distributions"]:
            images_by_distribution[distribution].add(image)

    result: list[dict[str, object]] = []
    for name, factory in sorted(PROCESS_SCHEMA_BUNDLES.items()):
        module = factory.__module__.split(".", 1)[0]
        binding_support = distributions_by_module.get(module)
        if binding_support is None:
            raise ContractFreezeError(f"process protocol has no distribution owner: {name}")
        contract_owner = f"{binding_support.removesuffix('-support')}-protocol"
        if contract_owner not in internal[binding_support]:
            raise ContractFreezeError(f"process protocol has ambiguous contract ownership: {name}")
        bundle = factory()
        protocols = bundle.get("protocols")
        if not isinstance(protocols, list):
            protocol = bundle.get("protocol")
            protocols = [protocol] if isinstance(protocol, str) else []
        providers = [
            {
                "distribution": distribution,
                "images": sorted(distribution_images),
            }
            for distribution, distribution_images in sorted(images_by_distribution.items())
            if roles[distribution] == "reference_component"
            and binding_support in release_contract._dependency_closure(internal, distribution)
        ]
        if not protocols or not providers:
            raise ContractFreezeError(f"process extension is incomplete: {name}")
        result.append(
            {
                "name": name,
                "binding": "http",
                "contract_owner": contract_owner,
                "contract_owner_role": roles[contract_owner],
                "binding_support": binding_support,
                "binding_support_role": roles[binding_support],
                "schema_bundle_format": bundle["format"],
                "protocols": protocols,
                "providers": providers,
            }
        )
    return result


def _click_type(parameter: Any) -> dict[str, object]:
    type_ = parameter.type
    result: dict[str, object] = {
        "class": f"{type(type_).__module__}.{type(type_).__qualname__}",
        "name": getattr(type_, "name", None),
    }
    choices = getattr(type_, "choices", None)
    if choices is not None:
        result["choices"] = list(choices)
    minimum = _json_value(getattr(type_, "min", None))
    maximum = _json_value(getattr(type_, "max", None))
    if minimum is not None:
        result["minimum"] = minimum
    if maximum is not None:
        result["maximum"] = maximum
    return result


def _click_parameter(parameter: Any) -> dict[str, object]:
    result: dict[str, object] = {
        "kind": type(parameter).__name__,
        "name": parameter.name,
        "required": parameter.required,
        "nargs": parameter.nargs,
        "multiple": parameter.multiple,
        "type": _click_type(parameter),
    }
    if hasattr(parameter, "opts"):
        result["options"] = list(parameter.opts)
    if hasattr(parameter, "secondary_opts"):
        result["secondary_options"] = list(parameter.secondary_opts)
    for attribute in ("is_flag", "count", "envvar"):
        if hasattr(parameter, attribute):
            result[attribute] = _json_value(getattr(parameter, attribute))
    default = _json_value(parameter.default)
    if default is not None:
        result["default"] = default
    return result


CLI_RESULT_CONTRACT_SCHEMA = "riverhog-cli-result-contract/v1"
CLI_COMMAND_JSON_OUTPUT = "$command-json-output"
CLI_OPERATION_APPLICATIONS = {
    "piggity": "riverhog",
    "riverhog-ftp-adapter": "riverhog-ftp-adapter",
    "stove0": "stove0",
}
CLI_MODULES = {
    "gogurt": "gogurt.cli",
    "mango-fish": "mango_fish.cli",
    "piggity": "piggity.main",
    "riverhog-ftp-adapter": "riverhog_ftp_adapter.app",
    "riverhog-recover": "riverhog_recover.cli",
    "riverhog-storage-adapter-conformance": "riverhog_storage_adapter_support.conformance",
    "riverhog-storage-adapter-filesystem-materialize": (
        "riverhog_storage_adapter_filesystem.materialize_cli"
    ),
    "riverhog-storage-adapter-schemas": "riverhog_storage_adapter_support.schemas",
    "stove0": "stove0_cli.main",
    "stove0-observer-conformance": "stove0_observer_support.conformance",
    "stove0-observer-schemas": "stove0_observer_support.schemas",
    "stove0-review-planning": "stove0_review_planning.conformance",
    "stove0-review-sampler-conformance": "stove0_review_sampler_support.conformance",
    "stove0-review-sampler-schemas": "stove0_review_sampler_support.schemas",
    "stove0-target-conformance": "stove0_target_support.conformance",
    "stove0-target-schemas": "stove0_target_support.schemas",
}
CLI_SIMPLE_OUTCOME_SELECTOR_KINDS = frozenset(
    {
        "action-returned-zero",
        "application-error",
        "command-completed",
        "conformance-completed",
        "contract-report-completed",
        "delegated-action-returned-nonzero",
        "interactive-confirmation-mismatch",
        "listener-runtime-returned",
        "local-audit-problem-count-positive",
        "materialization-completed",
        "materialization-error",
        "parser-rejected-invocation",
        "plan-reported-blockers",
        "recovery-error",
        "relay-pass-reported-failures",
        "schema-bundle-emitted",
        "service-runtime-returned",
        "state-schema-error",
        "state-schema-operation-completed",
    }
)


def _click_terminating_controls(command: Any) -> list[dict[str, object]]:
    context = command.make_context(command.name or "command", [], resilient_parsing=True)
    controls: list[dict[str, object]] = []
    help_options = list(command.get_help_option_names(context))
    if help_options:
        controls.append(
            {
                "id": "help",
                "trigger": {"kind": "option-present", "options": help_options},
                "exit_status": 0,
                "stdout": "noncontractual-framework-help",
                "stderr": "empty",
            }
        )
    for parameter in command.params:
        options = list(getattr(parameter, "opts", ()))
        if "--version" in options:
            controls.append(
                {
                    "id": "version",
                    "trigger": {"kind": "option-present", "options": options},
                    "exit_status": 0,
                    "stdout": {
                        "kind": "installed-coordinated-release-version",
                        "serialization": "noncontractual",
                    },
                    "stderr": "empty",
                }
            )
        if any(option in {"--install-completion", "--show-completion"} for option in options):
            raise ContractFreezeError(
                f"released CLI exposes an unaccounted completion control: {command.name}: {options}"
            )
    if bool(getattr(command, "no_args_is_help", False)):
        controls.append(
            {
                "id": "implicit-help",
                "trigger": {"kind": "empty-invocation"},
                "exit_status": 2,
                "stdout": "empty",
                "stderr": "noncontractual-framework-help",
            }
        )
    return controls


def _click_command(command: Any, *, name: str) -> dict[str, object]:
    parameters = [
        parameter
        for parameter in command.params
        if "--version" not in set(getattr(parameter, "opts", ()))
    ]
    result: dict[str, object] = {
        "name": name,
        "parameters": [_click_parameter(parameter) for parameter in parameters],
        "terminating_controls": _click_terminating_controls(command),
    }
    commands = getattr(command, "commands", None)
    if isinstance(commands, Mapping):
        result["commands"] = {
            child_name: _click_command(child, name=child_name)
            for child_name, child in sorted(commands.items())
        }
    return result


def _argparse_action(action: argparse.Action) -> dict[str, object]:
    result: dict[str, object] = {
        "kind": type(action).__name__,
        "dest": action.dest,
        "options": list(action.option_strings),
        "required": action.required,
        "nargs": action.nargs,
    }
    if action.choices is not None:
        result["choices"] = list(action.choices)
    if action.type is not None:
        result["type"] = getattr(action.type, "__qualname__", repr(action.type))
    default = _json_value(action.default)
    if default is not None and action.default is not argparse.SUPPRESS:
        result["default"] = default
    return result


def _argparse_command(
    parser: argparse.ArgumentParser, *, name: str | None = None
) -> dict[str, object]:
    terminating_controls: list[dict[str, object]] = []
    for action in parser._actions:
        if isinstance(action, argparse._HelpAction):
            terminating_controls.append(
                {
                    "id": "help",
                    "trigger": {
                        "kind": "option-present",
                        "options": list(action.option_strings),
                    },
                    "exit_status": 0,
                    "stdout": "noncontractual-framework-help",
                    "stderr": "empty",
                }
            )
        elif isinstance(action, argparse._VersionAction):
            terminating_controls.append(
                {
                    "id": "version",
                    "trigger": {
                        "kind": "option-present",
                        "options": list(action.option_strings),
                    },
                    "exit_status": 0,
                    "stdout": {
                        "kind": "installed-coordinated-release-version",
                        "serialization": "noncontractual",
                    },
                    "stderr": "empty",
                }
            )
    actions = [
        action
        for action in parser._actions
        if not isinstance(action, argparse._HelpAction)
        and not isinstance(action, argparse._VersionAction)
        and not isinstance(action, argparse._SubParsersAction)
    ]
    subparsers = next(
        (action for action in parser._actions if isinstance(action, argparse._SubParsersAction)),
        None,
    )
    result: dict[str, object] = {
        "name": name or parser.prog,
        "parameters": [_argparse_action(action) for action in actions],
        "terminating_controls": terminating_controls,
    }
    if subparsers is not None:
        result["commands"] = {
            child_name: _argparse_command(child, name=child_name)
            for child_name, child in sorted(subparsers.choices.items())
        }
    return result


def _merge_cli_result_contract(
    base: Mapping[str, object], override: Mapping[str, object]
) -> dict[str, object]:
    result = copy.deepcopy(dict(base))
    for key, value in override.items():
        current = result.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            result[key] = _merge_cli_result_contract(current, value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _validate_cli_result_outcomes(
    authority: str,
    command: str,
    kind: str,
    outcomes: object,
) -> None:
    if not isinstance(outcomes, list) or not outcomes:
        raise ContractFreezeError(
            f"CLI {kind} outcomes must be a nonempty list: {authority}: {command}"
        )
    identities: set[str] = set()
    for outcome in outcomes:
        if not isinstance(outcome, Mapping):
            raise ContractFreezeError(
                f"CLI {kind} outcome must be an object: {authority}: {command}"
            )
        identity = outcome.get("id")
        if not isinstance(identity, str) or not identity or identity in identities:
            raise ContractFreezeError(
                f"CLI {kind} outcome identity is missing or duplicate: {authority}: {command}"
            )
        identities.add(identity)
        if set(outcome) != {"id", "selected_by", "exit_status", "stdout", "stderr"}:
            raise ContractFreezeError(
                f"CLI {kind} outcome shape is not exact: {authority}: {command}: {identity}"
            )
        selector = outcome["selected_by"]
        if (
            not isinstance(selector, Mapping)
            or not isinstance(selector.get("kind"), str)
            or not selector.get("kind")
        ):
            raise ContractFreezeError(
                f"CLI {kind} outcome selector is invalid: {authority}: {command}: {identity}"
            )
        exit_status = outcome["exit_status"]
        if not isinstance(exit_status, int) and not (
            isinstance(exit_status, Mapping)
            and set(exit_status) == {"kind", "minimum", "maximum"}
            and exit_status.get("kind") == "delegated"
            and isinstance(exit_status.get("minimum"), int)
            and isinstance(exit_status.get("maximum"), int)
            and cast(int, exit_status["minimum"]) <= cast(int, exit_status["maximum"])
        ):
            raise ContractFreezeError(
                f"CLI {kind} exit status is not exact: {authority}: {command}: {identity}"
            )
        for channel in ("stdout", "stderr"):
            value = outcome[channel]
            if (
                not isinstance(value, Mapping)
                or not value
                or not all(
                    mode in {"human", "json", "all"}
                    and (
                        isinstance(semantics, str)
                        or (isinstance(semantics, Mapping) and bool(semantics))
                    )
                    for mode, semantics in value.items()
                )
            ):
                raise ContractFreezeError(
                    f"CLI {kind} {channel} contract is invalid: {authority}: {command}: {identity}"
                )


def _validate_cli_outcome_selector(
    authority: str,
    command: str,
    outcome_id: str,
    selector: Mapping[str, object],
    parameters: Sequence[Mapping[str, object]],
) -> None:
    kind = selector.get("kind")
    if kind in CLI_SIMPLE_OUTCOME_SELECTOR_KINDS:
        if set(selector) != {"kind"}:
            raise ContractFreezeError(
                f"CLI outcome selector shape is not exact: "
                f"{authority}: {command}: {outcome_id}: {kind}"
            )
        return
    state_predicates = {
        "archive-copy-state": "failed",
        "custody-deadline-expired": "not-finalized",
    }
    if kind in state_predicates:
        if set(selector) != {"kind", "state"} or selector.get("state") != state_predicates[kind]:
            raise ContractFreezeError(
                f"CLI outcome state predicate is invalid: "
                f"{authority}: {command}: {outcome_id}: {kind}"
            )
        return
    if kind not in {"option-equals", "option-present", "option-absent", "options-absent"}:
        raise ContractFreezeError(
            f"CLI outcome selector kind is unknown: {authority}: {command}: {outcome_id}: {kind}"
        )
    parameter_names = {
        str(parameter.get("name") or parameter.get("dest"))
        for parameter in parameters
        if parameter.get("name") or parameter.get("dest")
    }
    if kind == "option-equals":
        expected_keys = {"kind", "parameter", "value"}
        selected = [selector.get("parameter")]
    elif kind in {"option-present", "option-absent"}:
        expected_keys = {"kind", "parameter"}
        selected = [selector.get("parameter")]
    else:
        expected_keys = {"kind", "parameters"}
        raw = selector.get("parameters")
        if not isinstance(raw, list) or not raw:
            raise ContractFreezeError(
                f"CLI outcome selector has no parameters: {authority}: {command}: {outcome_id}"
            )
        selected = raw
    if set(selector) != expected_keys or any(
        not isinstance(parameter, str) or parameter not in parameter_names for parameter in selected
    ):
        raise ContractFreezeError(
            f"CLI outcome selector does not resolve to discovered parameters: "
            f"{authority}: {command}: {outcome_id}"
        )


def _operation_success_authority(
    operation: operation_qualification.Operation,
    openapi: Mapping[str, object],
) -> dict[str, object]:
    application = cast(Mapping[str, object], openapi[operation.application])
    paths = cast(Mapping[str, object], application["paths"])
    path_item = cast(Mapping[str, object], paths[operation.path])
    operation_document = cast(Mapping[str, object], path_item[operation.method.casefold()])
    responses = cast(Mapping[str, object], operation_document["responses"])
    statuses = sorted(status for status in responses if status.startswith("2"))
    if len(statuses) != 1:
        raise ContractFreezeError(
            f"CLI operation has no single successful response: "
            f"{operation.application}: {operation.operation_id}: {statuses}"
        )
    status = statuses[0]
    response = cast(Mapping[str, object], responses[status])
    content = cast(Mapping[str, object], response.get("content", {}))
    media = content.get("application/json")
    if not isinstance(media, Mapping) or not isinstance(media.get("schema"), Mapping):
        raise ContractFreezeError(
            f"CLI JSON output operation has no JSON response schema: "
            f"{operation.application}: {operation.operation_id}"
        )
    return {
        "kind": "http-operation-response",
        "application": operation.application,
        "operation_id": operation.operation_id,
        "method": operation.method,
        "path": operation.path,
        "status": status,
        "schema": copy.deepcopy(media["schema"]),
    }


def _resolved_cli_format_authorities(
    identity: str, schema_documents: Mapping[str, object]
) -> list[dict[str, object]]:
    matches: list[dict[str, object]] = []
    for schema_authority, document_value in schema_documents.items():
        if not isinstance(document_value, Mapping):
            continue
        document = cast(Mapping[str, object], document_value)
        if schema_authority == identity:
            matches.append({"kind": "schema-authority", "authority": schema_authority})
            continue
        properties = document.get("properties")
        format_property = (
            cast(Mapping[str, object], properties).get("format")
            if isinstance(properties, Mapping)
            else None
        )
        if isinstance(format_property, Mapping) and format_property.get("const") == identity:
            matches.append({"kind": "schema-authority", "authority": schema_authority})
        if document.get("format") == identity:
            matches.append({"kind": "document-authority", "authority": schema_authority})
        schemas = document.get("schemas")
        if not isinstance(schemas, Mapping):
            continue
        for schema_name, schema_value in schemas.items():
            if not isinstance(schema_value, Mapping):
                continue
            schema_properties = cast(Mapping[str, object], schema_value).get("properties")
            schema_format = (
                cast(Mapping[str, object], schema_properties).get("format")
                if isinstance(schema_properties, Mapping)
                else None
            )
            if isinstance(schema_format, Mapping) and schema_format.get("const") == identity:
                matches.append(
                    {
                        "kind": "schema-authority",
                        "authority": schema_authority,
                        "definition": str(schema_name),
                    }
                )
    return matches


def _resolve_cli_output_authority(
    *,
    authority: str,
    command: str,
    outcome_id: str,
    declared: object,
    output_authorities: Mapping[str, object],
    operations_by_command: Mapping[tuple[str, str], Sequence[operation_qualification.Operation]],
    operations_by_id: Mapping[tuple[str, str], operation_qualification.Operation],
    openapi: Mapping[str, object],
    schema_documents: Mapping[str, object],
) -> object:
    if declared != CLI_COMMAND_JSON_OUTPUT:
        if isinstance(declared, str):
            if declared == "empty" or declared.startswith("noncontractual-"):
                return declared
            if not declared:
                raise ContractFreezeError(
                    f"CLI JSON format identity is empty: {authority}: {command}: {outcome_id}"
                )
            matches = _resolved_cli_format_authorities(declared, schema_documents)
            if len(matches) != 1:
                raise ContractFreezeError(
                    f"CLI JSON format identity does not resolve to one schema authority: "
                    f"{authority}: {command}: {outcome_id}: {declared}: {len(matches)}"
                )
            return matches[0]
        if not isinstance(declared, Mapping):
            raise ContractFreezeError(
                f"CLI JSON output authority is invalid: {authority}: {command}: {outcome_id}"
            )
        kind = declared.get("kind")
        identity = declared.get("identity")
        schema = declared.get("schema")
        if kind == "cli-local-exact-json":
            document = declared.get("document")
            if (
                set(declared) != {"kind", "identity", "document"}
                or not isinstance(identity, str)
                or not identity
                or not isinstance(document, Mapping)
                or document.get("format") != identity
            ):
                raise ContractFreezeError(
                    f"CLI exact JSON authority is invalid: {authority}: {command}: {outcome_id}"
                )
            return copy.deepcopy(declared)
        if kind == "schema-format-or-null":
            if set(declared) != {"kind", "identity"} or not isinstance(identity, str):
                raise ContractFreezeError(
                    f"CLI nullable JSON format authority is invalid: "
                    f"{authority}: {command}: {outcome_id}"
                )
            matches = _resolved_cli_format_authorities(identity, schema_documents)
            if len(matches) != 1 or matches[0].get("kind") != "schema-authority":
                raise ContractFreezeError(
                    f"CLI nullable JSON format does not resolve to one schema authority: "
                    f"{authority}: {command}: {outcome_id}: {identity}: {len(matches)}"
                )
            return {**matches[0], "nullable": True}
        if kind == "cli-local-json-sequence":
            records = declared.get("records")
            sequence = declared.get("sequence")
            if (
                set(declared) != {"kind", "identity", "framing", "records", "sequence"}
                or not isinstance(identity, str)
                or not identity
                or declared.get("framing") != "newline-delimited-json"
                or not isinstance(records, Mapping)
                or not records
                or not isinstance(sequence, Mapping)
                or set(sequence) != {"start", "repeated", "end"}
                or not all(isinstance(name, str) and name in records for name in sequence.values())
                or len(set(sequence.values())) != 3
                or set(records) != set(sequence.values())
            ):
                raise ContractFreezeError(
                    f"CLI JSON sequence authority is invalid: {authority}: {command}: {outcome_id}"
                )
            for record_schema in records.values():
                if not isinstance(record_schema, Mapping):
                    raise ContractFreezeError(
                        f"CLI JSON sequence record schema is invalid: "
                        f"{authority}: {command}: {outcome_id}"
                    )
                Draft202012Validator.check_schema(dict(record_schema))
            return copy.deepcopy(declared)
        if (
            kind not in {"cli-local-json-schema", "python-model"}
            or not isinstance(identity, str)
            or not identity
            or not isinstance(schema, Mapping)
        ):
            raise ContractFreezeError(
                f"CLI JSON output authority is invalid: {authority}: {command}: {outcome_id}"
            )
        Draft202012Validator.check_schema(dict(schema))
        return copy.deepcopy(declared)

    application = CLI_OPERATION_APPLICATIONS.get(authority)
    override = output_authorities.get(command)
    if override is None:
        candidates = operations_by_command.get((application or "", command), ())
        if len(candidates) != 1:
            raise ContractFreezeError(
                f"CLI JSON output has no exact implementation-owned authority: "
                f"{authority}: {command}: {len(candidates)} operation candidates"
            )
        return _operation_success_authority(candidates[0], openapi)
    if not isinstance(override, Mapping):
        raise ContractFreezeError(f"CLI output authority is invalid: {authority}: {command}")
    by_outcome = override.get("outcomes")
    selected = by_outcome.get(outcome_id) if isinstance(by_outcome, Mapping) else override
    if not isinstance(selected, Mapping):
        raise ContractFreezeError(
            f"CLI output authority has no outcome mapping: {authority}: {command}: {outcome_id}"
        )
    selected_kind = selected.get("kind")
    if selected_kind == "operation-response":
        operation_id = selected.get("operation_id")
        operation = operations_by_id.get((application or "", str(operation_id)))
        if operation is None or command not in operation.cli_commands:
            raise ContractFreezeError(
                f"CLI output references an unrelated operation: "
                f"{authority}: {command}: {operation_id}"
            )
        return _operation_success_authority(operation, openapi)
    if selected_kind == "openapi-schema":
        schema_name = selected.get("schema")
        components = cast(
            Mapping[str, object],
            cast(Mapping[str, object], openapi[application or ""])["components"],
        )
        schemas = cast(Mapping[str, object], components.get("schemas", {}))
        if not isinstance(schema_name, str) or schema_name not in schemas:
            raise ContractFreezeError(
                f"CLI output references an unknown OpenAPI schema: "
                f"{authority}: {command}: {schema_name}"
            )
        return {
            "kind": "openapi-schema",
            "application": application,
            "schema": schema_name,
            "definition": copy.deepcopy(schemas[schema_name]),
        }
    if selected_kind == "cli-local-json-schema":
        identity = selected.get("identity")
        schema = selected.get("schema")
        if not isinstance(identity, str) or not identity or not isinstance(schema, Mapping):
            raise ContractFreezeError(
                f"CLI-local output authority is invalid: {authority}: {command}"
            )
        Draft202012Validator.check_schema(dict(schema))
        return copy.deepcopy(selected)
    raise ContractFreezeError(
        f"CLI output authority kind is invalid: {authority}: {command}: {selected_kind}"
    )


def _apply_cli_result_contract(
    authority: str,
    root: dict[str, object],
    *,
    operations: Sequence[operation_qualification.Operation],
    openapi: Mapping[str, object],
    schema_documents: Mapping[str, object] | None = None,
) -> dict[str, object]:
    module = importlib.import_module(CLI_MODULES[authority])
    resolved_schema_documents = (
        _schema_documents() if schema_documents is None else schema_documents
    )
    declaration = getattr(module, "_CLI_RESULT_CONTRACT", None)
    if not isinstance(declaration, Mapping):
        raise ContractFreezeError(f"CLI has no implementation-owned result contract: {authority}")
    if declaration.get("schema") != CLI_RESULT_CONTRACT_SCHEMA:
        raise ContractFreezeError(f"CLI result contract schema is invalid: {authority}")
    if set(declaration) != {
        "schema",
        "identity_prefix",
        "default_profile",
        "profiles",
        "command_profiles",
        "command_overrides",
        "executable_groups",
        "outcome_selectors",
        "output_authorities",
        "version_distribution",
    }:
        raise ContractFreezeError(f"CLI result contract shape is invalid: {authority}")
    identity_prefix = declaration["identity_prefix"]
    default_profile = declaration["default_profile"]
    profiles = declaration["profiles"]
    command_profiles = declaration["command_profiles"]
    command_overrides = declaration["command_overrides"]
    executable_groups = declaration["executable_groups"]
    outcome_selectors = declaration["outcome_selectors"]
    output_authorities = declaration["output_authorities"]
    version_distribution = declaration["version_distribution"]
    if not isinstance(identity_prefix, str) or not identity_prefix:
        raise ContractFreezeError(f"CLI result identity prefix is invalid: {authority}")
    if not isinstance(default_profile, str) or not isinstance(profiles, Mapping):
        raise ContractFreezeError(f"CLI result profiles are invalid: {authority}")
    if not isinstance(command_profiles, Mapping) or not isinstance(command_overrides, Mapping):
        raise ContractFreezeError(f"CLI command result mappings are invalid: {authority}")
    if not isinstance(executable_groups, list) or not all(
        isinstance(item, str) for item in executable_groups
    ):
        raise ContractFreezeError(f"CLI executable-group declarations are invalid: {authority}")
    if not isinstance(outcome_selectors, Mapping) or not isinstance(output_authorities, Mapping):
        raise ContractFreezeError(f"CLI outcome/output declarations are invalid: {authority}")
    if version_distribution is not None and not isinstance(version_distribution, str):
        raise ContractFreezeError(f"CLI version distribution is invalid: {authority}")

    operations_by_command: dict[tuple[str, str], list[operation_qualification.Operation]] = (
        defaultdict(list)
    )
    operations_by_id: dict[tuple[str, str], operation_qualification.Operation] = {}
    for operation in operations:
        operations_by_id[(operation.application, operation.operation_id)] = operation
        for current in operation.cli_commands:
            operations_by_command[(operation.application, current)].append(operation)

    seen_commands: set[str] = set()
    seen_groups: set[str] = set()

    def walk(node: dict[str, object], path: tuple[str, ...]) -> None:
        command = "$root" if not path else " ".join(path)
        children = cast(Mapping[str, dict[str, object]], node.get("commands", {}))
        if children:
            seen_groups.add(command)
        executable = not children or command in executable_groups
        if executable:
            seen_commands.add(command)
            profile_name = command_profiles.get(command, default_profile)
            profile = profiles.get(profile_name) if isinstance(profile_name, str) else None
            if not isinstance(profile, Mapping):
                raise ContractFreezeError(
                    f"CLI command references an unknown result profile: "
                    f"{authority}: {command}: {profile_name}"
                )
            override = command_overrides.get(command, {})
            if not isinstance(override, Mapping):
                raise ContractFreezeError(
                    f"CLI command result override is invalid: {authority}: {command}"
                )
            result = _merge_cli_result_contract(profile, override)
            profile_id = result.pop("id", None)
            if set(result) != {
                "structured_output",
                "human_json_relationship",
                "success",
                "failures",
            }:
                raise ContractFreezeError(
                    f"CLI result profile shape is invalid: {authority}: {command}"
                )
            relationship = result.get("human_json_relationship")
            structured_output = result.get("structured_output")
            if not isinstance(profile_id, str) or not profile_id:
                raise ContractFreezeError(
                    f"CLI result profile identity is invalid: {authority}: {command}"
                )
            if relationship not in {
                "same-semantic-result",
                "mode-specific-results",
                "not-applicable",
            }:
                raise ContractFreezeError(
                    f"CLI human/JSON relationship is invalid: {authority}: {command}"
                )
            output_relationships: dict[object, str] = {
                "optional-json": "same-semantic-result",
                "mode-specific": "mode-specific-results",
                "always-json": "not-applicable",
                "none": "not-applicable",
            }
            expected_relationship = output_relationships.get(structured_output)
            if expected_relationship != relationship:
                raise ContractFreezeError(
                    f"CLI structured-output mode is invalid: {authority}: {command}"
                )
            for result_kind in ("success", "failures"):
                for outcome in cast(list[dict[str, object]], result[result_kind]):
                    selector = outcome_selectors.get(outcome["id"])
                    if not isinstance(selector, Mapping):
                        raise ContractFreezeError(
                            f"CLI outcome has no implementation-owned selector: "
                            f"{authority}: {command}: {outcome['id']}"
                        )
                    _validate_cli_outcome_selector(
                        authority,
                        command,
                        str(outcome["id"]),
                        selector,
                        cast(Sequence[Mapping[str, object]], node.get("parameters", [])),
                    )
                    outcome["selected_by"] = copy.deepcopy(selector)
                    for channel in ("stdout", "stderr"):
                        channel_contract = cast(dict[str, object], outcome[channel])
                        if "json" in channel_contract:
                            channel_contract["json"] = _resolve_cli_output_authority(
                                authority=authority,
                                command=command,
                                outcome_id=str(outcome["id"]),
                                declared=channel_contract["json"],
                                output_authorities=output_authorities,
                                operations_by_command=operations_by_command,
                                operations_by_id=operations_by_id,
                                openapi=openapi,
                                schema_documents=resolved_schema_documents,
                            )
            _validate_cli_result_outcomes(authority, command, "success", result.get("success"))
            _validate_cli_result_outcomes(authority, command, "failure", result.get("failures"))
            suffix = "root" if not path else "/".join(path)
            node["result_contract"] = {
                "identity": f"{identity_prefix}/{suffix}/v1",
                "profile_id": profile_id,
                **result,
            }
        for child_name, child in sorted(children.items()):
            walk(child, (*path, child_name))

    walk(root, ())
    declared_commands = set(command_profiles) | set(command_overrides)
    stale_commands = declared_commands - seen_commands
    if stale_commands:
        raise ContractFreezeError(
            f"CLI result declarations reference unknown commands: {authority}: "
            f"{sorted(stale_commands)}"
        )
    unknown_groups = set(executable_groups) - seen_groups
    if unknown_groups:
        raise ContractFreezeError(
            f"CLI executable-group declarations reference unknown groups: "
            f"{authority}: {sorted(unknown_groups)}"
        )
    stale_outputs = set(output_authorities) - seen_commands
    if stale_outputs:
        raise ContractFreezeError(
            f"CLI output declarations reference unknown commands: {authority}: "
            f"{sorted(stale_outputs)}"
        )
    controls = cast(list[dict[str, object]], root.get("terminating_controls", []))
    version_controls = [item for item in controls if item.get("id") == "version"]
    if bool(version_controls) != bool(version_distribution):
        raise ContractFreezeError(
            f"CLI version declaration differs from discovered control: {authority}"
        )
    for control in version_controls:
        stdout = cast(dict[str, object], control["stdout"])
        stdout["distribution"] = version_distribution
    return root


def _apply_cli_occurrence_authorities(
    authority: str,
    root: dict[str, object],
    *,
    operations: Sequence[operation_qualification.Operation],
    openapi: Mapping[str, object],
) -> dict[str, object]:
    """Resolve implementation-owned batch bindings against discovered commands and HTTP inputs."""

    module = importlib.import_module(CLI_MODULES[authority])
    declarations = getattr(module, "_CLI_OCCURRENCE_AUTHORITIES", {})
    if not isinstance(declarations, Mapping):
        raise ContractFreezeError(f"CLI occurrence authorities are invalid: {authority}")
    application = CLI_OPERATION_APPLICATIONS.get(authority)
    for command, bindings in declarations.items():
        node = root
        try:
            for part in command.split():
                node = cast(dict[str, dict[str, object]], node["commands"])[part]
        except (AttributeError, KeyError, TypeError) as exc:
            raise ContractFreezeError(
                f"CLI occurrence authority references an unknown command: {authority}: {command}"
            ) from exc
        if not isinstance(bindings, Mapping):
            raise ContractFreezeError(
                f"CLI occurrence bindings are invalid: {authority}: {command}"
            )
        for name, binding in bindings.items():
            if not isinstance(binding, Mapping) or set(binding) != {"operation_id", "parameter"}:
                raise ContractFreezeError(f"CLI occurrence binding is invalid: {command}: {name}")
            parameters = [
                item
                for item in cast(list[dict[str, object]], node["parameters"])
                if item["name"] == name and item.get("multiple") is True and item.get("nargs") == 1
            ]
            candidates = [
                operation
                for operation in operations
                if operation.application == application
                and operation.operation_id == binding["operation_id"]
                and command in operation.cli_commands
            ]
            if len(parameters) != 1 or len(candidates) != 1:
                raise ContractFreezeError(
                    f"CLI occurrence binding does not resolve uniquely: "
                    f"{authority}: {command}: {name}"
                )
            operation = candidates[0]
            operation_pointer = "/" + "/".join(
                part.replace("~", "~0").replace("/", "~1")
                for part in (
                    "external_contract",
                    "http_openapi",
                    operation.application,
                    "paths",
                    operation.path,
                    operation.method.casefold(),
                )
            )
            document = cast(
                Mapping[str, object],
                pointer_value({"external_contract": {"http_openapi": openapi}}, operation_pointer),
            )
            inputs = [
                index
                for index, item in enumerate(cast(list[dict[str, object]], document["parameters"]))
                if item.get("in") == "query" and item.get("name") == binding["parameter"]
            ]
            if len(inputs) != 1:
                raise ContractFreezeError(
                    f"CLI occurrence binding has no unique HTTP query parameter: {command}: {name}"
                )
            schema_pointer = f"{operation_pointer}/parameters/{inputs[0]}/schema"
            schema = cast(
                Mapping[str, object],
                pointer_value({"external_contract": {"http_openapi": openapi}}, schema_pointer),
            )
            # A repeated scalar option supplies the entire query array, never a nested member.
            variants = [(schema_pointer, schema)]
            if "anyOf" in schema:
                variants = [
                    (f"{schema_pointer}/anyOf/{index}", branch)
                    for index, branch in enumerate(
                        cast(list[Mapping[str, object]], schema["anyOf"])
                    )
                ]
            array_pointers = [
                pointer for pointer, branch in variants if branch.get("type") == "array"
            ]
            if len(array_pointers) != 1:
                raise ContractFreezeError(
                    f"CLI occurrence binding has no unique HTTP array: {command}: {name}"
                )
            parameters[0]["occurrences_authority"] = array_pointers[0]
    return root


def _cli_surfaces(
    operations: Sequence[operation_qualification.Operation] | None = None,
    openapi: Mapping[str, object] | None = None,
    schema_documents: Mapping[str, object] | None = None,
) -> dict[str, object]:
    resolved_operations = (
        operation_qualification.operation_matrix() if operations is None else operations
    )
    resolved_openapi = _openapi_surfaces() if openapi is None else openapi
    resolved_schemas = _schema_documents() if schema_documents is None else schema_documents
    surfaces = {
        "gogurt": _click_command(get_command(gogurt_app), name="gogurt"),
        "mango-fish": _argparse_command(mango_fish_parser()),
        "piggity": _click_command(get_command(piggity_app), name="piggity"),
        "riverhog-ftp-adapter": _argparse_command(ftp_adapter_parser()),
        "riverhog-recover": _argparse_command(recovery_parser()),
        "riverhog-storage-adapter-conformance": _argparse_command(storage_conformance_parser()),
        "riverhog-storage-adapter-filesystem-materialize": _argparse_command(
            filesystem_materialize_parser()
        ),
        "riverhog-storage-adapter-schemas": _argparse_command(storage_schemas_parser()),
        "stove0": _click_command(get_command(stove0_app), name="stove0"),
        "stove0-observer-conformance": _argparse_command(observer_conformance_parser()),
        "stove0-observer-schemas": _argparse_command(observer_schemas_parser()),
        "stove0-review-planning": _argparse_command(review_planning_parser()),
        "stove0-review-sampler-conformance": _argparse_command(sampler_conformance_parser()),
        "stove0-review-sampler-schemas": _argparse_command(sampler_schemas_parser()),
        "stove0-target-conformance": _argparse_command(target_conformance_parser()),
        "stove0-target-schemas": _argparse_command(target_schemas_parser()),
    }
    return {
        authority: _apply_cli_occurrence_authorities(
            authority,
            _apply_cli_result_contract(
                authority,
                surface,
                operations=resolved_operations,
                openapi=resolved_openapi,
                schema_documents=resolved_schemas,
            ),
            operations=resolved_operations,
            openapi=resolved_openapi,
        )
        for authority, surface in surfaces.items()
    }


def _console_script_registry(
    projection: Mapping[str, object],
    *,
    include_dispositions: bool = True,
) -> dict[str, object]:
    """Reconcile installed entry points without using dispositions as discovery input."""

    boundaries = cast(Mapping[str, object], projection["boundaries"])
    external = cast(Mapping[str, object], projection["external_contract"])
    cli_names = set(cast(Mapping[str, object], external["cli"]))
    exceptions = load_exceptions(CONTRACT_FREEZE_EXCEPTIONS)
    excluded = {
        item["candidate_id"]: item
        for item in exceptions["exclusion"]
        if item["candidate_id"].startswith("console-script:")
    }
    detections: list[dict[str, str]] = []
    resolutions: list[dict[str, str]] = []
    dispositions: list[dict[str, str]] = []
    protected_names: set[str] = set()
    for component_index, component in enumerate(
        cast(list[dict[str, object]], boundaries["components"])
    ):
        distribution = str(component["distribution"])
        for name, target in sorted(
            cast(Mapping[str, object], component["console_scripts"]).items()
        ):
            candidate_id = f"console-script:{distribution}:{name}"
            detection_id = f"installed-entry-point:{distribution}:{name}"
            pointer = (
                f"/boundaries/components/{component_index}/console_scripts/"
                f"{name.replace('~', '~0').replace('/', '~1')}"
            )
            detections.append(
                {
                    "id": detection_id,
                    "kind": "installed-entry-point",
                    "distribution": distribution,
                    "name": name,
                    "target": str(target),
                    "source_pointer": pointer,
                }
            )
            resolutions.append(
                {
                    "detection_id": detection_id,
                    "candidate_id": candidate_id,
                    "name": name,
                    "distribution": distribution,
                }
            )
            if not include_dispositions:
                continue
            if name in cli_names:
                if candidate_id in excluded:
                    raise ContractFreezeError(
                        f"protected CLI cannot also be excluded: {candidate_id}"
                    )
                protected_names.add(name)
                dispositions.append(
                    {
                        "candidate_id": candidate_id,
                        "disposition": "protected",
                        "policy_id": "compatibility/cli/v1",
                        "reason": "The installed entry point exposes a maintained CLI parser tree.",
                    }
                )
                continue
            exception = excluded.get(candidate_id)
            if exception is None:
                raise ContractFreezeError(
                    f"installed entry point lacks an explicit disposition: {candidate_id}"
                )
            dispositions.append(
                {
                    "candidate_id": candidate_id,
                    "disposition": "excluded",
                    "policy_id": exception["policy_id"],
                    "reason": exception["reason"],
                }
            )
    candidate_ids = {item["candidate_id"] for item in resolutions}
    stale = sorted(set(excluded) - candidate_ids) if include_dispositions else []
    if stale:
        raise ContractFreezeError(f"console-script exclusions are stale: {stale}")
    missing_cli = sorted(cli_names - protected_names) if include_dispositions else []
    if missing_cli:
        raise ContractFreezeError(f"maintained CLIs lack installed entry points: {missing_cli}")
    return {
        "detector": "release-installed-entry-point",
        "detections": detections,
        "resolutions": resolutions,
        "candidates": [
            {
                "id": item["candidate_id"],
                "distribution": item["distribution"],
                "name": item["name"],
            }
            for item in resolutions
        ],
        "dispositions": dispositions,
        "coverage": {
            "detected": len(detections),
            "resolved": len(resolutions),
            "protected": sum(item["disposition"] == "protected" for item in dispositions),
            "excluded": sum(item["disposition"] == "excluded" for item in dispositions),
            "unresolved": 0,
            "undispositioned": len(candidate_ids) - len(dispositions),
            "stale_exceptions": 0,
        },
    }


def _environment_inventory(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    detections = _environment_detections(projects)
    exceptions = load_exceptions(CONTRACT_FREEZE_EXCEPTIONS)
    resolution_by_detection = {item["detection_id"]: item for item in exceptions["resolution"]}
    detection_ids = {str(item["id"]) for item in detections}
    stale = sorted(set(resolution_by_detection) - detection_ids)
    if stale:
        raise ContractFreezeError(f"configuration resolution exceptions are stale: {stale}")
    redundant = sorted(
        str(item["id"])
        for item in detections
        if item["resolved_names"] and item["id"] in resolution_by_detection
    )
    if redundant:
        raise ContractFreezeError(f"configuration resolution exceptions are redundant: {redundant}")
    unresolved = sorted(
        str(item["id"])
        for item in detections
        if not item["resolved_names"] and item["id"] not in resolution_by_detection
    )
    if unresolved:
        raise ContractFreezeError(f"configuration reads are unresolved: {unresolved}")
    contracts: dict[tuple[str, str], dict[str, object]] = {}
    for detection in detections:
        consumer = str(detection["consumer"])
        for name in cast(list[str], detection["resolved_names"]):
            key = (consumer, name)
            current = contracts.setdefault(
                key,
                {
                    "id": f"{consumer}:environment:{name}",
                    "name": name,
                    "owner": consumer,
                    "consumers": [consumer],
                    "input_shape": "environment-string",
                    "default_expressions": [],
                    "bindings": [],
                },
            )
            cast(list[str], current["default_expressions"]).append(
                str(detection["default_expression"])
            )
            cast(list[dict[str, str]], current["bindings"]).append(
                {
                    "detection_id": str(detection["id"]),
                    "consumer": consumer,
                    "path": str(detection["path"]),
                    "scope": str(detection["scope"]),
                    "operation": str(detection["operation"]),
                    "expression": str(detection["expression"]),
                }
            )
    result = list(contracts.values())
    for contract in result:
        contract["default_expressions"] = sorted(
            set(cast(list[str], contract["default_expressions"]))
        )
        contract["bindings"] = sorted(
            cast(list[dict[str, str]], contract["bindings"]),
            key=lambda value: (
                value["consumer"],
                value["path"],
                value["scope"],
                value["expression"],
            ),
        )
    return sorted(result, key=lambda value: (str(value["owner"]), str(value["name"])))


def _environment_detections(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    return _cached_environment_detections(tuple(projects))


@cache
def _cached_environment_detections(
    projects: tuple[release_contract.Project, ...],
) -> list[dict[str, object]]:
    return discover_environment_reads(ROOT, projects)


def _environment_resolutions(
    projects: list[release_contract.Project],
) -> list[dict[str, str]]:
    detections = _environment_detections(projects)
    by_id = {str(item["id"]): item for item in detections}
    exceptions = load_exceptions(CONTRACT_FREEZE_EXCEPTIONS)
    result: list[dict[str, str]] = []
    for item in exceptions["resolution"]:
        detection = by_id.get(item["detection_id"])
        if detection is None:
            raise ContractFreezeError(
                f"configuration resolution exception is stale: {item['detection_id']}"
            )
        if detection["resolved_names"]:
            raise ContractFreezeError(
                f"configuration resolution exception is redundant: {item['detection_id']}"
            )
        result.append(dict(item))
    return sorted(result, key=lambda item: item["detection_id"])


def _environment_names(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    return [
        {
            key: item[key]
            for key in (
                "id",
                "name",
                "owner",
                "consumers",
                "input_shape",
                "default_expressions",
            )
        }
        for item in _environment_inventory(projects)
    ]


def _configuration_document_inventory(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    detections = _configuration_document_detections(projects)
    unresolved = [
        str(item["id"])
        for item in detections
        if item["authority_module"] is None or item["authority_qualname"] is None
    ]
    if unresolved:
        raise ContractFreezeError(f"configuration document reads are unresolved: {unresolved}")
    grouped: dict[tuple[str, str], dict[str, object]] = {}
    for detection in detections:
        module_name = str(detection["authority_module"])
        qualname = str(detection["authority_qualname"])
        authority_module = importlib.import_module(module_name)
        value: object = authority_module
        for part in qualname.split("."):
            value = getattr(value, part)
        source = (
            {
                "module": module_name,
                "symbol": qualname,
                "path": Path(str(authority_module.__file__)).resolve().relative_to(ROOT).as_posix(),
            }
            if isinstance(value, Mapping)
            else _source_ref(value)
        )
        owner = _source_owner(projects, source)
        key = (module_name, qualname)
        candidate_id = f"{owner}:configuration:{_semantic_symbol_id(qualname)}"
        current = grouped.setdefault(
            key,
            {
                "id": candidate_id,
                "owner": owner,
                "module": module_name,
                "qualname": qualname,
                "source": source,
                "consumers": [],
                "input_shapes": [],
                "detection_ids": [],
                "value": value,
            },
        )
        if current["owner"] != owner or current["source"] != source:
            raise ContractFreezeError(
                f"configuration authority resolves inconsistently: {module_name}.{qualname}"
            )
        cast(list[str], current["consumers"]).append(str(detection["consumer"]))
        cast(list[str], current["input_shapes"]).append(str(detection["input_shape"]))
        cast(list[str], current["detection_ids"]).append(str(detection["id"]))
    result = list(grouped.values())
    candidate_ids = [str(item["id"]) for item in result]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ContractFreezeError(
            "configuration authorities require distinct public semantic names"
        )
    for item in result:
        item["consumers"] = sorted(set(cast(list[str], item["consumers"])))
        item["input_shapes"] = sorted(set(cast(list[str], item["input_shapes"])))
        item["detection_ids"] = sorted(set(cast(list[str], item["detection_ids"])))
    return sorted(result, key=lambda item: str(item["id"]))


def _configuration_document_detections(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    return _cached_configuration_document_detections(tuple(projects))


@cache
def _cached_configuration_document_detections(
    projects: tuple[release_contract.Project, ...],
) -> list[dict[str, object]]:
    return discover_configuration_documents(ROOT, projects)


def _configuration_documents(
    projects: list[release_contract.Project],
) -> dict[str, object]:
    documents: dict[str, object] = {}
    for item in _configuration_document_inventory(projects):
        value = item["value"]
        if "json-schema" in cast(list[str], item["input_shapes"]):
            if not isinstance(value, Mapping):
                raise ContractFreezeError(
                    f"configuration JSON Schema is not a mapping: {item['id']}"
                )
            document: object = dict(value)
        else:
            model_json_schema = getattr(value, "model_json_schema", None)
            if not callable(model_json_schema):
                raise ContractFreezeError(
                    f"configuration model has no JSON Schema projection: {item['id']}"
                )
            document = model_json_schema(mode="validation")
        documents[str(item["id"])] = document
    return dict(sorted(documents.items()))


def _configuration_environment_patterns(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    del projects
    definitions = (
        (
            ARCHIVE_STORE_ENVIRONMENT_TEMPLATE,
            ARCHIVE_STORE_ENVIRONMENT_SETTINGS,
            "RIVERHOG_ARCHIVE_STORES",
            "_archive_store_environment_name",
        ),
        (
            RETRIEVAL_CACHE_STORE_ENVIRONMENT_TEMPLATE,
            RETRIEVAL_CACHE_STORE_ENVIRONMENT_SETTINGS,
            "RIVERHOG_RETRIEVAL_CACHE_STORES",
            "_retrieval_cache_store_environment_name",
        ),
    )
    return [
        {
            "id": f"riverhog-server:environment-pattern:{template}",
            "owner": "riverhog-server",
            "consumers": ["riverhog-server"],
            "template": template,
            "input_shape": "environment-string",
            "settings": list(settings),
            "parameters": {
                "store": {
                    "source": source,
                    "normalization": "uppercase-dashes-to-underscores",
                },
                "setting": list(settings),
            },
            "source_symbol": symbol,
        }
        for template, settings, source, symbol in definitions
    ]


def _schema_documents() -> dict[str, object]:
    documents: dict[str, object] = {}
    for path in sorted(ROOT.glob("**/*.schema.json")):
        if ".venv" in path.parts or ".git" in path.parts:
            continue
        document = json.loads(path.read_text(encoding="utf-8"))
        authority = document.get("$id")
        if not isinstance(authority, str) or not authority:
            raise ContractFreezeError(f"protocol schema has no stable $id: {path}")
        if authority in documents:
            raise ContractFreezeError(f"protocol schema $id is not unique: {authority}")
        documents[authority] = document
    bundles = {name: factory() for name, factory in PROCESS_SCHEMA_BUNDLES.items()}
    documents.update({f"generated:{name}": document for name, document in sorted(bundles.items())})
    return dict(sorted(documents.items()))


def _state_contract(config: dict[str, Any]) -> dict[str, object]:
    owners: list[dict[str, object]] = []
    for owner in config["state"]["owners"]:
        try:
            current = state_contract.project_owner(owner)
        except state_contract.StateContractError as exc:
            raise ContractFreezeError(str(exc)) from exc
        owners.append(current)
    return {"schema": config["state"]["schema"], "owners": owners}


def _openapi_surfaces() -> dict[str, object]:
    return {
        surface.name: surface.app.openapi()
        for surface in operation_qualification.application_surfaces()
    }


def _http_route_supplements(
    documents: Mapping[str, object],
    operations: Sequence[operation_qualification.Operation],
) -> list[dict[str, object]]:
    """Return semantic HTTP routes deliberately absent from OpenAPI."""

    documented: set[tuple[str, str, str]] = set()
    for application, document in documents.items():
        paths = cast(Mapping[str, object], cast(Mapping[str, object], document).get("paths", {}))
        for path, path_item in paths.items():
            for method in cast(Mapping[str, object], path_item):
                if str(method).casefold() in operation_qualification.SUPPORTED_ROUTE_METHODS:
                    documented.add((application, str(method).upper(), path))
    semantic_fields = (
        "application",
        "operation_id",
        "method",
        "path",
        "classification",
        "response_authority",
        "read_collection",
    )
    return [
        {
            key: getattr(operation, key)
            for key in semantic_fields
            if getattr(operation, key) is not None
        }
        for operation in operations
        if (operation.application, operation.method, operation.path) not in documented
    ]


def _source_ref(value: object) -> dict[str, str]:
    module: str
    candidate: object
    if inspect.ismodule(value):
        module = str(value.__name__)
        symbol = "<module>"
        candidate = value
    else:
        module = str(getattr(value, "__module__", type(value).__module__))
        symbol = str(getattr(value, "__qualname__", type(value).__qualname__))
        candidate = value if callable(value) else type(value)
    try:
        source = inspect.getsourcefile(candidate)
    except TypeError:
        source = None
    result = {"module": module, "symbol": symbol}
    if source is not None:
        path = Path(source).resolve()
        if path.is_relative_to(ROOT):
            result["path"] = path.relative_to(ROOT).as_posix()
    return result


def _source_owner(projects: list[release_contract.Project], source: Mapping[str, str]) -> str:
    path = source.get("path")
    if path is None:
        raise ContractFreezeError("public source has no repository-owned distribution")
    candidates = [
        project
        for project in projects
        if path == project.path or path.startswith(f"{project.path}/")
    ]
    if not candidates:
        raise ContractFreezeError(f"public source is outside every component: {path}")
    owner = max(candidates, key=lambda project: len(project.path))
    if sum(project.path == owner.path for project in candidates) != 1:
        raise ContractFreezeError(f"public source has ambiguous component ownership: {path}")
    return owner.name


def _linked_source_ref(value: object) -> dict[str, object]:
    """Locate a repository callable at the line inspected for this trace."""

    if not callable(value):
        raise ContractFreezeError("source binding is not callable")
    definition = inspect.unwrap(value)
    source: dict[str, object] = dict(_source_ref(definition))
    if "path" not in source:
        raise ContractFreezeError(f"callable has no repository source: {source}")
    source["line"] = inspect.getsourcelines(definition)[1]
    return source


def _operation_trace(external: Mapping[str, object]) -> list[dict[str, object]]:
    """Bind discovered operations to actual exported clients and command callbacks."""

    python = cast(Mapping[str, Mapping[str, object]], external["python"])
    exported_classes = {
        identity: getattr(importlib.import_module(str(item["module"])), str(item["name"]))
        for identity, item in python.items()
        if item["unit"] == "export"
        and cast(Mapping[str, object], item["contract"])["kind"] == "class"
    }
    surfaces = {surface.name: surface for surface in operation_qualification.application_surfaces()}
    records: list[dict[str, object]] = []
    for operation in operation_qualification.operation_matrix():
        record = asdict(operation)
        surface = surfaces[operation.application]
        client_bindings: list[dict[str, object]] = []
        if operation.client is not None:
            client_types = [
                client for client in surface.client_types if client.__name__ == operation.client
            ]
            if len(client_types) != 1:
                raise ContractFreezeError(f"ambiguous operation client: {operation.operation_id}")
            client_type = client_types[0]
            method = next(
                (
                    supplement.client_method
                    for supplement in surface.supplemental_operations
                    if supplement.operation_id == operation.operation_id
                ),
                operation.operation_id,
            )
            for identity, exported in exported_classes.items():
                if exported is not client_type:
                    continue
                member = f"{identity}.{method}"
                client_bindings.append(
                    {
                        "public_identity": member,
                        "source": _linked_source_ref(getattr(client_type, method)),
                    }
                )
        record["client_bindings"] = client_bindings
        record["cli_bindings"] = [
            {"command": command, "source": _linked_source_ref(callback)}
            for command, callback, _has_json in surface.cli_commands
            if command in operation.cli_commands
        ]
        records.append(record)
    return records


def _openapi_trace() -> list[dict[str, object]]:
    traced: list[dict[str, object]] = []
    for surface in operation_qualification.application_surfaces():
        routes: list[dict[str, object]] = []
        for route, path in operation_qualification._application_routes(surface.app):
            operation_id = getattr(route, "operation_id", None) or getattr(route, "name", None)
            endpoint = getattr(route, "endpoint", None)
            if not operation_id or endpoint is None:
                continue
            routes.append(
                {
                    "operation_id": operation_id,
                    "path": path,
                    "methods": sorted(getattr(route, "methods", ()) or ()),
                    "source": _linked_source_ref(endpoint),
                }
            )
        traced.append(
            {
                "id": f"openapi:{surface.name}",
                "source": _source_ref(type(surface.app)),
                "routes": sorted(
                    routes,
                    key=lambda item: (str(item["path"]), str(item["operation_id"])),
                ),
            }
        )
    return traced


def _configuration_trace(
    projects: list[release_contract.Project],
) -> list[dict[str, object]]:
    return [
        {
            "id": f"configuration:{item['id']}",
            "owner": item["owner"],
            "source": item["source"],
            "consumers": item["consumers"],
            "input_shapes": item["input_shapes"],
            "detection_ids": item["detection_ids"],
        }
        for item in _configuration_document_inventory(projects)
    ]


def _protocol_trace() -> list[dict[str, object]]:
    traced: list[dict[str, object]] = []
    for path in sorted(ROOT.glob("**/*.schema.json")):
        if ".venv" in path.parts or ".git" in path.parts:
            continue
        relative = path.relative_to(ROOT).as_posix()
        document = json.loads(path.read_text(encoding="utf-8"))
        authority = document.get("$id")
        if not isinstance(authority, str) or not authority:
            raise ContractFreezeError(f"protocol schema has no stable $id: {path}")
        traced.append(
            {
                "id": f"protocol:{authority}",
                "source": {"path": relative},
            }
        )
    traced.extend(
        {
            "id": f"protocol:generated:{name}",
            "source": _source_ref(factory),
        }
        for name, factory in sorted(PROCESS_SCHEMA_BUNDLES.items())
    )
    return traced


def _cli_trace(projects: list[release_contract.Project]) -> list[dict[str, object]]:
    traced: list[dict[str, object]] = []
    for name, module in sorted(CLI_MODULES.items()):
        source = _source_ref(importlib.import_module(module))
        traced.append(
            {
                "id": f"cli:{name}",
                "owner": _source_owner(projects, source),
                "source": source,
            }
        )
    return traced


def _python_trace(registry: Mapping[str, object]) -> list[dict[str, object]]:
    detections = cast(list[dict[str, object]], registry["detections"])
    return [
        {
            "id": f"python:{detection['distribution']}:{detection['module']}",
            "source": {
                "path": detection["path"],
                "module": detection["module"],
            },
        }
        for detection in detections
    ]


def _state_trace() -> list[dict[str, object]]:
    config = _project_config(ROOT / "release.toml")
    traced: list[dict[str, object]] = []
    for owner in config["state"]["owners"]:
        declarations: list[dict[str, str]] = []
        for module_name, symbol in state_contract.declaration_symbols(owner):
            module = importlib.import_module(module_name)
            source = _source_ref(getattr(module, symbol))
            if "path" not in source:
                source = _source_ref(module)
                source["symbol"] = symbol
            declarations.append(source)
        traced.append(
            {
                "id": f"state:{owner['id']}",
                "declarations": declarations,
                "fixtures": [
                    {
                        "path": path,
                        "sha256": hashlib.sha256((ROOT / path).read_bytes()).hexdigest(),
                    }
                    for path in owner["fixtures"]
                ],
            }
        )
    return traced


def _environment_trace() -> list[dict[str, object]]:
    projects = release_contract.validate_release_contract(ROOT)
    return [
        {
            "id": f"configuration-environment:{item['owner']}:{item['name']}",
            "bindings": item["bindings"],
        }
        for item in _environment_inventory(projects)
    ]


def _configuration_pattern_trace() -> list[dict[str, object]]:
    projects = release_contract.validate_release_contract(ROOT)
    patterns = _configuration_environment_patterns(projects)
    return [
        {
            "id": f"configuration-environment-pattern:{pattern['owner']}:{pattern['template']}",
            "bindings": [
                {
                    "consumer": cast(list[str], pattern["consumers"])[0],
                    "path": "riverhog/src/riverhog_core/runtime_config.py",
                    "expression": str(pattern["source_symbol"]),
                }
            ],
        }
        for pattern in patterns
    ]


def _configuration_registry(
    projects: list[release_contract.Project],
    *,
    include_dispositions: bool = True,
) -> dict[str, object]:
    contracts = _environment_inventory(projects)
    patterns = _configuration_environment_patterns(projects)
    detections = _environment_detections(projects)
    resolutions = _environment_resolutions(projects)
    records = [
        {
            key: contract[key]
            for key in (
                "id",
                "name",
                "owner",
                "consumers",
                "input_shape",
                "default_expressions",
            )
        }
        | {
            "detection_ids": sorted(
                {
                    str(binding["detection_id"])
                    for binding in cast(list[dict[str, str]], contract["bindings"])
                }
            ),
            "source_authority_id": (
                f"configuration-environment:{contract['owner']}:{contract['name']}"
            ),
        }
        for contract in contracts
    ]
    pattern_records = [
        {
            key: pattern[key]
            for key in (
                "id",
                "owner",
                "consumers",
                "template",
                "input_shape",
                "settings",
                "parameters",
            )
        }
        | {
            "source_authority_id": (
                f"configuration-environment-pattern:{pattern['owner']}:{pattern['template']}"
            )
        }
        for pattern in patterns
    ]
    candidates = [
        {
            "id": str(record["id"]),
            "kind": "configuration-environment",
            "authority": str(record["owner"]),
            "source_authority_id": str(record["source_authority_id"]),
        }
        for record in records
    ] + [
        {
            "id": str(record["id"]),
            "kind": "configuration-environment-pattern",
            "authority": str(record["owner"]),
            "source_authority_id": str(record["source_authority_id"]),
        }
        for record in pattern_records
    ]
    candidate_ids_by_detection: defaultdict[str, list[str]] = defaultdict(list)
    for record in records:
        for detection_id in cast(list[str], record["detection_ids"]):
            candidate_ids_by_detection[detection_id].append(str(record["id"]))
    resolution_exceptions = {item["detection_id"]: item for item in resolutions}
    resolved_detections = [
        {
            "detection_id": str(detection["id"]),
            "candidate_ids": sorted(candidate_ids_by_detection[str(detection["id"])]),
            **(
                {
                    "source_authority_id": resolution_exceptions[str(detection["id"])][
                        "source_authority_id"
                    ]
                }
                if str(detection["id"]) in resolution_exceptions
                else {}
            ),
        }
        for detection in detections
    ]
    dispositions = (
        [
            {
                "candidate_id": str(candidate["id"]),
                "disposition": "protected",
                "policy_id": "compatibility/configuration/v1",
                "reason": "The release-exposed implementation reads this configuration contract.",
            }
            for candidate in candidates
        ]
        if include_dispositions
        else []
    )
    return {
        "schema": CONFIGURATION_DISCOVERY_SCHEMA,
        "detector": "implementation-ast-environment-read",
        "detections": detections,
        "resolutions": resolved_detections,
        "resolution_exceptions": resolutions,
        "candidates": candidates,
        "dispositions": dispositions,
        "records": records,
        "patterns": pattern_records,
        "counts": {
            "detections": len(detections),
            "resolved_detections": len(resolved_detections),
            "resolution_exceptions": len(resolutions),
            "contracts": len(records),
            "patterns": len(pattern_records),
            "unique_environment_names": len({str(record["name"]) for record in records}),
            "by_owner": dict(sorted(Counter(str(record["owner"]) for record in records).items())),
        },
        "coverage": {
            "unowned": 0,
            "unconsumed": 0,
            "unresolved": 0,
            "duplicate_conflicts": 0,
            "stale_exceptions": 0,
            "redundant_exceptions": 0,
            "undispositioned": len(candidates) - len(dispositions),
        },
    }


def _configuration_document_registry(
    projects: list[release_contract.Project],
    *,
    include_dispositions: bool = True,
) -> dict[str, object]:
    detections = _configuration_document_detections(projects)
    inventory = _configuration_document_inventory(projects)
    candidate_by_detection = {
        detection_id: str(item["id"])
        for item in inventory
        for detection_id in cast(list[str], item["detection_ids"])
    }
    resolutions = [
        {
            "detection_id": str(detection["id"]),
            "candidate_id": candidate_by_detection[str(detection["id"])],
        }
        for detection in detections
    ]
    candidates = [
        {
            key: item[key]
            for key in (
                "id",
                "owner",
                "module",
                "qualname",
                "source",
                "consumers",
                "input_shapes",
                "detection_ids",
            )
        }
        for item in inventory
    ]
    dispositions = (
        [
            {
                "candidate_id": str(item["id"]),
                "disposition": "protected",
                "policy_id": "compatibility/configuration/v1",
                "reason": (
                    "A release-exposed runtime validator consumes this configuration authority."
                ),
            }
            for item in inventory
        ]
        if include_dispositions
        else []
    )
    return {
        "schema": CONFIGURATION_DISCOVERY_SCHEMA,
        "detector": "release-runtime-configuration-validator",
        "detections": detections,
        "resolutions": resolutions,
        "candidates": candidates,
        "dispositions": dispositions,
        "counts": {
            "detections": len(detections),
            "resolved_detections": len(resolutions),
            "contracts": len(candidates),
        },
        "coverage": {
            "detected": len(detections),
            "resolved": len(resolutions),
            "protected": len(dispositions),
            "excluded": 0,
            "unresolved": 0,
            "duplicate_conflicts": 0,
            "undispositioned": len(candidates) - len(dispositions),
        },
    }


def _authority_registry(
    projects: list[release_contract.Project], projection: Mapping[str, object]
) -> dict[str, object]:
    config = _project_config(ROOT / "release.toml")
    external = cast(Mapping[str, object], projection["external_contract"])
    state = cast(Mapping[str, object], external["durable_state"])
    component_authorities = sorted(project.name for project in projects)
    state_authorities = sorted(
        str(owner["id"]) for owner in cast(list[dict[str, object]], state["owners"])
    )
    declared = [
        {"id": name, "meaning": meaning}
        for name, meaning in sorted(cast(dict[str, str], config["contract_authorities"]).items())
    ]
    identities = [
        *component_authorities,
        *state_authorities,
        *(str(item["id"]) for item in declared),
    ]
    if len(identities) != len(set(identities)):
        raise ContractFreezeError("contract authority registry contains duplicate identities")
    return {
        "schema": AUTHORITY_REGISTRY_SCHEMA,
        "declaration_source": "release.toml",
        "component_authorities": component_authorities,
        "state_authorities": state_authorities,
        "declared_authorities": declared,
        "noncontractual_projection": [dict(item) for item in NONCONTRACTUAL_PROJECTION_AUTHORITIES],
    }


def _release_surface_registries(
    projects: list[release_contract.Project],
    projection: Mapping[str, object],
    *,
    include_dispositions: bool = True,
) -> dict[str, dict[str, object]]:
    """Return discovery stages with disposition as an independently removable phase."""

    return {
        "configuration": _configuration_registry(
            projects, include_dispositions=include_dispositions
        ),
        "configuration_documents": _configuration_document_registry(
            projects, include_dispositions=include_dispositions
        ),
        "console_scripts": _console_script_registry(
            projection, include_dispositions=include_dispositions
        ),
        "python_packages": _python_registry(projects, include_dispositions=include_dispositions),
    }


def trace_projection(projection: Mapping[str, object]) -> dict[str, object]:
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    external = cast(Mapping[str, object], projection["external_contract"])
    extents = cast(Mapping[str, object], external["extents"])
    decisions = cast(list[dict[str, object]], extents["decisions"])
    semantic_payload = json.dumps(projection, separators=(",", ":"), sort_keys=True).encode()
    rendered_payload = (json.dumps(projection, indent=2, sort_keys=True) + "\n").encode()
    projects = release_contract.validate_release_contract(ROOT)
    surface_registries = _release_surface_registries(projects, projection)
    configuration_registry = surface_registries["configuration"]
    configuration_document_registry = surface_registries["configuration_documents"]
    python_registry = surface_registries["python_packages"]
    console_script_registry = surface_registries["console_scripts"]
    authority_registry = _authority_registry(projects, projection)
    operation_records = _operation_trace(external)
    exception_source = (
        CONTRACT_FREEZE_EXCEPTIONS.relative_to(ROOT).as_posix()
        if CONTRACT_FREEZE_EXCEPTIONS.is_relative_to(ROOT)
        else str(CONTRACT_FREEZE_EXCEPTIONS)
    )
    sources: list[dict[str, object]] = [
        {"id": "release:release.toml", "source": {"path": "release.toml"}},
        *[
            cast(
                dict[str, object],
                {
                    "id": f"release-distribution:{project.name}",
                    "owner": project.name,
                    "source": {"path": f"{project.path}/pyproject.toml"},
                },
            )
            for project in projects
        ],
        {
            "id": "release-images:docker-bake",
            "source": {"path": "docker-bake.hcl"},
        },
        {
            "id": "release-installation:planner",
            "source": {
                "path": "scripts/release_installation.py",
                "symbol": "INSTALLATION_POLICY",
            },
        },
        {
            "id": "release-publication:planner",
            "source": {
                "path": "scripts/release.py",
                "symbol": "publication_contract",
            },
        },
        {
            "id": "audit:contract-freeze-exceptions",
            "source": {"path": exception_source},
        },
        *_openapi_trace(),
        *_cli_trace(projects),
        *_configuration_trace(projects),
        *_environment_trace(),
        *_configuration_pattern_trace(),
        *_protocol_trace(),
        *_python_trace(python_registry),
        *_state_trace(),
    ]
    source_ids = [str(source["id"]) for source in sources]
    if len(source_ids) != len(set(source_ids)):
        raise ContractFreezeError("contract trace source identities are not unique")
    invalid_resolution_sources = sorted(
        item["source_authority_id"]
        for item in cast(list[dict[str, str]], configuration_registry["resolution_exceptions"])
        if item["source_authority_id"] not in source_ids
    )
    if invalid_resolution_sources:
        raise ContractFreezeError(
            f"configuration resolution exceptions have unknown authorities: "
            f"{invalid_resolution_sources}"
        )
    source_kinds = dict(
        sorted(Counter(identity.split(":", 1)[0] for identity in source_ids).items())
    )
    segmented_extent_witnesses, segmented_links = extent_witnesses.bind_segmented_decisions(
        ROOT,
        decisions,
    )
    segmented_by_id = {str(link["id"]): link for link in segmented_links}
    segmented_extent_witness_link_count = sum(
        len(cast(list[str], link["segmented_extent_witnesses"])) for link in segmented_links
    )
    return {
        "schema": TRACE_SCHEMA,
        "contract_schema": projection["schema"],
        "boundary_canonical_sha256": _boundary_canonical_sha256(boundaries),
        "contract_canonical_sha256": hashlib.sha256(semantic_payload).hexdigest(),
        "contract_projection_sha256": hashlib.sha256(rendered_payload).hexdigest(),
        "sources": sources,
        "segmented_extent_witnesses": segmented_extent_witnesses,
        "extent_sources": [
            {
                "id": decision["id"],
                "owner": decision["owner"],
                "source_pointer": decision["source_pointer"],
                **(
                    {
                        "segmented_extent_witnesses": segmented_by_id[str(decision["id"])][
                            "segmented_extent_witnesses"
                        ]
                    }
                    if str(decision["id"]) in segmented_by_id
                    else {}
                ),
            }
            for decision in decisions
        ],
        "authority_registry": authority_registry,
        "configuration_registry": configuration_registry,
        "configuration_document_registry": configuration_document_registry,
        "python_registry": python_registry,
        "console_script_registry": console_script_registry,
        "operation_qualification": {
            "schema": operation_qualification.SCHEMA,
            "records": operation_records,
        },
        "coverage": {
            "source_authorities": len(sources),
            "source_kinds": source_kinds,
            "extent_decisions": len(decisions),
            "extent_source_links": len(decisions),
            "operation_qualification_records": len(operation_records),
            "segmented_decisions": len(segmented_links),
            "segmented_extent_witness_links": segmented_extent_witness_link_count,
            "segmented_extent_witnesses": len(segmented_extent_witnesses),
        },
    }


def contract_projection() -> dict[str, object]:
    projects = release_contract.validate_release_contract(ROOT)
    config = _project_config(ROOT / "release.toml")
    components = _component_boundaries(projects)
    python_surfaces = _python_surfaces(projects)
    http_openapi = _openapi_surfaces()
    protocol_schemas = _schema_documents()
    operations = operation_qualification.operation_matrix()
    cli_surfaces = _cli_surfaces(operations, http_openapi, protocol_schemas)
    published_distributions = {project.name for project in projects}
    for authority, root in cli_surfaces.items():
        controls = cast(
            Sequence[object], cast(Mapping[str, object], root).get("terminating_controls", ())
        )
        for control in controls:
            if not isinstance(control, Mapping) or control.get("id") != "version":
                continue
            stdout = control.get("stdout")
            distribution = stdout.get("distribution") if isinstance(stdout, Mapping) else None
            if distribution not in published_distributions:
                raise ContractFreezeError(
                    f"CLI version control does not resolve to a published distribution: "
                    f"{authority}: {distribution}"
                )
    external_contract: dict[str, object] = {
        "release": {
            "publication": release_contract.publication_contract(ROOT, projects),
            "compatibility": config["compatibility"],
        },
        "http_openapi": http_openapi,
        "http_route_supplements": _http_route_supplements(http_openapi, operations),
        "cli": cli_surfaces,
        "configuration_environment": _environment_names(projects),
        "configuration_environment_patterns": _configuration_environment_patterns(projects),
        "configuration_documents": _configuration_documents(projects),
        "protocol_schemas": protocol_schemas,
        "python": python_surfaces,
        "durable_state": _state_contract(config),
    }
    external_contract["extents"] = extent_contract.extent_projection(external_contract)
    boundaries: dict[str, object] = {
        "reference_policy": config["references"]["policy"],
        "components": components,
        "runtime_images": config["images"],
        "entry_point_extensions": _extension_points(projects),
        "process_extensions": _process_extensions(projects),
    }
    _require_declared_boundary_freeze(config, boundaries)
    return {
        "schema": SCHEMA,
        "series": "v1",
        "boundaries": boundaries,
        "external_contract": external_contract,
    }


def _render() -> str:
    projection = contract_projection()
    return canonical_bytes(
        build_atlas(
            projection,
            trace_projection(projection),
            component_descriptions=_component_descriptions(),
        ).root
    ).decode()


def _render_trace(projection: Mapping[str, object]) -> str:
    return json.dumps(trace_projection(projection), indent=2, sort_keys=True) + "\n"


def _generated_atlas() -> tuple[dict[str, object], dict[str, object], ContractAtlas]:
    projection = contract_projection()
    trace = trace_projection(projection)
    return (
        projection,
        trace,
        build_atlas(
            projection,
            trace,
            component_descriptions=_component_descriptions(),
        ),
    )


def _component_descriptions() -> dict[str, str]:
    """Return the release-owned descriptions used by human boundary navigation."""

    projects = release_contract.validate_release_contract(ROOT)
    descriptions = {project.name: project.description for project in projects}
    if any(not description.strip() for description in descriptions.values()):
        raise ContractFreezeError("every release component requires a maintained description")
    return descriptions


def _load_checked_projection(path: Path = OUTPUT) -> dict[str, object]:
    return reassemble_projection(load_atlas(path))


def _load_checked_trace(path: Path = OUTPUT) -> dict[str, object]:
    return reassemble_trace(load_atlas(path))


def _extent_diff(
    previous: Mapping[str, object] | None,
    current: Mapping[str, object],
) -> dict[str, dict[str, int]]:
    """Summarize the semantic extent diff by its owning contract boundary."""

    def decisions(projection: Mapping[str, object] | None) -> dict[str, dict[str, object]]:
        if projection is None:
            return {}
        try:
            values = cast(
                list[dict[str, object]],
                cast(
                    Mapping[str, object],
                    cast(Mapping[str, object], projection["external_contract"])["extents"],
                )["decisions"],
            )
        except (KeyError, TypeError):
            return {}
        return {str(value["id"]): value for value in values}

    old = decisions(previous)
    new = decisions(current)
    summary: dict[str, Counter[str]] = defaultdict(Counter)
    for identity in sorted(set(old) | set(new)):
        before = old.get(identity)
        after = new.get(identity)
        if before == after:
            continue
        value = after if after is not None else before
        if value is None:
            continue
        change = "added" if before is None else "removed" if after is None else "changed"
        summary[str(value["owner"])][change] += 1
    return {
        owner: {kind: counts.get(kind, 0) for kind in ("added", "changed", "removed")}
        for owner, counts in sorted(summary.items())
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check", help="Verify the checked-in v1 projection.")
    subparsers.add_parser("update", help="Replace the checked-in v1 projection.")
    subparsers.add_parser("summary", help="Print the v1 atlas roll-up and identities.")
    list_parser = subparsers.add_parser("list", help="List native semantic contract elements.")
    list_parser.add_argument("--authority", "--owner", dest="authority")
    list_parser.add_argument("--interface", "--kind", dest="interface")
    list_parser.add_argument("--disposition", choices=("protected", "excluded"))
    list_parser.add_argument("--policy")
    show_parser = subparsers.add_parser("show", help="Print one complete semantic dossier as JSON.")
    show_parser.add_argument("element_id")
    return parser


def _checked_atlas_matches(generated: ContractAtlas) -> bool:
    if not OUTPUT.is_file() or OUTPUT.read_bytes() != canonical_bytes(generated.root):
        return False
    expected = {OUTPUT.parent / relative for relative in generated.files}
    actual = (
        {path for path in ATLAS_DIRECTORY.rglob("*") if path.is_file()}
        if ATLAS_DIRECTORY.is_dir()
        else set()
    )
    if actual != expected:
        return False
    return all(
        path.read_bytes() == generated.files[path.relative_to(OUTPUT.parent).as_posix()]
        for path in expected
    )


def _write_atlas(atlas: ContractAtlas) -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    ATLAS_DIRECTORY.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_bytes(canonical_bytes(atlas.root))
    expected = {OUTPUT.parent / relative for relative in atlas.files}
    for relative, payload in atlas.files.items():
        path = OUTPUT.parent / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    for path in sorted(ATLAS_DIRECTORY.rglob("*"), reverse=True):
        if path.is_file() and path not in expected:
            path.unlink()
        elif path.is_dir() and not any(path.iterdir()):
            path.rmdir()
    if LEGACY_TRACE_OUTPUT.exists():
        LEGACY_TRACE_OUTPUT.unlink()


def _summary(atlas: ContractAtlas) -> dict[str, object]:
    root = atlas.root
    return {
        "schema": root["schema"],
        "series": root["series"],
        "identities": root["identities"],
        "counts": root["counts"],
        "discovery_anomalies": cast(Mapping[str, object], root["discovery"])["anomalies"],
        "atlas_root": cast(Mapping[str, object], root["atlas"])["root"],
    }


def _listed_elements(atlas: ContractAtlas, args: argparse.Namespace) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for element in cast(Sequence[dict[str, object]], atlas.root["elements"]):
        if args.authority and element["authority"] != args.authority:
            continue
        if args.interface and element["interface"] != args.interface:
            continue
        if args.disposition and element["disposition"] != args.disposition:
            continue
        if args.policy and args.policy not in cast(Sequence[str], element["policy_ids"]):
            continue
        result.append(element)
    if not args.disposition or args.disposition == "excluded":
        for exclusion in cast(
            Sequence[dict[str, object]],
            cast(Mapping[str, object], atlas.root["discovery"])["exclusions"],
        ):
            if args.authority or args.interface:
                continue
            if args.policy and exclusion["policy_id"] != args.policy:
                continue
            result.append(exclusion)
    return result


def _shown_element(atlas: ContractAtlas, element_id: str) -> dict[str, object]:
    element = next(
        (
            item
            for item in cast(Sequence[Mapping[str, object]], atlas.root["elements"])
            if item["id"] == element_id
        ),
        None,
    )
    if element is None:
        raise ContractFreezeError(f"unknown contract element: {element_id}")
    projection = cast(Mapping[str, object], atlas.root["projection"])
    trace = cast(Mapping[str, object], atlas.root["trace"])
    source_index = {
        str(source["id"]): source
        for source in cast(Sequence[Mapping[str, object]], atlas.root["sources"])
    }
    decisions = {
        str(decision["id"]): decision
        for decision in cast(
            Sequence[Mapping[str, object]],
            cast(
                Mapping[str, object],
                cast(Mapping[str, object], projection["external_contract"])["extents"],
            )["decisions"],
        )
    }
    return {
        "element": element,
        "values": [
            pointer_value(projection, pointer)
            for pointer in cast(Sequence[str], element["pointers"])
        ],
        "sources": [
            source_index[source] for source in cast(Sequence[str], element["source_authority_ids"])
        ],
        "extent_decisions": [
            decisions[identity] for identity in cast(Sequence[str], element["extent_decision_ids"])
        ],
        "trace_schema": trace["schema"],
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command in {"summary", "list", "show"}:
            checked = load_atlas(OUTPUT)
            payload = (
                _summary(checked)
                if args.command == "summary"
                else _listed_elements(checked, args)
                if args.command == "list"
                else _shown_element(checked, str(args.element_id))
            )
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0
        projection, _trace, atlas = _generated_atlas()
        extent_diff: dict[str, dict[str, int]] | None = None
        if args.command == "update":
            previous: Mapping[str, object] | None = None
            if OUTPUT.is_file():
                try:
                    loaded = json.loads(OUTPUT.read_bytes())
                    if loaded.get("schema") == SCHEMA:
                        previous = loaded
                    elif isinstance(loaded.get("projection"), Mapping):
                        # Representation migrations may intentionally make the checked human
                        # atlas fail the new validator. The monolithic closure still carries
                        # enough prior semantic data to report its extent delta safely.
                        previous = reassemble_projection(
                            ContractAtlas(root=cast(dict[str, object], loaded), files={})
                        )
                    else:
                        previous = _load_checked_projection()
                except (ContractAtlasError, AttributeError, KeyError, json.JSONDecodeError):
                    previous = None
            extent_diff = _extent_diff(previous, projection)
            _write_atlas(atlas)
        elif not _checked_atlas_matches(atlas) or LEGACY_TRACE_OUTPUT.exists():
            raise ContractFreezeError(
                "the v1 contract machine closure or human atlas is stale; "
                "run `make contract-freeze-update` and review the semantic diff"
            )
        root_bytes = canonical_bytes(atlas.root)
        print(
            json.dumps(
                {
                    "output": OUTPUT.relative_to(ROOT).as_posix(),
                    "sha256": hashlib.sha256(root_bytes).hexdigest(),
                    "contract_elements": cast(Mapping[str, object], atlas.root["counts"])[
                        "contract_elements"
                    ],
                    "atlas_documents": cast(Mapping[str, object], atlas.root["counts"])[
                        "atlas_documents"
                    ],
                    "identities": atlas.root["identities"],
                    "status": "updated" if args.command == "update" else "current",
                    **({"extent_diff": extent_diff} if extent_diff is not None else {}),
                },
                sort_keys=True,
            )
        )
        return 0
    except (
        ContractFreezeError,
        ContractAtlasError,
        DiscoveryError,
        extent_witnesses.ExtentWitnessError,
        release_contract.ReleaseError,
    ) as exc:
        print(f"contract freeze failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

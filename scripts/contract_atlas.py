#!/usr/bin/env python3
"""Build and validate the Riverhog v1 machine closure and human audit atlas."""

from __future__ import annotations

import copy
import hashlib
import json
import posixpath
import re
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, cast

import rfc8785

ROOT_SCHEMA = "riverhog-contract-machine-closure/v1"
ATLAS_SCHEMA = "riverhog-contract-human-atlas/v1"
CONTRACT_IDENTITY_SCHEMA = "riverhog-v1-semantic-contract/v1"
COVERAGE_IDENTITY_SCHEMA = "riverhog-v1-discovery-coverage/v1"
TRACE_IDENTITY_SCHEMA = "riverhog-v1-source-proof-trace/v1"
REPRESENTATION_IDENTITY_SCHEMA = "riverhog-v1-human-atlas-representation/v1"
DETECTOR_CLOSURE_SCHEMA = "riverhog-contract-detector-closure/v1"
ATLAS_DIRECTORY = "riverhog-v1"
# Human-review ergonomics target only; this is not a v1 contract extent or validity rule.
AUDIT_DOCUMENT_TARGET_BYTES = 128 * 1024
RELATIONSHIP_SCHEMA = "riverhog-contract-human-relationships/v1"
CONTRACT_MAP_SCHEMA = "riverhog-contract-human-map/v1"

_SCHEMA_MAPPING_KEYWORDS = frozenset(
    {"$defs", "definitions", "dependentSchemas", "patternProperties", "properties"}
)
_SCHEMA_SEQUENCE_KEYWORDS = frozenset({"allOf", "anyOf", "oneOf", "prefixItems"})
_SCHEMA_VALUE_KEYWORDS = frozenset(
    {
        "additionalItems",
        "additionalProperties",
        "contains",
        "contentSchema",
        "else",
        "if",
        "items",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)


def structural_json_schema(schema: object) -> object:
    """Remove prose annotations only at actual JSON Schema nodes."""

    if isinstance(schema, bool):
        return schema
    if not isinstance(schema, Mapping):
        raise ContractAtlasError("a structural JSON Schema value is not a schema")

    normalized: dict[str, object] = {}
    for key, value in schema.items():
        if key in {"title", "description"}:
            continue
        if key in _SCHEMA_MAPPING_KEYWORDS and isinstance(value, Mapping):
            normalized[key] = {
                str(name): structural_json_schema(child) for name, child in value.items()
            }
        elif key in _SCHEMA_SEQUENCE_KEYWORDS and isinstance(value, list):
            normalized[key] = [structural_json_schema(child) for child in value]
        elif key == "dependencies" and isinstance(value, Mapping):
            normalized[key] = {
                str(name): (
                    structural_json_schema(child)
                    if isinstance(child, (bool, Mapping))
                    else copy.deepcopy(child)
                )
                for name, child in value.items()
            }
        elif key in _SCHEMA_VALUE_KEYWORDS and isinstance(value, (bool, Mapping)):
            normalized[key] = structural_json_schema(value)
        elif key == "items" and isinstance(value, list):
            normalized[key] = [structural_json_schema(child) for child in value]
        else:
            normalized[key] = copy.deepcopy(value)
    return normalized


INTERFACE_LABELS: dict[str, str] = {
    "artifact-verification": "Artifact Verification",
    "cli": "CLI",
    "compatibility-guarantees": "Compatibility Guarantees",
    "configuration": "Configuration Documents",
    "configuration-environment": "Configuration Environment",
    "durable-state": "Durable State",
    "extent": "Extent Contract",
    "http-operations": "HTTP Operations",
    "http-schemas": "HTTP Schemas",
    "http-service-declaration": "HTTP Service Declaration",
    "http-security-schemes": "HTTP Security Schemes",
    "installation-roots": "Installation Roots",
    "process-protocol": "Process Protocol",
    "process-protocol-operations": "Process Protocol Operations",
    "process-protocol-schemas": "Process Protocol Schemas",
    "publication-locations": "Publication Locations",
    "python": "Python",
    "python-distributions": "Python Distributions",
    "release-artifacts": "Release Artifacts",
    "runtime-images": "Runtime Images",
    "schema": "Schemas",
    "versioning-tags": "Versioning and Tags",
}
RELEASE_INTERFACES = (
    "runtime-images",
    "python-distributions",
    "installation-roots",
    "release-artifacts",
    "publication-locations",
    "artifact-verification",
    "versioning-tags",
    "compatibility-guarantees",
)
RELEASE_INTERFACE_PREFIXES = {
    "artifact-verification": "Trust: ",
    "compatibility-guarantees": "Compatibility: ",
    "installation-roots": "Installation root: ",
    "publication-locations": "Coordinates: ",
    "python-distributions": "Python distribution: ",
    "release-artifacts": "Release artifact: ",
    "runtime-images": "Runtime image: ",
    "versioning-tags": "Versioning: ",
}
INTERFACE_ORDER = {
    interface: index
    for index, interface in enumerate(
        (
            *RELEASE_INTERFACES,
            "http-operations",
            "http-schemas",
            "http-service-declaration",
            "http-security-schemes",
            "process-protocol",
            "process-protocol-operations",
            "process-protocol-schemas",
            "schema",
        )
    )
}
INTERFACE_PURPOSES: dict[str, str] = {
    "artifact-verification": "External verification mechanisms for published artifacts.",
    "compatibility-guarantees": "Coordinated v1 compatibility promises.",
    "durable-state": "Persisted structures, schema heads, and v1 transition obligations.",
    "http-operations": "Callable HTTP operations.",
    "http-schemas": "Supporting HTTP data definitions; these are not callable operations.",
    "http-service-declaration": (
        "The HTTP service format and identity declaration; this is not a callable operation."
    ),
    "http-security-schemes": (
        "Supporting HTTP authorization definitions; these are not callable operations."
    ),
    "installation-roots": "Maintained installation roots and their exact installation forms.",
    "process-protocol": (
        "Cross-participant protocol identity, compatibility, and common acceptance rules. "
        "Exact operations and schemas are separately owned below."
    ),
    "process-protocol-operations": (
        "Callable operations in an independently deployed process protocol; method and path "
        "describe that protocol binding, not a product/service HTTP API."
    ),
    "process-protocol-schemas": "Structured values exchanged by a process protocol.",
    "publication-locations": "Stable externally used publication locations.",
    "python": "Declared public imports and their selected exact structural contracts.",
    "python-distributions": "Published Python distribution identities and artifact forms.",
    "release-artifacts": "Discrete files published with a coordinated release.",
    "runtime-images": "Published OCI runtime-image identities.",
    "schema": "Standalone structured-value contracts; these do not define an interaction.",
    "versioning-tags": "Coordinated version and immutable-tag semantics.",
}

EXCLUSION_POLICIES: tuple[dict[str, str], ...] = (
    {
        "id": "exclusion/process-launcher-not-cli/v1",
        "meaning": (
            "The installed entry point starts a separately inventoried process protocol and "
            "does not expose an independently maintained human or JSON CLI."
        ),
        "scope": "Installed service, adapter, observer, target, sampler, and effect launchers.",
    },
    {
        "id": "exclusion/python-package-no-declared-api/v1",
        "meaning": (
            "The installed Python package declares no explicit __all__ surface and therefore "
            "does not expose a freeze-protected Python API."
        ),
        "scope": "Importable packages carried by release wheels without declared exports.",
    },
)

DETECTORS: tuple[dict[str, str], ...] = (
    {"id": "cli-tree", "authority": "installed parser tree"},
    {"id": "configuration-document", "authority": "validated configuration schema"},
    {"id": "configuration-environment", "authority": "executable environment binding"},
    {"id": "durable-state", "authority": "checked current state baseline"},
    {"id": "extent", "authority": "exhaustive extent classifier"},
    {"id": "http-openapi", "authority": "running ASGI app"},
    {"id": "operation-matrix", "authority": "executable operation parity matrix"},
    {
        "id": "process-protocol-bundle",
        "authority": "generated process-protocol bundle",
    },
    {"id": "protocol-schema", "authority": "published standalone schema document"},
    {
        "id": "python-public-unit",
        "authority": "declared release-package export or directly declared public member",
    },
    {"id": "release-metadata", "authority": "validated release contract"},
)

QUALIFICATION_ROUTES: dict[str, tuple[str, ...]] = {
    "artifact-verification": ("make release-check", "make dist-smoke", "make build"),
    "cli": ("make dist-smoke", "make operation-qualification"),
    "compatibility-guarantees": ("make release-check", "make dist-smoke", "make build"),
    "configuration": ("make unit", "make compose-smoke"),
    "configuration-environment": ("make unit", "make compose-smoke"),
    "durable-state": ("make release-check", "make database-qualification"),
    "extent": ("make contract-freeze", "make operation-qualification"),
    "http-operations": ("make operation-qualification", "make compose-smoke"),
    "http-schemas": ("make operation-qualification", "make compose-smoke"),
    "http-security-schemes": ("make operation-qualification", "make compose-smoke"),
    "http-service-declaration": ("make operation-qualification", "make compose-smoke"),
    "installation-roots": ("make release-check", "make dist-smoke", "make build"),
    "process-protocol": ("make dist-smoke", "make build"),
    "process-protocol-operations": ("make dist-smoke", "make build"),
    "process-protocol-schemas": ("make dist-smoke", "make build"),
    "publication-locations": ("make release-check", "make dist-smoke", "make build"),
    "python": ("make dist-smoke", "make build"),
    "python-distributions": ("make release-check", "make dist-smoke", "make build"),
    "release-artifacts": ("make release-check", "make dist-smoke", "make build"),
    "runtime-images": ("make release-check", "make dist-smoke", "make build"),
    "schema": ("make dist-smoke", "make build"),
    "versioning-tags": ("make release-check", "make dist-smoke", "make build"),
    "excluded": ("make dist-smoke", "make build"),
}


class ContractAtlasError(RuntimeError):
    """The generated closure or atlas is incomplete, ambiguous, or stale."""


@dataclass(frozen=True)
class ContractAtlas:
    """One monolithic machine closure and its exact generated Markdown atlas."""

    root: dict[str, object]
    files: dict[str, bytes]


def canonical_bytes(value: object) -> bytes:
    """Return RFC 8785 canonical JSON bytes."""

    try:
        return rfc8785.dumps(cast(Any, value))
    except (rfc8785.CanonicalizationError, TypeError) as exc:
        raise ContractAtlasError(f"value is not RFC 8785 canonicalizable: {exc}") from exc


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _semantic_identity(
    projection: Mapping[str, object],
    policies: Mapping[str, object],
    unsafe_integer_paths: Sequence[str],
) -> dict[str, object]:
    """Return semantic contract identity without boundary-governance evidence."""

    return {
        "schema": CONTRACT_IDENTITY_SCHEMA,
        "series": projection["series"],
        "external_contract": projection["external_contract"],
        "policies": {key: value for key, value in policies.items() if key != "exclusion"},
        "unsafe_integer_paths": [
            path for path in unsafe_integer_paths if path.startswith("/external_contract/")
        ],
    }


def _encoded_json(value: object) -> tuple[object, list[str]]:
    """Return canonicalizable JSON plus pointers for integers outside I-JSON."""

    integer_paths: list[str] = []

    def encode(current: object, pointer: str) -> object:
        if isinstance(current, bool):
            return current
        if isinstance(current, int) and not (-(2**53) + 1 <= current <= (2**53) - 1):
            integer_paths.append(pointer)
            return str(current)
        if isinstance(current, Mapping):
            return {
                str(key): encode(child, f"{pointer}/{_escape_pointer(str(key))}")
                for key, child in current.items()
            }
        if isinstance(current, list):
            return [encode(child, f"{pointer}/{index}") for index, child in enumerate(current)]
        return current

    encoded = encode(value, "")
    return json.loads(canonical_bytes(encoded)), integer_paths


def normalized_json(value: object) -> object:
    """Normalize JSON semantics without distinguishing integral float spelling."""

    return _encoded_json(value)[0]


def _decode_unsafe_integers(value: object, paths: Sequence[str]) -> object:
    value = json.loads(json.dumps(value))
    for pointer in paths:
        parts = _pointer_parts(pointer)
        current = value
        for part in parts[:-1]:
            current = (
                current[int(part)]
                if isinstance(current, list)
                else cast(dict[str, object], current)[part]
            )
        if not parts:
            if not isinstance(current, str):
                raise ContractAtlasError("encoded root integer is not decimal text")
            value = int(current)
            continue
        terminal = parts[-1]
        if isinstance(current, list):
            encoded = current[int(terminal)]
            if not isinstance(encoded, str):
                raise ContractAtlasError(f"encoded integer is not decimal text at {pointer}")
            current[int(terminal)] = int(encoded)
        else:
            parent = cast(dict[str, object], current)
            encoded = parent[terminal]
            if not isinstance(encoded, str):
                raise ContractAtlasError(f"encoded integer is not decimal text at {pointer}")
            parent[terminal] = int(encoded)
    return value


def _escape_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _pointer_parts(pointer: str) -> list[str]:
    if not pointer:
        return []
    if not pointer.startswith("/"):
        raise ContractAtlasError(f"invalid JSON pointer: {pointer}")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def pointer_value(value: object, pointer: str) -> object:
    current = value
    for part in _pointer_parts(pointer):
        if isinstance(current, list):
            current = current[int(part)]
        elif isinstance(current, Mapping):
            current = current[part]
        else:
            raise ContractAtlasError(f"pointer crosses a scalar: {pointer}")
    return current


def _slug(value: str, *, limit: int = 88) -> str:
    rendered = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-") or "root"
    if len(rendered) <= limit:
        return rendered
    suffix = hashlib.sha256(value.encode()).hexdigest()[:10]
    return f"{rendered[: limit - 11].rstrip('-')}-{suffix}"


def _source_index(trace: Mapping[str, object]) -> dict[str, dict[str, object]]:
    sources = {
        str(source["id"]): dict(source)
        for source in cast(Sequence[Mapping[str, object]], trace["sources"])
    }
    sources.update(
        {
            "generator:contract-projection": {
                "id": "generator:contract-projection",
                "source": {"path": "scripts/contract_freeze.py", "symbol": "contract_projection"},
            },
            "operations:operation-matrix": {
                "id": "operations:operation-matrix",
                "source": {
                    "path": "scripts/operation_qualification.py",
                    "symbol": "operation_matrix",
                },
            },
            "extent:extent-contract": {
                "id": "extent:extent-contract",
                "source": {"path": "scripts/extent_contract.py", "symbol": "extent_projection"},
            },
        }
    )
    return dict(sorted(sources.items()))


def _policy_registry(projection: Mapping[str, object]) -> dict[str, object]:
    external = cast(Mapping[str, object], projection["external_contract"])
    release = cast(Mapping[str, object], external["release"])
    compatibility = cast(Mapping[str, object], release["compatibility"])
    publication = cast(Mapping[str, object], release["publication"])
    publication_policies = cast(Mapping[str, object], publication["policy"])
    publication_base = "/external_contract/release/publication"
    distribution_pointers = [
        f"{publication_base}/distributions/{_escape_pointer(str(name))}"
        for name in sorted(cast(Mapping[str, object], publication["distributions"]))
    ]
    image_pointers = [
        f"{publication_base}/runtime_images/{_escape_pointer(str(name))}"
        for name in sorted(cast(Mapping[str, object], publication["runtime_images"]))
    ]
    installation_pointers = [
        f"{publication_base}/installation_roots/{_escape_pointer(str(name))}"
        for name in sorted(cast(Mapping[str, object], publication["installation_roots"]))
    ]
    extents = cast(Mapping[str, object], external["extents"])
    return {
        "compatibility": [
            {
                "id": f"compatibility/{key.replace('_', '-')}/v1",
                "meaning": value,
                "applies_to": [f"/external_contract/release/compatibility/{key}"],
            }
            for key, value in sorted(compatibility.items())
        ],
        "publication": [
            {
                "id": "publication/role-retention/v1",
                "meaning": publication_policies["role_retention"],
                "source_pointer": f"{publication_base}/policy/role_retention",
                "applies_to": [
                    *distribution_pointers,
                    *image_pointers,
                    *installation_pointers,
                ],
            },
            {
                "id": "publication/platform-scope/v1",
                "meaning": publication_policies["platform_scope"],
                "source_pointer": f"{publication_base}/policy/platform_scope",
                "applies_to": [*image_pointers, *installation_pointers],
            },
            {
                "id": "publication/image-digest-scope/v1",
                "meaning": publication_policies["image_digest_scope"],
                "source_pointer": f"{publication_base}/policy/image_digest_scope",
                "applies_to": image_pointers,
            },
        ],
        "extent_principles": [
            {
                "id": f"extent-principle/{key.replace('_', '-')}/v1",
                "meaning": value,
                "applies_to": ["/external_contract/extents"],
            }
            for key, value in sorted(cast(Mapping[str, object], extents["principles"]).items())
        ],
        "extent_rules": [
            {
                "id": f"extent-rule/{key}",
                "meaning": value,
                "applies_to": ["/external_contract/extents/decisions"],
            }
            for key, value in sorted(cast(Mapping[str, object], extents["rules"]).items())
        ],
        "exclusion": list(EXCLUSION_POLICIES),
    }


def _compatibility_policies(interface: str) -> list[str]:
    mapping = {
        "cli": "compatibility/cli/v1",
        "configuration": "compatibility/configuration/v1",
        "configuration-environment": "compatibility/configuration/v1",
        "durable-state": "compatibility/durable-state/v1",
        "extent": "extent-principle/logical-totals/v1",
        "http-operations": "compatibility/http-api/v1",
        "http-schemas": "compatibility/http-api/v1",
        "http-security-schemes": "compatibility/http-api/v1",
        "http-service-declaration": "compatibility/http-api/v1",
        "process-protocol": "compatibility/components/v1",
        "process-protocol-operations": "compatibility/components/v1",
        "process-protocol-schemas": "compatibility/components/v1",
        "python": "compatibility/python-api/v1",
        "artifact-verification": "compatibility/components/v1",
        "compatibility-guarantees": "compatibility/components/v1",
        "installation-roots": "compatibility/components/v1",
        "publication-locations": "compatibility/components/v1",
        "python-distributions": "compatibility/components/v1",
        "release-artifacts": "compatibility/components/v1",
        "runtime-images": "compatibility/components/v1",
        "versioning-tags": "compatibility/components/v1",
        "schema": "compatibility/components/v1",
    }
    return [mapping[interface]]


def _element_id(authority: str, interface: str, title: str, pointers: Sequence[str]) -> str:
    digest = canonical_sha256([authority, interface, title, list(pointers)])[:10]
    return f"{interface}:{_slug(authority, limit=42)}:{_slug(title, limit=52)}:{digest}"


def _add_element(
    elements: list[dict[str, object]],
    *,
    authority: str,
    interface: str,
    title: str,
    pointers: Sequence[str],
    detector: str,
    source_ids: Iterable[str],
    details: Mapping[str, object] | None = None,
) -> dict[str, object]:
    normalized_pointers = sorted(set(pointers))
    item: dict[str, object] = {
        "id": _element_id(authority, interface, title, normalized_pointers),
        "authority": authority,
        "interface": interface,
        "title": title,
        "pointers": normalized_pointers,
        "detector": detector,
        "disposition": "protected",
        "policy_ids": _compatibility_policies(interface),
        "source_authority_ids": sorted({"generator:contract-projection", *source_ids}),
        "qualification_routes": list(QUALIFICATION_ROUTES[interface]),
        "extent_decision_ids": [],
        "related_element_ids": [],
    }
    if details:
        item["details"] = dict(details)
    elements.append(item)
    return item


def _walk_cli(
    elements: list[dict[str, object]],
    authority: str,
    node: Mapping[str, object],
    pointer: str,
    command_path: tuple[str, ...],
    *,
    source_id: str,
) -> None:
    name = str(node.get("name") or (command_path[-1] if command_path else authority))
    current_path = (
        (*command_path, name) if not command_path or command_path[-1] != name else command_path
    )
    pointers = [
        f"{pointer}/{key}"
        for key in ("name", "parameters", "terminating_controls", "result_contract")
        if key in node
    ]
    result_contract = node.get("result_contract")
    details: dict[str, object] = {"command_path": list(current_path)}
    if isinstance(result_contract, Mapping):
        details.update(
            {
                "executable": True,
                "result_identity": result_contract["identity"],
                "result_profile_id": result_contract["profile_id"],
                "structured_output": result_contract["structured_output"],
                "terminating_control_count": len(
                    cast(Sequence[object], node.get("terminating_controls", ()))
                ),
            }
        )
    _add_element(
        elements,
        authority=authority,
        interface="cli",
        title=" ".join(current_path),
        pointers=pointers,
        detector="cli-tree",
        source_ids=[source_id],
        details=details,
    )
    for child_name, child in sorted(
        cast(Mapping[str, Mapping[str, object]], node.get("commands", {})).items()
    ):
        _walk_cli(
            elements,
            authority,
            child,
            f"{pointer}/commands/{_escape_pointer(child_name)}",
            current_path,
            source_id=source_id,
        )


def _protocol_owner(authority: str, sources: Mapping[str, Mapping[str, object]]) -> str:
    source = sources.get(f"protocol:{authority}", {})
    location = cast(Mapping[str, object], source.get("source", {}))
    module = location.get("module")
    if isinstance(module, str) and module:
        return module.split(".", 1)[0].replace("_", "-")
    path = location.get("path")
    if isinstance(path, str):
        if path.startswith("packages/"):
            return path.split("/", 2)[1]
        marker = "/src/"
        if marker in path:
            return path.split(marker, 1)[1].split("/", 1)[0].replace("_", "-")
    return authority


def _publication_classification(role: str) -> str:
    classifications = {
        "deployed_implementation",
        "end_user_artifact",
        "reusable_library",
        "internal_build_unit",
        "reference_application",
        "reference_component",
    }
    if role not in classifications:
        raise ContractAtlasError(f"published unit has no release classification: {role}")
    return role


def _release_elements(elements: list[dict[str, object]], release: Mapping[str, object]) -> None:
    publication = cast(Mapping[str, object], release["publication"])
    base = "/external_contract/release/publication"
    common_sources = ["release:release.toml", "release-publication:planner"]

    for section, interface in (
        ("versioning", "versioning-tags"),
        ("coordinates", "publication-locations"),
    ):
        for name in sorted(cast(Mapping[str, object], publication[section])):
            _add_element(
                elements,
                authority="release",
                interface=interface,
                title=f"{section.title()}: {name.replace('_', ' ')}",
                pointers=[f"{base}/{section}/{_escape_pointer(name)}"],
                detector="release-metadata",
                source_ids=common_sources,
            )

    distributions = cast(Mapping[str, Mapping[str, object]], publication["distributions"])
    for name, unit in sorted(distributions.items()):
        role = str(unit["role"])
        _add_element(
            elements,
            authority="release",
            interface="python-distributions",
            title=f"Python distribution: {name}",
            pointers=[f"{base}/distributions/{_escape_pointer(name)}"],
            detector="release-metadata",
            source_ids=[*common_sources, f"release-distribution:{name}"],
            details={
                "classification": _publication_classification(role),
                "publication_kind": "distribution",
                "role": role,
                "semantic_owners": [name],
            },
        )
        elements[-1]["policy_ids"] = sorted(
            {
                *cast(Sequence[str], elements[-1]["policy_ids"]),
                "publication/role-retention/v1",
            }
        )

    for target, unit in sorted(
        cast(Mapping[str, Mapping[str, object]], publication["runtime_images"]).items()
    ):
        roots = [str(item) for item in cast(Sequence[object], unit["distribution_roots"])]
        root_roles = {str(distributions[root]["role"]) for root in roots}
        if unit["role"] == "product":
            classification = "product"
        elif len(root_roles) == 1:
            classification = _publication_classification(next(iter(root_roles)))
        else:
            raise ContractAtlasError(f"runtime image crosses publication roles: {target}")
        _add_element(
            elements,
            authority="release",
            interface="runtime-images",
            title=f"Runtime image: {target}",
            pointers=[f"{base}/runtime_images/{_escape_pointer(target)}"],
            detector="release-metadata",
            source_ids=[*common_sources, "release-images:docker-bake"],
            details={
                "classification": classification,
                "publication_kind": "runtime-image",
                "role": unit["role"],
                "semantic_owners": roots,
            },
        )
        elements[-1]["policy_ids"] = sorted(
            {
                *cast(Sequence[str], elements[-1]["policy_ids"]),
                "publication/image-digest-scope/v1",
                "publication/platform-scope/v1",
                "publication/role-retention/v1",
            }
        )

    for name, unit in sorted(
        cast(Mapping[str, Mapping[str, object]], publication["installation_roots"]).items()
    ):
        distribution = str(unit["distribution"])
        role = str(distributions[distribution]["role"])
        _add_element(
            elements,
            authority="release",
            interface="installation-roots",
            title=f"Installation root: {name}",
            pointers=[f"{base}/installation_roots/{_escape_pointer(name)}"],
            detector="release-metadata",
            source_ids=[
                *common_sources,
                "release-installation:planner",
                f"release-distribution:{distribution}",
            ],
            details={
                "classification": _publication_classification(role),
                "publication_kind": "installation-root",
                "role": role,
                "semantic_owners": [distribution],
            },
        )
        elements[-1]["policy_ids"] = sorted(
            {
                *cast(Sequence[str], elements[-1]["policy_ids"]),
                "publication/platform-scope/v1",
                "publication/role-retention/v1",
            }
        )

    for section, title_prefix, interface in (
        ("release_artifacts", "Release artifact", "release-artifacts"),
        ("trust", "Trust", "artifact-verification"),
    ):
        for name in sorted(cast(Mapping[str, object], publication[section])):
            _add_element(
                elements,
                authority="release",
                interface=interface,
                title=f"{title_prefix}: {name}",
                pointers=[f"{base}/{section}/{_escape_pointer(name)}"],
                detector="release-metadata",
                source_ids=common_sources,
            )

    compatibility = cast(Mapping[str, object], release["compatibility"])
    for policy_name in sorted(compatibility):
        element = _add_element(
            elements,
            authority="release",
            interface="compatibility-guarantees",
            title=f"Compatibility: {policy_name.replace('_', ' ')}",
            pointers=[f"/external_contract/release/compatibility/{_escape_pointer(policy_name)}"],
            detector="release-metadata",
            source_ids=["release:release.toml"],
        )
        element["policy_ids"] = [f"compatibility/{policy_name.replace('_', '-')}/v1"]


def _external_elements(
    projection: Mapping[str, object], trace: Mapping[str, object]
) -> list[dict[str, object]]:
    external = cast(Mapping[str, object], projection["external_contract"])
    sources = _source_index(trace)
    elements: list[dict[str, object]] = []

    _release_elements(elements, cast(Mapping[str, object], external["release"]))

    http = cast(Mapping[str, Mapping[str, object]], external["http_openapi"])
    for authority, document in sorted(http.items()):
        metadata_pointers = [
            f"/external_contract/http_openapi/{_escape_pointer(authority)}/{key}"
            for key in ("openapi", "info")
            if key in document
        ]
        _add_element(
            elements,
            authority=authority,
            interface="http-service-declaration",
            title=f"{authority} HTTP service",
            pointers=metadata_pointers,
            detector="http-openapi",
            source_ids=[f"openapi:{authority}"],
        )
        paths = cast(Mapping[str, Mapping[str, object]], document.get("paths", {}))
        for path, path_item in sorted(paths.items()):
            for method, operation in sorted(path_item.items()):
                pointer = (
                    f"/external_contract/http_openapi/{_escape_pointer(authority)}/paths/"
                    f"{_escape_pointer(path)}/{_escape_pointer(method)}"
                )
                if method.casefold() not in {
                    "delete",
                    "get",
                    "head",
                    "options",
                    "patch",
                    "post",
                    "put",
                }:
                    title = f"{path} {method} metadata"
                else:
                    title = f"{method.upper()} {path}"
                details = {
                    "method": method.upper(),
                    "path": path,
                    **(
                        {"operation_id": operation["operationId"]}
                        if isinstance(operation, Mapping) and "operationId" in operation
                        else {}
                    ),
                }
                _add_element(
                    elements,
                    authority=authority,
                    interface="http-operations",
                    title=title,
                    pointers=[pointer],
                    detector="http-openapi",
                    source_ids=[f"openapi:{authority}"],
                    details=details,
                )
        components = cast(Mapping[str, object], document.get("components", {}))
        component_interfaces = {
            "schemas": "http-schemas",
            "securitySchemes": "http-security-schemes",
        }
        unexpected_components = sorted(set(components) - set(component_interfaces))
        if unexpected_components:
            raise ContractAtlasError(
                f"OpenAPI components lack an explicit navigation home: "
                f"{authority}: {unexpected_components}"
            )
        for component_kind, values in sorted(components.items()):
            if not isinstance(values, Mapping):
                raise ContractAtlasError(
                    f"OpenAPI component collection is not a mapping: {authority}: {component_kind}"
                )
            for name in sorted(values):
                _add_element(
                    elements,
                    authority=authority,
                    interface=component_interfaces[component_kind],
                    title=f"{component_kind}: {name}",
                    pointers=[
                        f"/external_contract/http_openapi/{_escape_pointer(authority)}/"
                        f"components/{_escape_pointer(component_kind)}/{_escape_pointer(name)}"
                    ],
                    detector="http-openapi",
                    source_ids=[f"openapi:{authority}"],
                )

    for index, operation in enumerate(
        cast(Sequence[Mapping[str, object]], external["http_route_supplements"])
    ):
        authority = str(operation["application"])
        operation_id = str(operation["operation_id"])
        element = _add_element(
            elements,
            authority=authority,
            interface="http-operations",
            title=f"{operation['method']} {operation['path']}",
            pointers=[f"/external_contract/http_route_supplements/{index}"],
            detector="http-openapi",
            source_ids=[f"openapi:{authority}"],
            details={
                "method": operation["method"],
                "path": operation["path"],
                "operation_id": operation_id,
                "supplemental": True,
            },
        )
        element["policy_ids"] = ["compatibility/http-api/v1"]

    for authority, node in sorted(
        cast(Mapping[str, Mapping[str, object]], external["cli"]).items()
    ):
        source_id = f"cli:{authority}"
        owner = str(sources[source_id]["owner"])
        _walk_cli(
            elements,
            owner,
            node,
            f"/external_contract/cli/{_escape_pointer(authority)}",
            (),
            source_id=source_id,
        )

    for section in ("configuration_environment", "configuration_environment_patterns"):
        for index, item in enumerate(cast(Sequence[Mapping[str, object]], external[section])):
            name = str(item.get("name", item.get("template", f"{section}-{index}")))
            authority = str(item["owner"])
            _add_element(
                elements,
                authority=authority,
                interface="configuration-environment",
                title=name,
                pointers=[f"/external_contract/{section}/{index}"],
                detector="configuration-environment",
                source_ids=[
                    (f"configuration-environment-pattern:{authority}:{item['template']}")
                    if section.endswith("patterns")
                    else f"configuration-environment:{authority}:{name}",
                ],
                details={
                    "consumers": list(cast(Sequence[str], item["consumers"])),
                    "input_shape": item["input_shape"],
                    **(
                        {
                            "default_expressions": list(
                                cast(Sequence[str], item["default_expressions"])
                            )
                        }
                        if "default_expressions" in item
                        else {}
                    ),
                },
            )

    for authority in sorted(cast(Mapping[str, object], external["configuration_documents"])):
        source_id = f"configuration:{authority}"
        _add_element(
            elements,
            authority=str(sources[source_id]["owner"]),
            interface="configuration",
            title=f"{authority} configuration",
            pointers=[f"/external_contract/configuration_documents/{_escape_pointer(authority)}"],
            detector="configuration-document",
            source_ids=[source_id],
        )

    for schema_authority, document in sorted(
        cast(Mapping[str, Mapping[str, object]], external["protocol_schemas"]).items()
    ):
        authority = _protocol_owner(schema_authority, sources)
        base = f"/external_contract/protocol_schemas/{_escape_pointer(schema_authority)}"
        schemas = document.get("schemas")
        if schema_authority.startswith("generated:") and isinstance(schemas, Mapping):
            binding = document.get("http_binding")
            if not isinstance(binding, Mapping) or set(binding) != {"operations"}:
                raise ContractAtlasError(
                    "generated process protocol has an unexpected binding shape: "
                    f"{schema_authority}"
                )
            operations = binding["operations"]
            if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes)):
                raise ContractAtlasError(
                    f"generated process protocol operations are not a sequence: {schema_authority}"
                )
            metadata = [
                f"{base}/{_escape_pointer(key)}"
                for key in document
                if key not in {"http_binding", "schemas"}
            ]
            protocol_element = _add_element(
                elements,
                authority=authority,
                interface="process-protocol",
                title=f"{schema_authority} protocol",
                pointers=metadata,
                detector="process-protocol-bundle",
                source_ids=[f"protocol:{schema_authority}"],
            )
            related: list[dict[str, object]] = []
            for index, operation in enumerate(operations):
                if not isinstance(operation, Mapping):
                    raise ContractAtlasError(
                        "generated process protocol operation is not a mapping: "
                        f"{schema_authority}: {index}"
                    )
                operation_method = operation.get("method")
                operation_path = operation.get("path")
                if not isinstance(operation_method, str) or not isinstance(operation_path, str):
                    raise ContractAtlasError(
                        "generated process protocol operation lacks method/path: "
                        f"{schema_authority}: {index}"
                    )
                related.append(
                    _add_element(
                        elements,
                        authority=authority,
                        interface="process-protocol-operations",
                        title=f"{operation_method.upper()} {operation_path}",
                        pointers=[f"{base}/http_binding/operations/{index}"],
                        detector="process-protocol-bundle",
                        source_ids=[f"protocol:{schema_authority}"],
                        details={
                            "method": operation_method.upper(),
                            "path": operation_path,
                        },
                    )
                )
            for name in sorted(schemas):
                related.append(
                    _add_element(
                        elements,
                        authority=authority,
                        interface="process-protocol-schemas",
                        title=f"{schema_authority}: {name}",
                        pointers=[f"{base}/schemas/{_escape_pointer(name)}"],
                        detector="process-protocol-bundle",
                        source_ids=[f"protocol:{schema_authority}"],
                    )
                )
            cast(list[str], protocol_element["related_element_ids"]).extend(
                str(item["id"]) for item in related
            )
            for item in related:
                cast(list[str], item["related_element_ids"]).append(str(protocol_element["id"]))
        else:
            _add_element(
                elements,
                authority=authority,
                interface="schema",
                title=str(document.get("title", schema_authority)),
                pointers=[base],
                detector="protocol-schema",
                source_ids=[f"protocol:{schema_authority}"],
            )

    python_elements: dict[str, dict[str, object]] = {}
    for public_identity, surface in sorted(
        cast(Mapping[str, Mapping[str, object]], external["python"]).items()
    ):
        authority = str(surface["distribution"])
        module = str(surface["module"])
        unit = str(surface["unit"])
        item = _add_element(
            elements,
            authority=authority,
            interface="python",
            title=public_identity,
            pointers=[f"/external_contract/python/{_escape_pointer(public_identity)}"],
            detector="python-public-unit",
            source_ids=[f"python:{authority}:{module}"],
            details={
                "module": module,
                "public_identity": public_identity,
                "unit": unit,
                **({"owner": surface["owner"]} if "owner" in surface else {}),
            },
        )
        python_elements[public_identity] = item
    for public_identity, python_element in python_elements.items():
        python_details = cast(Mapping[str, object], python_element["details"])
        owner_identity = python_details.get("owner")
        if owner_identity is None:
            continue
        owner_item = python_elements.get(str(owner_identity))
        if owner_item is None:
            raise ContractAtlasError(
                f"Python public member lacks its exported owner: {public_identity}"
            )
        cast(list[str], python_element["related_element_ids"]).append(str(owner_item["id"]))
        cast(list[str], owner_item["related_element_ids"]).append(str(python_element["id"]))

    state = cast(Mapping[str, object], external["durable_state"])
    for index, state_owner in enumerate(cast(Sequence[Mapping[str, object]], state["owners"])):
        authority = str(state_owner["id"])
        base = f"/external_contract/durable_state/owners/{index}"
        structure = cast(Mapping[str, object], state_owner["structure"])
        structure_kind = str(structure.get("kind", ""))
        split_keys = {
            "relational-schema": {"tables", "unique_indexes"},
            "json-documents": {"documents"},
            "composite": {"units"},
        }.get(structure_kind)
        if split_keys is None:
            _add_element(
                elements,
                authority=authority,
                interface="durable-state",
                title=f"{authority} durable state",
                pointers=[base],
                detector="durable-state",
                source_ids=[f"state:{authority}"],
                details={"state_owner": authority, "state_unit": structure_kind},
            )
            continue
        collection_keys = {key for key, value in structure.items() if isinstance(value, list)}
        if collection_keys != split_keys:
            raise ContractAtlasError(
                f"durable-state structure has unknown collection fields: "
                f"{authority}: {sorted(collection_keys)}"
            )
        parent = _add_element(
            elements,
            authority=authority,
            interface="durable-state",
            title=f"{authority} durable-state identity",
            pointers=[
                *(
                    f"{base}/{_escape_pointer(str(key))}"
                    for key in state_owner
                    if key != "structure"
                ),
                *(
                    f"{base}/structure/{_escape_pointer(str(key))}"
                    for key in structure
                    if key not in split_keys
                ),
                *(
                    f"{base}/structure/{_escape_pointer(key)}"
                    for key in sorted(split_keys)
                    if not cast(Sequence[object], structure[key])
                ),
            ],
            detector="durable-state",
            source_ids=[f"state:{authority}"],
            details={"state_owner": authority, "state_unit": "identity"},
        )
        children: list[dict[str, object]] = []
        for collection_key in sorted(split_keys):
            items = cast(Sequence[Mapping[str, object]], structure[collection_key])
            for item_index, item in enumerate(items):
                unit_name = str(item.get("name", item.get("id", item_index)))
                unit_kind = (
                    "relational-table"
                    if collection_key == "tables"
                    else "unique-index"
                    if collection_key == "unique_indexes"
                    else "json-document"
                    if collection_key == "documents"
                    else str(item.get("kind", "durable-unit"))
                )
                child = _add_element(
                    elements,
                    authority=authority,
                    interface="durable-state",
                    title=f"{authority}: {unit_name}",
                    pointers=[f"{base}/structure/{collection_key}/{item_index}"],
                    detector="durable-state",
                    source_ids=[f"state:{authority}"],
                    details={
                        "state_owner": authority,
                        "state_unit": unit_kind,
                    },
                )
                children.append(child)
        cast(list[str], parent["related_element_ids"]).extend(
            str(child["id"]) for child in children
        )
        for child in children:
            cast(list[str], child["related_element_ids"]).append(str(parent["id"]))

    extents = cast(Mapping[str, object], external["extents"])
    for key in sorted(cast(Mapping[str, object], extents["principles"])):
        element = _add_element(
            elements,
            authority="extent-contract",
            interface="extent",
            title=f"Extent principle: {key.replace('_', ' ')}",
            pointers=[f"/external_contract/extents/principles/{_escape_pointer(key)}"],
            detector="extent",
            source_ids=["extent:extent-contract"],
        )
        element["policy_ids"] = [f"extent-principle/{key.replace('_', '-')}/v1"]
    for key in sorted(cast(Mapping[str, object], extents["rules"])):
        element = _add_element(
            elements,
            authority="extent-contract",
            interface="extent",
            title=f"Extent rule: {key.removesuffix('/v1').replace('-', ' ')}",
            pointers=[f"/external_contract/extents/rules/{_escape_pointer(key)}"],
            detector="extent",
            source_ids=["extent:extent-contract"],
        )
        element["policy_ids"] = [f"extent-rule/{key}"]
    return elements


def _attach_extent_decisions(
    elements: list[dict[str, object]], projection: Mapping[str, object]
) -> None:
    external = cast(Mapping[str, object], projection["external_contract"])
    decisions = cast(
        Sequence[Mapping[str, object]], cast(Mapping[str, object], external["extents"])["decisions"]
    )
    candidates = [
        (pointer, element)
        for element in elements
        if element["interface"] != "extent"
        for pointer in cast(Sequence[str], element["pointers"])
    ]
    unbound: dict[str, list[tuple[int, Mapping[str, object]]]] = defaultdict(list)
    for index, decision in enumerate(decisions):
        source_pointer = str(decision["source_pointer"])
        matching = [
            (len(pointer), element)
            for pointer, element in candidates
            if source_pointer == pointer or source_pointer.startswith(f"{pointer}/")
        ]
        if matching:
            _, element = max(matching, key=lambda item: item[0])
            cast(list[str], element["extent_decision_ids"]).append(str(decision["id"]))
            cast(list[str], element["policy_ids"]).append(f"extent-rule/{decision['rule']}")
        else:
            unbound[str(decision["owner"])].append((index, decision))
    for owner, values in sorted(unbound.items()):
        element = _add_element(
            elements,
            authority=owner,
            interface="extent",
            title=f"{owner} extent decisions",
            pointers=[f"/external_contract/extents/decisions/{index}" for index, _ in values],
            detector="extent",
            source_ids=["extent:extent-contract"],
        )
        element["extent_decision_ids"] = [str(value["id"]) for _, value in values]
        cast(list[str], element["policy_ids"]).extend(
            f"extent-rule/{value['rule']}" for _, value in values
        )
    for element in elements:
        element["extent_decision_ids"] = sorted(
            set(cast(Sequence[str], element["extent_decision_ids"]))
        )
        element["policy_ids"] = sorted(set(cast(Sequence[str], element["policy_ids"])))


def _link_operation_qualification(
    elements: list[dict[str, object]], trace: Mapping[str, object]
) -> None:
    http: dict[tuple[str, str], dict[str, object]] = {}
    cli: dict[tuple[str, str], dict[str, object]] = {}
    for element in elements:
        details = cast(Mapping[str, object], element.get("details", {}))
        operation_id = details.get("operation_id")
        if element["interface"] == "http-operations" and operation_id:
            http[(str(element["authority"]), str(operation_id))] = element
        elif element["interface"] == "cli":
            command_parts = cast(Sequence[str], details.get("command_path", ()))
            command = " ".join(command_parts)
            cli[(str(element["authority"]), command)] = element
            if len(command_parts) > 1:
                cli[(str(element["authority"]), " ".join(command_parts[1:]))] = element
    qualification = cast(Mapping[str, object], trace["operation_qualification"])
    operation_values = cast(Sequence[Mapping[str, object]], qualification["records"])
    operation_by_id = {
        (str(value["application"]), str(value["operation_id"])): value for value in operation_values
    }
    if len(operation_by_id) != len(operation_values):
        raise ContractAtlasError("operation qualification repeats an application operation")
    cli_authority = {
        "riverhog": "piggity",
        "riverhog-ftp-adapter": "riverhog-ftp-adapter",
        "stove0": "stove0-client",
    }
    for key, record in operation_by_id.items():
        http_element = http.get(key)
        if http_element is None:
            if record.get("classification") == "service-internal":
                continue
            raise ContractAtlasError(
                f"external operation qualification lacks an exact HTTP contract: {key}"
            )
        cast(list[str], http_element["source_authority_ids"]).append("operations:operation-matrix")
        details = cast(dict[str, object], http_element["details"])
        details["qualification_key"] = list(key)
        for command in cast(Sequence[str], record.get("cli_commands", ())):
            command_authority = cli_authority.get(key[0], key[0])
            cli_element = cli.get((command_authority, command))
            if cli_element is None:
                candidates = [
                    element
                    for element in elements
                    if element["interface"] == "cli"
                    and element["authority"] == command_authority
                    and " ".join(
                        cast(
                            Sequence[str],
                            cast(Mapping[str, object], element.get("details", {})).get(
                                "command_path", ()
                            ),
                        )
                    ).endswith(f" {command}")
                ]
                if len(candidates) == 1:
                    cli_element = candidates[0]
            if cli_element is None:
                raise ContractAtlasError(
                    f"operation qualification references an unknown CLI command: {key}: {command}"
                )
            cast(list[str], http_element["related_element_ids"]).append(str(cli_element["id"]))
            cast(list[str], cli_element["related_element_ids"]).append(str(http_element["id"]))
    for element in elements:
        element["source_authority_ids"] = sorted(
            set(cast(Sequence[str], element["source_authority_ids"]))
        )
        element["related_element_ids"] = sorted(
            set(cast(Sequence[str], element["related_element_ids"]))
        )


def _excluded_launchers(trace: Mapping[str, object]) -> list[dict[str, object]]:
    registry = cast(Mapping[str, object], trace["console_script_registry"])
    detections = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], registry["detections"])
    }
    resolutions = {
        str(item["candidate_id"]): item
        for item in cast(Sequence[Mapping[str, object]], registry["resolutions"])
    }
    exclusions: list[dict[str, object]] = []
    for disposition in cast(Sequence[Mapping[str, object]], registry["dispositions"]):
        if disposition["disposition"] != "excluded":
            continue
        candidate_id = str(disposition["candidate_id"])
        resolution = resolutions[candidate_id]
        detection = detections[str(resolution["detection_id"])]
        exclusions.append(
            {
                "id": f"excluded:{candidate_id}",
                "candidate_id": candidate_id,
                "kind": "console-script",
                "detector": str(registry["detector"]),
                "disposition": "excluded",
                "policy_id": disposition["policy_id"],
                "reason": disposition["reason"],
                "boundary_pointer": detection["source_pointer"],
                "source_authority_ids": ["release:release.toml"],
                "installed_target": detection["target"],
            }
        )
    return exclusions


def _excluded_python_packages(trace: Mapping[str, object]) -> list[dict[str, object]]:
    registry = cast(Mapping[str, object], trace["python_registry"])
    detections = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], registry["detections"])
    }
    resolutions = {
        str(item["candidate_id"]): item
        for item in cast(Sequence[Mapping[str, object]], registry["resolutions"])
    }
    exclusions: list[dict[str, object]] = []
    for disposition in cast(Sequence[Mapping[str, object]], registry["dispositions"]):
        if disposition["disposition"] != "excluded":
            continue
        candidate_id = str(disposition["candidate_id"])
        resolution = resolutions[candidate_id]
        detection = detections[str(resolution["detection_id"])]
        distribution = str(detection["distribution"])
        module = str(detection["module"])
        exclusions.append(
            {
                "id": f"excluded:{candidate_id}",
                "candidate_id": candidate_id,
                "kind": "python-package",
                "detector": registry["detector"],
                "disposition": "excluded",
                "policy_id": disposition["policy_id"],
                "reason": disposition["reason"],
                "boundary_pointer": detection["path"],
                "source_authority_ids": [f"python:{distribution}:{module}"],
                "installed_target": module,
            }
        )
    return exclusions


def _detector_meta_closure(
    projection: Mapping[str, object], trace: Mapping[str, object]
) -> dict[str, object]:
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    channels: list[dict[str, object]] = []
    for component in cast(Sequence[Mapping[str, object]], boundaries["components"]):
        distribution = str(component["distribution"])
        channels.append(
            {
                "id": f"distribution:{distribution}",
                "kind": "distribution",
                "detector": "release-metadata",
            }
        )
    python_registry = cast(Mapping[str, object], trace["python_registry"])
    python_dispositions = {
        str(item["candidate_id"]): item
        for item in cast(Sequence[Mapping[str, object]], python_registry["dispositions"])
    }
    for resolution in cast(Sequence[Mapping[str, object]], python_registry["resolutions"]):
        candidate_id = str(resolution["candidate_id"])
        disposition = python_dispositions[candidate_id]
        channels.append(
            {
                "id": candidate_id,
                "kind": "python",
                "detector": python_registry["detector"],
                "disposition": disposition["disposition"],
                "policy_id": disposition["policy_id"],
            }
        )
    console_registry = cast(Mapping[str, object], trace["console_script_registry"])
    console_dispositions = {
        str(item["candidate_id"]): item
        for item in cast(Sequence[Mapping[str, object]], console_registry["dispositions"])
    }
    for resolution in cast(Sequence[Mapping[str, object]], console_registry["resolutions"]):
        candidate_id = str(resolution["candidate_id"])
        disposition = console_dispositions[candidate_id]
        channels.append(
            {
                "id": candidate_id,
                "kind": "console-script",
                "detector": console_registry["detector"],
                "disposition": disposition["disposition"],
                "policy_id": disposition["policy_id"],
            }
        )
    for point in cast(Sequence[Mapping[str, object]], boundaries["entry_point_extensions"]):
        channels.append(
            {
                "id": f"extension-entry-point:{point['group']}",
                "kind": "extension-entry-point",
                "detector": "python-public-unit",
            }
        )
    for point in cast(Sequence[Mapping[str, object]], boundaries["process_extensions"]):
        channels.append(
            {
                "id": f"process-protocol:{point['name']}",
                "kind": "process-protocol",
                "detector": "process-protocol-bundle",
            }
        )
    for image_kind, images in sorted(
        cast(Mapping[str, object], boundaries["runtime_images"]).items()
    ):
        if isinstance(images, Mapping):
            for image in sorted(images):
                channels.append(
                    {
                        "id": f"runtime-image:{image_kind}:{image}",
                        "kind": "runtime-image",
                        "detector": "release-metadata",
                    }
                )
    ids = [str(channel["id"]) for channel in channels]
    detector_bindings: dict[str, list[str]] = defaultdict(list)
    exclusion_bindings: dict[str, list[str]] = defaultdict(list)
    for channel in channels:
        detector_bindings[str(channel["detector"])].append(str(channel["id"]))
        if channel.get("disposition") == "excluded":
            exclusion_bindings[str(channel["policy_id"])].append(str(channel["id"]))
    return {
        "schema": DETECTOR_CLOSURE_SCHEMA,
        "detector_bindings": {
            key: sorted(value) for key, value in sorted(detector_bindings.items())
        },
        "exclusion_bindings": {
            key: sorted(value) for key, value in sorted(exclusion_bindings.items())
        },
        "coverage": {
            "channels": len(channels),
            "by_kind": dict(sorted(Counter(str(item["kind"]) for item in channels).items())),
            "by_detector": dict(
                sorted(
                    Counter(
                        str(item["detector"]) for item in channels if "detector" in item
                    ).items()
                )
            ),
            "protected": sum(item.get("disposition") == "protected" for item in channels),
            "excluded": sum(item.get("disposition") == "excluded" for item in channels),
            "missing": 0,
            "duplicate": len(ids) - len(set(ids)),
            "stale": 0,
            "undecided": 0,
        },
    }


def _validate_staged_registry(
    registry: Mapping[str, object],
    *,
    label: str,
    require_one_resolution_per_detection: bool,
) -> None:
    detections = cast(Sequence[Mapping[str, object]], registry["detections"])
    candidates = cast(Sequence[Mapping[str, object]], registry["candidates"])
    dispositions = cast(Sequence[Mapping[str, object]], registry["dispositions"])
    detection_ids = [str(item["id"]) for item in detections]
    candidate_ids = [str(item["id"]) for item in candidates]
    disposition_ids = [str(item["candidate_id"]) for item in dispositions]
    resolutions = cast(Sequence[Mapping[str, object]], registry["resolutions"])
    resolution_pairs = [
        (str(item["detection_id"]), str(item["candidate_id"])) for item in resolutions
    ]
    resolution_detection_ids = [item[0] for item in resolution_pairs]
    resolution_candidate_ids = [item[1] for item in resolution_pairs]
    if (
        len(detection_ids) != len(set(detection_ids))
        or len(candidate_ids) != len(set(candidate_ids))
        or len(disposition_ids) != len(set(disposition_ids))
        or set(candidate_ids) != set(disposition_ids)
        or len(resolution_pairs) != len(set(resolution_pairs))
        or set(resolution_detection_ids) != set(detection_ids)
        or set(resolution_candidate_ids) != set(candidate_ids)
    ):
        raise ContractAtlasError(f"{label} discovery stages are incomplete or duplicated")
    if require_one_resolution_per_detection:
        if len(resolution_detection_ids) != len(set(resolution_detection_ids)):
            raise ContractAtlasError(f"{label} detection-to-candidate resolution is not exact")
    coverage = cast(Mapping[str, object], registry["coverage"])
    if (
        coverage.get("detected") != len(detections)
        or coverage.get("resolved") != len(cast(Sequence[object], registry["resolutions"]))
        or coverage.get("protected")
        != sum(item["disposition"] == "protected" for item in dispositions)
        or coverage.get("excluded")
        != sum(item["disposition"] == "excluded" for item in dispositions)
        or coverage.get("undispositioned") != 0
    ):
        raise ContractAtlasError(f"{label} discovery coverage is stale")


def _counts(
    elements: Sequence[Mapping[str, object]], exclusions: Sequence[Mapping[str, object]] = ()
) -> dict[str, object]:
    return {
        "contract_elements": len(elements),
        "excluded_candidates": len(exclusions),
        "extent_decisions": sum(
            len(cast(Sequence[object], item["extent_decision_ids"])) for item in elements
        ),
        "by_authority": dict(sorted(Counter(str(item["authority"]) for item in elements).items())),
        "by_interface": dict(sorted(Counter(str(item["interface"]) for item in elements).items())),
        "by_policy": dict(
            sorted(
                Counter(
                    [
                        policy
                        for item in elements
                        for policy in cast(Sequence[str], item["policy_ids"])
                    ]
                    + [str(item["policy_id"]) for item in exclusions]
                ).items()
            )
        ),
        "by_detector": dict(sorted(Counter(str(item["detector"]) for item in elements).items())),
        "by_source_authority": dict(
            sorted(
                Counter(
                    source
                    for item in [*elements, *exclusions]
                    for source in cast(Sequence[str], item["source_authority_ids"])
                ).items()
            )
        ),
        "by_qualification_route": dict(
            sorted(
                Counter(
                    route
                    for item in elements
                    for route in cast(Sequence[str], item["qualification_routes"])
                ).items()
            )
        ),
    }


def _terminal_pointers(value: object, pointer: str = "") -> list[str]:
    if isinstance(value, Mapping) and value:
        return [
            child
            for key, item in value.items()
            for child in _terminal_pointers(item, f"{pointer}/{_escape_pointer(str(key))}")
        ]
    if isinstance(value, list) and value:
        return [
            child
            for index, item in enumerate(value)
            for child in _terminal_pointers(item, f"{pointer}/{index}")
        ]
    return [pointer]


def _projection_coverage(
    elements: Sequence[Mapping[str, object]],
    policies: Mapping[str, object],
    projection: Mapping[str, object],
    noncontractual_projection: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    pointers = [pointer for item in elements for pointer in cast(Sequence[str], item["pointers"])]
    policy_pointers = [
        str(policy["source_pointer"])
        for values in policies.values()
        for policy in cast(Sequence[Mapping[str, object]], values)
        if "source_pointer" in policy
    ]
    if len(policy_pointers) != len(set(policy_pointers)):
        raise ContractAtlasError("policy source ownership is duplicated")
    noncontractual_pointers = [
        pointer
        for item in noncontractual_projection
        for pointer in cast(Sequence[str], item["pointers"])
    ]
    terminals = _terminal_pointers(projection)
    extent_prefix = "/external_contract/extents/decisions/"
    noncontractual_terminals = [
        pointer
        for pointer in terminals
        if any(
            pointer == excluded or pointer.startswith(f"{excluded}/")
            for excluded in noncontractual_pointers
        )
    ]
    semantic_terminals = [
        pointer
        for pointer in terminals
        if not pointer.startswith(extent_prefix)
        and not any(
            pointer == excluded or pointer.startswith(f"{excluded}/")
            for excluded in noncontractual_pointers
        )
    ]
    owned = {
        terminal: [
            pointer
            for pointer in [*pointers, *policy_pointers]
            if terminal == pointer or terminal.startswith(f"{pointer}/")
        ]
        for terminal in semantic_terminals
    }
    decisions = cast(
        Sequence[Mapping[str, object]],
        cast(
            Mapping[str, object],
            cast(Mapping[str, object], projection["external_contract"])["extents"],
        )["decisions"],
    )
    declared_decision_ids = [str(item["id"]) for item in decisions]
    represented_decision_ids = [
        identity
        for item in elements
        for identity in cast(Sequence[str], item["extent_decision_ids"])
    ]
    return {
        "projection_terminals": len(terminals),
        "semantic_terminals": len(semantic_terminals),
        "policy_terminals": sum(
            any(
                terminal == pointer or terminal.startswith(f"{pointer}/")
                for pointer in policy_pointers
            )
            for terminal in semantic_terminals
        ),
        "noncontractual_terminals": len(noncontractual_terminals),
        "extent_decisions": len(declared_decision_ids),
        "missing": sum(not owners for owners in owned.values())
        + len(set(declared_decision_ids) - set(represented_decision_ids)),
        "multiply_represented": sum(len(owners) > 1 for owners in owned.values())
        + len(represented_decision_ids)
        - len(set(represented_decision_ids)),
        "stale": len(set(represented_decision_ids) - set(declared_decision_ids)),
    }


def _validate_authority_registry(
    elements: Sequence[Mapping[str, object]],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
) -> Sequence[Mapping[str, object]]:
    registry = cast(Mapping[str, object], trace["authority_registry"])
    if registry.get("schema") != "riverhog-contract-authority-registry/v1":
        raise ContractAtlasError("contract authority registry has another schema")
    declared = cast(Sequence[Mapping[str, object]], registry["declared_authorities"])
    declared_ids = [str(item["id"]) for item in declared]
    if len(declared_ids) != len(set(declared_ids)) or any(
        not str(item.get("meaning", "")).strip() for item in declared
    ):
        raise ContractAtlasError("declared contract authorities are incomplete")
    allowed = {
        *cast(Sequence[str], registry["component_authorities"]),
        *cast(Sequence[str], registry["state_authorities"]),
        *declared_ids,
    }
    unknown = sorted({str(item["authority"]) for item in elements} - allowed)
    if unknown:
        raise ContractAtlasError(f"contract elements lack legitimate authorities: {unknown}")

    noncontractual = cast(Sequence[Mapping[str, object]], registry["noncontractual_projection"])
    record_ids = [str(item["id"]) for item in noncontractual]
    pointers = [
        str(pointer)
        for item in noncontractual
        for pointer in cast(Sequence[str], item.get("pointers", ()))
    ]
    if (
        len(record_ids) != len(set(record_ids))
        or len(pointers) != len(set(pointers))
        or any(not str(item.get("reason", "")).strip() for item in noncontractual)
    ):
        raise ContractAtlasError("non-contractual projection dispositions are incomplete")
    for pointer in pointers:
        pointer_value(projection, pointer)
    owned_pointers = [
        str(pointer) for item in elements for pointer in cast(Sequence[str], item["pointers"])
    ]
    overlap = sorted(
        pointer
        for pointer in pointers
        if any(
            pointer == owned or pointer.startswith(f"{owned}/") or owned.startswith(f"{pointer}/")
            for owned in owned_pointers
        )
    )
    if overlap:
        raise ContractAtlasError(f"non-contractual projection is also owned as contract: {overlap}")
    return noncontractual


def _validate_process_protocol_units(
    elements: Sequence[Mapping[str, object]], projection: Mapping[str, object]
) -> None:
    external = cast(Mapping[str, object], projection["external_contract"])
    documents = cast(Mapping[str, Mapping[str, object]], external["protocol_schemas"])
    by_pointer = {
        str(pointer): item for item in elements for pointer in cast(Sequence[str], item["pointers"])
    }
    for schema_authority, document in documents.items():
        base = f"/external_contract/protocol_schemas/{_escape_pointer(schema_authority)}"
        schemas = document.get("schemas")
        if not schema_authority.startswith("generated:") or not isinstance(schemas, Mapping):
            item = by_pointer.get(base)
            if item is None or item["interface"] != "schema":
                raise ContractAtlasError(
                    f"standalone schema lacks exact schema ownership: {schema_authority}"
                )
            continue

        binding = document.get("http_binding")
        if not isinstance(binding, Mapping) or set(binding) != {"operations"}:
            raise ContractAtlasError(
                f"generated process protocol binding shape is unresolved: {schema_authority}"
            )
        operations = cast(Sequence[Mapping[str, object]], binding["operations"])
        metadata_pointers = {
            f"{base}/{_escape_pointer(key)}"
            for key in document
            if key not in {"http_binding", "schemas"}
        }
        parent_candidates = {
            str(item["id"]): item
            for pointer in metadata_pointers
            if (item := by_pointer.get(pointer)) is not None
        }
        if len(parent_candidates) != 1:
            raise ContractAtlasError(
                f"process protocol metadata lacks one exact owner: {schema_authority}"
            )
        parent = next(iter(parent_candidates.values()))
        if (
            parent["interface"] != "process-protocol"
            or set(cast(Sequence[str], parent["pointers"])) != metadata_pointers
            or any(
                pointer.startswith(f"{base}/http_binding") or pointer.startswith(f"{base}/schemas")
                for pointer in cast(Sequence[str], parent["pointers"])
            )
        ):
            raise ContractAtlasError(
                f"process protocol parent duplicates operation/schema semantics: {schema_authority}"
            )

        children: list[Mapping[str, object]] = []
        for index, operation in enumerate(operations):
            pointer = f"{base}/http_binding/operations/{index}"
            item = by_pointer.get(pointer)
            details = cast(Mapping[str, object], item.get("details", {})) if item else {}
            if (
                item is None
                or item["interface"] != "process-protocol-operations"
                or details.get("method") != str(operation["method"]).upper()
                or details.get("path") != operation["path"]
            ):
                raise ContractAtlasError(
                    f"process protocol operation lacks exact ownership: {schema_authority}: {index}"
                )
            children.append(item)
        for name in schemas:
            pointer = f"{base}/schemas/{_escape_pointer(str(name))}"
            item = by_pointer.get(pointer)
            if item is None or item["interface"] != "process-protocol-schemas":
                raise ContractAtlasError(
                    f"process protocol schema lacks exact ownership: {schema_authority}: {name}"
                )
            children.append(item)
        child_ids = {str(item["id"]) for item in children}
        if set(cast(Sequence[str], parent["related_element_ids"])) != child_ids or any(
            str(parent["id"]) not in cast(Sequence[str], item["related_element_ids"])
            for item in children
        ):
            raise ContractAtlasError(
                f"process protocol child references are incomplete: {schema_authority}"
            )


def _validate_python_units(
    elements: Sequence[Mapping[str, object]],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
) -> None:
    external = cast(Mapping[str, object], projection["external_contract"])
    surfaces = cast(Mapping[str, Mapping[str, object]], external["python"])
    python_elements = [item for item in elements if item["interface"] == "python"]
    expected_pointers = {
        f"/external_contract/python/{_escape_pointer(public_identity)}": public_identity
        for public_identity in surfaces
    }
    actual_by_pointer = {
        str(cast(Sequence[str], item["pointers"])[0]): item for item in python_elements
    }
    if (
        len(python_elements) != len(surfaces)
        or any(len(cast(Sequence[str], item["pointers"])) != 1 for item in python_elements)
        or set(actual_by_pointer) != set(expected_pointers)
    ):
        raise ContractAtlasError("Python exact-unit projection and atlas differ")

    element_by_identity: dict[str, Mapping[str, object]] = {}
    expected_candidates: set[str] = set()
    for pointer, public_identity in expected_pointers.items():
        surface = surfaces[public_identity]
        item = actual_by_pointer[pointer]
        distribution = str(surface["distribution"])
        module = str(surface["module"])
        details = cast(Mapping[str, object], item.get("details", {}))
        if (
            item["authority"] != distribution
            or item["title"] != public_identity
            or details.get("module") != module
            or details.get("public_identity") != public_identity
            or details.get("unit") != surface["unit"]
            or f"python:{distribution}:{module}"
            not in cast(Sequence[str], item["source_authority_ids"])
        ):
            raise ContractAtlasError(f"Python exact unit is misbound: {public_identity}")
        expected_candidates.add(f"python:{distribution}:{public_identity}")
        element_by_identity[public_identity] = item

    for public_identity, surface in surfaces.items():
        if surface["unit"] != "member":
            continue
        owner = str(surface["owner"])
        owner_surface = surfaces.get(owner)
        member = element_by_identity[public_identity]
        owner_element = element_by_identity.get(owner)
        if (
            owner_surface is None
            or owner_surface["unit"] != "export"
            or owner_element is None
            or str(owner_element["id"]) not in cast(Sequence[str], member["related_element_ids"])
            or str(member["id"]) not in cast(Sequence[str], owner_element["related_element_ids"])
        ):
            raise ContractAtlasError(f"Python member lacks exact exported owner: {public_identity}")

    registry = cast(Mapping[str, object], trace["python_registry"])
    protected = {
        str(item["candidate_id"])
        for item in cast(Sequence[Mapping[str, object]], registry["dispositions"])
        if item["disposition"] == "protected"
    }
    candidates = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], registry["candidates"])
    }
    if protected != expected_candidates or not expected_candidates <= set(candidates):
        raise ContractAtlasError("Python registry does not protect every exact public unit")


def _validate_release_units(
    elements: Sequence[Mapping[str, object]], projection: Mapping[str, object]
) -> None:
    external = cast(Mapping[str, object], projection["external_contract"])
    release = cast(Mapping[str, object], external["release"])
    publication = cast(Mapping[str, object], release["publication"])
    base = "/external_contract/release/publication"
    expected: dict[str, str] = {}
    for section, interface in (
        ("versioning", "versioning-tags"),
        ("coordinates", "publication-locations"),
        ("distributions", "python-distributions"),
        ("runtime_images", "runtime-images"),
        ("installation_roots", "installation-roots"),
        ("release_artifacts", "release-artifacts"),
        ("trust", "artifact-verification"),
    ):
        expected.update(
            {
                f"{base}/{section}/{_escape_pointer(str(name))}": interface
                for name in cast(Mapping[str, object], publication[section])
            }
        )
    expected.update(
        {
            f"/external_contract/release/compatibility/{_escape_pointer(str(name))}": (
                "compatibility-guarantees"
            )
            for name in cast(Mapping[str, object], release["compatibility"])
        }
    )
    release_elements = [item for item in elements if item["authority"] == "release"]
    actual = {str(cast(Sequence[str], item["pointers"])[0]): item for item in release_elements}
    if (
        len(actual) != len(release_elements)
        or any(len(cast(Sequence[str], item["pointers"])) != 1 for item in release_elements)
        or set(actual) != set(expected)
    ):
        raise ContractAtlasError("release publication exact-unit projection and atlas differ")
    if any(item["interface"] != expected[pointer] for pointer, item in actual.items()):
        raise ContractAtlasError("release unit belongs to the wrong semantic interface")

    distributions = cast(Mapping[str, Mapping[str, object]], publication["distributions"])
    publication_policy_ids = {
        "publication/role-retention/v1",
        "publication/platform-scope/v1",
        "publication/image-digest-scope/v1",
    }
    for pointer, item in actual.items():
        details = cast(Mapping[str, object], item.get("details", {}))
        policy_ids = set(cast(Sequence[str], item["policy_ids"]))
        expected_publication_policies: set[str] = set()
        if "/distributions/" in pointer:
            name = pointer.rsplit("/", 1)[-1].replace("~1", "/").replace("~0", "~")
            if f"release-distribution:{name}" not in cast(
                Sequence[str], item["source_authority_ids"]
            ):
                raise ContractAtlasError(f"distribution lacks source metadata evidence: {name}")
            unit = cast(Mapping[str, object], pointer_value(projection, pointer))
            if not str(unit.get("requires_python", "")):
                raise ContractAtlasError(f"distribution lacks Requires-Python: {name}")
            if details.get("classification") != unit["role"]:
                raise ContractAtlasError(f"distribution classification is stale: {name}")
            expected_publication_policies = {"publication/role-retention/v1"}
        elif "/runtime_images/" in pointer:
            unit = cast(Mapping[str, object], pointer_value(projection, pointer))
            roots = [str(value) for value in cast(Sequence[object], unit["distribution_roots"])]
            root_roles = {str(distributions[root]["role"]) for root in roots}
            expected_classification = (
                "product"
                if unit["role"] == "product"
                else next(iter(root_roles))
                if len(root_roles) == 1
                else None
            )
            if details.get("classification") != expected_classification:
                raise ContractAtlasError(f"runtime-image classification is stale: {pointer}")
            expected_publication_policies = set(publication_policy_ids)
        elif "/installation_roots/" in pointer:
            unit = cast(Mapping[str, object], pointer_value(projection, pointer))
            role = distributions[str(unit["distribution"])]["role"]
            if details.get("classification") != role:
                raise ContractAtlasError(f"installation-root classification is stale: {pointer}")
            expected_publication_policies = {
                "publication/role-retention/v1",
                "publication/platform-scope/v1",
            }
        if policy_ids & publication_policy_ids != expected_publication_policies:
            raise ContractAtlasError(f"release publication policy application is stale: {pointer}")
    if any(
        pointer in actual
        for pointer in (
            f"{base}/schema",
            *(
                f"{base}/policy/{_escape_pointer(str(name))}"
                for name in cast(Mapping[str, object], publication["policy"])
            ),
        )
    ):
        raise ContractAtlasError("release metadata or policy is duplicated as a semantic unit")
    if "platforms" in release:
        raise ContractAtlasError("release envelope exposes a global platform claim")


def _assign_dossiers(elements: list[dict[str, object]]) -> None:
    used: set[str] = set()
    for element in sorted(elements, key=lambda item: str(item["id"])):
        authority = _slug(str(element["authority"]), limit=72)
        interface = _slug(str(element["interface"]), limit=48)
        base = _slug(str(element["title"]), limit=88)
        path = f"{ATLAS_DIRECTORY}/authorities/{authority}/{interface}/{base}.md"
        if path in used:
            suffix = canonical_sha256(element["id"])[:8]
            path = f"{ATLAS_DIRECTORY}/authorities/{authority}/{interface}/{base}-{suffix}.md"
        used.add(path)
        element["dossier"] = path


def _relative_link(source: str, target: str) -> str:
    return posixpath.relpath(target, posixpath.dirname(source))


def _anchor_id(kind: str, identity: str) -> str:
    """Return a deterministic presentation-only anchor for an exact identity."""

    prefixes = {
        "exclusion": "x",
        "extent": "e",
        "identity": "i",
        "policy": "p",
        "policy-application": "pa",
        "qualification": "q",
        "relationship-edge": "re",
        "relationship-node": "rn",
        "source": "src",
        "subject": "s",
    }
    prefix = prefixes.get(kind, _slug(kind, limit=12))
    return f"{prefix}-{hashlib.sha256(identity.encode()).hexdigest()[:10]}"


def _anchor_link(source: str, target: str, anchor: str) -> str:
    if source == target:
        return f"#{anchor}"
    return f"{_relative_link(source, target)}#{anchor}"


def _anchor_marker(kind: str, identity: str) -> str:
    return f'<a id="{_anchor_id(kind, identity)}"></a>'


def _html_anchor(anchor: str) -> str:
    return f'<a id="{anchor}"></a>'


def _subject_anchor(pointer: str) -> str:
    return _anchor_id("subject", pointer)


def _subject_marker(pointer: str, placed: set[str]) -> str:
    if pointer in placed:
        return ""
    placed.add(pointer)
    return f'<a id="{_subject_anchor(pointer)}"></a>'


def _policy_anchor(policy_id: str) -> str:
    return _anchor_id("policy", policy_id)


def _policy_application_anchor(element_id: str, policy_id: str) -> str:
    return _anchor_id("policy-application", f"{element_id}\n{policy_id}")


def _source_anchor(source_id: str) -> str:
    return _anchor_id("source", source_id)


def _qualification_anchor(route: str) -> str:
    return _anchor_id("qualification", route)


def _interface_label(interface: str) -> str:
    try:
        return INTERFACE_LABELS[interface]
    except KeyError as exc:
        raise ContractAtlasError(f"interface lacks a human navigation label: {interface}") from exc


def _interface_sort_key(interface: str) -> tuple[int, str]:
    return (INTERFACE_ORDER.get(interface, len(INTERFACE_ORDER)), interface)


def _contextual_labels(
    items: Sequence[Mapping[str, object]],
    candidates: Callable[[Mapping[str, object]], Sequence[str]],
) -> dict[str, str]:
    """Choose the shortest structurally derived label unique among siblings."""

    ordered = sorted(items, key=lambda item: str(item["title"]))
    options: dict[str, list[str]] = {}
    positions: dict[str, int] = {}
    for item in ordered:
        identity = str(item["id"])
        available = list(dict.fromkeys(label for label in candidates(item) if label))
        if not available or available[-1] != str(item["title"]):
            available.append(str(item["title"]))
        options[identity] = available
        positions[identity] = 0

    while True:
        by_label: dict[str, list[str]] = defaultdict(list)
        for identity, available in options.items():
            by_label[available[positions[identity]]].append(identity)
        collisions = [identities for identities in by_label.values() if len(identities) > 1]
        if not collisions:
            return {
                identity: available[positions[identity]] for identity, available in options.items()
            }
        advanced = False
        for identities in collisions:
            for identity in identities:
                if positions[identity] + 1 < len(options[identity]):
                    positions[identity] += 1
                    advanced = True
        if not advanced:
            raise ContractAtlasError("contextual inventory labels remain ambiguous")


def _relationship_node_anchor(node_id: str) -> str:
    return _anchor_id("relationship-node", node_id)


def _relationship_edge_anchor(edge: Mapping[str, object]) -> str:
    return _anchor_id(
        "relationship-edge",
        canonical_sha256(
            {
                "type": edge["type"],
                "source": edge["source"],
                "target": edge["target"],
                "scope": edge.get("scope"),
                "binding": edge.get("binding"),
            }
        ),
    )


def _md(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").replace("](", "]&#40;")


def _compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _one_cli_authority_element(
    matches: Iterable[Mapping[str, object]], *, kind: str
) -> Mapping[str, object]:
    resolved = list(matches)
    if len(resolved) != 1:
        raise ContractAtlasError(f"CLI {kind} authority resolves to {len(resolved)} atlas elements")
    return resolved[0]


def _cli_authority_reference(
    semantics: Mapping[str, object],
    *,
    element: Mapping[str, object],
    pointer: str,
    path: str,
    projection: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> str:
    kind = str(semantics.get("kind", ""))
    elements = elements_by_id.values()
    target: Mapping[str, object]
    anchor: str | None = None

    if kind == "http-operation-response":
        required = {"application", "operation_id", "method", "path", "status", "schema"}
        if not required <= set(semantics):
            raise ContractAtlasError("CLI HTTP response authority is incomplete")
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["authority"] == semantics["application"]
                and item["interface"] == "http-operations"
                and cast(Mapping[str, object], item.get("details", {})).get("operation_id")
                == semantics["operation_id"]
                and cast(Mapping[str, object], item.get("details", {})).get("method")
                == semantics["method"]
                and cast(Mapping[str, object], item.get("details", {})).get("path")
                == semantics["path"]
            ),
            kind=kind,
        )
        operation_pointer = cast(Sequence[str], target["pointers"])[0]
        response_pointer = (
            f"{operation_pointer}/responses/{_escape_pointer(str(semantics['status']))}"
        )
        response = pointer_value(projection, response_pointer)
        media = (
            cast(Mapping[str, object], response).get("content", {})
            if isinstance(response, Mapping)
            else {}
        )
        json_media = (
            cast(Mapping[str, object], media).get("application/json")
            if isinstance(media, Mapping)
            else None
        )
        if not isinstance(json_media, Mapping) or json_media.get("schema") != semantics["schema"]:
            raise ContractAtlasError("CLI HTTP response authority differs from its operation")
        anchor = _subject_anchor(response_pointer)
        label = f"HTTP {semantics['operation_id']} response {semantics['status']}"
    elif kind == "openapi-schema":
        required = {"application", "schema", "definition"}
        if not required <= set(semantics):
            raise ContractAtlasError("CLI OpenAPI schema authority is incomplete")
        schema_pointer = (
            f"/external_contract/http_openapi/{_escape_pointer(str(semantics['application']))}/"
            f"components/schemas/{_escape_pointer(str(semantics['schema']))}"
        )
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["authority"] == semantics["application"]
                and item["interface"] == "http-schemas"
                and schema_pointer in cast(Sequence[str], item["pointers"])
            ),
            kind=kind,
        )
        if pointer_value(projection, schema_pointer) != semantics["definition"]:
            raise ContractAtlasError("CLI OpenAPI schema authority has a different definition")
        label = f"OpenAPI {semantics['application']}.{semantics['schema']}"
    elif kind == "python-model":
        identity = semantics.get("identity")
        if not isinstance(identity, str) or not isinstance(semantics.get("schema"), Mapping):
            raise ContractAtlasError("CLI Python model authority is incomplete")
        declared_owner, separator, declared_symbol = identity.rpartition(".")
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["interface"] == "python"
                and (
                    item["title"] == identity
                    or (
                        bool(separator)
                        and item["authority"] == declared_owner
                        and str(item["title"]).rsplit(".", 1)[-1] == declared_symbol
                    )
                )
            ),
            kind=kind,
        )
        value = pointer_value(projection, cast(Sequence[str], target["pointers"])[0])
        contract = cast(Mapping[str, object], value).get("contract", {})
        if not isinstance(contract, Mapping) or contract.get("schema") != structural_json_schema(
            semantics["schema"]
        ):
            raise ContractAtlasError("CLI Python model authority has a different schema")
        label = identity
    elif kind == "schema-authority":
        authority = semantics.get("authority")
        if not isinstance(authority, str) or not authority:
            raise ContractAtlasError("CLI schema authority is incomplete")
        schema_pointer = f"/external_contract/protocol_schemas/{_escape_pointer(authority)}"
        definition = semantics.get("definition")
        expected_interface = "schema"
        if definition is not None:
            if not isinstance(definition, str) or not definition:
                raise ContractAtlasError("CLI schema definition authority is invalid")
            schema_pointer += f"/schemas/{_escape_pointer(definition)}"
            expected_interface = "process-protocol-schemas"
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["interface"] == expected_interface
                and schema_pointer in cast(Sequence[str], item["pointers"])
            ),
            kind=kind,
        )
        pointer_value(projection, schema_pointer)
        label = str(target["title"])
    elif kind == "document-authority":
        authority = semantics.get("authority")
        if not isinstance(authority, str) or not authority:
            raise ContractAtlasError("CLI document authority is incomplete")
        base = f"/external_contract/protocol_schemas/{_escape_pointer(authority)}"
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["interface"] == "process-protocol"
                and cast(Sequence[str], item["pointers"])
                and all(
                    candidate.startswith(f"{base}/")
                    for candidate in cast(Sequence[str], item["pointers"])
                )
            ),
            kind=kind,
        )
        label = str(target["title"])
    elif kind in {
        "cli-local-exact-json",
        "cli-local-json-schema",
        "cli-local-json-sequence",
    }:
        identity = semantics.get("identity")
        if not isinstance(identity, str) or not identity:
            raise ContractAtlasError("CLI-local structured authority has no identity")
        target = element
        anchor = _subject_anchor(pointer)
        label = identity
    else:
        raise ContractAtlasError(f"CLI structured output has no atlas resolver: {kind or '<none>'}")

    target_path = str(target["dossier"])
    href = (
        _anchor_link(path, target_path, anchor)
        if anchor is not None
        else _relative_link(path, target_path)
    )
    return f"[{_md(label)}]({href})"


def _cli_channel_summary(
    value: Mapping[str, object],
    *,
    element: Mapping[str, object],
    pointer: str,
    path: str,
    projection: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> str:
    parts: list[str] = []
    for mode, semantics in value.items():
        if not isinstance(semantics, Mapping):
            parts.append(f"{mode}: `{_md(semantics)}`")
            continue
        parts.append(
            f"{mode}: "
            + _cli_authority_reference(
                semantics,
                element=element,
                pointer=pointer,
                path=path,
                projection=projection,
                elements_by_id=elements_by_id,
            )
        )
    return "; ".join(parts)


def _shape_summary(value: object) -> str:
    if isinstance(value, Mapping):
        if set(value) == {"$ref"}:
            return str(value["$ref"])
        parts: list[str] = []
        for key in (
            "$ref",
            "type",
            "format",
            "const",
            "enum",
            "minimum",
            "maximum",
            "minLength",
            "maxLength",
            "minItems",
            "maxItems",
            "pattern",
        ):
            if key in value:
                rendered_value = json.dumps(
                    value[key], ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                parts.append(f"{key}={rendered_value}")
        properties = value.get("properties")
        if isinstance(properties, Mapping):
            parts.append("fields=" + ", ".join(f"`{name}`" for name in properties))
        if "items" in value:
            parts.append(f"items=({_shape_summary(value['items'])})")
        alternatives = [key for key in ("oneOf", "anyOf", "allOf") if key in value]
        for key in alternatives:
            variants = cast(Sequence[object], value[key])
            parts.append(f"{key}=" + " | ".join(_shape_summary(item) for item in variants))
        unrendered = sorted(
            set(value)
            - {
                "$ref",
                "type",
                "format",
                "const",
                "enum",
                "minimum",
                "maximum",
                "minLength",
                "maxLength",
                "minItems",
                "maxItems",
                "pattern",
                "properties",
                "items",
                "oneOf",
                "anyOf",
                "allOf",
                "description",
                "title",
                "default",
            }
        )
        if unrendered:
            parts.append("additional keys=" + ", ".join(f"`{key}`" for key in unrendered))
        return "; ".join(parts) or "empty object"
    if isinstance(value, list):
        if all(not isinstance(item, (Mapping, list)) for item in value):
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return "items=" + " | ".join(_shape_summary(item) for item in value)
    return json.dumps(value, ensure_ascii=False)


def _render_schema(
    value: Mapping[str, object], base_pointer: str, placed_subjects: set[str]
) -> list[str]:
    lines: list[str] = []
    for key in (
        "$id",
        "title",
        "description",
        "type",
        "format",
        "protocol",
        "protocols",
        "bundle_sha256",
    ):
        if key in value:
            pointer = f"{base_pointer}/{_escape_pointer(key)}"
            rendered = (
                json.dumps(value[key], ensure_ascii=False)
                if isinstance(value[key], (list, Mapping))
                else str(value[key])
            )
            lines.append(f"- {_subject_marker(pointer, placed_subjects)}`{key}`: {_md(rendered)}")
    required = set(cast(Sequence[str], value.get("required", ())))
    properties = value.get("properties")
    if isinstance(properties, Mapping):
        lines.extend(
            [
                "",
                "### Fields",
                "",
                "| Field | Required | Shape | Description |",
                "|---|---:|---|---|",
            ]
        )
        for name, field in properties.items():
            field_map = cast(Mapping[str, object], field) if isinstance(field, Mapping) else {}
            pointer = f"{base_pointer}/properties/{_escape_pointer(str(name))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(name)}` | "
                f"{'yes' if name in required else 'no'} | "
                f"{_md(_shape_summary(field))} | {_md(field_map.get('description', ''))} |"
            )
    schemas = value.get("schemas")
    if isinstance(schemas, Mapping):
        lines.extend(["", "### Schemas", "", "| Schema | Shape |", "|---|---|"])
        for name, schema in schemas.items():
            pointer = f"{base_pointer}/schemas/{_escape_pointer(str(name))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(name)}` | "
                f"{_md(_shape_summary(schema))} |"
            )
    definitions = value.get("$defs")
    if isinstance(definitions, Mapping):
        lines.extend(["", "### Definitions", "", "| Definition | Shape |", "|---|---|"])
        for name, schema in definitions.items():
            pointer = f"{base_pointer}/$defs/{_escape_pointer(str(name))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(name)}` | "
                f"{_md(_shape_summary(schema))} |"
            )
    if lines:
        lines.insert(0, _subject_marker(base_pointer, placed_subjects))
    return lines


def _render_python(
    value: Mapping[str, object], base_pointer: str, placed_subjects: set[str]
) -> list[str]:
    """Render one exact declared Python unit without hiding structural promises."""

    lines = [_subject_marker(base_pointer, placed_subjects)]
    for key in ("distribution", "module", "name", "owner", "unit"):
        if key in value:
            pointer = f"{base_pointer}/{_escape_pointer(key)}"
            lines.append(
                f"- {_subject_marker(pointer, placed_subjects)}`{key}`: `{_md(value[key])}`"
            )
    contract = cast(Mapping[str, object], value["contract"])
    contract_pointer = f"{base_pointer}/contract"
    lines.extend(["", "### Declared structure", ""])
    for key in ("kind", "signature", "type", "value"):
        if key in contract:
            pointer = f"{contract_pointer}/{_escape_pointer(key)}"
            rendered = _compact_json(contract[key])
            lines.append(f"- {_subject_marker(pointer, placed_subjects)}`{key}`: `{_md(rendered)}`")
    enum_values = contract.get("enum_values")
    if isinstance(enum_values, Mapping):
        lines.extend(["", "#### Enum members", "", "| Member | Value |", "|---|---|"])
        for name, item in enum_values.items():
            pointer = f"{contract_pointer}/enum_values/{_escape_pointer(str(name))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(name)}` | "
                f"`{_md(_compact_json(item))}` |"
            )
    fields = contract.get("fields")
    if isinstance(fields, list):
        lines.extend(
            ["", "#### Dataclass fields", "", "| Field | Type | Default |", "|---|---|---|"]
        )
        for index, item in enumerate(cast(Sequence[Mapping[str, object]], fields)):
            pointer = f"{contract_pointer}/fields/{index}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(item['name'])}` | "
                f"`{_md(item['type'])}` | `{_md(item['default'])}` |"
            )
    schema = contract.get("schema")
    if isinstance(schema, Mapping):
        lines.extend(["", "#### Validated model schema", ""])
        lines.extend(
            _render_schema(
                cast(Mapping[str, object], schema),
                f"{contract_pointer}/schema",
                placed_subjects,
            )
        )
    return lines


def _render_durable_state(
    pointers: Sequence[str],
    values: Sequence[object],
    details: Mapping[str, object],
    placed_subjects: set[str],
) -> list[str]:
    """Render one bounded, owner-projected durable-state audit unit."""

    unit = str(details["state_unit"])
    if unit == "identity":
        return [
            "| Authority fact | Value |",
            "|---|---|",
            *(
                f"| {_subject_marker(pointer, placed_subjects)}"
                f"`{_md(_pointer_parts(pointer)[-1])}` "
                f"| `{_md(_compact_json(value))}` |"
                for pointer, value in zip(pointers, values, strict=True)
            ),
        ]
    if len(pointers) != 1 or len(values) != 1 or not isinstance(values[0], Mapping):
        raise ContractAtlasError(f"durable-state unit is not exact: {details['state_owner']}")
    pointer = pointers[0]
    value = cast(Mapping[str, object], values[0])
    lines = [_subject_marker(pointer, placed_subjects)]
    if unit == "relational-table":
        lines.extend(
            [
                f"- Table: `{_md(value['name'])}`",
                "",
                "### Columns",
                "",
                "| Column | Type | Nullable | Default | Other constraints |",
                "|---|---|---:|---|---|",
            ]
        )
        for index, column in enumerate(cast(Sequence[Mapping[str, object]], value["columns"])):
            column_pointer = f"{pointer}/columns/{index}"
            other = {
                key: item
                for key, item in column.items()
                if key not in {"name", "type", "nullable", "default", "definition"}
            }
            lines.append(
                f"| {_subject_marker(column_pointer, placed_subjects)}`{_md(column['name'])}` | "
                f"`{_md(column['type'])}` | {'yes' if column['nullable'] else 'no'} | "
                f"`{_md(column.get('default', '—'))}` | "
                f"{_md(_compact_json(other)) if other else '—'} |"
            )
        constraints = cast(Sequence[Mapping[str, object]], value.get("constraints", ()))
        if constraints:
            lines.extend(
                [
                    "",
                    "### Table constraints",
                    "",
                    "| Kind | Name | Exact definition |",
                    "|---|---|---|",
                ]
            )
            for index, constraint in enumerate(constraints):
                constraint_pointer = f"{pointer}/constraints/{index}"
                lines.append(
                    f"| {_subject_marker(constraint_pointer, placed_subjects)}"
                    f"`{_md(constraint['kind'])}` | "
                    f"`{_md(constraint.get('name', '—'))}` | `{_md(constraint['definition'])}` |"
                )
        return lines
    if unit == "unique-index":
        return [
            *lines,
            "| Index fact | Value |",
            "|---|---|",
            *(f"| `{_md(key)}` | `{_md(_compact_json(item))}` |" for key, item in value.items()),
        ]
    if unit == "json-document":
        lines.extend([f"- Document: `{_md(value['id'])}`", "", "### Document schema", ""])
        schema = value.get("schema")
        if not isinstance(schema, Mapping):
            raise ContractAtlasError("durable JSON document has no exact schema")
        lines.extend(
            _render_schema(cast(Mapping[str, object], schema), f"{pointer}/schema", placed_subjects)
        )
        return lines
    # Composite units and deliberately simple component-owned structures are
    # still rendered losslessly below and retain their bounded unit dossier.
    lines.extend(_render_generic([pointer], [value], placed_subjects))
    return lines


def _render_http(
    value: Mapping[str, object],
    details: Mapping[str, object],
    base_pointer: str,
    placed_subjects: set[str],
) -> list[str]:
    lines = [_subject_marker(base_pointer, placed_subjects)]
    for key in ("operationId", "summary", "description", "deprecated"):
        if key in value:
            pointer = f"{base_pointer}/{_escape_pointer(key)}"
            lines.append(f"- {_subject_marker(pointer, placed_subjects)}`{key}`: {_md(value[key])}")
    if "security" in value:
        pointer = f"{base_pointer}/security"
        lines.append(
            f"- {_subject_marker(pointer, placed_subjects)}`security`: "
            f"`{_md(json.dumps(value['security'], sort_keys=True))}`"
        )
    parameters = cast(Sequence[Mapping[str, object]], value.get("parameters", ()))
    if parameters:
        lines.extend(
            ["", "### Parameters", "", "| Name | In | Required | Schema |", "|---|---|---:|---|"]
        )
        for index, item in enumerate(parameters):
            pointer = f"{base_pointer}/parameters/{index}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(item.get('name', ''))}` | "
                f"{_md(item.get('in', ''))} | "
                f"{'yes' if item.get('required') else 'no'} | "
                f"{_md(_shape_summary(item.get('schema')))} |"
            )
    if "requestBody" in value:
        pointer = f"{base_pointer}/requestBody"
        lines.extend(
            [
                "",
                f"### {_subject_marker(pointer, placed_subjects)}Request body",
                "",
                f"`{_md(json.dumps(value['requestBody'], sort_keys=True))}`",
            ]
        )
    responses = value.get("responses")
    if isinstance(responses, Mapping):
        lines.extend(["", "### Responses", "", "| Status | Description |", "|---|---|"])
        for status, response in responses.items():
            pointer = f"{base_pointer}/responses/{_escape_pointer(str(status))}"
            description = (
                cast(Mapping[str, object], response).get("description", "")
                if isinstance(response, Mapping)
                else ""
            )
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(status)}` | "
                f"{_md(description)} |"
            )
    del details
    return lines


def _render_cli(
    pointers: Sequence[str],
    values: Sequence[object],
    placed_subjects: set[str],
    *,
    element: Mapping[str, object],
    path: str,
    projection: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> list[str]:
    parameters: Sequence[Mapping[str, object]] = ()
    parameters_pointer = ""
    name = ""
    name_pointer = ""
    result_contract: Mapping[str, object] | None = None
    result_pointer = ""
    terminating_controls: Sequence[Mapping[str, object]] = ()
    terminating_controls_pointer = ""
    for pointer, value in zip(pointers, values, strict=True):
        if isinstance(value, str):
            name = value
            name_pointer = pointer
        elif isinstance(value, list) and pointer.endswith("/parameters"):
            parameters = cast(Sequence[Mapping[str, object]], value)
            parameters_pointer = pointer
        elif isinstance(value, list) and pointer.endswith("/terminating_controls"):
            terminating_controls = cast(Sequence[Mapping[str, object]], value)
            terminating_controls_pointer = pointer
        elif isinstance(value, Mapping):
            result_contract = value
            result_pointer = pointer
    lines = (
        [f"- {_subject_marker(name_pointer, placed_subjects)}Parser name: `{_md(name)}`"]
        if name
        else []
    )
    if parameters:
        lines.extend(
            [
                "",
                "### Parameters",
                "",
                "| Name | Kind | Required | Type | Options |",
                "|---|---|---:|---|---|",
            ]
        )
        for index, item in enumerate(parameters):
            pointer = f"{parameters_pointer}/{index}"
            parameter_name = item.get("name", item.get("dest", ""))
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(parameter_name)}` | "
                f"{_md(item.get('kind', ''))} | "
                f"{'yes' if item.get('required') else 'no'} | {_md(item.get('type', ''))} | "
                f"{_md(', '.join(cast(Sequence[str], item.get('options', ()))))} |"
            )
    if terminating_controls:
        lines.extend(
            [
                "",
                "### Terminating controls",
                "",
                "| Identity | Trigger | Exit status | stdout | stderr |",
                "|---|---|---:|---|---|",
            ]
        )
        for index, control in enumerate(terminating_controls):
            pointer = f"{terminating_controls_pointer}/{index}"
            lines.append(
                f"| {_subject_marker(f'{pointer}/id', placed_subjects)}"
                f"`{_md(control['id'])}` | "
                f"{_subject_marker(f'{pointer}/trigger', placed_subjects)}"
                f"`{_md(_compact_json(control['trigger']))}` | "
                f"{_subject_marker(f'{pointer}/exit_status', placed_subjects)}"
                f"`{_md(control['exit_status'])}` | "
                f"{_subject_marker(f'{pointer}/stdout', placed_subjects)}"
                f"`{_md(_compact_json(control['stdout']))}` | "
                f"{_subject_marker(f'{pointer}/stderr', placed_subjects)}"
                f"`{_md(_compact_json(control['stderr']))}` |"
            )
    if result_contract is not None:
        lines.extend(
            [
                "",
                "### Result and failure contract",
                "",
                f"- {_subject_marker(f'{result_pointer}/identity', placed_subjects)}"
                f"Result identity: `{_md(result_contract['identity'])}`",
                f"- {_subject_marker(f'{result_pointer}/profile_id', placed_subjects)}"
                f"Profile: `{_md(result_contract['profile_id'])}`",
                f"- {_subject_marker(f'{result_pointer}/structured_output', placed_subjects)}"
                f"Structured output: `{_md(result_contract['structured_output'])}`",
                f"- {_subject_marker(f'{result_pointer}/human_json_relationship', placed_subjects)}"
                "Human/JSON relationship: "
                f"`{_md(result_contract['human_json_relationship'])}`",
            ]
        )
        for key, title in (("success", "Success outcomes"), ("failures", "Failure outcomes")):
            outcomes = cast(Sequence[Mapping[str, object]], result_contract[key])
            lines.extend(
                [
                    "",
                    f"#### {title}",
                    "",
                    "| Identity | Selected by | Exit status | stdout | stderr |",
                    "|---|---|---|---|---|",
                ]
            )
            for index, outcome in enumerate(outcomes):
                outcome_pointer = f"{result_pointer}/{key}/{index}"
                status = outcome["exit_status"]
                rendered_status = (
                    json.dumps(status, sort_keys=True, separators=(",", ":"))
                    if isinstance(status, Mapping)
                    else str(status)
                )
                stdout_pointer = f"{outcome_pointer}/stdout"
                stderr_pointer = f"{outcome_pointer}/stderr"
                stdout = _cli_channel_summary(
                    cast(Mapping[str, object], outcome["stdout"]),
                    element=element,
                    pointer=stdout_pointer,
                    path=path,
                    projection=projection,
                    elements_by_id=elements_by_id,
                )
                stderr = _cli_channel_summary(
                    cast(Mapping[str, object], outcome["stderr"]),
                    element=element,
                    pointer=stderr_pointer,
                    path=path,
                    projection=projection,
                    elements_by_id=elements_by_id,
                )
                lines.append(
                    f"| {_subject_marker(f'{outcome_pointer}/id', placed_subjects)}"
                    f"`{_md(outcome['id'])}` | "
                    f"{_subject_marker(f'{outcome_pointer}/selected_by', placed_subjects)}"
                    f"`{_md(_compact_json(outcome['selected_by']))}` | "
                    f"{_subject_marker(f'{outcome_pointer}/exit_status', placed_subjects)}"
                    f"`{_md(rendered_status)}` | "
                    f"{_subject_marker(stdout_pointer, placed_subjects)}{stdout} | "
                    f"{_subject_marker(stderr_pointer, placed_subjects)}{stderr} |"
                )
    return lines


def _render_operation(
    value: Mapping[str, object], base_pointer: str, placed_subjects: set[str]
) -> list[str]:
    def rendered(item: object) -> object:
        return (
            json.dumps(item, ensure_ascii=False, sort_keys=True)
            if isinstance(item, (Mapping, list))
            else item
        )

    return [
        _subject_marker(base_pointer, placed_subjects),
        "| Concern | Contract |",
        "|---|---|",
        *(
            f"| {_subject_marker(f'{base_pointer}/{_escape_pointer(str(key))}', placed_subjects)}"
            f"`{_md(key)}` | {_md(rendered(item))} |"
            for key, item in value.items()
        ),
    ]


def _render_generic(
    pointers: Sequence[str], values: Sequence[object], placed_subjects: set[str]
) -> list[str]:
    if len(values) == 1 and isinstance(values[0], Mapping):
        value = cast(Mapping[str, object], values[0])
        schema_lines = _render_schema(value, pointers[0], placed_subjects)
        if schema_lines:
            return schema_lines
    large_value = values[0] if len(values) == 1 else list(values)
    if isinstance(large_value, Mapping):
        base_pointer = pointers[0]
        lines = [
            _subject_marker(base_pointer, placed_subjects),
            "| Field | Shape |",
            "|---|---|",
        ]
        for key, item in large_value.items():
            pointer = f"{base_pointer}/{_escape_pointer(str(key))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(key)}` | "
                f"{_md(_shape_summary(item))} |"
            )
        return lines
    if len(values) > 1:
        lines = [
            "| Subject | Shape |",
            "|---|---|",
        ]
        for pointer, subject_value in zip(pointers, values, strict=True):
            label = _pointer_parts(pointer)[-1]
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(label)}` | "
                f"{_md(_shape_summary(subject_value))} |"
            )
        return lines
    return [
        _subject_marker(pointers[0], placed_subjects),
        f"- Shape: {_shape_summary(large_value)}",
    ]


def _pretty_json(value: object) -> str:
    """Return exact, readable JSON without accidentally creating Markdown links."""

    return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True).replace("](", "]\\u0028")


def _exact_contract_lines(pointers: Sequence[str], values: Sequence[object]) -> list[str]:
    lines = [
        "### Exact owned JSON",
        "",
        "The following JSON is the complete value owned at each machine-authority pointer. "
        "No contractual fields are summarized away.",
        "",
    ]
    for pointer, value in zip(pointers, values, strict=True):
        if len(pointers) > 1:
            lines.extend([f"### `{pointer}`", ""])
        lines.extend(
            [
                f"<!-- exact-contract-value: {canonical_sha256(value)} -->",
                "",
                "```json",
                _pretty_json(value),
                "```",
                "",
            ]
        )
    return lines


def _local_contract_references(
    authority: str,
    values: Sequence[object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> list[Mapping[str, object]]:
    """Resolve local OpenAPI references to their exact atlas owners."""

    prefix = f"/external_contract/http_openapi/{authority}"
    owners: dict[str, Mapping[str, object]] = {}
    for element in elements_by_id.values():
        if element["authority"] != authority or element["interface"] != "http-schemas":
            continue
        for pointer in cast(Sequence[str], element["pointers"]):
            if pointer.startswith(f"{prefix}/components/"):
                owners[f"#{pointer.removeprefix(prefix)}"] = element

    references: set[str] = set()

    def visit(value: object) -> None:
        if isinstance(value, Mapping):
            reference = value.get("$ref")
            if isinstance(reference, str):
                references.add(reference)
            for child in value.values():
                visit(child)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for child in value:
                visit(child)

    for value in values:
        visit(value)
    return sorted(
        {
            str(owners[reference]["id"]): owners[reference]
            for reference in references
            if reference in owners
        }.values(),
        key=lambda item: str(item["title"]),
    )


def _subject_label(
    element: Mapping[str, object], pointer: str, projection: Mapping[str, object]
) -> str:
    """Describe one exact pointer without inventing a second semantic identity."""

    owned = [
        candidate
        for candidate in cast(Sequence[str], element["pointers"])
        if pointer == candidate or pointer.startswith(f"{candidate}/")
    ]
    base = max(owned, key=len) if owned else ""
    if pointer == base:
        return str(element["title"])

    relative = pointer[len(base) :].removeprefix("/") if base else pointer.removeprefix("/")
    parts = _pointer_parts(f"/{relative}") if relative else []
    value = pointer_value(projection, pointer)
    if parts and parts[0].isdigit() and base.endswith("/parameters"):
        if isinstance(value, Mapping):
            options = cast(Sequence[str], value.get("options", ()))
            name = str(options[0]) if options else str(value.get("name", parts[0]))
            return f"CLI parameter {name}"
        return f"CLI parameter {parts[0]}"

    labels: list[str] = []
    index = 0
    while index < len(parts):
        part = parts[index]
        following = parts[index + 1] if index + 1 < len(parts) else None
        if part == "$defs" and following is not None:
            labels.append(f"definition {following}")
            index += 2
        elif part == "schemas" and following is not None:
            labels.append(f"schema {following}")
            index += 2
        elif part == "properties" and following is not None:
            labels.append(f"field {following}")
            index += 2
        elif part == "parameters" and following is not None:
            parameter_pointer = f"{base}/parameters/{_escape_pointer(following)}"
            parameter = pointer_value(projection, parameter_pointer)
            parameter_name = (
                str(parameter.get("name", following))
                if isinstance(parameter, Mapping)
                else following
            )
            labels.append(f"parameter {parameter_name}")
            index += 2
        elif part == "responses" and following is not None:
            labels.append(f"response {following}")
            index += 2
        elif part == "requestBody":
            labels.append("request body")
            index += 1
        elif part == "items":
            labels.append("items")
            index += 1
        elif part == "additionalProperties":
            labels.append("additional values")
            index += 1
        elif part in {"allOf", "anyOf", "oneOf"} and following is not None and following.isdigit():
            alternative_pointer = f"{base}/" + "/".join(
                _escape_pointer(value) for value in parts[: index + 2]
            )
            alternative = pointer_value(projection, alternative_pointer)
            alternative_kind = ""
            if isinstance(alternative, Mapping):
                if isinstance(alternative.get("type"), str):
                    alternative_kind = str(alternative["type"])
                elif isinstance(alternative.get("$ref"), str):
                    alternative_kind = str(alternative["$ref"]).rsplit("/", 1)[-1]
            labels.append(
                f"{alternative_kind} value"
                if alternative_kind
                else f"{part} alternative {int(following) + 1}"
            )
            index += 2
        elif part == "schema":
            index += 1
        else:
            labels.append(part)
            index += 1
    return " · ".join(labels) or str(element["title"])


def _subject_reference(
    *,
    element: Mapping[str, object],
    pointer: str,
    projection: Mapping[str, object],
    placed_subjects: set[str],
) -> str:
    label = _md(_subject_label(element, pointer, projection))
    anchor = _subject_anchor(pointer)
    if pointer in placed_subjects:
        return f"[{label}](#{anchor})"
    ancestors = [candidate for candidate in placed_subjects if pointer.startswith(f"{candidate}/")]
    if ancestors:
        parent = max(ancestors, key=len)
        placed_subjects.add(pointer)
        return f"{_html_anchor(anchor)}[{label}](#{_subject_anchor(parent)})"
    placed_subjects.add(pointer)
    return f'<a id="{anchor}"></a>{label}'


def _interface_index_path(authority: str, interface: str) -> str:
    return (
        f"{ATLAS_DIRECTORY}/authorities/{_slug(authority, limit=72)}/"
        f"{_slug(interface, limit=48)}/index.md"
    )


def _extension_context_path(extension: Mapping[str, object]) -> str:
    """Return the identity-derived side-context path for one extension boundary."""

    return f"{ATLAS_DIRECTORY}/extensions/{_slug(str(extension['id']), limit=96)}.md"


def _render_dossier(
    element: Mapping[str, object],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> bytes:
    path = str(element["dossier"])
    authority_path = (
        f"{ATLAS_DIRECTORY}/authorities/{_slug(str(element['authority']), limit=72)}/index.md"
    )
    interface_path = (
        f"{ATLAS_DIRECTORY}/authorities/{_slug(str(element['authority']), limit=72)}/"
        f"{_slug(str(element['interface']), limit=48)}/index.md"
    )
    policy_path = f"{ATLAS_DIRECTORY}/policies/index.md"
    source_evidence_path = f"{ATLAS_DIRECTORY}/evidence/sources.md"
    pointers = cast(Sequence[str], element["pointers"])
    values = [pointer_value(projection, pointer) for pointer in pointers]
    placed_subjects: set[str] = set()
    source_index = _source_index(trace)
    details = cast(Mapping[str, object], element.get("details", {}))
    purpose = "Exact externally visible contract owned by this semantic dossier."
    if len(values) == 1 and isinstance(values[0], Mapping):
        value = cast(Mapping[str, object], values[0])
        purpose = str(value.get("summary", value.get("description", purpose))).strip() or purpose
    interface_label = _interface_label(str(element["interface"]))
    lines = [
        f"# {element['title']}",
        "",
        f"[Atlas]({_relative_link(path, f'{ATLAS_DIRECTORY}/index.md')}) · "
        f"[Authority]({_relative_link(path, authority_path)}) · "
        f"[Interface]({_relative_link(path, interface_path)}) · "
        f"[Policies]({_relative_link(path, policy_path)})",
        "",
        f"<!-- contract-element: {element['id']} -->",
        "",
        purpose,
        "",
        "| Audit field | Value |",
        "|---|---|",
        f"| Authority | [{_md(element['authority'])}]({_relative_link(path, authority_path)}) |",
        f"| Interface | [{_md(interface_label)}]({_relative_link(path, interface_path)}) |",
        "",
        "## External contract",
        "",
    ]
    interface = str(element["interface"])
    if (
        interface == "http-operations"
        and len(values) == 1
        and isinstance(values[0], Mapping)
        and "method" in details
        and not details.get("supplemental")
    ):
        lines.extend(
            _render_http(
                cast(Mapping[str, object], values[0]), details, pointers[0], placed_subjects
            )
        )
    elif interface == "cli":
        lines.extend(
            _render_cli(
                pointers,
                values,
                placed_subjects,
                element=element,
                path=path,
                projection=projection,
                elements_by_id=elements_by_id,
            )
        )
    elif interface == "python" and len(values) == 1 and isinstance(values[0], Mapping):
        lines.extend(
            _render_python(cast(Mapping[str, object], values[0]), pointers[0], placed_subjects)
        )
    elif interface == "durable-state":
        lines.extend(_render_durable_state(pointers, values, details, placed_subjects))
    elif (
        interface in {"http-operations", "process-protocol-operations"}
        and len(values) == 1
        and isinstance(values[0], Mapping)
    ):
        lines.extend(
            _render_operation(cast(Mapping[str, object], values[0]), pointers[0], placed_subjects)
        )
    elif interface in RELEASE_INTERFACES and len(values) == 1 and isinstance(values[0], Mapping):
        lines.extend(
            _render_operation(cast(Mapping[str, object], values[0]), pointers[0], placed_subjects)
        )
    else:
        lines.extend(_render_generic(pointers, values, placed_subjects))
    lines.append("")

    semantic_owners = cast(Sequence[str], details.get("semantic_owners", ()))
    if semantic_owners:
        relationship_evidence = f"{ATLAS_DIRECTORY}/evidence/relationships.md"
        lines.extend(
            [
                "## Existing ownership context",
                "",
                "Publication preserves these existing component authorities; it does not "
                "reclassify or duplicate their interfaces.",
                "",
                *(
                    f"- [{_md(owner)}]"
                    f"({_anchor_link(path, relationship_evidence, component_anchor)})"
                    for owner in semantic_owners
                    for component_anchor in (_relationship_node_anchor(f"component:{owner}"),)
                ),
                "",
            ]
        )

    extent_ids = cast(Sequence[str], element["extent_decision_ids"])
    if extent_ids:
        decisions = {
            str(item["id"]): item
            for item in cast(
                Sequence[Mapping[str, object]],
                cast(
                    Mapping[str, object],
                    cast(Mapping[str, object], projection["external_contract"])["extents"],
                )["decisions"],
            )
        }
        lines.extend(["### Progression, limits, and lifecycle", ""])
        decision_groups: dict[str, list[tuple[str, Mapping[str, object]]]] = defaultdict(list)
        for identity in extent_ids:
            decision = decisions[identity]
            decision_groups[str(decision["rule"])].append((identity, decision))
        for rule, group in sorted(decision_groups.items()):
            rule_id = f"extent-rule/{rule}"
            structural_keys = {
                "id",
                "owner",
                "source_pointer",
                "dimension",
                "unit",
                "policy",
                "rule",
            }
            detail_maps = [
                {key: value for key, value in decision.items() if key not in structural_keys}
                for _identity, decision in group
            ]
            common_keys = set(detail_maps[0])
            for details_map in detail_maps[1:]:
                common_keys &= set(details_map)
            common_details = {
                key: detail_maps[0][key]
                for key in sorted(common_keys)
                if all(details_map[key] == detail_maps[0][key] for details_map in detail_maps[1:])
            }
            shared = "; ".join(
                f"{key}={_compact_json(value)}" for key, value in common_details.items()
            )
            lines.extend(
                [
                    f"#### [{_md(rule_id)}]"
                    f"({_anchor_link(path, policy_path, _policy_anchor(rule_id))})",
                    "",
                    *(
                        [f"Shared facts for every subject below: {_md(shared)}", ""]
                        if shared
                        else []
                    ),
                    "| Applies to | Contract | Bounds or reason |",
                    "|---|---|---|",
                ]
            )
            for _identity, decision in group:
                bounds = "; ".join(
                    f"{key}={_compact_json(value)}"
                    for key, value in decision.items()
                    if key not in structural_keys and key not in common_details
                )
                source_pointer = str(decision["source_pointer"])
                subject = _subject_reference(
                    element=element,
                    pointer=source_pointer,
                    projection=projection,
                    placed_subjects=placed_subjects,
                )
                contract = f"{decision['dimension']} · {decision['unit']} · {decision['policy']}"
                rendered_bounds = _md(bounds) if bounds else "shared above"
                lines.append(f"| {subject} | `{_md(contract)}` | {rendered_bounds} |")
            lines.append("")

    related_ids = cast(Sequence[str], element["related_element_ids"])
    referenced = _local_contract_references(str(element["authority"]), values, elements_by_id)
    if related_ids or referenced:
        lines.extend(["## Maintained corroboration", ""])
    if related_ids:
        lines.extend(["### Related interface records", ""])
        for related_id in related_ids:
            related = elements_by_id[related_id]
            lines.append(f"- [{related['title']}]({_relative_link(path, str(related['dossier']))})")
        lines.append("")
    if referenced:
        lines.extend(["### Referenced contract dossiers", ""])
        for owner in referenced:
            lines.append(f"- [{owner['title']}]({_relative_link(path, str(owner['dossier']))})")
        lines.append("")

    lines.extend(
        [
            "## Governing policies",
            "",
            *(
                f"- {_html_anchor(_policy_application_anchor(str(element['id']), policy))}"
                f"[{_md(policy)}]({_anchor_link(path, policy_path, _policy_anchor(policy))})"
                for policy in cast(Sequence[str], element["policy_ids"])
            ),
            "",
            "## Evidence",
            "",
            "### Qualification",
            "",
            *(
                f"- [{_md(route)}]"
                f"({_anchor_link(path, source_evidence_path, _qualification_anchor(route))})"
                for route in cast(Sequence[str], element["qualification_routes"])
            ),
            "",
            "### Executable sources",
            "",
        ]
    )
    for source_id in cast(Sequence[str], element["source_authority_ids"]):
        source = source_index[source_id]
        location = cast(Mapping[str, object], source.get("source", {}))
        bindings = cast(Sequence[Mapping[str, object]], source.get("bindings", ()))
        declarations = cast(Sequence[Mapping[str, object]], source.get("declarations", ()))
        rendered = str(
            location.get(
                "path",
                location.get(
                    "module",
                    bindings[0]["path"]
                    if bindings
                    else declarations[0]["path"]
                    if declarations
                    else source_id,
                ),
            )
        )
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        lines.append(
            f"- [{_md(source_id)}]"
            f"({_anchor_link(path, source_evidence_path, _source_anchor(source_id))}) — "
            f"`{rendered}{symbol}`"
        )
    qualification_key = details.get("qualification_key")
    if isinstance(qualification_key, Sequence) and not isinstance(qualification_key, str):
        records = cast(
            Sequence[Mapping[str, object]],
            cast(Mapping[str, object], trace["operation_qualification"])["records"],
        )
        matching_records = [
            record
            for record in records
            if [record["application"], record["operation_id"]] == list(qualification_key)
        ]
        if len(matching_records) != 1:
            raise ContractAtlasError(
                f"HTTP contract has ambiguous operation qualification evidence: {qualification_key}"
            )
        lines.extend(
            [
                "",
                "### Operation qualification evidence",
                "",
                "This evidence proves maintained client, CLI, response-authority, and provider "
                "qualification without creating a second semantic operation.",
                "",
                "```json",
                _pretty_json(matching_records[0]),
                "```",
            ]
        )
    if element["interface"] == "configuration-environment":
        configuration_sources = [
            source_index[source_id]
            for source_id in cast(Sequence[str], element["source_authority_ids"])
            if source_index[source_id].get("bindings")
        ]
        lines.extend(
            [
                "",
                "### Configuration authority and bindings",
                "",
                "The owning implementation defines the setting. The parser expression records "
                "each independently discovered consumer binding and effective default exercised "
                "by qualification.",
                "",
                "| Kind | Consumer | Source | Authority |",
                "|---|---|---|---|",
            ]
        )
        for source in configuration_sources:
            for declaration in cast(Sequence[Mapping[str, object]], source.get("declarations", ())):
                lines.append(
                    f"| declaration | — | `{_md(declaration['path'])}` | "
                    f"`{_md(declaration['pointer'])}` |"
                )
            for binding in cast(Sequence[Mapping[str, object]], source["bindings"]):
                lines.append(
                    f"| parser | `{_md(binding['consumer'])}` | `{_md(binding['path'])}` | "
                    f"`{_md(binding['expression'])}` |"
                )
    lines.extend(
        [
            "",
            "### Machine authority",
            "",
            *(f"- `{pointer}`" for pointer in pointers),
            "",
            *_exact_contract_lines(pointers, values),
        ]
    )
    return ("\n".join(lines).rstrip() + "\n").encode()


def _table_counts(
    values: Mapping[str, object],
    label: str,
    *,
    links: Mapping[str, str] | None = None,
    anchors: Mapping[str, str] | None = None,
) -> list[str]:
    def cell(key: str) -> str:
        marker = _html_anchor(anchors[key]) if anchors is not None and key in anchors else ""
        value = (
            f"[{_md(key)}]({links[key]})" if links is not None and key in links else f"`{_md(key)}`"
        )
        return f"{marker}{value}"

    return [
        f"| {label} | Count |",
        "|---|---:|",
        *(f"| {cell(str(key))} | {value} |" for key, value in values.items()),
    ]


def _relationship_model(
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    elements: Sequence[Mapping[str, object]],
    component_descriptions: Mapping[str, str],
) -> dict[str, object]:
    """Derive human navigation from frozen boundary and release metadata."""

    boundaries = cast(Mapping[str, object], projection["boundaries"])
    components = cast(Sequence[Mapping[str, object]], boundaries["components"])
    component_names = {str(item["distribution"]) for item in components}
    if set(component_descriptions) != component_names or any(
        not value.strip() for value in component_descriptions.values()
    ):
        raise ContractAtlasError(
            "relationship navigation requires one maintained description per release component"
        )
    authority_counts = Counter(str(item["authority"]) for item in elements)
    interface_counts = Counter(
        (str(item["authority"]), str(item["interface"])) for item in elements
    )

    def semantic_interface(authority: str, interface: str) -> dict[str, object]:
        count = interface_counts[(authority, interface)]
        if count == 0:
            raise ContractAtlasError(
                f"extension relationship lacks an existing semantic interface: "
                f"{authority}: {interface}"
            )
        return {
            "authority": authority,
            "interface": interface,
            "label": _interface_label(interface),
            "contract_elements": count,
        }

    nodes: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []
    for component in components:
        name = str(component["distribution"])
        nodes.append(
            {
                "id": f"component:{name}",
                "kind": "component",
                "name": name,
                "path": component["path"],
                "role": component["role"],
                "description": component_descriptions[name],
                "description_source": f"{component['path']}/pyproject.toml#/project/description",
                "contract_elements": authority_counts.get(name, 0),
            }
        )
        for dependency in cast(Sequence[str], component["dependencies"]):
            edges.append(
                {
                    "type": "depends-on",
                    "source": f"component:{name}",
                    "target": f"component:{dependency}",
                    "scope": "required",
                }
            )
        for extra, dependencies in cast(
            Mapping[str, Sequence[str]], component["optional_dependencies"]
        ).items():
            for dependency in dependencies:
                edges.append(
                    {
                        "type": "depends-on",
                        "source": f"component:{name}",
                        "target": f"component:{dependency}",
                        "scope": f"optional:{extra}",
                    }
                )

    for item in cast(Sequence[Mapping[str, object]], boundaries["entry_point_extensions"]):
        group = str(item["group"])
        node_id = f"extension-point:{group}"
        nodes.append(
            {
                "id": node_id,
                "kind": "extension-point",
                "name": group,
                "description": f"Entry-point extension boundary owned by {item['owner']}.",
                "owner": item["owner"],
                "contract_elements": 0,
                "semantic_interfaces": [semantic_interface(str(item["owner"]), "python")],
            }
        )
        edges.append(
            {
                "type": "owns-extension-point",
                "source": f"component:{item['owner']}",
                "target": node_id,
            }
        )
        for provider in cast(Sequence[Mapping[str, object]], item["providers"]):
            edges.append(
                {
                    "type": "implements-extension-point",
                    "source": f"component:{provider['distribution']}",
                    "target": node_id,
                    "binding": provider["name"],
                }
            )

    for item in cast(Sequence[Mapping[str, object]], boundaries["process_extensions"]):
        name = str(item["name"])
        node_id = f"process-protocol:{name}"
        nodes.append(
            {
                "id": node_id,
                "kind": "process-protocol",
                "name": name,
                "description": (
                    f"Independently deployed process protocol owned by {item['contract_owner']}."
                ),
                "owner": item["contract_owner"],
                "protocols": item["protocols"],
                "contract_elements": 0,
                "semantic_interfaces": [
                    semantic_interface(str(item["contract_owner"]), "python"),
                    semantic_interface(str(item["binding_support"]), "process-protocol"),
                    semantic_interface(str(item["binding_support"]), "process-protocol-operations"),
                    semantic_interface(str(item["binding_support"]), "process-protocol-schemas"),
                ],
            }
        )
        edges.extend(
            [
                {
                    "type": "owns-protocol",
                    "source": f"component:{item['contract_owner']}",
                    "target": node_id,
                },
                {
                    "type": "binds-protocol",
                    "source": f"component:{item['binding_support']}",
                    "target": node_id,
                    "binding": item["binding"],
                },
            ]
        )
        for provider in cast(Sequence[Mapping[str, object]], item["providers"]):
            edges.append(
                {
                    "type": "implements-protocol",
                    "source": f"component:{provider['distribution']}",
                    "target": node_id,
                }
            )

    runtime_images = cast(
        Mapping[str, Mapping[str, object]],
        cast(Mapping[str, object], boundaries["runtime_images"])["runtime"],
    )
    for image, config in runtime_images.items():
        distributions = cast(Sequence[str], config.get("distributions", ()))
        node_id = f"image:runtime:{image}"
        nodes.append(
            {
                "id": node_id,
                "kind": "runtime-image",
                "name": image,
                "image_kind": "runtime",
                "role": config.get("role"),
                "description": config["description"],
                "description_source": f"release.toml#/images/runtime/{image}/description",
                "contract_elements": authority_counts.get(image, 0),
            }
        )
        for distribution in distributions:
            edges.append(
                {
                    "type": "packaged-in",
                    "source": f"component:{distribution}",
                    "target": node_id,
                }
            )

    release = cast(
        Mapping[str, object], cast(Mapping[str, object], projection["external_contract"])["release"]
    )
    publication = cast(Mapping[str, object], release["publication"])
    installation_roots = cast(Mapping[str, Mapping[str, object]], publication["installation_roots"])
    installation_methods = {str(value["method"]) for value in installation_roots.values()}
    if len(installation_methods) != 1:
        raise ContractAtlasError("publication installation roots lack one exact method")
    installation_method = next(iter(installation_methods))
    installation_id = f"installation:{installation_method}"
    nodes.append(
        {
            "id": installation_id,
            "kind": "installation",
            "name": installation_method,
            "description": (
                "Coordinated end-user installation roots declared by the release contract."
            ),
            "contract_elements": 0,
        }
    )
    for root, unit in installation_roots.items():
        edges.append(
            {
                "type": "installed-as",
                "source": f"component:{unit['distribution']}",
                "target": installation_id,
                "binding": root,
            }
        )

    node_ids = [str(item["id"]) for item in nodes]
    if len(node_ids) != len(set(node_ids)):
        raise ContractAtlasError("relationship navigation repeats a node")
    if any(
        str(edge["source"]) not in node_ids or str(edge["target"]) not in node_ids for edge in edges
    ):
        raise ContractAtlasError("relationship navigation contains an unresolved edge")
    product_nodes = [
        str(item["id"])
        for item in nodes
        if item.get("kind") == "component" and item.get("role") == "deployed_implementation"
    ]
    if len(product_nodes) != 1:
        raise ContractAtlasError("relationship navigation requires one product implementation")
    product_images = [
        str(item["id"])
        for item in nodes
        if item.get("kind") == "runtime-image" and item.get("role") == "product"
    ]
    if len(product_images) != 1:
        raise ContractAtlasError("relationship navigation requires one product runtime image")
    if not any(
        edge["type"] == "packaged-in"
        and edge["source"] == product_nodes[0]
        and edge["target"] == product_images[0]
        for edge in edges
    ):
        raise ContractAtlasError("product runtime image does not contain its implementation")
    source_index = _source_index(trace)
    component_nodes = [item for item in nodes if item["kind"] == "component"]
    component_node_by_name = {str(item["name"]): item for item in component_nodes}
    authorities = sorted({str(item["authority"]) for item in elements})
    authority_owners: dict[str, set[str]] = defaultdict(set)

    def paths_in(value: object) -> Iterable[str]:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if key == "path" and isinstance(child, str):
                    yield child
                yield from paths_in(child)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for child in value:
                yield from paths_in(child)

    for authority in authorities:
        matching_name = next((item for item in component_nodes if item["name"] == authority), None)
        if matching_name is not None:
            authority_owners[authority].add(str(matching_name["id"]))
        for element in (item for item in elements if item["authority"] == authority):
            for pointer in cast(Sequence[str], element["pointers"]):
                value = pointer_value(projection, pointer)
                if isinstance(value, Mapping):
                    for field in ("distribution", "consumer"):
                        owner = component_node_by_name.get(str(value.get(field, "")))
                        if owner is not None:
                            authority_owners[authority].add(str(owner["id"]))
            for source_id in cast(Sequence[str], element["source_authority_ids"]):
                source = source_index[source_id]
                for source_path in paths_in(source):
                    candidates = [
                        item
                        for item in component_nodes
                        if source_path == item["path"] or source_path.startswith(f"{item['path']}/")
                    ]
                    if candidates:
                        longest = max(len(str(item["path"])) for item in candidates)
                        authority_owners[authority].update(
                            str(item["id"])
                            for item in candidates
                            if len(str(item["path"])) == longest
                        )

    product_node = next(item for item in nodes if item["id"] == product_images[0])
    map_specs = (
        (
            "release-envelope",
            "Release envelope",
            None,
            None,
        ),
        ("riverhog-product", "Riverhog product", None, None),
        (
            "riverhog-service",
            "Riverhog service",
            "riverhog-product",
            "surfaces/riverhog.md#riverhog-service",
        ),
        (
            "riverhog-contracts",
            "Riverhog-owned contracts and libraries",
            "riverhog-product",
            "surfaces/riverhog.md#riverhog-owned-contracts-and-libraries",
        ),
        (
            "riverhog-implementation",
            "Implementation and build",
            "riverhog-product",
            "surfaces/riverhog.md#implementation-and-build",
        ),
        ("nonnormative-references", "Maintainer-selected nonnormative references", None, None),
        (
            "riverhog-references",
            "Riverhog references",
            "nonnormative-references",
            "surfaces/references.md#riverhog-references",
        ),
        ("gogurt", "Gogurt", "nonnormative-references", "surfaces/references.md#gogurt"),
        (
            "mango-fish",
            "Mango Fish",
            "nonnormative-references",
            "surfaces/references.md#mango-fish",
        ),
        ("piggity", "Piggity", "nonnormative-references", "surfaces/references.md#piggity"),
        ("stove0", "Stove0", "nonnormative-references", "surfaces/stove0.md"),
        ("stove0-application", "Application", "stove0", "surfaces/stove0.md#application"),
        ("stove0-observers", "Observers", "stove0", "surfaces/stove0.md#observers"),
        ("stove0-targets", "Targets", "stove0", "surfaces/stove0.md#targets"),
        ("stove0-review", "Review", "stove0", "surfaces/stove0.md#review"),
        ("stove0-recipes", "Recipes", "stove0", "surfaces/stove0.md#recipes"),
        ("cross-cutting", "Cross-cutting v1 authorities", None, "surfaces/cross-cutting.md"),
    )
    mapped: dict[str, list[dict[str, object]]] = {
        map_id: [] for map_id, _title, _parent, _path in map_specs
    }
    component_by_id = {str(item["id"]): item for item in component_nodes}
    declared_authority_meanings = {
        str(item["id"]): str(item["meaning"])
        for item in cast(
            Sequence[Mapping[str, object]],
            cast(Mapping[str, object], trace["authority_registry"])["declared_authorities"],
        )
    }

    def stove0_node(authority: str) -> str:
        if "review" in authority:
            return "stove0-review"
        if "observer" in authority or authority in {
            "stove0-media-metadata-observer-contracts",
            "stove0-media-sampling-observer-contracts",
        }:
            return "stove0-observers"
        if "recipe" in authority:
            return "stove0-recipes"
        if "target" in authority or "media-archive" in authority:
            return "stove0-targets"
        return "stove0-application"

    for authority in authorities:
        owners = [component_by_id[item] for item in sorted(authority_owners[authority])]
        roles = {str(item["role"]) for item in owners}
        if authority == "release":
            map_id = "release-envelope"
        elif authority == product_node["name"]:
            map_id = "riverhog-service"
        elif owners and all(
            item["role"] in {"reference_application", "reference_component"}
            or (item["role"] == "reusable_library" and str(item["path"]).startswith("reference/"))
            for item in owners
        ):
            if authority.startswith("stove0"):
                map_id = stove0_node(authority)
            elif authority.startswith("gogurt"):
                map_id = "gogurt"
            elif authority.startswith("mango-fish"):
                map_id = "mango-fish"
            elif authority.startswith("piggity"):
                map_id = "piggity"
            else:
                map_id = "riverhog-references"
        elif owners and roles == {"reusable_library"}:
            map_id = "riverhog-contracts"
        elif owners and roles <= {"deployed_implementation", "internal_build_unit"}:
            map_id = "riverhog-implementation"
        else:
            map_id = "cross-cutting"
        purpose = declared_authority_meanings.get(authority)
        if purpose is None:
            purpose = " ".join(sorted({str(item["description"]) for item in owners}))
        if not purpose:
            purpose = "Cross-cutting generated contract authority."
        direct_interfaces = [
            {
                "id": interface,
                "label": _interface_label(interface),
                "contract_elements": count,
            }
            for (candidate_authority, interface), count in sorted(
                interface_counts.items(),
                key=lambda item: (item[0][0], _interface_sort_key(item[0][1])),
            )
            if candidate_authority == authority
        ]
        mapped[map_id].append(
            {
                "authority": authority,
                "contract_elements": authority_counts[authority],
                "owner_component_ids": sorted(authority_owners[authority]),
                "purpose": purpose,
                "interfaces": direct_interfaces,
            }
        )
    contract_map_nodes = [
        {
            "id": map_id,
            "title": title,
            "parent": parent,
            **({"path": path} if path is not None else {}),
            "authorities": mapped[map_id],
        }
        for map_id, title, parent, path in map_specs
    ]
    mapped_authorities = [
        str(item["authority"])
        for node in contract_map_nodes
        for item in cast(Sequence[Mapping[str, object]], node["authorities"])
    ]
    if sorted(mapped_authorities) != authorities or len(mapped_authorities) != len(
        set(mapped_authorities)
    ):
        raise ContractAtlasError("human contract map does not partition exact authorities")
    return {
        "schema": RELATIONSHIP_SCHEMA,
        "center": product_nodes[0],
        "product": product_images[0],
        "reference_policy": boundaries["reference_policy"],
        "contract_map": {
            "schema": CONTRACT_MAP_SCHEMA,
            "nodes": contract_map_nodes,
        },
        "nodes": sorted(nodes, key=lambda value: str(value["id"])),
        "edges": sorted(
            edges,
            key=lambda value: (
                str(value["type"]),
                str(value["source"]),
                str(value["target"]),
                str(value.get("scope", "")),
                str(value.get("binding", "")),
            ),
        ),
    }


def _authority_index_path(authority: str) -> str:
    return f"{ATLAS_DIRECTORY}/authorities/{_slug(authority, limit=72)}/index.md"


def _contract_map_nodes(relationship: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    contract_map = cast(Mapping[str, object], relationship["contract_map"])
    return {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], contract_map["nodes"])
    }


def _map_node_link(source: str, node: Mapping[str, object]) -> str:
    path = node.get("path")
    title = _md(node["title"])
    if path is None:
        return title
    return f"[{title}]({_relative_link(source, f'{ATLAS_DIRECTORY}/{path}')})"


def _render_contract_map(
    relationship: Mapping[str, object],
    *,
    source: str,
) -> list[str]:
    """Render the one canonical human-facing map of the repository contract."""

    nodes = _contract_map_nodes(relationship)
    children: dict[str | None, list[Mapping[str, object]]] = defaultdict(list)
    for node in nodes.values():
        children[cast(str | None, node.get("parent"))].append(node)

    relationship_nodes = cast(Sequence[Mapping[str, object]], relationship["nodes"])
    relationship_nodes_by_id = {str(item["id"]): item for item in relationship_nodes}
    extension_nodes_by_id = {
        str(item["id"]): item
        for item in relationship_nodes
        if item["kind"] in {"extension-point", "process-protocol"}
    }
    extensions_by_owner_interface: dict[tuple[str, str], list[Mapping[str, object]]] = defaultdict(
        list
    )
    for extension in extension_nodes_by_id.values():
        owner = str(extension["owner"])
        owner_interfaces = [
            str(item["interface"])
            for item in cast(Sequence[Mapping[str, object]], extension["semantic_interfaces"])
            if item["authority"] == owner
        ]
        if len(owner_interfaces) != 1:
            raise ContractAtlasError(
                f"extension must resolve to one owning semantic interface: {extension['id']}"
            )
        extensions_by_owner_interface[(owner, owner_interfaces[0])].append(extension)
    extensions_by_provider: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for edge in cast(Sequence[Mapping[str, object]], relationship["edges"]):
        if edge["type"] not in {"implements-extension-point", "implements-protocol"}:
            continue
        extension = extension_nodes_by_id[str(edge["target"])]
        provider = relationship_nodes_by_id[str(edge["source"])]
        extensions_by_provider[str(provider["name"])].append(extension)

    def extension_links(extensions: Sequence[Mapping[str, object]]) -> str:
        return ", ".join(
            f"[{_md(item['name'])}]({_relative_link(source, _extension_context_path(item))})"
            for item in sorted(extensions, key=lambda value: str(value["name"]))
        )

    def render_authority(
        authority: Mapping[str, object],
        *,
        prefix: str,
    ) -> list[str]:
        authority_name = str(authority["authority"])
        provider_extensions = extensions_by_provider.get(authority_name, ())
        provider_annotation = ""
        if provider_extensions:
            kinds = {str(item["kind"]) for item in provider_extensions}
            label = (
                "Implements protocol" if kinds == {"process-protocol"} else "Provider for extension"
            )
            if len(provider_extensions) != 1:
                label += "s"
            provider_annotation = f" {label}: {extension_links(provider_extensions)}."
        result = [
            f"{prefix}- [{_md(authority_name)}]"
            f"({_relative_link(source, _authority_index_path(authority_name))}) — "
            f"{_md(authority['purpose'])}{provider_annotation}"
        ]
        for interface in cast(Sequence[Mapping[str, object]], authority["interfaces"]):
            interface_id = str(interface["id"])
            owner_extensions = extensions_by_owner_interface.get((authority_name, interface_id), ())
            owner_annotation = ""
            if owner_extensions:
                kinds = {str(item["kind"]) for item in owner_extensions}
                label = "Defines protocol" if kinds == {"process-protocol"} else "Defines extension"
                if len(owner_extensions) != 1:
                    label += "s"
                owner_annotation = f" — {label}: {extension_links(owner_extensions)}."
            result.append(
                f"{prefix}  - [{_md(interface['label'])}]"
                f"({_relative_link(source, _interface_index_path(authority_name, interface_id))}) "
                f"({interface['contract_elements']}){owner_annotation}"
            )
        return result

    def render_node(node: Mapping[str, object], depth: int) -> list[str]:
        prefix = "  " * depth
        authorities = cast(Sequence[Mapping[str, object]], node["authorities"])
        result = [f"{prefix}- {_map_node_link(source, node)}"]
        for authority in authorities:
            result.extend(render_authority(authority, prefix=f"{prefix}  "))
        for child in children.get(str(node["id"]), []):
            result.extend(render_node(child, depth + 1))
        return result

    lines = ["## Contract map", ""]
    for top in children[None]:
        lines.extend([f"### {_map_node_link(source, top)}", ""])
        top_children = children.get(str(top["id"]), [])
        if top_children:
            for child in top_children:
                lines.extend(render_node(child, 0))
        else:
            authorities = cast(Sequence[Mapping[str, object]], top["authorities"])
            for authority in authorities:
                lines.extend(render_authority(authority, prefix=""))
        lines.append("")
    return lines


def _render_surface_authorities(
    *,
    source: str,
    node: Mapping[str, object],
    relationship: Mapping[str, object],
    elements: Sequence[Mapping[str, object]],
) -> list[str]:
    records = cast(Sequence[Mapping[str, object]], node["authorities"])
    by_authority: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for element in elements:
        by_authority[str(element["authority"])].append(element)
    relationship_nodes = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], relationship["nodes"])
    }
    lines = [
        f"## {node['title']}",
        "",
        f"Authorities: **{len(records)}** · Contract elements: "
        f"**{sum(cast(int, item['contract_elements']) for item in records)}**",
        "",
    ]
    if not records:
        return lines
    lines.extend(
        [
            "| Exact authority | Contract elements | Interfaces | Maintained purpose |",
            "|---|---:|---|---|",
        ]
    )
    for record in records:
        authority = str(record["authority"])
        interfaces = sorted(
            {str(item["interface"]) for item in by_authority[authority]},
            key=_interface_sort_key,
        )
        owners = [
            relationship_nodes[owner]
            for owner in cast(Sequence[str], record["owner_component_ids"])
        ]
        purposes = sorted({str(owner["description"]) for owner in owners})
        lines.append(
            f"| [{_md(authority)}]({_relative_link(source, _authority_index_path(authority))}) | "
            f"{record['contract_elements']} | "
            f"{_md(', '.join(_interface_label(interface) for interface in interfaces))} | "
            f"{_md(' '.join(purposes) or 'Cross-cutting generated authority.')} |"
        )
    lines.append("")
    return lines


def _render_contract_surfaces(
    relationship: Mapping[str, object],
    elements: Sequence[Mapping[str, object]],
) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    """Render pages only where the auditor's semantic question changes."""

    root_path = f"{ATLAS_DIRECTORY}/index.md"
    nodes = _contract_map_nodes(relationship)
    files: dict[str, bytes] = {}
    metadata: dict[str, dict[str, object]] = {}

    surface_specs = (
        (
            f"{ATLAS_DIRECTORY}/surfaces/riverhog.md",
            "Riverhog product",
            "Riverhog owns the public archive service and the reusable contracts that define "
            "its maintained extension boundaries. Reference implementations remain nonnormative.",
            ("riverhog-service", "riverhog-contracts", "riverhog-implementation"),
        ),
        (
            f"{ATLAS_DIRECTORY}/surfaces/references.md",
            "Maintainer-selected Riverhog references",
            str(relationship["reference_policy"]),
            ("riverhog-references", "gogurt", "mango-fish", "piggity"),
        ),
        (
            f"{ATLAS_DIRECTORY}/surfaces/stove0.md",
            "Stove0",
            "Stove0 is a maintainer-selected, nonnormative Riverhog reference application. "
            "It owns its interfaces and state without becoming Riverhog authority.",
            (
                "stove0-application",
                "stove0-observers",
                "stove0-targets",
                "stove0-review",
                "stove0-recipes",
            ),
        ),
        (
            f"{ATLAS_DIRECTORY}/surfaces/cross-cutting.md",
            "Cross-cutting v1 authorities",
            "These generated authorities bind repository-wide configuration, state, extent, "
            "and boundary semantics without becoming a separate product surface.",
            ("cross-cutting",),
        ),
    )
    for path, title, description, node_ids in surface_specs:
        lines = [
            f"# {title}",
            "",
            f"[Atlas]({_relative_link(path, root_path)})",
            "",
            description,
            "",
            "## Surface shape",
            "",
            "| Semantic area | Exact authorities | Contract elements |",
            "|---|---:|---:|",
        ]
        for node_id in node_ids:
            node = nodes[node_id]
            authorities = cast(Sequence[Mapping[str, object]], node["authorities"])
            lines.append(
                f"| {node['title']} | {len(authorities)} | "
                f"{sum(cast(int, item['contract_elements']) for item in authorities)} |"
            )
        lines.append("")
        for node_id in node_ids:
            lines.extend(
                _render_surface_authorities(
                    source=path,
                    node=nodes[node_id],
                    relationship=relationship,
                    elements=elements,
                )
            )
        counts = {
            "authorities": sum(
                len(cast(Sequence[object], nodes[node_id]["authorities"])) for node_id in node_ids
            ),
            "contract_elements": sum(
                cast(int, item["contract_elements"])
                for node_id in node_ids
                for item in cast(Sequence[Mapping[str, object]], nodes[node_id]["authorities"])
            ),
        }
        files[path] = ("\n".join(lines).rstrip() + "\n").encode()
        metadata[path] = {
            "kind": "semantic-surface",
            "counts": counts,
            "map_node_ids": list(node_ids),
        }
    return files, metadata


def _render_extension_contexts(
    relationship: Mapping[str, object],
) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    """Render non-semantic context for each frozen extension boundary."""

    root_path = f"{ATLAS_DIRECTORY}/index.md"
    evidence_path = f"{ATLAS_DIRECTORY}/evidence/relationships.md"
    nodes = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], relationship["nodes"])
    }
    edges = cast(Sequence[Mapping[str, object]], relationship["edges"])
    extensions = sorted(
        (
            item
            for item in nodes.values()
            if item["kind"] in {"extension-point", "process-protocol"}
        ),
        key=lambda item: str(item["id"]),
    )
    files: dict[str, bytes] = {}
    metadata: dict[str, dict[str, object]] = {}
    for extension in extensions:
        path = _extension_context_path(extension)
        owner = str(extension["owner"])
        owner_edges = [
            edge
            for edge in edges
            if edge["target"] == extension["id"]
            and edge["type"] in {"owns-extension-point", "owns-protocol"}
        ]
        if len(owner_edges) != 1:
            raise ContractAtlasError(
                f"extension must have one exact owner relationship: {extension['id']}"
            )
        implementation_edges = sorted(
            (
                edge
                for edge in edges
                if edge["target"] == extension["id"]
                and edge["type"] in {"implements-extension-point", "implements-protocol"}
            ),
            key=lambda edge: (str(edge["source"]), str(edge.get("binding", ""))),
        )
        mechanism = (
            "Python entry-point extension"
            if extension["kind"] == "extension-point"
            else "independently deployed process protocol"
        )
        node_link = _anchor_link(
            path, evidence_path, _relationship_node_anchor(str(extension["id"]))
        )
        owner_link = _anchor_link(path, evidence_path, _relationship_edge_anchor(owner_edges[0]))
        lines = [
            f"# {extension['name']}",
            "",
            f"[Atlas]({_relative_link(path, root_path)}) · [Relationship evidence]({node_link})",
            "",
            str(extension["description"]),
            "",
            f"- Identity: `{extension['id']}`",
            f"- Mechanism: {mechanism}",
            f"- Owner: [{_md(owner)}]"
            f"({_relative_link(path, _authority_index_path(owner))}) "
            f"([exact relationship]({owner_link}))",
            "",
            "## Semantic interfaces",
            "",
        ]
        for interface in cast(Sequence[Mapping[str, object]], extension["semantic_interfaces"]):
            interface_authority = str(interface["authority"])
            interface_id = str(interface["interface"])
            interface_path = _interface_index_path(interface_authority, interface_id)
            binding_edges = [
                edge
                for edge in edges
                if edge["target"] == extension["id"]
                and edge["source"] == f"component:{interface_authority}"
                and edge["type"] == "binds-protocol"
            ]
            if len(binding_edges) > 1:
                raise ContractAtlasError(
                    f"semantic interface repeats an extension binding: {extension['id']}"
                )
            binding_link = (
                _anchor_link(
                    path,
                    evidence_path,
                    _relationship_edge_anchor(binding_edges[0]),
                )
                if binding_edges
                else ""
            )
            binding = f" ([exact binding]({binding_link}))" if binding_link else ""
            lines.append(
                f"- [{_md(interface_authority)} · {_md(interface['label'])}]"
                f"({_relative_link(path, interface_path)}){binding}"
            )
        lines.extend(
            [
                "",
                "## Checked-in nonnormative implementations",
                "",
            ]
        )
        if implementation_edges:
            for edge in implementation_edges:
                provider = nodes[str(edge["source"])]
                edge_link = _anchor_link(path, evidence_path, _relationship_edge_anchor(edge))
                lines.append(
                    f"- [{_md(provider['name'])}]({edge_link}) — {_md(provider['description'])}"
                )
        else:
            lines.append("No checked-in implementation is part of this conformance set.")
        files[path] = ("\n".join(lines).rstrip() + "\n").encode()
        metadata[path] = {
            "kind": "extension-context",
            "counts": {},
            "extension_id": extension["id"],
        }
    if len(files) != len(extensions):
        raise ContractAtlasError("extension context paths are not unique")
    return files, metadata


def _render_atlas(
    elements: list[dict[str, object]],
    policies: Mapping[str, object],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    identities: Mapping[str, object],
    exclusions: Sequence[Mapping[str, object]],
    discovery: Mapping[str, object],
    component_descriptions: Mapping[str, str],
) -> tuple[dict[str, bytes], list[dict[str, object]], dict[str, object]]:
    files: dict[str, bytes] = {}
    descriptors: list[dict[str, object]] = []
    source_evidence_path = f"{ATLAS_DIRECTORY}/evidence/sources.md"
    by_id = {str(item["id"]): item for item in elements}
    grouped: dict[str, dict[str, list[dict[str, object]]]] = defaultdict(lambda: defaultdict(list))
    for item in elements:
        grouped[str(item["authority"])][str(item["interface"])].append(item)

    for element in elements:
        files[str(element["dossier"])] = _render_dossier(element, projection, trace, by_id)

    policy_path = f"{ATLAS_DIRECTORY}/policies/index.md"
    policy_lines = [
        "# V1 contract policies",
        "",
        f"[Atlas]({_relative_link(policy_path, f'{ATLAS_DIRECTORY}/index.md')})",
        "",
        "Policies are defined once here and referenced from every dossier where they are "
        "proven to apply. "
        "Implementation-correctness witnesses remain outside this contract freeze artifact.",
    ]
    applications = Counter(
        [policy for item in elements for policy in cast(Sequence[str], item["policy_ids"])]
        + [str(item["policy_id"]) for item in exclusions]
    )
    policy_applications: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for element in elements:
        for policy_id in cast(Sequence[str], element["policy_ids"]):
            policy_applications[policy_id].append(element)
    for exclusion in exclusions:
        policy_applications[str(exclusion["policy_id"])].append(exclusion)
    for category, values in policies.items():
        category_policies = cast(Sequence[Mapping[str, object]], values)
        policy_lines.extend(
            [
                "",
                f"## {str(category).replace('_', ' ').title()}",
                "",
                "| Policy | Applications |",
                "|---|---:|",
                *(
                    f"| [{_md(policy['id'])}]"
                    f"(#{_policy_anchor(str(policy['id']))}) | "
                    f"{applications.get(str(policy['id']), 0)} |"
                    for policy in category_policies
                ),
                "",
                "### Definitions",
                "",
            ]
        )
        for policy in category_policies:
            meaning = policy["meaning"]
            application_items = policy_applications[str(policy["id"])]
            applicability = policy.get("applies_to", policy.get("scope", "declared applications"))
            executable_authorities = sorted(
                {
                    source
                    for item in application_items
                    for source in cast(Sequence[str], item["source_authority_ids"])
                }
            )
            observable = (
                {
                    key: value
                    for key, value in cast(Mapping[str, object], meaning).items()
                    if key
                    in {
                        "capacity_behavior",
                        "completion",
                        "exceeded",
                        "hidden_maximum",
                        "silent_truncation",
                    }
                }
                if isinstance(meaning, Mapping)
                else {
                    "conforming_result": "the observable surface satisfies the stated meaning",
                    "violation": "the observable surface contradicts the stated meaning",
                }
            )
            rendered_meaning = (
                "\n".join(
                    [
                        "| Rule field | Value |",
                        "|---|---|",
                        *(
                            f"| `{_md(key)}` | {_md(value)} |"
                            for key, value in cast(Mapping[str, object], meaning).items()
                        ),
                    ]
                )
                if isinstance(meaning, Mapping)
                else str(meaning)
            )
            executable_links = []
            for source in executable_authorities:
                source_target = _anchor_link(
                    policy_path, source_evidence_path, _source_anchor(source)
                )
                executable_links.append(f"  - [{_md(source)}]({source_target})")
            policy_lines.extend(
                [
                    _html_anchor(_policy_anchor(str(policy["id"]))),
                    f"#### `{policy['id']}`",
                    "",
                    rendered_meaning,
                    "",
                    f"- Applicability: `{_md(json.dumps(applicability, ensure_ascii=False))}`",
                    "- Observable result or violation: "
                    f"`{_md(json.dumps(observable, ensure_ascii=False, sort_keys=True))}`",
                    "- Executable authorities:",
                    *executable_links,
                    "",
                    f"Applications: **{applications.get(str(policy['id']), 0)}**",
                ]
            )
    files[policy_path] = ("\n".join(policy_lines).rstrip() + "\n").encode()

    for authority, interfaces in sorted(grouped.items()):
        authority_slug = _slug(authority, limit=72)
        authority_path = f"{ATLAS_DIRECTORY}/authorities/{authority_slug}/index.md"
        authority_elements = [item for values in interfaces.values() for item in values]
        counts = _counts(authority_elements)
        lines = [
            f"# {authority}",
            "",
            f"[Atlas]({_relative_link(authority_path, f'{ATLAS_DIRECTORY}/index.md')}) · "
            f"[Policies]({_relative_link(authority_path, policy_path)})",
            "",
            f"Contract elements: **{counts['contract_elements']}** · "
            f"Extent decisions: **{counts['extent_decisions']}**",
            "",
            "## Interfaces",
            "",
        ]
        for interface, values in sorted(
            interfaces.items(), key=lambda item: _interface_sort_key(item[0])
        ):
            interface_path = (
                f"{ATLAS_DIRECTORY}/authorities/{authority_slug}/"
                f"{_slug(interface, limit=48)}/index.md"
            )
            lines.append(
                f"- [{_interface_label(interface)}]"
                f"({_relative_link(authority_path, interface_path)}) ({len(values)})"
            )
        files[authority_path] = ("\n".join(lines).rstrip() + "\n").encode()

        for interface, values in sorted(
            interfaces.items(), key=lambda item: _interface_sort_key(item[0])
        ):
            interface_path = (
                f"{ATLAS_DIRECTORY}/authorities/{authority_slug}/"
                f"{_slug(interface, limit=48)}/index.md"
            )
            lines = [
                f"# {authority}: {_interface_label(interface)}",
                "",
                f"[Atlas]({_relative_link(interface_path, f'{ATLAS_DIRECTORY}/index.md')}) · "
                f"[Authority]({_relative_link(interface_path, authority_path)}) · "
                f"[Policies]({_relative_link(interface_path, policy_path)})",
                "",
                INTERFACE_PURPOSES.get(
                    interface,
                    f"{_interface_label(interface)} contract owned by {authority}.",
                ),
                "",
                "## Semantic dossiers",
                "",
            ]
            if interface in RELEASE_INTERFACES:
                prefix = RELEASE_INTERFACE_PREFIXES[interface]

                def release_label_candidates(
                    item: Mapping[str, object], prefix: str = prefix
                ) -> list[str]:
                    title = str(item["title"])
                    return [title[len(prefix) :] if title.startswith(prefix) else title, title]

                labels = _contextual_labels(
                    values,
                    release_label_candidates,
                )
                lines.extend(
                    [
                        "| Exact unit | Classification |",
                        "|---|---|",
                    ]
                )
                for release_item in sorted(values, key=lambda value: str(value["title"])):
                    details = cast(Mapping[str, object], release_item.get("details", {}))
                    classification = details.get("classification", "—")
                    rendered_classification = (
                        f"`{_md(classification)}`" if classification != "—" else "—"
                    )
                    lines.append(
                        f"| [{_md(labels[str(release_item['id'])])}]"
                        f"({_relative_link(interface_path, str(release_item['dossier']))}) | "
                        f"{rendered_classification} |"
                    )
            elif interface == "cli":
                labels = _contextual_labels(
                    values,
                    lambda item: [
                        " ".join(
                            cast(
                                Sequence[str],
                                cast(Mapping[str, object], item.get("details", {})).get(
                                    "command_path", ()
                                ),
                            )[1:]
                        )
                        if len(
                            cast(
                                Sequence[str],
                                cast(Mapping[str, object], item.get("details", {})).get(
                                    "command_path", ()
                                ),
                            )
                        )
                        > 1
                        else str(item["title"]),
                        str(item["title"]),
                    ],
                )
                executable = sorted(
                    (
                        item
                        for item in values
                        if cast(Mapping[str, object], item.get("details", {})).get("executable")
                    ),
                    key=lambda item: str(item["title"]),
                )
                groups = sorted(
                    (item for item in values if item not in executable),
                    key=lambda item: str(item["title"]),
                )
                lines.extend(
                    [
                        f"Executable commands: **{len(executable)}** · "
                        f"Command groups: **{len(groups)}**",
                        "",
                        "### Executable commands",
                        "",
                    ]
                )
                for item in executable:
                    lines.append(
                        f"- [{_md(labels[str(item['id'])])}]"
                        f"({_relative_link(interface_path, str(item['dossier']))})"
                    )
                if groups:
                    lines.extend(["", "### Command groups", ""])
                    for item in groups:
                        lines.append(
                            f"- [{_md(labels[str(item['id'])])}]"
                            f"({_relative_link(interface_path, str(item['dossier']))})"
                        )
            elif interface == "python":
                by_module: dict[str, list[dict[str, object]]] = defaultdict(list)
                for item in values:
                    details = cast(Mapping[str, object], item.get("details", {}))
                    by_module[str(details["module"])].append(item)
                for module, module_values in sorted(by_module.items()):
                    exports = {
                        str(cast(Mapping[str, object], item["details"])["public_identity"]): item
                        for item in module_values
                        if cast(Mapping[str, object], item["details"])["unit"] == "export"
                    }
                    members: dict[str, list[dict[str, object]]] = defaultdict(list)
                    for item in module_values:
                        details = cast(Mapping[str, object], item["details"])
                        if details["unit"] == "member":
                            members[str(details["owner"])].append(item)
                    if set(members) - set(exports):
                        raise ContractAtlasError(
                            f"Python interface index has members without exports: {module}"
                        )
                    export_values = [exports[identity] for identity in sorted(exports)]

                    def export_label_candidates(
                        item: Mapping[str, object], module_name: str = module
                    ) -> list[str]:
                        public = str(cast(Mapping[str, object], item["details"])["public_identity"])
                        return [public.removeprefix(f"{module_name}."), public]

                    export_labels = _contextual_labels(
                        export_values,
                        export_label_candidates,
                    )
                    lines.extend([f"### `{_md(module)}`", ""])
                    for public_identity, item in sorted(exports.items()):
                        lines.append(
                            f"- [{_md(export_labels[str(item['id'])])}]"
                            f"({_relative_link(interface_path, str(item['dossier']))})"
                        )
                        owner_members = sorted(
                            members.get(public_identity, ()), key=lambda value: str(value["title"])
                        )

                        def member_label_candidates(
                            member: Mapping[str, object],
                            owner_identity: str = public_identity,
                            module_name: str = module,
                        ) -> list[str]:
                            title = str(member["title"])
                            return [
                                title.removeprefix(f"{owner_identity}."),
                                title.removeprefix(f"{module_name}."),
                                title,
                            ]

                        member_labels = _contextual_labels(
                            owner_members,
                            member_label_candidates,
                        )
                        for member in owner_members:
                            lines.append(
                                f"  - [{_md(member_labels[str(member['id'])])}]"
                                f"({_relative_link(interface_path, str(member['dossier']))})"
                            )
                    lines.append("")
            else:
                values.sort(
                    key=lambda item: (
                        (
                            str(
                                cast(Mapping[str, object], item.get("details", {})).get(
                                    "method", ""
                                )
                            ),
                            str(
                                cast(Mapping[str, object], item.get("details", {})).get("path", "")
                            ),
                            str(item["title"]),
                        )
                        if interface in {"http-operations", "process-protocol-operations"}
                        else (str(item["title"]),)
                    )
                )
                labels = (
                    _contextual_labels(
                        values,
                        lambda item: [
                            str(item["title"]).removeprefix("schemas: "),
                            str(item["title"]),
                        ],
                    )
                    if interface == "http-schemas"
                    else {str(item["id"]): str(item["title"]) for item in values}
                )
                for item in values:
                    lines.append(
                        f"- [{_md(labels[str(item['id'])])}]"
                        f"({_relative_link(interface_path, str(item['dossier']))})"
                    )
            files[interface_path] = ("\n".join(lines).rstrip() + "\n").encode()

    relationship = _relationship_model(projection, trace, elements, component_descriptions)
    surface_files, surface_metadata = _render_contract_surfaces(relationship, elements)
    extension_files, extension_metadata = _render_extension_contexts(relationship)
    files.update(surface_files)
    files.update(extension_files)

    root_counts = _counts(elements, exclusions)
    root_path = f"{ATLAS_DIRECTORY}/index.md"
    evidence_path = f"{ATLAS_DIRECTORY}/evidence/index.md"
    exclusion_path = f"{ATLAS_DIRECTORY}/evidence/exclusions.md"
    authority_evidence_path = f"{ATLAS_DIRECTORY}/evidence/authorities.md"
    configuration_evidence_path = f"{ATLAS_DIRECTORY}/evidence/configuration.md"
    source_evidence_path = f"{ATLAS_DIRECTORY}/evidence/sources.md"
    relationship_evidence_path = f"{ATLAS_DIRECTORY}/evidence/relationships.md"
    identity_evidence_path = f"{ATLAS_DIRECTORY}/evidence/identities.md"
    policy_by_id = {
        str(policy["id"]): policy
        for values in policies.values()
        for policy in cast(Sequence[Mapping[str, object]], values)
    }
    exclusion_lines = [
        "# Explicitly excluded candidates",
        "",
        f"[Atlas]({_relative_link(exclusion_path, root_path)}) · "
        f"[Freeze evidence]({_relative_link(exclusion_path, evidence_path)}) · "
        f"[Policies]({_relative_link(exclusion_path, policy_path)})",
        "",
        "This page supports the ‘no more’ side of the audit by naming every discovered release "
        "candidate intentionally excluded from freeze protection.",
        "",
        f"Excluded candidates: **{len(exclusions)}**",
        "",
    ]
    for policy_id in sorted({str(item["policy_id"]) for item in exclusions}):
        policy = policy_by_id[policy_id]
        exclusion_lines.extend(
            [
                f"## [{policy_id}]"
                f"({_anchor_link(exclusion_path, policy_path, _policy_anchor(policy_id))})",
                "",
                str(policy["meaning"]),
                "",
                "### Excluded candidates",
                "",
            ]
        )
        for exclusion in sorted(
            (item for item in exclusions if item["policy_id"] == policy_id),
            key=lambda value: str(value["id"]),
        ):
            exclusion_lines.extend(
                [
                    f"- {_anchor_marker('exclusion', str(exclusion['id']))}`{exclusion['id']}`",
                    f"  - kind: `{exclusion['kind']}`",
                    f"  - installed target: `{exclusion['installed_target']}`",
                ]
            )
        exclusion_lines.extend(
            [
                "",
                "### Exact accounting",
                "",
                "| Candidate | Boundary source | Detector | Source authority |",
                "|---|---|---|---|",
            ]
        )
        for exclusion in sorted(
            (item for item in exclusions if item["policy_id"] == policy_id),
            key=lambda value: str(value["id"]),
        ):
            source_links = ", ".join(
                f"[{_md(source)}]"
                f"({_anchor_link(exclusion_path, source_evidence_path, _source_anchor(source))})"
                for source in cast(Sequence[str], exclusion["source_authority_ids"])
            )
            exclusion_lines.append(
                f"| [{_md(exclusion['id'])}]"
                f"(#{_anchor_id('exclusion', str(exclusion['id']))}) | "
                f"`{_md(exclusion['boundary_pointer'])}` | "
                f"`{_md(exclusion['detector'])}` | {source_links} |"
            )
    files[exclusion_path] = ("\n".join(exclusion_lines).rstrip() + "\n").encode()

    source_index = _source_index(trace)
    source_counts = cast(Mapping[str, object], root_counts["by_source_authority"])
    source_lines = [
        "# Source and qualification inventory",
        "",
        f"[Atlas]({_relative_link(source_evidence_path, root_path)}) · "
        f"[Freeze evidence]({_relative_link(source_evidence_path, evidence_path)})",
        "",
        "This page is proof routing, not contract navigation. Every dossier names its locally "
        "applicable executable sources and qualification routes.",
        "",
        "## Qualification routes",
        "",
        *_table_counts(
            cast(Mapping[str, object], root_counts["by_qualification_route"]),
            "Qualification route",
            anchors={
                route: _qualification_anchor(route)
                for route in cast(Mapping[str, object], root_counts["by_qualification_route"])
            },
        ),
        "",
        "## Source authorities",
        "",
        f"Source authorities: **{len(source_index)}**",
        "",
        "| Source authority | Applications | Executable location |",
        "|---|---:|---|",
    ]
    for source_id in source_index:
        count = source_counts.get(source_id, 0)
        source_record = source_index[source_id]
        location = cast(Mapping[str, object], source_record.get("source", {}))
        bindings = cast(Sequence[Mapping[str, object]], source_record.get("bindings", ()))
        declarations = cast(Sequence[Mapping[str, object]], source_record.get("declarations", ()))
        rendered = str(
            location.get(
                "path",
                location.get(
                    "module",
                    bindings[0]["path"]
                    if bindings
                    else declarations[0]["path"]
                    if declarations
                    else source_id,
                ),
            )
        )
        if len(bindings) > 1:
            rendered += f" (+{len(bindings) - 1} binding)"
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        source_lines.append(
            f"| {_html_anchor(_source_anchor(source_id))}`{_md(source_id)}` | {count} | "
            f"`{_md(rendered + symbol)}` |"
        )
    state_sources = [
        source
        for source in source_index.values()
        if str(source["id"]).startswith("state:") and source.get("fixtures")
    ]
    if state_sources:
        source_lines.extend(
            [
                "",
                "## Durable-state fixture evidence",
                "",
                "Fixtures prove restart and introspection behavior; component declarations "
                "above remain the semantic structure authorities.",
                "",
                "| State authority | Fixture | SHA-256 |",
                "|---|---|---|",
            ]
        )
        for state_source in state_sources:
            for fixture in cast(Sequence[Mapping[str, object]], state_source["fixtures"]):
                source_lines.append(
                    f"| `{_md(state_source['id'])}` | `{_md(fixture['path'])}` | "
                    f"`{_md(fixture['sha256'])}` |"
                )
    files[source_evidence_path] = ("\n".join(source_lines).rstrip() + "\n").encode()

    authority_registry = cast(Mapping[str, object], trace["authority_registry"])
    declared_authorities = cast(
        Sequence[Mapping[str, object]], authority_registry["declared_authorities"]
    )
    noncontractual_projection = cast(
        Sequence[Mapping[str, object]], authority_registry["noncontractual_projection"]
    )
    configuration_registry = cast(Mapping[str, object], trace["configuration_registry"])
    configuration_document_registry = cast(
        Mapping[str, object], trace["configuration_document_registry"]
    )
    configuration_counts = cast(Mapping[str, object], configuration_registry["counts"])
    configuration_document_counts = cast(
        Mapping[str, object], configuration_document_registry["counts"]
    )
    configuration_coverage = cast(Mapping[str, object], configuration_registry["coverage"])
    configuration_dossiers: dict[str, str] = {}
    for element in elements:
        if element["interface"] != "configuration-environment":
            continue
        for pointer in cast(Sequence[str], element["pointers"]):
            value = pointer_value(projection, pointer)
            if isinstance(value, Mapping) and isinstance(value.get("id"), str):
                configuration_dossiers[str(value["id"])] = str(element["dossier"])
    configuration_document_dossiers = {
        pointer.rsplit("/", 1)[-1].replace("~1", "/").replace("~0", "~"): str(element["dossier"])
        for element in elements
        if element["interface"] == "configuration"
        for pointer in cast(Sequence[str], element["pointers"])
        if pointer.startswith("/external_contract/configuration_documents/")
    }
    configuration_lines = [
        "# Configuration ownership registry",
        "",
        f"[Atlas]({_relative_link(configuration_evidence_path, root_path)}) · "
        f"[Freeze evidence]({_relative_link(configuration_evidence_path, evidence_path)})",
        "",
        "This is the exhaustive discovery and reconciliation view. It does not own any "
        "setting's semantics; every protected entry links to its normative owner's dossier.",
        "",
        "## Coverage",
        "",
        "| Check | Result |",
        "|---|---:|",
        *(
            f"| {_md(name.replace('_', ' '))} | {'pass' if count == 0 else count} |"
            for name, count in configuration_coverage.items()
        ),
        "",
        "## Shape",
        "",
        f"Environment contracts: **{configuration_counts['contracts']}** · "
        f"Configuration documents: **{configuration_document_counts['contracts']}** · "
        f"Unique names: **{configuration_counts['unique_environment_names']}** · "
        f"Parameterized families: **{configuration_counts['patterns']}** · "
        f"Raw implementation reads: **{configuration_counts['detections']}** · "
        f"Explicit ambiguity resolutions: **{configuration_counts['resolution_exceptions']}**",
        "",
        *_table_counts(
            cast(Mapping[str, object], configuration_counts["by_owner"]), "Normative owner"
        ),
        "",
        "## Exact environment contracts",
        "",
        "| Normative owner | Setting | Consumers | Default expressions | Source |",
        "|---|---|---|---|---|",
    ]
    for record in cast(Sequence[Mapping[str, object]], configuration_registry["records"]):
        contract_id = str(record["id"])
        dossier = configuration_dossiers.get(contract_id)
        setting = (
            f"[{_md(record['name'])}]({_relative_link(configuration_evidence_path, dossier)})"
            if dossier is not None
            else f"`{_md(record['name'])}`"
        )
        source_id = str(record["source_authority_id"])
        source_link = _anchor_link(
            configuration_evidence_path,
            source_evidence_path,
            _source_anchor(source_id),
        )
        configuration_lines.append(
            f"| `{_md(record['owner'])}` | {setting} | "
            f"`{_md(', '.join(cast(Sequence[str], record['consumers'])))}` | "
            f"`{_md(', '.join(cast(Sequence[str], record['default_expressions'])))}` | "
            f"[{_md(source_id)}]({source_link}) |"
        )
    patterns = cast(Sequence[Mapping[str, object]], configuration_registry["patterns"])
    if patterns:
        configuration_lines.extend(
            [
                "",
                "## Parameterized families",
                "",
                "| Normative owner | Template | Consumers | Settings |",
                "|---|---|---|---|",
            ]
        )
        for pattern in patterns:
            dossier = configuration_dossiers.get(str(pattern["id"]))
            template = (
                f"[{_md(pattern['template'])}]"
                f"({_relative_link(configuration_evidence_path, dossier)})"
                if dossier is not None
                else f"`{_md(pattern['template'])}`"
            )
            configuration_lines.append(
                f"| `{_md(pattern['owner'])}` | {template} | "
                f"`{_md(', '.join(cast(Sequence[str], pattern['consumers'])))}` | "
                f"`{_md(', '.join(cast(Sequence[str], pattern['settings'])))}` |"
            )
    configuration_lines.extend(
        [
            "",
            "## Exact configuration documents",
            "",
            "| Normative owner | Configuration authority | Consumers | Input shape | Source |",
            "|---|---|---|---|---|",
        ]
    )
    for record in cast(
        Sequence[Mapping[str, object]], configuration_document_registry["candidates"]
    ):
        contract_id = str(record["id"])
        dossier = configuration_document_dossiers.get(contract_id)
        authority = (
            f"[{_md(contract_id)}]({_relative_link(configuration_evidence_path, dossier)})"
            if dossier is not None
            else f"`{_md(contract_id)}`"
        )
        source_id = f"configuration:{contract_id}"
        source_link = _anchor_link(
            configuration_evidence_path,
            source_evidence_path,
            _source_anchor(source_id),
        )
        configuration_lines.append(
            f"| `{_md(record['owner'])}` | {authority} | "
            f"`{_md(', '.join(cast(Sequence[str], record['consumers'])))}` | "
            f"`{_md(', '.join(cast(Sequence[str], record['input_shapes'])))}` | "
            f"[{_md(source_id)}]({source_link}) |"
        )
    files[configuration_evidence_path] = ("\n".join(configuration_lines).rstrip() + "\n").encode()

    authority_lines = [
        "# Exact authority inventory",
        "",
        f"[Atlas]({_relative_link(authority_evidence_path, root_path)}) · "
        f"[Freeze evidence]({_relative_link(authority_evidence_path, evidence_path)})",
        "",
        "This page is intentionally an alphabetical reconciliation inventory, not another "
        "contract map.",
        "",
        "## Aggregate ownership",
        "",
        *_table_counts(cast(Mapping[str, object], root_counts["by_interface"]), "Interface"),
        "",
        "## Declared aggregate authorities",
        "",
        "These cross-component authorities are explicit repository decisions. Component and "
        "durable-state authorities come directly from their frozen registries.",
        "",
        "| Authority | Normative scope |",
        "|---|---|",
        *(f"| `{_md(item['id'])}` | {_md(item['meaning'])} |" for item in declared_authorities),
        "",
        "## Non-contractual projection machinery",
        "",
        "These values remain in the exact machine projection for validation, but do not own "
        "external product promises.",
        "",
        "| Projection record | Machine authority | Reason |",
        "|---|---|---|",
        *(
            f"| `{_md(item['id'])}` | "
            f"`{_md(', '.join(cast(Sequence[str], item['pointers'])))}` | "
            f"{_md(item['reason'])} |"
            for item in noncontractual_projection
        ),
        "",
        "## Authorities",
        "",
        "| Authority | Contract elements | Interfaces |",
        "|---|---:|---|",
    ]
    for authority, interfaces in sorted(grouped.items()):
        authority_elements = [item for values in interfaces.values() for item in values]
        authority_lines.append(
            f"| [{_md(authority)}]"
            f"({_relative_link(authority_evidence_path, _authority_index_path(authority))}) | "
            f"{len(authority_elements)} | {_md(', '.join(sorted(interfaces)))} |"
        )
    files[authority_evidence_path] = ("\n".join(authority_lines).rstrip() + "\n").encode()

    relationship_nodes = cast(Sequence[Mapping[str, object]], relationship["nodes"])
    relationship_edges = cast(Sequence[Mapping[str, object]], relationship["edges"])
    nodes_by_id = {str(item["id"]): item for item in relationship_nodes}
    relationship_lines = [
        "# Relationship-edge inventory",
        "",
        f"[Atlas]({_relative_link(relationship_evidence_path, root_path)}) · "
        f"[Freeze evidence]({_relative_link(relationship_evidence_path, evidence_path)})",
        "",
        "This is the exact generated node and edge set behind the human contract map. It is "
        "evidence, not a second navigation hierarchy.",
        "",
        "## Relationship shape",
        "",
        *_table_counts(
            dict(sorted(Counter(str(item["kind"]) for item in relationship_nodes).items())),
            "Node kind",
        ),
        "",
        *_table_counts(
            dict(sorted(Counter(str(item["type"]) for item in relationship_edges).items())),
            "Relationship",
        ),
        "",
        "## Exact nodes",
        "",
        "| Identity | Kind | Name | Role or owner | Maintained purpose |",
        "|---|---|---|---|---|",
    ]
    for node in relationship_nodes:
        node_name = str(node["name"])
        rendered_name = (
            f"[{_md(node_name)}]"
            f"({_relative_link(relationship_evidence_path, _authority_index_path(node_name))})"
            if node_name in grouped
            else f"`{_md(node_name)}`"
        )
        relationship_lines.append(
            f"| {_html_anchor(_relationship_node_anchor(str(node['id'])))}`{_md(node['id'])}` | "
            f"`{_md(node['kind'])}` | {rendered_name} | "
            f"`{_md(node.get('role', node.get('owner', '—')))}` | "
            f"{_md(node['description'])} |"
        )
    relationship_lines.extend(
        [
            "",
            "## Exact edges",
            "",
            "| From | Relationship | To | Scope or binding |",
            "|---|---|---|---|",
        ]
    )
    for edge in relationship_edges:
        detail = edge.get("scope", edge.get("binding", ""))
        source_node = nodes_by_id[str(edge["source"])]
        target_node = nodes_by_id[str(edge["target"])]
        relationship_lines.append(
            f"| {_html_anchor(_relationship_edge_anchor(edge))}"
            f"[{_md(source_node['name'])}](#{_relationship_node_anchor(str(source_node['id']))}) | "
            f"`{_md(edge['type'])}` | "
            f"[{_md(target_node['name'])}](#{_relationship_node_anchor(str(target_node['id']))}) | "
            f"`{_md(detail)}` |"
        )
    files[relationship_evidence_path] = ("\n".join(relationship_lines).rstrip() + "\n").encode()

    identity_lines = [
        "# Identity domains",
        "",
        f"[Atlas]({_relative_link(identity_evidence_path, root_path)}) · "
        f"[Freeze evidence]({_relative_link(identity_evidence_path, evidence_path)})",
        "",
        "These independent identities distinguish frozen semantics, discovery coverage, proof "
        "trace, and the replaceable human representation.",
        "",
        "| Identity domain | SHA-256 |",
        "|---|---|",
        *(
            f"| {_anchor_marker('identity', str(name))}`{_md(name)}` | `{_md(value)}` |"
            for name, value in identities.items()
        ),
        "",
        f"{_anchor_marker('identity', 'atlas_representation_sha256')}"
        "The byte-exact `atlas_representation_sha256` is recorded at "
        "`/identities/atlas_representation_sha256` in the machine closure. It cannot be embedded "
        "inside the document bytes that it identifies.",
    ]
    files[identity_evidence_path] = ("\n".join(identity_lines).rstrip() + "\n").encode()

    anomalies = cast(Mapping[str, object], discovery["anomalies"])
    evidence_lines = [
        "# Freeze evidence",
        "",
        f"[Atlas]({_relative_link(evidence_path, root_path)})",
        "",
        "This layer answers whether the discovered external universe was accounted for exactly. "
        "Machine closure does not assert that the contract is minimal, desirable, or freeze-ready; "
        "that remains the human audit decision.",
        "",
        "## What the machine proves",
        "",
        "| Check | Result |",
        "|---|---:|",
        *(
            f"| {_md(name.replace('_', ' '))} | {'pass' if count == 0 else count} |"
            for name, count in anomalies.items()
        ),
        "",
        "## Closure totals",
        "",
        "| Measure | Value |",
        "|---|---:|",
        f"| Detected constructs | {len(cast(Sequence[object], discovery['detections']))} |",
        f"| Exact resolutions | {len(cast(Sequence[object], discovery['resolutions']))} |",
        f"| Resolved candidates | {len(cast(Sequence[object], discovery['candidates']))} |",
        f"| Explicit dispositions | {len(cast(Sequence[object], discovery['dispositions']))} |",
        f"| Contract elements | {root_counts['contract_elements']} |",
        f"| Extent decisions | {root_counts['extent_decisions']} |",
        f"| Explicit exclusions | {root_counts['excluded_candidates']} |",
        f"| Source authorities | {len(source_index)} |",
        "",
        "## Exact evidence",
        "",
        f"- [Explicit exclusions]({_relative_link(evidence_path, exclusion_path)})",
        f"- [Exact authority inventory]({_relative_link(evidence_path, authority_evidence_path)})",
        "- [Configuration ownership registry]"
        f"({_relative_link(evidence_path, configuration_evidence_path)})",
        "- [Source and qualification inventory]"
        f"({_relative_link(evidence_path, source_evidence_path)})",
        "- [Relationship-edge inventory]"
        f"({_relative_link(evidence_path, relationship_evidence_path)})",
        f"- [Identity domains]({_relative_link(evidence_path, identity_evidence_path)})",
    ]
    files[evidence_path] = ("\n".join(evidence_lines).rstrip() + "\n").encode()

    root_lines = [
        "# Riverhog repository v1 contract audit",
        "",
        "> **Audit question:** Is this exactly the external contract the Riverhog repository "
        "should support for v1 — no more, no less?",
        "",
        "**Audit path:** Scope → Semantics → Evidence",
        "",
        *_render_contract_map(relationship, source=root_path),
        "",
        "## Contract-wide policies",
        "",
        f"[Review the normative policies that govern the contract.]"
        f"({_relative_link(root_path, policy_path)})",
        "",
        "## Freeze evidence",
        "",
        f"[Verify completeness, exclusions, ownership, identities, and proof.]"
        f"({_relative_link(root_path, evidence_path)})",
    ]
    files[root_path] = ("\n".join(root_lines).rstrip() + "\n").encode()

    dossier_by_path = {str(item["dossier"]): item for item in elements}
    for path, payload in sorted(files.items()):
        dossier_element = dossier_by_path.get(path)
        if dossier_element is not None:
            document_counts = _counts([dossier_element])
            kind = "dossier"
        elif path == root_path:
            document_counts = root_counts
            kind = "root-index"
        elif path == policy_path:
            document_counts = {
                "policies": sum(len(cast(Sequence[object], value)) for value in policies.values())
            }
            kind = "policy-index"
        elif path == exclusion_path:
            document_counts = {"excluded_candidates": len(exclusions)}
            kind = "exclusion-index"
        elif path == evidence_path:
            document_counts = {
                "contract_elements": root_counts["contract_elements"],
                "extent_decisions": root_counts["extent_decisions"],
                "excluded_candidates": len(exclusions),
                "source_authorities": len(source_index),
            }
            kind = "evidence-index"
        elif path == authority_evidence_path:
            document_counts = {
                "authorities": len(grouped),
                "declared_aggregate_authorities": len(declared_authorities),
                "noncontractual_projection_records": len(noncontractual_projection),
            }
            kind = "evidence-authority-inventory"
        elif path == configuration_evidence_path:
            document_counts = {
                "configuration_contracts": configuration_counts["contracts"],
                "configuration_patterns": configuration_counts["patterns"],
            }
            kind = "evidence-configuration-inventory"
        elif path == source_evidence_path:
            document_counts = {
                "source_authorities": len(source_index),
                "qualification_routes": len(
                    cast(Mapping[str, object], root_counts["by_qualification_route"])
                ),
            }
            kind = "evidence-source-inventory"
        elif path == relationship_evidence_path:
            document_counts = {
                "nodes": len(relationship_nodes),
                "edges": len(relationship_edges),
            }
            kind = "evidence-relationship-inventory"
        elif path == identity_evidence_path:
            document_counts = {"identity_domains": len(identities) + 1}
            kind = "evidence-identity-inventory"
        elif path in surface_metadata:
            surface_descriptor = surface_metadata[path]
            document_counts = cast(dict[str, object], surface_descriptor["counts"])
            kind = str(surface_descriptor["kind"])
        elif path in extension_metadata:
            extension_descriptor = extension_metadata[path]
            document_counts = cast(dict[str, object], extension_descriptor["counts"])
            kind = str(extension_descriptor["kind"])
        elif path.endswith("/index.md") and path.count("/") == 3:
            authority_slug = path.split("/")[2]
            subset = [
                item
                for item in elements
                if _slug(str(item["authority"]), limit=72) == authority_slug
            ]
            document_counts = _counts(subset)
            kind = "authority-index"
        else:
            parts = path.split("/")
            authority_slug, interface_slug = parts[2], parts[3]
            subset = [
                item
                for item in elements
                if _slug(str(item["authority"]), limit=72) == authority_slug
                and _slug(str(item["interface"]), limit=48) == interface_slug
            ]
            document_counts = _counts(subset)
            kind = "interface-index"
        descriptors.append(
            {
                "path": path,
                "kind": kind,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "counts": document_counts,
                **({"element_id": dossier_element["id"]} if dossier_element is not None else {}),
                **(
                    {
                        key: value
                        for key, value in surface_metadata[path].items()
                        if key not in {"kind", "counts"}
                    }
                    if path in surface_metadata
                    else {}
                ),
                **(
                    {
                        key: value
                        for key, value in extension_metadata[path].items()
                        if key not in {"kind", "counts"}
                    }
                    if path in extension_metadata
                    else {}
                ),
            }
        )
    return files, descriptors, relationship


def build_atlas(
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    *,
    component_descriptions: Mapping[str, str],
) -> ContractAtlas:
    """Build one exact machine closure and its generated human navigation."""

    encoded_projection, projection_integer_paths = _encoded_json(projection)
    encoded_trace, trace_integer_paths = _encoded_json(trace)
    normalized_projection = cast(dict[str, object], encoded_projection)
    normalized_trace = cast(dict[str, object], encoded_trace)
    elements = _external_elements(normalized_projection, normalized_trace)
    _attach_extent_decisions(elements, normalized_projection)
    _link_operation_qualification(elements, normalized_trace)
    noncontractual_projection = _validate_authority_registry(
        elements, normalized_projection, normalized_trace
    )
    ids = [str(item["id"]) for item in elements]
    if len(ids) != len(set(ids)):
        raise ContractAtlasError("semantic contract element identities are not unique")
    _assign_dossiers(elements)
    elements.sort(key=lambda item: str(item["id"]))
    exclusions = [
        *_excluded_launchers(normalized_trace),
        *_excluded_python_packages(normalized_trace),
    ]
    source_index = _source_index(normalized_trace)
    for item in [*elements, *exclusions]:
        missing = set(cast(Sequence[str], item["source_authority_ids"])) - set(source_index)
        if missing:
            raise ContractAtlasError(f"contract element has unresolved sources: {sorted(missing)}")
    policies = _policy_registry(normalized_projection)
    declared_policy_ids = {
        str(policy["id"])
        for values in policies.values()
        for policy in cast(Sequence[Mapping[str, object]], values)
    }
    used_policy_ids = {
        policy for item in elements for policy in cast(Sequence[str], item["policy_ids"])
    } | {str(item["policy_id"]) for item in exclusions}
    if not used_policy_ids <= declared_policy_ids:
        undeclared = sorted(used_policy_ids - declared_policy_ids)
        raise ContractAtlasError(f"contract elements use undeclared policies: {undeclared}")
    detections = [
        {
            "id": f"detection:{item['id']}",
            "detector": item["detector"],
            "source_authority_ids": item["source_authority_ids"],
            "pointers": item["pointers"],
        }
        for item in elements
    ] + [
        {
            "id": f"detection:{item['candidate_id']}",
            "detector": item["detector"],
            "source_authority_ids": item["source_authority_ids"],
            "source": item["boundary_pointer"],
        }
        for item in exclusions
    ]
    resolutions = [
        {
            "detection_id": f"detection:{item['id']}",
            "candidate_id": f"candidate:{item['id']}",
            "authority": item["authority"],
            "interface": item["interface"],
            "element_id": item["id"],
        }
        for item in elements
    ] + [
        {
            "detection_id": f"detection:{item['candidate_id']}",
            "candidate_id": item["candidate_id"],
            "kind": item["kind"],
        }
        for item in exclusions
    ]
    candidates = [
        {
            "id": f"candidate:{item['id']}",
            "detector": item["detector"],
            "element_id": item["id"],
            "source_authority_ids": item["source_authority_ids"],
        }
        for item in elements
    ] + [
        {
            "id": item["candidate_id"],
            "detector": item["detector"],
            "source_authority_ids": item["source_authority_ids"],
        }
        for item in exclusions
    ]
    dispositions = [
        {
            "candidate_id": f"candidate:{item['id']}",
            "disposition": "protected",
            "policy_ids": item["policy_ids"],
        }
        for item in elements
    ] + [
        {
            "candidate_id": item["candidate_id"],
            "disposition": "excluded",
            "policy_ids": [item["policy_id"]],
        }
        for item in exclusions
    ]
    meta_closure = _detector_meta_closure(normalized_projection, normalized_trace)
    projection_coverage = _projection_coverage(
        elements, policies, normalized_projection, noncontractual_projection
    )
    detection_ids = [str(item["id"]) for item in detections]
    resolution_detection_ids = [str(item["detection_id"]) for item in resolutions]
    candidate_ids = [str(item["id"]) for item in candidates]
    disposition_candidate_ids = [str(item["candidate_id"]) for item in dispositions]
    discovery = {
        "detectors": list(DETECTORS),
        "meta_closure": meta_closure,
        "detections": detections,
        "resolutions": resolutions,
        "candidates": candidates,
        "dispositions": dispositions,
        "exclusions": exclusions,
        "projection_coverage": projection_coverage,
        "anomalies": {
            "missing": cast(int, projection_coverage["missing"])
            + len(set(detection_ids) - set(resolution_detection_ids)),
            "duplicate": (len(detection_ids) - len(set(detection_ids)))
            + (len(candidate_ids) - len(set(candidate_ids))),
            "stale": cast(int, projection_coverage["stale"])
            + len(set(resolution_detection_ids) - set(detection_ids)),
            "undecided": len(set(candidate_ids) - set(disposition_candidate_ids)),
            "multiply_disposed": len(disposition_candidate_ids)
            - len(set(disposition_candidate_ids)),
            "multiply_represented": projection_coverage["multiply_represented"],
        },
    }
    if any(cast(Mapping[str, int], discovery["anomalies"]).values()):
        raise ContractAtlasError(
            f"semantic atlas does not exactly own the machine projection: {discovery['anomalies']}"
        )
    semantic_identity = _semantic_identity(
        normalized_projection, policies, projection_integer_paths
    )
    coverage_identity = {
        "schema": COVERAGE_IDENTITY_SCHEMA,
        "discovery": discovery,
        "elements": elements,
    }
    trace_identity = {
        "schema": TRACE_IDENTITY_SCHEMA,
        "trace": normalized_trace,
        "unsafe_integer_paths": trace_integer_paths,
        "sources": source_index,
        "qualification_routes": {
            key: list(value) for key, value in sorted(QUALIFICATION_ROUTES.items())
        },
    }
    boundaries = cast(Mapping[str, object], normalized_projection["boundaries"])
    identities: dict[str, object] = {
        "boundary_canonical_sha256": canonical_sha256(boundaries),
        "boundary_legacy_sha256": hashlib.sha256(
            json.dumps(boundaries, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest(),
        "external_contract_sha256": canonical_sha256(normalized_projection["external_contract"]),
        "semantic_contract_sha256": canonical_sha256(semantic_identity),
        "coverage_sha256": canonical_sha256(coverage_identity),
        "trace_sha256": canonical_sha256(trace_identity),
    }
    files, documents, relationship = _render_atlas(
        elements,
        policies,
        normalized_projection,
        normalized_trace,
        identities,
        exclusions,
        discovery,
        component_descriptions,
    )
    representation_identity = {
        "schema": REPRESENTATION_IDENTITY_SCHEMA,
        "documents": documents,
        "relationships": relationship,
    }
    identities["atlas_representation_sha256"] = canonical_sha256(representation_identity)
    counts = _counts(elements, exclusions)
    counts["source_authorities"] = len(source_index)
    counts["atlas_documents"] = len(documents)
    root: dict[str, object] = {
        "schema": ROOT_SCHEMA,
        "series": normalized_projection["series"],
        "projection_schema": normalized_projection["schema"],
        "policies": policies,
        "projection": normalized_projection,
        "projection_unsafe_integer_paths": projection_integer_paths,
        "trace": normalized_trace,
        "trace_unsafe_integer_paths": trace_integer_paths,
        "sources": list(source_index.values()),
        "elements": elements,
        "discovery": discovery,
        "counts": counts,
        "identities": identities,
        "atlas": {
            "schema": ATLAS_SCHEMA,
            "directory": ATLAS_DIRECTORY,
            "root": f"{ATLAS_DIRECTORY}/index.md",
            "documents": documents,
            "relationships": relationship,
        },
    }
    atlas = ContractAtlas(root=root, files=files)
    validate_atlas(
        atlas,
        projection=projection,
        trace=trace,
        component_descriptions=component_descriptions,
    )
    return atlas


def _atlas_paths(root: Mapping[str, object]) -> set[str]:
    atlas = cast(Mapping[str, object], root["atlas"])
    directory = str(atlas["directory"])
    directory_path = PurePosixPath(directory)
    if directory_path.is_absolute() or directory_path.parts != (directory,):
        raise ContractAtlasError("atlas directory is not one safe relative path component")
    paths: set[str] = set()
    for document in cast(Sequence[Mapping[str, object]], atlas["documents"]):
        path = str(document["path"])
        candidate = PurePosixPath(path)
        if (
            candidate.is_absolute()
            or not candidate.parts
            or candidate.parts[0] != directory
            or any(part in {"", ".", ".."} for part in candidate.parts)
        ):
            raise ContractAtlasError(f"atlas document path is unsafe: {path}")
        if candidate.suffix != ".md" or path in paths:
            raise ContractAtlasError(f"atlas document path is invalid or duplicated: {path}")
        paths.add(path)
    return paths


def _reachable_atlas_documents(root_path: str, files: Mapping[str, bytes]) -> set[str]:
    """Return documents reachable through generated local Markdown links."""

    link_pattern = re.compile(r"\]\(([^)\s]+)\)")
    explicit_anchor_pattern = re.compile(r'<a id="([a-z0-9-]+)"></a>')

    def document_anchors(payload: bytes) -> set[str]:
        rendered = payload.decode()
        explicit_anchors = explicit_anchor_pattern.findall(rendered)
        if len(explicit_anchors) != len(set(explicit_anchors)):
            raise ContractAtlasError("atlas document repeats a stable local anchor")
        anchors = set(explicit_anchors)
        heading_counts: Counter[str] = Counter()
        for line in rendered.splitlines():
            match = re.match(r"^#{1,6}\s+(.+?)\s*$", line)
            if match is None:
                continue
            heading = re.sub(r"<[^>]+>", "", match.group(1))
            heading = re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", heading)
            heading = heading.replace("`", "").casefold()
            anchor = re.sub(r"[^\w\- ]", "", heading)
            anchor = re.sub(r"\s+", "-", anchor).strip("-")
            if not anchor:
                continue
            duplicate = heading_counts[anchor]
            heading_counts[anchor] += 1
            anchors.add(anchor if duplicate == 0 else f"{anchor}-{duplicate}")
        return anchors

    anchors_by_path = {path: document_anchors(payload) for path, payload in files.items()}
    graph: dict[str, set[str]] = {path: set() for path in files}
    for source, payload in files.items():
        for target in link_pattern.findall(payload.decode()):
            local_path, separator, fragment = target.partition("#")
            if local_path.startswith("/") or re.match(r"^[a-z][a-z0-9+.-]*:", local_path):
                continue
            resolved = (
                source
                if not local_path
                else posixpath.normpath(posixpath.join(posixpath.dirname(source), local_path))
            )
            if resolved not in files:
                raise ContractAtlasError(
                    f"atlas document has an unresolved local link: {source} -> {target}"
                )
            if separator and fragment not in anchors_by_path[resolved]:
                raise ContractAtlasError(
                    f"atlas document has an unresolved local anchor: {source} -> {target}"
                )
            graph[source].add(resolved)

    reached: set[str] = set()
    pending = [root_path]
    while pending:
        path = pending.pop()
        if path in reached:
            continue
        reached.add(path)
        pending.extend(graph[path] - reached)
    return reached


def validate_atlas(
    atlas: ContractAtlas,
    *,
    projection: Mapping[str, object] | None = None,
    trace: Mapping[str, object] | None = None,
    component_descriptions: Mapping[str, str] | None = None,
) -> None:
    """Recompute closure, ownership, roll-up, source, policy, and identity proofs."""

    root = atlas.root
    if root.get("schema") != ROOT_SCHEMA:
        raise ContractAtlasError(f"unexpected machine closure schema: {root.get('schema')}")
    paths = _atlas_paths(root)
    if paths != set(atlas.files):
        raise ContractAtlasError("machine-referenced atlas files differ from the bundle contents")
    descriptors = {
        str(item["path"]): item
        for item in cast(
            Sequence[Mapping[str, object]], cast(Mapping[str, object], root["atlas"])["documents"]
        )
    }
    for path, payload in atlas.files.items():
        descriptor = descriptors[path]
        if (
            descriptor["bytes"] != len(payload)
            or descriptor["sha256"] != hashlib.sha256(payload).hexdigest()
        ):
            raise ContractAtlasError(f"atlas document identity mismatch: {path}")
    elements = cast(Sequence[Mapping[str, object]], root["elements"])
    ids = [str(item["id"]) for item in elements]
    elements_by_id = {str(item["id"]): item for item in elements}
    if len(ids) != len(set(ids)):
        raise ContractAtlasError("contract element identities are not unique")
    unknown_interfaces = sorted(
        {str(item["interface"]) for item in elements} - set(INTERFACE_LABELS)
    )
    if unknown_interfaces:
        raise ContractAtlasError(f"contract elements use unknown interfaces: {unknown_interfaces}")
    projection_value = cast(Mapping[str, object], root["projection"])
    trace_value = cast(Mapping[str, object], root["trace"])
    _validate_process_protocol_units(elements, projection_value)
    _validate_python_units(elements, projection_value, trace_value)
    _validate_release_units(elements, projection_value)
    if any("family" in item for item in elements):
        raise ContractAtlasError("semantic-family metadata remains in the contract atlas")
    dossiers = [str(item["dossier"]) for item in elements]
    if len(dossiers) != len(set(dossiers)) or not set(dossiers) <= paths:
        raise ContractAtlasError("each contract element must own one unique atlas dossier")
    extent_decisions = {
        str(item["id"]): item
        for item in cast(
            Sequence[Mapping[str, object]],
            cast(
                Mapping[str, object],
                cast(Mapping[str, object], projection_value["external_contract"])["extents"],
            )["decisions"],
        )
    }
    policy_path = f"{ATLAS_DIRECTORY}/policies/index.md"
    source_evidence_path = f"{ATLAS_DIRECTORY}/evidence/sources.md"
    for item in elements:
        marker = f"<!-- contract-element: {item['id']} -->".encode()
        dossier = atlas.files[str(item["dossier"])]
        dossier_text = dossier.decode()
        if marker not in dossier:
            raise ContractAtlasError(
                f"atlas dossier does not identify its contract element: {item['id']}"
            )
        if "| Contract elements |" in dossier_text or "| Extent decisions |" in dossier_text:
            raise ContractAtlasError(f"atlas dossier repeats aggregate accounting: {item['id']}")
        if any(
            policy.encode() not in dossier for policy in cast(Sequence[str], item["policy_ids"])
        ):
            raise ContractAtlasError(
                f"atlas dossier does not expose every effective policy: {item['id']}"
            )
        for policy in cast(Sequence[str], item["policy_ids"]):
            link = _anchor_link(str(item["dossier"]), policy_path, _policy_anchor(policy))
            application_anchor = _policy_application_anchor(str(item["id"]), policy)
            if (
                f"]({link})" not in dossier_text
                or dossier_text.count(f'id="{application_anchor}"') != 1
            ):
                raise ContractAtlasError(
                    f"atlas dossier does not route an exact policy application: {item['id']}"
                )
        for route in cast(Sequence[str], item["qualification_routes"]):
            link = _anchor_link(
                str(item["dossier"]),
                source_evidence_path,
                _qualification_anchor(route),
            )
            if f"]({link})" not in dossier_text:
                raise ContractAtlasError(
                    f"atlas dossier does not route its qualification: {item['id']}"
                )
        for source_id in cast(Sequence[str], item["source_authority_ids"]):
            link = _anchor_link(
                str(item["dossier"]), source_evidence_path, _source_anchor(source_id)
            )
            if f"]({link})" not in dossier_text:
                raise ContractAtlasError(
                    f"atlas dossier does not route its executable source: {item['id']}"
                )
        extent_ids = cast(Sequence[str], item["extent_decision_ids"])
        subject_pointers = {
            str(extent_decisions[identity]["source_pointer"]) for identity in extent_ids
        }
        for subject_pointer in subject_pointers:
            anchor = _subject_anchor(subject_pointer)
            if dossier_text.count(f'id="{anchor}"') != 1:
                raise ContractAtlasError(
                    f"atlas dossier does not anchor an exact extent subject: {item['id']}"
                )
        if extent_ids:
            extent_section = dossier_text.split("### Progression, limits, and lifecycle\n", 1)[
                1
            ].split("\n## Governing policies", 1)[0]
            rendered_rows = [
                line
                for line in extent_section.splitlines()
                if line.startswith("| ")
                and not line.startswith("| Applies to ")
                and not line.startswith("|---")
            ]
            if len(rendered_rows) != len(extent_ids):
                raise ContractAtlasError(
                    f"atlas dossier does not render every extent decision once: {item['id']}"
                )
        for pointer in cast(Sequence[str], item["pointers"]):
            value = pointer_value(root["projection"], pointer)
            exact = (
                f"<!-- exact-contract-value: {canonical_sha256(value)} -->\n\n"
                f"```json\n{_pretty_json(value)}\n```"
            ).encode()
            if exact not in dossier:
                raise ContractAtlasError(
                    f"atlas dossier does not render its complete contract value: {item['id']}"
                )
        for referenced in _local_contract_references(
            str(item["authority"]),
            [
                pointer_value(root["projection"], pointer)
                for pointer in cast(Sequence[str], item["pointers"])
            ],
            elements_by_id,
        ):
            reference_link = (
                f"[{referenced['title']}]"
                f"({_relative_link(str(item['dossier']), str(referenced['dossier']))})"
            ).encode()
            if reference_link not in dossier:
                raise ContractAtlasError(
                    f"atlas dossier does not route its referenced contract: {item['id']}"
                )

    operation_qualification = cast(Mapping[str, object], trace_value["operation_qualification"])
    if operation_qualification.get("schema") != "riverhog-operation-qualification/v1":
        raise ContractAtlasError("operation qualification evidence has another schema")
    qualification_records = cast(
        Sequence[Mapping[str, object]],
        operation_qualification["records"],
    )
    qualified_element_records: list[tuple[tuple[str, str], Mapping[str, object]]] = []
    for item in elements:
        details_value = item.get("details")
        if item["interface"] != "http-operations" or not isinstance(details_value, Mapping):
            continue
        qualification_key_value = details_value.get("qualification_key")
        if not isinstance(qualification_key_value, Sequence) or isinstance(
            qualification_key_value, str
        ):
            continue
        if len(qualification_key_value) != 2:
            raise ContractAtlasError("HTTP qualification key does not have two fields")
        qualified_element_records.append(
            (
                (str(qualification_key_value[0]), str(qualification_key_value[1])),
                item,
            )
        )
    qualified_elements = dict(qualified_element_records)
    externally_qualified = {
        (str(record["application"]), str(record["operation_id"])): record
        for record in qualification_records
        if record.get("classification") != "service-internal"
    }
    qualification_keys = [
        (str(record["application"]), str(record["operation_id"]))
        for record in qualification_records
    ]
    if (
        len(qualified_element_records) != len(qualified_elements)
        or len(qualification_keys) != len(set(qualification_keys))
        or set(qualified_elements) != set(externally_qualified)
    ):
        raise ContractAtlasError(
            "external operation qualification does not resolve to exact HTTP contracts"
        )
    for qualification_key, item in qualified_elements.items():
        record = externally_qualified[qualification_key]
        details = cast(Mapping[str, object], item["details"])
        if (
            details.get("method") != record["method"]
            or details.get("path") != record["path"]
            or "operations:operation-matrix"
            not in cast(Sequence[str], item["source_authority_ids"])
            or _pretty_json(record) not in atlas.files[str(item["dossier"])].decode()
        ):
            raise ContractAtlasError(
                f"HTTP contract has stale qualification evidence: {qualification_key}"
            )
    discovery = cast(Mapping[str, object], root["discovery"])
    if discovery["anomalies"] != {
        "missing": 0,
        "duplicate": 0,
        "stale": 0,
        "undecided": 0,
        "multiply_disposed": 0,
        "multiply_represented": 0,
    }:
        raise ContractAtlasError("contract discovery contains unresolved anomalies")
    noncontractual_projection = _validate_authority_registry(
        elements, projection_value, trace_value
    )
    observed_projection_coverage = _projection_coverage(
        elements,
        cast(Mapping[str, object], root["policies"]),
        projection_value,
        noncontractual_projection,
    )
    if discovery["projection_coverage"] != observed_projection_coverage:
        raise ContractAtlasError("projection-to-atlas coverage is stale")
    detections = cast(Sequence[Mapping[str, object]], discovery["detections"])
    resolutions = cast(Sequence[Mapping[str, object]], discovery["resolutions"])
    candidates = cast(Sequence[Mapping[str, object]], discovery["candidates"])
    dispositions = cast(Sequence[Mapping[str, object]], discovery["dispositions"])
    protected_candidates = {
        str(item["candidate_id"]) for item in dispositions if item["disposition"] == "protected"
    }
    excluded_candidates = {
        str(item["candidate_id"]) for item in dispositions if item["disposition"] == "excluded"
    }
    candidate_ids = [str(item["id"]) for item in candidates]
    detection_ids = [str(item["id"]) for item in detections]
    resolved_detection_ids = [str(item["detection_id"]) for item in resolutions]
    resolved_candidate_ids = [str(item["candidate_id"]) for item in resolutions]
    candidate_elements = [str(item["element_id"]) for item in candidates if "element_id" in item]
    if (
        len(candidate_elements) != len(set(candidate_elements))
        or set(candidate_elements) != set(ids)
        or protected_candidates != {f"candidate:{identity}" for identity in ids}
        or excluded_candidates
        != {
            str(item["candidate_id"])
            for item in cast(Sequence[Mapping[str, object]], discovery["exclusions"])
        }
        or set(candidate_ids) != protected_candidates | excluded_candidates
        or len(detection_ids) != len(set(detection_ids))
        or len(resolved_detection_ids) != len(set(resolved_detection_ids))
        or set(detection_ids) != set(resolved_detection_ids)
        or set(candidate_ids) != set(resolved_candidate_ids)
    ):
        raise ContractAtlasError("discovery candidates and dispositions do not match the atlas")

    source_index = _source_index(trace_value)
    checked_sources = {
        str(item["id"]): dict(item)
        for item in cast(Sequence[Mapping[str, object]], root["sources"])
    }
    if checked_sources != source_index:
        raise ContractAtlasError("source authority index is stale")
    configuration_registry = cast(Mapping[str, object], trace_value["configuration_registry"])
    configuration_document_registry = cast(
        Mapping[str, object], trace_value["configuration_document_registry"]
    )
    configuration_coverage = cast(Mapping[str, object], configuration_registry["coverage"])
    if any(configuration_coverage.values()):
        raise ContractAtlasError("configuration registry contains unresolved ownership anomalies")
    _validate_staged_registry(
        cast(Mapping[str, object], trace_value["python_registry"]),
        label="Python package",
        require_one_resolution_per_detection=False,
    )
    _validate_staged_registry(
        cast(Mapping[str, object], trace_value["console_script_registry"]),
        label="console-script",
        require_one_resolution_per_detection=True,
    )
    _validate_staged_registry(
        configuration_document_registry,
        label="configuration-document",
        require_one_resolution_per_detection=True,
    )
    configuration_detection_ids = {
        str(item["id"])
        for item in cast(Sequence[Mapping[str, object]], configuration_registry["detections"])
    }
    configuration_resolved_detection_ids = {
        str(identity)
        for record in cast(Sequence[Mapping[str, object]], configuration_registry["records"])
        for identity in cast(Sequence[str], record["detection_ids"])
    } | {
        str(item["detection_id"])
        for item in cast(
            Sequence[Mapping[str, object]], configuration_registry["resolution_exceptions"]
        )
    }
    configuration_candidate_ids = {
        str(item["id"])
        for item in cast(Sequence[Mapping[str, object]], configuration_registry["candidates"])
    }
    configuration_disposition_ids = {
        str(item["candidate_id"])
        for item in cast(Sequence[Mapping[str, object]], configuration_registry["dispositions"])
    }
    configuration_resolution_ids = [
        str(item["detection_id"])
        for item in cast(Sequence[Mapping[str, object]], configuration_registry["resolutions"])
    ]
    if (
        configuration_detection_ids != configuration_resolved_detection_ids
        or len(configuration_resolution_ids) != len(set(configuration_resolution_ids))
        or set(configuration_resolution_ids) != configuration_detection_ids
        or configuration_candidate_ids != configuration_disposition_ids
    ):
        raise ContractAtlasError("configuration discovery stages are not exact")
    external = cast(Mapping[str, object], projection_value["external_contract"])
    projected_configuration = [
        *cast(Sequence[Mapping[str, object]], external["configuration_environment"]),
        *cast(Sequence[Mapping[str, object]], external["configuration_environment_patterns"]),
    ]
    registry_configuration = [
        *cast(Sequence[Mapping[str, object]], configuration_registry["records"]),
        *cast(Sequence[Mapping[str, object]], configuration_registry["patterns"]),
    ]
    if {str(record["id"]) for record in projected_configuration} != {
        str(record["id"]) for record in registry_configuration
    }:
        raise ContractAtlasError("configuration registry differs from external contract")
    configuration_element_ids = {
        str(value["id"])
        for item in elements
        if item["interface"] == "configuration-environment"
        for pointer in cast(Sequence[str], item["pointers"])
        if isinstance((value := pointer_value(projection_value, pointer)), Mapping)
        and isinstance(value.get("id"), str)
    }
    if configuration_element_ids != {str(record["id"]) for record in projected_configuration}:
        raise ContractAtlasError("configuration registry does not have exact atlas ownership")
    projected_configuration_documents = set(
        cast(Mapping[str, object], external["configuration_documents"])
    )
    discovered_configuration_documents = {
        str(item["id"])
        for item in cast(
            Sequence[Mapping[str, object]], configuration_document_registry["candidates"]
        )
    }
    if projected_configuration_documents != discovered_configuration_documents:
        raise ContractAtlasError("configuration-document registry differs from external contract")
    exclusions = cast(Sequence[Mapping[str, object]], discovery["exclusions"])
    policies = cast(Mapping[str, object], root["policies"])
    declared_policy_ids = {
        str(policy["id"])
        for values in policies.values()
        for policy in cast(Sequence[Mapping[str, object]], values)
    }
    used_policy_ids = {
        policy for item in elements for policy in cast(Sequence[str], item["policy_ids"])
    } | {str(item["policy_id"]) for item in exclusions}
    if not used_policy_ids <= declared_policy_ids:
        raise ContractAtlasError("contract element policy references are unresolved")
    policy_page = atlas.files[f"{ATLAS_DIRECTORY}/policies/index.md"].decode()
    for policy_id in declared_policy_ids:
        if policy_page.count(f'id="{_policy_anchor(policy_id)}"') != 1:
            raise ContractAtlasError(f"policy definition has no stable subject: {policy_id}")
    for item in [*elements, *exclusions]:
        if not set(cast(Sequence[str], item["source_authority_ids"])) <= set(source_index):
            raise ContractAtlasError(
                f"contract element source references are unresolved: {item['id']}"
            )

    root_path = str(cast(Mapping[str, object], root["atlas"])["root"])
    root_page = atlas.files[root_path].decode()
    ordered_headings = (
        "> **Audit question:**",
        "**Audit path:** Scope → Semantics → Evidence",
        "## Contract map",
        "## Contract-wide policies",
        "## Freeze evidence",
    )
    heading_offsets = [root_page.index(heading) for heading in ordered_headings]
    if heading_offsets != sorted(heading_offsets):
        raise ContractAtlasError(
            "atlas root must present the Scope → Semantics → Evidence audit path"
        )
    forbidden_root_terms = (
        "Guided contract map",
        "relationship map",
        "authority map",
        "Closure anomalies",
        "SHA-256",
        "Qualification route",
    )
    if any(term in root_page for term in forbidden_root_terms):
        raise ContractAtlasError("atlas root competes with its contract map or evidence layer")
    if (
        "contract elements" in root_page
        or re.search(r"— \d+ authorit(?:y|ies)", root_page) is not None
        or "Python extension:" in root_page
        or "Process protocol:" in root_page
    ):
        raise ContractAtlasError("atlas root repeats accounting or renders extensions as children")
    reachable_documents = _reachable_atlas_documents(root_path, atlas.files)
    if reachable_documents != set(atlas.files):
        unreachable = sorted(set(atlas.files) - reachable_documents)
        raise ContractAtlasError(
            f"atlas documents are not reachable from the front door: {unreachable}"
        )
    evidence_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/index.md"].decode()
    for name, count in cast(Mapping[str, object], discovery["anomalies"]).items():
        expected = "pass" if count == 0 else str(count)
        if f"| {_md(name.replace('_', ' '))} | {expected} |" not in evidence_page:
            raise ContractAtlasError(f"freeze evidence omits closure anomaly: {name}")
    identity_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/identities.md"].decode()
    for name, identity in cast(Mapping[str, object], root["identities"]).items():
        if identity_page.count(f'id="{_anchor_id("identity", str(name))}"') != 1:
            raise ContractAtlasError(f"identity evidence omits its stable subject: {name}")
        if name == "atlas_representation_sha256":
            if f"/identities/{name}" not in identity_page:
                raise ContractAtlasError("identity evidence omits its representation route")
        elif f"`{name}` | `{identity}` |" not in identity_page:
            raise ContractAtlasError(f"identity evidence omits independent identity: {name}")

    exclusion_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/exclusions.md"].decode()
    for item in exclusions:
        policy_record = next(
            candidate_policy
            for values in policies.values()
            for candidate_policy in cast(Sequence[Mapping[str, object]], values)
            if candidate_policy["id"] == item["policy_id"]
        )
        required_exclusion_values = [
            item["id"],
            item["kind"],
            item["installed_target"],
            item["boundary_pointer"],
            item["detector"],
            item["policy_id"],
            policy_record["meaning"],
            *cast(Sequence[str], item["source_authority_ids"]),
        ]
        if any(_md(value) not in exclusion_page for value in required_exclusion_values):
            raise ContractAtlasError(f"human exclusion inventory is incomplete: {item['id']}")
        if exclusion_page.count(f'id="{_anchor_id("exclusion", str(item["id"]))}"') != 1:
            raise ContractAtlasError(f"human exclusion has no stable subject: {item['id']}")

    source_evidence_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/sources.md"].decode()
    for route in cast(
        Mapping[str, object], cast(Mapping[str, object], root["counts"])["by_qualification_route"]
    ):
        if (
            f"`{route}`" not in source_evidence_page
            or source_evidence_page.count(f'id="{_qualification_anchor(str(route))}"') != 1
        ):
            raise ContractAtlasError(f"human evidence index omits route: {route}")
    for source_id, source in source_index.items():
        location = cast(Mapping[str, object], source.get("source", {}))
        rendered = str(location.get("path", location.get("module", source_id)))
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        if (
            f"`{source_id}`" not in source_evidence_page
            or f"`{rendered}{symbol}`" not in source_evidence_page
            or source_evidence_page.count(f'id="{_source_anchor(source_id)}"') != 1
        ):
            raise ContractAtlasError(f"human evidence index omits source: {source_id}")

    observed_counts = _counts(
        elements,
        exclusions,
    )
    checked_counts = cast(Mapping[str, object], root["counts"])
    for count_key, value in observed_counts.items():
        if checked_counts.get(count_key) != value:
            raise ContractAtlasError(f"root aggregate count is stale: {count_key}")
    if checked_counts.get("source_authorities") != len(source_index):
        raise ContractAtlasError("root source-authority count is stale")
    if checked_counts.get("atlas_documents") != len(descriptors):
        raise ContractAtlasError("root atlas-document count is stale")

    atlas_metadata = cast(Mapping[str, object], root["atlas"])
    relationship = cast(Mapping[str, object], atlas_metadata["relationships"])
    if relationship.get("schema") != RELATIONSHIP_SCHEMA:
        raise ContractAtlasError("atlas relationship navigation has an unexpected schema")
    if component_descriptions is not None:
        expected_relationship = _relationship_model(
            projection_value, trace_value, elements, component_descriptions
        )
        if relationship != expected_relationship:
            raise ContractAtlasError("atlas relationship navigation is stale")
    relationship_nodes = cast(Sequence[Mapping[str, object]], relationship["nodes"])
    relationship_edges = cast(Sequence[Mapping[str, object]], relationship["edges"])
    relationship_nodes_by_id = {str(item["id"]): item for item in relationship_nodes}
    if any(path.startswith(f"{ATLAS_DIRECTORY}/relationships/") for path in atlas.files):
        raise ContractAtlasError("relationship evidence must not become a second human map")
    contract_map = cast(Mapping[str, object], relationship["contract_map"])
    if contract_map.get("schema") != CONTRACT_MAP_SCHEMA:
        raise ContractAtlasError("human contract map has an unexpected schema")
    contract_map_nodes = cast(Sequence[Mapping[str, object]], contract_map["nodes"])
    contract_map_ids = [str(item["id"]) for item in contract_map_nodes]
    if len(contract_map_ids) != len(set(contract_map_ids)):
        raise ContractAtlasError("human contract map repeats a semantic node")
    if any(
        node.get("parent") is not None and node.get("parent") not in contract_map_ids
        for node in contract_map_nodes
    ):
        raise ContractAtlasError("human contract map contains an unresolved parent")
    mapped_authorities = [
        str(item["authority"])
        for node in contract_map_nodes
        for item in cast(Sequence[Mapping[str, object]], node["authorities"])
    ]
    exact_authorities = sorted({str(item["authority"]) for item in elements})
    if sorted(mapped_authorities) != exact_authorities or len(mapped_authorities) != len(
        set(mapped_authorities)
    ):
        raise ContractAtlasError(
            "human contract map does not own every exact authority exactly once"
        )
    element_counts_by_authority = Counter(str(item["authority"]) for item in elements)
    interface_counts_by_authority = Counter(
        (str(item["authority"]), str(item["interface"])) for item in elements
    )
    for node in contract_map_nodes:
        for record in cast(Sequence[Mapping[str, object]], node["authorities"]):
            authority = str(record["authority"])
            expected_interfaces = [
                {
                    "id": interface,
                    "label": _interface_label(interface),
                    "contract_elements": count,
                }
                for (candidate, interface), count in sorted(
                    interface_counts_by_authority.items(),
                    key=lambda item: (item[0][0], _interface_sort_key(item[0][1])),
                )
                if candidate == authority
            ]
            if (
                record["contract_elements"] != element_counts_by_authority[authority]
                or record.get("interfaces") != expected_interfaces
                or not str(record.get("purpose", "")).strip()
            ):
                raise ContractAtlasError(
                    f"human contract-map authority projection is stale: {authority}"
                )
        if "path" in node:
            path = f"{ATLAS_DIRECTORY}/{str(node['path']).split('#', 1)[0]}"
            if path not in reachable_documents:
                raise ContractAtlasError(f"human contract-map branch is unreachable: {node['id']}")
    authority_inventory_path = f"{ATLAS_DIRECTORY}/evidence/authorities.md"
    authority_inventory_page = atlas.files[authority_inventory_path].decode()
    for authority in exact_authorities:
        authority_path = _authority_index_path(authority)
        root_link = _relative_link(root_path, authority_path)
        inventory_link = _relative_link(authority_inventory_path, authority_path)
        if root_page.count(f"]({root_link})") != 1:
            raise ContractAtlasError(f"human contract map omits exact authority: {authority}")
        if f"]({inventory_link})" not in authority_inventory_page:
            raise ContractAtlasError(f"authority evidence omits exact authority: {authority}")
        for interface in sorted(
            {str(item["interface"]) for item in elements if item["authority"] == authority}
        ):
            interface_path = _interface_index_path(authority, interface)
            interface_link = _relative_link(root_path, interface_path)
            if root_page.count(f"]({interface_link})") != 1:
                raise ContractAtlasError(
                    f"human contract map omits direct interface navigation: "
                    f"{authority}: {interface}"
                )
            interface_page = atlas.files[interface_path].decode()
            if any(
                value in interface_page
                for value in (
                    "Contract elements:",
                    "Extent decisions:",
                    "| Policy | Count |",
                    "| Dossier | Extent decisions |",
                )
            ):
                raise ContractAtlasError(
                    f"human interface repeats aggregate accounting: {authority}: {interface}"
                )
            interface_elements = [
                item
                for item in elements
                if item["authority"] == authority and item["interface"] == interface
            ]
            for item in interface_elements:
                dossier_link = _relative_link(interface_path, str(item["dossier"]))
                if interface_page.count(f"]({dossier_link})") != 1:
                    raise ContractAtlasError(
                        f"human interface omits semantic dossier: {item['id']}"
                    )
    authority_registry = cast(Mapping[str, object], trace_value["authority_registry"])
    for item in cast(Sequence[Mapping[str, object]], authority_registry["declared_authorities"]):
        if any(_md(item[key]) not in authority_inventory_page for key in ("id", "meaning")):
            raise ContractAtlasError(
                f"authority evidence omits aggregate declaration: {item['id']}"
            )
    for item in cast(
        Sequence[Mapping[str, object]], authority_registry["noncontractual_projection"]
    ):
        authority_required = [
            item["id"],
            item["reason"],
            *cast(Sequence[str], item["pointers"]),
        ]
        if any(_md(value) not in authority_inventory_page for value in authority_required):
            raise ContractAtlasError(
                f"authority evidence omits non-contractual projection: {item['id']}"
            )
    relationship_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/relationships.md"].decode()
    for node in relationship_nodes:
        required = (node["id"], node["kind"], node["name"], node["description"])
        if (
            any(_md(value) not in relationship_page for value in required)
            or relationship_page.count(f'id="{_relationship_node_anchor(str(node["id"]))}"') != 1
        ):
            raise ContractAtlasError(f"relationship evidence omits node: {node['id']}")
        if node["kind"] in {"extension-point", "process-protocol"}:
            if node.get("contract_elements") != 0:
                raise ContractAtlasError(
                    f"extension relationship duplicates semantic accounting: {node['id']}"
                )
            semantic_interfaces = cast(
                Sequence[Mapping[str, object]], node.get("semantic_interfaces", ())
            )
            if not semantic_interfaces:
                raise ContractAtlasError(
                    f"extension relationship has no exact semantic interface: {node['id']}"
                )
            for semantic_interface_record in semantic_interfaces:
                semantic_interface_key = (
                    str(semantic_interface_record["authority"]),
                    str(semantic_interface_record["interface"]),
                )
                if interface_counts_by_authority[
                    semantic_interface_key
                ] != semantic_interface_record[
                    "contract_elements"
                ] or semantic_interface_record.get("label") != _interface_label(
                    semantic_interface_key[1]
                ):
                    raise ContractAtlasError(
                        f"extension relationship has stale semantic navigation: {node['id']}"
                    )
    for edge in relationship_edges:
        detail = edge.get("scope", edge.get("binding", ""))
        required = (
            relationship_nodes_by_id[str(edge["source"])]["name"],
            edge["type"],
            relationship_nodes_by_id[str(edge["target"])]["name"],
            detail,
        )
        if (
            any(_md(value) not in relationship_page for value in required)
            or relationship_page.count(f'id="{_relationship_edge_anchor(edge)}"') != 1
        ):
            raise ContractAtlasError(f"relationship evidence omits edge: {edge}")
    extension_nodes = [
        node
        for node in relationship_nodes
        if node["kind"] in {"extension-point", "process-protocol"}
    ]
    expected_extension_paths = {_extension_context_path(extension) for extension in extension_nodes}
    actual_extension_paths = {
        path for path in atlas.files if path.startswith(f"{ATLAS_DIRECTORY}/extensions/")
    }
    if actual_extension_paths != expected_extension_paths:
        raise ContractAtlasError("extension context pages are missing, duplicated, or stale")
    root_lines = root_page.splitlines()
    for extension in extension_nodes:
        extension_id = str(extension["id"])
        extension_path = _extension_context_path(extension)
        extension_page = atlas.files[extension_path].decode()
        extension_link = _relative_link(root_path, extension_path)
        descriptor = descriptors[extension_path]
        if (
            descriptor.get("kind") != "extension-context"
            or descriptor.get("extension_id") != extension_id
            or descriptor.get("counts") != {}
        ):
            raise ContractAtlasError(
                f"extension context descriptor is not relationship-only: {extension_id}"
            )
        if any(
            value in extension_page
            for value in (
                "Contract elements",
                "Extent decisions",
                "## Governing policies",
                "## Semantic dossiers",
            )
        ):
            raise ContractAtlasError(
                f"extension context duplicates semantic accounting: {extension_id}"
            )
        node_link = _anchor_link(
            extension_path,
            f"{ATLAS_DIRECTORY}/evidence/relationships.md",
            _relationship_node_anchor(extension_id),
        )
        if (
            f"- Identity: `{extension_id}`" not in extension_page
            or _md(extension["description"]) not in extension_page
            or f"]({node_link})" not in extension_page
            or "## Checked-in nonnormative implementations" not in extension_page
        ):
            raise ContractAtlasError(f"extension context is incomplete: {extension_id}")
        owner = str(extension["owner"])
        owner_interfaces = [
            item
            for item in cast(Sequence[Mapping[str, object]], extension["semantic_interfaces"])
            if item["authority"] == owner
        ]
        if len(owner_interfaces) != 1:
            raise ContractAtlasError(
                f"extension context does not have one owning interface: {extension_id}"
            )
        owner_interface = owner_interfaces[0]
        owner_interface_path = _interface_index_path(owner, str(owner_interface["interface"]))
        owner_interface_link = _relative_link(root_path, owner_interface_path)
        owner_lines = [line for line in root_lines if f"]({owner_interface_link})" in line]
        if (
            len(owner_lines) != 1
            or f"({owner_interface['contract_elements']})" not in owner_lines[0]
            or f"]({extension_link})" not in owner_lines[0]
            or owner_lines[0].index(f"({owner_interface['contract_elements']})")
            > owner_lines[0].index(f"]({extension_link})")
        ):
            raise ContractAtlasError(
                f"extension owner annotation is not inline after its interface count: "
                f"{extension_id}"
            )
        for semantic_interface_record in cast(
            Sequence[Mapping[str, object]], extension["semantic_interfaces"]
        ):
            semantic_path = _interface_index_path(
                str(semantic_interface_record["authority"]),
                str(semantic_interface_record["interface"]),
            )
            if f"]({_relative_link(extension_path, semantic_path)})" not in extension_page:
                raise ContractAtlasError(
                    f"extension context omits an exact semantic interface: {extension_id}"
                )
        for edge in [edge for edge in relationship_edges if edge["target"] == extension_id]:
            edge_link = _anchor_link(
                extension_path,
                f"{ATLAS_DIRECTORY}/evidence/relationships.md",
                _relationship_edge_anchor(edge),
            )
            if f"]({edge_link})" not in extension_page:
                raise ContractAtlasError(
                    f"extension context omits exact relationship evidence: {extension_id}"
                )
        implementation_edges = [
            edge
            for edge in relationship_edges
            if edge["target"] == extension_id
            and edge["type"] in {"implements-extension-point", "implements-protocol"}
        ]
        for edge in implementation_edges:
            provider = relationship_nodes_by_id[str(edge["source"])]
            edge_link = _anchor_link(
                extension_path,
                f"{ATLAS_DIRECTORY}/evidence/relationships.md",
                _relationship_edge_anchor(edge),
            )
            if (
                _md(provider["name"]) not in extension_page
                or _md(provider["description"]) not in extension_page
                or f"]({edge_link})" not in extension_page
            ):
                raise ContractAtlasError(
                    f"extension context omits a checked-in implementation: {extension_id}"
                )
            provider_name = str(provider["name"])
            if provider_name in exact_authorities:
                provider_authority_link = _relative_link(
                    root_path, _authority_index_path(provider_name)
                )
                provider_lines = [
                    line for line in root_lines if f"]({provider_authority_link}) —" in line
                ]
                if len(provider_lines) != 1 or f"]({extension_link})" not in provider_lines[0]:
                    raise ContractAtlasError(
                        f"extension provider annotation is not authority metadata: "
                        f"{provider_name}: {extension_id}"
                    )
                provider_interface_links = {
                    _relative_link(
                        root_path,
                        _interface_index_path(provider_name, str(item["interface"])),
                    )
                    for item in elements
                    if item["authority"] == provider_name
                }
                if any(
                    f"]({interface_link})" in line and f"]({extension_link})" in line
                    for line in root_lines
                    for interface_link in provider_interface_links
                ):
                    raise ContractAtlasError(
                        f"extension provider is misrepresented as interface ownership: "
                        f"{provider_name}: {extension_id}"
                    )
    if "Maintainer-selected nonnormative references" not in root_page:
        raise ContractAtlasError("atlas front door does not identify references as nonnormative")
    if any("/families/" in path for path in atlas.files) or "Semantic families" in root_page:
        raise ContractAtlasError("semantic-family navigation remains in the human atlas")
    for path, descriptor in descriptors.items():
        kind = descriptor["kind"]
        if kind == "root-index":
            expected_counts: Mapping[str, object] = _counts(elements, exclusions)
        elif kind == "policy-index":
            expected_counts = {
                "policies": sum(len(cast(Sequence[object], value)) for value in policies.values())
            }
        elif kind == "exclusion-index":
            expected_counts = {"excluded_candidates": len(exclusions)}
        elif kind == "evidence-index":
            expected_counts = {
                "contract_elements": checked_counts["contract_elements"],
                "extent_decisions": checked_counts["extent_decisions"],
                "excluded_candidates": len(exclusions),
                "source_authorities": len(source_index),
            }
        elif kind == "evidence-authority-inventory":
            authority_registry = cast(Mapping[str, object], trace_value["authority_registry"])
            expected_counts = {
                "authorities": len(exact_authorities),
                "declared_aggregate_authorities": len(
                    cast(
                        Sequence[Mapping[str, object]],
                        authority_registry["declared_authorities"],
                    )
                ),
                "noncontractual_projection_records": len(
                    cast(
                        Sequence[Mapping[str, object]],
                        authority_registry["noncontractual_projection"],
                    )
                ),
            }
        elif kind == "evidence-configuration-inventory":
            configuration_counts = cast(Mapping[str, object], configuration_registry["counts"])
            expected_counts = {
                "configuration_contracts": configuration_counts["contracts"],
                "configuration_patterns": configuration_counts["patterns"],
            }
        elif kind == "evidence-source-inventory":
            expected_counts = {
                "source_authorities": len(source_index),
                "qualification_routes": len(
                    cast(Mapping[str, object], checked_counts["by_qualification_route"])
                ),
            }
        elif kind == "evidence-relationship-inventory":
            expected_counts = {
                "nodes": len(relationship_nodes),
                "edges": len(relationship_edges),
            }
        elif kind == "evidence-identity-inventory":
            expected_counts = {
                "identity_domains": len(cast(Mapping[str, object], root["identities"]))
            }
        elif kind == "semantic-surface":
            map_node_ids = cast(Sequence[str], descriptor["map_node_ids"])
            selected_nodes = [
                node for node in contract_map_nodes if str(node["id"]) in map_node_ids
            ]
            expected_counts = {
                "authorities": sum(
                    len(cast(Sequence[object], node["authorities"])) for node in selected_nodes
                ),
                "contract_elements": sum(
                    cast(int, item["contract_elements"])
                    for node in selected_nodes
                    for item in cast(Sequence[Mapping[str, object]], node["authorities"])
                ),
            }
        elif kind == "extension-context":
            extension_id = str(descriptor["extension_id"])
            if extension_id not in {
                str(node["id"])
                for node in relationship_nodes
                if node["kind"] in {"extension-point", "process-protocol"}
            }:
                raise ContractAtlasError(
                    f"extension context names an unknown relationship: {extension_id}"
                )
            expected_counts = {}
        elif kind == "dossier":
            expected_counts = _counts([elements_by_id[str(descriptor["element_id"])]])
        else:
            parts = path.split("/")
            authority_slug = parts[2]
            subset = [
                item
                for item in elements
                if _slug(str(item["authority"]), limit=72) == authority_slug
            ]
            if kind == "interface-index":
                interface_slug = parts[3]
                subset = [
                    item
                    for item in subset
                    if _slug(str(item["interface"]), limit=48) == interface_slug
                ]
            expected_counts = _counts(subset)
        if descriptor["counts"] != expected_counts:
            raise ContractAtlasError(f"atlas roll-up count is stale: {path}")

    semantic_identity = _semantic_identity(
        projection_value,
        policies,
        cast(Sequence[str], root["projection_unsafe_integer_paths"]),
    )
    coverage_identity = {
        "schema": COVERAGE_IDENTITY_SCHEMA,
        "discovery": discovery,
        "elements": elements,
    }
    trace_identity = {
        "schema": TRACE_IDENTITY_SCHEMA,
        "trace": trace_value,
        "unsafe_integer_paths": root["trace_unsafe_integer_paths"],
        "sources": source_index,
        "qualification_routes": {
            key: list(value) for key, value in sorted(QUALIFICATION_ROUTES.items())
        },
    }
    representation_identity = {
        "schema": REPRESENTATION_IDENTITY_SCHEMA,
        "documents": atlas_metadata["documents"],
        "relationships": relationship,
    }
    boundaries = cast(Mapping[str, object], projection_value["boundaries"])
    observed_identities = {
        "boundary_canonical_sha256": canonical_sha256(boundaries),
        "boundary_legacy_sha256": hashlib.sha256(
            json.dumps(boundaries, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest(),
        "external_contract_sha256": canonical_sha256(projection_value["external_contract"]),
        "semantic_contract_sha256": canonical_sha256(semantic_identity),
        "coverage_sha256": canonical_sha256(coverage_identity),
        "trace_sha256": canonical_sha256(trace_identity),
        "atlas_representation_sha256": canonical_sha256(representation_identity),
    }
    if root["identities"] != observed_identities:
        raise ContractAtlasError("machine closure identities are stale")
    if projection is not None and reassemble_projection(atlas) != json.loads(
        json.dumps(projection)
    ):
        raise ContractAtlasError("machine closure does not preserve the exact logical projection")
    if trace is not None and reassemble_trace(atlas) != json.loads(json.dumps(trace)):
        raise ContractAtlasError("machine closure does not preserve the exact source/proof trace")


def load_atlas(path: Path) -> ContractAtlas:
    try:
        root = cast(dict[str, object], json.loads(path.read_bytes()))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractAtlasError(f"machine closure is unavailable: {path}") from exc
    if root.get("schema") != ROOT_SCHEMA:
        raise ContractAtlasError(f"unexpected machine closure schema: {root.get('schema')}")
    files: dict[str, bytes] = {}
    for relative in _atlas_paths(root):
        try:
            files[relative] = (path.parent / relative).read_bytes()
        except OSError as exc:
            raise ContractAtlasError(f"atlas document is unavailable: {relative}") from exc
    directory = path.parent / str(cast(Mapping[str, object], root["atlas"])["directory"])
    actual = (
        {
            candidate.relative_to(path.parent).as_posix()
            for candidate in directory.rglob("*")
            if candidate.is_file()
        }
        if directory.is_dir()
        else set()
    )
    if actual != set(files):
        raise ContractAtlasError("atlas directory contains stale or unreferenced files")
    atlas = ContractAtlas(root=root, files=files)
    validate_atlas(atlas)
    return atlas


def reassemble_projection(atlas: ContractAtlas) -> dict[str, object]:
    return cast(
        dict[str, object],
        _decode_unsafe_integers(
            atlas.root["projection"],
            cast(Sequence[str], atlas.root["projection_unsafe_integer_paths"]),
        ),
    )


def reassemble_trace(atlas: ContractAtlas) -> dict[str, object]:
    return cast(
        dict[str, object],
        _decode_unsafe_integers(
            atlas.root["trace"],
            cast(Sequence[str], atlas.root["trace_unsafe_integer_paths"]),
        ),
    )


def checked_file_set(path: Path) -> set[Path]:
    atlas = load_atlas(path)
    return {path, *(path.parent / relative for relative in atlas.files)}

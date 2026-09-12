#!/usr/bin/env python3
"""Build and validate the Riverhog v1 machine closure and human audit atlas."""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
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
FAMILY_INDEX_MINIMUM_ELEMENTS = 24
RELATIONSHIP_SCHEMA = "riverhog-contract-human-relationships/v1"
CONTRACT_MAP_SCHEMA = "riverhog-contract-human-map/v1"

EXCLUSION_POLICIES: tuple[dict[str, str], ...] = (
    {
        "id": "exclusion/process-launcher-not-cli/v1",
        "meaning": (
            "The installed entry point starts a separately inventoried process protocol and "
            "does not expose an independently maintained human or JSON CLI."
        ),
        "scope": "Installed service, adapter, observer, target, sampler, and effect launchers.",
    },
)

DETECTORS: tuple[dict[str, str], ...] = (
    {"id": "boundary", "authority": "validated release boundary projection"},
    {"id": "cli-tree", "authority": "installed parser tree"},
    {"id": "configuration-document", "authority": "validated configuration schema"},
    {"id": "configuration-environment", "authority": "executable environment binding"},
    {"id": "durable-state", "authority": "checked current state baseline"},
    {"id": "extent", "authority": "exhaustive extent classifier"},
    {"id": "http-openapi", "authority": "running ASGI app"},
    {"id": "operation-matrix", "authority": "executable operation parity matrix"},
    {"id": "protocol-schema", "authority": "published or generated protocol schema"},
    {"id": "python-export", "authority": "published reusable-library export"},
    {"id": "release-metadata", "authority": "validated release contract"},
)

QUALIFICATION_ROUTES: dict[str, tuple[str, ...]] = {
    "boundary": ("make release-check", "make build"),
    "cli": ("make dist-smoke", "make operation-qualification"),
    "configuration": ("make unit", "make compose-smoke"),
    "configuration-environment": ("make unit", "make compose-smoke"),
    "durable-state": ("make release-check", "make database-qualification"),
    "extent": ("make contract-freeze", "make operation-qualification"),
    "http": ("make operation-qualification", "make compose-smoke"),
    "operation": ("make operation-qualification",),
    "protocol": ("make dist-smoke", "make build"),
    "python": ("make dist-smoke", "make build"),
    "release": ("make release-check", "make build"),
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
            "configuration-environment:inventory": {
                "id": "configuration-environment:inventory",
                "source": {
                    "path": "scripts/contract_freeze.py",
                    "symbol": "_environment_inventory",
                },
            },
        }
    )
    return dict(sorted(sources.items()))


def _policy_registry(projection: Mapping[str, object]) -> dict[str, object]:
    external = cast(Mapping[str, object], projection["external_contract"])
    release = cast(Mapping[str, object], external["release"])
    compatibility = cast(Mapping[str, object], release["compatibility"])
    extents = cast(Mapping[str, object], external["extents"])
    return {
        "boundary": [
            {
                "id": "boundary/frozen-authority/v1",
                "meaning": "The authority and extension boundary is maintainer-frozen for v1.",
                "applies_to": ["/boundaries"],
            }
        ],
        "compatibility": [
            {
                "id": f"compatibility/{key.replace('_', '-')}/v1",
                "meaning": value,
                "applies_to": [f"/external_contract/release/compatibility/{key}"],
            }
            for key, value in sorted(compatibility.items())
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
        "boundary": "boundary/frozen-authority/v1",
        "cli": "compatibility/cli/v1",
        "configuration": "compatibility/configuration/v1",
        "configuration-environment": "compatibility/configuration/v1",
        "durable-state": "compatibility/components/v1",
        "extent": "extent-principle/logical-totals/v1",
        "http": "compatibility/http-api/v1",
        "operation": "compatibility/components/v1",
        "protocol": "compatibility/components/v1",
        "python": "compatibility/python-api/v1",
        "release": "compatibility/components/v1",
    }
    return [mapping[interface]]


def _element_id(authority: str, interface: str, title: str, pointers: Sequence[str]) -> str:
    digest = canonical_sha256([authority, interface, title, list(pointers)])[:10]
    return f"{interface}:{_slug(authority, limit=42)}:{_slug(title, limit=52)}:{digest}"


def _family_from_path(path: str) -> str:
    parts = [part for part in path.split("/") if part and not part.startswith("{")]
    return (
        parts[1] if parts and parts[0] == "v1" and len(parts) > 1 else parts[0] if parts else "root"
    )


def _add_element(
    elements: list[dict[str, object]],
    *,
    authority: str,
    interface: str,
    family: str,
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
        "family": family,
        "title": title,
        "pointers": normalized_pointers,
        "detector": detector,
        "disposition": "contractual",
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


def _boundary_elements(projection: Mapping[str, object]) -> list[dict[str, object]]:
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    elements: list[dict[str, object]] = []
    _add_element(
        elements,
        authority="repository",
        interface="boundary",
        family="identity",
        title="Contract projection identity",
        pointers=["/schema", "/series"],
        detector="boundary",
        source_ids=["release:release.toml"],
    )
    _add_element(
        elements,
        authority="repository",
        interface="boundary",
        family="references",
        title="Reference component policy",
        pointers=["/boundaries/reference_policy"],
        detector="boundary",
        source_ids=["release:release.toml"],
    )
    for index, component in enumerate(
        cast(Sequence[Mapping[str, object]], boundaries["components"])
    ):
        name = str(component["distribution"])
        _add_element(
            elements,
            authority=name,
            interface="boundary",
            family="components",
            title=f"{name} component boundary",
            pointers=[f"/boundaries/components/{index}"],
            detector="boundary",
            source_ids=["release:release.toml"],
        )
    for family, _values in sorted(cast(Mapping[str, object], boundaries["runtime_images"]).items()):
        _add_element(
            elements,
            authority="repository",
            interface="boundary",
            family="runtime-images",
            title=f"{family.replace('_', ' ').title()} runtime images",
            pointers=[f"/boundaries/runtime_images/{_escape_pointer(family)}"],
            detector="boundary",
            source_ids=["release:release.toml"],
        )
    for section, family in (
        ("entry_point_extensions", "entry-point-extensions"),
        ("process_extensions", "process-extensions"),
    ):
        for index, item in enumerate(cast(Sequence[Mapping[str, object]], boundaries[section])):
            authority = str(item.get("owner", item.get("contract_owner", "repository")))
            name = str(item.get("group", item.get("name", f"{section}-{index}")))
            _add_element(
                elements,
                authority=authority,
                interface="boundary",
                family=family,
                title=name,
                pointers=[f"/boundaries/{section}/{index}"],
                detector="boundary",
                source_ids=["release:release.toml"],
            )
    return elements


def _walk_cli(
    elements: list[dict[str, object]],
    authority: str,
    node: Mapping[str, object],
    pointer: str,
    command_path: tuple[str, ...],
) -> None:
    name = str(node.get("name") or (command_path[-1] if command_path else authority))
    current_path = (
        (*command_path, name) if not command_path or command_path[-1] != name else command_path
    )
    pointers = [f"{pointer}/{key}" for key in ("name", "parameters") if key in node]
    _add_element(
        elements,
        authority=authority,
        interface="cli",
        family=current_path[1] if len(current_path) > 1 else "root",
        title=" ".join(current_path),
        pointers=pointers,
        detector="cli-tree",
        source_ids=[f"cli:{authority}"],
        details={"command_path": list(current_path)},
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


def _external_elements(
    projection: Mapping[str, object], trace: Mapping[str, object]
) -> list[dict[str, object]]:
    external = cast(Mapping[str, object], projection["external_contract"])
    elements: list[dict[str, object]] = []

    for name, value in sorted(cast(Mapping[str, object], external["release"]).items()):
        if name == "compatibility" and isinstance(value, Mapping):
            for policy_name in sorted(value):
                element = _add_element(
                    elements,
                    authority="release",
                    interface="release",
                    family="compatibility",
                    title=f"Compatibility: {policy_name.replace('_', ' ')}",
                    pointers=[
                        f"/external_contract/release/compatibility/{_escape_pointer(policy_name)}"
                    ],
                    detector="release-metadata",
                    source_ids=["release:release.toml"],
                )
                element["policy_ids"] = [f"compatibility/{policy_name.replace('_', '-')}/v1"]
            continue
        _add_element(
            elements,
            authority="release",
            interface="release",
            family="release-contract",
            title=f"Release {name.replace('_', ' ')}",
            pointers=[f"/external_contract/release/{_escape_pointer(name)}"],
            detector="release-metadata",
            source_ids=["release:release.toml"],
        )

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
            interface="http",
            family="service",
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
                    interface="http",
                    family=_family_from_path(path),
                    title=title,
                    pointers=[pointer],
                    detector="http-openapi",
                    source_ids=[f"openapi:{authority}"],
                    details=details,
                )
        components = cast(Mapping[str, object], document.get("components", {}))
        for component_kind, values in sorted(components.items()):
            if isinstance(values, Mapping):
                for name in sorted(values):
                    _add_element(
                        elements,
                        authority=authority,
                        interface="http",
                        family=component_kind,
                        title=f"{component_kind}: {name}",
                        pointers=[
                            f"/external_contract/http_openapi/{_escape_pointer(authority)}/"
                            f"components/{_escape_pointer(component_kind)}/{_escape_pointer(name)}"
                        ],
                        detector="http-openapi",
                        source_ids=[f"openapi:{authority}"],
                    )
            else:
                _add_element(
                    elements,
                    authority=authority,
                    interface="http",
                    family="components",
                    title=f"HTTP component {component_kind}",
                    pointers=[
                        f"/external_contract/http_openapi/{_escape_pointer(authority)}/"
                        f"components/{_escape_pointer(component_kind)}"
                    ],
                    detector="http-openapi",
                    source_ids=[f"openapi:{authority}"],
                )

    for index, operation in enumerate(cast(Sequence[Mapping[str, object]], external["operations"])):
        authority = str(operation["application"])
        operation_id = str(operation["operation_id"])
        element = _add_element(
            elements,
            authority=authority,
            interface="operation",
            family=_family_from_path(str(operation.get("path", ""))),
            title=f"Operation parity: {operation_id}",
            pointers=[f"/external_contract/operations/{index}"],
            detector="operation-matrix",
            source_ids=["operations:operation-matrix"],
            details={"operation_id": operation_id},
        )
        element["policy_ids"] = sorted(
            {"compatibility/components/v1", "compatibility/cli/v1", "compatibility/http-api/v1"}
        )

    for authority, node in sorted(
        cast(Mapping[str, Mapping[str, object]], external["cli"]).items()
    ):
        _walk_cli(
            elements,
            authority,
            node,
            f"/external_contract/cli/{_escape_pointer(authority)}",
            (),
        )

    for section in ("configuration_environment", "configuration_environment_patterns"):
        for index, item in enumerate(cast(Sequence[Mapping[str, object]], external[section])):
            name = str(item.get("name", f"{section}-{index}"))
            _add_element(
                elements,
                authority="configuration",
                interface="configuration-environment",
                family="patterns" if section.endswith("patterns") else "variables",
                title=name,
                pointers=[f"/external_contract/{section}/{index}"],
                detector="configuration-environment",
                source_ids=[
                    "configuration-environment:inventory"
                    if section.endswith("patterns")
                    else f"configuration-environment:{name}"
                ],
            )

    for authority in sorted(cast(Mapping[str, object], external["configuration_documents"])):
        _add_element(
            elements,
            authority=authority,
            interface="configuration",
            family="documents",
            title=f"{authority} configuration",
            pointers=[f"/external_contract/configuration_documents/{_escape_pointer(authority)}"],
            detector="configuration-document",
            source_ids=[f"configuration:{authority}"],
        )

    sources = _source_index(trace)
    for schema_authority, document in sorted(
        cast(Mapping[str, Mapping[str, object]], external["protocol_schemas"]).items()
    ):
        authority = _protocol_owner(schema_authority, sources)
        base = f"/external_contract/protocol_schemas/{_escape_pointer(schema_authority)}"
        schemas = document.get("schemas")
        if schema_authority.startswith("generated:") and isinstance(schemas, Mapping):
            metadata = [f"{base}/{_escape_pointer(key)}" for key in document if key != "schemas"]
            _add_element(
                elements,
                authority=authority,
                interface="protocol",
                family="protocol",
                title=f"{schema_authority} protocol",
                pointers=metadata,
                detector="protocol-schema",
                source_ids=[f"protocol:{schema_authority}"],
            )
            for name in sorted(schemas):
                _add_element(
                    elements,
                    authority=authority,
                    interface="protocol",
                    family="schemas",
                    title=f"{schema_authority}: {name}",
                    pointers=[f"{base}/schemas/{_escape_pointer(name)}"],
                    detector="protocol-schema",
                    source_ids=[f"protocol:{schema_authority}"],
                )
        else:
            _add_element(
                elements,
                authority=authority,
                interface="protocol",
                family="schemas",
                title=str(document.get("title", schema_authority)),
                pointers=[base],
                detector="protocol-schema",
                source_ids=[f"protocol:{schema_authority}"],
            )

    for index, surface in enumerate(cast(Sequence[Mapping[str, object]], external["python"])):
        authority = str(surface["distribution"])
        module = str(surface["module"])
        _add_element(
            elements,
            authority=authority,
            interface="python",
            family="modules",
            title=module,
            pointers=[f"/external_contract/python/{index}"],
            detector="python-export",
            source_ids=[
                f"python:{authority}:{module}"
                if authority == "riverhog-client"
                else f"python:{authority}"
            ],
        )

    state = cast(Mapping[str, object], external["durable_state"])
    _add_element(
        elements,
        authority="durable-state",
        interface="durable-state",
        family="format",
        title="Durable-state contract",
        pointers=["/external_contract/durable_state/schema"],
        detector="durable-state",
        source_ids=["generator:contract-projection"],
    )
    for index, owner in enumerate(cast(Sequence[Mapping[str, object]], state["owners"])):
        authority = str(owner["id"])
        _add_element(
            elements,
            authority=authority,
            interface="durable-state",
            family="owners",
            title=f"{authority} durable state",
            pointers=[f"/external_contract/durable_state/owners/{index}"],
            detector="durable-state",
            source_ids=[f"state:{authority}"],
        )

    extents = cast(Mapping[str, object], external["extents"])
    for key in ("schema", "coverage", "sha256"):
        _add_element(
            elements,
            authority="extent-contract",
            interface="extent",
            family="policy",
            title=f"Extent {key.replace('_', ' ')}",
            pointers=[f"/external_contract/extents/{key}"],
            detector="extent",
            source_ids=["extent:extent-contract"],
        )
    for key in sorted(cast(Mapping[str, object], extents["principles"])):
        element = _add_element(
            elements,
            authority="extent-contract",
            interface="extent",
            family="principles",
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
            family="rules",
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
            family="decisions",
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


def _link_operation_parity(
    elements: list[dict[str, object]], projection: Mapping[str, object]
) -> None:
    operations: dict[tuple[str, str], dict[str, object]] = {}
    http: dict[tuple[str, str], dict[str, object]] = {}
    cli: dict[tuple[str, str], dict[str, object]] = {}
    for element in elements:
        details = cast(Mapping[str, object], element.get("details", {}))
        operation_id = details.get("operation_id")
        if element["interface"] == "operation" and operation_id:
            operations[(str(element["authority"]), str(operation_id))] = element
        elif element["interface"] == "http" and operation_id:
            http[(str(element["authority"]), str(operation_id))] = element
        elif element["interface"] == "cli":
            command_parts = cast(Sequence[str], details.get("command_path", ()))
            command = " ".join(command_parts)
            cli[(str(element["authority"]), command)] = element
            if len(command_parts) > 1:
                cli[(str(element["authority"]), " ".join(command_parts[1:]))] = element
    external = cast(Mapping[str, object], projection["external_contract"])
    operation_values = cast(Sequence[Mapping[str, object]], external["operations"])
    operation_by_id = {
        (str(value["application"]), str(value["operation_id"])): value for value in operation_values
    }
    cli_authority = {
        "riverhog": "piggity",
        "riverhog-ftp-adapter": "riverhog-ftp-adapter",
        "stove0": "stove0",
    }
    for key, operation in operations.items():
        # The operation projection is the exact authority for HTTP/CLI parity.
        # Cross-links improve navigation without transferring interface ownership.
        related: list[dict[str, object]] = []
        http_element = http.get(key)
        if http_element is not None:
            related.append(http_element)
        record = operation_by_id[key]
        for command in cast(Sequence[str], record.get("cli_commands", ())):
            cli_element = cli.get((cli_authority.get(key[0], key[0]), command))
            if cli_element is not None:
                related.append(cli_element)
        for related_element in related:
            cast(list[str], operation["related_element_ids"]).append(str(related_element["id"]))
            cast(list[str], related_element["related_element_ids"]).append(str(operation["id"]))


def _excluded_launchers(projection: Mapping[str, object]) -> list[dict[str, object]]:
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    external = cast(Mapping[str, object], projection["external_contract"])
    cli_names = set(cast(Mapping[str, object], external["cli"]))
    exclusions: list[dict[str, object]] = []
    for index, component in enumerate(
        cast(Sequence[Mapping[str, object]], boundaries["components"])
    ):
        for name, target in sorted(
            cast(Mapping[str, object], component["console_scripts"]).items()
        ):
            if name in cli_names:
                continue
            exclusions.append(
                {
                    "id": f"excluded:console-script:{name}",
                    "kind": "console-script",
                    "detector": "cli-tree",
                    "disposition": "excluded",
                    "policy_id": "exclusion/process-launcher-not-cli/v1",
                    "boundary_pointer": (
                        f"/boundaries/components/{index}/console_scripts/{_escape_pointer(name)}"
                    ),
                    "source_authority_ids": ["release:release.toml"],
                    "installed_target": target,
                }
            )
    return exclusions


def _detector_meta_closure(projection: Mapping[str, object]) -> dict[str, object]:
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    external = cast(Mapping[str, object], projection["external_contract"])
    cli_names = set(cast(Mapping[str, object], external["cli"]))
    python_units = {
        str(surface["distribution"])
        for surface in cast(Sequence[Mapping[str, object]], external["python"])
    }
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
        if distribution in python_units:
            channels.append(
                {"id": f"python:{distribution}", "kind": "python", "detector": "python-export"}
            )
        for name in sorted(cast(Mapping[str, object], component["console_scripts"])):
            channels.append(
                {
                    "id": f"console-script:{distribution}:{name}",
                    "kind": "console-script",
                    **(
                        {"detector": "cli-tree"}
                        if name in cli_names
                        else {
                            "disposition": "excluded",
                            "policy_id": "exclusion/process-launcher-not-cli/v1",
                        }
                    ),
                }
            )
    for point in cast(Sequence[Mapping[str, object]], boundaries["entry_point_extensions"]):
        channels.append(
            {
                "id": f"extension-entry-point:{point['group']}",
                "kind": "extension-entry-point",
                "detector": "python-export",
            }
        )
    for point in cast(Sequence[Mapping[str, object]], boundaries["process_extensions"]):
        channels.append(
            {
                "id": f"process-protocol:{point['name']}",
                "kind": "process-protocol",
                "detector": "protocol-schema",
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
        if "detector" in channel:
            detector_bindings[str(channel["detector"])].append(str(channel["id"]))
        else:
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
            "excluded": sum(item.get("disposition") == "excluded" for item in channels),
            "missing": 0,
            "duplicate": len(ids) - len(set(ids)),
            "stale": 0,
            "undecided": sum(("detector" in item) == ("disposition" in item) for item in channels),
        },
    }


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
    elements: Sequence[Mapping[str, object]], projection: Mapping[str, object]
) -> dict[str, object]:
    pointers = [pointer for item in elements for pointer in cast(Sequence[str], item["pointers"])]
    terminals = _terminal_pointers(projection)
    extent_prefix = "/external_contract/extents/decisions/"
    semantic_terminals = [pointer for pointer in terminals if not pointer.startswith(extent_prefix)]
    owned = {
        terminal: [
            pointer
            for pointer in pointers
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
        "extent_decisions": len(declared_decision_ids),
        "missing": sum(not owners for owners in owned.values())
        + len(set(declared_decision_ids) - set(represented_decision_ids)),
        "multiply_represented": sum(len(owners) > 1 for owners in owned.values())
        + len(represented_decision_ids)
        - len(set(represented_decision_ids)),
        "stale": len(set(represented_decision_ids) - set(declared_decision_ids)),
    }


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


def _md(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").replace("](", "]&#40;")


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


def _render_schema(value: Mapping[str, object]) -> list[str]:
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
            rendered = (
                json.dumps(value[key], ensure_ascii=False)
                if isinstance(value[key], (list, Mapping))
                else str(value[key])
            )
            lines.append(f"- `{key}`: {_md(rendered)}")
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
            lines.append(
                f"| `{_md(name)}` | {'yes' if name in required else 'no'} | "
                f"{_md(_shape_summary(field))} | {_md(field_map.get('description', ''))} |"
            )
    schemas = value.get("schemas")
    if isinstance(schemas, Mapping):
        lines.extend(["", "### Schemas", "", "| Schema | Shape |", "|---|---|"])
        for name, schema in schemas.items():
            lines.append(f"| `{_md(name)}` | {_md(_shape_summary(schema))} |")
    definitions = value.get("$defs")
    if isinstance(definitions, Mapping):
        lines.extend(["", "### Definitions", "", "| Definition | Shape |", "|---|---|"])
        for name, schema in definitions.items():
            lines.append(f"| `{_md(name)}` | {_md(_shape_summary(schema))} |")
    return lines


def _render_http(value: Mapping[str, object], details: Mapping[str, object]) -> list[str]:
    lines = []
    for key in ("operationId", "summary", "description", "deprecated"):
        if key in value:
            lines.append(f"- `{key}`: {_md(value[key])}")
    if "security" in value:
        lines.append(f"- `security`: `{_md(json.dumps(value['security'], sort_keys=True))}`")
    parameters = cast(Sequence[Mapping[str, object]], value.get("parameters", ()))
    if parameters:
        lines.extend(
            ["", "### Parameters", "", "| Name | In | Required | Schema |", "|---|---|---:|---|"]
        )
        for item in parameters:
            lines.append(
                f"| `{_md(item.get('name', ''))}` | {_md(item.get('in', ''))} | "
                f"{'yes' if item.get('required') else 'no'} | "
                f"{_md(_shape_summary(item.get('schema')))} |"
            )
    if "requestBody" in value:
        lines.extend(
            [
                "",
                "### Request body",
                "",
                f"`{_md(json.dumps(value['requestBody'], sort_keys=True))}`",
            ]
        )
    responses = value.get("responses")
    if isinstance(responses, Mapping):
        lines.extend(["", "### Responses", "", "| Status | Description |", "|---|---|"])
        for status, response in responses.items():
            description = (
                cast(Mapping[str, object], response).get("description", "")
                if isinstance(response, Mapping)
                else ""
            )
            lines.append(f"| `{_md(status)}` | {_md(description)} |")
    del details
    return lines


def _render_cli(values: Sequence[object]) -> list[str]:
    parameters: Sequence[Mapping[str, object]] = ()
    name = ""
    for value in values:
        if isinstance(value, str):
            name = value
        elif isinstance(value, list):
            parameters = cast(Sequence[Mapping[str, object]], value)
    lines = [f"- Parser name: `{_md(name)}`"] if name else []
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
        for item in parameters:
            lines.append(
                f"| `{_md(item.get('name', ''))}` | {_md(item.get('kind', ''))} | "
                f"{'yes' if item.get('required') else 'no'} | {_md(item.get('type', ''))} | "
                f"{_md(', '.join(cast(Sequence[str], item.get('options', ()))))} |"
            )
    return lines


def _render_operation(value: Mapping[str, object]) -> list[str]:
    def rendered(item: object) -> object:
        return (
            json.dumps(item, ensure_ascii=False, sort_keys=True)
            if isinstance(item, (Mapping, list))
            else item
        )

    return [
        "| Concern | Contract |",
        "|---|---|",
        *(f"| `{_md(key)}` | {_md(rendered(item))} |" for key, item in value.items()),
    ]


def _render_generic(values: Sequence[object]) -> list[str]:
    if len(values) == 1 and isinstance(values[0], Mapping):
        value = cast(Mapping[str, object], values[0])
        schema_lines = _render_schema(value)
        if schema_lines:
            return schema_lines
    large_value = values[0] if len(values) == 1 else list(values)
    if isinstance(large_value, Mapping):
        return [
            "| Field | Shape |",
            "|---|---|",
            *(
                f"| `{_md(key)}` | {_md(_shape_summary(item))} |"
                for key, item in large_value.items()
            ),
        ]
    return [f"- Shape: {_shape_summary(large_value)}"]


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
        if element["authority"] != authority or element["interface"] != "http":
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
    pointers = cast(Sequence[str], element["pointers"])
    values = [pointer_value(projection, pointer) for pointer in pointers]
    source_index = _source_index(trace)
    details = cast(Mapping[str, object], element.get("details", {}))
    purpose = "Exact externally visible contract owned by this semantic dossier."
    if len(values) == 1 and isinstance(values[0], Mapping):
        value = cast(Mapping[str, object], values[0])
        purpose = str(value.get("summary", value.get("description", purpose))).strip() or purpose
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
        f"| Authority | `{_md(element['authority'])}` |",
        f"| Interface | `{_md(element['interface'])}` |",
        f"| Family | `{_md(element['family'])}` |",
        "| Contract elements | 1 |",
        f"| Extent decisions | {len(cast(Sequence[object], element['extent_decision_ids']))} |",
        "",
        "## External contract",
        "",
    ]
    interface = str(element["interface"])
    if (
        interface == "http"
        and len(values) == 1
        and isinstance(values[0], Mapping)
        and "method" in details
    ):
        lines.extend(_render_http(cast(Mapping[str, object], values[0]), details))
    elif interface == "cli":
        lines.extend(_render_cli(values))
    elif interface == "operation" and len(values) == 1 and isinstance(values[0], Mapping):
        lines.extend(_render_operation(cast(Mapping[str, object], values[0])))
    else:
        lines.extend(_render_generic(values))
    lines.append("")

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
        lines.extend(
            [
                "### Progression, limits, and lifecycle",
                "",
                "| Dimension | Unit | Policy | Bounds or reason |",
                "|---|---|---|---|",
            ]
        )
        for identity in extent_ids:
            decision = decisions[identity]
            bounds = ", ".join(
                f"{key}={value}"
                for key, value in decision.items()
                if key
                in {
                    "minimum",
                    "maximum",
                    "semantic_maximum",
                    "declared_operational_maximum",
                    "reason",
                }
            )
            lines.append(
                f"| {_md(decision['dimension'])} | {_md(decision['unit'])} | "
                f"`{_md(decision['policy'])}` | {_md(bounds)} |"
            )
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
            *(f"- `{policy}`" for policy in cast(Sequence[str], element["policy_ids"])),
            "",
            "## Evidence",
            "",
            "### Qualification",
            "",
            *(f"- `{route}`" for route in cast(Sequence[str], element["qualification_routes"])),
            "",
            "### Executable sources",
            "",
        ]
    )
    for source_id in cast(Sequence[str], element["source_authority_ids"]):
        source = source_index[source_id]
        location = cast(Mapping[str, object], source.get("source", {}))
        rendered = str(location.get("path", location.get("module", source_id)))
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        lines.append(f"- `{source_id}` — `{rendered}{symbol}`")
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


def _table_counts(values: Mapping[str, object], label: str) -> list[str]:
    return [
        f"| {label} | Count |",
        "|---|---:|",
        *(f"| `{_md(key)}` | {value} |" for key, value in values.items()),
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
                "contract_elements": authority_counts.get(str(item["contract_owner"]), 0),
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
    installation = cast(Mapping[str, object], release["installation"])
    installation_id = f"installation:{installation['method']}"
    nodes.append(
        {
            "id": installation_id,
            "kind": "installation",
            "name": installation["method"],
            "description": (
                "Coordinated end-user installation roots declared by the release contract."
            ),
            "contract_elements": 0,
        }
    )
    for root in cast(Sequence[str], installation["roots"]):
        edges.append(
            {
                "type": "installed-as",
                "source": f"component:{root}",
                "target": installation_id,
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
            "riverhog-extensions",
            "Riverhog extension boundaries",
            "riverhog-product",
            "surfaces/riverhog.md#extension-boundaries",
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
        if authority == product_node["name"]:
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
        mapped[map_id].append(
            {
                "authority": authority,
                "contract_elements": authority_counts[authority],
                "owner_component_ids": sorted(authority_owners[authority]),
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
    riverhog_extensions = sorted(
        (
            item
            for item in relationship_nodes
            if item["kind"] in {"extension-point", "process-protocol"}
            and str(item.get("owner", "")).startswith("riverhog")
        ),
        key=lambda item: str(item["name"]),
    )

    def render_node(node: Mapping[str, object], depth: int) -> list[str]:
        prefix = "  " * depth
        authorities = cast(Sequence[Mapping[str, object]], node["authorities"])
        descendants = children.get(str(node["id"]), [])
        displayed_authorities = len(authorities) + sum(
            len(cast(Sequence[object], child["authorities"])) for child in descendants
        )
        contract_elements = sum(cast(int, item["contract_elements"]) for item in authorities) + sum(
            cast(int, item["contract_elements"])
            for child in descendants
            for item in cast(Sequence[Mapping[str, object]], child["authorities"])
        )
        result = [
            f"{prefix}- {_map_node_link(source, node)} — "
            f"{displayed_authorities} "
            f"{'authority' if displayed_authorities == 1 else 'authorities'}, "
            f"{contract_elements} "
            f"{'contract element' if contract_elements == 1 else 'contract elements'}"
        ]
        for authority in authorities:
            authority_name = str(authority["authority"])
            element_label = (
                "contract element" if authority["contract_elements"] == 1 else "contract elements"
            )
            result.append(
                f"{prefix}  - [{_md(authority_name)}]"
                f"({_relative_link(source, _authority_index_path(authority_name))}) — "
                f"{authority['contract_elements']} {element_label}"
            )
        if node["id"] == "riverhog-extensions":
            for extension in riverhog_extensions:
                result.append(f"{prefix}  - `{_md(extension['name'])}`")
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
                name = str(authority["authority"])
                element_label = (
                    "contract element"
                    if authority["contract_elements"] == 1
                    else "contract elements"
                )
                lines.append(
                    f"- [{_md(name)}]({_relative_link(source, _authority_index_path(name))}) — "
                    f"{authority['contract_elements']} {element_label}"
                )
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
        interfaces = sorted({str(item["interface"]) for item in by_authority[authority]})
        owners = [
            relationship_nodes[owner]
            for owner in cast(Sequence[str], record["owner_component_ids"])
        ]
        purposes = sorted({str(owner["description"]) for owner in owners})
        lines.append(
            f"| [{_md(authority)}]({_relative_link(source, _authority_index_path(authority))}) | "
            f"{record['contract_elements']} | {_md(', '.join(interfaces))} | "
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
            "release, and boundary semantics without becoming a separate product surface.",
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
            if node_id == "riverhog-service":
                service_elements = [item for item in elements if item["authority"] == "riverhog"]
                lines.extend(
                    [
                        "### Semantic families",
                        "",
                        *_table_counts(
                            dict(
                                sorted(
                                    Counter(
                                        str(item["family"]) for item in service_elements
                                    ).items()
                                )
                            ),
                            "Family",
                        ),
                        "",
                    ]
                )
            if node_id == "riverhog-contracts":
                extension_nodes = sorted(
                    (
                        item
                        for item in cast(Sequence[Mapping[str, object]], relationship["nodes"])
                        if item["kind"] in {"extension-point", "process-protocol"}
                        and str(item.get("owner", "")).startswith("riverhog")
                    ),
                    key=lambda item: str(item["name"]),
                )
                lines.extend(["## Extension boundaries", ""])
                for extension in extension_nodes:
                    lines.extend(
                        [
                            f"- `{extension['name']}`",
                            f"  - {extension['description']}",
                        ]
                    )
                lines.append("")
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
        metadata[path] = {"kind": "semantic-surface", "counts": counts, "map_node_ids": node_ids}
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
                    f"| `{policy['id']}` | {applications.get(str(policy['id']), 0)} |"
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
            policy_lines.extend(
                [
                    f"#### `{policy['id']}`",
                    "",
                    rendered_meaning,
                    "",
                    f"- Applicability: `{_md(json.dumps(applicability, ensure_ascii=False))}`",
                    "- Observable result or violation: "
                    f"`{_md(json.dumps(observable, ensure_ascii=False, sort_keys=True))}`",
                    "- Executable authorities:",
                    *(f"  - `{source}`" for source in executable_authorities),
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
            *_table_counts(cast(Mapping[str, object], counts["by_interface"]), "Interface"),
            "",
            "## Interfaces",
            "",
        ]
        for interface, values in sorted(interfaces.items()):
            interface_path = (
                f"{ATLAS_DIRECTORY}/authorities/{authority_slug}/"
                f"{_slug(interface, limit=48)}/index.md"
            )
            lines.append(
                f"- [{interface}]({_relative_link(authority_path, interface_path)}) — "
                f"{len(values)} elements"
            )
        files[authority_path] = ("\n".join(lines).rstrip() + "\n").encode()

        for interface, values in sorted(interfaces.items()):
            interface_path = (
                f"{ATLAS_DIRECTORY}/authorities/{authority_slug}/"
                f"{_slug(interface, limit=48)}/index.md"
            )
            interface_counts = _counts(values)
            families: dict[str, list[dict[str, object]]] = defaultdict(list)
            for item in values:
                families[str(item["family"])].append(item)
            use_family_indexes = len(values) >= FAMILY_INDEX_MINIMUM_ELEMENTS and len(families) > 1
            lines = [
                f"# {authority}: {interface}",
                "",
                f"[Atlas]({_relative_link(interface_path, f'{ATLAS_DIRECTORY}/index.md')}) · "
                f"[Authority]({_relative_link(interface_path, authority_path)}) · "
                f"[Policies]({_relative_link(interface_path, policy_path)})",
                "",
                f"Contract elements: **{interface_counts['contract_elements']}** · "
                f"Extent decisions: **{interface_counts['extent_decisions']}**",
                "",
                *_table_counts(
                    {family: len(items) for family, items in sorted(families.items())},
                    "Family",
                ),
                "",
                *_table_counts(cast(Mapping[str, object], interface_counts["by_policy"]), "Policy"),
                "",
            ]
            if use_family_indexes:
                lines.extend(
                    [
                        "## Semantic families",
                        "",
                        "| Family | Contract elements | Extent decisions |",
                        "|---|---:|---:|",
                    ]
                )
                for family, family_values in sorted(families.items()):
                    family_path = (
                        f"{ATLAS_DIRECTORY}/authorities/{authority_slug}/"
                        f"{_slug(interface, limit=48)}/families/{_slug(family, limit=72)}/index.md"
                    )
                    family_counts = _counts(family_values)
                    lines.append(
                        f"| [{_md(family)}]({_relative_link(interface_path, family_path)}) | "
                        f"{family_counts['contract_elements']} | "
                        f"{family_counts['extent_decisions']} |"
                    )
                    family_lines = [
                        f"# {authority}: {interface}: {family}",
                        "",
                        f"[Atlas]({_relative_link(family_path, f'{ATLAS_DIRECTORY}/index.md')}) · "
                        f"[Authority]({_relative_link(family_path, authority_path)}) · "
                        f"[Interface]({_relative_link(family_path, interface_path)}) · "
                        f"[Policies]({_relative_link(family_path, policy_path)})",
                        "",
                        f"Contract elements: **{family_counts['contract_elements']}** · "
                        f"Extent decisions: **{family_counts['extent_decisions']}**",
                        "",
                        *_table_counts(
                            cast(Mapping[str, object], family_counts["by_policy"]), "Policy"
                        ),
                        "",
                        "## Semantic dossiers",
                        "",
                        "| Dossier | Extent decisions |",
                        "|---|---:|",
                    ]
                    for item in sorted(family_values, key=lambda value: str(value["title"])):
                        extent_count = len(cast(Sequence[object], item["extent_decision_ids"]))
                        family_lines.append(
                            f"| [{_md(item['title'])}]"
                            f"({_relative_link(family_path, str(item['dossier']))}) | "
                            f"{extent_count} |"
                        )
                    files[family_path] = ("\n".join(family_lines).rstrip() + "\n").encode()
            else:
                lines.extend(
                    [
                        "## Semantic dossiers",
                        "",
                        "| Dossier | Family | Extent decisions |",
                        "|---|---|---:|",
                    ]
                )
                for item in sorted(
                    values, key=lambda value: (str(value["family"]), str(value["title"]))
                ):
                    extent_count = len(cast(Sequence[object], item["extent_decision_ids"]))
                    lines.append(
                        f"| [{_md(item['title'])}]"
                        f"({_relative_link(interface_path, str(item['dossier']))}) | "
                        f"`{_md(item['family'])}` | {extent_count} |"
                    )
            files[interface_path] = ("\n".join(lines).rstrip() + "\n").encode()

    relationship = _relationship_model(projection, trace, elements, component_descriptions)
    surface_files, surface_metadata = _render_contract_surfaces(relationship, elements)
    files.update(surface_files)

    root_counts = _counts(elements, exclusions)
    root_path = f"{ATLAS_DIRECTORY}/index.md"
    evidence_path = f"{ATLAS_DIRECTORY}/evidence/index.md"
    exclusion_path = f"{ATLAS_DIRECTORY}/evidence/exclusions.md"
    authority_evidence_path = f"{ATLAS_DIRECTORY}/evidence/authorities.md"
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
        "This page supports the ‘no more’ side of the audit by naming every discovered delivery "
        "candidate intentionally excluded from the external CLI surface.",
        "",
        f"Excluded candidates: **{len(exclusions)}**",
        "",
    ]
    for policy_id in sorted({str(item["policy_id"]) for item in exclusions}):
        policy = policy_by_id[policy_id]
        exclusion_lines.extend(
            [
                f"## `{policy_id}`",
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
                    f"- `{exclusion['id']}`",
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
                f"`{_md(source)}`"
                for source in cast(Sequence[str], exclusion["source_authority_ids"])
            )
            exclusion_lines.append(
                f"| `{_md(exclusion['id'])}` | `{_md(exclusion['boundary_pointer'])}` | "
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
        ),
        "",
        "## Source authorities",
        "",
        f"Source authorities: **{len(source_index)}**",
        "",
        "| Source authority | Applications | Executable location |",
        "|---|---:|---|",
    ]
    for source_id, count in source_counts.items():
        source = source_index[source_id]
        location = cast(Mapping[str, object], source.get("source", {}))
        rendered = str(location.get("path", location.get("module", source_id)))
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        source_lines.append(f"| `{_md(source_id)}` | {count} | `{_md(rendered + symbol)}` |")
    files[source_evidence_path] = ("\n".join(source_lines).rstrip() + "\n").encode()

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
        relationship_lines.append(
            f"| `{_md(node['id'])}` | `{_md(node['kind'])}` | `{_md(node['name'])}` | "
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
        relationship_lines.append(
            f"| `{_md(nodes_by_id[str(edge['source'])]['name'])}` | "
            f"`{_md(edge['type'])}` | `{_md(nodes_by_id[str(edge['target'])]['name'])}` | "
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
        *(f"| `{_md(name)}` | `{_md(value)}` |" for name, value in identities.items()),
        "",
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
        f"| Contract elements | {root_counts['contract_elements']} |",
        f"| Extent decisions | {root_counts['extent_decisions']} |",
        f"| Explicit exclusions | {root_counts['excluded_candidates']} |",
        f"| Source authorities | {len(source_index)} |",
        "",
        "## Exact evidence",
        "",
        f"- [Explicit exclusions]({_relative_link(evidence_path, exclusion_path)})",
        f"- [Exact authority inventory]({_relative_link(evidence_path, authority_evidence_path)})",
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
            document_counts = {"authorities": len(grouped)}
            kind = "evidence-authority-inventory"
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
        elif path.endswith("/index.md") and path.count("/") == 3:
            authority_slug = path.split("/")[2]
            subset = [
                item
                for item in elements
                if _slug(str(item["authority"]), limit=72) == authority_slug
            ]
            document_counts = _counts(subset)
            kind = "authority-index"
        elif "/families/" in path:
            parts = path.split("/")
            authority_slug, interface_slug, family_slug = parts[2], parts[3], parts[5]
            subset = [
                item
                for item in elements
                if _slug(str(item["authority"]), limit=72) == authority_slug
                and _slug(str(item["interface"]), limit=48) == interface_slug
                and _slug(str(item["family"]), limit=72) == family_slug
            ]
            document_counts = _counts(subset)
            kind = "family-index"
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
    elements = [
        *_boundary_elements(normalized_projection),
        *_external_elements(normalized_projection, normalized_trace),
    ]
    _attach_extent_decisions(elements, normalized_projection)
    _link_operation_parity(elements, normalized_projection)
    ids = [str(item["id"]) for item in elements]
    if len(ids) != len(set(ids)):
        raise ContractAtlasError("semantic contract element identities are not unique")
    _assign_dossiers(elements)
    elements.sort(key=lambda item: str(item["id"]))
    exclusions = _excluded_launchers(normalized_projection)
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
    candidates = [
        {
            "id": f"candidate:{item['id']}",
            "detector": item["detector"],
            "disposition": "contractual",
            "element_id": item["id"],
            "source_authority_ids": item["source_authority_ids"],
        }
        for item in elements
    ]
    meta_closure = _detector_meta_closure(normalized_projection)
    projection_coverage = _projection_coverage(elements, normalized_projection)
    discovery = {
        "detectors": list(DETECTORS),
        "meta_closure": meta_closure,
        "candidates": candidates,
        "exclusions": exclusions,
        "projection_coverage": projection_coverage,
        "anomalies": {
            "missing": projection_coverage["missing"],
            "duplicate": len(candidates) - len({str(item["id"]) for item in candidates}),
            "stale": projection_coverage["stale"],
            "undecided": 0,
            "multiply_disposed": 0,
            "multiply_represented": projection_coverage["multiply_represented"],
        },
    }
    if any(cast(Mapping[str, int], discovery["anomalies"]).values()):
        raise ContractAtlasError(
            f"semantic atlas does not exactly own the machine projection: {discovery['anomalies']}"
        )
    semantic_identity = {
        "schema": CONTRACT_IDENTITY_SCHEMA,
        "series": normalized_projection["series"],
        "boundaries": normalized_projection["boundaries"],
        "external_contract": normalized_projection["external_contract"],
        "policies": {key: value for key, value in policies.items() if key != "exclusion"},
        "unsafe_integer_paths": projection_integer_paths,
    }
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
    graph: dict[str, set[str]] = {path: set() for path in files}
    for source, payload in files.items():
        for target in link_pattern.findall(payload.decode()):
            local_path = target.split("#", 1)[0]
            if not local_path:
                continue
            if local_path.startswith("/") or re.match(r"^[a-z][a-z0-9+.-]*:", local_path):
                continue
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(source), local_path))
            if resolved not in files:
                raise ContractAtlasError(
                    f"atlas document has an unresolved local link: {source} -> {target}"
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
    dossiers = [str(item["dossier"]) for item in elements]
    if len(dossiers) != len(set(dossiers)) or not set(dossiers) <= paths:
        raise ContractAtlasError("each contract element must own one unique atlas dossier")
    for item in elements:
        marker = f"<!-- contract-element: {item['id']} -->".encode()
        dossier = atlas.files[str(item["dossier"])]
        if marker not in dossier:
            raise ContractAtlasError(
                f"atlas dossier does not identify its contract element: {item['id']}"
            )
        if any(
            policy.encode() not in dossier for policy in cast(Sequence[str], item["policy_ids"])
        ):
            raise ContractAtlasError(
                f"atlas dossier does not expose every effective policy: {item['id']}"
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
    projection_value = cast(Mapping[str, object], root["projection"])
    observed_projection_coverage = _projection_coverage(elements, projection_value)
    if discovery["projection_coverage"] != observed_projection_coverage:
        raise ContractAtlasError("projection-to-atlas coverage is stale")
    candidates = cast(Sequence[Mapping[str, object]], discovery["candidates"])
    candidate_elements = [str(item.get("element_id")) for item in candidates]
    if (
        len(candidate_elements) != len(set(candidate_elements))
        or set(candidate_elements) != set(ids)
        or any(item.get("disposition") != "contractual" for item in candidates)
    ):
        raise ContractAtlasError("contractual discovery candidates do not match atlas elements")

    trace_value = cast(Mapping[str, object], root["trace"])
    source_index = _source_index(trace_value)
    checked_sources = {
        str(item["id"]): dict(item)
        for item in cast(Sequence[Mapping[str, object]], root["sources"])
    }
    if checked_sources != source_index:
        raise ContractAtlasError("source authority index is stale")
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
        if name == "atlas_representation_sha256":
            if f"/identities/{name}" not in identity_page:
                raise ContractAtlasError("identity evidence omits its representation route")
        elif f"| `{name}` | `{identity}` |" not in identity_page:
            raise ContractAtlasError(f"identity evidence omits independent identity: {name}")

    exclusion_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/exclusions.md"].decode()
    for item in exclusions:
        policy = next(
            policy
            for values in policies.values()
            for policy in cast(Sequence[Mapping[str, object]], values)
            if policy["id"] == item["policy_id"]
        )
        required_exclusion_values = [
            item["id"],
            item["kind"],
            item["installed_target"],
            item["boundary_pointer"],
            item["detector"],
            item["policy_id"],
            policy["meaning"],
            *cast(Sequence[str], item["source_authority_ids"]),
        ]
        if any(_md(value) not in exclusion_page for value in required_exclusion_values):
            raise ContractAtlasError(f"human exclusion inventory is incomplete: {item['id']}")

    source_evidence_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/sources.md"].decode()
    for route in cast(
        Mapping[str, object], cast(Mapping[str, object], root["counts"])["by_qualification_route"]
    ):
        if f"`{route}`" not in source_evidence_page:
            raise ContractAtlasError(f"human evidence index omits route: {route}")
    for source_id, source in source_index.items():
        location = cast(Mapping[str, object], source.get("source", {}))
        rendered = str(location.get("path", location.get("module", source_id)))
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        if (
            f"`{source_id}`" not in source_evidence_page
            or f"`{rendered}{symbol}`" not in source_evidence_page
        ):
            raise ContractAtlasError(f"human evidence index omits source: {source_id}")

    observed_counts = _counts(
        elements,
        exclusions,
    )
    checked_counts = cast(Mapping[str, object], root["counts"])
    for key, value in observed_counts.items():
        if checked_counts.get(key) != value:
            raise ContractAtlasError(f"root aggregate count is stale: {key}")
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
    for node in contract_map_nodes:
        for record in cast(Sequence[Mapping[str, object]], node["authorities"]):
            if record["contract_elements"] != element_counts_by_authority[str(record["authority"])]:
                raise ContractAtlasError(
                    f"human contract-map count is stale: {record['authority']}"
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
        if f"]({root_link})" not in root_page:
            raise ContractAtlasError(f"human contract map omits exact authority: {authority}")
        if f"]({inventory_link})" not in authority_inventory_page:
            raise ContractAtlasError(f"authority evidence omits exact authority: {authority}")
    relationship_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/relationships.md"].decode()
    for node in relationship_nodes:
        required = (node["id"], node["kind"], node["name"], node["description"])
        if any(_md(value) not in relationship_page for value in required):
            raise ContractAtlasError(f"relationship evidence omits node: {node['id']}")
    for edge in relationship_edges:
        detail = edge.get("scope", edge.get("binding", ""))
        required = (
            relationship_nodes_by_id[str(edge["source"])]["name"],
            edge["type"],
            relationship_nodes_by_id[str(edge["target"])]["name"],
            detail,
        )
        if any(_md(value) not in relationship_page for value in required):
            raise ContractAtlasError(f"relationship evidence omits edge: {edge}")
    if "Maintainer-selected nonnormative references" not in root_page:
        raise ContractAtlasError("atlas front door does not identify references as nonnormative")
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
            expected_counts = {"authorities": len(exact_authorities)}
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
            elif kind == "family-index":
                interface_slug, family_slug = parts[3], parts[5]
                subset = [
                    item
                    for item in subset
                    if _slug(str(item["interface"]), limit=48) == interface_slug
                    and _slug(str(item["family"]), limit=72) == family_slug
                ]
            expected_counts = _counts(subset)
        if descriptor["counts"] != expected_counts:
            raise ContractAtlasError(f"atlas roll-up count is stale: {path}")
        if kind == "family-index":
            page = atlas.files[path].decode()
            for item in subset:
                link = _relative_link(path, str(item["dossier"]))
                if f"]({link})" not in page:
                    raise ContractAtlasError(f"semantic-family index omits dossier: {item['id']}")

    semantic_identity = {
        "schema": CONTRACT_IDENTITY_SCHEMA,
        "series": projection_value["series"],
        "boundaries": projection_value["boundaries"],
        "external_contract": projection_value["external_contract"],
        "policies": {key: value for key, value in policies.items() if key != "exclusion"},
        "unsafe_integer_paths": root["projection_unsafe_integer_paths"],
    }
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

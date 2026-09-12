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
MAX_HUMAN_DOCUMENT_BYTES = 128 * 1024
FAMILY_INDEX_MINIMUM_ELEMENTS = 24
RELATIONSHIP_SCHEMA = "riverhog-contract-human-relationships/v1"

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
        if "type" in value:
            return str(value["type"])
        if "$ref" in value:
            return str(value["$ref"])
        return f"object ({len(value)} fields)"
    if isinstance(value, list):
        return f"array ({len(value)} items)"
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
        "## Complete owned contract",
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
        "| Audit field | Value |",
        "|---|---|",
        f"| Authority | `{_md(element['authority'])}` |",
        f"| Interface | `{_md(element['interface'])}` |",
        f"| Family | `{_md(element['family'])}` |",
        "| Contract elements | 1 |",
        f"| Extent decisions | {len(cast(Sequence[object], element['extent_decision_ids']))} |",
        "",
        "## Machine authority",
        "",
        *(f"- `{pointer}`" for pointer in pointers),
        "",
        "## Effective policies",
        "",
        *(f"- `{policy}`" for policy in cast(Sequence[str], element["policy_ids"])),
        "",
        "## Executable sources and proof",
        "",
    ]
    for source_id in cast(Sequence[str], element["source_authority_ids"]):
        source = source_index[source_id]
        location = cast(Mapping[str, object], source.get("source", {}))
        rendered = str(location.get("path", location.get("module", source_id)))
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        lines.append(f"- `{source_id}` — `{rendered}{symbol}`")
    lines.extend(
        [
            *(
                f"- Proof: `{route}`"
                for route in cast(Sequence[str], element["qualification_routes"])
            ),
            "",
        ]
    )
    related_ids = cast(Sequence[str], element["related_element_ids"])
    if related_ids:
        lines.extend(["## Related interface records", ""])
        for related_id in related_ids:
            related = elements_by_id[related_id]
            lines.append(f"- [{related['title']}]({_relative_link(path, str(related['dossier']))})")
        lines.append("")
    referenced = _local_contract_references(str(element["authority"]), values, elements_by_id)
    if referenced:
        lines.extend(["## Referenced contract dossiers", ""])
        for owner in referenced:
            lines.append(f"- [{owner['title']}]({_relative_link(path, str(owner['dossier']))})")
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
                "## Extent decisions",
                "",
                "| Dimension | Unit | Policy | Bounds/reason |",
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
    lines.extend(["## Contract summary", ""])
    interface = str(element["interface"])
    details = cast(Mapping[str, object], element.get("details", {}))
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
    lines.extend(["", *_exact_contract_lines(pointers, values)])
    return ("\n".join(lines).rstrip() + "\n").encode()


def _table_counts(values: Mapping[str, object], label: str) -> list[str]:
    return [
        f"| {label} | Count |",
        "|---|---:|",
        *(f"| `{_md(key)}` | {value} |" for key, value in values.items()),
    ]


def _relationship_model(
    projection: Mapping[str, object],
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
    return {
        "schema": RELATIONSHIP_SCHEMA,
        "center": product_nodes[0],
        "product": product_images[0],
        "reference_policy": boundaries["reference_policy"],
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


def _render_relationships(
    relationship: Mapping[str, object],
    elements: Sequence[Mapping[str, object]],
) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    """Render the frozen graph as an unfamiliar auditor's orientation layer."""

    root_path = f"{ATLAS_DIRECTORY}/index.md"
    base = f"{ATLAS_DIRECTORY}/relationships"
    relationship_path = f"{base}/index.md"
    component_index_path = f"{base}/components/index.md"
    library_path = f"{base}/riverhog-libraries/index.md"
    extension_path = f"{base}/extensions/index.md"
    runtime_path = f"{base}/runtime-images/index.md"
    installation_path = f"{base}/installation/index.md"
    reference_path = f"{base}/references/index.md"
    nodes = cast(Sequence[Mapping[str, object]], relationship["nodes"])
    edges = cast(Sequence[Mapping[str, object]], relationship["edges"])
    by_id = {str(item["id"]): item for item in nodes}
    component_nodes = [item for item in nodes if item["kind"] == "component"]
    authority_names = {str(item["authority"]) for item in elements}

    def component_page(node: Mapping[str, object]) -> str:
        return f"{base}/components/{_slug(str(node['name']), limit=72)}.md"

    def node_link(source: str, node: Mapping[str, object]) -> str:
        if node["kind"] == "component":
            return f"[{_md(node['name'])}]({_relative_link(source, component_page(node))})"
        return f"`{_md(node['name'])}`"

    files: dict[str, bytes] = {}
    metadata: dict[str, dict[str, object]] = {}
    role_counts = dict(sorted(Counter(str(item["role"]) for item in component_nodes).items()))
    kind_counts = dict(sorted(Counter(str(item["kind"]) for item in nodes).items()))
    edge_counts = dict(sorted(Counter(str(item["type"]) for item in edges).items()))
    center = by_id[str(relationship["center"])]
    product = by_id[str(relationship["product"])]
    product_authority_path = (
        f"{ATLAS_DIRECTORY}/authorities/{_slug(str(product['name']), limit=72)}/index.md"
    )
    center_edges = [edge for edge in edges if center["id"] in {edge["source"], edge["target"]}]
    lines = [
        "# Riverhog-centered authority relationships",
        "",
        f"[Atlas]({_relative_link(relationship_path, root_path)})",
        "",
        "This generated view explains how the frozen authorities fit together. It does not "
        "transfer ownership: every edge comes from release roles, dependency metadata, extension "
        "ownership, protocol ownership, image composition, or installation roots already present "
        "in executable authorities.",
        "",
        "## Riverhog service boundary",
        "",
        "| Layer | Authority | Purpose | Contract elements |",
        "|---|---|---|---:|",
        f"| Public service | [{_md(product['name'])}]"
        f"({_relative_link(relationship_path, product_authority_path)}) | "
        f"{_md(product['description'])} | {product['contract_elements']} |",
        f"| Packaged implementation | {node_link(relationship_path, center)} | "
        f"{_md(center['description'])} | {center['contract_elements']} |",
        "",
        "## Relationship shape",
        "",
        *_table_counts(role_counts, "Component role"),
        "",
        *_table_counts(kind_counts, "Node kind"),
        "",
        *_table_counts(edge_counts, "Relationship"),
        "",
        "## Drill down",
        "",
        "- [Riverhog-owned reusable contracts and libraries]"
        f"({_relative_link(relationship_path, library_path)})",
        "- [Independently implementable extension boundaries]"
        f"({_relative_link(relationship_path, extension_path)})",
        f"- [Runtime-image composition]({_relative_link(relationship_path, runtime_path)})",
        "- [Installed end-user and recovery surfaces]"
        f"({_relative_link(relationship_path, installation_path)})",
        "- [Nonnormative reference ecosystem]"
        f"({_relative_link(relationship_path, reference_path)})",
        "- [Complete component relationship inventory]"
        f"({_relative_link(relationship_path, component_index_path)})",
        "",
        "## Direct Riverhog product relationships",
        "",
        "| Direction | Relationship | Counterparty | Scope or binding |",
        "|---|---|---|---|",
    ]
    for edge in center_edges:
        center_outgoing = edge["source"] == center["id"]
        other = by_id[str(edge["target"] if center_outgoing else edge["source"])]
        detail = edge.get("scope", edge.get("binding", ""))
        lines.append(
            f"| {'outgoing' if center_outgoing else 'incoming'} | `{_md(edge['type'])}` | "
            f"{node_link(relationship_path, other)} | `{_md(detail)}` |"
        )
    files[relationship_path] = ("\n".join(lines).rstrip() + "\n").encode()
    metadata[relationship_path] = {
        "kind": "relationship-index",
        "counts": {"nodes": len(nodes), "edges": len(edges)},
    }

    component_lines = [
        "# Complete component relationship inventory",
        "",
        f"[Atlas]({_relative_link(component_index_path, root_path)}) · "
        f"[Relationships]({_relative_link(component_index_path, relationship_path)})",
        "",
        "Every release component appears exactly once below. Purpose text is the maintained "
        "project description; role, path, and relationships are executable release metadata.",
    ]
    for role in sorted(role_counts):
        role_nodes = sorted(
            (item for item in component_nodes if item["role"] == role),
            key=lambda value: str(value["name"]),
        )
        component_lines.extend(
            [
                "",
                f"## {role.replace('_', ' ').title()}",
                "",
                f"Components: **{len(role_nodes)}**",
                "",
                "| Component | Purpose | Owned contract elements |",
                "|---|---|---:|",
            ]
        )
        for node in role_nodes:
            component_lines.append(
                f"| {node_link(component_index_path, node)} | {_md(node['description'])} | "
                f"{node['contract_elements']} |"
            )
    files[component_index_path] = ("\n".join(component_lines).rstrip() + "\n").encode()
    metadata[component_index_path] = {
        "kind": "relationship-component-index",
        "counts": {"components": len(component_nodes), "roles": len(role_counts)},
    }

    for node in component_nodes:
        path = component_page(node)
        outgoing = [edge for edge in edges if edge["source"] == node["id"]]
        incoming = [edge for edge in edges if edge["target"] == node["id"]]
        node_lines = [
            f"# {node['name']}",
            "",
            f"[Atlas]({_relative_link(path, root_path)}) · "
            f"[Relationships]({_relative_link(path, relationship_path)}) · "
            f"[Components]({_relative_link(path, component_index_path)})",
            "",
            str(node["description"]),
            "",
            "| Boundary field | Value |",
            "|---|---|",
            f"| Release role | `{_md(node['role'])}` |",
            f"| Source path | `{_md(node['path'])}` |",
            f"| Description source | `{_md(node['description_source'])}` |",
            f"| Owned contract elements | {node['contract_elements']} |",
        ]
        if node["name"] in authority_names:
            authority_path = (
                f"{ATLAS_DIRECTORY}/authorities/{_slug(str(node['name']), limit=72)}/index.md"
            )
            node_lines.extend(
                [
                    "",
                    f"[Open exact owned authority]({_relative_link(path, authority_path)})",
                ]
            )
        node_lines.extend(
            [
                "",
                "## Typed relationships",
                "",
                "| Direction | Relationship | Counterparty | Scope or binding |",
                "|---|---|---|---|",
            ]
        )
        for edge, direction in [
            *((edge, "outgoing") for edge in outgoing),
            *((edge, "incoming") for edge in incoming),
        ]:
            other = by_id[str(edge["target"] if direction == "outgoing" else edge["source"])]
            detail = edge.get("scope", edge.get("binding", ""))
            node_lines.append(
                f"| {direction} | `{_md(edge['type'])}` | {node_link(path, other)} | "
                f"`{_md(detail)}` |"
            )
        if not outgoing and not incoming:
            node_lines.append("| — | — | No declared cross-component relationship | — |")
        files[path] = ("\n".join(node_lines).rstrip() + "\n").encode()
        metadata[path] = {
            "kind": "relationship-component",
            "counts": {
                "contract_elements": node["contract_elements"],
                "incoming_edges": len(incoming),
                "outgoing_edges": len(outgoing),
            },
            "relationship_node_id": node["id"],
        }

    libraries = sorted(
        (
            item
            for item in component_nodes
            if item["role"] == "reusable_library" and str(item["path"]).startswith("packages/")
        ),
        key=lambda value: str(value["name"]),
    )
    library_lines = [
        "# Riverhog-owned reusable contracts and libraries",
        "",
        f"[Atlas]({_relative_link(library_path, root_path)}) · "
        f"[Relationships]({_relative_link(library_path, relationship_path)})",
        "",
        "These reusable release units live under the Riverhog package boundary. Their individual "
        "pages show exact dependency and extension relationships.",
        "",
        f"Components: **{len(libraries)}**",
        "",
        "| Component | Purpose | Owned contract elements |",
        "|---|---|---:|",
    ]
    for node in libraries:
        library_lines.append(
            f"| {node_link(library_path, node)} | {_md(node['description'])} | "
            f"{node['contract_elements']} |"
        )
    files[library_path] = ("\n".join(library_lines).rstrip() + "\n").encode()
    metadata[library_path] = {
        "kind": "relationship-library-index",
        "counts": {"components": len(libraries)},
    }

    extension_nodes = [
        item for item in nodes if item["kind"] in {"extension-point", "process-protocol"}
    ]
    extension_lines = [
        "# Independently implementable extension boundaries",
        "",
        f"[Atlas]({_relative_link(extension_path, root_path)}) · "
        f"[Relationships]({_relative_link(extension_path, relationship_path)})",
        "",
        "The owner defines the boundary. Checked-in providers are optional, nonnormative "
        "references and do not define an exhaustive implementation set.",
        "",
        f"Extension boundaries: **{len(extension_nodes)}**",
        "",
        "| Boundary | Kind | Owner | Binding support | Reference implementations |",
        "|---|---|---|---|---|",
    ]
    for node in sorted(extension_nodes, key=lambda value: str(value["name"])):
        node_edges = [edge for edge in edges if edge["target"] == node["id"]]
        owners = [
            by_id[str(edge["source"])]
            for edge in node_edges
            if str(edge["type"]).startswith("owns-")
        ]
        bindings = [
            by_id[str(edge["source"])] for edge in node_edges if edge["type"] == "binds-protocol"
        ]
        providers = [
            by_id[str(edge["source"])]
            for edge in node_edges
            if str(edge["type"]).startswith("implements-")
        ]
        provider_links = (
            ", ".join(node_link(extension_path, item) for item in providers) or "none checked in"
        )
        extension_lines.append(
            f"| `{_md(node['name'])}` | `{_md(node['kind'])}` | "
            f"{', '.join(node_link(extension_path, item) for item in owners)} | "
            f"{', '.join(node_link(extension_path, item) for item in bindings) or '—'} | "
            f"{provider_links} |"
        )
    files[extension_path] = ("\n".join(extension_lines).rstrip() + "\n").encode()
    metadata[extension_path] = {
        "kind": "relationship-extension-index",
        "counts": {"extension_boundaries": len(extension_nodes)},
    }

    runtime_nodes = sorted(
        (item for item in nodes if item["kind"] == "runtime-image"),
        key=lambda value: str(value["name"]),
    )
    runtime_lines = [
        "# Runtime-image composition",
        "",
        f"[Atlas]({_relative_link(runtime_path, root_path)}) · "
        f"[Relationships]({_relative_link(runtime_path, relationship_path)})",
        "",
        "Image purpose and composition are the exact release metadata; reference images remain "
        "optional and nonnormative.",
        "",
        f"Runtime images: **{len(runtime_nodes)}**",
        "",
        "| Image | Role | Purpose | Description source | Packaged components |",
        "|---|---|---|---|---|",
    ]
    for node in runtime_nodes:
        distributions = [
            by_id[str(edge["source"])]
            for edge in edges
            if edge["type"] == "packaged-in" and edge["target"] == node["id"]
        ]
        runtime_lines.append(
            f"| `{_md(node['name'])}` | `{_md(node['role'])}` | "
            f"{_md(node['description'])} | `{_md(node['description_source'])}` | "
            f"{', '.join(node_link(runtime_path, item) for item in distributions)} |"
        )
    files[runtime_path] = ("\n".join(runtime_lines).rstrip() + "\n").encode()
    metadata[runtime_path] = {
        "kind": "relationship-runtime-index",
        "counts": {"runtime_images": len(runtime_nodes)},
    }

    installed_edges = [edge for edge in edges if edge["type"] == "installed-as"]
    installation_lines = [
        "# Installed end-user and recovery surfaces",
        "",
        f"[Atlas]({_relative_link(installation_path, root_path)}) · "
        f"[Relationships]({_relative_link(installation_path, relationship_path)})",
        "",
        "These are the exact coordinated installation roots from the release contract.",
        "",
        f"Installation roots: **{len(installed_edges)}**",
        "",
        "| Component | Purpose | Installation method |",
        "|---|---|---|",
    ]
    for edge in installed_edges:
        node = by_id[str(edge["source"])]
        installation = by_id[str(edge["target"])]
        installation_lines.append(
            f"| {node_link(installation_path, node)} | {_md(node['description'])} | "
            f"`{_md(installation['name'])}` |"
        )
    files[installation_path] = ("\n".join(installation_lines).rstrip() + "\n").encode()
    metadata[installation_path] = {
        "kind": "relationship-installation-index",
        "counts": {"installation_roots": len(installed_edges)},
    }

    references = sorted(
        (
            item
            for item in component_nodes
            if item["role"] in {"reference_application", "reference_component"}
            or (item["role"] == "reusable_library" and str(item["path"]).startswith("reference/"))
        ),
        key=lambda value: (str(value["role"]), str(value["name"])),
    )
    reference_lines = [
        "# Nonnormative reference ecosystem",
        "",
        f"[Atlas]({_relative_link(reference_path, root_path)}) · "
        f"[Relationships]({_relative_link(reference_path, relationship_path)})",
        "",
        str(relationship["reference_policy"]),
        "",
        "These components exercise maintained boundaries; they do not make their implementation "
        "choices authoritative or promise an expanding implementation corpus.",
        "",
        f"Reference components and libraries: **{len(references)}**",
    ]
    for role in sorted({str(item["role"]) for item in references}):
        role_nodes = [item for item in references if item["role"] == role]
        reference_lines.extend(
            [
                "",
                f"## {role.replace('_', ' ').title()}",
                "",
                "| Component | Purpose |",
                "|---|---|",
            ]
        )
        for node in role_nodes:
            reference_lines.append(
                f"| {node_link(reference_path, node)} | {_md(node['description'])} |"
            )
    reference_images = [item for item in runtime_nodes if item["role"] == "reference"]
    reference_lines.extend(
        [
            "",
            "## Reference runtime images",
            "",
            f"Images: **{len(reference_images)}**",
            "",
            "| Image | Purpose |",
            "|---|---|",
            *(
                f"| `{_md(node['name'])}` | {_md(node['description'])} |"
                for node in reference_images
            ),
        ]
    )
    files[reference_path] = ("\n".join(reference_lines).rstrip() + "\n").encode()
    metadata[reference_path] = {
        "kind": "relationship-reference-index",
        "counts": {
            "reference_components": len(references),
            "reference_images": len(reference_images),
        },
    }
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
    for category, values in policies.items():
        policy_lines.extend(["", f"## {str(category).replace('_', ' ').title()}", ""])
        for policy in cast(Sequence[Mapping[str, object]], values):
            meaning = policy["meaning"]
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
                    f"### `{policy['id']}`",
                    "",
                    rendered_meaning,
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

    relationship = _relationship_model(projection, elements, component_descriptions)
    relationship_files, relationship_metadata = _render_relationships(relationship, elements)
    files.update(relationship_files)

    root_counts = _counts(elements, exclusions)
    root_path = f"{ATLAS_DIRECTORY}/index.md"
    exclusion_path = f"{ATLAS_DIRECTORY}/exclusions/index.md"
    evidence_path = f"{ATLAS_DIRECTORY}/evidence/index.md"
    policy_by_id = {
        str(policy["id"]): policy
        for values in policies.values()
        for policy in cast(Sequence[Mapping[str, object]], values)
    }
    exclusion_lines = [
        "# Explicitly excluded candidates",
        "",
        f"[Atlas]({_relative_link(exclusion_path, root_path)}) · "
        f"[Policies]({_relative_link(exclusion_path, policy_path)})",
        "",
        "These are every discovered delivery candidate intentionally excluded from the external "
        "CLI surface. The complete list is part of the discovery/disposition closure.",
        "",
        f"Excluded candidates: **{len(exclusions)}**",
        "",
        "| Candidate | Kind | Installed target | Boundary source | Detector | Policy and reason | "
        "Source authority |",
        "|---|---|---|---|---|---|---|",
    ]
    for exclusion in sorted(exclusions, key=lambda value: str(value["id"])):
        policy_id = str(exclusion["policy_id"])
        policy = policy_by_id[policy_id]
        source_links = ", ".join(
            f"`{_md(source)}`" for source in cast(Sequence[str], exclusion["source_authority_ids"])
        )
        exclusion_lines.append(
            f"| `{_md(exclusion['id'])}` | `{_md(exclusion['kind'])}` | "
            f"`{_md(exclusion['installed_target'])}` | "
            f"`{_md(exclusion['boundary_pointer'])}` | "
            f"`{_md(exclusion['detector'])}` | "
            f"`{_md(policy_id)}` — {_md(policy['meaning'])} | "
            f"{source_links} |"
        )
    files[exclusion_path] = ("\n".join(exclusion_lines).rstrip() + "\n").encode()

    source_index = _source_index(trace)
    source_counts = cast(Mapping[str, object], root_counts["by_source_authority"])
    evidence_lines = [
        "# Executable sources and qualification routes",
        "",
        f"[Atlas]({_relative_link(evidence_path, root_path)})",
        "",
        "Every dossier names its executable source authorities and applicable qualification "
        "routes. This index summarizes that exact proof routing without making implementation "
        "witnesses part of the semantic contract.",
        "",
        "## Qualification-route applications",
        "",
        *_table_counts(
            cast(Mapping[str, object], root_counts["by_qualification_route"]),
            "Qualification route",
        ),
        "",
        "## Source-authority applications",
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
        evidence_lines.append(f"| `{_md(source_id)}` | {count} | `{_md(rendered + symbol)}` |")
    files[evidence_path] = ("\n".join(evidence_lines).rstrip() + "\n").encode()

    anomalies = cast(Mapping[str, object], discovery["anomalies"])
    root_lines = [
        "# Riverhog v1 contract atlas",
        "",
        "This generated atlas is the human navigation of the exact monolithic machine closure in "
        "`../riverhog-v1.json`. It is organized by authority, interface, and native semantic "
        "dossier; "
        "no page boundary changes contract identity.",
        "",
        "## Closure status",
        "",
        "Status: **complete** — every discovered candidate has exactly one disposition and every "
        "contractual fact has exactly one human owner.",
        "",
        f"Contract elements: **{root_counts['contract_elements']}** · "
        f"Extent decisions: **{root_counts['extent_decisions']}** · "
        f"Excluded candidates: **{root_counts['excluded_candidates']}** · "
        f"Source authorities: **{len(source_index)}** · "
        f"Atlas documents: **{len(files) + 1}**",
        "",
        "### Closure anomalies",
        "",
        *_table_counts(anomalies, "Anomaly"),
        "",
        "### Independent evidence identities",
        "",
        "| Identity domain | SHA-256 |",
        "|---|---|",
        *(f"| `{_md(name)}` | `{_md(value)}` |" for name, value in identities.items()),
        "",
        "The byte-exact `atlas_representation_sha256` is recorded at "
        "`/identities/atlas_representation_sha256` in the machine closure. It cannot be embedded "
        "inside the document bytes that it identifies.",
        "",
        "## Audit navigation",
        "",
        "- [Relationship-aware boundary map](relationships/index.md)",
        f"- [Contract-policy registry]({_relative_link(root_path, policy_path)})",
        f"- [Explicit exclusions]({_relative_link(root_path, exclusion_path)})",
        "- [Executable sources and qualification routes]"
        f"({_relative_link(root_path, evidence_path)})",
        "",
        "## Aggregate contract shape",
        "",
        *_table_counts(cast(Mapping[str, object], root_counts["by_interface"]), "Interface"),
        "",
        *_table_counts(cast(Mapping[str, object], root_counts["by_detector"]), "Detector"),
        "",
        *_table_counts(
            cast(Mapping[str, object], root_counts["by_qualification_route"]),
            "Qualification route",
        ),
        "",
        "## Complete authority inventory",
        "",
        "The relationship map explains how these authorities interact. This flat inventory remains "
        "the exact completeness view.",
        "",
        "| Authority | Contract elements | Interfaces |",
        "|---|---:|---|",
    ]
    for authority, interfaces in sorted(grouped.items()):
        authority_path = f"{ATLAS_DIRECTORY}/authorities/{_slug(authority, limit=72)}/index.md"
        count = sum(len(values) for values in interfaces.values())
        root_lines.append(
            f"| [{_md(authority)}]({_relative_link(root_path, authority_path)}) | {count} | "
            f"{', '.join(f'`{name}`' for name in sorted(interfaces))} |"
        )
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
                "source_authorities": len(source_index),
                "qualification_routes": len(
                    cast(Mapping[str, object], root_counts["by_qualification_route"])
                ),
            }
            kind = "evidence-index"
        elif path in relationship_metadata:
            relationship_descriptor = relationship_metadata[path]
            document_counts = cast(dict[str, object], relationship_descriptor["counts"])
            kind = str(relationship_descriptor["kind"])
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
                        for key, value in relationship_metadata[path].items()
                        if key not in {"kind", "counts"}
                    }
                    if path in relationship_metadata
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
        if len(payload) > MAX_HUMAN_DOCUMENT_BYTES:
            raise ContractAtlasError(f"atlas document exceeds the human context budget: {path}")
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
    if root_page.index("## Aggregate contract shape") > root_page.index(
        "## Complete authority inventory"
    ):
        raise ContractAtlasError("atlas root must summarize before enumerating")
    for name, count in cast(Mapping[str, object], discovery["anomalies"]).items():
        if f"| `{name}` | {count} |" not in root_page:
            raise ContractAtlasError(f"atlas root omits closure anomaly: {name}")
    for name, identity in cast(Mapping[str, object], root["identities"]).items():
        if name == "atlas_representation_sha256":
            if f"/identities/{name}" not in root_page:
                raise ContractAtlasError("atlas root omits its representation-identity route")
        elif f"| `{name}` | `{identity}` |" not in root_page:
            raise ContractAtlasError(f"atlas root omits independent identity: {name}")

    exclusion_page = atlas.files[f"{ATLAS_DIRECTORY}/exclusions/index.md"].decode()
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

    evidence_page = atlas.files[f"{ATLAS_DIRECTORY}/evidence/index.md"].decode()
    for route in cast(
        Mapping[str, object], cast(Mapping[str, object], root["counts"])["by_qualification_route"]
    ):
        if f"`{route}`" not in evidence_page:
            raise ContractAtlasError(f"human evidence index omits route: {route}")
    for source_id, source in source_index.items():
        location = cast(Mapping[str, object], source.get("source", {}))
        rendered = str(location.get("path", location.get("module", source_id)))
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        if f"`{source_id}`" not in evidence_page or f"`{rendered}{symbol}`" not in evidence_page:
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
            projection_value, elements, component_descriptions
        )
        if relationship != expected_relationship:
            raise ContractAtlasError("atlas relationship navigation is stale")
    relationship_nodes = cast(Sequence[Mapping[str, object]], relationship["nodes"])
    relationship_edges = cast(Sequence[Mapping[str, object]], relationship["edges"])
    relationship_nodes_by_id = {str(item["id"]): item for item in relationship_nodes}
    relationship_files = {
        path: payload.decode()
        for path, payload in atlas.files.items()
        if path.startswith(f"{ATLAS_DIRECTORY}/relationships/")
    }
    relationship_text = "\n".join(relationship_files.values())
    for node in relationship_nodes:
        if _md(node["name"]) not in relationship_text:
            raise ContractAtlasError(f"human relationship navigation omits node: {node['id']}")
        if node["kind"] == "component":
            component_path = (
                f"{ATLAS_DIRECTORY}/relationships/components/"
                f"{_slug(str(node['name']), limit=72)}.md"
            )
            component_page = relationship_files.get(component_path, "")
            if any(
                value not in component_page
                for value in (
                    str(node["description"]),
                    f"`{_md(node['role'])}`",
                    f"`{_md(node['path'])}`",
                    f"{node['contract_elements']}",
                )
            ):
                raise ContractAtlasError(
                    f"human relationship component is incomplete: {node['id']}"
                )
            for edge in (item for item in relationship_edges if item["source"] == node["id"]):
                target = relationship_nodes_by_id[str(edge["target"])]
                detail = edge.get("scope", edge.get("binding", ""))
                if any(
                    value not in component_page
                    for value in (
                        f"`{_md(edge['type'])}`",
                        _md(target["name"]),
                        f"`{_md(detail)}`",
                    )
                ):
                    raise ContractAtlasError(
                        f"human relationship component omits edge: {node['id']}"
                    )
        elif node["kind"] == "runtime-image":
            runtime_page = relationship_files.get(
                f"{ATLAS_DIRECTORY}/relationships/runtime-images/index.md", ""
            )
            if any(
                _md(node[field]) not in runtime_page
                for field in ("name", "role", "description", "description_source")
            ):
                raise ContractAtlasError(
                    f"human relationship runtime inventory is incomplete: {node['id']}"
                )
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
                "source_authorities": len(source_index),
                "qualification_routes": len(
                    cast(Mapping[str, object], checked_counts["by_qualification_route"])
                ),
            }
        elif kind == "relationship-index":
            expected_counts = {
                "nodes": len(relationship_nodes),
                "edges": len(relationship_edges),
            }
        elif kind == "relationship-component-index":
            component_nodes = [item for item in relationship_nodes if item["kind"] == "component"]
            expected_counts = {
                "components": len(component_nodes),
                "roles": len({str(item["role"]) for item in component_nodes}),
            }
        elif kind == "relationship-component":
            node = relationship_nodes_by_id[str(descriptor["relationship_node_id"])]
            expected_counts = {
                "contract_elements": node["contract_elements"],
                "incoming_edges": sum(edge["target"] == node["id"] for edge in relationship_edges),
                "outgoing_edges": sum(edge["source"] == node["id"] for edge in relationship_edges),
            }
        elif kind == "relationship-library-index":
            expected_counts = {
                "components": sum(
                    item["kind"] == "component"
                    and item.get("role") == "reusable_library"
                    and str(item.get("path", "")).startswith("packages/")
                    for item in relationship_nodes
                )
            }
        elif kind == "relationship-extension-index":
            expected_counts = {
                "extension_boundaries": sum(
                    item["kind"] in {"extension-point", "process-protocol"}
                    for item in relationship_nodes
                )
            }
        elif kind == "relationship-runtime-index":
            expected_counts = {
                "runtime_images": sum(
                    item["kind"] == "runtime-image" for item in relationship_nodes
                )
            }
        elif kind == "relationship-installation-index":
            expected_counts = {
                "installation_roots": sum(
                    item["type"] == "installed-as" for item in relationship_edges
                )
            }
        elif kind == "relationship-reference-index":
            expected_counts = {
                "reference_components": sum(
                    item["kind"] == "component"
                    and (
                        item.get("role") in {"reference_application", "reference_component"}
                        or (
                            item.get("role") == "reusable_library"
                            and str(item.get("path", "")).startswith("reference/")
                        )
                    )
                    for item in relationship_nodes
                ),
                "reference_images": sum(
                    item["kind"] == "runtime-image" and item.get("role") == "reference"
                    for item in relationship_nodes
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

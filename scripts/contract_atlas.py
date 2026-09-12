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
    encoded = json.dumps(
        values[0] if len(values) == 1 else list(values),
        indent=2,
        ensure_ascii=False,
        sort_keys=True,
    ).replace("](", "]\\u0028")
    if len(encoded.encode()) <= 12 * 1024:
        return ["```json", encoded, "```"]
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
    return [
        f"This semantic unit contains {_shape_summary(large_value)}. Its exact value is in the "
        "machine closure at the pointers above."
    ]


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
    lines.extend(["## Contract", ""])
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
    return ("\n".join(lines).rstrip() + "\n").encode()


def _table_counts(values: Mapping[str, object], label: str) -> list[str]:
    return [
        f"| {label} | Count |",
        "|---|---:|",
        *(f"| `{_md(key)}` | {value} |" for key, value in values.items()),
    ]


def _render_atlas(
    elements: list[dict[str, object]],
    policies: Mapping[str, object],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    identities: Mapping[str, object],
    exclusions: Sequence[Mapping[str, object]],
) -> tuple[dict[str, bytes], list[dict[str, object]]]:
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
                *_table_counts(cast(Mapping[str, object], interface_counts["by_policy"]), "Policy"),
                "",
                "## Semantic dossiers",
                "",
                "| Dossier | Family | Extent decisions |",
                "|---|---|---:|",
            ]
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

    root_counts = _counts(elements, exclusions)
    root_path = f"{ATLAS_DIRECTORY}/index.md"
    root_lines = [
        "# Riverhog v1 contract atlas",
        "",
        "This generated atlas is the human navigation of the exact monolithic machine closure in "
        "`../riverhog-v1.json`. It is organized by authority, interface, and native semantic "
        "dossier; "
        "no page boundary changes contract identity.",
        "",
        f"Semantic contract: `{identities['semantic_contract_sha256']}`",
        "",
        f"Contract elements: **{root_counts['contract_elements']}** · "
        f"Extent decisions: **{root_counts['extent_decisions']}** · "
        f"Excluded candidates: **{root_counts['excluded_candidates']}**",
        "",
        f"[Policy registry]({_relative_link(root_path, policy_path)})",
        "",
        "## Authorities",
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
    root_lines.extend(
        [
            "",
            "## Aggregate interface counts",
            "",
            *_table_counts(cast(Mapping[str, object], root_counts["by_interface"]), "Interface"),
        ]
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
            }
        )
    return files, descriptors


def build_atlas(projection: Mapping[str, object], trace: Mapping[str, object]) -> ContractAtlas:
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
    files, documents = _render_atlas(
        elements,
        policies,
        normalized_projection,
        normalized_trace,
        identities,
        exclusions,
    )
    representation_identity = {
        "schema": REPRESENTATION_IDENTITY_SCHEMA,
        "documents": documents,
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
        },
    }
    atlas = ContractAtlas(root=root, files=files)
    validate_atlas(atlas, projection=projection, trace=trace)
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
            pointer_value(root["projection"], pointer)

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

    elements_by_id = {str(item["id"]): item for item in elements}
    for path, descriptor in descriptors.items():
        kind = descriptor["kind"]
        if kind == "root-index":
            expected_counts: Mapping[str, object] = _counts(elements, exclusions)
        elif kind == "policy-index":
            expected_counts = {
                "policies": sum(len(cast(Sequence[object], value)) for value in policies.values())
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
            expected_counts = _counts(subset)
        if descriptor["counts"] != expected_counts:
            raise ContractAtlasError(f"atlas roll-up count is stale: {path}")

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
        "documents": cast(Mapping[str, object], root["atlas"])["documents"],
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

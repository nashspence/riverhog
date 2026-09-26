"""Typed internal model and canonical closure primitives for Riverhog contract audit."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from riverhog_canonical_json import CanonicalJsonError
from riverhog_canonical_json import canonical_json_bytes as jcs_bytes

ROOT_FORMAT = "riverhog-contract-discovery/v1"
COVERAGE_IDENTITY_FORMAT = "riverhog-v1-discovery-coverage/v1"
TRACE_IDENTITY_FORMAT = "riverhog-v1-source-proof-trace/v1"
DETECTOR_CLOSURE_FORMAT = "riverhog-contract-detector-closure/v1"

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


class ContractAtlasError(RuntimeError):
    """The generated closure or atlas is incomplete, ambiguous, or stale."""


@dataclass(frozen=True)
class DiscoveredContract:
    """Internal source-derived contract candidates and audit associations."""

    root: dict[str, object]


@dataclass(frozen=True)
class InterfaceDescriptor:
    """Closed internal ownership record for one externally visible interface kind."""

    identity: str
    label: str
    purpose: str
    order: int
    qualification_routes: tuple[str, ...]
    navigation_provider: str
    renderer: str


@dataclass(frozen=True)
class ContractElement:
    """Stable typed identity of one canonical JSON contract element."""

    identity: str
    authority: str
    interface: str
    title: str
    pointers: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> ContractElement:
        return cls(
            identity=str(value["id"]),
            authority=str(value["authority"]),
            interface=str(value["interface"]),
            title=str(value["title"]),
            pointers=tuple(str(item) for item in cast(Sequence[object], value["pointers"])),
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


def canonical_bytes(value: object) -> bytes:
    """Return RFC 8785 canonical JSON bytes."""

    def json_value(current: object) -> object:
        if isinstance(current, Mapping):
            return {key: json_value(child) for key, child in current.items()}
        if isinstance(current, (list, tuple)):
            return [json_value(child) for child in current]
        return current

    try:
        return jcs_bytes(json_value(value))
    except CanonicalJsonError as exc:
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


_INTERFACE_DESCRIPTORS = (
    InterfaceDescriptor(
        "artifact-verification",
        "Artifact Verification",
        "External verification mechanisms for published artifacts.",
        5,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
    InterfaceDescriptor(
        "cli",
        "CLI",
        "{label} contract owned by {authority}.",
        17,
        ("make dist-smoke", "make operation-qualification"),
        "cli",
        "cli",
    ),
    InterfaceDescriptor(
        "compatibility-guarantees",
        "Compatibility Guarantees",
        "Coordinated v1 compatibility promises.",
        7,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
    InterfaceDescriptor(
        "configuration",
        "Configuration Documents",
        "{label} contract owned by {authority}.",
        17,
        ("make unit", "make compose-smoke"),
        "configuration",
        "schema",
    ),
    InterfaceDescriptor(
        "configuration-environment",
        "Configuration Environment",
        "{label} contract owned by {authority}.",
        17,
        ("make unit", "make compose-smoke"),
        "atomic",
        "generic",
    ),
    InterfaceDescriptor(
        "durable-state",
        "Durable State",
        "Persisted structures, schema heads, and v1 transition obligations.",
        17,
        ("make release-check", "make database-qualification"),
        "durable-state",
        "durable-state",
    ),
    InterfaceDescriptor(
        "extent",
        "Extent Contract",
        "{label} contract owned by {authority}.",
        17,
        ("make contract-freeze", "make operation-qualification"),
        "extent",
        "generic",
    ),
    InterfaceDescriptor(
        "http-operations",
        "HTTP Operations",
        "Callable HTTP operations.",
        8,
        ("make operation-qualification", "make compose-smoke"),
        "atomic",
        "http-operation",
    ),
    InterfaceDescriptor(
        "http-schemas",
        "HTTP Schemas",
        "Supporting HTTP data definitions; these are not callable operations.",
        9,
        ("make operation-qualification", "make compose-smoke"),
        "http-schema",
        "schema",
    ),
    InterfaceDescriptor(
        "http-service-declaration",
        "HTTP Service Declaration",
        "The HTTP service format and identity declaration; this is not a callable operation.",
        10,
        ("make operation-qualification", "make compose-smoke"),
        "http-service",
        "generic",
    ),
    InterfaceDescriptor(
        "http-security-schemes",
        "HTTP Security Schemes",
        "Supporting HTTP authorization definitions; these are not callable operations.",
        11,
        ("make operation-qualification", "make compose-smoke"),
        "http-security",
        "generic",
    ),
    InterfaceDescriptor(
        "installation-roots",
        "Installation Roots",
        "Maintained installation roots and their exact installation forms.",
        2,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
    InterfaceDescriptor(
        "process-protocol",
        "Process Protocol",
        "Cross-participant protocol identity, compatibility, and common acceptance rules. "
        "Exact operations and schemas are separately owned below.",
        12,
        ("make dist-smoke", "make build"),
        "process-protocol",
        "generic",
    ),
    InterfaceDescriptor(
        "process-protocol-operations",
        "Process Protocol Operations",
        "Callable operations in an independently deployed process protocol; method and path "
        "describe that protocol binding, not a product/service HTTP API.",
        13,
        ("make dist-smoke", "make build"),
        "atomic",
        "operation",
    ),
    InterfaceDescriptor(
        "process-protocol-schemas",
        "Process Protocol Schemas",
        "Structured values exchanged by a process protocol.",
        14,
        ("make dist-smoke", "make build"),
        "process-schema",
        "schema",
    ),
    InterfaceDescriptor(
        "publication-locations",
        "Publication Locations",
        "Stable externally used publication locations.",
        4,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
    InterfaceDescriptor(
        "publication-policies",
        "Publication Policies",
        "Rules governing published distribution, installation, and image identities.",
        6,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
    InterfaceDescriptor(
        "python",
        "Python",
        "Declared public imports and their selected exact structural contracts.",
        17,
        ("make dist-smoke", "make build"),
        "python",
        "python",
    ),
    InterfaceDescriptor(
        "python-distributions",
        "Python Distributions",
        "Published Python distribution identities and artifact forms.",
        1,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
    InterfaceDescriptor(
        "release-artifacts",
        "Release Artifacts",
        "Discrete files published with a coordinated release.",
        3,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
    InterfaceDescriptor(
        "runtime-images",
        "Runtime Images",
        "Published OCI runtime-image identities.",
        0,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
    InterfaceDescriptor(
        "schema",
        "Schemas",
        "Standalone structured-value contracts; these do not define an interaction.",
        16,
        ("make dist-smoke", "make build"),
        "atomic",
        "schema",
    ),
    InterfaceDescriptor(
        "versioning-tags",
        "Versioning and Tags",
        "Coordinated version and immutable-tag semantics.",
        6,
        ("make release-check", "make dist-smoke", "make build"),
        "release",
        "release",
    ),
)
INTERFACE_REGISTRY = {item.identity: item for item in _INTERFACE_DESCRIPTORS}
if len(INTERFACE_REGISTRY) != len(_INTERFACE_DESCRIPTORS):
    raise RuntimeError("contract-atlas interface descriptors repeat an identity")
QUALIFICATION_ROUTES = {
    key: value.qualification_routes for key, value in INTERFACE_REGISTRY.items()
}


def reassemble_projection(discovered: DiscoveredContract) -> dict[str, object]:
    return cast(
        dict[str, object],
        _decode_unsafe_integers(
            discovered.root["projection"],
            cast(Sequence[str], discovered.root["projection_unsafe_integer_paths"]),
        ),
    )


def reassemble_trace(discovered: DiscoveredContract) -> dict[str, object]:
    return cast(
        dict[str, object],
        _decode_unsafe_integers(
            discovered.root["trace"],
            cast(Sequence[str], discovered.root["trace_unsafe_integer_paths"]),
        ),
    )

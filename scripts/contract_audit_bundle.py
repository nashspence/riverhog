#!/usr/bin/env python3
"""Bounded, lossless representation for the Riverhog contract audit authority."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, cast

import rfc8785

ROOT_SCHEMA = "riverhog-contract-audit-bundle/v1"
CONTEXT_SCHEMA = "riverhog-contract-audit-context/v1"
CONTEXT_TRACE_SCHEMA = "riverhog-contract-audit-context-trace/v1"
CONTRACT_IDENTITY_SCHEMA = "riverhog-external-contract-facts/v1"
COVERAGE_IDENTITY_SCHEMA = "riverhog-contract-discovery-coverage/v1"
TRACE_IDENTITY_SCHEMA = "riverhog-contract-audit-trace/v1"
REPRESENTATION_IDENTITY_SCHEMA = "riverhog-contract-bundle-representation/v1"
DETECTOR_CLOSURE_SCHEMA = "riverhog-contract-detector-closure/v1"

ROOT_MAX_BYTES = 64 * 1024
CONTEXT_MAX_BYTES = 32 * 1024
# A fact is a semantic JSON subtree, never a byte slice. This leaves room for the
# context envelope and neighboring facts while preserving large natural units.
FACT_VALUE_MAX_BYTES = 24 * 1024
CONTEXT_COLUMNS: tuple[str, ...] = (
    "id",
    "owner",
    "kind",
    "policies",
    "dispositions",
    "normative",
    "trace",
)

CONTRACT_POLICIES: tuple[dict[str, str], ...] = (
    {
        "id": "external-contract-fact/v1",
        "meaning": (
            "The fact is part of the exact maintained Riverhog-repository v1 external contract."
        ),
        "scope": "All external-contract audit contexts unless a narrower policy is also listed.",
        "inheritance": "Inherited by every fact in the owning context.",
    },
    {
        "id": "external-extent-decision/v1",
        "meaning": (
            "The fact declares an intentional extent classification and its executable authority."
        ),
        "scope": "External-contract extent contexts.",
        "inheritance": (
            "Inherited by every fact in an extent context in addition to external-contract-fact/v1."
        ),
    },
)

EXCLUSION_POLICIES: tuple[dict[str, str], ...] = (
    {
        "id": "process-launcher-not-cli/v1",
        "meaning": (
            "The installed entry point only starts a separately inventoried process protocol and "
            "has no independent CLI contract."
        ),
        "scope": "Installed service, adapter, observer, target, sampler, and effect launchers.",
        "inheritance": "Never inherited; each excluded launcher candidate names it explicitly.",
    },
)

DETECTORS: tuple[dict[str, str], ...] = (
    {"id": "cli-tree", "candidate_kind": "cli", "authority": "installed parser tree"},
    {
        "id": "configuration-document",
        "candidate_kind": "configuration",
        "authority": "validated configuration schema",
    },
    {
        "id": "configuration-environment",
        "candidate_kind": "configuration-environment",
        "authority": "executable environment binding",
    },
    {
        "id": "durable-state",
        "candidate_kind": "durable-state",
        "authority": "checked current state baseline",
    },
    {
        "id": "extent",
        "candidate_kind": "extent",
        "authority": "exhaustive extent classifier",
    },
    {"id": "http-openapi", "candidate_kind": "http", "authority": "running ASGI app"},
    {
        "id": "operation-matrix",
        "candidate_kind": "operation",
        "authority": "executable operation parity matrix",
    },
    {
        "id": "protocol-schema",
        "candidate_kind": "protocol",
        "authority": "published or generated protocol schema",
    },
    {
        "id": "python-export",
        "candidate_kind": "python",
        "authority": "published reusable-library export",
    },
    {
        "id": "release-metadata",
        "candidate_kind": "release",
        "authority": "validated release contract",
    },
)

QUALIFICATION_ROUTES: dict[str, tuple[str, ...]] = {
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
    "boundary": ("make release-check", "make build"),
    "projection-trace": ("make contract-freeze",),
    "excluded": ("make dist-smoke", "make build"),
}


class AuditBundleError(RuntimeError):
    """The generated audit bundle is incomplete, ambiguous, or over budget."""


@dataclass(frozen=True)
class AuditBundle:
    """One root and all exact root-referenced context files."""

    root: dict[str, object]
    files: dict[str, bytes]


def canonical_bytes(value: object) -> bytes:
    """Return RFC 8785 canonical JSON bytes."""

    try:
        return rfc8785.dumps(cast(Any, value))
    except (rfc8785.CanonicalizationError, TypeError) as exc:
        raise AuditBundleError(f"value is not RFC 8785 canonicalizable: {exc}") from exc


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _escape_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _pointer_parts(pointer: str) -> list[str]:
    if not pointer:
        return []
    if not pointer.startswith("/"):
        raise AuditBundleError(f"invalid JSON pointer: {pointer}")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def _fact_id(scope: str, pointer: str) -> str:
    digest = hashlib.sha256(f"{scope}\0{pointer}".encode()).hexdigest()[:24]
    return f"fact:{scope}:{digest}"


def _encode_noncanonical_numbers(value: object) -> tuple[object, list[str], list[str]]:
    """Encode number forms RFC 8785 cannot preserve without losing their JSON type."""

    integer_paths: list[str] = []
    float_paths: list[str] = []

    def encode(current: object, pointer: str) -> object:
        if isinstance(current, bool):
            return current
        if isinstance(current, int) and not (-(2**53) + 1 <= current <= (2**53) - 1):
            integer_paths.append(pointer)
            return str(current)
        if isinstance(current, float) and current.is_integer():
            float_paths.append(pointer)
            return str(current)
        if isinstance(current, Mapping):
            return {
                str(key): encode(child, f"{pointer}/{_escape_pointer(str(key))}")
                for key, child in current.items()
            }
        if isinstance(current, list):
            return [encode(child, f"{pointer}/{index}") for index, child in enumerate(current)]
        return current

    return encode(value, ""), integer_paths, float_paths


def _decode_unsafe_integers(value: object, paths: Sequence[str]) -> object:
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
                raise AuditBundleError("encoded root integer is not decimal text")
            value = int(current)
            continue
        terminal = parts[-1]
        if isinstance(current, list):
            encoded = current[int(terminal)]
            if not isinstance(encoded, str):
                raise AuditBundleError(f"encoded integer is not decimal text at {pointer}")
            current[int(terminal)] = int(encoded)
        else:
            parent = cast(dict[str, object], current)
            encoded = parent[terminal]
            if not isinstance(encoded, str):
                raise AuditBundleError(f"encoded integer is not decimal text at {pointer}")
            parent[terminal] = int(encoded)
    return value


def _decode_integral_floats(value: object, paths: Sequence[str]) -> object:
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
                raise AuditBundleError("encoded root float is not decimal text")
            value = float(current)
            continue
        terminal = parts[-1]
        if isinstance(current, list):
            encoded = current[int(terminal)]
            if not isinstance(encoded, str):
                raise AuditBundleError(f"encoded float is not decimal text at {pointer}")
            current[int(terminal)] = float(encoded)
        else:
            parent = cast(dict[str, object], current)
            encoded = parent[terminal]
            if not isinstance(encoded, str):
                raise AuditBundleError(f"encoded float is not decimal text at {pointer}")
            parent[terminal] = float(encoded)
    return value


def semantic_facts(value: object, *, scope: str) -> list[dict[str, object]]:
    """Decompose JSON into bounded semantic subtrees, never encoded byte fragments."""

    facts: list[dict[str, object]] = []

    def visit(current: object, pointer: str) -> None:
        encoded, integer_paths, float_paths = _encode_noncanonical_numbers(current)
        candidate = {
            "id": _fact_id(scope, pointer),
            "kind": "value",
            "pointer": pointer,
            "value": encoded,
            **({"integer_paths": integer_paths} if integer_paths else {}),
            **({"float_paths": float_paths} if float_paths else {}),
        }
        if len(canonical_bytes(candidate)) <= FACT_VALUE_MAX_BYTES:
            facts.append(candidate)
            return
        if isinstance(current, Mapping):
            facts.append(
                {
                    "id": _fact_id(scope, pointer),
                    "kind": "object",
                    "pointer": pointer,
                }
            )
            for key, child in sorted(current.items()):
                if not isinstance(key, str):
                    raise AuditBundleError(f"non-string mapping key at {pointer}")
                visit(child, f"{pointer}/{_escape_pointer(key)}")
            return
        if isinstance(current, list):
            facts.append(
                {
                    "id": _fact_id(scope, pointer),
                    "kind": "array",
                    "pointer": pointer,
                }
            )
            for index, child in enumerate(current):
                visit(child, f"{pointer}/{index}")
            return
        raise AuditBundleError(f"single semantic value exceeds the fact budget at {pointer}")

    visit(value, "")
    identities = [str(fact["id"]) for fact in facts]
    pointers = [str(fact["pointer"]) for fact in facts]
    if len(identities) != len(set(identities)) or len(pointers) != len(set(pointers)):
        raise AuditBundleError(f"{scope} fact identities are not unique")
    return sorted(facts, key=lambda fact: str(fact["pointer"]))


def reassemble_facts(facts: Sequence[Mapping[str, object]]) -> object:
    """Reassemble an exact logical JSON value from semantic facts."""

    root: object | None = None
    initialized = False

    def pointer_order(item: Mapping[str, object]) -> tuple[int, tuple[tuple[int, object], ...]]:
        parts = _pointer_parts(str(item["pointer"]))
        ordered = tuple((0, int(part)) if part.isdigit() else (1, part) for part in parts)
        return len(parts), ordered

    for fact in sorted(facts, key=pointer_order):
        pointer = str(fact["pointer"])
        kind = str(fact["kind"])
        value: object
        if kind == "value":
            value = _decode_unsafe_integers(
                fact["value"], cast(Sequence[str], fact.get("integer_paths", ()))
            )
            value = _decode_integral_floats(value, cast(Sequence[str], fact.get("float_paths", ())))
        elif kind == "object":
            value = {}
        elif kind == "array":
            value = []
        else:
            raise AuditBundleError(f"unknown fact kind: {kind}")
        parts = _pointer_parts(pointer)
        if not parts:
            if initialized:
                raise AuditBundleError("logical root is represented more than once")
            root = value
            initialized = True
            continue
        if not initialized:
            raise AuditBundleError("logical root fact is absent")
        parent = root
        for part in parts[:-1]:
            if isinstance(parent, list):
                index = int(part)
                if index >= len(parent):
                    raise AuditBundleError(f"missing array container before {pointer}")
                parent = parent[index]
            elif isinstance(parent, dict):
                if part not in parent:
                    raise AuditBundleError(f"missing object container before {pointer}")
                parent = parent[part]
            else:
                raise AuditBundleError(f"non-container parent before {pointer}")
        terminal = parts[-1]
        if isinstance(parent, list):
            index = int(terminal)
            if index != len(parent):
                raise AuditBundleError(f"array facts are not contiguous at {pointer}")
            parent.append(value)
        elif isinstance(parent, dict):
            if terminal in parent:
                raise AuditBundleError(f"logical pointer is represented more than once: {pointer}")
            parent[terminal] = value
        else:
            raise AuditBundleError(f"non-container parent at {pointer}")
    if not initialized:
        raise AuditBundleError("logical fact set is empty")
    return root


def _slug(value: str) -> str:
    rendered = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return rendered[:48] or "root"


def _lookup_sequence(value: object, pointer: str, index: int) -> Mapping[str, object] | None:
    parts = _pointer_parts(pointer)
    if len(parts) <= index:
        return None
    current = value
    for part in parts[: index + 1]:
        if isinstance(current, list):
            current = current[int(part)]
        elif isinstance(current, Mapping):
            current = current[part]
        else:
            return None
    return cast(Mapping[str, object], current) if isinstance(current, Mapping) else None


def _contract_coordinates(
    fact: Mapping[str, object], external: Mapping[str, object]
) -> tuple[str, str, str, tuple[str, ...], tuple[str, ...]]:
    pointer = str(fact["pointer"])
    parts = _pointer_parts(pointer)
    section = parts[0] if parts else "external_contract"
    policies = ["external-contract-fact/v1"]
    source_ids = ["generator:contract-projection"]
    if section == "http_openapi":
        owner = parts[1] if len(parts) > 1 else "http"
        return (
            owner,
            "http",
            "http-openapi",
            tuple(policies),
            tuple([*source_ids, f"openapi:{owner}"] if len(parts) > 1 else source_ids),
        )
    if section == "operations":
        operation = _lookup_sequence(external, pointer, 1)
        owner = str(operation.get("application", "operations")) if operation else "operations"
        return (
            owner,
            "operation",
            "operation-matrix",
            tuple(policies),
            tuple([*source_ids, "operations:operation-matrix"]),
        )
    if section == "cli":
        authority = parts[1] if len(parts) > 1 else "cli"
        return (
            "installed-cli",
            "cli",
            "cli-tree",
            tuple(policies),
            tuple([*source_ids, f"cli:{authority}"] if len(parts) > 1 else source_ids),
        )
    if section == "configuration_documents":
        owner = parts[1] if len(parts) > 1 else "configuration"
        return (
            owner,
            "configuration",
            "configuration-document",
            tuple(policies),
            tuple([*source_ids, f"configuration:{owner}"] if len(parts) > 1 else source_ids),
        )
    if section in {"configuration_environment", "configuration_environment_patterns"}:
        environment = _lookup_sequence(external, pointer, 1)
        name = str(environment.get("name", "inventory")) if environment else "inventory"
        specific = f"configuration-environment:{name}"
        return (
            "configuration",
            "configuration-environment",
            "configuration-environment",
            tuple(policies),
            tuple([*source_ids, specific]),
        )
    if section == "protocol_schemas":
        authority = parts[1] if len(parts) > 1 else "protocol"
        return (
            "protocol-authorities",
            "protocol",
            "protocol-schema",
            tuple(policies),
            tuple([*source_ids, f"protocol:{authority}"] if len(parts) > 1 else source_ids),
        )
    if section == "python":
        surface = _lookup_sequence(external, pointer, 1)
        owner = str(surface.get("distribution", "python")) if surface else "python"
        module = str(surface.get("module", "")) if surface else ""
        specific = f"python:{owner}:{module}" if module else f"python:{owner}"
        return (
            "published-python",
            "python",
            "python-export",
            tuple(policies),
            tuple([*source_ids, specific]),
        )
    if section == "durable_state":
        owner = "durable-state"
        if len(parts) > 2 and parts[1] == "owners":
            state_owner = _lookup_sequence(external, pointer, 2)
            if state_owner:
                owner = str(state_owner.get("id", owner))
        return (
            owner,
            "durable-state",
            "durable-state",
            tuple(policies),
            tuple([*source_ids, f"state:{owner}"]),
        )
    if section == "extents":
        policies.append("external-extent-decision/v1")
        owner = "extent-contract"
        return (
            owner,
            "extent",
            "extent",
            tuple(policies),
            tuple([*source_ids, "extent:extent-contract"]),
        )
    return (
        "release",
        "release",
        "release-metadata",
        tuple(policies),
        tuple([*source_ids, "release:release.toml"]),
    )


def _boundary_coordinates(
    fact: Mapping[str, object], boundaries: Mapping[str, object]
) -> tuple[str, str]:
    pointer = str(fact["pointer"])
    parts = _pointer_parts(pointer)
    kind = parts[0].replace("_", "-") if parts else "boundary"
    owner = "repository"
    if len(parts) > 1 and parts[0] == "components":
        component = _lookup_sequence(boundaries, pointer, 1)
        if component:
            owner = str(component.get("distribution", owner))
    elif len(parts) > 1 and parts[0] == "runtime_images":
        owner = parts[1]
    elif len(parts) > 1 and parts[0] in {"entry_point_extensions", "process_extensions"}:
        item = _lookup_sequence(boundaries, pointer, 1)
        if item:
            owner = str(item.get("owner", item.get("contract_owner", owner)))
    return owner, f"boundary-{kind}"


def _trace_coordinates(fact: Mapping[str, object], trace: Mapping[str, object]) -> tuple[str, str]:
    pointer = str(fact["pointer"])
    parts = _pointer_parts(pointer)
    if len(parts) > 1 and parts[0] == "sources":
        return "repository", "projection-source-trace"
    if len(parts) > 1 and parts[0] == "extent_sources":
        return "repository", "projection-extent-trace"
    if len(parts) > 1 and parts[0] == "segmented_extent_witnesses":
        return "extent-contract", "projection-witness-trace"
    return "repository", "projection-trace-metadata"


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
    return sources


def _resolve_sources(
    source_ids: Iterable[str], sources: Mapping[str, dict[str, object]]
) -> list[dict[str, object]]:
    resolved: list[dict[str, object]] = []
    for identity in sorted(set(source_ids)):
        source = sources.get(identity)
        if source is None:
            # Parent container facts deliberately resolve through their generator;
            # leaf facts add their exact executable authority where one exists.
            continue
        compact = {"id": source["id"]}
        for field in ("source", "fixtures", "bindings"):
            if field in source:
                compact[field] = source[field]
        # Route inventories remain losslessly retained in projection-trace
        # contexts. The paired audit unit needs the exact owning executable,
        # not a repeated copy of every sibling route.
        resolved.append(compact)
    return resolved


def _candidate_for(fact: Mapping[str, object], *, detector: str, kind: str) -> dict[str, object]:
    fact_id = str(fact["id"])
    return {
        "id": f"candidate:{fact_id.removeprefix('fact:')}",
        "kind": kind,
        "detector": detector,
        "disposition": "contractual",
        "fact_id": fact_id,
    }


def _candidate_trace(
    candidate: Mapping[str, object], fact: Mapping[str, object], source_ids: Sequence[str]
) -> dict[str, object]:
    return {
        "candidate_id": candidate["id"],
        "fact_id": candidate["fact_id"],
        "projection_pointer": f"/external_contract{fact['pointer']}",
        "source_authority_ids": sorted(set(source_ids)),
    }


def _context_payload(
    identity: str,
    *,
    scope: str,
    owner: str,
    kind: str,
    policies: Sequence[str],
    facts: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    return {
        "schema": CONTEXT_SCHEMA,
        "id": identity,
        "scope": scope,
        "owner": owner,
        "kind": kind,
        "policies": list(policies),
        "facts": list(facts),
    }


def _trace_payload(
    identity: str,
    *,
    kind: str,
    candidates: Sequence[Mapping[str, object]] = (),
    candidate_sources: Sequence[Mapping[str, object]] = (),
    source_authorities: Sequence[Mapping[str, object]] = (),
    projection_trace_facts: Sequence[Mapping[str, object]] = (),
) -> dict[str, object]:
    return {
        "schema": CONTEXT_TRACE_SCHEMA,
        "context_id": identity,
        "candidates": list(candidates),
        "candidate_sources": list(candidate_sources),
        "source_authorities": list(source_authorities),
        "projection_trace_facts": list(projection_trace_facts),
        "qualification_routes": list(QUALIFICATION_ROUTES[kind]),
    }


def _descriptor(
    identity: str,
    *,
    owner: str,
    kind: str,
    scope: str,
    policies: Sequence[str],
    normative: tuple[str, bytes, int] | None,
    trace: tuple[str, bytes, int],
    candidates: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    trace_path, trace_bytes, trace_facts = trace
    result: dict[str, object] = {
        "id": identity,
        "owner": owner,
        "kind": kind,
        "scope": scope,
        "policies": list(policies),
        "trace": {
            "path": trace_path,
            "bytes": len(trace_bytes),
            "sha256": hashlib.sha256(trace_bytes).hexdigest(),
            "facts": trace_facts,
            "candidates": len(candidates),
        },
        "dispositions": sorted({str(candidate["disposition"]) for candidate in candidates}),
    }
    if normative is not None:
        path, payload, facts = normative
        result["normative"] = {
            "path": path,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "facts": facts,
        }
    return result


def _pack_normative_group(
    *,
    scope: str,
    owner: str,
    kind: str,
    policies: Sequence[str],
    detector: str | None,
    items: Sequence[tuple[dict[str, object], tuple[str, ...]]],
    sources: Mapping[str, dict[str, object]],
) -> list[tuple[dict[str, object], dict[str, object], list[dict[str, object]]]]:
    packed: list[tuple[dict[str, object], dict[str, object], list[dict[str, object]]]] = []
    current: list[tuple[dict[str, object], tuple[str, ...]]] = []

    def payload(
        selected: Sequence[tuple[dict[str, object], tuple[str, ...]]], index: int
    ) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
        group = f"{scope}\0{kind}\0{owner}"
        identity = f"c-{hashlib.sha256(group.encode()).hexdigest()[:12]}-{index:03d}"
        facts = [fact for fact, _source_ids in selected]
        candidates = (
            [_candidate_for(fact, detector=detector, kind=kind) for fact in facts]
            if detector is not None
            else []
        )
        candidate_sources = (
            [
                _candidate_trace(candidate, fact, source_ids)
                for candidate, (fact, source_ids) in zip(candidates, selected, strict=True)
            ]
            if candidates
            else []
        )
        source_ids = [identity for _fact, identities in selected for identity in identities]
        normative = _context_payload(
            identity,
            scope=scope,
            owner=owner,
            kind=kind,
            policies=policies,
            facts=facts,
        )
        traced = _trace_payload(
            identity,
            kind=kind if kind in QUALIFICATION_ROUTES else "boundary",
            candidates=candidates,
            candidate_sources=candidate_sources,
            source_authorities=_resolve_sources(source_ids, sources),
        )
        return normative, traced, candidates

    for item in items:
        proposed = [*current, item]
        normative, traced, _candidates = payload(proposed, len(packed) + 1)
        if (
            current
            and max(len(canonical_bytes(normative)), len(canonical_bytes(traced)))
            > CONTEXT_MAX_BYTES
        ):
            packed.append(payload(current, len(packed) + 1))
            current = [item]
        else:
            current = proposed
    if current:
        packed.append(payload(current, len(packed) + 1))
    for normative, traced, _candidates in packed:
        if max(len(canonical_bytes(normative)), len(canonical_bytes(traced))) > CONTEXT_MAX_BYTES:
            raise AuditBundleError(f"natural audit unit exceeds 32 KiB: {normative['id']}")
    return packed


def _pack_trace_group(
    *, owner: str, kind: str, facts: Sequence[dict[str, object]]
) -> list[dict[str, object]]:
    packed: list[dict[str, object]] = []
    current: list[dict[str, object]] = []

    def payload(selected: Sequence[dict[str, object]], index: int) -> dict[str, object]:
        group = f"trace\0{kind}\0{owner}"
        identity = f"t-{hashlib.sha256(group.encode()).hexdigest()[:12]}-{index:03d}"
        return _trace_payload(
            identity,
            kind="projection-trace",
            projection_trace_facts=selected,
        )

    for fact in facts:
        proposed = [*current, fact]
        if current and len(canonical_bytes(payload(proposed, len(packed) + 1))) > CONTEXT_MAX_BYTES:
            packed.append(payload(current, len(packed) + 1))
            current = [fact]
        else:
            current = proposed
    if current:
        packed.append(payload(current, len(packed) + 1))
    if any(len(canonical_bytes(item)) > CONTEXT_MAX_BYTES for item in packed):
        raise AuditBundleError(f"natural trace audit unit exceeds 32 KiB: {kind}:{owner}")
    return packed


def _excluded_launchers(
    projection: Mapping[str, object], cli_names: set[str]
) -> list[tuple[dict[str, object], dict[str, object]]]:
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    components = cast(Sequence[Mapping[str, object]], boundaries["components"])
    result: list[tuple[dict[str, object], dict[str, object]]] = []
    for index, component in enumerate(components):
        scripts = cast(Mapping[str, object], component["console_scripts"])
        for name, target in sorted(scripts.items()):
            if name in cli_names:
                continue
            candidate: dict[str, object] = {
                "id": f"candidate:console-script:{name}",
                "kind": "console-script",
                "detector": "cli-tree",
                "disposition": "excluded",
                "exclusion_policy": "process-launcher-not-cli/v1",
            }
            traced = {
                "candidate_id": candidate["id"],
                "boundary_pointer": (
                    f"/boundaries/components/{index}/console_scripts/{_escape_pointer(name)}"
                ),
                "source_authority_ids": ["release:release.toml"],
                "installed_target": target,
            }
            result.append((candidate, traced))
    return result


def _detector_meta_closure(
    projection: Mapping[str, object], cli_names: set[str]
) -> dict[str, object]:
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    external = cast(Mapping[str, object], projection["external_contract"])
    python_units = {
        str(surface["distribution"])
        for surface in cast(Sequence[Mapping[str, object]], external["python"])
    }
    components = cast(Sequence[Mapping[str, object]], boundaries["components"])
    channels: list[dict[str, object]] = []
    for component in components:
        distribution = str(component["distribution"])
        channels.append(
            {
                "id": f"distribution:{distribution}",
                "kind": "distribution",
                "release_unit": distribution,
                "detector": "release-metadata",
            }
        )
        if distribution in python_units:
            channels.append(
                {
                    "id": f"python:{distribution}",
                    "kind": "python",
                    "release_unit": distribution,
                    "detector": "python-export",
                }
            )
        for name in sorted(cast(Mapping[str, object], component["console_scripts"])):
            entry: dict[str, object] = {
                "id": f"console-script:{distribution}:{name}",
                "kind": "console-script",
                "release_unit": distribution,
            }
            if name in cli_names:
                entry["detector"] = "cli-tree"
            else:
                entry["disposition"] = "excluded"
                entry["exclusion_policy"] = "process-launcher-not-cli/v1"
            channels.append(entry)
    for point in cast(Sequence[Mapping[str, object]], boundaries["entry_point_extensions"]):
        group = str(point["group"])
        channels.append(
            {
                "id": f"extension-entry-point:{group}",
                "kind": "extension-entry-point",
                "release_unit": str(point["owner"]),
                "detector": "python-export",
            }
        )
    for point in cast(Sequence[Mapping[str, object]], boundaries["process_extensions"]):
        channels.append(
            {
                "id": f"process-protocol:{point['name']}",
                "kind": "process-protocol",
                "release_unit": str(point["contract_owner"]),
                "detector": "protocol-schema",
            }
        )
    runtime_images = cast(Mapping[str, Mapping[str, object]], boundaries["runtime_images"])
    for image_kind, images in sorted(runtime_images.items()):
        if not isinstance(images, Mapping):
            continue
        for image in sorted(images):
            channels.append(
                {
                    "id": f"runtime-image:{image_kind}:{image}",
                    "kind": "runtime-image",
                    "release_unit": image,
                    "detector": "release-metadata",
                }
            )
    for authority in sorted(cast(Mapping[str, object], external["protocol_schemas"])):
        channels.append(
            {
                "id": f"protocol-schema:{authority}",
                "kind": "protocol-schema",
                "release_unit": authority,
                "detector": "protocol-schema",
            }
        )
    identities = [str(channel["id"]) for channel in channels]
    undecided = [
        channel for channel in channels if ("detector" in channel) == ("disposition" in channel)
    ]
    duplicate = len(identities) - len(set(identities))
    detector_bindings: dict[str, list[str]] = defaultdict(list)
    exclusion_bindings: dict[str, list[str]] = defaultdict(list)
    for channel in channels:
        identity = str(channel["id"])
        if "detector" in channel:
            detector_bindings[str(channel["detector"])].append(identity)
        else:
            exclusion_bindings[str(channel["exclusion_policy"])].append(identity)
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
            "duplicate": duplicate,
            "stale": 0,
            "undecided": len(undecided),
        },
    }


def build_bundle(projection: Mapping[str, object], trace: Mapping[str, object]) -> AuditBundle:
    """Build and validate one bounded bundle from the executable logical authorities."""

    # The authority is JSON. Normalize Python-only container distinctions before
    # factoring it, exactly as the pre-cut checked projection did on serialization.
    projection = cast(Mapping[str, object], json.loads(json.dumps(projection, sort_keys=True)))
    trace = cast(Mapping[str, object], json.loads(json.dumps(trace, sort_keys=True)))
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    external = cast(Mapping[str, object], projection["external_contract"])
    boundary_facts = semantic_facts(boundaries, scope="boundary")
    contract_facts = semantic_facts(external, scope="contract")
    projection_trace_facts = semantic_facts(trace, scope="trace")
    sources = _source_index(trace)

    norm_groups: dict[
        tuple[str, str, str, tuple[str, ...], str | None],
        list[tuple[dict[str, object], tuple[str, ...]]],
    ] = defaultdict(list)
    all_candidates: list[dict[str, object]] = []
    all_candidate_sources: list[dict[str, object]] = []
    for fact in boundary_facts:
        owner, kind = _boundary_coordinates(fact, boundaries)
        norm_groups[("boundary", owner, kind, (), None)].append((fact, ("release:release.toml",)))
    for fact in contract_facts:
        owner, kind, detector, policies, source_ids = _contract_coordinates(fact, external)
        norm_groups[("external-contract", owner, kind, policies, detector)].append(
            (fact, source_ids)
        )

    files: dict[str, bytes] = {}
    descriptors: list[dict[str, object]] = []
    for (scope, owner, kind, policies, group_detector), items in sorted(norm_groups.items()):
        for normative, traced, candidates in _pack_normative_group(
            scope=scope,
            owner=owner,
            kind=kind,
            policies=policies,
            detector=group_detector,
            items=items,
            sources=sources,
        ):
            identity = str(normative["id"])
            normative_path = f"contexts/{identity}.json"
            trace_path = f"traces/{identity}.json"
            normative_bytes = canonical_bytes(normative)
            trace_bytes = canonical_bytes(traced)
            files[f"riverhog-v1/{normative_path}"] = normative_bytes
            files[f"riverhog-v1/{trace_path}"] = trace_bytes
            all_candidates.extend(candidates)
            all_candidate_sources.extend(cast(list[dict[str, object]], traced["candidate_sources"]))
            descriptors.append(
                _descriptor(
                    identity,
                    owner=owner,
                    kind=kind,
                    scope=scope,
                    policies=policies,
                    normative=(
                        normative_path,
                        normative_bytes,
                        len(cast(Sequence[object], normative["facts"])),
                    ),
                    trace=(trace_path, trace_bytes, 0),
                    candidates=candidates,
                )
            )

    trace_groups: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for fact in projection_trace_facts:
        owner, kind = _trace_coordinates(fact, trace)
        trace_groups[(owner, kind)].append(fact)
    for (owner, kind), facts in sorted(trace_groups.items()):
        for traced in _pack_trace_group(owner=owner, kind=kind, facts=facts):
            identity = str(traced["context_id"])
            trace_path = f"traces/{identity}.json"
            trace_bytes = canonical_bytes(traced)
            files[f"riverhog-v1/{trace_path}"] = trace_bytes
            descriptors.append(
                _descriptor(
                    identity,
                    owner=owner,
                    kind=kind,
                    scope="projection-trace",
                    policies=(),
                    normative=None,
                    trace=(
                        trace_path,
                        trace_bytes,
                        len(cast(Sequence[object], traced["projection_trace_facts"])),
                    ),
                    candidates=(),
                )
            )

    cli_names = set(cast(Mapping[str, object], external["cli"]))
    exclusions = _excluded_launchers(projection, cli_names)
    if exclusions:
        identity = "excluded-console-script-launchers-001"
        candidates = [candidate for candidate, _traced in exclusions]
        traced = _trace_payload(
            identity,
            kind="excluded",
            candidates=candidates,
            candidate_sources=[item for _candidate, item in exclusions],
            source_authorities=_resolve_sources(("release:release.toml",), sources),
        )
        trace_path = f"traces/{identity}.json"
        trace_bytes = canonical_bytes(traced)
        if len(trace_bytes) > CONTEXT_MAX_BYTES:
            raise AuditBundleError("excluded launcher context exceeds 32 KiB")
        files[f"riverhog-v1/{trace_path}"] = trace_bytes
        descriptors.append(
            _descriptor(
                identity,
                owner="repository",
                kind="excluded",
                scope="exclusion-only",
                policies=(),
                normative=None,
                trace=(trace_path, trace_bytes, 0),
                candidates=candidates,
            )
        )
        all_candidates.extend(candidates)
        all_candidate_sources.extend(item for _candidate, item in exclusions)

    meta_closure = _detector_meta_closure(projection, cli_names)
    contract_identity = {
        "schema": CONTRACT_IDENTITY_SCHEMA,
        "policies": list(CONTRACT_POLICIES),
        "facts": sorted(contract_facts, key=lambda item: str(item["id"])),
    }
    coverage_identity = {
        "schema": COVERAGE_IDENTITY_SCHEMA,
        "exclusion_policies": list(EXCLUSION_POLICIES),
        "detectors": list(DETECTORS),
        "meta_closure": meta_closure,
        "candidates": sorted(all_candidates, key=lambda item: str(item["id"])),
    }
    trace_identity = {
        "schema": TRACE_IDENTITY_SCHEMA,
        "candidate_sources": sorted(
            all_candidate_sources, key=lambda item: str(item["candidate_id"])
        ),
        "projection_trace_facts": sorted(projection_trace_facts, key=lambda item: str(item["id"])),
        "qualification_routes": {
            key: list(value) for key, value in sorted(QUALIFICATION_ROUTES.items())
        },
    }
    descriptors.sort(key=lambda item: str(item["id"]))
    representation_identity = {
        "schema": REPRESENTATION_IDENTITY_SCHEMA,
        "context_columns": list(CONTEXT_COLUMNS),
        "contexts": [
            [descriptor.get(column) for column in CONTEXT_COLUMNS] for descriptor in descriptors
        ],
    }
    contract_policy_ids = {
        policy
        for descriptor in descriptors
        for policy in cast(Sequence[str], descriptor["policies"])
    }
    declared_policy_ids = {str(policy["id"]) for policy in CONTRACT_POLICIES}
    if contract_policy_ids != declared_policy_ids:
        raise AuditBundleError("effective contract policies do not match the root registry")
    candidate_ids = [str(candidate["id"]) for candidate in all_candidates]
    contract_fact_ids = {str(fact["id"]) for fact in contract_facts}
    routed_fact_ids = {
        str(candidate["fact_id"])
        for candidate in all_candidates
        if candidate["disposition"] == "contractual"
    }
    coverage = {
        "contexts": len(descriptors),
        "facts": {
            "boundary": len(boundary_facts),
            "external_contract": len(contract_facts),
            "projection_trace": len(projection_trace_facts),
        },
        "candidates": len(all_candidates),
        "by_detector": dict(
            sorted(Counter(str(item["detector"]) for item in all_candidates).items())
        ),
        "by_kind": dict(sorted(Counter(str(item["kind"]) for item in all_candidates).items())),
        "by_disposition": dict(
            sorted(Counter(str(item["disposition"]) for item in all_candidates).items())
        ),
        "by_owner": dict(
            sorted(
                Counter(
                    str(item["owner"])
                    for item in descriptors
                    if item["scope"] == "external-contract"
                    for _ in range(
                        cast(
                            int,
                            cast(Mapping[str, object], item["normative"])["facts"],
                        )
                    )
                ).items()
            )
        ),
        "by_effective_policy": dict(
            sorted(
                Counter(
                    policy
                    for item in descriptors
                    for policy in cast(Sequence[str], item["policies"])
                    for _ in range(
                        cast(Mapping[str, int], cast(Mapping[str, object], item["normative"]))[
                            "facts"
                        ]
                    )
                ).items()
            )
        ),
        "anomalies": {
            "missing": len(contract_fact_ids - routed_fact_ids),
            "duplicate": len(candidate_ids) - len(set(candidate_ids)),
            "stale": 0,
            "undecided": sum(
                item["disposition"] not in {"contractual", "excluded"} for item in all_candidates
            ),
            "multiply_disposed": 0,
            "multiply_routed": len(routed_fact_ids) - len(contract_fact_ids),
            "multiply_represented": 0,
        },
    }
    root = {
        "schema": ROOT_SCHEMA,
        "series": projection["series"],
        "boundary": {
            "schema": projection["schema"],
            "canonical_sha256": canonical_sha256(boundaries),
            "legacy_canonical_sha256": hashlib.sha256(
                json.dumps(boundaries, separators=(",", ":"), sort_keys=True).encode()
            ).hexdigest(),
        },
        "policies": {
            "contract": list(CONTRACT_POLICIES),
            "exclusion": list(EXCLUSION_POLICIES),
        },
        "detectors": list(DETECTORS),
        "detector_meta_closure": meta_closure,
        "identities": {
            "external_contract_sha256": canonical_sha256(contract_identity),
            "coverage_sha256": canonical_sha256(coverage_identity),
            "trace_sha256": canonical_sha256(trace_identity),
            "representation_sha256": canonical_sha256(representation_identity),
        },
        "extent_contract": {
            "schema": cast(Mapping[str, object], external["extents"])["schema"],
            "sha256": cast(Mapping[str, object], external["extents"])["sha256"],
            "decisions": cast(
                Mapping[str, object], cast(Mapping[str, object], external["extents"])["coverage"]
            )["classified"],
        },
        "coverage": coverage,
        "context_directory": "riverhog-v1",
        "context_columns": list(CONTEXT_COLUMNS),
        "contexts": [
            [descriptor.get(column) for column in CONTEXT_COLUMNS] for descriptor in descriptors
        ],
    }
    bundle = AuditBundle(root=root, files=files)
    validate_bundle(bundle, projection=projection, trace=trace)
    return bundle


def context_descriptors(root: Mapping[str, object]) -> list[dict[str, object]]:
    columns = cast(Sequence[str], root["context_columns"])
    if tuple(columns) != CONTEXT_COLUMNS:
        raise AuditBundleError("audit root context columns are not canonical")
    rows = cast(Sequence[Sequence[object]], root["contexts"])
    if any(len(row) != len(columns) for row in rows):
        raise AuditBundleError("audit root context row does not match its columns")
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _context_files(root: Mapping[str, object]) -> set[str]:
    result: set[str] = set()
    directory = str(root["context_directory"])
    directory_path = PurePosixPath(directory)
    if (
        directory_path.is_absolute()
        or directory_path.as_posix() != directory
        or len(directory_path.parts) != 1
        or directory_path.name in {"", ".", ".."}
    ):
        raise AuditBundleError("audit context directory is not a safe relative directory")
    for context in context_descriptors(root):
        normative = context.get("normative")
        if isinstance(normative, Mapping):
            references = [str(normative["path"])]
        else:
            references = []
        traced = cast(Mapping[str, object], context["trace"])
        references.append(str(traced["path"]))
        for relative in references:
            relative_path = PurePosixPath(relative)
            if (
                relative_path.is_absolute()
                or relative_path.as_posix() != relative
                or not relative_path.parts
                or any(part in {"", ".", ".."} for part in relative_path.parts)
            ):
                raise AuditBundleError(f"audit context path is not safe: {relative}")
            bundled = f"{directory}/{relative}"
            if bundled in result:
                raise AuditBundleError(f"audit context is referenced more than once: {bundled}")
            result.add(bundled)
    return result


def validate_bundle(
    bundle: AuditBundle,
    *,
    projection: Mapping[str, object] | None = None,
    trace: Mapping[str, object] | None = None,
) -> None:
    """Validate budgets, reachability, identities, disposition, and lossless assembly."""

    root_bytes = canonical_bytes(bundle.root)
    if len(root_bytes) > ROOT_MAX_BYTES:
        raise AuditBundleError(f"audit root exceeds 64 KiB: {len(root_bytes)} bytes")
    referenced = _context_files(bundle.root)
    if referenced != set(bundle.files):
        raise AuditBundleError("root-referenced files differ from the bundle contents")
    all_facts: dict[str, list[dict[str, object]]] = defaultdict(list)
    candidates: list[dict[str, object]] = []
    candidate_sources: list[dict[str, object]] = []
    descriptors = context_descriptors(bundle.root)
    policy_registry = cast(Mapping[str, object], bundle.root["policies"])
    contract_policy_ids = {
        str(policy["id"])
        for policy in cast(Sequence[Mapping[str, object]], policy_registry["contract"])
    }
    exclusion_policy_ids = {
        str(policy["id"])
        for policy in cast(Sequence[Mapping[str, object]], policy_registry["exclusion"])
    }
    detector_ids = {
        str(detector["id"])
        for detector in cast(Sequence[Mapping[str, object]], bundle.root["detectors"])
    }
    effective_contract_policy_ids: set[str] = set()
    for descriptor in descriptors:
        normative_ref = descriptor.get("normative")
        descriptor_scope = (
            "exclusion-only" if descriptor["kind"] == "excluded" else "projection-trace"
        )
        if isinstance(normative_ref, Mapping):
            payload = bundle.files[f"{bundle.root['context_directory']}/{normative_ref['path']}"]
            if (
                len(payload) != normative_ref["bytes"]
                or hashlib.sha256(payload).hexdigest() != normative_ref["sha256"]
            ):
                raise AuditBundleError(f"normative context identity mismatch: {descriptor['id']}")
            if len(payload) > CONTEXT_MAX_BYTES:
                raise AuditBundleError(f"normative context exceeds 32 KiB: {descriptor['id']}")
            document = cast(dict[str, object], json.loads(payload))
            descriptor_scope = str(document["scope"])
            for field in ("id", "owner", "kind", "policies"):
                if document.get(field) != descriptor.get(field):
                    raise AuditBundleError(
                        f"normative context {field} differs from its root descriptor: "
                        f"{descriptor['id']}"
                    )
            if document.get("schema") != CONTEXT_SCHEMA:
                raise AuditBundleError(f"unexpected normative context schema: {descriptor['id']}")
            document_policies = set(cast(Sequence[str], document["policies"]))
            if document["scope"] == "external-contract":
                if not document_policies or not document_policies <= contract_policy_ids:
                    raise AuditBundleError(
                        f"external context has an invalid effective policy set: {descriptor['id']}"
                    )
                effective_contract_policy_ids.update(document_policies)
            elif document_policies:
                raise AuditBundleError(
                    f"non-contract context claims contract policies: {descriptor['id']}"
                )
            all_facts[str(document["scope"])].extend(
                cast(list[dict[str, object]], document["facts"])
            )
        trace_ref = cast(Mapping[str, object], descriptor["trace"])
        payload = bundle.files[f"{bundle.root['context_directory']}/{trace_ref['path']}"]
        if (
            len(payload) != trace_ref["bytes"]
            or hashlib.sha256(payload).hexdigest() != trace_ref["sha256"]
        ):
            raise AuditBundleError(f"trace context identity mismatch: {descriptor['id']}")
        if len(payload) > CONTEXT_MAX_BYTES:
            raise AuditBundleError(f"trace context exceeds 32 KiB: {descriptor['id']}")
        document = cast(dict[str, object], json.loads(payload))
        if document.get("schema") != CONTEXT_TRACE_SCHEMA:
            raise AuditBundleError(f"unexpected trace context schema: {descriptor['id']}")
        if document.get("context_id") != descriptor["id"]:
            raise AuditBundleError(f"trace context identity mismatch: {descriptor['id']}")
        route_kind = (
            "projection-trace"
            if descriptor_scope == "projection-trace"
            else "boundary"
            if descriptor_scope == "boundary"
            else "excluded"
            if descriptor_scope == "exclusion-only"
            else str(descriptor["kind"])
        )
        if document.get("qualification_routes") != list(QUALIFICATION_ROUTES[route_kind]):
            raise AuditBundleError(f"trace qualification route mismatch: {descriptor['id']}")
        candidates.extend(cast(list[dict[str, object]], document["candidates"]))
        candidate_sources.extend(cast(list[dict[str, object]], document["candidate_sources"]))
        all_facts["projection-trace"].extend(
            cast(list[dict[str, object]], document["projection_trace_facts"])
        )
    if effective_contract_policy_ids != contract_policy_ids:
        raise AuditBundleError("effective contract policies do not match the root registry")
    anomalies = cast(
        Mapping[str, int], cast(Mapping[str, object], bundle.root["coverage"])["anomalies"]
    )
    if any(anomalies.values()):
        raise AuditBundleError(f"audit coverage has anomalies: {dict(anomalies)}")
    contract_facts = all_facts["external-contract"]
    validate_candidate_routes(contract_facts, candidates)
    candidate_by_id = {str(candidate["id"]): candidate for candidate in candidates}
    candidate_source_by_id = {str(source["candidate_id"]): source for source in candidate_sources}
    if len(candidate_source_by_id) != len(candidate_sources):
        raise AuditBundleError("candidate trace identities are not unique")
    if set(candidate_source_by_id) != set(candidate_by_id):
        raise AuditBundleError("candidate and candidate-trace coverage differ")
    for candidate_id, candidate in candidate_by_id.items():
        source = candidate_source_by_id[candidate_id]
        if not cast(Sequence[object], source.get("source_authority_ids", ())):
            raise AuditBundleError(f"candidate trace lacks source authority: {candidate_id}")
        if candidate["disposition"] == "contractual":
            if source.get("fact_id") != candidate["fact_id"] or not isinstance(
                source.get("projection_pointer"), str
            ):
                raise AuditBundleError(f"contractual candidate trace is ambiguous: {candidate_id}")
        elif candidate.get("exclusion_policy") not in exclusion_policy_ids or not isinstance(
            source.get("boundary_pointer"), str
        ):
            raise AuditBundleError(f"excluded candidate trace is ambiguous: {candidate_id}")
        if candidate.get("detector") not in detector_ids:
            raise AuditBundleError(f"candidate uses an undeclared detector: {candidate_id}")
    closure = cast(Mapping[str, object], bundle.root["detector_meta_closure"])
    closure_coverage = cast(Mapping[str, int], closure["coverage"])
    if any(closure_coverage[key] for key in ("missing", "duplicate", "stale", "undecided")):
        raise AuditBundleError("detector meta-closure is incomplete")
    policies = cast(Mapping[str, object], bundle.root["policies"])
    identities = cast(Mapping[str, str], bundle.root["identities"])
    contract_identity = {
        "schema": CONTRACT_IDENTITY_SCHEMA,
        "policies": policies["contract"],
        "facts": sorted(contract_facts, key=lambda item: str(item["id"])),
    }
    coverage_identity = {
        "schema": COVERAGE_IDENTITY_SCHEMA,
        "exclusion_policies": policies["exclusion"],
        "detectors": bundle.root["detectors"],
        "meta_closure": closure,
        "candidates": sorted(candidates, key=lambda item: str(item["id"])),
    }
    trace_identity = {
        "schema": TRACE_IDENTITY_SCHEMA,
        "candidate_sources": sorted(candidate_sources, key=lambda item: str(item["candidate_id"])),
        "projection_trace_facts": sorted(
            all_facts["projection-trace"], key=lambda item: str(item["id"])
        ),
        "qualification_routes": {
            key: list(value) for key, value in sorted(QUALIFICATION_ROUTES.items())
        },
    }
    representation_identity = {
        "schema": REPRESENTATION_IDENTITY_SCHEMA,
        "context_columns": bundle.root["context_columns"],
        "contexts": bundle.root["contexts"],
    }
    observed_identities = {
        "external_contract_sha256": canonical_sha256(contract_identity),
        "coverage_sha256": canonical_sha256(coverage_identity),
        "trace_sha256": canonical_sha256(trace_identity),
        "representation_sha256": canonical_sha256(representation_identity),
    }
    if dict(identities) != observed_identities:
        raise AuditBundleError("audit root identities do not match their logical domains")
    if projection is not None:
        assembled = {
            "schema": cast(Mapping[str, object], bundle.root["boundary"])["schema"],
            "series": bundle.root["series"],
            "boundaries": reassemble_facts(all_facts["boundary"]),
            "external_contract": reassemble_facts(contract_facts),
        }
        if assembled != projection:
            raise AuditBundleError("bundle does not losslessly reassemble the logical projection")
    if trace is not None and reassemble_facts(all_facts["projection-trace"]) != trace:
        raise AuditBundleError("bundle does not losslessly reassemble the logical trace")


def load_bundle(path: Path) -> AuditBundle:
    try:
        root = cast(dict[str, object], json.loads(path.read_bytes()))
    except (OSError, json.JSONDecodeError) as exc:
        raise AuditBundleError(f"audit root is unavailable: {path}") from exc
    if root.get("schema") != ROOT_SCHEMA:
        raise AuditBundleError(f"unexpected audit root schema: {root.get('schema')}")
    files: dict[str, bytes] = {}
    for relative in _context_files(root):
        candidate = path.parent / relative
        try:
            files[relative] = candidate.read_bytes()
        except OSError as exc:
            raise AuditBundleError(f"audit context is unavailable: {relative}") from exc
    context_directory = path.parent / str(root["context_directory"])
    actual = (
        {
            candidate.relative_to(path.parent).as_posix()
            for candidate in context_directory.rglob("*")
            if candidate.is_file()
        }
        if context_directory.is_dir()
        else set()
    )
    if actual != set(files):
        raise AuditBundleError("audit context directory contains stale or unreferenced files")
    bundle = AuditBundle(root=root, files=files)
    validate_bundle(bundle)
    return bundle


def validate_candidate_routes(
    facts: Sequence[Mapping[str, object]], candidates: Sequence[Mapping[str, object]]
) -> None:
    """Require exact dispositions while allowing independent candidates to corroborate a fact."""

    fact_ids = [str(item["id"]) for item in facts]
    if len(fact_ids) != len(set(fact_ids)):
        raise AuditBundleError("contract fact has multiple normative representations")
    candidate_ids = [str(item["id"]) for item in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise AuditBundleError("candidate identities are not unique")
    fact_id_set = set(fact_ids)
    routed: set[str] = set()
    for candidate in candidates:
        disposition = candidate.get("disposition")
        if disposition == "contractual":
            if "exclusion_policy" in candidate or not isinstance(candidate.get("fact_id"), str):
                raise AuditBundleError("contractual candidate has an ambiguous route")
            fact_id = str(candidate["fact_id"])
            if fact_id not in fact_id_set:
                raise AuditBundleError("contractual candidate routes to an unknown fact")
            routed.add(fact_id)
        elif disposition == "excluded":
            if "fact_id" in candidate or not candidate.get("exclusion_policy"):
                raise AuditBundleError("excluded candidate has an ambiguous disposition")
        else:
            raise AuditBundleError("candidate disposition is missing or undecided")
    if routed != fact_id_set:
        raise AuditBundleError("contract facts and contractual candidate routes differ")


def reassemble_projection(bundle: AuditBundle) -> dict[str, object]:
    facts: dict[str, list[dict[str, object]]] = defaultdict(list)
    for descriptor in context_descriptors(bundle.root):
        normative = descriptor.get("normative")
        if not isinstance(normative, Mapping):
            continue
        document = cast(
            Mapping[str, object],
            json.loads(bundle.files[f"{bundle.root['context_directory']}/{normative['path']}"]),
        )
        facts[str(document["scope"])].extend(cast(list[dict[str, object]], document["facts"]))
    return {
        "schema": cast(Mapping[str, object], bundle.root["boundary"])["schema"],
        "series": bundle.root["series"],
        "boundaries": reassemble_facts(facts["boundary"]),
        "external_contract": reassemble_facts(facts["external-contract"]),
    }


def reassemble_trace(bundle: AuditBundle) -> dict[str, object]:
    facts: list[dict[str, object]] = []
    for descriptor in context_descriptors(bundle.root):
        traced = cast(Mapping[str, object], descriptor["trace"])
        document = cast(
            Mapping[str, object],
            json.loads(bundle.files[f"{bundle.root['context_directory']}/{traced['path']}"]),
        )
        facts.extend(cast(list[dict[str, object]], document["projection_trace_facts"]))
    return cast(dict[str, object], reassemble_facts(facts))


def checked_file_set(path: Path) -> set[Path]:
    bundle = load_bundle(path)
    return {path, *(path.parent / relative for relative in bundle.files)}

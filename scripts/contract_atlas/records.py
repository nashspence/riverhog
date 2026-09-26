"""Separate declared contract meaning from discovery and qualification accounting."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import extent_contract

from .model import ContractAtlasError, DiscoveredContract, canonical_sha256, pointer_value

CLOSURE_FORMAT = "riverhog-contract-closure/v1"
AUDIT_FORMAT = "riverhog-contract-audit-record/v1"
AUDIT_FILENAME = "riverhog-v1-audit.json"
AUDIT_PRESENTATION = {
    "extent_marker": {
        "glyph": "📦",
        "label": "Extent",
        "meaning": (
            "An open extent qualification affects this declared subject or branch. "
            "Follow the marker for its recorded scope and candidate checks. "
            "The marker does not report a runtime failure; "
            "an unmarked subject carries no approval claim."
        ),
    },
    "reference_routes": [
        {"path": "audit-key.html", "label": "Audit key"},
        {"path": "qualifications.html", "label": "Open qualifications"},
        {"path": "accounting.html", "label": "Accounting checks"},
        {"path": "sources.html", "label": "Sources and qualifications"},
        {"path": "configuration.html", "label": "Configuration comparison"},
        {"path": "relationships.html", "label": "Declared relationships"},
        {"path": "identities.html", "label": "Snapshot identities"},
        {"path": "../riverhog-v1-audit.json", "label": "Bound Audit Record"},
    ],
}


@dataclass(frozen=True)
class ContractBundle:
    """A standalone Closure with its separately bound Audit Record."""

    closure: dict[str, object]
    audit: dict[str, object]


def load_closure(path: Path) -> dict[str, object]:
    """Load a Closure without requiring audit data or a source checkout."""

    try:
        value = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractAtlasError(f"contract closure is unavailable: {path}") from exc
    if not isinstance(value, dict):
        raise ContractAtlasError("contract closure root is not an object")
    validate_closure(value)
    return cast(dict[str, object], value)


def load_bundle(path: Path) -> ContractBundle:
    """Load and validate an exact pair of checked machine records."""

    closure = load_closure(path)
    audit_path = path.with_name(AUDIT_FILENAME)
    try:
        value = json.loads(audit_path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractAtlasError(f"audit record is unavailable: {audit_path}") from exc
    if not isinstance(value, dict):
        raise ContractAtlasError("audit record root is not an object")
    audit = cast(dict[str, object], value)
    validate_audit_record(closure, audit)
    return ContractBundle(closure, audit)


_CONTRACT_ELEMENT_FIELDS = frozenset(
    {"id", "authority", "interface", "title", "pointers", "related_element_ids"}
)
_AUDIT_FIELDS = frozenset(
    {
        "format",
        "closure_sha256",
        "source_projection_format",
        "source_projection_sha256",
        "source_projection_unsafe_integer_paths",
        "source_elements_sha256",
        "extent_analysis",
        "trace",
        "trace_unsafe_integer_paths",
        "sources",
        "element_overlays",
        "discovery",
        "counts",
        "policies",
        "source_identities",
        "presentation",
    }
)
_EXTENT_ANALYSIS_FIELDS = frozenset(
    {"format", "decisions", "coverage", "source_sha256", "analysis_rules", "authoring_fields"}
)


def _declared_extents(extents: Mapping[str, object]) -> dict[str, object]:
    declared = extent_contract.normative_extent_declarations()
    if extents.get("principles") != declared["principles"]:
        raise ContractAtlasError("extent principles differ from their declaration authority")
    rules = cast(Mapping[str, object], extents["rules"])
    for identity, value in cast(Mapping[str, object], declared["rules"]).items():
        original = cast(Mapping[str, object], rules[identity])
        without_authoring = {key: item for key, item in original.items() if key != "requirement"}
        if value != without_authoring:
            raise ContractAtlasError(f"extent rule differs from its declaration: {identity}")
    return declared


def _contract_elements(
    elements: Sequence[Mapping[str, object]], normative_projection: Mapping[str, object]
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    for element in elements:
        pointers = cast(Sequence[str], element["pointers"])
        resolved = []
        for pointer in pointers:
            try:
                pointer_value(normative_projection, pointer)
            except (KeyError, IndexError, TypeError, ValueError, ContractAtlasError):
                resolved.append(False)
            else:
                resolved.append(True)
        if all(resolved):
            selected.append(
                {
                    key: copy.deepcopy(element[key])
                    for key in _CONTRACT_ELEMENT_FIELDS
                    if key in element
                }
            )
            continue
        raise ContractAtlasError(
            f"contract element has unresolved declared values: {element['id']}"
        )
    selected_ids = {str(element["id"]) for element in selected}
    for element in selected:
        element["related_element_ids"] = [
            identity
            for identity in cast(Sequence[str], element.get("related_element_ids", []))
            if identity in selected_ids
        ]
    return selected


def build_records(discovered: DiscoveredContract) -> tuple[dict[str, object], dict[str, object]]:
    """Partition one validated discovered model into Closure and bound Audit Record."""

    root = discovered.root
    projection = cast(Mapping[str, object], root["projection"])
    external = cast(Mapping[str, object], projection["external_contract"])
    extents = cast(Mapping[str, object], external["extents"])
    declared = _declared_extents(extents)
    normative_projection = copy.deepcopy(projection)
    cast(dict[str, object], normative_projection["external_contract"])["extents"] = declared
    elements = _contract_elements(
        cast(Sequence[Mapping[str, object]], root["elements"]), normative_projection
    )
    unsafe_paths = [
        pointer
        for pointer in cast(Sequence[str], root["projection_unsafe_integer_paths"])
        if _pointer_exists(normative_projection, pointer)
    ]
    closure: dict[str, object] = {
        "format": CLOSURE_FORMAT,
        "series": projection["series"],
        "boundaries": copy.deepcopy(projection["boundaries"]),
        "external_contract": copy.deepcopy(normative_projection["external_contract"]),
        "unsafe_integer_paths": unsafe_paths,
        "elements": elements,
    }
    validate_closure(closure)
    selected_ids = {str(element["id"]) for element in elements}
    all_elements = cast(Sequence[Mapping[str, object]], root["elements"])
    overlays = [
        {
            "id": element["id"],
            **{
                key: copy.deepcopy(value)
                for key, value in element.items()
                if key not in _CONTRACT_ELEMENT_FIELDS
            },
        }
        for element in all_elements
        if element["id"] in selected_ids
    ]
    if selected_ids != {str(element["id"]) for element in all_elements}:
        raise ContractAtlasError("discovered contract elements were silently excluded")
    all_rules = cast(Mapping[str, Mapping[str, object]], extents["rules"])
    declared_rule_ids = set(cast(Mapping[str, object], declared["rules"]))
    audit: dict[str, object] = {
        "format": AUDIT_FORMAT,
        "closure_sha256": canonical_sha256(closure),
        "source_projection_format": root["projection_format"],
        "source_projection_sha256": canonical_sha256(projection),
        "source_projection_unsafe_integer_paths": copy.deepcopy(
            root["projection_unsafe_integer_paths"]
        ),
        "source_elements_sha256": canonical_sha256(all_elements),
        "extent_analysis": {
            "format": extents["format"],
            "decisions": copy.deepcopy(extents["decisions"]),
            "coverage": copy.deepcopy(extents["coverage"]),
            "source_sha256": extents["sha256"],
            "analysis_rules": {
                key: copy.deepcopy(value)
                for key, value in all_rules.items()
                if key not in declared_rule_ids
            },
            "authoring_fields": {
                key: {
                    field: copy.deepcopy(value[field]) for field in value if field == "requirement"
                }
                for key, value in all_rules.items()
                if key in declared_rule_ids and "requirement" in value
            },
        },
        "trace": copy.deepcopy(root["trace"]),
        "trace_unsafe_integer_paths": copy.deepcopy(root["trace_unsafe_integer_paths"]),
        "sources": copy.deepcopy(root["sources"]),
        "element_overlays": overlays,
        "discovery": copy.deepcopy(root["discovery"]),
        "counts": copy.deepcopy(root["counts"]),
        "policies": copy.deepcopy(root["policies"]),
        "source_identities": copy.deepcopy(root["identities"]),
        "presentation": copy.deepcopy(AUDIT_PRESENTATION),
    }
    validate_audit_record(closure, audit)
    return closure, audit


def _pointer_exists(value: Mapping[str, object], pointer: str) -> bool:
    try:
        pointer_value(value, pointer)
    except (KeyError, IndexError, TypeError, ValueError, ContractAtlasError):
        return False
    return True


def validate_closure(closure: Mapping[str, object]) -> None:
    """Validate a standalone machine Closure without a source tree or Audit Record."""

    if (
        set(closure)
        != {
            "format",
            "series",
            "boundaries",
            "external_contract",
            "unsafe_integer_paths",
            "elements",
        }
        or closure.get("format") != CLOSURE_FORMAT
    ):
        raise ContractAtlasError("contract closure has an unknown or incomplete format")
    if closure["series"] != "v1":
        raise ContractAtlasError("contract closure has an unknown series")
    external = closure["external_contract"]
    if not isinstance(external, Mapping):
        raise ContractAtlasError("contract closure has no external contract")
    extents = external.get("extents")
    if not isinstance(extents, Mapping) or set(extents) != {"format", "principles", "rules"}:
        raise ContractAtlasError("contract closure mixes extent declarations and analysis")
    if extents["format"] != "riverhog-extent-declarations/v1":
        raise ContractAtlasError("contract closure has an unknown extent format")
    values = cast(Sequence[Mapping[str, object]], closure["elements"])
    if not isinstance(values, list):
        raise ContractAtlasError("contract closure elements must be a list")
    seen: set[str] = set()
    for element in values:
        if not isinstance(element, Mapping) or set(element) != _CONTRACT_ELEMENT_FIELDS:
            raise ContractAtlasError("contract closure element has an unreviewed field role")
        identity = element["id"]
        if not isinstance(identity, str) or identity in seen:
            raise ContractAtlasError("contract closure element identities are invalid")
        seen.add(identity)
        pointers = element["pointers"]
        if (
            not isinstance(pointers, list)
            or not pointers
            or not all(
                isinstance(pointer, str)
                and pointer.startswith("/external_contract/")
                and _pointer_exists(closure, pointer)
                for pointer in pointers
            )
        ):
            raise ContractAtlasError(
                f"contract closure element has an unresolved value: {identity}"
            )
    for element in values:
        if not set(cast(Sequence[str], element["related_element_ids"])) <= seen:
            raise ContractAtlasError("contract closure has an unresolved element relation")
    unsafe = closure["unsafe_integer_paths"]
    if (
        not isinstance(unsafe, list)
        or len(unsafe) != len(set(unsafe))
        or any(
            not isinstance(pointer, str) or not _pointer_exists(closure, pointer)
            for pointer in unsafe
        )
    ):
        raise ContractAtlasError("contract closure has invalid exact-integer paths")
    canonical_sha256(closure)


def validate_audit_record(closure: Mapping[str, object], audit: Mapping[str, object]) -> None:
    """Reject an unbound or internally stale audit overlay."""

    validate_closure(closure)
    if (
        set(audit) != _AUDIT_FIELDS
        or audit.get("format") != AUDIT_FORMAT
        or audit.get("closure_sha256") != canonical_sha256(closure)
    ):
        raise ContractAtlasError("audit record is not bound to this contract closure")
    if audit["presentation"] != AUDIT_PRESENTATION:
        raise ContractAtlasError("audit presentation differs from its declared meaning")
    analysis = cast(Mapping[str, object], audit["extent_analysis"])
    if not isinstance(analysis, Mapping) or set(analysis) != _EXTENT_ANALYSIS_FIELDS:
        raise ContractAtlasError("audit extent analysis has an unreviewed field role")
    extents = cast(
        Mapping[str, object], cast(Mapping[str, object], closure["external_contract"])["extents"]
    )
    restored_rules = copy.deepcopy(cast(dict[str, object], extents["rules"]))
    restored_rules.update(cast(Mapping[str, object], analysis["analysis_rules"]))
    for identity, fields in cast(
        Mapping[str, Mapping[str, object]], analysis["authoring_fields"]
    ).items():
        if identity not in restored_rules or set(fields) != {"requirement"}:
            raise ContractAtlasError("audit authoring field has no declared rule owner")
        cast(dict[str, object], restored_rules[identity]).update(fields)
    restored_extents = {
        "format": analysis["format"],
        "principles": extents["principles"],
        "rules": restored_rules,
        "decisions": analysis["decisions"],
        "coverage": analysis["coverage"],
    }
    if canonical_sha256(restored_extents) != analysis["source_sha256"]:
        raise ContractAtlasError("audit extent partition does not restore its source value")
    external = copy.deepcopy(cast(dict[str, object], closure["external_contract"]))
    external["extents"] = {**restored_extents, "sha256": analysis["source_sha256"]}
    restored_projection = {
        "format": audit["source_projection_format"],
        "series": closure["series"],
        "boundaries": closure["boundaries"],
        "external_contract": external,
    }
    if canonical_sha256(restored_projection) != audit["source_projection_sha256"]:
        raise ContractAtlasError("audit partition does not restore the discovered projection")
    elements = {
        str(element["id"]): element
        for element in cast(Sequence[Mapping[str, object]], closure["elements"])
    }
    overlays = cast(Sequence[Mapping[str, object]], audit["element_overlays"])
    if {str(item["id"]) for item in overlays} != set(elements) or len(overlays) != len(elements):
        raise ContractAtlasError("audit element overlays do not match contract subjects")
    if any(set(overlay) & (_CONTRACT_ELEMENT_FIELDS - {"id"}) for overlay in overlays):
        raise ContractAtlasError("audit element overlay attempts to replace a contract field")
    restored_elements = [
        {
            **elements[str(overlay["id"])],
            **{key: value for key, value in overlay.items() if key != "id"},
        }
        for overlay in overlays
    ]
    restored_elements.sort(key=lambda item: str(item["id"]))
    if canonical_sha256(restored_elements) != audit["source_elements_sha256"]:
        raise ContractAtlasError("audit element partition does not restore discovery records")
    decisions = cast(
        Sequence[Mapping[str, object]],
        analysis["decisions"],
    )
    decision_ids: set[str] = set()
    for decision in decisions:
        identity = str(decision["id"])
        if identity in decision_ids or not _pointer_exists(
            closure, str(decision["source_pointer"])
        ):
            raise ContractAtlasError(f"extent analysis has a stale or repeated subject: {identity}")
        decision_ids.add(identity)
    for overlay in overlays:
        if not set(cast(Sequence[str], overlay["extent_decision_ids"])) <= decision_ids:
            raise ContractAtlasError("audit element has an unresolved extent decision")
    trace = cast(Mapping[str, object], audit["trace"])
    for witness in cast(Sequence[Mapping[str, object]], trace["segmented_extent_witnesses"]):
        if (
            witness.get("association_status") != "candidate"
            or witness.get("result_reference") is not None
            or not _pointer_exists(closure, str(witness["rule_pointer"]))
            or any(
                not _pointer_exists(closure, str(pointer))
                for pointer in cast(Sequence[str], witness["subject_pointers"])
            )
        ):
            raise ContractAtlasError("audit witness has an unbound contract subject or result")

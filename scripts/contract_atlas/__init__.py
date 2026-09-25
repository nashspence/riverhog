"""Discover the exact Riverhog contract and its audit associations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

from .discovery import (
    DETECTORS,
    _attach_extent_decisions,
    _counts,
    _detector_meta_closure,
    _external_elements,
    _link_operation_qualification,
    _policy_registry,
    _projection_coverage,
    _source_index,
    _validate_authority_registry,
    _validate_process_protocol_units,
    _validate_python_units,
    _validate_release_units,
)
from .model import (
    COVERAGE_IDENTITY_FORMAT,
    INTERFACE_REGISTRY,
    QUALIFICATION_ROUTES,
    ROOT_FORMAT,
    TRACE_IDENTITY_FORMAT,
    DiscoveredContract,
    _decode_unsafe_integers,
    _encoded_json,
    canonical_bytes,
    canonical_sha256,
    normalized_json,
    pointer_value,
    reassemble_projection,
    reassemble_trace,
    structural_json_schema,
)
from .model import (
    ContractAtlasError as ContractAtlasError,
)

__all__ = [
    "INTERFACE_REGISTRY",
    "_decode_unsafe_integers",
    "canonical_bytes",
    "canonical_sha256",
    "normalized_json",
    "pointer_value",
    "reassemble_projection",
    "reassemble_trace",
    "structural_json_schema",
]


def build_discovered_contract(
    projection: Mapping[str, object],
    trace: Mapping[str, object],
) -> DiscoveredContract:
    """Discover contractual candidates and account for the full source projection."""

    encoded_projection, projection_integer_paths = _encoded_json(projection)
    encoded_trace, trace_integer_paths = _encoded_json(trace)
    normalized_projection = cast(dict[str, object], encoded_projection)
    normalized_trace = cast(dict[str, object], encoded_trace)
    elements = _external_elements(normalized_projection, normalized_trace)
    _attach_extent_decisions(elements, normalized_projection)
    _link_operation_qualification(elements, normalized_trace, normalized_projection)
    noncontractual_projection = _validate_authority_registry(
        elements, normalized_projection, normalized_trace
    )
    ids = [str(item["id"]) for item in elements]
    if len(ids) != len(set(ids)):
        raise ContractAtlasError("semantic contract element identities are not unique")
    elements.sort(key=lambda item: str(item["id"]))
    source_index = _source_index(normalized_trace)
    for item in elements:
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
    }
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
    ]
    candidates = [
        {
            "id": f"candidate:{item['id']}",
            "detector": item["detector"],
            "element_id": item["id"],
            "source_authority_ids": item["source_authority_ids"],
        }
        for item in elements
    ]
    dispositions = [
        {
            "candidate_id": f"candidate:{item['id']}",
            "disposition": "protected",
            "policy_ids": item["policy_ids"],
        }
        for item in elements
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
    _validate_process_protocol_units(elements, normalized_projection)
    _validate_python_units(elements, normalized_projection, normalized_trace)
    _validate_release_units(elements, normalized_projection)
    coverage_identity = {
        "format": COVERAGE_IDENTITY_FORMAT,
        "discovery": discovery,
        "elements": elements,
    }
    trace_identity = {
        "format": TRACE_IDENTITY_FORMAT,
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
        "source_projection_sha256": canonical_sha256(normalized_projection),
        "coverage_sha256": canonical_sha256(coverage_identity),
        "trace_sha256": canonical_sha256(trace_identity),
    }
    counts = _counts(elements)
    counts["source_authorities"] = len(source_index)
    root: dict[str, object] = {
        "format": ROOT_FORMAT,
        "series": normalized_projection["series"],
        "projection_format": normalized_projection["format"],
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
    }
    return DiscoveredContract(root=root)

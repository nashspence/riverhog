"""Repo-internal Riverhog contract-atlas construction and validation package."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
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
)
from .dossier_rendering import _cli_authority_reference, _pretty_json
from .model import (
    ATLAS_DIRECTORY,
    ATLAS_SCHEMA,
    AUDIT_PRIMARY_CONTENT_TARGET_BYTES,
    COVERAGE_IDENTITY_SCHEMA,
    INTERFACE_LABELS,
    INTERFACE_REGISTRY,
    QUALIFICATION_ROUTES,
    RELATIONSHIP_SCHEMA,
    REPRESENTATION_IDENTITY_SCHEMA,
    ROOT_SCHEMA,
    TRACE_IDENTITY_SCHEMA,
    GeneratedDocument,
    NavigationIdentity,
    _decode_unsafe_integers,
    _encoded_json,
    _escape_pointer,
    _semantic_identity,
    canonical_bytes,
    canonical_sha256,
    normalized_json,
    pointer_value,
    reassemble_projection,
    reassemble_trace,
    structural_json_schema,
)
from .model import (
    ContractAtlas as ContractAtlas,
)
from .model import (
    ContractAtlasError as ContractAtlasError,
)
from .navigation import (
    _anchor_id,
    _assign_dossiers,
    _contextual_labels,
    _dossier_navigation_labels,
    _extension_context_path,
    _interface_index_path,
    _interface_navigation_labels,
    _md,
    _policy_anchor,
    _relationship_edge_anchor,
    _relationship_node_anchor,
    _relative_link,
    _source_anchor,
    _subject_anchor,
)
from .relationships import _relationship_model
from .rendering import _render_atlas
from .validation import _atlas_paths, _reachable_atlas_documents, validate_atlas

__all__ = [
    "AUDIT_PRIMARY_CONTENT_TARGET_BYTES",
    "INTERFACE_LABELS",
    "INTERFACE_REGISTRY",
    "RELATIONSHIP_SCHEMA",
    "NavigationIdentity",
    "_anchor_id",
    "_cli_authority_reference",
    "_contextual_labels",
    "_decode_unsafe_integers",
    "_dossier_navigation_labels",
    "_escape_pointer",
    "_extension_context_path",
    "_interface_index_path",
    "_interface_navigation_labels",
    "_md",
    "_policy_anchor",
    "_pretty_json",
    "_reachable_atlas_documents",
    "_relationship_edge_anchor",
    "_relationship_model",
    "_relationship_node_anchor",
    "_relative_link",
    "_source_anchor",
    "_subject_anchor",
    "canonical_bytes",
    "normalized_json",
    "pointer_value",
    "reassemble_projection",
    "reassemble_trace",
    "structural_json_schema",
]


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
        discovery,
        component_descriptions,
        projection_integer_paths=projection_integer_paths,
    )
    for document in documents:
        GeneratedDocument(
            path=str(document["path"]),
            sha256=str(document["sha256"]),
            bytes=cast(int, document["bytes"]),
        )
    representation_identity = {
        "schema": REPRESENTATION_IDENTITY_SCHEMA,
        "documents": documents,
        "relationships": relationship,
    }
    identities["atlas_representation_sha256"] = canonical_sha256(representation_identity)
    counts = _counts(elements)
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


def checked_file_set(path: Path) -> set[Path]:
    atlas = load_atlas(path)
    return {path, *(path.parent / relative for relative in atlas.files)}

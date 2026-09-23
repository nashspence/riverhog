"""Complete machine-closure and generated-atlas validation."""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import cast

from .discovery import (
    _counts,
    _policy_registry,
    _projection_coverage,
    _source_index,
    _validate_authority_registry,
    _validate_process_protocol_units,
    _validate_python_units,
    _validate_release_units,
    _validate_staged_registry,
)
from .dossier_rendering import _local_contract_references, _pretty_json
from .model import (
    ATLAS_DIRECTORY,
    COVERAGE_IDENTITY_SCHEMA,
    INTERFACE_REGISTRY,
    QUALIFICATION_ROUTES,
    RELATIONSHIP_SCHEMA,
    REPRESENTATION_IDENTITY_SCHEMA,
    ROOT_SCHEMA,
    TRACE_IDENTITY_SCHEMA,
    ContractAtlas,
    ContractAtlasError,
    _semantic_identity,
    _slug,
    canonical_sha256,
    pointer_value,
    reassemble_projection,
    reassemble_trace,
)
from .navigation import (
    EXTENT_PRINCIPLES_PATH,
    MACHINE_ARTIFACT_TARGET,
    QUALIFICATION_ROUTES_PATH,
    RELATIONSHIP_EDGES_PATH,
    RELATIONSHIP_NODES_PATH,
    SOURCE_AUTHORITIES_PATH,
    _anchor_id,
    _anchor_link,
    _dossier_navigation_labels,
    _extension_context_path,
    _interface_index_path,
    _interface_label,
    _interface_navigation_labels,
    _md,
    _policy_application_anchor,
    _policy_definition_elements,
    _policy_destination,
    _qualification_anchor,
    _relationship_edge_anchor,
    _relationship_node_anchor,
    _relative_link,
    _repository_source_targets,
    _source_anchor,
    _source_location_links,
    _subject_anchor,
)
from .relationships import _relationship_model
from .rendering import _authority_index_path


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


def _reachable_atlas_documents(
    root_path: str,
    files: Mapping[str, bytes],
    *,
    repository_sources: frozenset[str] | set[str] = frozenset(),
) -> set[str]:
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
                if resolved == MACHINE_ARTIFACT_TARGET and not separator:
                    continue
                repository_target = f"{resolved}#{fragment}" if separator else resolved
                if repository_target in repository_sources:
                    continue
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
        {str(item["interface"]) for item in elements} - set(INTERFACE_REGISTRY)
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
    policies = cast(Mapping[str, object], root["policies"])
    if policies != _policy_registry(projection_value):
        raise ContractAtlasError("policy definitions or declared scope differ from the projection")
    policy_definitions = _policy_definition_elements(elements, policies)
    source_evidence_path = SOURCE_AUTHORITIES_PATH
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
        related_elements = [
            elements_by_id[identity]
            for identity in cast(Sequence[str], item["related_element_ids"])
        ]
        referenced_elements = _local_contract_references(
            str(item["authority"]),
            [
                pointer_value(projection_value, pointer)
                for pointer in cast(Sequence[str], item["pointers"])
            ],
            elements_by_id,
        )
        for heading, targets in (
            ("Related interface records", related_elements),
            ("Referenced contract elements", referenced_elements),
        ):
            # At-use schema links may repeat a destination. The corroboration
            # inventory still lists each target exactly once in its own section.
            section = dossier_text.partition(f"### {heading}\n")[2].split("\n#", 1)[0]
            labels = _dossier_navigation_labels(item, targets)
            for target in targets:
                link = _relative_link(str(item["dossier"]), str(target["dossier"]))
                rendered = f"[{_md(labels[str(target['id'])])}]({link})"
                if section.count(rendered) != 1:
                    raise ContractAtlasError(
                        f"dossier navigation is not exact: {item['id']} -> {target['id']}"
                    )
        if any(
            policy.encode() not in dossier for policy in cast(Sequence[str], item["policy_ids"])
        ):
            raise ContractAtlasError(
                f"atlas dossier does not expose every effective policy: {item['id']}"
            )
        for policy in cast(Sequence[str], item["policy_ids"]):
            link = _anchor_link(
                str(item["dossier"]), *_policy_destination(policy, policy_definitions)
            )
            application_anchor = _policy_application_anchor(str(item["id"]), policy)
            if (
                f"]({link})" not in dossier_text
                or dossier_text.count(f'id="{application_anchor}"') != 1
            ):
                raise ContractAtlasError(
                    f"atlas dossier does not route an exact policy application: {item['id']}"
                )
        if item["extent_decision_ids"]:
            link = _relative_link(str(item["dossier"]), EXTENT_PRINCIPLES_PATH)
            if f"]({link})" not in dossier_text:
                raise ContractAtlasError(
                    f"atlas element omits governing extent principles: {item['id']}"
                )
        for route in cast(Sequence[str], item["qualification_routes"]):
            link = _anchor_link(
                str(item["dossier"]),
                QUALIFICATION_ROUTES_PATH,
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
    candidate_ids = [str(item["id"]) for item in candidates]
    detection_ids = [str(item["id"]) for item in detections]
    resolved_detection_ids = [str(item["detection_id"]) for item in resolutions]
    resolved_candidate_ids = [str(item["candidate_id"]) for item in resolutions]
    candidate_elements = [str(item["element_id"]) for item in candidates if "element_id" in item]
    if (
        len(candidate_elements) != len(set(candidate_elements))
        or set(candidate_elements) != set(ids)
        or protected_candidates != {f"candidate:{identity}" for identity in ids}
        or set(candidate_ids) != protected_candidates
        or len(dispositions) != len(candidate_ids)
        or any(item["disposition"] != "protected" for item in dispositions)
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
    policies = cast(Mapping[str, object], root["policies"])
    declared_policy_ids = {
        str(policy["id"])
        for values in policies.values()
        for policy in cast(Sequence[Mapping[str, object]], values)
    }
    used_policy_ids = {
        policy for item in elements for policy in cast(Sequence[str], item["policy_ids"])
    }
    if not used_policy_ids <= declared_policy_ids:
        raise ContractAtlasError("contract element policy references are unresolved")
    for policy_id in declared_policy_ids:
        policy_target, anchor = _policy_destination(policy_id, policy_definitions)
        policy_page = atlas.files[policy_target].decode()
        if policy_page.count(f'id="{anchor}"') != 1:
            raise ContractAtlasError(f"policy definition has no stable subject: {policy_id}")
    for item in elements:
        if not set(cast(Sequence[str], item["source_authority_ids"])) <= set(source_index):
            raise ContractAtlasError(
                f"contract element source references are unresolved: {item['id']}"
            )

    root_path = str(cast(Mapping[str, object], root["atlas"])["root"])
    root_page = atlas.files[root_path].decode()
    ordered_headings = (
        "> **Audit question:**",
        "## Audit references",
        "## Authorities and interfaces",
    )
    if any(heading not in root_page for heading in ordered_headings):
        raise ContractAtlasError("atlas root lacks scope, references, or exact inventory")
    heading_offsets = [root_page.index(heading) for heading in ordered_headings]
    if heading_offsets != sorted(heading_offsets):
        raise ContractAtlasError("atlas references must be discoverable before the inventory")
    for path in (root_path, f"{ATLAS_DIRECTORY}/evidence/identities.md"):
        if f"]({_relative_link(path, MACHINE_ARTIFACT_TARGET)})" not in atlas.files[path].decode():
            raise ContractAtlasError(f"atlas page omits its exact machine artifact: {path}")
    reachable_documents = _reachable_atlas_documents(
        root_path,
        atlas.files,
        repository_sources=_repository_source_targets(trace_value, list(source_index.values())),
    )
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

    source_evidence_page = atlas.files[SOURCE_AUTHORITIES_PATH].decode()
    qualification_page = atlas.files[QUALIFICATION_ROUTES_PATH].decode()
    command_routes = set(
        cast(
            Mapping[str, object],
            cast(Mapping[str, object], root["counts"])["by_qualification_route"],
        )
    ) | {
        route
        for witness in cast(
            Sequence[Mapping[str, object]], trace_value["segmented_extent_witnesses"]
        )
        for route in cast(Sequence[str], witness["gates"])
    }
    for route in command_routes:
        if (
            f"`{route}`" not in qualification_page
            or qualification_page.count(f'id="{_qualification_anchor(str(route))}"') != 1
        ):
            raise ContractAtlasError(f"human evidence index omits route: {route}")
    for source_id, source in source_index.items():
        rows = [
            line
            for line in source_evidence_page.splitlines()
            if f'id="{_source_anchor(source_id)}"' in line
        ]
        if (
            len(rows) != 1
            or f"`{source_id}`" not in rows[0]
            or any(
                link not in rows[0]
                for link in _source_location_links(SOURCE_AUTHORITIES_PATH, source)
            )
        ):
            raise ContractAtlasError(f"human evidence index omits source: {source_id}")

    observed_counts = _counts(
        elements,
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
            projection_value, elements, component_descriptions
        )
        if relationship != expected_relationship:
            raise ContractAtlasError("atlas relationship navigation is stale")
    interface_counts_by_authority = Counter(
        (str(item["authority"]), str(item["interface"])) for item in elements
    )
    relationship_nodes = cast(Sequence[Mapping[str, object]], relationship["nodes"])
    relationship_edges = cast(Sequence[Mapping[str, object]], relationship["edges"])
    relationship_nodes_by_id = {str(item["id"]): item for item in relationship_nodes}
    if any(path.startswith(f"{ATLAS_DIRECTORY}/relationships/") for path in atlas.files):
        raise ContractAtlasError("relationship evidence must not become a second human map")
    exact_authorities = sorted({str(item["authority"]) for item in elements})
    authority_inventory_path = f"{ATLAS_DIRECTORY}/evidence/authorities.md"
    authority_inventory_page = atlas.files[authority_inventory_path].decode()
    for authority in exact_authorities:
        authority_path = _authority_index_path(authority)
        root_link = _relative_link(root_path, authority_path)
        if root_page.count(f"]({root_link})") != 1:
            raise ContractAtlasError(f"authority inventory omits exact authority: {authority}")
        for interface in sorted(
            {str(item["interface"]) for item in elements if item["authority"] == authority}
        ):
            interface_path = _interface_index_path(authority, interface)
            interface_link = _relative_link(root_path, interface_path)
            if root_page.count(f"]({interface_link})") != 1:
                raise ContractAtlasError(
                    f"authority inventory omits direct interface navigation: "
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
            interface_labels = _interface_navigation_labels(interface, interface_elements)
            for item in interface_elements:
                dossier_link = _relative_link(interface_path, str(item["dossier"]))
                rendered = f"[{_md(interface_labels[str(item['id'])])}]({dossier_link})"
                if interface_page.count(rendered) != 1:
                    raise ContractAtlasError(
                        f"human interface navigation is not exact: {item['id']}"
                    )
    authority_registry = cast(Mapping[str, object], trace_value["authority_registry"])
    for item in cast(Sequence[Mapping[str, object]], authority_registry["declared_authorities"]):
        if item["id"] not in exact_authorities:
            if _md(item["meaning"]) not in authority_inventory_page:
                raise ContractAtlasError(
                    f"authority evidence omits unused aggregate declaration: {item['id']}"
                )
            continue
        aggregate_target = _authority_index_path(str(item["id"]))
        if (
            f"]({_relative_link(authority_inventory_path, aggregate_target)})"
            not in authority_inventory_page
            or _md(item["meaning"]) not in atlas.files[aggregate_target].decode()
        ):
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
    relationship_page = atlas.files[RELATIONSHIP_NODES_PATH].decode()
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
    relationship_page = atlas.files[RELATIONSHIP_EDGES_PATH].decode()
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
                "## Contract elements",
            )
        ):
            raise ContractAtlasError(
                f"extension context duplicates semantic accounting: {extension_id}"
            )
        node_link = _anchor_link(
            extension_path,
            RELATIONSHIP_NODES_PATH,
            _relationship_node_anchor(extension_id),
        )
        if (
            f"- Identity: `{extension_id}`" not in extension_page
            or _md(extension["description"]) not in extension_page
            or f"]({node_link})" not in extension_page
            or "## Supplied implementations" not in extension_page
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
                RELATIONSHIP_EDGES_PATH,
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
                RELATIONSHIP_EDGES_PATH,
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
    if any("/families/" in path for path in atlas.files) or "Semantic families" in root_page:
        raise ContractAtlasError("semantic-family navigation remains in the human atlas")
    for path, descriptor in descriptors.items():
        kind = descriptor["kind"]
        if kind == "root-index":
            expected_counts: Mapping[str, object] = _counts(elements)
        elif kind == "policy-index":
            expected_counts = {
                "policies": sum(len(cast(Sequence[object], value)) for value in policies.values())
            }
        elif kind == "evidence-index":
            expected_counts = {
                "contract_elements": checked_counts["contract_elements"],
                "extent_decisions": checked_counts["extent_decisions"],
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
                "qualification_routes": len(command_routes),
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
        elif kind == "audit-reference":
            expected_counts = {}
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

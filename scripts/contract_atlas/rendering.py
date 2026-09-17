"""Exact Markdown rendering and document inventory for the Riverhog contract atlas."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import cast

from .discovery import _counts, _source_index
from .dossier_rendering import _render_cli_navigation_tree, _render_dossier
from .model import (
    ATLAS_DIRECTORY,
    INTERFACE_REGISTRY,
    ContractAtlasError,
    _decode_unsafe_integers,
    _slug,
)
from .navigation import (
    CONFIGURATION_DOCUMENTS_PATH,
    CONFIGURATION_FAMILIES_PATH,
    CONFIGURATION_SETTINGS_PATH,
    RELATIONSHIP_EDGES_PATH,
    RELATIONSHIP_NODES_PATH,
    _anchor_id,
    _anchor_link,
    _element_progression_witnesses,
    _extension_context_path,
    _html_anchor,
    _interface_index_path,
    _interface_label,
    _interface_navigation_labels,
    _interface_sort_key,
    _md,
    _navigation_identity,
    _policy_definition_elements,
    _qualification_legend,
    _qualified_name_link,
    _relationship_edge_anchor,
    _relationship_node_anchor,
    _relative_link,
    _scope_qualification_path,
)
from .reference_rendering import (
    _render_evidence_references,
    _render_policy_references,
    _render_qualification_scope,
)
from .relationships import _relationship_model


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


def _authority_index_path(authority: str) -> str:
    return f"{ATLAS_DIRECTORY}/authorities/{_slug(authority, limit=72)}/index.md"


def _render_authority_inventory(
    relationship: Mapping[str, object],
    grouped: Mapping[str, Mapping[str, Sequence[Mapping[str, object]]]],
    descriptions: Mapping[str, str],
    qualifications: Mapping[str, Sequence[str]],
    *,
    source: str,
    publication_policy_count: int = 0,
) -> list[str]:
    """Render exact authorities and interfaces without inferred categories."""

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
        authority_name: str,
        interfaces: Mapping[str, Sequence[Mapping[str, object]]],
    ) -> list[str]:
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
        authority_path = _authority_index_path(authority_name)
        affected = any(
            qualifications[str(item["id"])] for values in interfaces.values() for item in values
        )
        name = _qualified_name_link(
            source,
            authority_path,
            authority_name,
            _scope_qualification_path(authority_path) if affected else "",
        )
        result = [f"- {name} — {_md(descriptions[authority_name])}{provider_annotation}"]
        for interface_id, values in sorted(
            interfaces.items(), key=lambda item: _interface_sort_key(item[0])
        ):
            owner_extensions = extensions_by_owner_interface.get((authority_name, interface_id), ())
            owner_annotation = ""
            if owner_extensions:
                kinds = {str(item["kind"]) for item in owner_extensions}
                label = "Defines protocol" if kinds == {"process-protocol"} else "Defines extension"
                if len(owner_extensions) != 1:
                    label += "s"
                owner_annotation = f" — {label}: {extension_links(owner_extensions)}."
            target = _interface_index_path(authority_name, interface_id)
            affected = any(qualifications[str(item["id"])] for item in values)
            name = _qualified_name_link(
                source,
                target,
                _interface_label(interface_id),
                _scope_qualification_path(target) if affected else "",
            )
            result.append(f"  - {name} ({len(values)}){owner_annotation}")
        if authority_name == "release" and publication_policy_count:
            target = f"{ATLAS_DIRECTORY}/policies/publication/index.md"
            result.append(
                f"  - [Publication policies]({_relative_link(source, target)}) "
                f"— {publication_policy_count} policy-owned promises"
            )
        return result

    lines = ["## Authorities and interfaces", ""]
    for authority, interfaces in sorted(grouped.items()):
        lines.extend(render_authority(authority, interfaces))
        lines.append("")
    return lines


def _authority_descriptions(
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    component_descriptions: Mapping[str, str],
) -> dict[str, str]:
    """Use explicit aggregate declarations and component ownership, never name categories."""
    descriptions = dict(component_descriptions)
    external = cast(Mapping[str, object], projection["external_contract"])
    state = cast(Mapping[str, object], external["durable_state"])
    for owner in cast(Sequence[Mapping[str, object]], state["owners"]):
        descriptions[str(owner["id"])] = component_descriptions[str(owner["distribution"])]
    registry = cast(Mapping[str, object], trace["authority_registry"])
    for authority in cast(Sequence[Mapping[str, object]], registry["declared_authorities"]):
        descriptions[str(authority["id"])] = str(authority["meaning"])
    return descriptions


def _interface_qualifications(
    values: Sequence[Mapping[str, object]],
    qualifications: Mapping[str, Sequence[str]],
    interface_path: str,
) -> tuple[list[str], dict[str, str]]:
    witness_sets = {tuple(qualifications[str(item["id"])]) for item in values}
    if not any(witness_sets):
        return [], {}
    lines = _qualification_legend()
    if len(witness_sets) == 1:
        target = _relative_link(interface_path, _scope_qualification_path(interface_path))
        lines.extend(
            [
                f"**All contract elements on this page** [(!)]({target}) share the "
                "same recorded evidence gaps.",
                "",
            ]
        )
        return lines, {}
    return lines, {
        str(
            item["id"]
        ): f" [(!)]({_relative_link(interface_path, str(item['dossier']))}#evidence-gaps)"
        for item in values
        if qualifications[str(item["id"])]
    }


def _render_extension_contexts(
    relationship: Mapping[str, object],
    grouped: Mapping[str, Mapping[str, Sequence[Mapping[str, object]]]],
    qualifications: Mapping[str, Sequence[str]],
) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    """Render non-semantic context for each frozen extension boundary."""

    root_path = f"{ATLAS_DIRECTORY}/index.md"
    evidence_path = RELATIONSHIP_EDGES_PATH
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
            path, RELATIONSHIP_NODES_PATH, _relationship_node_anchor(str(extension["id"]))
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
            affected = any(
                qualifications[str(item["id"])]
                for item in grouped[interface_authority][interface_id]
            )
            name = _qualified_name_link(
                path,
                interface_path,
                f"{interface_authority} · {interface['label']}",
                _scope_qualification_path(interface_path) if affected else "",
            )
            if affected and not any("**(!)**" in line for line in lines):
                lines.extend(_qualification_legend())
            lines.append(f"- {name}{binding}")
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
    discovery: Mapping[str, object],
    component_descriptions: Mapping[str, str],
    *,
    projection_integer_paths: Sequence[str],
) -> tuple[dict[str, bytes], list[dict[str, object]], dict[str, object]]:
    files: dict[str, bytes] = {}
    descriptors: list[dict[str, object]] = []
    by_id = {str(item["id"]): item for item in elements}
    primary_projection = cast(
        Mapping[str, object], _decode_unsafe_integers(projection, projection_integer_paths)
    )
    grouped: dict[str, dict[str, list[dict[str, object]]]] = defaultdict(lambda: defaultdict(list))
    for item in elements:
        grouped[str(item["authority"])][str(item["interface"])].append(item)

    qualifications = _element_progression_witnesses(elements, trace)
    policy_definitions = _policy_definition_elements(elements)
    witnesses = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], trace["segmented_extent_witnesses"])
    }
    reference_paths: set[str] = set()
    descriptions = _authority_descriptions(projection, trace, component_descriptions)
    missing_descriptions = set(grouped) - set(descriptions)
    if missing_descriptions:
        raise ContractAtlasError(
            f"authorities lack declared descriptions: {sorted(missing_descriptions)}"
        )

    for element in elements:
        files[str(element["dossier"])] = _render_dossier(
            element,
            projection,
            trace,
            by_id,
            primary_projection=primary_projection,
            policy_definitions=policy_definitions,
        )

    policy_path = f"{ATLAS_DIRECTORY}/policies/index.md"
    policy_files = _render_policy_references(
        elements, policies, primary_projection, policy_definitions
    )
    files.update(policy_files)
    reference_paths.update(policy_files)

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
            _md(descriptions[authority]),
            "",
            f"Contract elements: **{counts['contract_elements']}** · "
            f"Extent decisions: **{counts['extent_decisions']}**",
            "",
            "## Interfaces",
            "",
        ]
        if any(qualifications[str(item["id"])] for item in authority_elements):
            lines.extend(_qualification_legend())
            scope_path = _scope_qualification_path(authority_path)
            files[scope_path] = _render_qualification_scope(
                authority, scope_path, authority_path, authority_elements, qualifications, witnesses
            )
            reference_paths.add(scope_path)
        if authority == "release":
            target = f"{ATLAS_DIRECTORY}/policies/publication/index.md"
            lines.extend(
                [
                    f"[Publication policies]({_relative_link(authority_path, target)}) "
                    "— promises owned directly by release policy records.",
                    "",
                ]
            )
        for interface, values in sorted(
            interfaces.items(), key=lambda item: _interface_sort_key(item[0])
        ):
            interface_path = (
                f"{ATLAS_DIRECTORY}/authorities/{authority_slug}/"
                f"{_slug(interface, limit=48)}/index.md"
            )
            affected = any(qualifications[str(item["id"])] for item in values)
            name = _qualified_name_link(
                authority_path,
                interface_path,
                _interface_label(interface),
                _scope_qualification_path(interface_path) if affected else "",
            )
            lines.append(f"- {name} ({len(values)})")
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
                INTERFACE_REGISTRY[interface].purpose.format(
                    label=_interface_label(interface), authority=authority
                ),
                "",
                "## Contract elements",
                "",
            ]
            if any(qualifications[str(item["id"])] for item in values):
                scope_path = _scope_qualification_path(interface_path)
                files[scope_path] = _render_qualification_scope(
                    f"{authority}: {_interface_label(interface)}",
                    scope_path,
                    interface_path,
                    values,
                    qualifications,
                    witnesses,
                )
                reference_paths.add(scope_path)
            if interface in {"configuration", "configuration-environment"}:
                registry = cast(Mapping[str, object], trace["configuration_registry"])
                documents = cast(Mapping[str, object], trace["configuration_document_registry"])
                for target, records, label in (
                    (
                        CONFIGURATION_SETTINGS_PATH,
                        registry["records"],
                        "Compare environment settings",
                    ),
                    (
                        CONFIGURATION_FAMILIES_PATH,
                        registry["patterns"],
                        "Compare parameterized families",
                    ),
                    (
                        CONFIGURATION_DOCUMENTS_PATH,
                        documents["candidates"],
                        "Compare configuration documents",
                    ),
                ):
                    if any(
                        record["owner"] == authority
                        for record in cast(Sequence[Mapping[str, object]], records)
                    ):
                        destination = _anchor_link(
                            interface_path, target, _anchor_id("configuration-owner", authority)
                        )
                        lines.extend([f"[{label}]({destination}) for this authority.", ""])
            if any(
                str(policy).startswith("publication/")
                for item in values
                for policy in cast(Sequence[str], item["policy_ids"])
            ):
                target = f"{ATLAS_DIRECTORY}/policies/publication/index.md"
                lines.extend(
                    [
                        f"[Publication policies]({_relative_link(interface_path, target)}) "
                        "govern these publication units.",
                        "",
                    ]
                )
            labels = _interface_navigation_labels(interface, values)
            qualification_lines, suffixes = _interface_qualifications(
                values, qualifications, interface_path
            )
            lines.extend(qualification_lines)
            entries = {
                str(item["id"]): (
                    ("**" if str(item["id"]) in suffixes else "")
                    + f"[{_md(labels[str(item['id'])])}]"
                    f"({_relative_link(interface_path, str(item['dossier']))})"
                    + ("**" if str(item["id"]) in suffixes else "")
                    + suffixes.get(str(item["id"]), "")
                )
                for item in values
            }

            renderer = INTERFACE_REGISTRY[interface].renderer
            if renderer == "durable-state":
                lines.extend(
                    [
                        "| Exact unit | Kind |",
                        "|---|---|",
                    ]
                )
                for state_item in sorted(values, key=lambda value: str(value["title"])):
                    kind = _navigation_identity(state_item).kind
                    if kind is None:
                        raise ContractAtlasError(
                            f"durable-state navigation lacks a kind: {state_item['id']}"
                        )
                    lines.append(f"| {entries[str(state_item['id'])]} | {_md(kind)} |")
            elif renderer == "release":
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
                        f"| {entries[str(release_item['id'])]} | {rendered_classification} |"
                    )
            elif renderer == "cli":
                executable_count = sum(
                    bool(cast(Mapping[str, object], item.get("details", {})).get("executable"))
                    for item in values
                )
                group_count = len(values) - executable_count
                lines.extend(
                    [
                        f"Executable commands: **{executable_count}** · "
                        f"Command groups: **{group_count}**",
                        "",
                        "### Command tree",
                        "",
                        *_render_cli_navigation_tree(
                            values, interface_path=interface_path, suffixes=suffixes
                        ),
                    ]
                )
            elif renderer == "python":
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
                    lines.extend([f"### `{_md(module)}`", ""])
                    for public_identity, item in sorted(exports.items()):
                        lines.append(f"- {entries[str(item['id'])]}")
                        owner_members = sorted(
                            members.get(public_identity, ()), key=lambda value: str(value["title"])
                        )

                        for member in owner_members:
                            lines.append(f"  - {entries[str(member['id'])]}")
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
                for item in values:
                    lines.append(f"- {entries[str(item['id'])]}")
            files[interface_path] = ("\n".join(lines).rstrip() + "\n").encode()

    relationship = _relationship_model(projection, elements, component_descriptions)
    extension_files, extension_metadata = _render_extension_contexts(
        relationship, grouped, qualifications
    )
    files.update(extension_files)

    root_counts = _counts(elements)
    root_path = f"{ATLAS_DIRECTORY}/index.md"
    evidence_path = f"{ATLAS_DIRECTORY}/evidence/index.md"
    authority_evidence_path = f"{ATLAS_DIRECTORY}/evidence/authorities.md"
    configuration_evidence_path = f"{ATLAS_DIRECTORY}/evidence/configuration.md"
    source_evidence_path = f"{ATLAS_DIRECTORY}/evidence/sources.md"
    relationship_evidence_path = f"{ATLAS_DIRECTORY}/evidence/relationships.md"
    identity_evidence_path = f"{ATLAS_DIRECTORY}/evidence/identities.md"
    source_index = _source_index(trace)
    evidence_files = _render_evidence_references(
        elements,
        primary_projection,
        trace,
        identities,
        discovery,
        root_counts,
        source_index,
        relationship,
        qualifications,
        policy_definitions,
    )
    files.update(evidence_files)
    reference_paths.update(evidence_files)
    authority_registry = cast(Mapping[str, object], trace["authority_registry"])
    declared_authorities = cast(
        Sequence[Mapping[str, object]], authority_registry["declared_authorities"]
    )
    noncontractual_projection = cast(
        Sequence[Mapping[str, object]], authority_registry["noncontractual_projection"]
    )
    configuration_counts = cast(
        Mapping[str, object], cast(Mapping[str, object], trace["configuration_registry"])["counts"]
    )
    relationship_nodes = cast(Sequence[Mapping[str, object]], relationship["nodes"])
    relationship_edges = cast(Sequence[Mapping[str, object]], relationship["edges"])

    root_lines = [
        "# Riverhog repository v1 contract audit",
        "",
        "> **Audit question:** Is this exactly the external contract the Riverhog repository "
        "should support for v1 — no more, no less?",
        "",
        "This generated snapshot accounts for the externally exposed surfaces discovered "
        "from this repository revision. Discovery means inclusion; no separate acceptance "
        "decision is required.",
        "",
        f"Included contract elements: **{root_counts['contract_elements']}** · "
        f"Extent decisions: **{root_counts['extent_decisions']}**. "
        "Complete accounting does not establish desirable contracts, behavioral proof, "
        "freeze approval, or release readiness.",
        "",
        str(relationship["reference_policy"]),
        "",
        "## Audit references",
        "",
        f"- [Governing policies]({_relative_link(root_path, policy_path)}) — "
        "compatibility, publication, and extent rules; "
        "applicable contract elements link exact definitions.",
        f"- [Accounting checks]({_relative_link(root_path, evidence_path)}) — "
        "closure results and reconciliation inventories.",
        f"- [Sources and qualifications]({_relative_link(root_path, source_evidence_path)}) — "
        "source bindings, candidate tests, and unestablished obligations; no executed attestation.",
        f"- [Configuration comparison]({_relative_link(root_path, configuration_evidence_path)}) — "
        "owners, consumers, and default expressions across settings.",
        f"- [Declared relationships]({_relative_link(root_path, relationship_evidence_path)}) — "
        "exact dependency, packaging, and extension joins.",
        f"- [Snapshot identities]({_relative_link(root_path, identity_evidence_path)}) "
        f"and [machine artifact](../{ATLAS_DIRECTORY}.json) — "
        "match semantic, accounting, trace, and presentation records.",
        "",
        *(_qualification_legend() if any(qualifications.values()) else []),
        *_render_authority_inventory(
            relationship,
            grouped,
            descriptions,
            qualifications,
            source=root_path,
            publication_policy_count=len(cast(Sequence[object], policies["publication"])),
        ),
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
        elif path == evidence_path:
            document_counts = {
                "contract_elements": root_counts["contract_elements"],
                "extent_decisions": root_counts["extent_decisions"],
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
                    set(cast(Mapping[str, object], root_counts["by_qualification_route"]))
                    | {
                        route
                        for witness in witnesses.values()
                        for route in cast(Sequence[str], witness["gates"])
                    }
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
        elif path in extension_metadata:
            extension_descriptor = extension_metadata[path]
            document_counts = cast(dict[str, object], extension_descriptor["counts"])
            kind = str(extension_descriptor["kind"])
        elif path in reference_paths:
            document_counts = {}
            kind = "audit-reference"
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
                        for key, value in extension_metadata[path].items()
                        if key not in {"kind", "counts"}
                    }
                    if path in extension_metadata
                    else {}
                ),
            }
        )
    return files, descriptors, relationship

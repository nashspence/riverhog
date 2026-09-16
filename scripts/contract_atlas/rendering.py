"""Exact Markdown rendering and document inventory for the Riverhog contract atlas."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from typing import cast

from .discovery import _counts, _source_index
from .dossier_rendering import _render_cli_navigation_tree, _render_dossier
from .model import (
    ATLAS_DIRECTORY,
    INTERFACE_REGISTRY,
    ContractAtlasError,
    _slug,
    pointer_value,
)
from .navigation import (
    _anchor_id,
    _anchor_link,
    _anchor_marker,
    _extension_context_path,
    _html_anchor,
    _interface_index_path,
    _interface_label,
    _interface_navigation_labels,
    _interface_sort_key,
    _md,
    _navigation_identity,
    _policy_anchor,
    _qualification_anchor,
    _relationship_edge_anchor,
    _relationship_node_anchor,
    _relative_link,
    _repository_source_link,
    _source_anchor,
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
        authority: Mapping[str, object],
        *,
        prefix: str,
    ) -> list[str]:
        authority_name = str(authority["authority"])
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
        result = [
            f"{prefix}- [{_md(authority_name)}]"
            f"({_relative_link(source, _authority_index_path(authority_name))}) — "
            f"{_md(authority['purpose'])}{provider_annotation}"
        ]
        for interface in cast(Sequence[Mapping[str, object]], authority["interfaces"]):
            interface_id = str(interface["id"])
            owner_extensions = extensions_by_owner_interface.get((authority_name, interface_id), ())
            owner_annotation = ""
            if owner_extensions:
                kinds = {str(item["kind"]) for item in owner_extensions}
                label = "Defines protocol" if kinds == {"process-protocol"} else "Defines extension"
                if len(owner_extensions) != 1:
                    label += "s"
                owner_annotation = f" — {label}: {extension_links(owner_extensions)}."
            result.append(
                f"{prefix}  - [{_md(interface['label'])}]"
                f"({_relative_link(source, _interface_index_path(authority_name, interface_id))}) "
                f"({interface['contract_elements']}){owner_annotation}"
            )
        return result

    def render_node(node: Mapping[str, object], depth: int) -> list[str]:
        prefix = "  " * depth
        authorities = cast(Sequence[Mapping[str, object]], node["authorities"])
        result = [f"{prefix}- {_map_node_link(source, node)}"]
        for authority in authorities:
            result.extend(render_authority(authority, prefix=f"{prefix}  "))
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
                lines.extend(render_authority(authority, prefix=""))
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
        interfaces = sorted(
            {str(item["interface"]) for item in by_authority[authority]},
            key=_interface_sort_key,
        )
        owners = [
            relationship_nodes[owner]
            for owner in cast(Sequence[str], record["owner_component_ids"])
        ]
        purposes = sorted({str(owner["description"]) for owner in owners})
        lines.append(
            f"| [{_md(authority)}]({_relative_link(source, _authority_index_path(authority))}) | "
            f"{record['contract_elements']} | "
            f"{_md(', '.join(_interface_label(interface) for interface in interfaces))} | "
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
            "and boundary semantics without becoming a separate product surface.",
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
        metadata[path] = {
            "kind": "semantic-surface",
            "counts": counts,
            "map_node_ids": list(node_ids),
        }
    return files, metadata


def _render_extension_contexts(
    relationship: Mapping[str, object],
) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    """Render non-semantic context for each frozen extension boundary."""

    root_path = f"{ATLAS_DIRECTORY}/index.md"
    evidence_path = f"{ATLAS_DIRECTORY}/evidence/relationships.md"
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
            path, evidence_path, _relationship_node_anchor(str(extension["id"]))
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
            lines.append(
                f"- [{_md(interface_authority)} · {_md(interface['label'])}]"
                f"({_relative_link(path, interface_path)}){binding}"
            )
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
) -> tuple[dict[str, bytes], list[dict[str, object]], dict[str, object]]:
    files: dict[str, bytes] = {}
    descriptors: list[dict[str, object]] = []
    source_evidence_path = f"{ATLAS_DIRECTORY}/evidence/sources.md"
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
    )
    policy_applications: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for element in elements:
        for policy_id in cast(Sequence[str], element["policy_ids"]):
            policy_applications[policy_id].append(element)
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
                    f"| [{_md(policy['id'])}]"
                    f"(#{_policy_anchor(str(policy['id']))}) | "
                    f"{applications.get(str(policy['id']), 0)} |"
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
            executable_links = []
            for source in executable_authorities:
                source_target = _anchor_link(
                    policy_path, source_evidence_path, _source_anchor(source)
                )
                executable_links.append(f"  - [{_md(source)}]({source_target})")
            policy_lines.extend(
                [
                    _html_anchor(_policy_anchor(str(policy["id"]))),
                    f"#### `{policy['id']}`",
                    "",
                    rendered_meaning,
                    "",
                    f"- Applicability: `{_md(json.dumps(applicability, ensure_ascii=False))}`",
                    "- Observable result or violation: "
                    f"`{_md(json.dumps(observable, ensure_ascii=False, sort_keys=True))}`",
                    "- Executable authorities:",
                    *executable_links,
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
            "## Interfaces",
            "",
        ]
        for interface, values in sorted(
            interfaces.items(), key=lambda item: _interface_sort_key(item[0])
        ):
            interface_path = (
                f"{ATLAS_DIRECTORY}/authorities/{authority_slug}/"
                f"{_slug(interface, limit=48)}/index.md"
            )
            lines.append(
                f"- [{_interface_label(interface)}]"
                f"({_relative_link(authority_path, interface_path)}) ({len(values)})"
            )
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
                "## Semantic dossiers",
                "",
            ]
            labels = _interface_navigation_labels(interface, values)
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
                    lines.append(
                        f"| [{_md(labels[str(state_item['id'])])}]"
                        f"({_relative_link(interface_path, str(state_item['dossier']))}) | "
                        f"{_md(kind)} |"
                    )
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
                        f"| [{_md(labels[str(release_item['id'])])}]"
                        f"({_relative_link(interface_path, str(release_item['dossier']))}) | "
                        f"{rendered_classification} |"
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
                        *_render_cli_navigation_tree(values, interface_path=interface_path),
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
                        lines.append(
                            f"- [{_md(labels[str(item['id'])])}]"
                            f"({_relative_link(interface_path, str(item['dossier']))})"
                        )
                        owner_members = sorted(
                            members.get(public_identity, ()), key=lambda value: str(value["title"])
                        )

                        for member in owner_members:
                            lines.append(
                                f"  - [{_md(labels[str(member['id'])])}]"
                                f"({_relative_link(interface_path, str(member['dossier']))})"
                            )
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
                    lines.append(
                        f"- [{_md(labels[str(item['id'])])}]"
                        f"({_relative_link(interface_path, str(item['dossier']))})"
                    )
            files[interface_path] = ("\n".join(lines).rstrip() + "\n").encode()

    relationship = _relationship_model(projection, trace, elements, component_descriptions)
    surface_files, surface_metadata = _render_contract_surfaces(relationship, elements)
    extension_files, extension_metadata = _render_extension_contexts(relationship)
    files.update(surface_files)
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
    source_counts = cast(Mapping[str, object], root_counts["by_source_authority"])
    source_lines = [
        "# Source and qualification inventory",
        "",
        f"[Atlas]({_relative_link(source_evidence_path, root_path)}) · "
        f"[Freeze evidence]({_relative_link(source_evidence_path, evidence_path)})",
        "",
        "This page routes audit work to executable sources and qualification commands. "
        "A binding means the source exists; it does not mean a behavioral claim was proved. "
        "This checked-in atlas contains no executed qualification result or CI attestation. "
        "A passing run applies only to its executed checks and exact source SHA; main CI "
        "does not imply release or provider qualification.",
        "",
        "## Qualification routes",
        "",
        *_table_counts(
            cast(Mapping[str, object], root_counts["by_qualification_route"]),
            "Qualification route",
            anchors={
                route: _qualification_anchor(route)
                for route in cast(Mapping[str, object], root_counts["by_qualification_route"])
            },
        ),
        "",
        "## Source authorities",
        "",
        f"Source authorities: **{len(source_index)}**",
        "",
        "| Source authority | Applications | Executable location |",
        "|---|---:|---|",
    ]
    for source_id in source_index:
        count = source_counts.get(source_id, 0)
        source_record = source_index[source_id]
        location = cast(Mapping[str, object], source_record.get("source", {}))
        bindings = cast(Sequence[Mapping[str, object]], source_record.get("bindings", ()))
        declarations = cast(Sequence[Mapping[str, object]], source_record.get("declarations", ()))
        rendered = str(
            location.get(
                "path",
                location.get(
                    "module",
                    bindings[0]["path"]
                    if bindings
                    else declarations[0]["path"]
                    if declarations
                    else source_id,
                ),
            )
        )
        if len(bindings) > 1:
            rendered += f" (+{len(bindings) - 1} binding)"
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        source_lines.append(
            f"| {_html_anchor(_source_anchor(source_id))}`{_md(source_id)}` | {count} | "
            f"`{_md(rendered + symbol)}` |"
        )
    state_sources = [
        source
        for source in source_index.values()
        if str(source["id"]).startswith("state:") and source.get("fixtures")
    ]
    if state_sources:
        source_lines.extend(
            [
                "",
                "## Durable-state fixture evidence",
                "",
                "Fixtures prove restart and introspection behavior; component declarations "
                "above remain the semantic structure authorities.",
                "",
                "| State authority | Fixture | SHA-256 |",
                "|---|---|---|",
            ]
        )
        for state_source in state_sources:
            for fixture in cast(Sequence[Mapping[str, object]], state_source["fixtures"]):
                source_lines.append(
                    f"| `{_md(state_source['id'])}` | `{_md(fixture['path'])}` | "
                    f"`{_md(fixture['sha256'])}` |"
                )
    witnesses = cast(Sequence[Mapping[str, object]], trace["segmented_extent_witnesses"])
    source_lines.extend(
        [
            "",
            "## Progression evidence and open obligations",
            "",
            "Each group lists its unestablished obligations. Test-symbol existence and "
            "owner/reason matching validate routing only. A shared codec test "
            "requires a separate route-wiring argument; mutable browsing carries no implied "
            "snapshot-completeness guarantee.",
            "",
            "| Candidate group | Bound extent decisions | Candidate tests | Unestablished claims |",
            "|---|---:|---:|---|",
        ]
    )
    for witness in witnesses:
        witness_id = str(witness["id"])
        witness_anchor = _anchor_id("extent-witness", witness_id)
        witness_label = (
            f"[{_md(witness_id)}](#{witness_anchor})"
            if witness["test_scopes"]
            else f"{_html_anchor(witness_anchor)}`{_md(witness_id)}`"
        )
        bound_count = sum(
            witness_id in cast(Sequence[str], link.get("segmented_extent_witnesses", ()))
            for link in cast(Sequence[Mapping[str, object]], trace["extent_sources"])
        )
        source_lines.append(
            f"| {witness_label} | "
            f"{bound_count} | {len(cast(Sequence[str], witness['test_node_ids']))} | "
            + ", ".join(
                _md(claim.replace("_", " "))
                for claim in cast(Sequence[str], witness["unestablished_claims"])
            )
            + " |"
        )
    source_lines.extend(["", "### Reviewed test scopes", ""])
    for witness in witnesses:
        if not witness["test_scopes"]:
            continue
        source_lines.extend(
            [
                f"#### {_html_anchor(_anchor_id('extent-witness', str(witness['id'])))}"
                f"{_md(witness['id'])}",
                "",
            ]
        )
        for test in cast(Sequence[Mapping[str, object]], witness["test_scopes"]):
            test_source = cast(Mapping[str, object], test["source"])
            link = _repository_source_link(source_evidence_path, test_source, str(test["node_id"]))
            source_lines.extend([f"- {link}: {_md(test['scope'])}", ""])
    files[source_evidence_path] = ("\n".join(source_lines).rstrip() + "\n").encode()

    authority_registry = cast(Mapping[str, object], trace["authority_registry"])
    declared_authorities = cast(
        Sequence[Mapping[str, object]], authority_registry["declared_authorities"]
    )
    noncontractual_projection = cast(
        Sequence[Mapping[str, object]], authority_registry["noncontractual_projection"]
    )
    configuration_registry = cast(Mapping[str, object], trace["configuration_registry"])
    configuration_document_registry = cast(
        Mapping[str, object], trace["configuration_document_registry"]
    )
    configuration_counts = cast(Mapping[str, object], configuration_registry["counts"])
    configuration_document_counts = cast(
        Mapping[str, object], configuration_document_registry["counts"]
    )
    configuration_coverage = cast(Mapping[str, object], configuration_registry["coverage"])
    configuration_dossiers: dict[str, str] = {}
    for element in elements:
        if element["interface"] != "configuration-environment":
            continue
        for pointer in cast(Sequence[str], element["pointers"]):
            value = pointer_value(projection, pointer)
            if isinstance(value, Mapping) and isinstance(value.get("id"), str):
                configuration_dossiers[str(value["id"])] = str(element["dossier"])
    configuration_document_dossiers = {
        pointer.rsplit("/", 1)[-1].replace("~1", "/").replace("~0", "~"): str(element["dossier"])
        for element in elements
        if element["interface"] == "configuration"
        for pointer in cast(Sequence[str], element["pointers"])
        if pointer.startswith("/external_contract/configuration_documents/")
    }
    configuration_lines = [
        "# Configuration ownership registry",
        "",
        f"[Atlas]({_relative_link(configuration_evidence_path, root_path)}) · "
        f"[Freeze evidence]({_relative_link(configuration_evidence_path, evidence_path)})",
        "",
        "This is the exhaustive discovery and reconciliation view. It does not own any "
        "setting's semantics; every protected entry links to its normative owner's dossier.",
        "",
        "## Coverage",
        "",
        "| Check | Result |",
        "|---|---:|",
        *(
            f"| {_md(name.replace('_', ' '))} | {'pass' if count == 0 else count} |"
            for name, count in configuration_coverage.items()
        ),
        "",
        "## Shape",
        "",
        f"Environment contracts: **{configuration_counts['contracts']}** · "
        f"Configuration documents: **{configuration_document_counts['contracts']}** · "
        f"Unique names: **{configuration_counts['unique_environment_names']}** · "
        f"Parameterized families: **{configuration_counts['patterns']}** · "
        f"Raw implementation reads: **{configuration_counts['detections']}** · "
        f"Explicit ambiguity resolutions: **{configuration_counts['resolution_exceptions']}**",
        "",
        *_table_counts(
            cast(Mapping[str, object], configuration_counts["by_owner"]), "Normative owner"
        ),
        "",
        "## Exact environment contracts",
        "",
        "| Normative owner | Setting | Consumers | Default expressions | Source |",
        "|---|---|---|---|---|",
    ]
    for record in cast(Sequence[Mapping[str, object]], configuration_registry["records"]):
        contract_id = str(record["id"])
        dossier = configuration_dossiers.get(contract_id)
        setting = (
            f"[{_md(record['name'])}]({_relative_link(configuration_evidence_path, dossier)})"
            if dossier is not None
            else f"`{_md(record['name'])}`"
        )
        source_id = str(record["source_authority_id"])
        source_link = _anchor_link(
            configuration_evidence_path,
            source_evidence_path,
            _source_anchor(source_id),
        )
        configuration_lines.append(
            f"| `{_md(record['owner'])}` | {setting} | "
            f"`{_md(', '.join(cast(Sequence[str], record['consumers'])))}` | "
            f"`{_md(', '.join(cast(Sequence[str], record['default_expressions'])))}` | "
            f"[{_md(source_id)}]({source_link}) |"
        )
    patterns = cast(Sequence[Mapping[str, object]], configuration_registry["patterns"])
    if patterns:
        configuration_lines.extend(
            [
                "",
                "## Parameterized families",
                "",
                "| Normative owner | Template | Consumers | Settings |",
                "|---|---|---|---|",
            ]
        )
        for pattern in patterns:
            dossier = configuration_dossiers.get(str(pattern["id"]))
            template = (
                f"[{_md(pattern['template'])}]"
                f"({_relative_link(configuration_evidence_path, dossier)})"
                if dossier is not None
                else f"`{_md(pattern['template'])}`"
            )
            configuration_lines.append(
                f"| `{_md(pattern['owner'])}` | {template} | "
                f"`{_md(', '.join(cast(Sequence[str], pattern['consumers'])))}` | "
                f"`{_md(', '.join(cast(Sequence[str], pattern['settings'])))}` |"
            )
    configuration_lines.extend(
        [
            "",
            "## Exact configuration documents",
            "",
            "| Normative owner | Configuration authority | Consumers | Input shape | Source |",
            "|---|---|---|---|---|",
        ]
    )
    for record in cast(
        Sequence[Mapping[str, object]], configuration_document_registry["candidates"]
    ):
        contract_id = str(record["id"])
        dossier = configuration_document_dossiers.get(contract_id)
        authority = (
            f"[{_md(contract_id)}]({_relative_link(configuration_evidence_path, dossier)})"
            if dossier is not None
            else f"`{_md(contract_id)}`"
        )
        source_id = f"configuration:{contract_id}"
        source_link = _anchor_link(
            configuration_evidence_path,
            source_evidence_path,
            _source_anchor(source_id),
        )
        configuration_lines.append(
            f"| `{_md(record['owner'])}` | {authority} | "
            f"`{_md(', '.join(cast(Sequence[str], record['consumers'])))}` | "
            f"`{_md(', '.join(cast(Sequence[str], record['input_shapes'])))}` | "
            f"[{_md(source_id)}]({source_link}) |"
        )
    files[configuration_evidence_path] = ("\n".join(configuration_lines).rstrip() + "\n").encode()

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
        "## Declared aggregate authorities",
        "",
        "These cross-component authorities are explicit repository decisions. Component and "
        "durable-state authorities come directly from their frozen registries.",
        "",
        "| Authority | Normative scope |",
        "|---|---|",
        *(f"| `{_md(item['id'])}` | {_md(item['meaning'])} |" for item in declared_authorities),
        "",
        "## Non-contractual projection machinery",
        "",
        "These values remain in the exact machine projection for validation, but do not own "
        "external product promises.",
        "",
        "| Projection record | Machine authority | Reason |",
        "|---|---|---|",
        *(
            f"| `{_md(item['id'])}` | "
            f"`{_md(', '.join(cast(Sequence[str], item['pointers'])))}` | "
            f"{_md(item['reason'])} |"
            for item in noncontractual_projection
        ),
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
        node_name = str(node["name"])
        rendered_name = (
            f"[{_md(node_name)}]"
            f"({_relative_link(relationship_evidence_path, _authority_index_path(node_name))})"
            if node_name in grouped
            else f"`{_md(node_name)}`"
        )
        relationship_lines.append(
            f"| {_html_anchor(_relationship_node_anchor(str(node['id'])))}`{_md(node['id'])}` | "
            f"`{_md(node['kind'])}` | {rendered_name} | "
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
        source_node = nodes_by_id[str(edge["source"])]
        target_node = nodes_by_id[str(edge["target"])]
        relationship_lines.append(
            f"| {_html_anchor(_relationship_edge_anchor(edge))}"
            f"[{_md(source_node['name'])}](#{_relationship_node_anchor(str(source_node['id']))}) | "
            f"`{_md(edge['type'])}` | "
            f"[{_md(target_node['name'])}](#{_relationship_node_anchor(str(target_node['id']))}) | "
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
        *(
            f"| {_anchor_marker('identity', str(name))}`{_md(name)}` | `{_md(value)}` |"
            for name, value in identities.items()
        ),
        "",
        f"{_anchor_marker('identity', 'atlas_representation_sha256')}"
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
        "Discovery identifies externally exposed contract surfaces. Every discovered candidate "
        "is included automatically; no separate acceptance decision is required.",
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
        f"| Detected constructs | {len(cast(Sequence[object], discovery['detections']))} |",
        f"| Exact resolutions | {len(cast(Sequence[object], discovery['resolutions']))} |",
        f"| Resolved candidates | {len(cast(Sequence[object], discovery['candidates']))} |",
        f"| Included candidates | {len(cast(Sequence[object], discovery['dispositions']))} |",
        f"| Contract elements | {root_counts['contract_elements']} |",
        f"| Extent decisions | {root_counts['extent_decisions']} |",
        f"| Source authorities | {len(source_index)} |",
        "",
        "## Exact evidence",
        "",
        f"- [Exact authority inventory]({_relative_link(evidence_path, authority_evidence_path)})",
        "- [Configuration ownership registry]"
        f"({_relative_link(evidence_path, configuration_evidence_path)})",
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
        f"[Verify completeness, ownership, identities, and proof.]"
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
        elif path in extension_metadata:
            extension_descriptor = extension_metadata[path]
            document_counts = cast(dict[str, object], extension_descriptor["counts"])
            kind = str(extension_descriptor["kind"])
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
                        for key, value in surface_metadata[path].items()
                        if key not in {"kind", "counts"}
                    }
                    if path in surface_metadata
                    else {}
                ),
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

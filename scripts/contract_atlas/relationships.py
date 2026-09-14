"""Riverhog-specific relationship and guided contract-map construction."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import cast

from .discovery import _source_index
from .model import (
    CONTRACT_MAP_SCHEMA,
    RELATIONSHIP_SCHEMA,
    ContractAtlasError,
    RelationshipEdge,
    RelationshipNode,
    pointer_value,
)
from .navigation import _interface_label, _interface_sort_key


def _relationship_model(
    projection: Mapping[str, object],
    trace: Mapping[str, object],
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
    interface_counts = Counter(
        (str(item["authority"]), str(item["interface"])) for item in elements
    )

    def semantic_interface(authority: str, interface: str) -> dict[str, object]:
        count = interface_counts[(authority, interface)]
        if count == 0:
            raise ContractAtlasError(
                f"extension relationship lacks an existing semantic interface: "
                f"{authority}: {interface}"
            )
        return {
            "authority": authority,
            "interface": interface,
            "label": _interface_label(interface),
            "contract_elements": count,
        }

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
                "semantic_interfaces": [semantic_interface(str(item["owner"]), "python")],
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
                "contract_elements": 0,
                "semantic_interfaces": [
                    semantic_interface(str(item["contract_owner"]), "python"),
                    semantic_interface(str(item["binding_support"]), "process-protocol"),
                    semantic_interface(str(item["binding_support"]), "process-protocol-operations"),
                    semantic_interface(str(item["binding_support"]), "process-protocol-schemas"),
                ],
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
    publication = cast(Mapping[str, object], release["publication"])
    installation_roots = cast(Mapping[str, Mapping[str, object]], publication["installation_roots"])
    installation_methods = {str(value["method"]) for value in installation_roots.values()}
    if len(installation_methods) != 1:
        raise ContractAtlasError("publication installation roots lack one exact method")
    installation_method = next(iter(installation_methods))
    installation_id = f"installation:{installation_method}"
    nodes.append(
        {
            "id": installation_id,
            "kind": "installation",
            "name": installation_method,
            "description": (
                "Coordinated end-user installation roots declared by the release contract."
            ),
            "contract_elements": 0,
        }
    )
    for root, unit in installation_roots.items():
        edges.append(
            {
                "type": "installed-as",
                "source": f"component:{unit['distribution']}",
                "target": installation_id,
                "binding": root,
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
    source_index = _source_index(trace)
    component_nodes = [item for item in nodes if item["kind"] == "component"]
    component_node_by_name = {str(item["name"]): item for item in component_nodes}
    authorities = sorted({str(item["authority"]) for item in elements})
    authority_owners: dict[str, set[str]] = defaultdict(set)

    def paths_in(value: object) -> Iterable[str]:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if key == "path" and isinstance(child, str):
                    yield child
                yield from paths_in(child)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for child in value:
                yield from paths_in(child)

    for authority in authorities:
        matching_name = next((item for item in component_nodes if item["name"] == authority), None)
        if matching_name is not None:
            authority_owners[authority].add(str(matching_name["id"]))
        for element in (item for item in elements if item["authority"] == authority):
            for pointer in cast(Sequence[str], element["pointers"]):
                value = pointer_value(projection, pointer)
                if isinstance(value, Mapping):
                    for field in ("distribution", "consumer"):
                        owner = component_node_by_name.get(str(value.get(field, "")))
                        if owner is not None:
                            authority_owners[authority].add(str(owner["id"]))
            for source_id in cast(Sequence[str], element["source_authority_ids"]):
                source = source_index[source_id]
                for source_path in paths_in(source):
                    candidates = [
                        item
                        for item in component_nodes
                        if source_path == item["path"] or source_path.startswith(f"{item['path']}/")
                    ]
                    if candidates:
                        longest = max(len(str(item["path"])) for item in candidates)
                        authority_owners[authority].update(
                            str(item["id"])
                            for item in candidates
                            if len(str(item["path"])) == longest
                        )

    product_node = next(item for item in nodes if item["id"] == product_images[0])
    map_specs = (
        (
            "release-envelope",
            "Release envelope",
            None,
            None,
        ),
        ("riverhog-product", "Riverhog product", None, None),
        (
            "riverhog-service",
            "Riverhog service",
            "riverhog-product",
            "surfaces/riverhog.md#riverhog-service",
        ),
        (
            "riverhog-contracts",
            "Riverhog-owned contracts and libraries",
            "riverhog-product",
            "surfaces/riverhog.md#riverhog-owned-contracts-and-libraries",
        ),
        (
            "riverhog-implementation",
            "Implementation and build",
            "riverhog-product",
            "surfaces/riverhog.md#implementation-and-build",
        ),
        ("nonnormative-references", "Maintainer-selected nonnormative references", None, None),
        (
            "riverhog-references",
            "Riverhog references",
            "nonnormative-references",
            "surfaces/references.md#riverhog-references",
        ),
        ("gogurt", "Gogurt", "nonnormative-references", "surfaces/references.md#gogurt"),
        (
            "mango-fish",
            "Mango Fish",
            "nonnormative-references",
            "surfaces/references.md#mango-fish",
        ),
        ("piggity", "Piggity", "nonnormative-references", "surfaces/references.md#piggity"),
        ("stove0", "Stove0", "nonnormative-references", "surfaces/stove0.md"),
        ("stove0-application", "Application", "stove0", "surfaces/stove0.md#application"),
        ("stove0-observers", "Observers", "stove0", "surfaces/stove0.md#observers"),
        ("stove0-targets", "Targets", "stove0", "surfaces/stove0.md#targets"),
        ("stove0-review", "Review", "stove0", "surfaces/stove0.md#review"),
        ("stove0-recipes", "Recipes", "stove0", "surfaces/stove0.md#recipes"),
        ("cross-cutting", "Cross-cutting v1 authorities", None, "surfaces/cross-cutting.md"),
    )
    mapped: dict[str, list[dict[str, object]]] = {
        map_id: [] for map_id, _title, _parent, _path in map_specs
    }
    component_by_id = {str(item["id"]): item for item in component_nodes}
    declared_authority_meanings = {
        str(item["id"]): str(item["meaning"])
        for item in cast(
            Sequence[Mapping[str, object]],
            cast(Mapping[str, object], trace["authority_registry"])["declared_authorities"],
        )
    }

    def stove0_node(authority: str) -> str:
        if "review" in authority:
            return "stove0-review"
        if "observer" in authority or authority in {
            "stove0-media-metadata-observer-contracts",
            "stove0-media-sampling-observer-contracts",
        }:
            return "stove0-observers"
        if "recipe" in authority:
            return "stove0-recipes"
        if "target" in authority or "media-archive" in authority:
            return "stove0-targets"
        return "stove0-application"

    for authority in authorities:
        owners = [component_by_id[item] for item in sorted(authority_owners[authority])]
        roles = {str(item["role"]) for item in owners}
        if authority == "release":
            map_id = "release-envelope"
        elif authority == product_node["name"]:
            map_id = "riverhog-service"
        elif owners and all(
            item["role"] in {"reference_application", "reference_component"}
            or (item["role"] == "reusable_library" and str(item["path"]).startswith("reference/"))
            for item in owners
        ):
            if authority.startswith("stove0"):
                map_id = stove0_node(authority)
            elif authority.startswith("gogurt"):
                map_id = "gogurt"
            elif authority.startswith("mango-fish"):
                map_id = "mango-fish"
            elif authority.startswith("piggity"):
                map_id = "piggity"
            else:
                map_id = "riverhog-references"
        elif owners and roles == {"reusable_library"}:
            map_id = "riverhog-contracts"
        elif owners and roles <= {"deployed_implementation", "internal_build_unit"}:
            map_id = "riverhog-implementation"
        else:
            map_id = "cross-cutting"
        purpose = declared_authority_meanings.get(authority)
        if purpose is None:
            purpose = " ".join(sorted({str(item["description"]) for item in owners}))
        if not purpose:
            purpose = "Cross-cutting generated contract authority."
        direct_interfaces = [
            {
                "id": interface,
                "label": _interface_label(interface),
                "contract_elements": count,
            }
            for (candidate_authority, interface), count in sorted(
                interface_counts.items(),
                key=lambda item: (item[0][0], _interface_sort_key(item[0][1])),
            )
            if candidate_authority == authority
        ]
        mapped[map_id].append(
            {
                "authority": authority,
                "contract_elements": authority_counts[authority],
                "owner_component_ids": sorted(authority_owners[authority]),
                "purpose": purpose,
                "interfaces": direct_interfaces,
            }
        )
    contract_map_nodes = [
        {
            "id": map_id,
            "title": title,
            "parent": parent,
            **({"path": path} if path is not None else {}),
            "authorities": mapped[map_id],
        }
        for map_id, title, parent, path in map_specs
    ]
    mapped_authorities = [
        str(item["authority"])
        for node in contract_map_nodes
        for item in cast(Sequence[Mapping[str, object]], node["authorities"])
    ]
    if sorted(mapped_authorities) != authorities or len(mapped_authorities) != len(
        set(mapped_authorities)
    ):
        raise ContractAtlasError("human contract map does not partition exact authorities")
    for node in nodes:
        RelationshipNode(str(node["id"]), str(node["kind"]), str(node["name"]))
    for edge in edges:
        RelationshipEdge(str(edge["type"]), str(edge["source"]), str(edge["target"]))
    return {
        "schema": RELATIONSHIP_SCHEMA,
        "center": product_nodes[0],
        "product": product_images[0],
        "reference_policy": boundaries["reference_policy"],
        "contract_map": {
            "schema": CONTRACT_MAP_SCHEMA,
            "nodes": contract_map_nodes,
        },
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

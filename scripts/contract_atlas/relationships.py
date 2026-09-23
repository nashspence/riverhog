"""Declared component, publication, and extension relationships."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import cast

from .model import (
    RELATIONSHIP_SCHEMA,
    ContractAtlasError,
    RelationshipEdge,
    RelationshipNode,
)
from .navigation import _interface_label


def _relationship_model(
    projection: Mapping[str, object],
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
    for node in nodes:
        RelationshipNode(str(node["id"]), str(node["kind"]), str(node["name"]))
    for edge in edges:
        RelationshipEdge(str(edge["type"]), str(edge["source"]), str(edge["target"]))
    return {
        "schema": RELATIONSHIP_SCHEMA,
        "center": product_nodes[0],
        "product": product_images[0],
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

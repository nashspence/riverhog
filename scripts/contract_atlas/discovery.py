"""Discovery, ownership, and machine-closure construction for the Riverhog atlas."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from pathlib import PurePosixPath
from typing import cast

import extent_contract

from .model import (
    DETECTOR_CLOSURE_FORMAT,
    QUALIFICATION_ROUTES,
    ContractAtlasError,
    ContractElement,
    _escape_pointer,
    _slug,
    canonical_sha256,
    pointer_value,
)

DETECTORS: tuple[dict[str, str], ...] = (
    {"id": "cli-tree", "authority": "installed parser tree"},
    {"id": "configuration-document", "authority": "validated configuration schema"},
    {"id": "configuration-environment", "authority": "executable environment binding"},
    {"id": "durable-state", "authority": "checked current state baseline"},
    {"id": "extent", "authority": "exhaustive extent classifier"},
    {"id": "http-openapi", "authority": "running ASGI app"},
    {"id": "operation-matrix", "authority": "executable operation parity matrix"},
    {
        "id": "process-protocol-bundle",
        "authority": "generated process-protocol bundle",
    },
    {"id": "protocol-schema", "authority": "published standalone schema document"},
    {
        "id": "python-public-unit",
        "authority": (
            "declared release-package export or public member declared on its class "
            "or inherited from a release-owned package, using Python method resolution"
        ),
    },
    {"id": "release-metadata", "authority": "validated release contract"},
)


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
        }
    )
    return dict(sorted(sources.items()))


def _policy_registry(projection: Mapping[str, object]) -> dict[str, object]:
    external = cast(Mapping[str, object], projection["external_contract"])
    release = cast(Mapping[str, object], external["release"])
    compatibility = cast(Mapping[str, object], release["compatibility"])
    publication = cast(Mapping[str, object], release["publication"])
    publication_policies = cast(Mapping[str, object], publication["policy"])
    publication_base = "/external_contract/release/publication"
    distribution_pointers = [
        f"{publication_base}/distributions/{_escape_pointer(str(name))}"
        for name in sorted(cast(Mapping[str, object], publication["distributions"]))
    ]
    image_pointers = [
        f"{publication_base}/runtime_images/{_escape_pointer(str(name))}"
        for name in sorted(cast(Mapping[str, object], publication["runtime_images"]))
    ]
    installation_pointers = [
        f"{publication_base}/installation_roots/{_escape_pointer(str(name))}"
        for name in sorted(cast(Mapping[str, object], publication["installation_roots"]))
    ]
    extents = cast(Mapping[str, object], external["extents"])
    return {
        "compatibility": [
            {
                "id": f"compatibility/{key.replace('_', '-')}/v1",
                "meaning": value,
                "definition_pointer": f"/external_contract/release/compatibility/{key}",
            }
            for key, value in sorted(compatibility.items())
        ],
        "publication": [
            {
                "id": "publication/role-retention/v1",
                "meaning": publication_policies["role_retention"],
                "definition_pointer": f"{publication_base}/policy/role_retention",
                "applies_to": [
                    *distribution_pointers,
                    *image_pointers,
                    *installation_pointers,
                ],
            },
            {
                "id": "publication/platform-scope/v1",
                "meaning": publication_policies["platform_scope"],
                "definition_pointer": f"{publication_base}/policy/platform_scope",
                "applies_to": [*image_pointers, *installation_pointers],
            },
            {
                "id": "publication/image-identity-scope/v1",
                "meaning": publication_policies["image_identity_scope"],
                "definition_pointer": f"{publication_base}/policy/image_identity_scope",
                "applies_to": image_pointers,
            },
        ],
        "extent_principles": [
            {
                "id": f"extent-principle/{key.replace('_', '-')}/v1",
                "meaning": value,
                "definition_pointer": (
                    f"/external_contract/extents/principles/{_escape_pointer(key)}"
                ),
                "applies_to": ["/external_contract/extents"],
            }
            for key, value in sorted(cast(Mapping[str, object], extents["principles"]).items())
        ],
        "extent_rules": [
            {
                "id": f"extent-rule/{key}",
                "meaning": value,
                "definition_pointer": f"/external_contract/extents/rules/{_escape_pointer(key)}",
                "applies_to": ["/external_contract/extents"],
            }
            for key, value in sorted(
                cast(
                    Mapping[str, object], extent_contract.normative_extent_declarations()["rules"]
                ).items()
            )
        ],
    }


def _compatibility_policies(interface: str) -> list[str]:
    mapping = {
        "cli": "compatibility/cli/v1",
        "configuration": "compatibility/configuration/v1",
        "configuration-environment": "compatibility/configuration/v1",
        "durable-state": "compatibility/durable-state/v1",
        "http-operations": "compatibility/http-api/v1",
        "http-schemas": "compatibility/http-api/v1",
        "http-security-schemes": "compatibility/http-api/v1",
        "http-service-declaration": "compatibility/http-api/v1",
        "process-protocol": "compatibility/components/v1",
        "process-protocol-operations": "compatibility/components/v1",
        "process-protocol-schemas": "compatibility/components/v1",
        "python": "compatibility/python-api/v1",
        "artifact-verification": "compatibility/components/v1",
        "installation-roots": "compatibility/components/v1",
        "publication-locations": "compatibility/components/v1",
        "python-distributions": "compatibility/components/v1",
        "release-artifacts": "compatibility/components/v1",
        "runtime-images": "compatibility/components/v1",
        "versioning-tags": "compatibility/components/v1",
        "schema": "compatibility/components/v1",
    }
    if interface in {"extent", "compatibility-guarantees", "publication-policies"}:
        return []
    return [mapping[interface]]


def _element_id(authority: str, interface: str, title: str, pointers: Sequence[str]) -> str:
    digest = canonical_sha256([authority, interface, title, list(pointers)])[:10]
    return f"{interface}:{_slug(authority, limit=42)}:{_slug(title, limit=52)}:{digest}"


def _add_element(
    elements: list[dict[str, object]],
    *,
    authority: str,
    interface: str,
    title: str,
    pointers: Sequence[str],
    detector: str,
    source_ids: Iterable[str],
    details: Mapping[str, object] | None = None,
) -> dict[str, object]:
    normalized_pointers = sorted(set(pointers))
    item: dict[str, object] = {
        "id": _element_id(authority, interface, title, normalized_pointers),
        "authority": authority,
        "interface": interface,
        "title": title,
        "pointers": normalized_pointers,
        "detector": detector,
        "disposition": "protected",
        "policy_ids": _compatibility_policies(interface),
        "source_authority_ids": sorted({"generator:contract-projection", *source_ids}),
        "qualification_routes": list(QUALIFICATION_ROUTES[interface]),
        "extent_decision_ids": [],
        "related_element_ids": [],
    }
    if details:
        item["details"] = dict(details)
    ContractElement.from_mapping(item)
    elements.append(item)
    return item


def _walk_cli(
    elements: list[dict[str, object]],
    authority: str,
    node: Mapping[str, object],
    pointer: str,
    command_path: tuple[str, ...],
    *,
    source_id: str,
) -> None:
    name = str(node.get("name") or (command_path[-1] if command_path else authority))
    current_path = (*command_path, name)
    pointers = [
        f"{pointer}/{key}"
        for key in (
            "name",
            "parameters",
            "subcommand_required",
            "allow_abbrev",
            "allow_extra_args",
            "allow_interspersed_args",
            "ignore_unknown_options",
            "mutually_exclusive_groups",
            "terminating_controls",
            "result_contract",
        )
        if key in node
    ]
    result_contract = node.get("result_contract")
    details: dict[str, object] = {"command_path": list(current_path)}
    if isinstance(result_contract, Mapping):
        details.update(
            {
                "executable": True,
                "result_identity": result_contract["identity"],
                "result_profile_id": result_contract["profile_id"],
                "structured_output": result_contract["structured_output"],
                "terminating_control_count": len(
                    cast(Sequence[object], node.get("terminating_controls", ()))
                ),
            }
        )
    _add_element(
        elements,
        authority=authority,
        interface="cli",
        title=" ".join(current_path),
        pointers=pointers,
        detector="cli-tree",
        source_ids=[source_id],
        details=details,
    )
    for child_name, child in sorted(
        cast(Mapping[str, Mapping[str, object]], node.get("commands", {})).items()
    ):
        _walk_cli(
            elements,
            authority,
            child,
            f"{pointer}/commands/{_escape_pointer(child_name)}",
            current_path,
            source_id=source_id,
        )


def _source_component(location: Mapping[str, object], projection: Mapping[str, object]) -> str:
    """Resolve a source against declared project roots, never import-name spelling."""
    path = location.get("path")
    if (
        not isinstance(path, str)
        or PurePosixPath(path).is_absolute()
        or ".." in PurePosixPath(path).parts
    ):
        raise ContractAtlasError(f"source has no declared project owner: {location}")
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    components = cast(Sequence[Mapping[str, object]], boundaries["components"])
    candidates = [
        component
        for component in components
        if path == component["path"] or path.startswith(f"{component['path']}/")
    ]
    if not candidates:
        raise ContractAtlasError(f"source has no declared project owner: {location}")
    longest = max(len(str(component["path"])) for component in candidates)
    owners = [component for component in candidates if len(str(component["path"])) == longest]
    if len(owners) != 1:
        raise ContractAtlasError(f"source has ambiguous project ownership: {location}")
    return str(owners[0]["distribution"])


def _cli_binding_element(
    binding: Mapping[str, object],
    elements: Sequence[Mapping[str, object]],
    projection: Mapping[str, object],
) -> Mapping[str, object]:
    """Operation-matrix commands are root-relative, as recorded by its parser walk."""
    _source_component(cast(Mapping[str, object], binding["source"]), projection)
    command = str(binding["command"])
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    executable = binding.get("executable")
    owners = [
        str(component["distribution"])
        for component in cast(Sequence[Mapping[str, object]], boundaries["components"])
        if executable in cast(Mapping[str, object], component["console_scripts"])
    ]
    if len(owners) != 1:
        raise ContractAtlasError(f"CLI executable has no unique declared owner: {executable}")
    owner = owners[0]
    candidates = []
    for element in elements:
        if element["interface"] != "cli" or element["authority"] != owner:
            continue
        details = cast(Mapping[str, object], element.get("details", {}))
        path = cast(Sequence[str], details.get("command_path", ()))
        if (
            path
            and path[0] == executable
            and " ".join(path[1:]) == command
            and details.get("result_identity") == binding.get("result_identity")
            and binding.get("result_identity")
        ):
            candidates.append(element)
    if len(candidates) != 1:
        raise ContractAtlasError(
            f"operation CLI binding has no unique executable command: {owner}: {command}: "
            f"{len(candidates)} candidates"
        )
    return candidates[0]


def _publication_classification(role: str) -> str:
    classifications = {
        "deployed_implementation",
        "end_user_artifact",
        "reusable_library",
        "internal_build_unit",
        "application",
        "component",
    }
    if role not in classifications:
        raise ContractAtlasError(f"published unit has no release classification: {role}")
    return role


def _release_elements(elements: list[dict[str, object]], release: Mapping[str, object]) -> None:
    publication = cast(Mapping[str, object], release["publication"])
    base = "/external_contract/release/publication"
    common_sources = ["release:release.toml", "release-publication:planner"]

    for section, interface in (
        ("versioning", "versioning-tags"),
        ("coordinates", "publication-locations"),
        ("policy", "publication-policies"),
    ):
        for name in sorted(cast(Mapping[str, object], publication[section])):
            _add_element(
                elements,
                authority="release",
                interface=interface,
                title=f"{section.title()}: {name.replace('_', ' ')}",
                pointers=[f"{base}/{section}/{_escape_pointer(name)}"],
                detector="release-metadata",
                source_ids=common_sources,
            )

    distributions = cast(Mapping[str, Mapping[str, object]], publication["distributions"])
    for name, unit in sorted(distributions.items()):
        role = str(unit["role"])
        _add_element(
            elements,
            authority="release",
            interface="python-distributions",
            title=f"Python distribution: {name}",
            pointers=[f"{base}/distributions/{_escape_pointer(name)}"],
            detector="release-metadata",
            source_ids=[*common_sources, f"release-distribution:{name}"],
            details={
                "classification": _publication_classification(role),
                "publication_kind": "distribution",
                "role": role,
                "semantic_owners": [name],
            },
        )
        elements[-1]["policy_ids"] = sorted(
            {
                *cast(Sequence[str], elements[-1]["policy_ids"]),
                "publication/role-retention/v1",
            }
        )

    for target, unit in sorted(
        cast(Mapping[str, Mapping[str, object]], publication["runtime_images"]).items()
    ):
        roots = [str(item) for item in cast(Sequence[object], unit["distribution_roots"])]
        root_roles = {str(distributions[root]["role"]) for root in roots}
        if unit["role"] == "product":
            classification = "product"
        elif len(root_roles) == 1:
            classification = _publication_classification(next(iter(root_roles)))
        else:
            raise ContractAtlasError(f"runtime image crosses publication roles: {target}")
        _add_element(
            elements,
            authority="release",
            interface="runtime-images",
            title=f"Runtime image: {target}",
            pointers=[f"{base}/runtime_images/{_escape_pointer(target)}"],
            detector="release-metadata",
            source_ids=[*common_sources, "release-images:docker-bake"],
            details={
                "classification": classification,
                "publication_kind": "runtime-image",
                "role": unit["role"],
                "semantic_owners": roots,
            },
        )
        elements[-1]["policy_ids"] = sorted(
            {
                *cast(Sequence[str], elements[-1]["policy_ids"]),
                "publication/image-identity-scope/v1",
                "publication/platform-scope/v1",
                "publication/role-retention/v1",
            }
        )

    for name, unit in sorted(
        cast(Mapping[str, Mapping[str, object]], publication["installation_roots"]).items()
    ):
        distribution = str(unit["distribution"])
        role = str(distributions[distribution]["role"])
        _add_element(
            elements,
            authority="release",
            interface="installation-roots",
            title=f"Installation root: {name}",
            pointers=[f"{base}/installation_roots/{_escape_pointer(name)}"],
            detector="release-metadata",
            source_ids=[
                *common_sources,
                "release-installation:planner",
                f"release-distribution:{distribution}",
            ],
            details={
                "classification": _publication_classification(role),
                "publication_kind": "installation-root",
                "role": role,
                "semantic_owners": [distribution],
            },
        )
        elements[-1]["policy_ids"] = sorted(
            {
                *cast(Sequence[str], elements[-1]["policy_ids"]),
                "publication/platform-scope/v1",
                "publication/role-retention/v1",
            }
        )

    for section, title_prefix, interface in (
        ("release_artifacts", "Release artifact", "release-artifacts"),
        ("trust", "Trust", "artifact-verification"),
    ):
        for name in sorted(cast(Mapping[str, object], publication[section])):
            _add_element(
                elements,
                authority="release",
                interface=interface,
                title=f"{title_prefix}: {name}",
                pointers=[f"{base}/{section}/{_escape_pointer(name)}"],
                detector="release-metadata",
                source_ids=common_sources,
            )

    compatibility = cast(Mapping[str, object], release["compatibility"])
    for policy_name in sorted(compatibility):
        _add_element(
            elements,
            authority="release",
            interface="compatibility-guarantees",
            title=f"Compatibility: {policy_name.replace('_', ' ')}",
            pointers=[f"/external_contract/release/compatibility/{_escape_pointer(policy_name)}"],
            detector="release-metadata",
            source_ids=["release:release.toml"],
        )


def _external_elements(
    projection: Mapping[str, object], trace: Mapping[str, object]
) -> list[dict[str, object]]:
    external = cast(Mapping[str, object], projection["external_contract"])
    sources = _source_index(trace)
    elements: list[dict[str, object]] = []

    _release_elements(elements, cast(Mapping[str, object], external["release"]))

    http = cast(Mapping[str, Mapping[str, object]], external["http_openapi"])
    for authority, document in sorted(http.items()):
        metadata_pointers = [
            f"/external_contract/http_openapi/{_escape_pointer(authority)}/{key}"
            for key in ("openapi", "info")
            if key in document
        ]
        _add_element(
            elements,
            authority=authority,
            interface="http-service-declaration",
            title=f"{authority} HTTP service",
            pointers=metadata_pointers,
            detector="http-openapi",
            source_ids=[f"openapi:{authority}"],
        )
        paths = cast(Mapping[str, Mapping[str, object]], document.get("paths", {}))
        for path, path_item in sorted(paths.items()):
            for method, operation in sorted(path_item.items()):
                pointer = (
                    f"/external_contract/http_openapi/{_escape_pointer(authority)}/paths/"
                    f"{_escape_pointer(path)}/{_escape_pointer(method)}"
                )
                if method.casefold() not in {
                    "delete",
                    "get",
                    "head",
                    "options",
                    "patch",
                    "post",
                    "put",
                }:
                    title = f"{path} {method} metadata"
                else:
                    title = f"{method.upper()} {path}"
                details = {
                    "method": method.upper(),
                    "path": path,
                    **(
                        {"operation_id": operation["operationId"]}
                        if isinstance(operation, Mapping) and "operationId" in operation
                        else {}
                    ),
                }
                _add_element(
                    elements,
                    authority=authority,
                    interface="http-operations",
                    title=title,
                    pointers=[pointer],
                    detector="http-openapi",
                    source_ids=[f"openapi:{authority}"],
                    details=details,
                )
        components = cast(Mapping[str, object], document.get("components", {}))
        component_interfaces = {
            "schemas": "http-schemas",
            "securitySchemes": "http-security-schemes",
        }
        unexpected_components = sorted(set(components) - set(component_interfaces))
        if unexpected_components:
            raise ContractAtlasError(
                f"OpenAPI components lack an explicit navigation home: "
                f"{authority}: {unexpected_components}"
            )
        for component_kind, values in sorted(components.items()):
            if not isinstance(values, Mapping):
                raise ContractAtlasError(
                    f"OpenAPI component collection is not a mapping: {authority}: {component_kind}"
                )
            for name in sorted(values):
                _add_element(
                    elements,
                    authority=authority,
                    interface=component_interfaces[component_kind],
                    title=f"{component_kind}: {name}",
                    pointers=[
                        f"/external_contract/http_openapi/{_escape_pointer(authority)}/"
                        f"components/{_escape_pointer(component_kind)}/{_escape_pointer(name)}"
                    ],
                    detector="http-openapi",
                    source_ids=[f"openapi:{authority}"],
                )

    for index, operation in enumerate(
        cast(Sequence[Mapping[str, object]], external["http_route_supplements"])
    ):
        authority = str(operation["application"])
        operation_id = str(operation["operation_id"])
        element = _add_element(
            elements,
            authority=authority,
            interface="http-operations",
            title=f"{operation['method']} {operation['path']}",
            pointers=[f"/external_contract/http_route_supplements/{index}"],
            detector="http-openapi",
            source_ids=[f"openapi:{authority}"],
            details={
                "method": operation["method"],
                "path": operation["path"],
                "operation_id": operation_id,
                "supplemental": True,
            },
        )
        element["policy_ids"] = ["compatibility/http-api/v1"]

    for authority, node in sorted(
        cast(Mapping[str, Mapping[str, object]], external["cli"]).items()
    ):
        source_id = f"cli:{authority}"
        owner = str(sources[source_id]["owner"])
        _walk_cli(
            elements,
            owner,
            node,
            f"/external_contract/cli/{_escape_pointer(authority)}",
            (),
            source_id=source_id,
        )

    for section in ("configuration_environment", "configuration_environment_patterns"):
        for index, item in enumerate(
            cast(Sequence[Mapping[str, object]], external.get(section, []))
        ):
            name = str(item.get("name", item.get("template", f"{section}-{index}")))
            authority = str(item["owner"])
            _add_element(
                elements,
                authority=authority,
                interface="configuration-environment",
                title=name,
                pointers=[f"/external_contract/{section}/{index}"],
                detector="configuration-environment",
                source_ids=[
                    (f"configuration-environment-pattern:{authority}:{item['template']}")
                    if section.endswith("patterns")
                    else f"configuration-environment:{authority}:{name}",
                ],
                details={
                    "consumers": list(cast(Sequence[str], item["consumers"])),
                    "input_shape": item["input_shape"],
                    **(
                        {
                            "default_expressions": list(
                                cast(Sequence[str], item["default_expressions"])
                            )
                        }
                        if "default_expressions" in item
                        else {}
                    ),
                },
            )

    for authority in sorted(cast(Mapping[str, object], external["configuration_documents"])):
        source_id = f"configuration:{authority}"
        _add_element(
            elements,
            authority=str(sources[source_id]["owner"]),
            interface="configuration",
            title=f"{authority} configuration",
            pointers=[f"/external_contract/configuration_documents/{_escape_pointer(authority)}"],
            detector="configuration-document",
            source_ids=[source_id],
        )

    for schema_authority, document in sorted(
        cast(Mapping[str, Mapping[str, object]], external["protocol_schemas"]).items()
    ):
        authority = _source_component(
            cast(Mapping[str, object], sources[f"protocol:{schema_authority}"]["source"]),
            projection,
        )
        base = f"/external_contract/protocol_schemas/{_escape_pointer(schema_authority)}"
        schemas = document.get("schemas")
        if schema_authority.startswith("generated:") and isinstance(schemas, Mapping):
            binding = document.get("http_binding")
            if not isinstance(binding, Mapping) or set(binding) != {"operations"}:
                raise ContractAtlasError(
                    "generated process protocol has an unexpected binding shape: "
                    f"{schema_authority}"
                )
            operations = binding["operations"]
            if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes)):
                raise ContractAtlasError(
                    f"generated process protocol operations are not a sequence: {schema_authority}"
                )
            metadata = [
                f"{base}/{_escape_pointer(key)}"
                for key in document
                if key not in {"http_binding", "schemas"}
            ]
            protocol_element = _add_element(
                elements,
                authority=authority,
                interface="process-protocol",
                title=f"{schema_authority} protocol",
                pointers=metadata,
                detector="process-protocol-bundle",
                source_ids=[f"protocol:{schema_authority}"],
            )
            related: list[dict[str, object]] = []
            for index, operation in enumerate(operations):
                if not isinstance(operation, Mapping):
                    raise ContractAtlasError(
                        "generated process protocol operation is not a mapping: "
                        f"{schema_authority}: {index}"
                    )
                operation_method = operation.get("method")
                operation_path = operation.get("path")
                if not isinstance(operation_method, str) or not isinstance(operation_path, str):
                    raise ContractAtlasError(
                        "generated process protocol operation lacks method/path: "
                        f"{schema_authority}: {index}"
                    )
                related.append(
                    _add_element(
                        elements,
                        authority=authority,
                        interface="process-protocol-operations",
                        title=f"{operation_method.upper()} {operation_path}",
                        pointers=[f"{base}/http_binding/operations/{index}"],
                        detector="process-protocol-bundle",
                        source_ids=[f"protocol:{schema_authority}"],
                        details={
                            "method": operation_method.upper(),
                            "path": operation_path,
                        },
                    )
                )
            for name in sorted(schemas):
                related.append(
                    _add_element(
                        elements,
                        authority=authority,
                        interface="process-protocol-schemas",
                        title=f"{schema_authority}: {name}",
                        pointers=[f"{base}/schemas/{_escape_pointer(name)}"],
                        detector="process-protocol-bundle",
                        source_ids=[f"protocol:{schema_authority}"],
                    )
                )
            cast(list[str], protocol_element["related_element_ids"]).extend(
                str(item["id"]) for item in related
            )
            for item in related:
                cast(list[str], item["related_element_ids"]).append(str(protocol_element["id"]))
        else:
            _add_element(
                elements,
                authority=authority,
                interface="schema",
                title=str(document.get("title", schema_authority)),
                pointers=[base],
                detector="protocol-schema",
                source_ids=[f"protocol:{schema_authority}"],
            )

    python_elements: dict[str, dict[str, object]] = {}
    for public_identity, surface in sorted(
        cast(Mapping[str, Mapping[str, object]], external["python"]).items()
    ):
        authority = str(surface["distribution"])
        module = str(surface["module"])
        unit = str(surface["unit"])
        item = _add_element(
            elements,
            authority=authority,
            interface="python",
            title=public_identity,
            pointers=[f"/external_contract/python/{_escape_pointer(public_identity)}"],
            detector="python-public-unit",
            source_ids=[f"python:{authority}:{module}"],
            details={
                "module": module,
                "public_identity": public_identity,
                "unit": unit,
                **({"owner": surface["owner"]} if "owner" in surface else {}),
            },
        )
        python_elements[public_identity] = item
    for public_identity, python_element in python_elements.items():
        python_details = cast(Mapping[str, object], python_element["details"])
        owner_identity = python_details.get("owner")
        if owner_identity is None:
            continue
        owner_item = python_elements.get(str(owner_identity))
        if owner_item is None:
            raise ContractAtlasError(
                f"Python public member lacks its exported owner: {public_identity}"
            )
        cast(list[str], python_element["related_element_ids"]).append(str(owner_item["id"]))
        cast(list[str], owner_item["related_element_ids"]).append(str(python_element["id"]))

    state = cast(Mapping[str, object], external["durable_state"])
    for index, state_owner in enumerate(cast(Sequence[Mapping[str, object]], state["owners"])):
        authority = str(state_owner["id"])
        base = f"/external_contract/durable_state/owners/{index}"
        structure = cast(Mapping[str, object], state_owner["structure"])
        structure_kind = str(structure.get("kind", ""))
        split_keys = {
            "relational-schema": {"tables", "unique_indexes"},
            "json-documents": {"documents"},
            "composite": {"units"},
        }.get(structure_kind)
        if split_keys is None:
            _add_element(
                elements,
                authority=authority,
                interface="durable-state",
                title=f"{authority} durable state",
                pointers=[base],
                detector="durable-state",
                source_ids=[f"state:{authority}"],
                details={"state_owner": authority, "state_unit": structure_kind},
            )
            continue
        collection_keys = {key for key, value in structure.items() if isinstance(value, list)}
        if collection_keys != split_keys:
            raise ContractAtlasError(
                f"durable-state structure has unknown collection fields: "
                f"{authority}: {sorted(collection_keys)}"
            )
        parent = _add_element(
            elements,
            authority=authority,
            interface="durable-state",
            title=f"{authority} durable-state identity",
            pointers=[
                *(
                    f"{base}/{_escape_pointer(str(key))}"
                    for key in state_owner
                    if key != "structure"
                ),
                *(
                    f"{base}/structure/{_escape_pointer(str(key))}"
                    for key in structure
                    if key not in split_keys
                ),
                *(
                    f"{base}/structure/{_escape_pointer(key)}"
                    for key in sorted(split_keys)
                    if not cast(Sequence[object], structure[key])
                ),
            ],
            detector="durable-state",
            source_ids=[f"state:{authority}"],
            details={"state_owner": authority, "state_unit": "identity"},
        )
        children: list[dict[str, object]] = []
        for collection_key in sorted(split_keys):
            items = cast(Sequence[Mapping[str, object]], structure[collection_key])
            for item_index, item in enumerate(items):
                unit_name = str(item.get("name", item.get("id", item_index)))
                unit_kind = (
                    "relational-table"
                    if collection_key == "tables"
                    else "unique-index"
                    if collection_key == "unique_indexes"
                    else "json-document"
                    if collection_key == "documents"
                    else str(item.get("kind", "durable-unit"))
                )
                child = _add_element(
                    elements,
                    authority=authority,
                    interface="durable-state",
                    title=f"{authority}: {unit_name}",
                    pointers=[f"{base}/structure/{collection_key}/{item_index}"],
                    detector="durable-state",
                    source_ids=[f"state:{authority}"],
                    details={
                        "state_owner": authority,
                        "state_unit": unit_kind,
                    },
                )
                children.append(child)
        cast(list[str], parent["related_element_ids"]).extend(
            str(child["id"]) for child in children
        )
        for child in children:
            cast(list[str], child["related_element_ids"]).append(str(parent["id"]))

    extents = cast(Mapping[str, object], external["extents"])
    for key in sorted(cast(Mapping[str, object], extents["principles"])):
        _add_element(
            elements,
            authority="extent-contract",
            interface="extent",
            title=f"Extent principle: {key.replace('_', ' ')}",
            pointers=[f"/external_contract/extents/principles/{_escape_pointer(key)}"],
            detector="extent",
            source_ids=["extent:extent-contract"],
        )
    for key in sorted(
        cast(Mapping[str, object], extent_contract.normative_extent_declarations()["rules"])
    ):
        _add_element(
            elements,
            authority="extent-contract",
            interface="extent",
            title=f"Extent rule: {key.removesuffix('/v1').replace('-', ' ')}",
            pointers=[f"/external_contract/extents/rules/{_escape_pointer(key)}"],
            detector="extent",
            source_ids=["extent:extent-contract"],
        )
    return elements


def _attach_extent_decisions(
    elements: list[dict[str, object]], projection: Mapping[str, object]
) -> None:
    external = cast(Mapping[str, object], projection["external_contract"])
    decisions = cast(
        Sequence[Mapping[str, object]], cast(Mapping[str, object], external["extents"])["decisions"]
    )
    candidates = [
        (pointer, element)
        for element in elements
        if element["interface"] != "extent"
        for pointer in cast(Sequence[str], element["pointers"])
    ]
    unbound: list[str] = []
    for decision in decisions:
        source_pointer = str(decision["source_pointer"])
        matching = [
            (len(pointer), element)
            for pointer, element in candidates
            if source_pointer == pointer or source_pointer.startswith(f"{pointer}/")
        ]
        if matching:
            _, element = max(matching, key=lambda item: item[0])
            cast(list[str], element["extent_decision_ids"]).append(str(decision["id"]))
        else:
            unbound.append(str(decision["id"]))
    if unbound:
        raise ContractAtlasError(f"extent analysis has unresolved subject associations: {unbound}")
    for element in elements:
        element["extent_decision_ids"] = sorted(
            set(cast(Sequence[str], element["extent_decision_ids"]))
        )
        element["policy_ids"] = sorted(set(cast(Sequence[str], element["policy_ids"])))


def _link_operation_qualification(
    elements: list[dict[str, object]], trace: Mapping[str, object], projection: Mapping[str, object]
) -> None:
    http: dict[tuple[str, str], dict[str, object]] = {}
    python: dict[str, dict[str, object]] = {}
    for element in elements:
        details = cast(Mapping[str, object], element.get("details", {}))
        operation_id = details.get("operation_id")
        if element["interface"] == "http-operations" and operation_id:
            http[(str(element["authority"]), str(operation_id))] = element
        elif element["interface"] == "python":
            python[str(details["public_identity"])] = element
    qualification = cast(Mapping[str, object], trace["operation_qualification"])
    operation_values = cast(Sequence[Mapping[str, object]], qualification["records"])
    operation_by_id = {
        (str(value["application"]), str(value["operation_id"])): value for value in operation_values
    }
    if len(operation_by_id) != len(operation_values):
        raise ContractAtlasError("operation qualification repeats an application operation")
    for key, record in operation_by_id.items():
        http_element = http.get(key)
        if http_element is None:
            if record.get("classification") == "service-internal":
                continue
            raise ContractAtlasError(
                f"external operation qualification lacks an exact HTTP contract: {key}"
            )
        cast(list[str], http_element["source_authority_ids"]).append("operations:operation-matrix")
        details = cast(dict[str, object], http_element["details"])
        details["qualification_key"] = list(key)
        related = [http_element]
        if record["client"] is not None and not record["client_bindings"]:
            raise ContractAtlasError(
                f"operation qualification lacks a Python client binding: {key}"
            )
        for binding in cast(Sequence[Mapping[str, object]], record["client_bindings"]):
            public_identity = str(binding["public_identity"])
            if (
                public_identity not in python
                or cast(Mapping[str, object], python[public_identity]["details"])["unit"]
                != "member"
            ):
                raise ContractAtlasError(
                    f"operation qualification lacks a Python contract member: {key}: "
                    f"{public_identity}"
                )
            related.append(python[public_identity])
        bindings = cast(Sequence[Mapping[str, object]], record["cli_bindings"])
        binding_commands = Counter(str(binding["command"]) for binding in bindings)
        if binding_commands != Counter(cast(Sequence[str], record["cli_commands"])) or any(
            count != 1 for count in binding_commands.values()
        ):
            raise ContractAtlasError(f"operation qualification lacks exact CLI bindings: {key}")
        for binding in bindings:
            related.append(
                cast(dict[str, object], _cli_binding_element(binding, elements, projection))
            )
        for member in related:
            cast(list[str], member["related_element_ids"]).extend(
                str(other["id"]) for other in related if other["interface"] != member["interface"]
            )
    for element in elements:
        element["source_authority_ids"] = sorted(
            set(cast(Sequence[str], element["source_authority_ids"]))
        )
        element["related_element_ids"] = sorted(
            set(cast(Sequence[str], element["related_element_ids"]))
        )


def _detector_meta_closure(
    projection: Mapping[str, object], trace: Mapping[str, object]
) -> dict[str, object]:
    boundaries = cast(Mapping[str, object], projection["boundaries"])
    channels: list[dict[str, object]] = []
    for component in cast(Sequence[Mapping[str, object]], boundaries["components"]):
        distribution = str(component["distribution"])
        channels.append(
            {
                "id": f"distribution:{distribution}",
                "kind": "distribution",
                "detector": "release-metadata",
            }
        )
    python_registry = cast(Mapping[str, object], trace["python_registry"])
    python_dispositions = {
        str(item["candidate_id"]): item
        for item in cast(Sequence[Mapping[str, object]], python_registry["dispositions"])
    }
    for resolution in cast(Sequence[Mapping[str, object]], python_registry["resolutions"]):
        candidate_id = str(resolution["candidate_id"])
        disposition = python_dispositions[candidate_id]
        channels.append(
            {
                "id": candidate_id,
                "kind": "python",
                "detector": python_registry["detector"],
                "disposition": disposition["disposition"],
                "policy_id": disposition["policy_id"],
            }
        )
    console_registry = cast(Mapping[str, object], trace["console_script_registry"])
    console_dispositions = {
        str(item["candidate_id"]): item
        for item in cast(Sequence[Mapping[str, object]], console_registry["dispositions"])
    }
    for resolution in cast(Sequence[Mapping[str, object]], console_registry["resolutions"]):
        candidate_id = str(resolution["candidate_id"])
        disposition = console_dispositions[candidate_id]
        channels.append(
            {
                "id": candidate_id,
                "kind": "console-script",
                "detector": console_registry["detector"],
                "disposition": disposition["disposition"],
                "policy_id": disposition["policy_id"],
            }
        )
    for point in cast(Sequence[Mapping[str, object]], boundaries["entry_point_extensions"]):
        channels.append(
            {
                "id": f"extension-entry-point:{point['group']}",
                "kind": "extension-entry-point",
                "detector": "python-public-unit",
            }
        )
    for point in cast(Sequence[Mapping[str, object]], boundaries["process_extensions"]):
        channels.append(
            {
                "id": f"process-protocol:{point['name']}",
                "kind": "process-protocol",
                "detector": "process-protocol-bundle",
            }
        )
    for image_kind, images in sorted(
        cast(Mapping[str, object], boundaries["runtime_images"]).items()
    ):
        if isinstance(images, Mapping):
            for image in sorted(images):
                channels.append(
                    {
                        "id": f"runtime-image:{image_kind}:{image}",
                        "kind": "runtime-image",
                        "detector": "release-metadata",
                    }
                )
    ids = [str(channel["id"]) for channel in channels]
    detector_bindings: dict[str, list[str]] = defaultdict(list)
    for channel in channels:
        detector_bindings[str(channel["detector"])].append(str(channel["id"]))
    return {
        "format": DETECTOR_CLOSURE_FORMAT,
        "detector_bindings": {
            key: sorted(value) for key, value in sorted(detector_bindings.items())
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
            "protected": sum(item.get("disposition") == "protected" for item in channels),
            "missing": 0,
            "duplicate": len(ids) - len(set(ids)),
            "stale": 0,
            "undecided": 0,
        },
    }


def _validate_staged_registry(
    registry: Mapping[str, object],
    *,
    label: str,
    require_one_resolution_per_detection: bool,
) -> None:
    detections = cast(Sequence[Mapping[str, object]], registry["detections"])
    candidates = cast(Sequence[Mapping[str, object]], registry["candidates"])
    dispositions = cast(Sequence[Mapping[str, object]], registry["dispositions"])
    detection_ids = [str(item["id"]) for item in detections]
    candidate_ids = [str(item["id"]) for item in candidates]
    disposition_ids = [str(item["candidate_id"]) for item in dispositions]
    resolutions = cast(Sequence[Mapping[str, object]], registry["resolutions"])
    resolution_pairs = [
        (str(item["detection_id"]), str(item["candidate_id"])) for item in resolutions
    ]
    resolution_detection_ids = [item[0] for item in resolution_pairs]
    resolution_candidate_ids = [item[1] for item in resolution_pairs]
    if (
        len(detection_ids) != len(set(detection_ids))
        or len(candidate_ids) != len(set(candidate_ids))
        or len(disposition_ids) != len(set(disposition_ids))
        or set(candidate_ids) != set(disposition_ids)
        or any(item["disposition"] != "protected" for item in dispositions)
        or len(resolution_pairs) != len(set(resolution_pairs))
        or set(resolution_detection_ids) != set(detection_ids)
        or set(resolution_candidate_ids) != set(candidate_ids)
    ):
        raise ContractAtlasError(f"{label} discovery stages are incomplete or duplicated")
    if require_one_resolution_per_detection:
        if len(resolution_detection_ids) != len(set(resolution_detection_ids)):
            raise ContractAtlasError(f"{label} detection-to-candidate resolution is not exact")
    coverage = cast(Mapping[str, object], registry["coverage"])
    if (
        coverage.get("detected") != len(detections)
        or coverage.get("resolved") != len(cast(Sequence[object], registry["resolutions"]))
        or coverage.get("protected")
        != sum(item["disposition"] == "protected" for item in dispositions)
        or coverage.get("undispositioned") != 0
    ):
        raise ContractAtlasError(f"{label} discovery coverage is stale")


def _counts(elements: Sequence[Mapping[str, object]]) -> dict[str, object]:
    return {
        "contract_elements": len(elements),
        "extent_decisions": sum(
            len(cast(Sequence[object], item["extent_decision_ids"])) for item in elements
        ),
        "by_authority": dict(sorted(Counter(str(item["authority"]) for item in elements).items())),
        "by_interface": dict(sorted(Counter(str(item["interface"]) for item in elements).items())),
        "by_policy": dict(
            sorted(
                Counter(
                    [
                        policy
                        for item in elements
                        for policy in cast(Sequence[str], item["policy_ids"])
                    ]
                ).items()
            )
        ),
        "by_detector": dict(sorted(Counter(str(item["detector"]) for item in elements).items())),
        "by_source_authority": dict(
            sorted(
                Counter(
                    source
                    for item in elements
                    for source in cast(Sequence[str], item["source_authority_ids"])
                ).items()
            )
        ),
        "by_qualification_route": dict(
            sorted(
                Counter(
                    route
                    for item in elements
                    for route in cast(Sequence[str], item["qualification_routes"])
                ).items()
            )
        ),
    }


def _terminal_pointers(value: object, pointer: str = "") -> list[str]:
    if isinstance(value, Mapping) and value:
        return [
            child
            for key, item in value.items()
            for child in _terminal_pointers(item, f"{pointer}/{_escape_pointer(str(key))}")
        ]
    if isinstance(value, list) and value:
        return [
            child
            for index, item in enumerate(value)
            for child in _terminal_pointers(item, f"{pointer}/{index}")
        ]
    return [pointer]


def _projection_coverage(
    elements: Sequence[Mapping[str, object]],
    policies: Mapping[str, object],
    projection: Mapping[str, object],
    noncontractual_projection: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    pointers = [pointer for item in elements for pointer in cast(Sequence[str], item["pointers"])]
    definition_pointers = [
        str(policy["definition_pointer"])
        for values in policies.values()
        for policy in cast(Sequence[Mapping[str, object]], values)
    ]
    if len(definition_pointers) != len(set(definition_pointers)):
        raise ContractAtlasError("policy source ownership is duplicated")
    # Policy definitions owned by elements are references, not second owners.
    policy_pointers = [pointer for pointer in definition_pointers if pointer not in pointers]
    noncontractual_pointers = [
        pointer
        for item in noncontractual_projection
        for pointer in cast(Sequence[str], item["pointers"])
    ]
    terminals = _terminal_pointers(projection)
    extent_prefix = "/external_contract/extents/decisions/"
    noncontractual_terminals = [
        pointer
        for pointer in terminals
        if any(
            pointer == excluded or pointer.startswith(f"{excluded}/")
            for excluded in noncontractual_pointers
        )
    ]
    semantic_terminals = [
        pointer
        for pointer in terminals
        if not pointer.startswith(extent_prefix)
        and not any(
            pointer == excluded or pointer.startswith(f"{excluded}/")
            for excluded in noncontractual_pointers
        )
    ]
    owned = {
        terminal: [
            pointer
            for pointer in [*pointers, *policy_pointers]
            if terminal == pointer or terminal.startswith(f"{pointer}/")
        ]
        for terminal in semantic_terminals
    }
    decisions = cast(
        Sequence[Mapping[str, object]],
        cast(
            Mapping[str, object],
            cast(Mapping[str, object], projection["external_contract"])["extents"],
        )["decisions"],
    )
    declared_decision_ids = [str(item["id"]) for item in decisions]
    represented_decision_ids = [
        identity
        for item in elements
        for identity in cast(Sequence[str], item["extent_decision_ids"])
    ]
    return {
        "projection_terminals": len(terminals),
        "semantic_terminals": len(semantic_terminals),
        "policy_terminals": sum(
            any(
                terminal == pointer or terminal.startswith(f"{pointer}/")
                for pointer in policy_pointers
            )
            for terminal in semantic_terminals
        ),
        "noncontractual_terminals": len(noncontractual_terminals),
        "extent_decisions": len(declared_decision_ids),
        "missing": sum(not owners for owners in owned.values())
        + len(set(declared_decision_ids) - set(represented_decision_ids)),
        "multiply_represented": sum(len(owners) > 1 for owners in owned.values())
        + len(represented_decision_ids)
        - len(set(represented_decision_ids)),
        "stale": len(set(represented_decision_ids) - set(declared_decision_ids)),
    }


def _validate_authority_registry(
    elements: Sequence[Mapping[str, object]],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
) -> Sequence[Mapping[str, object]]:
    registry = cast(Mapping[str, object], trace["authority_registry"])
    if registry.get("format") != "riverhog-contract-authority-registry/v1":
        raise ContractAtlasError("contract authority registry has another format")
    declared = cast(Sequence[Mapping[str, object]], registry["declared_authorities"])
    declared_ids = [str(item["id"]) for item in declared]
    if len(declared_ids) != len(set(declared_ids)) or any(
        not str(item.get("meaning", "")).strip() for item in declared
    ):
        raise ContractAtlasError("declared contract authorities are incomplete")
    allowed = {
        *cast(Sequence[str], registry["component_authorities"]),
        *cast(Sequence[str], registry["state_authorities"]),
        *declared_ids,
    }
    unknown = sorted({str(item["authority"]) for item in elements} - allowed)
    if unknown:
        raise ContractAtlasError(f"contract elements lack legitimate authorities: {unknown}")

    noncontractual = cast(Sequence[Mapping[str, object]], registry["noncontractual_projection"])
    record_ids = [str(item["id"]) for item in noncontractual]
    pointers = [
        str(pointer)
        for item in noncontractual
        for pointer in cast(Sequence[str], item.get("pointers", ()))
    ]
    if (
        len(record_ids) != len(set(record_ids))
        or len(pointers) != len(set(pointers))
        or any(not str(item.get("reason", "")).strip() for item in noncontractual)
    ):
        raise ContractAtlasError("non-contractual projection dispositions are incomplete")
    for pointer in pointers:
        pointer_value(projection, pointer)
    owned_pointers = [
        str(pointer) for item in elements for pointer in cast(Sequence[str], item["pointers"])
    ]
    overlap = sorted(
        pointer
        for pointer in pointers
        if any(
            pointer == owned or pointer.startswith(f"{owned}/") or owned.startswith(f"{pointer}/")
            for owned in owned_pointers
        )
    )
    if overlap:
        raise ContractAtlasError(f"non-contractual projection is also owned as contract: {overlap}")
    return noncontractual


def _validate_process_protocol_units(
    elements: Sequence[Mapping[str, object]], projection: Mapping[str, object]
) -> None:
    external = cast(Mapping[str, object], projection["external_contract"])
    documents = cast(Mapping[str, Mapping[str, object]], external["protocol_schemas"])
    by_pointer = {
        str(pointer): item for item in elements for pointer in cast(Sequence[str], item["pointers"])
    }
    for schema_authority, document in documents.items():
        base = f"/external_contract/protocol_schemas/{_escape_pointer(schema_authority)}"
        schemas = document.get("schemas")
        if not schema_authority.startswith("generated:") or not isinstance(schemas, Mapping):
            item = by_pointer.get(base)
            if item is None or item["interface"] != "schema":
                raise ContractAtlasError(
                    f"standalone schema lacks exact schema ownership: {schema_authority}"
                )
            continue

        binding = document.get("http_binding")
        if not isinstance(binding, Mapping) or set(binding) != {"operations"}:
            raise ContractAtlasError(
                f"generated process protocol binding shape is unresolved: {schema_authority}"
            )
        operations = cast(Sequence[Mapping[str, object]], binding["operations"])
        metadata_pointers = {
            f"{base}/{_escape_pointer(key)}"
            for key in document
            if key not in {"http_binding", "schemas"}
        }
        parent_candidates = {
            str(item["id"]): item
            for pointer in metadata_pointers
            if (item := by_pointer.get(pointer)) is not None
        }
        if len(parent_candidates) != 1:
            raise ContractAtlasError(
                f"process protocol metadata lacks one exact owner: {schema_authority}"
            )
        parent = next(iter(parent_candidates.values()))
        if (
            parent["interface"] != "process-protocol"
            or set(cast(Sequence[str], parent["pointers"])) != metadata_pointers
            or any(
                pointer.startswith(f"{base}/http_binding") or pointer.startswith(f"{base}/schemas")
                for pointer in cast(Sequence[str], parent["pointers"])
            )
        ):
            raise ContractAtlasError(
                f"process protocol parent duplicates operation/schema semantics: {schema_authority}"
            )

        children: list[Mapping[str, object]] = []
        for index, operation in enumerate(operations):
            pointer = f"{base}/http_binding/operations/{index}"
            item = by_pointer.get(pointer)
            details = cast(Mapping[str, object], item.get("details", {})) if item else {}
            if (
                item is None
                or item["interface"] != "process-protocol-operations"
                or details.get("method") != str(operation["method"]).upper()
                or details.get("path") != operation["path"]
            ):
                raise ContractAtlasError(
                    f"process protocol operation lacks exact ownership: {schema_authority}: {index}"
                )
            children.append(item)
        for name in schemas:
            pointer = f"{base}/schemas/{_escape_pointer(str(name))}"
            item = by_pointer.get(pointer)
            if item is None or item["interface"] != "process-protocol-schemas":
                raise ContractAtlasError(
                    f"process protocol schema lacks exact ownership: {schema_authority}: {name}"
                )
            children.append(item)
        child_ids = {str(item["id"]) for item in children}
        if set(cast(Sequence[str], parent["related_element_ids"])) != child_ids or any(
            str(parent["id"]) not in cast(Sequence[str], item["related_element_ids"])
            for item in children
        ):
            raise ContractAtlasError(
                f"process protocol child references are incomplete: {schema_authority}"
            )


def _validate_python_units(
    elements: Sequence[Mapping[str, object]],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
) -> None:
    external = cast(Mapping[str, object], projection["external_contract"])
    surfaces = cast(Mapping[str, Mapping[str, object]], external["python"])
    python_elements = [item for item in elements if item["interface"] == "python"]
    expected_pointers = {
        f"/external_contract/python/{_escape_pointer(public_identity)}": public_identity
        for public_identity in surfaces
    }
    actual_by_pointer = {
        str(cast(Sequence[str], item["pointers"])[0]): item for item in python_elements
    }
    if (
        len(python_elements) != len(surfaces)
        or any(len(cast(Sequence[str], item["pointers"])) != 1 for item in python_elements)
        or set(actual_by_pointer) != set(expected_pointers)
    ):
        raise ContractAtlasError("Python exact-unit projection and atlas differ")

    element_by_identity: dict[str, Mapping[str, object]] = {}
    expected_candidates: set[str] = set()
    for pointer, public_identity in expected_pointers.items():
        surface = surfaces[public_identity]
        item = actual_by_pointer[pointer]
        distribution = str(surface["distribution"])
        module = str(surface["module"])
        details = cast(Mapping[str, object], item.get("details", {}))
        if (
            item["authority"] != distribution
            or item["title"] != public_identity
            or details.get("module") != module
            or details.get("public_identity") != public_identity
            or details.get("unit") != surface["unit"]
            or f"python:{distribution}:{module}"
            not in cast(Sequence[str], item["source_authority_ids"])
        ):
            raise ContractAtlasError(f"Python exact unit is misbound: {public_identity}")
        expected_candidates.add(f"python:{distribution}:{public_identity}")
        element_by_identity[public_identity] = item

    for public_identity, surface in surfaces.items():
        if surface["unit"] != "member":
            continue
        owner = str(surface["owner"])
        owner_surface = surfaces.get(owner)
        member = element_by_identity[public_identity]
        owner_element = element_by_identity.get(owner)
        if (
            owner_surface is None
            or owner_surface["unit"] != "export"
            or owner_element is None
            or str(owner_element["id"]) not in cast(Sequence[str], member["related_element_ids"])
            or str(member["id"]) not in cast(Sequence[str], owner_element["related_element_ids"])
        ):
            raise ContractAtlasError(f"Python member lacks exact exported owner: {public_identity}")

    registry = cast(Mapping[str, object], trace["python_registry"])
    protected = {
        str(item["candidate_id"])
        for item in cast(Sequence[Mapping[str, object]], registry["dispositions"])
        if item["disposition"] == "protected"
    }
    candidates = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], registry["candidates"])
    }
    if protected != expected_candidates or not expected_candidates <= set(candidates):
        raise ContractAtlasError("Python registry does not protect every exact public unit")


def _validate_release_units(
    elements: Sequence[Mapping[str, object]], projection: Mapping[str, object]
) -> None:
    external = cast(Mapping[str, object], projection["external_contract"])
    release = cast(Mapping[str, object], external["release"])
    publication = cast(Mapping[str, object], release["publication"])
    base = "/external_contract/release/publication"
    expected: dict[str, str] = {}
    for section, interface in (
        ("versioning", "versioning-tags"),
        ("coordinates", "publication-locations"),
        ("policy", "publication-policies"),
        ("distributions", "python-distributions"),
        ("runtime_images", "runtime-images"),
        ("installation_roots", "installation-roots"),
        ("release_artifacts", "release-artifacts"),
        ("trust", "artifact-verification"),
    ):
        expected.update(
            {
                f"{base}/{section}/{_escape_pointer(str(name))}": interface
                for name in cast(Mapping[str, object], publication[section])
            }
        )
    expected.update(
        {
            f"/external_contract/release/compatibility/{_escape_pointer(str(name))}": (
                "compatibility-guarantees"
            )
            for name in cast(Mapping[str, object], release["compatibility"])
        }
    )
    release_elements = [item for item in elements if item["authority"] == "release"]
    actual = {str(cast(Sequence[str], item["pointers"])[0]): item for item in release_elements}
    if (
        len(actual) != len(release_elements)
        or any(len(cast(Sequence[str], item["pointers"])) != 1 for item in release_elements)
        or set(actual) != set(expected)
    ):
        raise ContractAtlasError("release publication exact-unit projection and atlas differ")
    if any(item["interface"] != expected[pointer] for pointer, item in actual.items()):
        raise ContractAtlasError("release unit belongs to the wrong semantic interface")

    distributions = cast(Mapping[str, Mapping[str, object]], publication["distributions"])
    publication_policy_ids = {
        "publication/role-retention/v1",
        "publication/platform-scope/v1",
        "publication/image-identity-scope/v1",
    }
    for pointer, item in actual.items():
        details = cast(Mapping[str, object], item.get("details", {}))
        policy_ids = set(cast(Sequence[str], item["policy_ids"]))
        expected_publication_policies: set[str] = set()
        if "/distributions/" in pointer:
            name = pointer.rsplit("/", 1)[-1].replace("~1", "/").replace("~0", "~")
            if f"release-distribution:{name}" not in cast(
                Sequence[str], item["source_authority_ids"]
            ):
                raise ContractAtlasError(f"distribution lacks source metadata evidence: {name}")
            unit = cast(Mapping[str, object], pointer_value(projection, pointer))
            if not str(unit.get("requires_python", "")):
                raise ContractAtlasError(f"distribution lacks Requires-Python: {name}")
            if details.get("classification") != unit["role"]:
                raise ContractAtlasError(f"distribution classification is stale: {name}")
            expected_publication_policies = {"publication/role-retention/v1"}
        elif "/runtime_images/" in pointer:
            unit = cast(Mapping[str, object], pointer_value(projection, pointer))
            roots = [str(value) for value in cast(Sequence[object], unit["distribution_roots"])]
            root_roles = {str(distributions[root]["role"]) for root in roots}
            expected_classification = (
                "product"
                if unit["role"] == "product"
                else next(iter(root_roles))
                if len(root_roles) == 1
                else None
            )
            if details.get("classification") != expected_classification:
                raise ContractAtlasError(f"runtime-image classification is stale: {pointer}")
            expected_publication_policies = set(publication_policy_ids)
        elif "/installation_roots/" in pointer:
            unit = cast(Mapping[str, object], pointer_value(projection, pointer))
            role = distributions[str(unit["distribution"])]["role"]
            if details.get("classification") != role:
                raise ContractAtlasError(f"installation-root classification is stale: {pointer}")
            expected_publication_policies = {
                "publication/role-retention/v1",
                "publication/platform-scope/v1",
            }
        if policy_ids & publication_policy_ids != expected_publication_policies:
            raise ContractAtlasError(f"release publication policy application is stale: {pointer}")
    if f"{base}/schema" in actual:
        raise ContractAtlasError("release metadata is duplicated as a semantic unit")
    if "platforms" in release:
        raise ContractAtlasError("release envelope exposes a global platform claim")

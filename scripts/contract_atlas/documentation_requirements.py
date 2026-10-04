"""Source-owned documentation obligations over the native contract ownership map."""

from __future__ import annotations

import copy
import hashlib
import inspect
import re
from collections.abc import Iterator, Mapping
from functools import cache
from pathlib import Path
from typing import Any, cast

from .model import (
    INTERFACE_REGISTRY,
    ContractAtlasError,
    _escape_pointer,
    canonical_bytes,
    canonical_sha256,
    pointer_value,
)

REQUIREMENTS_FORMAT = "riverhog-documentation-requirements/v1"
FINGERPRINT_PROFILE = "riverhog-documentation-owned-context/v1"

# This classifies field families, not subjects. Membership always comes from discovery.
FIELD_OWNERSHIP = (
    {
        "family": "cli",
        "meaning": "native parser and result declarations",
        "editorial": "command and visible input help",
        "development": "native help retained",
        "destinations": ["installed-help", "contract-render"],
    },
    {
        "family": "openapi",
        "meaning": "typed operations, schemas, security and vendor extensions",
        "editorial": "recognized summary/description annotations",
        "development": "framework annotations and concise source context retained",
        "destinations": ["served-openapi", "contract-render"],
    },
    {
        "family": "python",
        "meaning": "owned exports, signatures, fields and values",
        "editorial": "public reference and supported docstrings",
        "development": "implementation comments and docstrings retained",
        "destinations": ["installed-python", "contract-render"],
    },
    {
        "family": "distribution",
        "meaning": "coordinates, roles, dependencies, entry points and law",
        "editorial": "Summary and long description",
        "development": "concise purpose retained",
        "destinations": ["wheel-metadata", "sdist-metadata", "contract-render"],
    },
    {
        "family": "image",
        "meaning": "repository, platform, executable roots and operational labels",
        "editorial": "OCI description",
        "development": "concise purpose retained",
        "destinations": ["oci-config", "contract-render"],
    },
    {
        "family": "policy-state-configuration",
        "meaning": "all declarations and guarantees",
        "editorial": "explanation and guidance",
        "development": "all source authority retained",
        "destinations": ["contract-render"],
    },
)

_INTERFACES = frozenset(
    {
        "artifact-verification",
        "cli",
        "compatibility-guarantees",
        "configuration",
        "configuration-environment",
        "durable-state",
        "extent",
        "http-operations",
        "http-schemas",
        "http-security-schemes",
        "http-service-declaration",
        "installation-roots",
        "process-protocol",
        "process-protocol-operations",
        "process-protocol-schemas",
        "publication-locations",
        "publication-policies",
        "python",
        "python-distributions",
        "release-artifacts",
        "runtime-images",
        "schema",
        "versioning-tags",
    }
)
_SCHEMA_MAPS = {"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
_SCHEMA_VALUES = {
    "items",
    "additionalProperties",
    "unevaluatedProperties",
    "contains",
    "propertyNames",
    "not",
    "if",
    "then",
    "else",
    "unevaluatedItems",
}
_SCHEMA_LISTS = {"anyOf", "allOf", "oneOf", "prefixItems"}


def target_key(target: Mapping[str, Any]) -> str:
    """Exact owned subject identity; neither display names nor fuzzy aliases resolve it."""
    if not isinstance(target.get("element_id"), str) or not target["element_id"]:
        raise ContractAtlasError("documentation target needs an existing element_id")
    if set(target) == {"element_id", "pointer"}:
        pointer = target["pointer"]
        if (
            not isinstance(pointer, str)
            or (pointer and not pointer.startswith("/"))
            or re.search(r"~(?![01])", pointer)
        ):
            raise ContractAtlasError("documentation target has an invalid pointer")
    elif set(target) == {"element_id", "member"}:
        member = target["member"]
        if (
            not isinstance(member, dict)
            or set(member) != {"kind", "key"}
            or member["kind"]
            not in {
                "cli-parameter",
                "python-parameter",
                "python-result",
                "http-parameter",
                "python-field",
                "state-field",
            }
            or not isinstance(member["key"], str)
            or not member["key"]
        ):
            raise ContractAtlasError("documentation target has an invalid native member")
    else:
        raise ContractAtlasError("documentation target has unknown fields")
    return canonical_sha256(target)


def _schema_members(value: Any, prefix: str = "") -> Iterator[tuple[str, Any]]:
    """Only schema nodes introduce members; a payload field called description is data."""
    if not isinstance(value, dict):
        return
    for keyword in sorted(_SCHEMA_MAPS):
        children = value.get(keyword, {})
        if isinstance(children, dict):
            for name, child in sorted(children.items()):
                pointer = f"{prefix}/{keyword}/{_escape_pointer(name)}"
                yield pointer, child
                yield from _schema_members(child, pointer)
    for keyword in sorted(_SCHEMA_VALUES):
        if isinstance(value.get(keyword), dict):
            yield from _schema_members(value[keyword], f"{prefix}/{keyword}")
    for keyword in sorted(_SCHEMA_LISTS):
        for index, child in enumerate(value.get(keyword, [])):
            yield from _schema_members(child, f"{prefix}/{keyword}/{index}")


def _references(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        if isinstance(value.get("$ref"), str):
            yield value["$ref"]
        for child in value.values():
            yield from _references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _references(child)


def build_requirements(closure: Mapping[str, Any]) -> dict[str, Any]:
    """Exhaustive typed classification; unknown native families fail rather than waive coverage."""
    if set(INTERFACE_REGISTRY) != _INTERFACES:
        raise ContractAtlasError("documentation policy does not classify every native interface")
    elements = {e["id"]: e for e in closure["elements"]}
    by_pointer = {p: e["id"] for e in elements.values() for p in e["pointers"]}
    rows: dict[str, Any] = {}
    scopes: dict[str, Any] = {}
    contexts: dict[str, list[str]] = {}
    policy_context = {
        "compatibility": closure["external_contract"]["release"]["compatibility"],
        "extents": closure["external_contract"]["extents"],
    }

    def scope(value: Any) -> str:
        identity = canonical_sha256(value)
        scopes[identity] = copy.deepcopy(value)
        return identity

    policy_scope = scope(policy_context)
    element_scopes = {
        identity: scope({p: pointer_value(closure, p) for p in e["pointers"]})
        for identity, e in elements.items()
    }
    dependencies: dict[str, set[str]] = {}
    for identity, element in elements.items():
        direct = set(element.get("related_element_ids", []))
        for pointer in element["pointers"]:
            for reference in _references(pointer_value(closure, pointer)):
                candidates = []
                if reference.startswith("#/"):
                    # Local references are scoped to their native document, never another app.
                    for marker in (
                        "/http_openapi/",
                        "/configuration_documents/",
                        "/protocol_schemas/",
                    ):
                        if marker in pointer:
                            tail = pointer.split(marker, 1)[1].split("/", 1)[0]
                            candidates.append(
                                pointer.split(marker, 1)[0] + marker + tail + reference[1:]
                            )
                else:
                    candidates.append(
                        "/external_contract/protocol_schemas/" + _escape_pointer(reference)
                    )
                for candidate in candidates:
                    owners = [
                        p for p in by_pointer if candidate == p or candidate.startswith(p + "/")
                    ]
                    if owners:
                        direct.add(by_pointer[max(owners, key=len)])
        dependencies[identity] = direct

    @cache
    def context(identity: str) -> str:
        visited: set[str] = set()
        pending = [identity]
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            pending.extend(sorted(dependencies[current] - visited))
        values = sorted({policy_scope, *(element_scopes[item] for item in visited)})
        digest = canonical_sha256(values)
        contexts[digest] = values
        return digest

    def add(
        element: dict[str, Any],
        *,
        pointer: str = "",
        member: dict[str, str] | None = None,
        value: Any = None,
        rule: str = "authored",
        reason: str = "meaningful public subject",
        destination: dict[str, Any] | None = None,
    ) -> None:
        target = {
            "element_id": element["id"],
            **({"member": member} if member else {"pointer": pointer}),
        }
        key = target_key(target)
        if key in rows:
            raise ContractAtlasError(f"duplicate native documentation requirement: {target}")
        rows[key] = {
            "target": target,
            "authority": element["authority"],
            "interface": element["interface"],
            "title": element["title"],
            "rule": rule,
            "detail": False,
            "reason": reason,
            "meaning": {
                "owned": scope(value),
                "dependencies": context(element["id"]),
                "scope": "owned element, native references/relations and governing policies",
            },
            "destinations": [] if destination is None else [destination],
        }

    for element in sorted(elements.values(), key=lambda item: item["id"]):
        interface = element["interface"]
        if interface not in _INTERFACES:
            raise ContractAtlasError(f"unclassified documentation interface: {interface}")
        pointers = element["pointers"]
        values = {p: pointer_value(closure, p) for p in pointers}
        root: Any = next(iter(values.values())) if len(values) == 1 else values
        destination: dict[str, Any] | None = None
        if interface == "cli":
            from .human_contract import command_path

            path = list(command_path(pointers[0]))
            destination = {"kind": "cli", "command": path}
        elif interface.startswith("http-") and "/http_openapi/" in pointers[0]:
            base, native = pointers[0].split("/http_openapi/", 1)
            del base
            authority, _, location = native.partition("/")
            destination = {"kind": "openapi", "authority": authority, "pointer": "/" + location}
        elif interface == "python":
            from .documentation_native import owned_python_declaration, public_object

            native_object: Any = public_object(root)
            writable = owned_python_declaration(native_object) is not None
            destination = {
                "kind": "python",
                "identity": element["title"],
                "distribution": root["distribution"],
                "module": root["module"],
                "capability": "docstring" if writable else "reference",
            }
        elif interface in {"python-distributions", "runtime-images"}:
            destination = {
                "kind": "metadata" if interface == "python-distributions" else "oci",
                "name": pointers[0].rsplit("/", 1)[1].replace("~1", "/").replace("~0", "~"),
            }
        add(element, value=values, destination=destination)
        if interface == "cli":
            parameters: Any = next((v for p, v in values.items() if p.endswith("/parameters")), [])
            for parameter in parameters:
                # Suppressed inputs retain their facts without gaining visible prose.
                name = parameter.get("name", parameter.get("dest"))
                if not isinstance(name, str) or not name:
                    raise ContractAtlasError("native CLI parameter lacks its stable name")
                generated = (
                    parameter.get("help_visibility") == "suppressed"
                    or parameter.get("hidden") is True
                )
                add(
                    element,
                    member={"kind": "cli-parameter", "key": name},
                    value=parameter,
                    rule="structural" if generated else "authored",
                    reason="native suppressed input retains its semantic facts"
                    if generated
                    else "visible command input",
                    destination={**cast(dict[str, Any], destination), "parameter": name}
                    if not generated
                    else None,
                )
        elif interface in {"schema", "http-schemas", "process-protocol-schemas", "configuration"}:
            for pointer, value in _schema_members(root):
                add(
                    element,
                    pointer=pointer,
                    value=value,
                    destination={**destination, "pointer": destination["pointer"] + pointer}
                    if destination is not None
                    else None,
                )
        elif interface == "http-operations" and isinstance(root, dict):
            for index, parameter in enumerate(root.get("parameters", [])):
                if "$ref" in parameter:
                    raise ContractAtlasError(
                        "native OpenAPI parameter references need a resolved owner"
                    )
                add(
                    element,
                    member={
                        "kind": "http-parameter",
                        "key": parameter["in"] + ":" + parameter["name"],
                    },
                    value=parameter,
                    destination={
                        **destination,
                        "pointer": destination["pointer"] + f"/parameters/{index}",
                    }
                    if destination is not None
                    else None,
                )
            for code, response in root.get("responses", {}).items():
                add(
                    element,
                    pointer=f"/responses/{_escape_pointer(code)}",
                    value=response,
                    destination={
                        **destination,
                        "pointer": destination["pointer"] + f"/responses/{_escape_pointer(code)}",
                    }
                    if destination is not None
                    else None,
                )
                for name, header in response.get("headers", {}).items():
                    pointer = f"/responses/{_escape_pointer(code)}/headers/{_escape_pointer(name)}"
                    add(
                        element,
                        pointer=pointer,
                        value=header,
                        destination={**destination, "pointer": destination["pointer"] + pointer}
                        if destination is not None
                        else None,
                    )
            if "requestBody" in root:
                add(
                    element,
                    pointer="/requestBody",
                    value=root["requestBody"],
                    destination={**destination, "pointer": destination["pointer"] + "/requestBody"}
                    if destination is not None
                    else None,
                )
        elif interface == "python":
            contract = root["contract"]
            if contract.get("signature", "unavailable") != "unavailable":
                try:
                    signature = inspect.signature(native_object, eval_str=False)
                except (TypeError, ValueError) as exc:
                    raise ContractAtlasError(
                        "owned Python signature has no native parameter resolver: "
                        + element["title"]
                    ) from exc
                for argument in signature.parameters.values():
                    if argument.name in {"self", "cls"}:
                        continue
                    add(
                        element,
                        member={"kind": "python-parameter", "key": argument.name},
                        value={"name": argument.name, "signature": contract["signature"]},
                    )
                if signature.return_annotation is not inspect.Signature.empty:
                    add(
                        element,
                        member={"kind": "python-result", "key": "return"},
                        value={"signature": contract["signature"]},
                    )
            for field in contract.get("fields", []):
                add(element, member={"kind": "python-field", "key": field["name"]}, value=field)
            for pointer, value in _schema_members(contract.get("schema", {}), "/contract/schema"):
                add(element, pointer=pointer, value=value)
        elif interface == "durable-state" and isinstance(root, dict):
            for key in ("columns", "fields"):
                fields = root.get(key, [])
                if isinstance(fields, list):
                    for value in fields:
                        name = (
                            value
                            if isinstance(value, str)
                            else value.get("name")
                            if isinstance(value, dict)
                            else None
                        )
                        if not isinstance(name, str):
                            raise ContractAtlasError(
                                "durable-state field lacks a native named identity"
                            )
                        add(
                            element,
                            member={"kind": "state-field", "key": key + ":" + name},
                            value=value,
                        )
                elif isinstance(fields, dict):
                    for name, value in sorted(fields.items()):
                        add(element, pointer=f"/{key}/{_escape_pointer(name)}", value=value)

    _canonical_python_references(closure, rows)
    _constructor_field_references(closure, rows)
    profile = {
        "format": FINGERPRINT_PROFILE,
        "producer_sha256": canonical_sha256(
            {
                name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                for name in ("documentation_requirements.py", "documentation_audit.py")
            }
        ),
    }
    policy = {
        "format": REQUIREMENTS_FORMAT,
        "interfaces": sorted(_INTERFACES),
        "field_ownership": FIELD_OWNERSHIP,
        "member_policy": "roots and meaningful typed members; representation facets are structural",
        "fingerprint_profile": profile,
    }
    return {
        "format": REQUIREMENTS_FORMAT,
        "closure_sha256": canonical_sha256(closure),
        "policy": policy,
        "policy_sha256": canonical_sha256(policy),
        "fingerprint_profile": profile,
        "subjects": rows,
        "scopes": scopes,
        "contexts": contexts,
        "global_semantics": {
            "boundaries": copy.deepcopy(closure["boundaries"]),
            "series": closure["series"],
            "unsafe_integer_paths": closure["unsafe_integer_paths"],
        },
    }


def _canonical_python_references(closure: Mapping[str, Any], rows: dict[str, Any]) -> None:
    """Exact owned function/class identity proves reuse; similar names or values never do."""
    import inspect

    from .documentation_native import owned_python_declaration, public_object

    first: dict[int, tuple[object, str]] = {}
    aliases: dict[str, str] = {}
    members: dict[str, dict[str, str]] = {}
    for key, row in rows.items():
        members.setdefault(row["target"]["element_id"], {})[
            canonical_bytes({k: v for k, v in row["target"].items() if k != "element_id"}).decode()
        ] = key
    for element in sorted(closure["elements"], key=lambda item: item["id"]):
        if element["interface"] != "python":
            continue
        surface = cast(Mapping[str, Any], pointer_value(closure, element["pointers"][0]))
        value = public_object(surface)
        if not (inspect.isfunction(value) or inspect.isclass(value)):
            continue
        # No aliasing of interned immutable constants or third-party objects.
        if owned_python_declaration(value) is None:
            continue
        previous = first.get(id(value))
        if previous is not None and previous[0] is value:
            aliases[element["id"]] = previous[1]
        else:
            first[id(value)] = (value, element["id"])
    for alias, donor in aliases.items():
        for selector, key in members[alias].items():
            donor_key = members[donor].get(selector)
            if donor_key is not None:
                rows[key].update(
                    rule="reference",
                    canonical=rows[donor_key]["target"],
                    reason="native discovery resolves the identical owned Python object/member",
                )


def _constructor_field_references(closure: Mapping[str, Any], rows: dict[str, Any]) -> None:
    """Generated constructors refer to their actual declared fields, never similar type names."""
    import dataclasses
    import inspect

    from pydantic import BaseModel

    from .documentation_native import public_object

    for element in closure["elements"]:
        if element["interface"] != "python":
            continue
        value = public_object(
            cast(Mapping[str, Any], pointer_value(closure, element["pointers"][0]))
        )
        if not inspect.isclass(value):
            continue
        declared: dict[str, Any] = {}
        if issubclass(value, BaseModel):
            for name, field in value.model_fields.items():
                alias = field.alias if isinstance(field.alias, str) else name
                declared[alias] = {
                    "pointer": "/contract/schema/properties/" + _escape_pointer(alias)
                }
        elif dataclasses.is_dataclass(value):
            declared = {
                field.name: {"member": {"kind": "python-field", "key": field.name}}
                for field in dataclasses.fields(value)
                if field.init
            }
        for parameter, selector in declared.items():
            target = {
                "element_id": element["id"],
                "member": {"kind": "python-parameter", "key": parameter},
            }
            donor = {"element_id": element["id"], **selector}
            key, canonical = target_key(target), target_key(donor)
            if key in rows and canonical in rows and rows[key]["rule"] == "authored":
                rows[key].update(
                    rule="reference",
                    canonical=donor,
                    reason=(
                        "native generated constructor input is the identical dec"
                        "lared model/dataclass field"
                    ),
                )

"""Structural navigation and relationship-link identities for the Riverhog atlas."""

from __future__ import annotations

import hashlib
import posixpath
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from pathlib import PurePosixPath
from typing import cast
from urllib.parse import quote

from .model import (
    ATLAS_DIRECTORY,
    INTERFACE_LABELS,
    INTERFACE_ORDER,
    INTERFACE_REGISTRY,
    ContractAtlasError,
    NavigationIdentity,
    _pointer_parts,
    _slug,
    canonical_sha256,
)


def _element_progression_witnesses(
    elements: Sequence[Mapping[str, object]], trace: Mapping[str, object]
) -> dict[str, tuple[str, ...]]:
    """Join only owned extent decisions to explicitly recorded open witness groups."""
    witnesses = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], trace["segmented_extent_witnesses"])
    }
    by_extent: dict[str, tuple[str, ...]] = {}
    for source in cast(Sequence[Mapping[str, object]], trace["extent_sources"]):
        ids = cast(Sequence[str], source.get("segmented_extent_witnesses", ()))
        if any(identity not in witnesses for identity in ids):
            raise ContractAtlasError(f"extent references an unresolved witness: {source['id']}")
        by_extent[str(source["id"])] = tuple(
            identity for identity in ids if witnesses[identity]["unestablished_claims"]
        )
    return {
        str(item["id"]): tuple(
            sorted(
                {
                    witness
                    for extent in cast(Sequence[str], item["extent_decision_ids"])
                    for witness in by_extent.get(extent, ())
                }
            )
        )
        for item in elements
    }


def _assign_dossiers(elements: list[dict[str, object]]) -> None:
    used: set[str] = set()
    for element in sorted(elements, key=lambda item: str(item["id"])):
        authority = _slug(str(element["authority"]), limit=72)
        interface = _slug(str(element["interface"]), limit=48)
        base = _slug(str(element["title"]), limit=88)
        path = f"{ATLAS_DIRECTORY}/authorities/{authority}/{interface}/{base}.md"
        if path in used:
            suffix = canonical_sha256(element["id"])[:8]
            path = f"{ATLAS_DIRECTORY}/authorities/{authority}/{interface}/{base}-{suffix}.md"
        used.add(path)
        element["dossier"] = path


def _relative_link(source: str, target: str) -> str:
    return posixpath.relpath(target, posixpath.dirname(source))


def _repository_source_target(location: Mapping[str, object]) -> str:
    path = location.get("path")
    line = location.get("line")
    if (
        not isinstance(path, str)
        or not path
        or not PurePosixPath(path).parts
        or PurePosixPath(path).is_absolute()
        or ".." in PurePosixPath(path).parts
        or PurePosixPath(path).parts[0] in {".venv", ".git"}
        or not isinstance(line, int)
        or isinstance(line, bool)
        or line < 1
    ):
        raise ContractAtlasError(f"invalid repository source location: {location}")
    # Atlas paths are relative to qualification/contracts. Relative repository
    # links retain the revision currently being viewed on GitHub.
    return f"../../{quote(path, safe='/')}#L{line}"


def _repository_source_link(document: str, location: Mapping[str, object], label: str) -> str:
    # These targets leave qualification/contracts. Keep their parent traversal
    # lexical: relpath would resolve it against the checkout's current directory.
    parents = "../" * len(PurePosixPath(document).parent.parts)
    return f"[{_md(label)}]({parents}{_repository_source_target(location)})"


def _repository_source_targets(trace: Mapping[str, object]) -> set[str]:
    targets: set[str] = set()

    def visit(value: object) -> None:
        if isinstance(value, Mapping):
            source = value.get("source")
            if isinstance(source, Mapping) and "line" in source:
                targets.add(_repository_source_target(source))
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(trace)
    return targets


def _anchor_id(kind: str, identity: str) -> str:
    """Return a deterministic presentation-only anchor for an exact identity."""

    prefixes = {
        "extent": "e",
        "identity": "i",
        "policy": "p",
        "policy-application": "pa",
        "qualification": "q",
        "relationship-edge": "re",
        "relationship-node": "rn",
        "source": "src",
        "subject": "s",
    }
    prefix = prefixes.get(kind, _slug(kind, limit=12))
    return f"{prefix}-{hashlib.sha256(identity.encode()).hexdigest()[:10]}"


def _anchor_link(source: str, target: str, anchor: str) -> str:
    if source == target:
        return f"#{anchor}"
    return f"{_relative_link(source, target)}#{anchor}"


def _anchor_marker(kind: str, identity: str) -> str:
    return f'<a id="{_anchor_id(kind, identity)}"></a>'


def _html_anchor(anchor: str) -> str:
    return f'<a id="{anchor}"></a>'


def _subject_anchor(pointer: str) -> str:
    return _anchor_id("subject", pointer)


def _subject_marker(pointer: str, placed: set[str]) -> str:
    if pointer in placed:
        return ""
    placed.add(pointer)
    return f'<a id="{_subject_anchor(pointer)}"></a>'


def _policy_anchor(policy_id: str) -> str:
    return _anchor_id("policy", policy_id)


def _policy_application_anchor(element_id: str, policy_id: str) -> str:
    return _anchor_id("policy-application", f"{element_id}\n{policy_id}")


def _source_anchor(source_id: str) -> str:
    return _anchor_id("source", source_id)


def _qualification_anchor(route: str) -> str:
    return _anchor_id("qualification", route)


def _interface_label(interface: str) -> str:
    try:
        return INTERFACE_LABELS[interface]
    except KeyError as exc:
        raise ContractAtlasError(f"interface lacks a human navigation label: {interface}") from exc


def _interface_sort_key(interface: str) -> tuple[int, str]:
    return (INTERFACE_ORDER.get(interface, len(INTERFACE_ORDER)), interface)


def _contextual_labels(
    items: Sequence[Mapping[str, object]],
    candidates: Callable[[Mapping[str, object]], Sequence[str]],
) -> dict[str, str]:
    """Choose an exact structurally derived label unique among siblings."""

    ordered = sorted(items, key=lambda item: str(item["title"]))
    options: dict[str, list[str]] = {}
    positions: dict[str, int] = {}
    for item in ordered:
        identity = str(item["id"])
        available = list(dict.fromkeys(label for label in candidates(item) if label))
        if not available or available[-1] != str(item["title"]):
            available.append(str(item["title"]))
        options[identity] = available
        positions[identity] = 0

    while True:
        by_label: dict[str, list[str]] = defaultdict(list)
        for identity, available in options.items():
            by_label[available[positions[identity]]].append(identity)
        collisions = [identities for identities in by_label.values() if len(identities) > 1]
        if not collisions:
            return {
                identity: available[positions[identity]] for identity, available in options.items()
            }
        advanced = False
        for identities in collisions:
            for identity in identities:
                if positions[identity] + 1 < len(options[identity]):
                    positions[identity] += 1
                    advanced = True
        if not advanced:
            raise ContractAtlasError("contextual inventory labels remain ambiguous")


def _pointer_leaf(item: Mapping[str, object]) -> str:
    pointers = cast(Sequence[str], item["pointers"])
    if not pointers:
        raise ContractAtlasError(f"navigation identity has no structural pointer: {item['id']}")
    parts = _pointer_parts(pointers[0])
    if not parts:
        raise ContractAtlasError(f"navigation identity points at the projection root: {item['id']}")
    return parts[-1]


def _atomic_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    return NavigationIdentity((str(item["title"]),))


def _release_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    prefixes = {
        "artifact-verification": "Trust: ",
        "compatibility-guarantees": "Compatibility: ",
        "installation-roots": "Installation root: ",
        "publication-locations": "Coordinates: ",
        "python-distributions": "Python distribution: ",
        "release-artifacts": "Release artifact: ",
        "runtime-images": "Runtime image: ",
        "versioning-tags": "Versioning: ",
    }
    interface = str(item["interface"])
    leaf = _pointer_leaf(item)
    display = (
        leaf.replace("_", " ")
        if interface in {"compatibility-guarantees", "versioning-tags"}
        else leaf
    )
    expected = f"{prefixes[interface]}{display}"
    if str(item["title"]) != expected:
        raise ContractAtlasError(f"release navigation is not structurally exact: {item['id']}")
    if interface == "versioning-tags":
        components = tuple(leaf.split("_"))
        if len(components) == 1:
            components = ("versioning", *components)
        return NavigationIdentity(components, separator=" ")
    return NavigationIdentity((display,))


def _cli_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    details = cast(Mapping[str, object], item.get("details", {}))
    command_path = tuple(str(value) for value in cast(Sequence[object], details["command_path"]))
    if not command_path or str(item["title"]) != " ".join(command_path):
        raise ContractAtlasError(f"CLI navigation is not structurally exact: {item['id']}")
    context = command_path[:1] if len(command_path) > 1 else ()
    return NavigationIdentity(command_path, context=context, separator=" ")


def _configuration_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    leaf = _pointer_leaf(item)
    if str(item["title"]) != f"{leaf} configuration":
        raise ContractAtlasError(
            f"configuration navigation is not structurally exact: {item['id']}"
        )
    authority = str(item["authority"])
    prefix = f"{authority}:configuration:"
    if not leaf.startswith(prefix) or not leaf.removeprefix(prefix):
        raise ContractAtlasError(f"configuration identity has no owned local unit: {item['id']}")
    return NavigationIdentity(
        (authority, "configuration", leaf.removeprefix(prefix)),
        context=(authority, "configuration"),
    )


def _durable_state_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    details = cast(Mapping[str, object], item.get("details", {}))
    owner = str(details["state_owner"])
    unit = str(details["state_unit"])
    kind_labels = {
        "append-only-json-sequence": "Append-only JSON sequence",
        "identity": "Schema identity",
        "json-document": "JSON document",
        "opaque-bytes": "Opaque bytes",
        "relational-schema": "Relational schema",
        "relational-table": "Relational table",
        "text-document": "Text document",
        "unique-index": "Unique index",
    }
    try:
        kind = kind_labels[unit]
    except KeyError as exc:
        raise ContractAtlasError(f"unknown durable-state unit kind: {unit}") from exc
    title = str(item["title"])
    if unit == "identity":
        if title != f"{owner} durable-state identity":
            raise ContractAtlasError(f"durable-state identity is not exact: {item['id']}")
        return NavigationIdentity(("Schema identity",), kind=kind)
    if title == f"{owner} durable state":
        return NavigationIdentity(tuple(unit.split("-")), separator=" ", kind=kind)
    prefix = f"{owner}: "
    if not title.startswith(prefix):
        raise ContractAtlasError(f"durable-state unit is not exact: {item['id']}")
    return NavigationIdentity((owner, title[len(prefix) :]), context=(owner,), kind=kind)


def _extent_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    title = str(item["title"])
    if ": " not in title:
        raise ContractAtlasError(f"extent navigation is not structurally exact: {item['id']}")
    kind, leaf = title.split(": ", 1)
    return NavigationIdentity((kind, leaf))


def _http_schema_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    leaf = _pointer_leaf(item)
    if str(item["title"]) != f"schemas: {leaf}":
        raise ContractAtlasError(f"HTTP schema navigation is not structurally exact: {item['id']}")
    return NavigationIdentity((leaf,))


def _http_security_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    leaf = _pointer_leaf(item)
    if str(item["title"]) != f"securitySchemes: {leaf}":
        raise ContractAtlasError(
            f"HTTP security-scheme navigation is not structurally exact: {item['id']}"
        )
    return NavigationIdentity((leaf,))


def _http_service_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    authority = str(item["authority"])
    if str(item["title"]) != f"{authority} HTTP service":
        raise ContractAtlasError(f"HTTP service navigation is not structurally exact: {item['id']}")
    return NavigationIdentity(("Service declaration",))


def _protocol_name(item: Mapping[str, object]) -> str:
    parts = _pointer_parts(cast(Sequence[str], item["pointers"])[0])
    try:
        index = parts.index("protocol_schemas")
        return parts[index + 1]
    except (ValueError, IndexError) as exc:
        raise ContractAtlasError(f"process protocol pointer is not exact: {item['id']}") from exc


def _process_protocol_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    protocol = _protocol_name(item)
    if str(item["title"]) != f"{protocol} protocol":
        raise ContractAtlasError(f"process protocol navigation is not exact: {item['id']}")
    return NavigationIdentity((protocol,))


def _process_schema_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    protocol = _protocol_name(item)
    leaf = _pointer_leaf(item)
    if str(item["title"]) != f"{protocol}: {leaf}":
        raise ContractAtlasError(f"process schema navigation is not exact: {item['id']}")
    return NavigationIdentity((protocol, leaf), context=(protocol,))


def _python_navigation(item: Mapping[str, object]) -> NavigationIdentity:
    details = cast(Mapping[str, object], item.get("details", {}))
    public_identity = str(details["public_identity"])
    components = tuple(public_identity.split("."))
    unit = str(details["unit"])
    if unit == "export":
        context = tuple(str(details["module"]).split("."))
    elif unit == "member":
        context = tuple(str(details["owner"]).split("."))
    else:
        raise ContractAtlasError(f"unknown Python navigation unit: {unit}")
    if components[: len(context)] != context or str(item["title"]) != public_identity:
        raise ContractAtlasError(f"Python navigation is not structurally exact: {item['id']}")
    return NavigationIdentity(components, context=context, separator=".")


NavigationProvider = Callable[[Mapping[str, object]], NavigationIdentity]
_NAVIGATION_FUNCTIONS: dict[str, NavigationProvider] = {
    "atomic": _atomic_navigation,
    "cli": _cli_navigation,
    "configuration": _configuration_navigation,
    "durable-state": _durable_state_navigation,
    "extent": _extent_navigation,
    "http-schema": _http_schema_navigation,
    "http-security": _http_security_navigation,
    "http-service": _http_service_navigation,
    "process-protocol": _process_protocol_navigation,
    "process-schema": _process_schema_navigation,
    "python": _python_navigation,
    "release": _release_navigation,
}


def _navigation_identity(item: Mapping[str, object]) -> NavigationIdentity:
    interface = str(item["interface"])
    try:
        descriptor = INTERFACE_REGISTRY[interface]
        provider = _NAVIGATION_FUNCTIONS[descriptor.navigation_provider]
    except KeyError as exc:
        raise ContractAtlasError(
            f"interface navigation descriptor is incomplete: {interface}"
        ) from exc
    configured = {value.navigation_provider for value in INTERFACE_REGISTRY.values()}
    if configured != set(_NAVIGATION_FUNCTIONS):
        missing = sorted(configured - set(_NAVIGATION_FUNCTIONS))
        stale = sorted(set(_NAVIGATION_FUNCTIONS) - configured)
        raise ContractAtlasError(
            f"navigation-provider registry is not exact: missing={missing}, stale={stale}"
        )
    return provider(item)


def _navigation_candidates(item: Mapping[str, object]) -> list[str]:
    navigation = _navigation_identity(item)
    if (
        not navigation.components
        or any(not value for value in navigation.components)
        or navigation.components[: len(navigation.context)] != navigation.context
    ):
        raise ContractAtlasError(
            f"navigation identity has invalid structural context: {item['id']}"
        )
    visible = navigation.components[len(navigation.context) :]
    if not visible:
        visible = navigation.components[-1:]
    candidates = [navigation.separator.join(visible)]
    if visible != navigation.components:
        candidates.append(navigation.separator.join(navigation.components))
    return [*dict.fromkeys([*candidates, str(item["title"])])]


def _navigation_labels(items: Sequence[Mapping[str, object]]) -> dict[str, str]:
    return _contextual_labels(items, _navigation_candidates)


def _interface_navigation_contexts(
    interface: str,
    values: Sequence[Mapping[str, object]],
) -> list[list[Mapping[str, object]]]:
    if interface != "python":
        return [list(values)]
    contexts: list[list[Mapping[str, object]]] = []
    by_context: dict[tuple[str, str], list[Mapping[str, object]]] = defaultdict(list)
    for item in values:
        details = cast(Mapping[str, object], item["details"])
        unit = str(details["unit"])
        owner = str(details["module"] if unit == "export" else details["owner"])
        by_context[(unit, owner)].append(item)
    for key in sorted(by_context):
        contexts.append(by_context[key])
    return contexts


def _interface_navigation_labels(
    interface: str,
    values: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    if interface == "cli":
        return {str(item["id"]): _navigation_identity(item).components[-1] for item in values}
    labels: dict[str, str] = {}
    for context in _interface_navigation_contexts(interface, values):
        for identity, label in _navigation_labels(context).items():
            if identity in labels:
                raise ContractAtlasError(f"navigation identity appears in two contexts: {identity}")
            labels[identity] = label
    if set(labels) != {str(item["id"]) for item in values}:
        raise ContractAtlasError(f"interface navigation does not cover every {interface} item")
    return labels


def _relationship_node_anchor(node_id: str) -> str:
    return _anchor_id("relationship-node", node_id)


def _relationship_edge_anchor(edge: Mapping[str, object]) -> str:
    return _anchor_id(
        "relationship-edge",
        canonical_sha256(
            {
                "type": edge["type"],
                "source": edge["source"],
                "target": edge["target"],
                "scope": edge.get("scope"),
                "binding": edge.get("binding"),
            }
        ),
    )


def _md(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").replace("](", "]&#40;")


def _interface_index_path(authority: str, interface: str) -> str:
    return (
        f"{ATLAS_DIRECTORY}/authorities/{_slug(authority, limit=72)}/"
        f"{_slug(interface, limit=48)}/index.md"
    )


def _extension_context_path(extension: Mapping[str, object]) -> str:
    """Return the identity-derived side-context path for one extension boundary."""

    return f"{ATLAS_DIRECTORY}/extensions/{_slug(str(extension['id']), limit=96)}.md"


def _dossier_navigation_labels(
    element: Mapping[str, object],
    targets: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    local = [
        target
        for target in targets
        if target["authority"] == element["authority"]
        and target["interface"] == element["interface"]
    ]
    labels = _navigation_labels(local) if local else {}
    labels.update(
        {str(target["id"]): str(target["title"]) for target in targets if target not in local}
    )
    return labels

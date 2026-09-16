"""Exact Markdown rendering for individual Riverhog contract dossiers."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import cast

from .discovery import _source_index
from .model import (
    _SCHEMA_MAPPING_KEYWORDS,
    _SCHEMA_SEQUENCE_KEYWORDS,
    _SCHEMA_VALUE_KEYWORDS,
    ATLAS_DIRECTORY,
    INTERFACE_REGISTRY,
    ContractAtlasError,
    _escape_pointer,
    _pointer_parts,
    _slug,
    canonical_sha256,
    pointer_value,
    structural_json_schema,
)
from .navigation import (
    _anchor_id,
    _anchor_link,
    _dossier_navigation_labels,
    _html_anchor,
    _interface_label,
    _md,
    _navigation_identity,
    _policy_anchor,
    _policy_application_anchor,
    _qualification_anchor,
    _relationship_node_anchor,
    _relative_link,
    _repository_source_link,
    _source_anchor,
    _subject_anchor,
    _subject_marker,
)


def _render_cli_navigation_tree(
    values: Sequence[Mapping[str, object]], *, interface_path: str
) -> list[str]:
    by_path: dict[tuple[str, ...], Mapping[str, object]] = {}
    for item in values:
        path = _navigation_identity(item).components
        if path in by_path:
            raise ContractAtlasError(f"duplicate CLI command path: {' '.join(path)}")
        by_path[path] = item
    for path in by_path:
        if len(path) > 1 and path[:-1] not in by_path:
            raise ContractAtlasError(
                f"CLI command path lacks its structural parent: {' '.join(path)}"
            )

    lines: list[str] = []

    def append_path(path: tuple[str, ...], depth: int) -> None:
        item = by_path[path]
        lines.append(
            f"{'  ' * depth}- [{_md(path[-1])}]"
            f"({_relative_link(interface_path, str(item['dossier']))})"
        )
        children = sorted(
            candidate
            for candidate in by_path
            if len(candidate) == len(path) + 1 and candidate[:-1] == path
        )
        for child in children:
            append_path(child, depth + 1)

    roots = sorted(path for path in by_path if len(path) == 1)
    if not roots:
        raise ContractAtlasError("CLI interface has no root command")
    for root in roots:
        append_path(root, 0)
    if len(lines) != len(values):
        raise ContractAtlasError("CLI command tree does not cover every command")
    return lines


def _compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _remaining_fields(value: Mapping[str, object], handled: Iterable[str]) -> str:
    """Keep fields outside a specialized presentation visible at their use site."""

    return "; ".join(
        f"`{_md(key)}`: `{_md(_compact_json(item))}`"
        for key, item in value.items()
        if key not in handled
    )


def _remaining_lines(value: Mapping[str, object], handled: Iterable[str]) -> list[str]:
    return [
        f"- {_remaining_fields({key: item}, ())}"
        for key, item in value.items()
        if key not in handled
    ]


def _one_cli_authority_element(
    matches: Iterable[Mapping[str, object]], *, kind: str
) -> Mapping[str, object]:
    resolved = list(matches)
    if len(resolved) != 1:
        raise ContractAtlasError(f"CLI {kind} authority resolves to {len(resolved)} atlas elements")
    return resolved[0]


def _cli_authority_reference(
    semantics: Mapping[str, object],
    *,
    element: Mapping[str, object],
    pointer: str,
    path: str,
    projection: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> str:
    kind = str(semantics.get("kind", ""))
    elements = elements_by_id.values()
    target: Mapping[str, object]
    anchor: str | None = None
    represented = {"kind"}

    if kind == "http-operation-response":
        required = {"application", "operation_id", "method", "path", "status", "schema"}
        represented |= required
        if not required <= set(semantics):
            raise ContractAtlasError("CLI HTTP response authority is incomplete")
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["authority"] == semantics["application"]
                and item["interface"] == "http-operations"
                and cast(Mapping[str, object], item.get("details", {})).get("operation_id")
                == semantics["operation_id"]
                and cast(Mapping[str, object], item.get("details", {})).get("method")
                == semantics["method"]
                and cast(Mapping[str, object], item.get("details", {})).get("path")
                == semantics["path"]
            ),
            kind=kind,
        )
        operation_pointer = cast(Sequence[str], target["pointers"])[0]
        response_pointer = (
            f"{operation_pointer}/responses/{_escape_pointer(str(semantics['status']))}"
        )
        response = pointer_value(projection, response_pointer)
        media = (
            cast(Mapping[str, object], response).get("content", {})
            if isinstance(response, Mapping)
            else {}
        )
        json_media = (
            cast(Mapping[str, object], media).get("application/json")
            if isinstance(media, Mapping)
            else None
        )
        if not isinstance(json_media, Mapping) or json_media.get("schema") != semantics["schema"]:
            raise ContractAtlasError("CLI HTTP response authority differs from its operation")
        anchor = _subject_anchor(response_pointer)
        label = f"HTTP {semantics['operation_id']} response {semantics['status']}"
    elif kind == "openapi-schema":
        required = {"application", "schema", "definition"}
        represented |= required
        if not required <= set(semantics):
            raise ContractAtlasError("CLI OpenAPI schema authority is incomplete")
        schema_pointer = (
            f"/external_contract/http_openapi/{_escape_pointer(str(semantics['application']))}/"
            f"components/schemas/{_escape_pointer(str(semantics['schema']))}"
        )
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["authority"] == semantics["application"]
                and item["interface"] == "http-schemas"
                and schema_pointer in cast(Sequence[str], item["pointers"])
            ),
            kind=kind,
        )
        if pointer_value(projection, schema_pointer) != semantics["definition"]:
            raise ContractAtlasError("CLI OpenAPI schema authority has a different definition")
        label = f"OpenAPI {semantics['application']}.{semantics['schema']}"
    elif kind == "python-model":
        represented |= {"identity", "schema"}
        identity = semantics.get("identity")
        if not isinstance(identity, str) or not isinstance(semantics.get("schema"), Mapping):
            raise ContractAtlasError("CLI Python model authority is incomplete")
        declared_owner, separator, declared_symbol = identity.rpartition(".")
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["interface"] == "python"
                and (
                    item["title"] == identity
                    or (
                        bool(separator)
                        and item["authority"] == declared_owner
                        and str(item["title"]).rsplit(".", 1)[-1] == declared_symbol
                    )
                )
            ),
            kind=kind,
        )
        value = pointer_value(projection, cast(Sequence[str], target["pointers"])[0])
        contract = cast(Mapping[str, object], value).get("contract", {})
        if not isinstance(contract, Mapping) or contract.get("schema") != structural_json_schema(
            semantics["schema"]
        ):
            raise ContractAtlasError("CLI Python model authority has a different schema")
        label = identity
    elif kind == "schema-authority":
        represented |= {"authority", "definition"}
        authority = semantics.get("authority")
        if not isinstance(authority, str) or not authority:
            raise ContractAtlasError("CLI schema authority is incomplete")
        schema_pointer = f"/external_contract/protocol_schemas/{_escape_pointer(authority)}"
        definition = semantics.get("definition")
        expected_interface = "schema"
        if definition is not None:
            if not isinstance(definition, str) or not definition:
                raise ContractAtlasError("CLI schema definition authority is invalid")
            schema_pointer += f"/schemas/{_escape_pointer(definition)}"
            expected_interface = "process-protocol-schemas"
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["interface"] == expected_interface
                and schema_pointer in cast(Sequence[str], item["pointers"])
            ),
            kind=kind,
        )
        pointer_value(projection, schema_pointer)
        label = str(target["title"])
    elif kind == "document-authority":
        represented |= {"authority"}
        authority = semantics.get("authority")
        if not isinstance(authority, str) or not authority:
            raise ContractAtlasError("CLI document authority is incomplete")
        base = f"/external_contract/protocol_schemas/{_escape_pointer(authority)}"
        target = _one_cli_authority_element(
            (
                item
                for item in elements
                if item["interface"] == "process-protocol"
                and cast(Sequence[str], item["pointers"])
                and all(
                    candidate.startswith(f"{base}/")
                    for candidate in cast(Sequence[str], item["pointers"])
                )
            ),
            kind=kind,
        )
        label = str(target["title"])
    elif kind in {
        "cli-local-exact-json",
        "cli-local-json-schema",
        "cli-local-json-sequence",
    }:
        # These definitions are rendered in the local structured-output section.
        represented |= {"identity"} | {
            "cli-local-json-schema": {"schema"},
            "cli-local-exact-json": {"document"},
            "cli-local-json-sequence": {"records", "framing", "sequence"},
        }[kind]
        identity = semantics.get("identity")
        if not isinstance(identity, str) or not identity:
            raise ContractAtlasError("CLI-local structured authority has no identity")
        target = element
        anchor = _subject_anchor(pointer)
        label = identity
    else:
        raise ContractAtlasError(f"CLI structured output has no atlas resolver: {kind or '<none>'}")

    target_path = str(target["dossier"])
    href = (
        _anchor_link(path, target_path, anchor)
        if anchor is not None
        else _relative_link(path, target_path)
    )
    modifiers = _remaining_fields(semantics, represented)
    return f"[{_md(label)}]({href})" + (f"; {modifiers}" if modifiers else "")


def _cli_channel_summary(
    value: Mapping[str, object],
    *,
    element: Mapping[str, object],
    pointer: str,
    path: str,
    projection: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> str:
    parts: list[str] = []
    for mode, semantics in value.items():
        if not isinstance(semantics, Mapping):
            parts.append(f"{mode}: `{_md(_compact_json(semantics))}`")
            continue
        parts.append(
            f"{mode}: "
            + _cli_authority_reference(
                semantics,
                element=element,
                pointer=f"{pointer}/{_escape_pointer(str(mode))}",
                path=path,
                projection=projection,
                elements_by_id=elements_by_id,
            )
        )
    return "; ".join(parts)


def _schema_items(value: Mapping[str, object]) -> Iterable[tuple[str, object]]:
    keys = dict.fromkeys(("$ref", "type", "format", "const", "enum", "minimum", "maximum", *value))
    return ((key, value[key]) for key in keys if key in value)


def _shape_summary(value: object, reference_link: Callable[[str], str] | None = None) -> str:
    """Describe an actual schema, keeping literal defaults separate from subschemas."""

    if value is True or value == {}:
        return "any JSON value"
    if value is False:
        return "no JSON value"
    if not isinstance(value, Mapping):
        raise ContractAtlasError("schema summary received a non-schema value")
    parts: list[str] = []
    for key, item in _schema_items(value):
        if key == "$ref" and isinstance(item, str) and reference_link is not None:
            parts.append(reference_link(item))
        elif key in _SCHEMA_MAPPING_KEYWORDS and isinstance(item, Mapping):
            rendered = "; ".join(
                f"{_md(name)}: ({_shape_summary(child, reference_link)})"
                for name, child in item.items()
            )
            parts.append(f"{key}={{{rendered}}}")
        elif key in _SCHEMA_SEQUENCE_KEYWORDS and isinstance(item, list):
            parts.append(
                f"{key}=["
                + "; ".join(f"({_shape_summary(child, reference_link)})" for child in item)
                + "]"
            )
        elif key in _SCHEMA_VALUE_KEYWORDS and isinstance(item, (bool, Mapping)):
            parts.append(f"{key}=({_shape_summary(item, reference_link)})")
        else:
            parts.append(f"{_md(key)}={_md(_compact_json(item))}")
    return "; ".join(parts) or "any JSON value"


def _schema_reference_linker(
    value: object,
    base_pointer: str,
    *,
    element: Mapping[str, object] | None = None,
    elements_by_id: Mapping[str, Mapping[str, object]] | None = None,
) -> Callable[[str], str]:
    """Resolve schema references in their document, never by a short definition name."""

    owners: dict[str, Mapping[str, object]] = {}
    if element is not None and elements_by_id is not None:
        prefix = f"/external_contract/http_openapi/{element['authority']}"
        for owner in elements_by_id.values():
            if owner["authority"] != element["authority"] or owner["interface"] != "http-schemas":
                continue
            for pointer in cast(Sequence[str], owner["pointers"]):
                if pointer.startswith(f"{prefix}/components/"):
                    reference = f"#{pointer.removeprefix(prefix)}"
                    if reference in owners:
                        raise ContractAtlasError(
                            f"schema reference has multiple owners: {reference}"
                        )
                    owners[reference] = owner

    def link(reference: str) -> str:
        if reference in owners:
            assert element is not None
            owner = owners[reference]
            label = _pointer_parts(reference[1:])[-1]
            return (
                f"[{_md(label)}]({_relative_link(str(element['dossier']), str(owner['dossier']))})"
            )
        if reference == "#" or reference.startswith("#/$defs/"):
            # Validate the whole pointer, including escaped names and recursive references.
            try:
                pointer_value(value, reference[1:])
            except (KeyError, IndexError, TypeError, ValueError) as exc:
                raise ContractAtlasError(f"unresolved local schema reference: {reference}") from exc
            label = _pointer_parts(reference[1:])[-1] if reference != "#" else "schema root"
            return f"[{_md(label)}](#{_subject_anchor(base_pointer + reference[1:])})"
        if reference.startswith("#"):
            raise ContractAtlasError(f"unresolved local schema reference: {reference}")
        return f"`{_md(reference)}`"

    return link


def _schema_needs_detail(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    return bool(set(value) & {"properties", "$defs", "if", "allOf", "prefixItems"}) or any(
        _schema_needs_detail(child)
        for key, item in value.items()
        for child in (
            item
            if key in _SCHEMA_SEQUENCE_KEYWORDS and isinstance(item, list)
            else [item]
            if key in _SCHEMA_VALUE_KEYWORDS
            else []
        )
    )


def _render_schema(
    value: object,
    base_pointer: str,
    placed_subjects: set[str],
    *,
    heading_level: int = 3,
    element: Mapping[str, object] | None = None,
    elements_by_id: Mapping[str, Mapping[str, object]] | None = None,
) -> list[str]:
    """Render fields and nested schemas in linked sections within their owning dossier."""

    pending: list[tuple[object, str, str]] = [(value, base_pointer, "")]
    lines: list[str] = []
    reference_link = _schema_reference_linker(
        value, base_pointer, element=element, elements_by_id=elements_by_id
    )

    def shape(schema: object) -> str:
        return _shape_summary(schema, reference_link)

    def describe(schema: object, pointer: str, label: str) -> str:
        if _schema_needs_detail(schema):
            pending.append((schema, pointer, label))
            return f"[See {_md(label)}](#{_subject_anchor(pointer)})"
        return shape(schema)

    for schema, pointer, context in pending:
        level = heading_level + bool(context)
        if context:
            lines.extend(
                [
                    "",
                    f"{'#' * heading_level} {_subject_marker(pointer, placed_subjects)}{context}",
                    "",
                ]
            )
        else:
            lines.extend([_subject_marker(pointer, placed_subjects), ""])
        if isinstance(schema, bool) or schema == {}:
            lines.append(f"- Accepts: {shape(schema)}.")
            continue
        if not isinstance(schema, Mapping):
            raise ContractAtlasError(f"schema renderer received a non-schema: {pointer}")

        def label(text: str, context: str = context) -> str:
            return f"{context} · {text}" if context else text

        structured = {"properties", "$defs", "allOf", "anyOf", "oneOf", "prefixItems"}
        for key, item in _schema_items(schema):
            if key in structured:
                expected = Mapping if key in {"properties", "$defs"} else list
                if not isinstance(item, expected):
                    raise ContractAtlasError(f"invalid schema structure at {pointer}/{key}")
                continue
            child_pointer = f"{pointer}/{_escape_pointer(str(key))}"
            if key == "$ref" and isinstance(item, str):
                rendered = reference_link(item)
            elif key in _SCHEMA_VALUE_KEYWORDS and isinstance(item, Mapping):
                rendered = describe(item, child_pointer, label(f"`{key}`"))
            else:
                rendered = f"`{_md(_compact_json(item))}`"
            marker = (
                ""
                if _schema_needs_detail(item) and key in _SCHEMA_VALUE_KEYWORDS
                else (_subject_marker(child_pointer, placed_subjects))
            )
            lines.append(f"- {marker}`{key}`: {rendered}")

        properties = schema.get("properties")
        if isinstance(properties, Mapping):
            required = cast(Sequence[str], schema.get("required", ()))
            lines.extend(
                [
                    "",
                    f"{'#' * level} Fields",
                    "",
                    "| Field | Required | Shape | Description |",
                    "|---|---:|---|---|",
                ]
            )
            for name, field in properties.items():
                field_pointer = f"{pointer}/properties/{_escape_pointer(str(name))}"
                field_map = field if isinstance(field, Mapping) else {}
                # The description has its own column for inline fields.
                inline_field = (
                    {key: item for key, item in field_map.items() if key != "description"}
                    if isinstance(field, Mapping) and not _schema_needs_detail(field)
                    else field
                )
                description = describe(inline_field, field_pointer, label(f"field `{name}`"))
                marker = (
                    ""
                    if _schema_needs_detail(field)
                    else _subject_marker(field_pointer, placed_subjects)
                )
                lines.append(
                    f"| {marker}`{_md(name)}` | {'yes' if name in required else 'no'} | "
                    f"{description} | {_md(field_map.get('description', ''))} |"
                )

        for key, meaning in (
            ("allOf", "All must match"),
            ("anyOf", "At least one must match"),
            ("oneOf", "Exactly one must match"),
            ("prefixItems", "Positional item schemas, in order"),
        ):
            variants = schema.get(key)
            if not isinstance(variants, list):
                continue
            lines.extend(["", f"{'#' * level} {meaning} (`{key}`)", ""])
            conditional = key == "allOf" and all(
                isinstance(item, Mapping) and "if" in item and set(item) <= {"if", "then", "else"}
                for item in variants
            )
            if conditional:
                lines.extend(
                    [
                        "| Rule | If schema matches | Then must match | Otherwise must match |",
                        "|---|---|---|---|",
                    ]
                )
            else:
                lines.extend(["| Alternative | Schema |", "|---|---|"])
            for index, item in enumerate(variants):
                child_pointer = f"{pointer}/{key}/{index}"
                if conditional:
                    clauses = cast(Mapping[str, object], item)
                    rendered = " | ".join(
                        shape(clauses[clause]) if clause in clauses else "no additional constraint"
                        for clause in ("if", "then", "else")
                    )
                    marker = _subject_marker(child_pointer, placed_subjects)
                else:
                    rendered = describe(
                        item, child_pointer, label(f"`{key}` alternative {index + 1}")
                    )
                    marker = (
                        ""
                        if _schema_needs_detail(item)
                        else _subject_marker(child_pointer, placed_subjects)
                    )
                lines.append(f"| {marker}{index + 1} | {rendered} |")

        nested = schema.get("$defs")
        if isinstance(nested, Mapping):
            lines.extend(["", f"{'#' * level} Definitions", ""])
            for name, child in nested.items():
                child_pointer = f"{pointer}/$defs/{_escape_pointer(str(name))}"
                child_label = label(f"definition `{name}`")
                pending.append((child, child_pointer, child_label))
                lines.append(f"- [{_md(name)}](#{_subject_anchor(child_pointer)})")
    return lines


def _render_python(
    value: Mapping[str, object], base_pointer: str, placed_subjects: set[str]
) -> list[str]:
    """Render one exact declared Python unit without hiding structural promises."""

    lines = [_subject_marker(base_pointer, placed_subjects)]
    lines.extend(
        _remaining_lines(value, {"distribution", "module", "name", "owner", "unit", "contract"})
    )
    for key in ("distribution", "module", "name", "owner", "unit"):
        if key in value:
            pointer = f"{base_pointer}/{_escape_pointer(key)}"
            lines.append(
                f"- {_subject_marker(pointer, placed_subjects)}`{key}`: `{_md(value[key])}`"
            )
    contract = cast(Mapping[str, object], value["contract"])
    contract_pointer = f"{base_pointer}/contract"
    lines.extend(["", "### Declared structure", ""])
    for key in ("kind", "signature", "type", "value"):
        if key in contract:
            pointer = f"{contract_pointer}/{_escape_pointer(key)}"
            rendered = _compact_json(contract[key])
            lines.append(f"- {_subject_marker(pointer, placed_subjects)}`{key}`: `{_md(rendered)}`")
    lines.extend(
        _remaining_lines(
            contract, {"kind", "signature", "type", "value", "enum_values", "fields", "schema"}
        )
    )
    enum_values = contract.get("enum_values")
    if isinstance(enum_values, Mapping):
        lines.extend(["", "#### Enum members", "", "| Member | Value |", "|---|---|"])
        for name, item in enum_values.items():
            pointer = f"{contract_pointer}/enum_values/{_escape_pointer(str(name))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(name)}` | "
                f"`{_md(_compact_json(item))}` |"
            )
    fields = contract.get("fields")
    if isinstance(fields, list):
        lines.extend(
            ["", "#### Dataclass fields", "", "| Field | Type | Default |", "|---|---|---|"]
        )
        for index, item in enumerate(cast(Sequence[Mapping[str, object]], fields)):
            pointer = f"{contract_pointer}/fields/{index}"
            other = _remaining_fields(item, {"name", "type", "default"})
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(item['name'])}` | "
                f"`{_md(item['type'])}` | `{_md(item['default'])}`"
                f"{'<br>' + other if other else ''} |"
            )
    schema = contract.get("schema")
    if isinstance(schema, Mapping):
        lines.extend(["", "#### Validated model schema", ""])
        lines.extend(
            _render_schema(
                cast(Mapping[str, object], schema),
                f"{contract_pointer}/schema",
                placed_subjects,
                heading_level=5,
            )
        )
    return lines


def _render_relational_table(
    value: Mapping[str, object], pointer: str, placed_subjects: set[str]
) -> list[str]:
    lines = [_subject_marker(pointer, placed_subjects), ""]
    lines.extend(_remaining_lines(value, {"name", "columns", "constraints"}))
    lines.extend(
        [
            f"### Table: `{_md(value['name'])}`",
            "",
            "#### Columns",
            "",
            "| Column | Type | Nullable | Default | Other constraints |",
            "|---|---|---:|---|---|",
        ]
    )
    for index, column in enumerate(cast(Sequence[Mapping[str, object]], value["columns"])):
        column_pointer = f"{pointer}/columns/{index}"
        other = {
            key: item
            for key, item in column.items()
            if key not in {"name", "type", "nullable", "default", "definition"}
        }
        lines.append(
            f"| {_subject_marker(column_pointer, placed_subjects)}`{_md(column['name'])}` | "
            f"`{_md(column['type'])}` | {'yes' if column['nullable'] else 'no'} | "
            f"`{_md(column.get('default', '—'))}` | "
            f"{_md(_compact_json(other)) if other else '—'} |"
        )
    constraints = cast(Sequence[Mapping[str, object]], value.get("constraints", ()))
    if constraints:
        lines.extend(
            [
                "",
                "#### Table constraints",
                "",
                "| Kind | Name | Exact definition |",
                "|---|---|---|",
            ]
        )
        for index, constraint in enumerate(constraints):
            constraint_pointer = f"{pointer}/constraints/{index}"
            lines.append(
                f"| {_subject_marker(constraint_pointer, placed_subjects)}"
                f"`{_md(constraint['kind'])}` | "
                f"`{_md(constraint.get('name', '—'))}` | `{_md(constraint['definition'])}` |"
            )
        # Parsed columns/references/expressions restate the exact SQL definition.
        for constraint in constraints:
            extra = _remaining_fields(
                constraint, {"kind", "name", "definition", "columns", "references", "expression"}
            )
            if extra:
                lines.extend(["", f"Constraint `{_md(constraint['definition'])}`: {extra}"])
    return lines


def _render_durable_state(
    pointers: Sequence[str],
    values: Sequence[object],
    details: Mapping[str, object],
    placed_subjects: set[str],
) -> list[str]:
    """Render one bounded, owner-projected durable-state audit unit."""

    unit = str(details["state_unit"])
    if unit == "identity":
        return [
            "| Authority fact | Value |",
            "|---|---|",
            *(
                f"| {_subject_marker(pointer, placed_subjects)}"
                f"`{_md(_pointer_parts(pointer)[-1])}` "
                f"| `{_md(_compact_json(value))}` |"
                for pointer, value in zip(pointers, values, strict=True)
            ),
        ]
    if len(pointers) != 1 or len(values) != 1 or not isinstance(values[0], Mapping):
        raise ContractAtlasError(f"durable-state unit is not exact: {details['state_owner']}")
    pointer = pointers[0]
    value = cast(Mapping[str, object], values[0])
    if unit == "relational-table":
        return _render_relational_table(value, pointer, placed_subjects)
    lines = [_subject_marker(pointer, placed_subjects), ""]
    if unit == "unique-index":
        return [
            *lines,
            "| Index fact | Value |",
            "|---|---|",
            *(f"| `{_md(key)}` | `{_md(_compact_json(item))}` |" for key, item in value.items()),
        ]
    if unit == "json-document":
        lines.extend([f"- Document: `{_md(value['id'])}`", "", "### Document schema", ""])
        lines.extend(_remaining_lines(value, {"id", "schema"}))
        schema = value.get("schema")
        if not isinstance(schema, Mapping):
            raise ContractAtlasError("durable JSON document has no exact schema")
        lines.extend(_render_schema(schema, f"{pointer}/schema", placed_subjects, heading_level=4))
        return lines
    if unit == "relational-schema":
        metadata = {key: item for key, item in value.items() if key != "tables"}
        lines.extend(_render_generic([pointer], [metadata], placed_subjects))
        for index, table in enumerate(cast(Sequence[Mapping[str, object]], value["tables"])):
            lines.extend(
                ["", *_render_relational_table(table, f"{pointer}/tables/{index}", placed_subjects)]
            )
        return lines
    if unit == "append-only-json-sequence":
        metadata = {key: item for key, item in value.items() if key != "record_schema"}
        lines.extend(_render_generic([pointer], [metadata], placed_subjects))
        lines.extend(["", "### Record schema", ""])
        lines.extend(
            _render_schema(
                value["record_schema"], f"{pointer}/record_schema", placed_subjects, heading_level=4
            )
        )
        return lines
    # Composite units and deliberately simple component-owned structures are
    # still rendered losslessly below and retain their bounded unit dossier.
    lines.extend(_render_generic([pointer], [value], placed_subjects))
    return lines


def _render_http(
    value: Mapping[str, object],
    details: Mapping[str, object],
    base_pointer: str,
    placed_subjects: set[str],
    *,
    element: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> list[str]:
    reference_link = _schema_reference_linker(
        value, base_pointer, element=element, elements_by_id=elements_by_id
    )

    def shape(schema: object) -> str:
        return _shape_summary(schema, reference_link)

    def media_shape(media: Mapping[str, object]) -> str:
        schema = shape(media["schema"]) if "schema" in media else "not declared"
        other = _remaining_fields(media, {"schema"})
        return schema + (f"; {other}" if other else "")

    lines = [_subject_marker(base_pointer, placed_subjects)]
    # All scalar/extension facts, including permission formulas, retain literal keys.
    for key, item in value.items():
        if key in {"parameters", "requestBody", "responses"} and item:
            continue
        pointer = f"{base_pointer}/{_escape_pointer(key)}"
        lines.append(
            f"- {_subject_marker(pointer, placed_subjects)}`{_md(key)}`: "
            f"`{_md(_compact_json(item))}`"
        )
    parameters = cast(Sequence[Mapping[str, object]], value.get("parameters", ()))
    if parameters:
        parameter_keys = {"name", "in", "required", "schema"}
        has_descriptions = any(set(item) - parameter_keys for item in parameters)
        lines.extend(
            [
                "",
                "### Parameters",
                "",
                "| Name | In | Required | Default | Schema |"
                + (" Description |" if has_descriptions else ""),
                "|---|---|---:|---|---|" + ("---|" if has_descriptions else ""),
            ]
        )
        for index, item in enumerate(parameters):
            pointer = f"{base_pointer}/parameters/{index}"
            schema = item.get("schema", {})
            default = (
                f"`{_md(_compact_json(schema['default']))}`"
                if isinstance(schema, Mapping) and "default" in schema
                else "not declared"
            )
            parameter_shape = (
                {key: item for key, item in schema.items() if key != "default"}
                if isinstance(schema, Mapping)
                else schema
            )
            description = _md(item.get("description", ""))
            other = _remaining_fields(item, {"name", "in", "required", "schema", "description"})
            if other:
                description += f"<br>{other}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(item.get('name', ''))}` | "
                f"{_md(item.get('in', ''))} | "
                f"{'yes' if item.get('required') else 'no'} | "
                f"{default} | {shape(parameter_shape)} |"
                + (f" {description} |" if has_descriptions else "")
            )
    if value.get("requestBody"):
        pointer = f"{base_pointer}/requestBody"
        body = cast(Mapping[str, object], value["requestBody"])
        lines.extend(["", f"### {_subject_marker(pointer, placed_subjects)}Request body", ""])
        lines.extend(_remaining_lines(body, {"content"} if body.get("content") else set()))
        content = cast(Mapping[str, Mapping[str, object]], body.get("content", {}))
        if content:
            lines.extend(["", "| Media type | Schema |", "|---|---|"])
            for media_type, media in content.items():
                lines.append(f"| {_md(media_type)} | {media_shape(media)} |")
    responses = value.get("responses")
    if isinstance(responses, Mapping):
        lines.extend(
            [
                "",
                "### Responses",
                "",
                "| Status | Description | Media type | Schema | Declared error codes |",
                "|---|---|---|---|---|",
            ]
        )
        for status, response in responses.items():
            pointer = f"{base_pointer}/responses/{_escape_pointer(str(status))}"
            response_map = cast(Mapping[str, object], response)
            content = cast(Mapping[str, Mapping[str, object]], response_map.get("content", {}))
            codes = ", ".join(
                f"`{_md(code)}`"
                for code in cast(Sequence[str], response_map.get("x-riverhog-error-codes", ()))
            )
            for media_type, media in content.items() or [("—", {})]:
                lines.append(
                    f"| {_subject_marker(pointer, placed_subjects)}`{_md(status)}` | "
                    f"{_md(response_map.get('description', ''))} | {_md(media_type)} | "
                    f"{media_shape(media)} | "
                    f"{codes or 'not declared'} |"
                )
        for status, response in responses.items():
            response_map = cast(Mapping[str, object], response)
            handled = {"description"}
            handled.update(
                key
                for key in ("content", "headers", "x-riverhog-error-codes")
                if response_map.get(key)
            )
            other = _remaining_fields(response_map, handled)
            if other:
                lines.extend(["", f"Response `{_md(status)}`: {other}"])
        headers = [
            (str(status), str(name), header)
            for status, response in responses.items()
            for name, header in cast(
                Mapping[str, Mapping[str, object]],
                cast(Mapping[str, object], response).get("headers", {}),
            ).items()
        ]
        if headers:
            lines.extend(
                [
                    "",
                    "#### Response headers",
                    "",
                    "| Status | Header | Required | Schema | Description |",
                    "|---|---|---|---|---|",
                ]
            )
            for status, name, header in headers:
                pointer = (
                    f"{base_pointer}/responses/{_escape_pointer(status)}"
                    f"/headers/{_escape_pointer(name)}"
                )
                required = (
                    ("yes" if header["required"] else "no")
                    if "required" in header
                    else "not declared"
                )
                description = _md(header.get("description", ""))
                other = _remaining_fields(header, {"required", "schema", "description"})
                if other:
                    description += f"<br>{other}"
                lines.append(
                    f"| `{_md(status)}` | {_subject_marker(pointer, placed_subjects)}"
                    f"`{_md(name)}` | "
                    f"{required} | "
                    f"{shape(header['schema']) if 'schema' in header else 'not declared'} | "
                    f"{description} |"
                )
    del details
    return lines


def _cli_parameter_type(parameter: Mapping[str, object]) -> str:
    type_ = parameter.get("type")
    if isinstance(type_, Mapping):
        parts = [_md(type_.get("name") or type_.get("class") or "not recorded")]
        constraints = type_
    else:
        parts = [_md(type_) if type_ is not None else "not recorded"]
        constraints = parameter
    for key in ("choices", "minimum", "maximum"):
        if key in constraints:
            bound = f"{key}=`{_md(_compact_json(constraints[key]))}`"
            open_key = {"minimum": "min_open", "maximum": "max_open"}.get(key)
            if open_key is not None and open_key in constraints:
                bound += " (exclusive)" if constraints[open_key] else " (inclusive)"
            parts.append(bound)
    if "clamp" in constraints:
        parts.append(
            "outside range: clamp to boundary" if constraints["clamp"] else "outside range: reject"
        )
    if "exists" in constraints:
        parts.extend(
            [
                "existence required" if constraints["exists"] else "existence not required",
                "regular files allowed" if constraints["file_okay"] else "regular files rejected",
                "directories allowed" if constraints["dir_okay"] else "directories rejected",
                "access checks on existing paths: "
                + (
                    ", ".join(
                        name
                        for key, name in (("readable", "read"), ("writable", "write"))
                        if constraints[key]
                    )
                    or "none"
                ),
                "resolve absolute path and symlinks: "
                + ("yes" if constraints["resolve_path"] else "no"),
                "dash bypasses path checks when files are allowed"
                if constraints["allow_dash"]
                else "dash uses normal path checks",
            ]
        )
    if isinstance(type_, Mapping):
        # Class is parser provenance; name is its exposed type. Open-bound flags
        # without a corresponding bound have no acceptance effect.
        other = _remaining_fields(
            type_,
            {
                "name",
                "class",
                "choices",
                "minimum",
                "maximum",
                "min_open",
                "max_open",
                "clamp",
                "exists",
                "file_okay",
                "dir_okay",
                "readable",
                "writable",
                "resolve_path",
                "allow_dash",
            },
        )
        if other:
            parts.append(other)
    return "; ".join(parts)


def _cli_invocation_summary(
    parameter: Mapping[str, object],
    arity: Mapping[str, object],
    occurrences: Mapping[str, object] | None,
) -> str:
    minimum, maximum = arity["minimum"], arity["maximum"]
    positional = parameter.get("kind") == "TyperArgument" or not parameter.get("options")
    form = "positional" if positional else "flag" if maximum == 0 else "option"
    parts = [f"{'required' if parameter.get('required') else 'optional'} {form}"]
    if maximum is None:
        parts.append(f"{minimum}+ values; no parser maximum")
    elif minimum == maximum:
        parts.append(f"{maximum} {'value' if maximum == 1 else 'values'}")
    else:
        parts.append(f"{minimum}–{maximum} values")
    if occurrences is not None:
        parts.append("counts repeats" if parameter.get("count") else "collects repeats")
        maximum = occurrences["maximum"]
        parts.append(
            "no declared occurrence maximum"
            if maximum is None
            else f"maximum {maximum} occurrences"
        )
    return "; ".join(parts)


def _render_cli(
    pointers: Sequence[str],
    values: Sequence[object],
    placed_subjects: set[str],
    *,
    element: Mapping[str, object],
    path: str,
    projection: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> list[str]:
    parameters: Sequence[Mapping[str, object]] = ()
    parameters_pointer = ""
    name = ""
    name_pointer = ""
    result_contract: Mapping[str, object] | None = None
    result_pointer = ""
    terminating_controls: Sequence[Mapping[str, object]] = ()
    terminating_controls_pointer = ""
    command_rules: dict[str, tuple[str, object]] = {}
    exclusive_groups: Sequence[Mapping[str, object]] = ()
    exclusive_groups_pointer = ""
    local_outputs: list[tuple[str, str, Mapping[str, object]]] = []
    remaining: list[str] = []
    for pointer, value in zip(pointers, values, strict=True):
        if value == []:
            remaining.extend(_render_generic([pointer], [value], placed_subjects))
            continue
        if isinstance(value, str) and pointer.endswith("/name"):
            name = value
            name_pointer = pointer
        elif isinstance(value, list) and pointer.endswith("/parameters"):
            parameters = cast(Sequence[Mapping[str, object]], value)
            parameters_pointer = pointer
        elif isinstance(value, list) and pointer.endswith("/terminating_controls"):
            terminating_controls = cast(Sequence[Mapping[str, object]], value)
            terminating_controls_pointer = pointer
        elif isinstance(value, list) and pointer.endswith("/mutually_exclusive_groups"):
            exclusive_groups = cast(Sequence[Mapping[str, object]], value)
            exclusive_groups_pointer = pointer
        elif pointer.rsplit("/", 1)[-1] in {
            "subcommand_required",
            "allow_abbrev",
            "allow_extra_args",
            "allow_interspersed_args",
            "ignore_unknown_options",
        }:
            command_rules[pointer.rsplit("/", 1)[-1]] = (pointer, value)
        elif isinstance(value, Mapping) and pointer.endswith("/result_contract"):
            result_contract = value
            result_pointer = pointer
        else:
            remaining.extend(_render_generic([pointer], [value], placed_subjects))
    lines = (
        [f"- {_subject_marker(name_pointer, placed_subjects)}Parser name: `{_md(name)}`"]
        if name_pointer
        else []
    )
    lines.extend(remaining)
    for key, label, yes, no in (
        ("subcommand_required", "Subcommand selection", "required", "optional"),
        ("allow_abbrev", "Unique long-option abbreviations", "accepted", "not accepted"),
        ("allow_extra_args", "Extra arguments at this parser", "accepted", "rejected"),
        (
            "allow_interspersed_args",
            "Options after positional arguments at this parser",
            "parsed as options",
            "left as arguments",
        ),
        (
            "ignore_unknown_options",
            "Unknown options at this parser",
            "left as arguments",
            "rejected",
        ),
    ):
        if key in command_rules:
            rule_pointer, enabled = command_rules[key]
            lines.append(
                f"- {_subject_marker(rule_pointer, placed_subjects)}"
                f"{label}: {yes if enabled else no}."
            )
            if key == "allow_extra_args" and enabled and "subcommand_required" in command_rules:
                lines[-1] += " Subcommand selection and child parsing still apply."
    if parameters:
        extents = cast(
            Sequence[Mapping[str, object]],
            cast(
                Mapping[str, object],
                cast(Mapping[str, object], projection["external_contract"])["extents"],
            )["decisions"],
        )
        parameter_extents: dict[tuple[str, str], Mapping[str, object]] = {}
        for decision in extents:
            subject, unit = str(decision["source_pointer"]), str(decision["unit"])
            if subject.startswith(f"{parameters_pointer}/") and unit in {
                "values-per-occurrence",
                "occurrences",
            }:
                extent_key = (subject, unit)
                if extent_key in parameter_extents:
                    raise ContractAtlasError(f"duplicate CLI parameter extent: {extent_key}")
                parameter_extents[extent_key] = decision
        lines.extend(
            [
                "",
                "### Parameters",
                "",
                "Value counts describe supplied CLI values per occurrence. Defaults and "
                "environment inputs below are recorded parser metadata; **not recorded** "
                "does not imply an explicit null default or the absence of other fallbacks.",
                "",
                "| Parameter / spelling | Invocation | Type / constraints | "
                "Default / environment |",
                "|---|---|---|---|",
            ]
        )
        for index, item in enumerate(parameters):
            pointer = f"{parameters_pointer}/{index}"
            parameter_name = item.get("name", item.get("dest", ""))
            spellings = [
                f"`{_md(option)}`" for option in cast(Sequence[str], item.get("options", ()))
            ]
            secondary = cast(Sequence[str], item.get("secondary_options", ()))
            if secondary:
                spellings.append(
                    "alternate: " + ", ".join(f"`{_md(option)}`" for option in secondary)
                )
            arity = parameter_extents.get((pointer, "values-per-occurrence"))
            if arity is None:
                raise ContractAtlasError(f"CLI parameter lacks value arity: {pointer}")
            invocation = _cli_invocation_summary(
                item, arity, parameter_extents.get((pointer, "occurrences"))
            )
            default = (
                f"`{_md(_compact_json(item['default']))}`" if "default" in item else "not recorded"
            )
            if "envvar" in item:
                default += f"<br>Env: `{_md(_compact_json(item['envvar']))}`"
            other = _remaining_fields(
                item,
                {
                    "name",
                    "dest",
                    "options",
                    "secondary_options",
                    "required",
                    "type",
                    "default",
                    "envvar",
                    "occurrences_authority",
                    "choices",
                    "minimum",
                    "maximum",
                    "clamp",
                    # Invocation/extent rendering accounts for these parser rules.
                    "kind",
                    "is_flag",
                    "nargs",
                    "count",
                    "multiple",
                },
            )
            if other:
                default += f"<br>{other}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(parameter_name)}`"
                f"{'<br>' + ', '.join(spellings) if spellings else ''} | "
                f"{invocation} | {_cli_parameter_type(item)} | {default} |"
            )
        for item in parameters:
            authority_pointer = item.get("occurrences_authority")
            if authority_pointer is None:
                continue
            if not isinstance(authority_pointer, str) or "/schema" not in authority_pointer:
                raise ContractAtlasError("CLI occurrence authority is not a schema pointer")
            target = _one_cli_authority_element(
                (
                    candidate
                    for candidate in elements_by_id.values()
                    if candidate["interface"] == "http-operations"
                    and any(
                        authority_pointer.startswith(f"{owned}/parameters/")
                        for owned in cast(Sequence[str], candidate["pointers"])
                    )
                ),
                kind="occurrence",
            )
            parameter_pointer = authority_pointer.rsplit("/schema", 1)[0]
            source = cast(Mapping[str, object], pointer_value(projection, parameter_pointer))
            schema = cast(Mapping[str, object], pointer_value(projection, authority_pointer))
            link = _anchor_link(path, str(target["dossier"]), _subject_anchor(parameter_pointer))
            lines.extend(
                [
                    "",
                    f"Repeated `{_md(item['name'])}` accepts at most **{schema['maxItems']}** "
                    f"occurrences, through "
                    f"[{_md(target['title'])} · {_md(source['name'])}]({link}).",
                ]
            )
    if exclusive_groups:
        lines.extend(["", "### Argument combinations", ""])
        for index, group in enumerate(exclusive_groups):
            members = []
            for parameter_index in cast(Sequence[int], group["parameters"]):
                if not 0 <= parameter_index < len(parameters):
                    raise ContractAtlasError(
                        "CLI mutually exclusive group has an unresolved member"
                    )
                parameter = parameters[parameter_index]
                options = cast(Sequence[str], parameter.get("options", ()))
                label = ", ".join(options) or str(parameter.get("dest", parameter.get("name")))
                anchor = _subject_anchor(f"{parameters_pointer}/{parameter_index}")
                members.append(f"[`{_md(label)}`](#{anchor})")
            rule = "Exactly one" if group["required"] else "At most one"
            lines.append(
                f"- {_subject_marker(f'{exclusive_groups_pointer}/{index}', placed_subjects)}"
                f"{rule} of: {', '.join(members)}."
            )
            lines.extend(_remaining_lines(group, {"required", "parameters"}))
    if terminating_controls:
        lines.extend(
            [
                "",
                "### Terminating controls",
                "",
                "| Identity | Trigger | Exit status | stdout | stderr |",
                "|---|---|---:|---|---|",
            ]
        )
        for index, control in enumerate(terminating_controls):
            pointer = f"{terminating_controls_pointer}/{index}"
            lines.append(
                f"| {_subject_marker(f'{pointer}/id', placed_subjects)}"
                f"`{_md(control['id'])}` | "
                f"{_subject_marker(f'{pointer}/trigger', placed_subjects)}"
                f"`{_md(_compact_json(control['trigger']))}` | "
                f"{_subject_marker(f'{pointer}/exit_status', placed_subjects)}"
                f"`{_md(control['exit_status'])}` | "
                f"{_subject_marker(f'{pointer}/stdout', placed_subjects)}"
                f"`{_md(_compact_json(control['stdout']))}` | "
                f"{_subject_marker(f'{pointer}/stderr', placed_subjects)}"
                f"`{_md(_compact_json(control['stderr']))}` |"
            )
        for control in terminating_controls:
            other = _remaining_fields(control, {"id", "trigger", "exit_status", "stdout", "stderr"})
            if other:
                lines.extend(["", f"Control `{_md(control['id'])}`: {other}"])
    if result_contract is not None:
        lines.extend(
            [
                "",
                "### Result and failure contract",
                "",
                f"- {_subject_marker(f'{result_pointer}/identity', placed_subjects)}"
                f"Result identity: `{_md(result_contract['identity'])}`",
                f"- {_subject_marker(f'{result_pointer}/profile_id', placed_subjects)}"
                f"Profile: `{_md(result_contract['profile_id'])}`",
                f"- {_subject_marker(f'{result_pointer}/structured_output', placed_subjects)}"
                f"Structured output: `{_md(result_contract['structured_output'])}`",
                f"- {_subject_marker(f'{result_pointer}/human_json_relationship', placed_subjects)}"
                "Human/JSON relationship: "
                f"`{_md(result_contract['human_json_relationship'])}`",
            ]
        )
        lines.extend(
            _remaining_lines(
                result_contract,
                {
                    "identity",
                    "profile_id",
                    "structured_output",
                    "human_json_relationship",
                    "success",
                    "failures",
                },
            )
        )
        for key, title in (("success", "Success outcomes"), ("failures", "Failure outcomes")):
            outcomes = cast(Sequence[Mapping[str, object]], result_contract[key])
            lines.extend(
                [
                    "",
                    f"#### {title}",
                    "",
                    "| Identity | Selected by | Exit status | stdout | stderr |",
                    "|---|---|---|---|---|",
                ]
            )
            for index, outcome in enumerate(outcomes):
                outcome_pointer = f"{result_pointer}/{key}/{index}"
                status = outcome["exit_status"]
                rendered_status = (
                    json.dumps(status, sort_keys=True, separators=(",", ":"))
                    if isinstance(status, Mapping)
                    else str(status)
                )
                stdout_pointer = f"{outcome_pointer}/stdout"
                stderr_pointer = f"{outcome_pointer}/stderr"
                stdout = _cli_channel_summary(
                    cast(Mapping[str, object], outcome["stdout"]),
                    element=element,
                    pointer=stdout_pointer,
                    path=path,
                    projection=projection,
                    elements_by_id=elements_by_id,
                )
                stderr = _cli_channel_summary(
                    cast(Mapping[str, object], outcome["stderr"]),
                    element=element,
                    pointer=stderr_pointer,
                    path=path,
                    projection=projection,
                    elements_by_id=elements_by_id,
                )
                lines.append(
                    f"| {_subject_marker(f'{outcome_pointer}/id', placed_subjects)}"
                    f"`{_md(outcome['id'])}` | "
                    f"{_subject_marker(f'{outcome_pointer}/selected_by', placed_subjects)}"
                    f"`{_md(_compact_json(outcome['selected_by']))}` | "
                    f"{_subject_marker(f'{outcome_pointer}/exit_status', placed_subjects)}"
                    f"`{_md(rendered_status)}` | "
                    f"{_subject_marker(stdout_pointer, placed_subjects)}{stdout} | "
                    f"{_subject_marker(stderr_pointer, placed_subjects)}{stderr} |"
                )
                for channel in ("stdout", "stderr"):
                    for mode, semantics in cast(Mapping[str, object], outcome[channel]).items():
                        if isinstance(semantics, Mapping) and str(
                            semantics.get("kind", "")
                        ).startswith("cli-local-"):
                            local_outputs.append(
                                (
                                    f"{outcome_pointer}/{channel}/{_escape_pointer(str(mode))}",
                                    f"{outcome['id']} · {channel} ({mode})",
                                    semantics,
                                )
                            )
            for outcome in outcomes:
                other = _remaining_fields(
                    outcome, {"id", "selected_by", "exit_status", "stdout", "stderr"}
                )
                if other:
                    lines.extend(["", f"Outcome `{_md(outcome['id'])}`: {other}"])
    if local_outputs:
        lines.extend(["", "### Local structured outputs", ""])
        for pointer, label, semantics in local_outputs:
            lines.extend(
                [
                    "",
                    f"#### {_subject_marker(pointer, placed_subjects)}"
                    f"`{_md(semantics['identity'])}`",
                    "",
                    f"Applies to: {_md(label)}.",
                    "",
                ]
            )
            kind = semantics["kind"]
            if kind == "cli-local-json-schema":
                lines.extend(
                    _render_schema(
                        semantics["schema"], f"{pointer}/schema", placed_subjects, heading_level=5
                    )
                )
            elif kind == "cli-local-exact-json":
                lines.extend(
                    _render_generic(
                        [f"{pointer}/document"], [semantics["document"]], placed_subjects
                    )
                )
            elif kind == "cli-local-json-sequence":
                metadata = {key: item for key, item in semantics.items() if key != "records"}
                lines.extend(_render_generic([pointer], [metadata], placed_subjects))
                for name, record_schema in cast(Mapping[str, object], semantics["records"]).items():
                    lines.extend(["", f"##### Record `{_md(name)}`", ""])
                    lines.extend(
                        _render_schema(
                            record_schema,
                            f"{pointer}/records/{_escape_pointer(str(name))}",
                            placed_subjects,
                            heading_level=6,
                        )
                    )
    return lines


def _render_operation(
    value: Mapping[str, object], base_pointer: str, placed_subjects: set[str]
) -> list[str]:
    return [
        _subject_marker(base_pointer, placed_subjects),
        "| Concern | Contract |",
        "|---|---|",
        *(
            f"| {_subject_marker(f'{base_pointer}/{_escape_pointer(str(key))}', placed_subjects)}"
            f"`{_md(key)}` | `{_md(_compact_json(item))}` |"
            for key, item in value.items()
        ),
    ]


def _render_generic(
    pointers: Sequence[str], values: Sequence[object], placed_subjects: set[str]
) -> list[str]:
    """Render ordinary contract records without interpreting their keys as schema keywords."""

    lines = ["", "| Field | Value |", "|---|---|"]

    def append(value: object, pointer: str, label: str) -> None:
        if isinstance(value, Mapping) and value:
            for key, child in value.items():
                append(
                    child,
                    f"{pointer}/{_escape_pointer(str(key))}",
                    f"{label} · {key}" if label else str(key),
                )
        elif isinstance(value, list) and any(isinstance(item, (Mapping, list)) for item in value):
            for index, child in enumerate(value):
                append(child, f"{pointer}/{index}", f"{label} · item {index + 1}")
        else:
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(label)}` | "
                f"`{_md(_compact_json(value))}` |"
            )

    for pointer, value in zip(pointers, values, strict=True):
        if isinstance(value, Mapping):
            lines.insert(0, _subject_marker(pointer, placed_subjects))
        label = (
            ""
            if len(values) == 1 and isinstance(value, Mapping) and value
            else (_pointer_parts(pointer)[-1])
        )
        append(value, pointer, label)
    return lines


def _pretty_json(value: object) -> str:
    """Return exact, readable JSON without accidentally creating Markdown links."""

    return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True).replace("](", "]\\u0028")


def _exact_contract_lines(
    pointers: Sequence[str], values: Sequence[object], *, encoded_integers: bool
) -> list[str]:
    lines = [
        "### Exact owned JSON",
        "",
        "<details>",
        "<summary>Expand exact machine-owned values</summary>",
        "",
        "The following JSON is the complete value owned at each machine-authority pointer. "
        "No contractual fields are summarized away.",
        "",
    ]
    if encoded_integers:
        lines.extend(
            [
                "Large integers appear as decimal strings in this machine representation. "
                "The machine artifact's `projection_unsafe_integer_paths` identifies them; "
                "primary content displays the recovered numeric values.",
                "",
            ]
        )
    for pointer, value in zip(pointers, values, strict=True):
        if len(pointers) > 1:
            lines.extend([f"### `{pointer}`", ""])
        lines.extend(
            [
                f"<!-- exact-contract-value: {canonical_sha256(value)} -->",
                "",
                "```json",
                _pretty_json(value),
                "```",
                "",
            ]
        )
    lines.extend(["</details>", ""])
    return lines


def _local_contract_references(
    authority: str,
    values: Sequence[object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> list[Mapping[str, object]]:
    """Resolve local OpenAPI references to their exact atlas owners."""

    prefix = f"/external_contract/http_openapi/{authority}"
    owners: dict[str, Mapping[str, object]] = {}
    for element in elements_by_id.values():
        if element["authority"] != authority or element["interface"] != "http-schemas":
            continue
        for pointer in cast(Sequence[str], element["pointers"]):
            if pointer.startswith(f"{prefix}/components/"):
                owners[f"#{pointer.removeprefix(prefix)}"] = element

    references: set[str] = set()

    def visit(value: object) -> None:
        if isinstance(value, Mapping):
            reference = value.get("$ref")
            if isinstance(reference, str):
                references.add(reference)
            for child in value.values():
                visit(child)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for child in value:
                visit(child)

    for value in values:
        visit(value)
    return sorted(
        {
            str(owners[reference]["id"]): owners[reference]
            for reference in references
            if reference in owners
        }.values(),
        key=lambda item: str(item["title"]),
    )


def _subject_label(
    element: Mapping[str, object], pointer: str, projection: Mapping[str, object]
) -> str:
    """Describe one exact pointer without inventing a second semantic identity."""

    owned = [
        candidate
        for candidate in cast(Sequence[str], element["pointers"])
        if pointer == candidate or pointer.startswith(f"{candidate}/")
    ]
    base = max(owned, key=len) if owned else ""
    if pointer == base:
        return str(element["title"])

    relative = pointer[len(base) :].removeprefix("/") if base else pointer.removeprefix("/")
    parts = _pointer_parts(f"/{relative}") if relative else []
    value = pointer_value(projection, pointer)
    if parts and parts[0].isdigit() and base.endswith("/parameters"):
        if isinstance(value, Mapping):
            options = cast(Sequence[str], value.get("options", ()))
            name = str(options[0]) if options else str(value.get("name", parts[0]))
            return f"CLI parameter {name}"
        return f"CLI parameter {parts[0]}"

    labels: list[str] = []
    index = 0
    while index < len(parts):
        part = parts[index]
        following = parts[index + 1] if index + 1 < len(parts) else None
        if part == "$defs" and following is not None:
            labels.append(f"definition {following}")
            index += 2
        elif part == "schemas" and following is not None:
            labels.append(f"schema {following}")
            index += 2
        elif part == "properties" and following is not None:
            labels.append(f"field {following}")
            index += 2
        elif part == "parameters" and following is not None:
            parameter_pointer = f"{base}/parameters/{_escape_pointer(following)}"
            parameter = pointer_value(projection, parameter_pointer)
            parameter_name = (
                str(parameter.get("name", following))
                if isinstance(parameter, Mapping)
                else following
            )
            labels.append(f"parameter {parameter_name}")
            index += 2
        elif part == "responses" and following is not None:
            labels.append(f"response {following}")
            index += 2
        elif part == "requestBody":
            labels.append("request body")
            index += 1
        elif part == "items":
            labels.append("items")
            index += 1
        elif part == "additionalProperties":
            labels.append("additional values")
            index += 1
        elif part in {"allOf", "anyOf", "oneOf"} and following is not None and following.isdigit():
            alternative_pointer = f"{base}/" + "/".join(
                _escape_pointer(value) for value in parts[: index + 2]
            )
            alternative = pointer_value(projection, alternative_pointer)
            alternative_kind = ""
            if isinstance(alternative, Mapping):
                if isinstance(alternative.get("type"), str):
                    alternative_kind = str(alternative["type"])
                elif isinstance(alternative.get("$ref"), str):
                    alternative_kind = str(alternative["$ref"]).rsplit("/", 1)[-1]
            labels.append(
                f"{alternative_kind} value"
                if alternative_kind
                else f"{part} alternative {int(following) + 1}"
            )
            index += 2
        elif part == "schema":
            index += 1
        else:
            labels.append(part)
            index += 1
    return " · ".join(labels) or str(element["title"])


def _subject_reference(
    *,
    element: Mapping[str, object],
    pointer: str,
    projection: Mapping[str, object],
    placed_subjects: set[str],
) -> str:
    label = _md(_subject_label(element, pointer, projection))
    anchor = _subject_anchor(pointer)
    if pointer in placed_subjects:
        return f"[{label}](#{anchor})"
    ancestors = [candidate for candidate in placed_subjects if pointer.startswith(f"{candidate}/")]
    if ancestors:
        parent = max(ancestors, key=len)
        placed_subjects.add(pointer)
        return f"{_html_anchor(anchor)}[{label}](#{_subject_anchor(parent)})"
    placed_subjects.add(pointer)
    return f'<a id="{anchor}"></a>{label}'


def _implementation_sources(
    element: Mapping[str, object],
    trace: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
) -> list[tuple[str, Mapping[str, object]]]:
    details = cast(Mapping[str, object], element.get("details", {}))
    if element["interface"] == "http-operations" and not details.get("supplemental"):
        source = _source_index(trace)[f"openapi:{element['authority']}"]
        routes = [
            cast(Mapping[str, object], route["source"])
            for route in cast(Sequence[Mapping[str, object]], source["routes"])
            if route["operation_id"] == details["operation_id"]
            and details["method"] in cast(Sequence[str], route["methods"])
        ]
        if len(routes) != 1:
            raise ContractAtlasError(f"HTTP handler source is ambiguous: {element['id']}")
        return [("Handler", routes[0])]
    if element["interface"] not in {"python", "cli"}:
        return []
    keys = {
        tuple(cast(Sequence[str], related_details["qualification_key"]))
        for identity in cast(Sequence[str], element["related_element_ids"])
        for related in (elements_by_id[identity],)
        for related_details in (cast(Mapping[str, object], related.get("details", {})),)
        if related["interface"] == "http-operations" and "qualification_key" in related_details
    }
    located: dict[str, tuple[str, Mapping[str, object]]] = {}
    qualification = cast(Mapping[str, object], trace["operation_qualification"])
    for record in cast(Sequence[Mapping[str, object]], qualification["records"]):
        if (record["application"], record["operation_id"]) not in keys:
            continue
        binding_key = "client_bindings" if element["interface"] == "python" else "cli_bindings"
        for binding in cast(Sequence[Mapping[str, object]], record[binding_key]):
            if element["interface"] == "python":
                matches = binding["public_identity"] == details["public_identity"]
                role = "Client method"
            else:
                command = " ".join(cast(Sequence[str], details["command_path"]))
                matches = command == binding["command"] or command.endswith(
                    f" {binding['command']}"
                )
                role = "Command callback"
            if matches:
                location = cast(Mapping[str, object], binding["source"])
                located[_compact_json(location)] = (role, location)
    return [located[key] for key in sorted(located)]


def _render_dossier(
    element: Mapping[str, object],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
    *,
    primary_projection: Mapping[str, object],
) -> bytes:
    path = str(element["dossier"])
    authority_path = (
        f"{ATLAS_DIRECTORY}/authorities/{_slug(str(element['authority']), limit=72)}/index.md"
    )
    interface_path = (
        f"{ATLAS_DIRECTORY}/authorities/{_slug(str(element['authority']), limit=72)}/"
        f"{_slug(str(element['interface']), limit=48)}/index.md"
    )
    policy_path = f"{ATLAS_DIRECTORY}/policies/index.md"
    source_evidence_path = f"{ATLAS_DIRECTORY}/evidence/sources.md"
    pointers = cast(Sequence[str], element["pointers"])
    exact_values = [pointer_value(projection, pointer) for pointer in pointers]
    projection = primary_projection
    values = [pointer_value(projection, pointer) for pointer in pointers]
    placed_subjects: set[str] = set()
    source_index = _source_index(trace)
    implementation_sources = _implementation_sources(element, trace, elements_by_id)
    details = cast(Mapping[str, object], element.get("details", {}))
    purpose = "Exact externally visible contract owned by this semantic dossier."
    if len(values) == 1 and isinstance(values[0], Mapping):
        value = cast(Mapping[str, object], values[0])
        purpose = str(value.get("summary", value.get("description", purpose))).strip() or purpose
    interface_label = _interface_label(str(element["interface"]))
    lines = [
        f"# {element['title']}",
        "",
        f"[Atlas]({_relative_link(path, f'{ATLAS_DIRECTORY}/index.md')}) · "
        f"[Authority]({_relative_link(path, authority_path)}) · "
        f"[Interface]({_relative_link(path, interface_path)}) · "
        f"[Policies]({_relative_link(path, policy_path)})",
        "",
        f"<!-- contract-element: {element['id']} -->",
        "",
        purpose,
        "",
        "| Audit field | Value |",
        "|---|---|",
        f"| Authority | [{_md(element['authority'])}]({_relative_link(path, authority_path)}) |",
        f"| Interface | [{_md(interface_label)}]({_relative_link(path, interface_path)}) |",
        "",
        "## External contract",
        "",
    ]
    interface = str(element["interface"])
    renderer = INTERFACE_REGISTRY[interface].renderer
    if (
        renderer == "http-operation"
        and len(values) == 1
        and isinstance(values[0], Mapping)
        and "method" in details
        and not details.get("supplemental")
    ):
        lines.extend(
            _render_http(
                cast(Mapping[str, object], values[0]),
                details,
                pointers[0],
                placed_subjects,
                element=element,
                elements_by_id=elements_by_id,
            )
        )
    elif renderer == "cli":
        lines.extend(
            _render_cli(
                pointers,
                values,
                placed_subjects,
                element=element,
                path=path,
                projection=projection,
                elements_by_id=elements_by_id,
            )
        )
    elif renderer == "python" and len(values) == 1 and isinstance(values[0], Mapping):
        lines.extend(
            _render_python(cast(Mapping[str, object], values[0]), pointers[0], placed_subjects)
        )
    elif renderer == "durable-state":
        lines.extend(_render_durable_state(pointers, values, details, placed_subjects))
    elif renderer == "schema" and len(values) == 1:
        lines.extend(
            _render_schema(
                values[0],
                pointers[0],
                placed_subjects,
                element=element,
                elements_by_id=elements_by_id,
            )
        )
    elif (
        renderer in {"http-operation", "operation"}
        and len(values) == 1
        and isinstance(values[0], Mapping)
    ):
        lines.extend(
            _render_operation(cast(Mapping[str, object], values[0]), pointers[0], placed_subjects)
        )
    elif renderer == "release" and len(values) == 1 and isinstance(values[0], Mapping):
        lines.extend(
            _render_operation(cast(Mapping[str, object], values[0]), pointers[0], placed_subjects)
        )
    else:
        lines.extend(_render_generic(pointers, values, placed_subjects))
    lines.append("")

    semantic_owners = cast(Sequence[str], details.get("semantic_owners", ()))
    if semantic_owners:
        relationship_evidence = f"{ATLAS_DIRECTORY}/evidence/relationships.md"
        lines.extend(
            [
                "## Existing ownership context",
                "",
                "Publication preserves these existing component authorities; it does not "
                "reclassify or duplicate their interfaces.",
                "",
                *(
                    f"- [{_md(owner)}]"
                    f"({_anchor_link(path, relationship_evidence, component_anchor)})"
                    for owner in semantic_owners
                    for component_anchor in (_relationship_node_anchor(f"component:{owner}"),)
                ),
                "",
            ]
        )

    extent_ids = cast(Sequence[str], element["extent_decision_ids"])
    if extent_ids:
        decisions = {
            str(item["id"]): item
            for item in cast(
                Sequence[Mapping[str, object]],
                cast(
                    Mapping[str, object],
                    cast(Mapping[str, object], projection["external_contract"])["extents"],
                )["decisions"],
            )
        }
        lines.extend(["### Progression, limits, and lifecycle", ""])
        decision_groups: dict[str, list[tuple[str, Mapping[str, object]]]] = defaultdict(list)
        for identity in extent_ids:
            decision = decisions[identity]
            decision_groups[str(decision["rule"])].append((identity, decision))
        for rule, group in sorted(decision_groups.items()):
            rule_id = f"extent-rule/{rule}"
            structural_keys = {
                "id",
                "owner",
                "source_pointer",
                "dimension",
                "unit",
                "policy",
                "rule",
            }
            detail_maps = [
                {key: value for key, value in decision.items() if key not in structural_keys}
                for _identity, decision in group
            ]
            common_keys = set(detail_maps[0])
            for details_map in detail_maps[1:]:
                common_keys &= set(details_map)
            common_details = {
                key: detail_maps[0][key]
                for key in sorted(common_keys)
                if all(details_map[key] == detail_maps[0][key] for details_map in detail_maps[1:])
            }
            shared = "; ".join(
                f"{key}={_compact_json(value)}" for key, value in common_details.items()
            )
            lines.extend(
                [
                    f"#### [{_md(rule_id)}]"
                    f"({_anchor_link(path, policy_path, _policy_anchor(rule_id))})",
                    "",
                    *(
                        [f"Shared facts for every subject below: {_md(shared)}", ""]
                        if shared
                        else []
                    ),
                    "| Applies to | Contract | Bounds or reason |",
                    "|---|---|---|",
                ]
            )
            for _identity, decision in group:
                bounds = "; ".join(
                    f"{key}={_compact_json(value)}"
                    for key, value in decision.items()
                    if key not in structural_keys and key not in common_details
                )
                source_pointer = str(decision["source_pointer"])
                subject = _subject_reference(
                    element=element,
                    pointer=source_pointer,
                    projection=projection,
                    placed_subjects=placed_subjects,
                )
                contract = f"{decision['dimension']} · {decision['unit']} · {decision['policy']}"
                rendered_bounds = _md(bounds) if bounds else "shared above"
                lines.append(f"| {subject} | `{_md(contract)}` | {rendered_bounds} |")
            lines.append("")

        witness_ids = sorted(
            {
                str(witness_id)
                for link in cast(Sequence[Mapping[str, object]], trace["extent_sources"])
                if link["id"] in extent_ids
                for witness_id in cast(Sequence[str], link.get("segmented_extent_witnesses", ()))
            }
        )
        if witness_ids:
            lines.extend(
                [
                    "### Progression evidence and open obligations",
                    "",
                    "These are candidate test bindings. Group-wide progression claims remain "
                    "unestablished; inspect the test scopes before applying a result "
                    "to this contract.",
                    "",
                    *(
                        f"- [{_md(witness_id)}]"
                        f"({_anchor_link(path, source_evidence_path, witness_anchor)})"
                        for witness_id in witness_ids
                        for witness_anchor in (_anchor_id("extent-witness", witness_id),)
                    ),
                    "",
                ]
            )

    related_ids = cast(Sequence[str], element["related_element_ids"])
    referenced = _local_contract_references(str(element["authority"]), values, elements_by_id)
    if related_ids or referenced:
        lines.extend(["## Maintained corroboration", ""])
    if related_ids:
        lines.extend(["### Related interface records", ""])
        related_elements = [elements_by_id[identity] for identity in related_ids]
        related_labels = _dossier_navigation_labels(element, related_elements)
        for related_id in related_ids:
            related = elements_by_id[related_id]
            lines.append(
                f"- [{_md(related_labels[related_id])}]"
                f"({_relative_link(path, str(related['dossier']))})"
            )
        lines.append("")
    if referenced:
        lines.extend(["### Referenced contract dossiers", ""])
        referenced_labels = _dossier_navigation_labels(element, referenced)
        for owner in referenced:
            lines.append(
                f"- [{_md(referenced_labels[str(owner['id'])])}]"
                f"({_relative_link(path, str(owner['dossier']))})"
            )
        lines.append("")

    lines.extend(
        [
            "## Governing policies",
            "",
            *(
                f"- {_html_anchor(_policy_application_anchor(str(element['id']), policy))}"
                f"[{_md(policy)}]({_anchor_link(path, policy_path, _policy_anchor(policy))})"
                for policy in cast(Sequence[str], element["policy_ids"])
            ),
            "",
            "## Evidence",
            "",
            "### Qualification",
            "",
            *(
                f"- [{_md(route)}]"
                f"({_anchor_link(path, source_evidence_path, _qualification_anchor(route))})"
                for route in cast(Sequence[str], element["qualification_routes"])
            ),
            "",
            "### Executable sources",
            "",
        ]
    )
    for source_id in cast(Sequence[str], element["source_authority_ids"]):
        if source_id.startswith("openapi:") and implementation_sources:
            lines.append(
                f"- **OpenAPI authority:** [{_md(source_id)}]"
                f"({_anchor_link(path, source_evidence_path, _source_anchor(source_id))})"
            )
            continue
        source = source_index[source_id]
        location = cast(Mapping[str, object], source.get("source", {}))
        bindings = cast(Sequence[Mapping[str, object]], source.get("bindings", ()))
        declarations = cast(Sequence[Mapping[str, object]], source.get("declarations", ()))
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
        symbol = f"::{location['symbol']}" if "symbol" in location else ""
        lines.append(
            f"- [{_md(source_id)}]"
            f"({_anchor_link(path, source_evidence_path, _source_anchor(source_id))}) — "
            f"`{rendered}{symbol}`"
        )
    for role, location in implementation_sources:
        label = f"{location['path']}::{location['symbol']}"
        lines.append(f"- **{role}:** {_repository_source_link(path, location, label)}")
    qualification_key = details.get("qualification_key")
    if isinstance(qualification_key, Sequence) and not isinstance(qualification_key, str):
        records = cast(
            Sequence[Mapping[str, object]],
            cast(Mapping[str, object], trace["operation_qualification"])["records"],
        )
        matching_records = [
            record
            for record in records
            if [record["application"], record["operation_id"]] == list(qualification_key)
        ]
        if len(matching_records) != 1:
            raise ContractAtlasError(
                f"HTTP contract has ambiguous operation qualification evidence: {qualification_key}"
            )
        lines.extend(
            [
                "",
                "### Structural operation bindings",
                "",
                "This generated record links maintained client, CLI, response-authority, and "
                "provider routes. It checks interface structure, not executed qualification, "
                "successful CLI execution, or human/JSON equivalence. Test bindings and "
                "qualification commands are audit leads, not run results.",
                "",
                "<details>",
                "<summary>Exact structural binding record</summary>",
                "",
                "```json",
                _pretty_json(matching_records[0]),
                "```",
                "",
                "</details>",
            ]
        )
    if element["interface"] == "configuration-environment":
        configuration_sources = [
            source_index[source_id]
            for source_id in cast(Sequence[str], element["source_authority_ids"])
            if source_index[source_id].get("bindings")
        ]
        lines.extend(
            [
                "",
                "### Configuration authority and bindings",
                "",
                "The owning implementation defines the setting. The parser expression records "
                "each independently discovered consumer binding and effective default exercised "
                "by qualification.",
                "",
                "| Kind | Consumer | Source | Authority |",
                "|---|---|---|---|",
            ]
        )
        for source in configuration_sources:
            for declaration in cast(Sequence[Mapping[str, object]], source.get("declarations", ())):
                lines.append(
                    f"| declaration | — | `{_md(declaration['path'])}` | "
                    f"`{_md(declaration['pointer'])}` |"
                )
            for binding in cast(Sequence[Mapping[str, object]], source["bindings"]):
                lines.append(
                    f"| parser | `{_md(binding['consumer'])}` | `{_md(binding['path'])}` | "
                    f"`{_md(binding['expression'])}` |"
                )
    lines.extend(
        [
            "",
            "### Machine authority",
            "",
            *(f"- `{pointer}`" for pointer in pointers),
            "",
            *_exact_contract_lines(pointers, exact_values, encoded_integers=exact_values != values),
        ]
    )
    return ("\n".join(lines).rstrip() + "\n").encode()

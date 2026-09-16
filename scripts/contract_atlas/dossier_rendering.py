"""Exact Markdown rendering for individual Riverhog contract dossiers."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import cast

from .discovery import _source_index
from .model import (
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

    if kind == "http-operation-response":
        required = {"application", "operation_id", "method", "path", "status", "schema"}
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
    return f"[{_md(label)}]({href})"


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
            parts.append(f"{mode}: `{_md(semantics)}`")
            continue
        parts.append(
            f"{mode}: "
            + _cli_authority_reference(
                semantics,
                element=element,
                pointer=pointer,
                path=path,
                projection=projection,
                elements_by_id=elements_by_id,
            )
        )
    return "; ".join(parts)


def _shape_summary(value: object) -> str:
    if isinstance(value, Mapping):
        if set(value) == {"$ref"}:
            return str(value["$ref"])
        parts: list[str] = []
        for key in (
            "$ref",
            "type",
            "format",
            "const",
            "enum",
            "minimum",
            "maximum",
            "minLength",
            "maxLength",
            "minItems",
            "maxItems",
            "pattern",
        ):
            if key in value:
                rendered_value = json.dumps(
                    value[key], ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                parts.append(f"{key}={rendered_value}")
        properties = value.get("properties")
        if isinstance(properties, Mapping):
            parts.append("fields=" + ", ".join(f"`{name}`" for name in properties))
        if "items" in value:
            parts.append(f"items=({_shape_summary(value['items'])})")
        alternatives = [key for key in ("oneOf", "anyOf", "allOf") if key in value]
        for key in alternatives:
            variants = cast(Sequence[object], value[key])
            parts.append(f"{key}=" + " | ".join(_shape_summary(item) for item in variants))
        unrendered = sorted(
            set(value)
            - {
                "$ref",
                "type",
                "format",
                "const",
                "enum",
                "minimum",
                "maximum",
                "minLength",
                "maxLength",
                "minItems",
                "maxItems",
                "pattern",
                "properties",
                "items",
                "oneOf",
                "anyOf",
                "allOf",
                "description",
                "title",
                "default",
            }
        )
        if unrendered:
            parts.append("additional keys=" + ", ".join(f"`{key}`" for key in unrendered))
        return "; ".join(parts) or "empty object"
    if isinstance(value, list):
        if all(not isinstance(item, (Mapping, list)) for item in value):
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return "items=" + " | ".join(_shape_summary(item) for item in value)
    return json.dumps(value, ensure_ascii=False)


def _render_schema(
    value: Mapping[str, object], base_pointer: str, placed_subjects: set[str]
) -> list[str]:
    lines: list[str] = []
    for key in (
        "$id",
        "title",
        "description",
        "type",
        "format",
        "protocol",
        "protocols",
        "bundle_sha256",
    ):
        if key in value:
            pointer = f"{base_pointer}/{_escape_pointer(key)}"
            rendered = (
                json.dumps(value[key], ensure_ascii=False)
                if isinstance(value[key], (list, Mapping))
                else str(value[key])
            )
            lines.append(f"- {_subject_marker(pointer, placed_subjects)}`{key}`: {_md(rendered)}")
    required = set(cast(Sequence[str], value.get("required", ())))
    properties = value.get("properties")
    if isinstance(properties, Mapping):
        lines.extend(
            [
                "",
                "### Fields",
                "",
                "| Field | Required | Shape | Description |",
                "|---|---:|---|---|",
            ]
        )
        for name, field in properties.items():
            field_map = cast(Mapping[str, object], field) if isinstance(field, Mapping) else {}
            pointer = f"{base_pointer}/properties/{_escape_pointer(str(name))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(name)}` | "
                f"{'yes' if name in required else 'no'} | "
                f"{_md(_shape_summary(field))} | {_md(field_map.get('description', ''))} |"
            )
    schemas = value.get("schemas")
    if isinstance(schemas, Mapping):
        lines.extend(["", "### Schemas", "", "| Schema | Shape |", "|---|---|"])
        for name, schema in schemas.items():
            pointer = f"{base_pointer}/schemas/{_escape_pointer(str(name))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(name)}` | "
                f"{_md(_shape_summary(schema))} |"
            )
    definitions = value.get("$defs")
    if isinstance(definitions, Mapping):
        lines.extend(["", "### Definitions", "", "| Definition | Shape |", "|---|---|"])
        for name, schema in definitions.items():
            pointer = f"{base_pointer}/$defs/{_escape_pointer(str(name))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(name)}` | "
                f"{_md(_shape_summary(schema))} |"
            )
    if lines:
        lines.insert(0, _subject_marker(base_pointer, placed_subjects))
    return lines


def _render_python(
    value: Mapping[str, object], base_pointer: str, placed_subjects: set[str]
) -> list[str]:
    """Render one exact declared Python unit without hiding structural promises."""

    lines = [_subject_marker(base_pointer, placed_subjects)]
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
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(item['name'])}` | "
                f"`{_md(item['type'])}` | `{_md(item['default'])}` |"
            )
    schema = contract.get("schema")
    if isinstance(schema, Mapping):
        lines.extend(["", "#### Validated model schema", ""])
        lines.extend(
            _render_schema(
                cast(Mapping[str, object], schema),
                f"{contract_pointer}/schema",
                placed_subjects,
            )
        )
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
    lines = [_subject_marker(pointer, placed_subjects)]
    if unit == "relational-table":
        lines.extend(
            [
                f"- Table: `{_md(value['name'])}`",
                "",
                "### Columns",
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
                    "### Table constraints",
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
        return lines
    if unit == "unique-index":
        return [
            *lines,
            "| Index fact | Value |",
            "|---|---|",
            *(f"| `{_md(key)}` | `{_md(_compact_json(item))}` |" for key, item in value.items()),
        ]
    if unit == "json-document":
        lines.extend([f"- Document: `{_md(value['id'])}`", "", "### Document schema", ""])
        schema = value.get("schema")
        if not isinstance(schema, Mapping):
            raise ContractAtlasError("durable JSON document has no exact schema")
        lines.extend(
            _render_schema(cast(Mapping[str, object], schema), f"{pointer}/schema", placed_subjects)
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
) -> list[str]:
    lines = [_subject_marker(base_pointer, placed_subjects)]
    for key in ("operationId", "summary", "description", "deprecated"):
        if key in value:
            pointer = f"{base_pointer}/{_escape_pointer(key)}"
            lines.append(f"- {_subject_marker(pointer, placed_subjects)}`{key}`: {_md(value[key])}")
    if "security" in value:
        pointer = f"{base_pointer}/security"
        lines.append(
            f"- {_subject_marker(pointer, placed_subjects)}`security`: "
            f"`{_md(json.dumps(value['security'], sort_keys=True))}`"
        )
    parameters = cast(Sequence[Mapping[str, object]], value.get("parameters", ()))
    if parameters:
        lines.extend(
            ["", "### Parameters", "", "| Name | In | Required | Schema |", "|---|---|---:|---|"]
        )
        for index, item in enumerate(parameters):
            pointer = f"{base_pointer}/parameters/{index}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(item.get('name', ''))}` | "
                f"{_md(item.get('in', ''))} | "
                f"{'yes' if item.get('required') else 'no'} | "
                f"{_md(_shape_summary(item.get('schema')))} |"
            )
    if "requestBody" in value:
        pointer = f"{base_pointer}/requestBody"
        lines.extend(
            [
                "",
                f"### {_subject_marker(pointer, placed_subjects)}Request body",
                "",
                f"`{_md(json.dumps(value['requestBody'], sort_keys=True))}`",
            ]
        )
    responses = value.get("responses")
    if isinstance(responses, Mapping):
        lines.extend(["", "### Responses", "", "| Status | Description |", "|---|---|"])
        for status, response in responses.items():
            pointer = f"{base_pointer}/responses/{_escape_pointer(str(status))}"
            description = (
                cast(Mapping[str, object], response).get("description", "")
                if isinstance(response, Mapping)
                else ""
            )
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(status)}` | "
                f"{_md(description)} |"
            )
    del details
    return lines


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
    for pointer, value in zip(pointers, values, strict=True):
        if isinstance(value, str):
            name = value
            name_pointer = pointer
        elif isinstance(value, list) and pointer.endswith("/parameters"):
            parameters = cast(Sequence[Mapping[str, object]], value)
            parameters_pointer = pointer
        elif isinstance(value, list) and pointer.endswith("/terminating_controls"):
            terminating_controls = cast(Sequence[Mapping[str, object]], value)
            terminating_controls_pointer = pointer
        elif isinstance(value, Mapping):
            result_contract = value
            result_pointer = pointer
    lines = (
        [f"- {_subject_marker(name_pointer, placed_subjects)}Parser name: `{_md(name)}`"]
        if name
        else []
    )
    if parameters:
        lines.extend(
            [
                "",
                "### Parameters",
                "",
                "| Name | Kind | Required | Type | Options |",
                "|---|---|---:|---|---|",
            ]
        )
        for index, item in enumerate(parameters):
            pointer = f"{parameters_pointer}/{index}"
            parameter_name = item.get("name", item.get("dest", ""))
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(parameter_name)}` | "
                f"{_md(item.get('kind', ''))} | "
                f"{'yes' if item.get('required') else 'no'} | {_md(item.get('type', ''))} | "
                f"{_md(', '.join(cast(Sequence[str], item.get('options', ()))))} |"
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
    return lines


def _render_operation(
    value: Mapping[str, object], base_pointer: str, placed_subjects: set[str]
) -> list[str]:
    def rendered(item: object) -> object:
        return (
            json.dumps(item, ensure_ascii=False, sort_keys=True)
            if isinstance(item, (Mapping, list))
            else item
        )

    return [
        _subject_marker(base_pointer, placed_subjects),
        "| Concern | Contract |",
        "|---|---|",
        *(
            f"| {_subject_marker(f'{base_pointer}/{_escape_pointer(str(key))}', placed_subjects)}"
            f"`{_md(key)}` | {_md(rendered(item))} |"
            for key, item in value.items()
        ),
    ]


def _render_generic(
    pointers: Sequence[str], values: Sequence[object], placed_subjects: set[str]
) -> list[str]:
    if len(values) == 1 and isinstance(values[0], Mapping):
        value = cast(Mapping[str, object], values[0])
        schema_lines = _render_schema(value, pointers[0], placed_subjects)
        if schema_lines:
            return schema_lines
    large_value = values[0] if len(values) == 1 else list(values)
    if isinstance(large_value, Mapping):
        base_pointer = pointers[0]
        lines = [
            _subject_marker(base_pointer, placed_subjects),
            "| Field | Shape |",
            "|---|---|",
        ]
        for key, item in large_value.items():
            pointer = f"{base_pointer}/{_escape_pointer(str(key))}"
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(key)}` | "
                f"{_md(_shape_summary(item))} |"
            )
        return lines
    if len(values) > 1:
        lines = [
            "| Subject | Shape |",
            "|---|---|",
        ]
        for pointer, subject_value in zip(pointers, values, strict=True):
            label = _pointer_parts(pointer)[-1]
            lines.append(
                f"| {_subject_marker(pointer, placed_subjects)}`{_md(label)}` | "
                f"{_md(_shape_summary(subject_value))} |"
            )
        return lines
    return [
        _subject_marker(pointers[0], placed_subjects),
        f"- Shape: {_shape_summary(large_value)}",
    ]


def _pretty_json(value: object) -> str:
    """Return exact, readable JSON without accidentally creating Markdown links."""

    return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True).replace("](", "]\\u0028")


def _exact_contract_lines(pointers: Sequence[str], values: Sequence[object]) -> list[str]:
    lines = [
        "### Exact owned JSON",
        "",
        "The following JSON is the complete value owned at each machine-authority pointer. "
        "No contractual fields are summarized away.",
        "",
    ]
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


def _render_dossier(
    element: Mapping[str, object],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    elements_by_id: Mapping[str, Mapping[str, object]],
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
    values = [pointer_value(projection, pointer) for pointer in pointers]
    placed_subjects: set[str] = set()
    source_index = _source_index(trace)
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
                cast(Mapping[str, object], values[0]), details, pointers[0], placed_subjects
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
                "```json",
                _pretty_json(matching_records[0]),
                "```",
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
            *_exact_contract_lines(pointers, values),
        ]
    )
    return ("\n".join(lines).rstrip() + "\n").encode()

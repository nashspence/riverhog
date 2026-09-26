"""Type-specific human reading of facts already owned by a Contract Closure."""

from __future__ import annotations

import hashlib
import html
from collections.abc import Mapping, Sequence
from typing import cast

from .model import ContractAtlasError, canonical_bytes


def _esc(value: object) -> str:
    return html.escape(str(value), quote=True).replace("\r", "&#13;")


def _token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _parts(pointer: str) -> list[str]:
    if not pointer.startswith("/"):
        raise ContractAtlasError(f"invalid contract pointer: {pointer}")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def _at(value: object, pointer: str) -> object:
    current = value
    for part in _parts(pointer):
        current = (
            current[int(part)]
            if isinstance(current, list)
            else cast(Mapping[str, object], current)[part]
        )
    return current


def _anchor(pointer: str) -> str:
    return "s-" + hashlib.sha256(pointer.encode()).hexdigest()[:24]


def _compact(value: object) -> str:
    return canonical_bytes(value).decode("utf-8")


def _code(value: object) -> str:
    return f"<code>{_esc(value if isinstance(value, str) else _compact(value))}</code>"


def _link(target: str, label: object) -> str:
    return f'<a href="{_esc(target)}">{_esc(label)}</a>'


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]], *, css: str = "") -> str:
    if not rows:
        return ""
    heading = "".join(f'<th scope="col">{_esc(item)}</th>' for item in headers)
    body = "".join(
        "<tr>"
        + "".join(
            f'<td data-label="{_esc(headers[index])}">{cell}</td>' for index, cell in enumerate(row)
        )
        + "</tr>"
        for row in rows
    )
    class_name = f' class="{_esc(css)}"' if css else ""
    return (
        f'<div class="record-collection"><table{class_name}><thead><tr>{heading}</tr>'
        f"</thead><tbody>{body}</tbody></table></div>"
    )


def _facts(value: Mapping[str, object], keys: Sequence[str]) -> str:
    rows = [(key, value[key]) for key in keys if key in value]
    if not rows:
        return ""
    return (
        '<dl class="facts">'
        + "".join(
            f"<dt>{_esc(key.replace('_', ' '))}</dt><dd>{_code(item)}</dd>" for key, item in rows
        )
        + "</dl>"
    )


def _shape(
    schema: object,
    pointer: str,
    references: Mapping[str, str],
    *,
    depth: int = 0,
) -> str:
    """Compact actual schema facts for comparison; detailed structure follows below."""

    if schema is True or schema == {}:
        return "any JSON value"
    if schema is False:
        return "no JSON value"
    if not isinstance(schema, Mapping):
        return _code(schema)
    if depth > 2:
        return "nested schema"
    parts: list[str] = []
    reference = schema.get("$ref")
    if isinstance(reference, str):
        destination = references.get(pointer + "/$ref")
        parts.append(_link(destination, reference) if destination else _code(reference))
    type_name = schema.get("type")
    if type_name is not None:
        parts.append(_code(type_name))
    if schema.get("nullable") is True or (isinstance(type_name, list) and "null" in type_name):
        parts.append("nullable")
    if "properties" in schema and isinstance(schema["properties"], Mapping):
        parts.append(f"{len(schema['properties'])} fields")
    if "items" in schema:
        parts.append(
            "items: " + _shape(schema["items"], pointer + "/items", references, depth=depth + 1)
        )
    for key in (
        "format",
        "const",
        "enum",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "uniqueItems",
        "x-riverhog-encoded-bytes-max",
        "x-unicode-normalization",
        "x-riverhog-extent",
        "additionalProperties",
        "default",
    ):
        if key in schema:
            item = schema[key]
            display = (
                _shape(item, pointer + "/" + key, references, depth=depth + 1)
                if isinstance(item, Mapping)
                else _code(item)
            )
            parts.append(f"{_esc(key)}: {display}")
    for key, meaning in (("allOf", "all"), ("anyOf", "any"), ("oneOf", "exactly one")):
        variants = schema.get(key)
        if isinstance(variants, list):
            if depth >= 1:
                parts.append(
                    _link("#" + _anchor(pointer), f"{meaning} of {len(variants)} alternatives")
                )
            else:
                parts.append(
                    f"{meaning} of: "
                    + "; ".join(
                        _shape(item, f"{pointer}/{key}/{index}", references, depth=depth + 1)
                        for index, item in enumerate(variants)
                    )
                )
    return "; ".join(parts) or "structured schema"


def _schema_needs_section(value: object) -> bool:
    return isinstance(value, Mapping) and bool(
        set(value)
        & {
            "properties",
            "$defs",
            "allOf",
            "anyOf",
            "oneOf",
            "prefixItems",
            "if",
            "then",
            "else",
            "items",
            "additionalProperties",
        }
    )


def _schema(
    value: object,
    pointer: str,
    references: Mapping[str, str],
    *,
    title: str = "Schema",
) -> str:
    """Present fields, composition, and nested definitions as reading sections."""

    pending: list[tuple[object, str, str]] = [(value, pointer, title)]
    seen: set[str] = set()
    sections: list[str] = []
    while pending:
        schema, current, heading = pending.pop(0)
        if current in seen:
            continue
        seen.add(current)
        section = [
            f'<section class="schema-section" id="{_anchor(current)}"><h3>{_esc(heading)}</h3>'
        ]
        section.append(f'<p class="shape">{_shape(schema, current, references)}</p>')
        if not isinstance(schema, Mapping):
            section.append("</section>")
            sections.append("".join(section))
            continue
        basic = (
            "$id",
            "$schema",
            "title",
            "description",
            "required",
            "type",
            "format",
            "const",
            "enum",
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "minLength",
            "maxLength",
            "minItems",
            "maxItems",
            "uniqueItems",
            "pattern",
            "additionalProperties",
            "discriminator",
            "x-unicode-normalization",
            "x-riverhog-encoded-bytes-max",
            "x-riverhog-extent",
        )
        section.append(_facts(schema, basic))
        properties = schema.get("properties")
        if isinstance(properties, Mapping):
            required = set(cast(Sequence[str], schema.get("required", ())))
            rows: list[tuple[str, ...]] = []
            for name, field in sorted(properties.items()):
                child_pointer = current + "/properties/" + _token(str(name))
                field_map = field if isinstance(field, Mapping) else {}
                label = _code(name)
                shape = _shape(field, child_pointer, references)
                if _schema_needs_section(field):
                    label = _link("#" + _anchor(child_pointer), name)
                    pending.append((field, child_pointer, f"Field: {name}"))
                description = _esc(field_map.get("description", ""))
                default = _code(field_map["default"]) if "default" in field_map else "not declared"
                rows.append(
                    (label, "yes" if name in required else "no", shape, default, description)
                )
            section.append("<h4>Fields</h4>")
            section.append(
                _table(("Field", "Required", "Shape and bounds", "Default", "Description"), rows)
            )
        for key, meaning in (
            ("allOf", "All must match"),
            ("anyOf", "At least one must match"),
            ("oneOf", "Exactly one must match"),
            ("prefixItems", "Positional items"),
        ):
            variants = schema.get(key)
            if not isinstance(variants, list):
                continue
            rows = []
            for index, item in enumerate(variants):
                child_pointer = f"{current}/{key}/{index}"
                label = str(index + 1)
                if _schema_needs_section(item):
                    label = _link("#" + _anchor(child_pointer), label)
                    pending.append((item, child_pointer, f"{meaning}: {index + 1}"))
                rows.append((label, _shape(item, child_pointer, references)))
            section.append(f"<h4>{_esc(meaning)}</h4>")
            section.append(_table(("Alternative", "Schema"), rows))
        for key in ("if", "then", "else", "items", "additionalProperties"):
            child = schema.get(key)
            if _schema_needs_section(child):
                child_pointer = current + "/" + key
                section.append(f"<p>{_link('#' + _anchor(child_pointer), key + ' schema')}</p>")
                pending.append((child, child_pointer, f"{key} schema"))
        definitions = schema.get("$defs")
        if isinstance(definitions, Mapping):
            links = []
            for name, child in sorted(definitions.items()):
                child_pointer = current + "/$defs/" + _token(str(name))
                links.append(f"<li>{_link('#' + _anchor(child_pointer), name)}</li>")
                pending.append((child, child_pointer, f"Definition: {name}"))
            section.append("<h4>Definitions</h4><ul>" + "".join(links) + "</ul>")
        section.append("</section>")
        sections.append("".join(section))
    return "".join(sections)


def _http(value: Mapping[str, object], pointer: str, references: Mapping[str, str]) -> str:
    if "operationId" not in value and "operation_id" in value:
        return "<h3>Supplemental HTTP operation binding</h3>" + _facts(value, sorted(value))
    parts = ["<h3>Operation</h3>"]
    if value.get("summary"):
        parts.append(f'<p class="lead">{_esc(value["summary"])}</p>')
    parts.append(
        _facts(
            value,
            (
                "operationId",
                "description",
                "tags",
                "security",
                "x-riverhog-permission-requirements",
                "x-riverhog-read-collection",
                "x-riverhog-interface",
            ),
        )
    )
    parameters = cast(Sequence[Mapping[str, object]], value.get("parameters", ()))
    if parameters:
        rows: list[tuple[str, ...]] = []
        for index, parameter in enumerate(parameters):
            schema = parameter.get("schema", {})
            schema_pointer = f"{pointer}/parameters/{index}/schema"
            default = (
                _code(schema["default"])
                if isinstance(schema, Mapping) and "default" in schema
                else "not declared"
            )
            rows.append(
                (
                    _code(parameter.get("name", "")),
                    _code(parameter.get("in", "")),
                    "yes" if parameter.get("required") else "no",
                    default,
                    _shape(schema, schema_pointer, references),
                    _esc(parameter.get("description", "")),
                )
            )
        parts.extend(
            (
                "<h3>Parameters</h3>",
                _table(("Name", "In", "Required", "Default", "Schema", "Description"), rows),
            )
        )
    request = value.get("requestBody")
    if isinstance(request, Mapping):
        parts.append("<h3>Request body</h3>")
        parts.append(_facts(request, ("required", "description")))
        content = cast(Mapping[str, Mapping[str, object]], request.get("content", {}))
        rows = [
            (
                _code(media_type),
                _shape(
                    media.get("schema", {}),
                    f"{pointer}/requestBody/content/{_token(media_type)}/schema",
                    references,
                ),
            )
            for media_type, media in sorted(content.items())
        ]
        parts.append(_table(("Media type", "Schema"), rows))
    responses = value.get("responses")
    if isinstance(responses, Mapping):
        rows = []
        header_rows = []
        for status, response in sorted(responses.items()):
            if not isinstance(response, Mapping):
                continue
            content = cast(Mapping[str, Mapping[str, object]], response.get("content", {}))
            media: Sequence[tuple[str, Mapping[str, object]]] = sorted(content.items()) or [
                ("—", {})
            ]
            codes = (
                ", ".join(
                    _code(code)
                    for code in cast(Sequence[str], response.get("x-riverhog-error-codes", ()))
                )
                or "not declared"
            )
            for media_type, body in media:
                path = (
                    f"{pointer}/responses/{_token(str(status))}/content/"
                    f"{_token(str(media_type))}/schema"
                )
                rows.append(
                    (
                        _code(status),
                        _esc(response.get("description", "")),
                        _code(media_type),
                        _shape(body.get("schema", {}), path, references),
                        codes,
                    )
                )
            for name, header in sorted(
                cast(Mapping[str, Mapping[str, object]], response.get("headers", {})).items()
            ):
                path = (
                    f"{pointer}/responses/{_token(str(status))}/headers/{_token(str(name))}/schema"
                )
                header_rows.append(
                    (
                        _code(status),
                        _code(name),
                        "yes"
                        if header.get("required")
                        else "no"
                        if "required" in header
                        else "not declared",
                        _shape(header.get("schema", {}), path, references),
                        _esc(header.get("description", "")),
                    )
                )
        parts.extend(
            (
                "<h3>Responses</h3>",
                _table(("Status", "Description", "Media type", "Schema", "Error codes"), rows),
            )
        )
        if header_rows:
            parts.extend(
                (
                    "<h4>Response headers</h4>",
                    _table(("Status", "Header", "Required", "Schema", "Description"), header_rows),
                )
            )
    return "".join(parts)


def _cli_type(parameter: Mapping[str, object]) -> str:
    kind = parameter.get("type")
    if isinstance(kind, Mapping):
        parts = [_code(kind.get("name", kind.get("class", "not recorded")))]
        constraints = kind
    else:
        parts = [_code(kind if kind is not None else "not recorded")]
        constraints = parameter
    for key in ("choices", "minimum", "maximum", "clamp", "exists", "file_okay", "dir_okay"):
        if key in constraints:
            parts.append(f"{_esc(key.replace('_', ' '))}: {_code(constraints[key])}")
    return "; ".join(parts)


def _channel(value: object) -> str:
    if isinstance(value, Mapping):
        kind = value.get("kind")
        if kind == "http-operation-response":
            return (
                "HTTP response "
                + _code(value.get("status", ""))
                + " from "
                + _code(value.get("method", ""))
                + " "
                + _code(value.get("path", ""))
                + (
                    " · " + _code(value["schema"]["$ref"])
                    if isinstance(value.get("schema"), Mapping) and "$ref" in value["schema"]
                    else ""
                )
            )
        if kind == "python-model":
            return "Python model " + _code(value.get("identity", ""))
        return "; ".join(f"{_esc(key)}: {_code(item)}" for key, item in sorted(value.items()))
    return _code(value)


def _cli(values: Mapping[str, object], pointer: str) -> str:
    parts = [
        "<h3>Command</h3>",
        _facts(
            values,
            (
                "name",
                "subcommand_required",
                "allow_extra_args",
                "allow_interspersed_args",
                "ignore_unknown_options",
                "allow_abbrev",
            ),
        ),
    ]
    parts.append(
        "<p>Parser flags and defaults are recorded inputs. A missing default does not "
        "assert an explicit null value or rule out other configured fallbacks.</p>"
    )
    parameters = cast(Sequence[Mapping[str, object]], values.get("parameters", ()))
    if parameters:
        rows: list[tuple[str, ...]] = []
        for parameter in parameters:
            options = ", ".join(
                _code(item) for item in cast(Sequence[str], parameter.get("options", ()))
            )
            secondary = ", ".join(
                _code(item) for item in cast(Sequence[str], parameter.get("secondary_options", ()))
            )
            spelling = _code(parameter.get("name", parameter.get("dest", "")))
            if options:
                spelling += "<br>" + options
            if secondary:
                spelling += "<br>Alternate: " + secondary
            invocation = ["required" if parameter.get("required") else "optional"]
            invocation.append(
                "argument"
                if parameter.get("kind") == "TyperArgument" or not parameter.get("options")
                else "option"
            )
            if "nargs" in parameter:
                invocation.append(f"{_esc(parameter['nargs'])} value(s) per use")
            if parameter.get("multiple"):
                invocation.append("repeatable")
            if parameter.get("count"):
                invocation.append("counts repeats")
            if parameter.get("is_flag"):
                invocation.append("flag")
            default = _code(parameter["default"]) if "default" in parameter else "not recorded"
            if parameter.get("envvar"):
                default += "<br>Environment: " + _code(parameter["envvar"])
            rows.append(
                (
                    spelling,
                    "; ".join(invocation),
                    _cli_type(parameter),
                    default,
                )
            )
        parts.extend(
            (
                "<h3>Arguments and options</h3>",
                _table(
                    (
                        "Parameter and spelling",
                        "Invocation",
                        "Type and constraints",
                        "Default and environment",
                    ),
                    rows,
                ),
            )
        )
    exclusive = cast(Sequence[Mapping[str, object]], values.get("mutually_exclusive_groups", ()))
    if exclusive:
        rows = []
        for group in exclusive:
            names = ", ".join(
                str(parameters[index].get("name", parameters[index].get("dest", index)))
                for index in cast(Sequence[int], group.get("parameters", ()))
            )
            rows.append(("Exactly one" if group.get("required") else "At most one", _esc(names)))
        parts.extend(("<h3>Argument combinations</h3>", _table(("Rule", "Parameters"), rows)))
    result = values.get("result_contract")
    if isinstance(result, Mapping):
        parts.extend(
            (
                "<h3>Result contract</h3>",
                _facts(
                    result,
                    ("identity", "profile_id", "structured_output", "human_json_relationship"),
                ),
            )
        )
        for key, label in (("success", "Success outcomes"), ("failures", "Failure outcomes")):
            rows = [
                (
                    _code(item.get("id", "")),
                    _code(item.get("exit_status", "")),
                    _channel(item.get("selected_by", {})),
                    "; ".join(
                        f"{_esc(channel)}: {_channel(content)}"
                        for channel, content in sorted(
                            cast(Mapping[str, object], item.get("stdout", {})).items()
                        )
                    ),
                    "; ".join(
                        f"{_esc(channel)}: {_channel(content)}"
                        for channel, content in sorted(
                            cast(Mapping[str, object], item.get("stderr", {})).items()
                        )
                    ),
                )
                for item in cast(Sequence[Mapping[str, object]], result.get(key, ()))
            ]
            if rows:
                parts.extend(
                    (
                        f"<h4>{label}</h4>",
                        _table(("Outcome", "Exit", "Selection", "stdout", "stderr"), rows),
                    )
                )
    controls = cast(Sequence[Mapping[str, object]], values.get("terminating_controls", ()))
    if controls:
        rows = [
            (
                _code(item.get("id", "")),
                _code(item.get("trigger", {})),
                _code(item.get("exit_status", "")),
                _code(item.get("stdout", "")),
                _code(item.get("stderr", "")),
            )
            for item in controls
        ]
        parts.extend(
            (
                "<h3>Terminating controls</h3>",
                _table(("Control", "Trigger", "Exit", "stdout", "stderr"), rows),
            )
        )
    del pointer
    return "".join(parts)


def _python(value: Mapping[str, object], pointer: str, references: Mapping[str, str]) -> str:
    contract = cast(Mapping[str, object], value.get("contract", {}))
    parts = ["<h3>Python declaration</h3>"]
    parts.append(_facts(value, ("distribution", "module", "name", "owner", "unit")))
    parts.append(_facts(contract, ("kind", "signature", "type", "value")))
    enum_values = contract.get("enum_values")
    if isinstance(enum_values, Mapping):
        parts.extend(
            (
                "<h4>Enum members</h4>",
                _table(
                    ("Member", "Value"),
                    [(_code(name), _code(item)) for name, item in sorted(enum_values.items())],
                ),
            )
        )
    fields = contract.get("fields")
    if isinstance(fields, list):
        rows = [
            (
                _code(item.get("name", "")),
                _code(item.get("type", "")),
                _code(item["default"]) if "default" in item else "not declared",
            )
            for item in cast(Sequence[Mapping[str, object]], fields)
        ]
        parts.extend(("<h4>Dataclass fields</h4>", _table(("Field", "Type", "Default"), rows)))
    if isinstance(contract.get("schema"), Mapping):
        parts.append(
            _schema(
                contract["schema"],
                pointer + "/contract/schema",
                references,
                title="Validated model schema",
            )
        )
    return "".join(parts)


def _relational_table(value: Mapping[str, object]) -> str:
    parts = [f"<h3>Table: {_code(value.get('name', ''))}</h3>"]
    columns = cast(Sequence[Mapping[str, object]], value.get("columns", ()))
    rows: list[tuple[str, ...]] = [
        (
            _code(column.get("name", "")),
            _code(column.get("type", "")),
            "yes" if column.get("nullable") else "no",
            _code(column["default"]) if "default" in column else "not declared",
            _code(column.get("definition", "")),
        )
        for column in columns
    ]
    parts.append(_table(("Column", "Type", "Nullable", "Default", "Exact definition"), rows))
    constraints = cast(Sequence[Mapping[str, object]], value.get("constraints", ()))
    if constraints:
        rows = [
            (
                _code(item.get("kind", "")),
                _code(item.get("name", "")),
                _code(item.get("definition", "")),
            )
            for item in constraints
        ]
        parts.extend(
            ("<h4>Table constraints</h4>", _table(("Kind", "Name", "Exact definition"), rows))
        )
    return "".join(parts)


def _durable(values: Sequence[tuple[str, object]], references: Mapping[str, str]) -> str:
    if len(values) > 1:
        return "<h3>Durable identity and transition</h3>" + _table(
            ("Authority fact", "Value"),
            [(_code(_parts(pointer)[-1]), _code(value)) for pointer, value in values],
        )
    pointer, value = values[0]
    if not isinstance(value, Mapping):
        return f'<p class="promise">{_code(value)}</p>'
    if "columns" in value and "constraints" in value:
        return _relational_table(value)
    if "columns" in value and "definition" in value and "table" in value:
        return (
            "<h3>Unique index</h3>"
            + _facts(value, ("name", "table", "definition"))
            + "<h4>Indexed columns</h4><ul>"
            + "".join(
                f"<li>{_code(column)}</li>" for column in cast(Sequence[object], value["columns"])
            )
            + "</ul>"
        )
    parts = [
        "<h3>Durable state unit</h3>",
        _facts(
            value,
            (
                "id",
                "name",
                "format",
                "head",
                "transition",
                "kind",
                "dialect",
                "user_version",
                "identity",
                "distribution",
                "header",
                "structure",
            ),
        ),
    ]
    if "tables" in value and isinstance(value["tables"], list):
        for table in cast(Sequence[Mapping[str, object]], value["tables"]):
            parts.append(_relational_table(table))
    if "unique_indexes" in value and isinstance(value["unique_indexes"], list):
        rows = [
            (
                _code(item.get("name", "")),
                _code(item.get("columns", ())),
                _code(item.get("definition", "")),
            )
            for item in cast(Sequence[Mapping[str, object]], value["unique_indexes"])
        ]
        if rows:
            parts.extend(
                ("<h4>Unique indexes</h4>", _table(("Name", "Columns", "Definition"), rows))
            )
    for key, label in (("schema", "Document schema"), ("record_schema", "Record schema")):
        if isinstance(value.get(key), Mapping):
            parts.append(_schema(value[key], pointer + "/" + key, references, title=label))
    if len(parts) == 2 and not parts[1]:
        parts.append(_facts(value, sorted(value)))
    return "".join(parts)


def _protocol_operation(
    value: Mapping[str, object], pointer: str, references: Mapping[str, str]
) -> str:
    parts = [
        "<h3>Process protocol operation</h3>",
        _facts(value, ("method", "path", "error_schema")),
    ]
    parameters = cast(Sequence[Mapping[str, object]], value.get("path_parameters", ()))
    if parameters:
        rows = [
            (
                _code(item.get("name", "")),
                _code(item.get("type", "")),
                "yes" if item.get("required") else "no",
            )
            for item in parameters
        ]
        parts.extend(("<h4>Path parameters</h4>", _table(("Name", "Type", "Required"), rows)))
    for key, label in (("request", "Request"), ("response", "Response")):
        record = value.get(key)
        if isinstance(record, Mapping):
            parts.extend((f"<h4>{label}</h4>", _facts(record, sorted(record))))
    errors = cast(Sequence[Mapping[str, object]], value.get("errors", ()))
    if errors:
        parts.extend(
            (
                "<h4>Errors</h4>",
                _table(
                    ("Status", "Code"),
                    [
                        (_code(item.get("status", "")), _code(item.get("code", "")))
                        for item in errors
                    ],
                ),
            )
        )
    del pointer, references
    return "".join(parts)


def _configuration_environment(value: Mapping[str, object]) -> str:
    parts = ["<h3>Environment setting</h3>", _facts(value, ("name", "owner", "input_shape"))]
    defaults = cast(Sequence[str], value.get("default_expressions", ()))
    if defaults:
        parts.extend(
            (
                "<h4>Default expressions</h4>",
                "<ul>" + "".join(f"<li>{_code(item)}</li>" for item in defaults) + "</ul>",
            )
        )
    consumers = cast(Sequence[str], value.get("consumers", ()))
    if consumers:
        parts.extend(
            (
                "<h4>Consumers</h4>",
                "<ul>" + "".join(f"<li>{_code(item)}</li>" for item in consumers) + "</ul>",
            )
        )
    return "".join(parts)


def _release(value: object) -> str:
    if not isinstance(value, Mapping):
        return f'<p class="promise">{_esc(value)}</p>'
    parts = ["<h3>Publication and compatibility facts</h3>"]
    if isinstance(value.get("description"), str):
        parts.append(f'<p class="lead">{_esc(value["description"])}</p>')
    scalar_keys = [
        key
        for key, item in sorted(value.items())
        if not isinstance(item, (Mapping, list)) and key != "description"
    ]
    parts.append(_facts(value, scalar_keys))
    for key, item in sorted(value.items()):
        if isinstance(item, Mapping):
            parts.extend((f"<h4>{_esc(key.replace('_', ' '))}</h4>", _facts(item, sorted(item))))
        elif isinstance(item, list):
            if item and all(isinstance(entry, Mapping) for entry in item):
                headers = tuple(sorted({field for entry in item for field in entry}))
                rows = [
                    tuple(_code(entry[field]) if field in entry else "—" for field in headers)
                    for entry in item
                ]
                parts.extend((f"<h4>{_esc(key.replace('_', ' '))}</h4>", _table(headers, rows)))
            else:
                parts.extend(
                    (
                        f"<h4>{_esc(key.replace('_', ' '))}</h4>",
                        "<ul>" + "".join(f"<li>{_code(entry)}</li>" for entry in item) + "</ul>",
                    )
                )
    return "".join(parts)


def _simple(value: object, title: str) -> str:
    if not isinstance(value, Mapping):
        return f'<p class="promise">{_esc(value)}</p>'
    return f"<h3>{_esc(title)}</h3>" + _facts(value, sorted(value))


def command_path(pointer: str) -> tuple[str, ...]:
    parts = _parts(pointer)
    if len(parts) < 4 or parts[:2] != ["external_contract", "cli"]:
        raise ContractAtlasError(f"CLI element has no parser path: {pointer}")
    path = [parts[2]]
    for index, part in enumerate(parts[:-1]):
        if part == "commands":
            path.append(parts[index + 1])
    return tuple(path)


def render_human(
    closure: Mapping[str, object],
    element: Mapping[str, object],
    references: Mapping[str, str],
) -> str:
    """Render the owned values as the known human interface, with exact data elsewhere."""

    pointers = cast(Sequence[str], element["pointers"])
    values = [(pointer, _at(closure, pointer)) for pointer in pointers]
    interface = str(element["interface"])
    if interface == "cli":
        parents = {pointer.rsplit("/", 1)[0] for pointer in pointers}
        if len(parents) != 1:
            raise ContractAtlasError("CLI element combines different command nodes")
        parent = next(iter(parents))
        record = {_parts(pointer)[-1]: value for pointer, value in values}
        body = _cli(record, parent)
    elif interface == "durable-state":
        body = _durable(values, references)
    elif interface in {"http-service-declaration", "process-protocol"} and len(values) > 1:
        record = {_parts(pointer)[-1]: value for pointer, value in values}
        body = _simple(
            record,
            "HTTP service" if interface == "http-service-declaration" else "Process protocol",
        )
    else:
        if len(values) != 1:
            raise ContractAtlasError(f"human renderer requires one owned value: {element['id']}")
        pointer, value = values[0]
        if interface == "http-operations" and isinstance(value, Mapping):
            body = _http(value, pointer, references)
        elif interface in {"schema", "http-schemas", "process-protocol-schemas", "configuration"}:
            body = _schema(value, pointer, references)
        elif interface == "python" and isinstance(value, Mapping):
            body = _python(value, pointer, references)
        elif interface == "process-protocol-operations" and isinstance(value, Mapping):
            body = _protocol_operation(value, pointer, references)
        elif interface == "configuration-environment" and isinstance(value, Mapping):
            body = _configuration_environment(value)
        elif interface in {
            "artifact-verification",
            "compatibility-guarantees",
            "installation-roots",
            "publication-locations",
            "python-distributions",
            "release-artifacts",
            "runtime-images",
            "versioning-tags",
        }:
            body = _release(value)
        elif interface == "extent":
            body = _simple(value, "Extent commitment")
        elif interface == "process-protocol":
            body = _simple(value, "Process protocol")
        elif interface == "http-service-declaration":
            body = _simple(value, "HTTP service")
        elif interface == "http-security-schemes":
            body = _simple(value, "HTTP authorization scheme")
        else:
            body = _simple(value, "Contract facts")
    return '<div class="human-contract">' + body + "</div>"


def inventory_fact(interface: str, value: object, pointer: str) -> str:
    """Small owned comparison facts at the real interface selection scope."""

    if interface == "compatibility-guarantees":
        return f'<span class="comparison-promise">{_esc(value)}</span>'
    if interface == "extent" and isinstance(value, str):
        return _esc(value)
    if not isinstance(value, Mapping):
        return _code(value)
    if interface == "python":
        contract = cast(Mapping[str, object], value.get("contract", {}))
        return _code(contract.get("kind", ""))
    if interface == "cli":
        return ""
    if interface == "http-operations":
        summary = value.get("summary")
        return _esc(summary) if isinstance(summary, str) else ""
    if interface in {"schema", "http-schemas", "process-protocol-schemas", "configuration"}:
        properties = value.get("properties", {})
        field_count = len(properties) if isinstance(properties, Mapping) else 0
        if field_count:
            return f"{field_count} {'field' if field_count == 1 else 'fields'}"
        for composition in ("oneOf", "anyOf", "allOf"):
            variants = value.get(composition)
            if isinstance(variants, list) and variants:
                return f"{len(variants)} alternatives"
        return ""
    if interface == "durable-state":
        if "columns" in value:
            columns = len(cast(Sequence[object], value["columns"]))
            constraints = len(cast(Sequence[object], value.get("constraints", ())))
            return f"{columns} columns" + (f" · {constraints} constraints" if constraints else "")
        return ""
    if interface == "configuration-environment":
        defaults = cast(Sequence[object], value.get("default_expressions", ()))
        return "default " + ", ".join(_code(item) for item in defaults) if defaults else ""
    if interface == "process-protocol-operations":
        response = value.get("response", {})
        statuses = response.get("statuses", ()) if isinstance(response, Mapping) else ()
        return "response " + _code(statuses) if statuses else ""
    for key in ("description", "coordinate", "repository", "role", "name", "title", "format"):
        if isinstance(value.get(key), str):
            return _esc(value[key])
    return ""

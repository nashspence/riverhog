"""Schema slices retain the local reference environment of their exact owner."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator
from pydantic import JsonValue

from stove0_protocol.models import JsonSchemaValidationProfile
from stove0_protocol.predicates import (
    MISSING,
    RowAll,
    RowAny,
    RowItems,
    RowNot,
    RowPredicate,
    pointer_parts,
    read_pointer,
)

_MAPS = {"$defs", "properties", "patternProperties", "dependentSchemas"}
_ARRAYS = {"allOf", "anyOf", "oneOf", "prefixItems"}
_SINGLES = {
    "additionalProperties",
    "unevaluatedProperties",
    "propertyNames",
    "items",
    "contains",
    "unevaluatedItems",
    "if",
    "then",
    "else",
    "not",
    "contentSchema",
}
_JSON_TYPES = {"null", "boolean", "integer", "number", "string", "array", "object"}
type SchemaNode = dict[str, Any] | bool


@dataclass(frozen=True, slots=True)
class SchemaSlice:
    root: dict[str, JsonValue]
    schema: SchemaNode

    def validator(self) -> Validator:
        return Draft202012Validator(self.root).evolve(schema=self.schema)

    def validate(self, value: JsonValue) -> None:
        error = next(self.validator().iter_errors(value), None)
        if error is not None:
            raise ValueError(f"interface record violates its exact owning schema: {error.message}")


def schema_slice(profile: JsonSchemaValidationProfile, pointer: str) -> SchemaSlice:
    parts, index = pointer_parts(pointer), 0
    node: SchemaNode = profile.document
    while index < len(parts):
        if not isinstance(node, dict):
            raise ValueError("interface record_schema_at does not select a schema location")
        keyword = parts[index]
        if keyword in _MAPS | _ARRAYS:
            if index + 1 >= len(parts):
                raise ValueError("interface record_schema_at selects a schema container")
            child = read_pointer(
                node,
                "/"
                + "/".join(
                    part.replace("~", "~0").replace("/", "~1") for part in parts[index : index + 2]
                ),
            )
            index += 2
        elif keyword in _SINGLES:
            child = node.get(keyword, MISSING)
            index += 1
        else:
            raise ValueError("interface record_schema_at selects data rather than a schema")
        if child is MISSING or not isinstance(child, (dict, bool)):
            raise ValueError("interface record_schema_at does not select a declared schema")
        node = child
    return SchemaSlice(root=profile.document, schema=node)


def _expanded(root: dict[str, JsonValue], node: SchemaNode) -> Iterator[SchemaNode]:
    pending, seen = [node], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if not isinstance(current, dict):
            yield current
            continue
        ref = current.get("$ref")
        if ref is not None:
            if not isinstance(ref, str) or not ref.startswith("#/"):
                # Anchors/dynamic references remain valid owning schema but
                # cannot justify a static narrowing by this pointer analysis.
                yield True
            else:
                target = read_pointer(root, ref[1:])
                if target is MISSING or not isinstance(target, (dict, bool)):
                    raise ValueError("unresolved local owning schema reference")
                pending.append(target)
        branches = tuple(current.get(key, ()) for key in ("allOf", "anyOf", "oneOf"))
        for children in branches:
            pending.extend(children)
        if not ref and not any(branches):
            yield current
        elif "type" in current or "properties" in current or "items" in current:
            yield current


def field_schemas(slice: SchemaSlice, pointer: str) -> tuple[SchemaNode, ...]:
    candidates: tuple[SchemaNode, ...] = (slice.schema,)
    for part in pointer_parts(pointer):
        next_candidates = []
        for node in candidates:
            for current in _expanded(slice.root, node):
                if current is False:
                    continue
                if current is True:
                    next_candidates.append(True)
                    continue
                if part in current.get("properties", {}):
                    next_candidates.append(current["properties"][part])
                elif "properties" in current or current.get("type") == "object":
                    additional = current.get("additionalProperties", True)
                    if additional is not False:
                        next_candidates.append(additional)
                elif (
                    part.isascii()
                    and part.isdecimal()
                    and (part == "0" or not part.startswith("0"))
                ):
                    prefix = current.get("prefixItems", ())
                    index = int(part)
                    next_candidates.append(
                        prefix[index] if index < len(prefix) else current.get("items", True)
                    )
                else:
                    next_candidates.append(True)
        candidates = tuple(next_candidates)
    return tuple(child for node in candidates for child in _expanded(slice.root, node))


def _types(root: dict[str, JsonValue], nodes: Sequence[SchemaNode]) -> set[str]:
    result: set[str] = set()
    for node in nodes:
        if node is True:
            result.update(_JSON_TYPES)
        elif isinstance(node, dict):
            declared = node.get("type")
            if declared is None:
                result.update(_JSON_TYPES)
            else:
                result.update((declared,) if isinstance(declared, str) else declared)
    return result


def _value_type(value: JsonValue) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if type(value) is int:
        return "integer"
    if type(value) is float:
        return "number"
    if isinstance(value, str):
        return "string"
    return "object" if isinstance(value, dict) else "array"


def validate_row_schema(predicate: RowPredicate, slice: SchemaSlice) -> None:
    """Reject provable operand/type errors without inventing required fields."""
    pending = [(predicate, slice)]
    while pending:
        node, context = pending.pop()
        if isinstance(node, bool):
            continue
        if isinstance(node, (RowAll, RowAny)):
            pending.extend(
                (child, context) for child in (node.all if isinstance(node, RowAll) else node.any)
            )
            continue
        if isinstance(node, RowNot):
            pending.append((node.negated, context))
            continue
        if isinstance(node, RowItems):
            schemas = field_schemas(context, node.items.path)
            if "array" not in _types(context.root, schemas):
                raise ValueError("items condition requires a declared array field")
            items = [
                candidate.get("items", True) if isinstance(candidate, dict) else True
                for candidate in schemas
            ]
            for item in items:
                pending.append((node.items.where, SchemaSlice(root=context.root, schema=item)))
            continue
        test = node.test
        schemas = field_schemas(context, test.path)
        if not schemas:
            if test.op == "exists":
                continue
            raise ValueError("condition field cannot exist in the exact interface record schema")
        types = _types(context.root, schemas)
        if "number" in types:
            types.add("integer")
        if test.op == "exists":
            continue
        if test.op == "contains":
            if not types & {"string", "array"} or (
                "array" not in types and not isinstance(test.value, str)
            ):
                raise ValueError("contains condition has incompatible declared operand types")
            continue
        if test.op == "in":
            assert isinstance(test.value, list)
            values: Sequence[JsonValue] = test.value
        else:
            values = (test.value,)
        if any(_value_type(value) not in types for value in values):
            raise ValueError("condition literal has an incompatible declared field type")

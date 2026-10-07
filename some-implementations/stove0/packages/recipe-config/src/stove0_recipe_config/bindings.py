"""The recipe language's explicit object binding operations."""

from __future__ import annotations

from copy import deepcopy

from pydantic import JsonValue
from stove0_protocol.predicates import MISSING, pointer_parts, read_pointer

from stove0_recipe_config.source import ValueBinding, validate_bindings


def bind_value(document: dict[str, JsonValue], binding: ValueBinding, value: JsonValue) -> None:
    parts = pointer_parts(binding.at)
    if not parts:
        if binding.mode == "insert" or not isinstance(value, dict):
            raise ValueError("root binding requires an object replace or merge")
        if binding.mode == "replace":
            document.clear()
        document.update(deepcopy(value))
        return
    container = document
    for part in parts[:-1]:
        if part not in container and binding.mode == "insert":
            container[part] = {}
        candidate = container.get(part, MISSING)
        if not isinstance(candidate, dict):
            raise ValueError("binding destination ancestors must be existing objects")
        container = candidate
    key = parts[-1]
    if binding.mode == "insert":
        if key in container:
            raise ValueError("insert binding collides with an existing field")
        container[key] = deepcopy(value)
    elif key not in container:
        raise ValueError("replace or merge binding requires a present field")
    elif binding.mode == "replace":
        container[key] = deepcopy(value)
    else:
        previous = container[key]
        if not isinstance(previous, dict) or not isinstance(value, dict):
            raise ValueError("merge-object requires object source and destination")
        previous.update(deepcopy(value))


def apply_bindings(
    *,
    intent: dict[str, JsonValue],
    options: dict[str, JsonValue],
    bindings: tuple[ValueBinding, ...],
    parameters: dict[str, JsonValue],
    evaluation: dict[str, JsonValue] | None,
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    validate_bindings(bindings)
    intent, options = deepcopy(intent), deepcopy(options)
    for binding in bindings:
        source = parameters if binding.source == "parameters" else evaluation
        if source is None:
            raise ValueError("call binding requires an actual evaluation context")
        value = read_pointer(source, binding.path)
        if value is MISSING:
            raise ValueError("call binding source is missing")
        bind_value(intent if binding.to == "intent" else options, binding, value)
    return intent, options

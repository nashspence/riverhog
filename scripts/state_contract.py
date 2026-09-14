"""Project durable-state structures from their component-owned authorities."""

from __future__ import annotations

import copy
import importlib
import re
from collections.abc import Mapping, Sequence
from typing import Any, cast

from jsonschema import Draft202012Validator
from pydantic import BaseModel


class StateContractError(RuntimeError):
    """A durable-state declaration cannot be projected exactly."""


def _split_top_level(value: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    index = 0
    while index < len(value):
        character = value[index]
        if quote is not None:
            if character == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if character in {"'", '"'}:
            quote = character
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth < 0:
                raise StateContractError("SQL authority has unbalanced parentheses")
        elif character == delimiter and depth == 0:
            part = value[start:index].strip()
            if part:
                parts.append(part)
            start = index + 1
        index += 1
    if quote is not None or depth != 0:
        raise StateContractError("SQL authority has an unterminated quote or parenthesis")
    final = value[start:].strip()
    if final:
        parts.append(final)
    return parts


def _statements(value: object) -> list[str]:
    if isinstance(value, str):
        return _split_top_level(value, ";")
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        statements = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise StateContractError("SQL authority contains a non-string statement")
            statements.extend(_split_top_level(item, ";"))
        return statements
    raise StateContractError("SQL authority must be a string or sequence of strings")


def _identifier(value: str) -> str:
    current = value.strip()
    if current.startswith('"') and current.endswith('"'):
        return current[1:-1].replace('""', '"')
    return current


def _identifier_list(value: str) -> list[str]:
    return [_identifier(item) for item in _split_top_level(value, ",")]


_COLUMN_CLAUSE = re.compile(
    r"\b(?:NOT\s+NULL|NULL|DEFAULT|PRIMARY\s+KEY|UNIQUE|CHECK|REFERENCES|GENERATED)\b",
    re.IGNORECASE,
)


def _top_level_clause_positions(value: str) -> list[tuple[int, str]]:
    positions: list[tuple[int, str]] = []
    depth = 0
    quote: str | None = None
    index = 0
    while index < len(value):
        character = value[index]
        if quote is not None:
            if character == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if character in {"'", '"'}:
            quote = character
            index += 1
            continue
        if character == "(":
            depth += 1
            index += 1
            continue
        if character == ")":
            depth -= 1
            index += 1
            continue
        if depth == 0:
            match = _COLUMN_CLAUSE.match(value, index)
            if match is not None:
                positions.append((index, re.sub(r"\s+", " ", match.group(0).upper())))
                index = match.end()
                continue
        index += 1
    return positions


def _column(value: str) -> dict[str, object]:
    match = re.match(
        r'(?P<name>"(?:""|[^"])+"|[A-Za-z_][A-Za-z0-9_]*)\s+(?P<body>.+)', value, re.DOTALL
    )
    if match is None:
        raise StateContractError(f"cannot parse SQL column definition: {value}")
    body = match.group("body").strip()
    clauses = _top_level_clause_positions(body)
    type_end = clauses[0][0] if clauses else len(body)
    column_type = body[:type_end].strip()
    if not column_type:
        raise StateContractError(f"SQL column has no type: {value}")
    result: dict[str, object] = {
        "name": _identifier(match.group("name")),
        "type": column_type,
        "nullable": not any(kind == "NOT NULL" for _position, kind in clauses),
        "definition": value,
    }
    kinds = [kind for _position, kind in clauses]
    if "PRIMARY KEY" in kinds:
        result["primary_key"] = True
        result["nullable"] = False
    if "UNIQUE" in kinds:
        result["unique"] = True
    for current_index, (position, kind) in enumerate(clauses):
        following = clauses[current_index + 1][0] if current_index + 1 < len(clauses) else len(body)
        payload = body[position:following].strip()
        if kind == "DEFAULT":
            result["default"] = payload[len("DEFAULT") :].strip()
        elif kind == "GENERATED":
            result["generated"] = payload
        elif kind == "CHECK":
            checks = cast(list[str], result.setdefault("checks", []))
            checks.append(payload[len("CHECK") :].strip())
        elif kind == "REFERENCES":
            result["references"] = payload[len("REFERENCES") :].strip()
    return result


def _table_constraint(value: str) -> dict[str, object]:
    current = value.strip()
    name: str | None = None
    constraint = re.match(
        r'CONSTRAINT\s+(?P<name>"(?:""|[^"])+"|[A-Za-z_][A-Za-z0-9_]*)\s+(?P<body>.+)',
        current,
        re.IGNORECASE | re.DOTALL,
    )
    if constraint is not None:
        name = _identifier(constraint.group("name"))
        current = constraint.group("body").strip()
    kind_match = re.match(
        r"(?P<kind>PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK)\b",
        current,
        re.IGNORECASE,
    )
    if kind_match is None:
        raise StateContractError(f"unsupported SQL table constraint: {value}")
    kind = re.sub(r"\s+", "-", kind_match.group("kind").casefold())
    result: dict[str, object] = {"kind": kind, "definition": value}
    if name is not None:
        result["name"] = name
    columns = re.search(r"\((?P<columns>[^()]*)\)", current)
    if columns is not None and kind in {"primary-key", "unique", "foreign-key"}:
        result["columns"] = _identifier_list(columns.group("columns"))
    if kind == "foreign-key":
        reference = re.search(
            r'REFERENCES\s+(?P<table>"(?:""|[^"])+"|[A-Za-z_][A-Za-z0-9_]*)\s*'
            r"\((?P<columns>[^()]*)\)(?P<actions>.*)$",
            current,
            re.IGNORECASE | re.DOTALL,
        )
        if reference is None:
            raise StateContractError(f"cannot parse SQL foreign key: {value}")
        result["references"] = {
            "table": _identifier(reference.group("table")),
            "columns": _identifier_list(reference.group("columns")),
            **(
                {"actions": reference.group("actions").strip()}
                if reference.group("actions").strip()
                else {}
            ),
        }
    elif kind == "check":
        result["expression"] = current[len(kind_match.group(0)) :].strip()
    return result


def relational_schema(ddl: object, *, dialect: str) -> dict[str, object]:
    """Return the semantic relational structure from one canonical DDL authority."""

    tables: list[dict[str, object]] = []
    unique_indexes: list[dict[str, object]] = []
    for raw_statement in _statements(ddl):
        statement = raw_statement.strip()
        table_match = re.fullmatch(
            r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+"
            r'(?P<name>"(?:""|[^"])+"|[A-Za-z_][A-Za-z0-9_]*)\s*'
            r"\((?P<body>.*)\)",
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        if table_match is not None:
            columns: list[dict[str, object]] = []
            constraints: list[dict[str, object]] = []
            for definition in _split_top_level(table_match.group("body"), ","):
                if re.match(
                    r"(?:CONSTRAINT\s+\S+\s+)?(?:PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|CHECK)\b",
                    definition,
                    re.IGNORECASE,
                ):
                    constraints.append(_table_constraint(definition))
                else:
                    columns.append(_column(definition))
            tables.append(
                {
                    "name": _identifier(table_match.group("name")),
                    "columns": columns,
                    "constraints": constraints,
                }
            )
            continue
        unique_match = re.fullmatch(
            r"CREATE\s+UNIQUE\s+INDEX\s+"
            r'(?P<name>"(?:""|[^"])+"|[A-Za-z_][A-Za-z0-9_]*)\s+ON\s+'
            r'(?P<table>"(?:""|[^"])+"|[A-Za-z_][A-Za-z0-9_]*)\s*'
            r"\((?P<columns>.*)\)",
            statement,
            re.IGNORECASE | re.DOTALL,
        )
        if unique_match is not None:
            unique_indexes.append(
                {
                    "name": _identifier(unique_match.group("name")),
                    "table": _identifier(unique_match.group("table")),
                    "columns": _identifier_list(unique_match.group("columns")),
                    "definition": statement,
                }
            )
            continue
        if re.match(r"CREATE\s+INDEX\b", statement, re.IGNORECASE):
            continue
        if re.fullmatch(r"PRAGMA\s+user_version\s*=\s*\d+", statement, re.IGNORECASE):
            continue
        if re.match(r"(?:INSERT|UPDATE|DELETE)\b", statement, re.IGNORECASE):
            # Migration data movement is transition behavior, not an ongoing schema default.
            continue
        raise StateContractError(f"unsupported SQL authority statement: {statement[:120]}")
    if not tables:
        raise StateContractError("SQL authority defines no tables")
    table_names = [str(table["name"]) for table in tables]
    if len(table_names) != len(set(table_names)):
        raise StateContractError("SQL authority defines a table more than once")
    return {
        "kind": "relational-schema",
        "dialect": dialect,
        "tables": tables,
        "unique_indexes": unique_indexes,
    }


def _model_schema(value: object, *, identity: str) -> dict[str, object]:
    if not isinstance(value, type) or not issubclass(value, BaseModel):
        raise StateContractError(f"durable document is not a Pydantic model: {identity}")
    schema = value.model_json_schema(mode="validation")
    Draft202012Validator.check_schema(schema)
    return {"id": identity, "schema": schema}


def _normalize_component_contract(value: object) -> object:
    if isinstance(value, Mapping):
        current = {str(key): _normalize_component_contract(item) for key, item in value.items()}
        if current.get("kind") == "sql-ddl":
            dialect = current.get("dialect")
            ddl = current.get("ddl")
            if not isinstance(dialect, str):
                raise StateContractError("component SQL state lacks a dialect")
            projected = relational_schema(ddl, dialect=dialect)
            return {
                **{key: item for key, item in current.items() if key not in {"kind", "ddl"}},
                **projected,
            }
        return current
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize_component_contract(item) for item in value]
    return copy.deepcopy(value)


def project_owner(owner: Mapping[str, object]) -> dict[str, object]:
    """Resolve one release declaration to its exact component-owned state structure."""

    descriptor = owner.get("structure")
    if not isinstance(descriptor, Mapping):
        raise StateContractError(f"durable state owner lacks structure: {owner.get('id')}")
    if not isinstance(descriptor.get("module"), str):
        raise StateContractError(f"durable state owner lacks structure module: {owner.get('id')}")
    module = importlib.import_module(str(descriptor["module"]))
    kind = descriptor.get("kind")
    structure: Mapping[str, object]
    if kind == "sql-ddl":
        if set(descriptor) != {"kind", "module", "symbol"}:
            raise StateContractError(f"SQL structure descriptor is not exact: {owner.get('id')}")
        symbol = descriptor.get("symbol")
        if not isinstance(symbol, str) or not hasattr(module, symbol):
            raise StateContractError(f"SQL structure symbol is missing: {owner.get('id')}")
        format_value = str(owner.get("format", ""))
        dialect = format_value.rsplit("/", 1)[-1]
        structure = relational_schema(getattr(module, symbol), dialect=dialect)
    elif kind == "pydantic-models":
        if set(descriptor) != {"kind", "module", "symbols"}:
            raise StateContractError(
                f"Pydantic structure descriptor is not exact: {owner.get('id')}"
            )
        symbols = descriptor.get("symbols")
        if (
            not isinstance(symbols, list)
            or not symbols
            or not all(isinstance(symbol, str) and hasattr(module, symbol) for symbol in symbols)
        ):
            raise StateContractError(f"Pydantic structure symbols are invalid: {owner.get('id')}")
        structure = {
            "kind": "json-documents",
            "documents": [
                _model_schema(getattr(module, symbol), identity=str(symbol)) for symbol in symbols
            ],
        }
    elif kind == "python-contract":
        if set(descriptor) != {"kind", "module", "symbol"}:
            raise StateContractError(f"Python structure descriptor is not exact: {owner.get('id')}")
        symbol = descriptor.get("symbol")
        factory = getattr(module, str(symbol), None)
        if not callable(factory):
            raise StateContractError(f"Python structure factory is missing: {owner.get('id')}")
        projected = _normalize_component_contract(cast(Any, factory)())
        if not isinstance(projected, Mapping):
            raise StateContractError(
                f"Python structure factory returned no mapping: {owner.get('id')}"
            )
        structure = projected
    else:
        raise StateContractError(f"unknown durable structure kind: {owner.get('id')}: {kind}")
    return {
        key: copy.deepcopy(value)
        for key, value in owner.items()
        if key not in {"fixtures", "structure"}
    } | {"structure": structure}


def declaration_symbols(owner: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    descriptor = cast(Mapping[str, object], owner["structure"])
    module = str(descriptor["module"])
    if descriptor["kind"] == "pydantic-models":
        return tuple(
            (module, str(symbol)) for symbol in cast(Sequence[object], descriptor["symbols"])
        )
    return ((module, str(descriptor["symbol"])),)

"""Source-owned, dimensionally checked growth declarations and primitive bounds."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

from riverhog_canonical_json import canonical_json_bytes

Unit = tuple[tuple[str, int], ...]
SCALAR: Unit = ()
COUNT: Unit = (("count", 1),)
OCTETS: Unit = (("octets", 1),)
OCTETS_PER_COUNT: Unit = (("count", -1), ("octets", 1))
FORMAT = "riverhog-growth-declarations/v1"
COST_MODEL = "riverhog-primitive-growth-costs/v1"
_NAME = re.compile(r"[a-z][a-z0-9_-]*\Z")
F = TypeVar("F", bound=Callable[..., Any])


def _unit(value: Unit) -> Unit:
    if (
        not isinstance(value, tuple)
        or any(
            not isinstance(term, tuple)
            or len(term) != 2
            or not isinstance(term[0], str)
            or not _NAME.fullmatch(term[0])
            or type(term[1]) is not int
            or term[1] == 0
            for term in value
        )
        or tuple(sorted(value)) != value
        or len({name for name, _ in value}) != len(value)
    ):
        raise ValueError("growth units must be normalized named integer powers")
    return value


def _product_units(values: tuple[Unit, ...]) -> Unit:
    powers: dict[str, int] = {}
    for value in values:
        for name, power in value:
            powers[name] = powers.get(name, 0) + power
    return tuple(sorted((name, power) for name, power in powers.items() if power))


@dataclass(frozen=True, slots=True)
class Expression:
    """A closed arithmetic tree; expressions never execute strings or callbacks."""

    op: Literal["constant", "dimension", "sum", "product", "maximum", "log2_ceil", "delegate"]
    terms: tuple[Expression, ...] = ()
    name: str = ""
    value: int = 0
    unit: Unit = SCALAR
    metric: str = ""
    arguments: tuple[tuple[str, Expression], ...] = ()

    def __post_init__(self) -> None:
        _unit(self.unit)
        if type(self.value) is not int or self.value < 0:
            raise ValueError("growth constants must be nonnegative integers")
        if not isinstance(self.terms, tuple) or any(
            not isinstance(term, Expression) for term in self.terms
        ):
            raise ValueError("growth operands must be typed expressions")
        if (
            not isinstance(self.arguments, tuple)
            or any(
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
                or not _NAME.fullmatch(item[0])
                or not isinstance(item[1], Expression)
                for item in self.arguments
            )
            or len({name for name, _ in self.arguments}) != len(self.arguments)
        ):
            raise ValueError("growth delegate arguments must have unique dimension names")
        if self.op == "constant":
            valid = not (self.terms or self.name or self.metric or self.arguments)
        elif self.op == "dimension":
            valid = bool(_NAME.fullmatch(self.name)) and not (
                self.terms or self.value or self.unit or self.metric or self.arguments
            )
        elif self.op in {"sum", "product", "maximum"}:
            valid = len(self.terms) >= 2 and not (
                self.name or self.value or self.unit or self.metric or self.arguments
            )
        elif self.op == "log2_ceil":
            valid = len(self.terms) == 1 and not (
                self.name or self.value or self.unit or self.metric or self.arguments
            )
        elif self.op == "delegate":
            valid = (
                bool(_NAME.fullmatch(self.name))
                and bool(_NAME.fullmatch(self.metric))
                and len(self.terms) == 1
                and not (self.value or self.unit)
            )
        else:
            valid = False
        if not valid:
            raise ValueError("growth expression has unknown or contradictory fields")

    def project(self) -> dict[str, object]:
        if self.op == "constant":
            return {"op": self.op, "value": self.value, "unit": [list(t) for t in self.unit]}
        if self.op == "dimension":
            return {"op": self.op, "name": self.name}
        if self.op == "delegate":
            return {
                "op": self.op,
                "name": self.name,
                "metric": self.metric,
                "multiplicity": self.terms[0].project(),
                "arguments": {name: term.project() for name, term in self.arguments},
            }
        return {"op": self.op, "terms": [term.project() for term in self.terms]}


@dataclass(frozen=True, slots=True)
class Dimension:
    name: str
    unit: Unit
    meaning: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or not _NAME.fullmatch(self.name)
            or not isinstance(self.meaning, str)
            or not self.meaning
        ):
            raise ValueError("growth dimensions require exact counting meanings")
        _unit(self.unit)


@dataclass(frozen=True, slots=True)
class Bound:
    metric: str
    unit: Unit
    relation: Literal["equal", "upper", "big_o"]
    expression: Expression
    meaning: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.metric, str)
            or not _NAME.fullmatch(self.metric)
            or self.relation not in {"equal", "upper", "big_o"}
            or not isinstance(self.meaning, str)
            or not self.meaning
            or not isinstance(self.expression, Expression)
        ):
            raise ValueError("growth bounds require a typed metric, relation and meaning")
        _unit(self.unit)


@dataclass(frozen=True, slots=True)
class Declaration:
    id: str
    authority: str
    subjects: tuple[str, ...]
    scope: str
    starts_at: str
    ends_at: str
    dimensions: tuple[Dimension, ...]
    bounds: tuple[Bound, ...]
    conditions: tuple[str, ...]
    invariants: tuple[str, ...]
    limitations: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            any(
                not isinstance(value, str) or not value
                for value in (self.id, self.authority, self.scope, self.starts_at, self.ends_at)
            )
            or not re.fullmatch(r"[a-z][a-z0-9._/-]+/v[1-9][0-9]*", self.id)
            or not self.authority
            or not isinstance(self.subjects, tuple)
            or not self.subjects
            or len(set(self.subjects)) != len(self.subjects)
            or any(
                not isinstance(subject, str)
                or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+", subject)
                for subject in self.subjects
            )
            or not self.scope
            or not self.starts_at
            or not self.ends_at
            or not self.dimensions
            or not self.bounds
            or not isinstance(self.dimensions, tuple)
            or not isinstance(self.bounds, tuple)
            or any(not isinstance(d, Dimension) for d in self.dimensions)
            or any(not isinstance(b, Bound) for b in self.bounds)
            or len({d.name for d in self.dimensions}) != len(self.dimensions)
            or len({b.metric for b in self.bounds}) != len(self.bounds)
            or any(
                not isinstance(values, tuple)
                or not values
                or any(not isinstance(v, str) or not v for v in values)
                for values in (self.conditions, self.invariants, self.limitations)
            )
        ):
            raise ValueError("growth declaration lacks unique owned dimensions or completion scope")

    def project(self) -> dict[str, object]:
        return {
            "id": self.id,
            "authority": self.authority,
            "subjects": list(self.subjects),
            "cost_model": COST_MODEL,
            "scope": {"kind": self.scope, "starts_at": self.starts_at, "ends_at": self.ends_at},
            "dimensions": {
                d.name: {"unit": [list(t) for t in d.unit], "meaning": d.meaning}
                for d in self.dimensions
            },
            "bounds": {
                b.metric: {
                    "unit": [list(t) for t in b.unit],
                    "relation": b.relation,
                    "mode": "worst-case",
                    "expression": b.expression.project(),
                    "meaning": b.meaning,
                }
                for b in self.bounds
            },
            "conditions": list(self.conditions),
            "invariants": list(self.invariants),
            "limitations": list(self.limitations),
        }


@dataclass(frozen=True, slots=True)
class Binding:
    declaration: Declaration
    operation: Callable[..., Any]
    delegates: tuple[tuple[str, Callable[..., Any]], ...]

    def project(self) -> dict[str, object]:
        return {
            **self.declaration.project(),
            "delegates": {
                name: {
                    "declaration": growth_binding(operation).declaration.id,
                }
                for name, operation in self.delegates
            },
        }


def growth_binding(operation: Callable[..., Any]) -> Binding:
    value = getattr(operation, "__riverhog_growth__", None)
    if not isinstance(value, Binding) or value.operation is not operation:
        raise ValueError("operation has no exact source-owned growth binding")
    return value


@dataclass(slots=True)
class _Context:
    declaration: Declaration
    delegates: dict[str, _Context]


def _source_context(binding: Binding, stack: tuple[str, ...] = ()) -> _Context:
    identity = binding.declaration.id
    if identity in stack:
        raise ValueError("growth composition has cyclic delegates")
    return _Context(
        binding.declaration,
        {
            name: _source_context(growth_binding(operation), (*stack, identity))
            for name, operation in binding.delegates
        },
    )


def _expression_unit(
    expression: Expression, context: _Context, stack: tuple[str, ...], relation: str
) -> Unit:
    declaration = context.declaration
    dimensions = {d.name: d.unit for d in declaration.dimensions}
    if expression.op == "constant":
        return expression.unit
    if expression.op == "dimension":
        if expression.name not in dimensions:
            raise ValueError("growth expression references an unknown dimension")
        return dimensions[expression.name]
    units = tuple(_expression_unit(term, context, stack, relation) for term in expression.terms)
    if expression.op == "product":
        return _product_units(units)
    if expression.op in {"sum", "maximum"}:
        if len(set(units)) != 1:
            raise ValueError("growth expression adds incompatible dimensions")
        return units[0]
    if expression.op == "log2_ceil":
        if units[0] not in (SCALAR, COUNT):
            raise ValueError("growth logarithm requires a scalar or count")
        return SCALAR
    if expression.name not in context.delegates or units != (COUNT,):
        raise ValueError("growth delegate lacks an owned call-count binding")
    child = context.delegates[expression.name]
    if child.declaration.id in stack:
        raise ValueError("growth composition has cyclic delegates")
    arguments = dict(expression.arguments)
    child_dimensions = {d.name: d.unit for d in child.declaration.dimensions}
    if set(arguments) != set(child_dimensions) or any(
        _expression_unit(arguments[name], context, stack, "equal") != unit
        for name, unit in child_dimensions.items()
    ):
        raise ValueError("growth delegate dimension mapping is incomplete or incompatible")
    bounds = {b.metric: b for b in child.declaration.bounds}
    if expression.metric not in bounds:
        raise ValueError("growth delegate metric is not declared")
    bound = bounds[expression.metric]
    if (relation == "equal" and bound.relation != "equal") or (
        relation == "upper" and bound.relation == "big_o"
    ):
        raise ValueError("growth delegate cannot establish the stronger parent relation")
    if (
        _expression_unit(bound.expression, child, (*stack, child.declaration.id), bound.relation)
        != bound.unit
    ):
        raise ValueError("growth delegate expression contradicts its metric unit")
    return bound.unit


def declare_growth(
    declaration: Declaration, *, delegates: tuple[tuple[str, Callable[..., Any]], ...] = ()
) -> Callable[[F], F]:
    """Attach declaration data to its exact function without wrapping execution."""

    if not isinstance(declaration, Declaration) or not isinstance(delegates, tuple):
        raise ValueError("growth binding requires typed source-owned declaration data")
    if len(dict(delegates)) != len(delegates) or any(
        not _NAME.fullmatch(name) or not callable(operation) for name, operation in delegates
    ):
        raise ValueError("growth delegates require unique callable bindings")

    def attach(operation: F) -> F:
        if hasattr(operation, "__riverhog_growth__"):
            raise ValueError("operation has duplicate growth declarations")
        binding = Binding(declaration, operation, delegates)
        context = _source_context(binding)
        for bound in declaration.bounds:
            if (
                _expression_unit(bound.expression, context, (declaration.id,), bound.relation)
                != bound.unit
            ):
                raise ValueError("growth bound expression contradicts its metric unit")
        vars(operation)["__riverhog_growth__"] = binding
        return operation

    return attach


def evaluate_bound(binding: Binding, metric: str, dimensions: Mapping[str, int]) -> int:
    """Evaluate primitive bounds with the same model used for standalone records."""

    context = _source_context(binding)
    bounds = {bound.metric: bound for bound in binding.declaration.bounds}
    if metric not in bounds:
        raise ValueError("growth metric is not declared")
    bound = bounds[metric]
    if (
        _expression_unit(bound.expression, context, (binding.declaration.id,), bound.relation)
        != bound.unit
    ):
        raise ValueError("growth metric expression has incompatible units")
    return _evaluate(context, metric, dimensions)


def project_bindings(bindings: tuple[Binding, ...]) -> dict[str, object]:
    """Project exact reachable source declarations once, without a second registry."""

    result: dict[str, object] = {}
    owners: dict[str, Callable[..., Any]] = {}
    pending = list(bindings)
    while pending:
        binding = pending.pop()
        identity = binding.declaration.id
        if identity in owners:
            if owners[identity] is not binding.operation:
                raise ValueError("growth declaration identity has multiple source owners")
            continue
        owners[identity] = binding.operation
        result[identity] = binding.project()
        pending.extend(growth_binding(operation) for _, operation in binding.delegates)
    projection: dict[str, object] = {
        "format": FORMAT,
        "cost_model": COST_MODEL,
        "declarations": result,
    }
    validate_projection(projection)
    return projection


def _fields(value: object, expected: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError("growth data has missing, unknown or contradictory fields")
    return value


def _read_unit(value: object) -> Unit:
    if not isinstance(value, list) or any(not isinstance(t, list) or len(t) != 2 for t in value):
        raise ValueError("growth unit data must contain named integer powers")
    return _unit(tuple((t[0], t[1]) for t in value))


def _read_expression(value: object, budget: list[int], *, depth: int = 0) -> Expression:
    budget[0] -= 1
    if budget[0] < 0 or depth > 64 or not isinstance(value, Mapping):
        raise ValueError("growth expression exceeds the bounded metadata parsing budget")
    op = value.get("op")
    if op == "constant":
        row = _fields(value, {"op", "value", "unit"})
        return Expression("constant", value=row["value"], unit=_read_unit(row["unit"]))
    if op == "dimension":
        row = _fields(value, {"op", "name"})
        return Expression("dimension", name=row["name"])
    if op == "delegate":
        row = _fields(value, {"op", "name", "metric", "multiplicity", "arguments"})
        if not isinstance(row["arguments"], Mapping):
            raise ValueError("growth delegate argument data must be a dimension mapping")
        return Expression(
            "delegate",
            name=row["name"],
            metric=row["metric"],
            terms=(_read_expression(row["multiplicity"], budget, depth=depth + 1),),
            arguments=tuple(
                (name, _read_expression(term, budget, depth=depth + 1))
                for name, term in sorted(row["arguments"].items())
            ),
        )
    if op not in {"sum", "product", "maximum", "log2_ceil"}:
        raise ValueError("growth expression data has an unknown operator")
    row = _fields(value, {"op", "terms"})
    if not isinstance(row["terms"], list):
        raise ValueError("growth expression operand data must be a list")
    return Expression(
        op,
        terms=tuple(_read_expression(term, budget, depth=depth + 1) for term in row["terms"]),
    )


def _projection_contexts(projection: object) -> dict[str, _Context]:
    root = _fields(projection, {"format", "cost_model", "declarations"})
    if root["format"] != FORMAT or root["cost_model"] != COST_MODEL:
        raise ValueError("growth projection has an unknown format or cost model")
    rows = root["declarations"]
    if not isinstance(rows, Mapping) or len(rows) > 4096:
        raise ValueError("growth declaration data exceeds the internal metadata budget")
    contexts: dict[str, _Context] = {}
    links: dict[str, dict[str, str]] = {}
    budget = [65536]
    for identity, value in rows.items():
        row = _fields(
            value,
            {
                "id",
                "authority",
                "subjects",
                "cost_model",
                "scope",
                "dimensions",
                "bounds",
                "conditions",
                "invariants",
                "limitations",
                "delegates",
            },
        )
        if row["id"] != identity or row["cost_model"] != COST_MODEL:
            raise ValueError("growth declaration key or cost model differs from its owner")
        scope = _fields(row["scope"], {"kind", "starts_at", "ends_at"})
        if not isinstance(row["dimensions"], Mapping) or not isinstance(row["bounds"], Mapping):
            raise ValueError("growth dimensions and metrics require exact named mappings")
        dimensions = []
        for name, data in row["dimensions"].items():
            dimension = _fields(data, {"unit", "meaning"})
            dimensions.append(Dimension(name, _read_unit(dimension["unit"]), dimension["meaning"]))
        bounds = []
        for name, data in row["bounds"].items():
            bound = _fields(data, {"unit", "relation", "mode", "expression", "meaning"})
            if bound["mode"] != "worst-case":
                raise ValueError("growth metric has an unestablished mode")
            bounds.append(
                Bound(
                    name,
                    _read_unit(bound["unit"]),
                    bound["relation"],
                    _read_expression(bound["expression"], budget),
                    bound["meaning"],
                )
            )
        for name in ("subjects", "conditions", "invariants", "limitations"):
            if not isinstance(row[name], list) or any(not isinstance(v, str) for v in row[name]):
                raise ValueError("growth scope data requires explicit strings")
        declaration = Declaration(
            id=identity,
            authority=row["authority"],
            subjects=tuple(row["subjects"]),
            scope=scope["kind"],
            starts_at=scope["starts_at"],
            ends_at=scope["ends_at"],
            dimensions=tuple(dimensions),
            bounds=tuple(bounds),
            conditions=tuple(row["conditions"]),
            invariants=tuple(row["invariants"]),
            limitations=tuple(row["limitations"]),
        )
        contexts[identity] = _Context(declaration, {})
        if not isinstance(row["delegates"], Mapping):
            raise ValueError("growth delegates require exact named declarations")
        references = {}
        for name, data in row["delegates"].items():
            delegate = _fields(data, {"declaration"})
            if not isinstance(name, str) or not _NAME.fullmatch(name):
                raise ValueError("growth delegate name is invalid")
            references[name] = delegate["declaration"]
        links[identity] = references
    for identity, references in links.items():
        if any(
            not isinstance(target, str) or target not in contexts for target in references.values()
        ):
            raise ValueError("growth composition has an unresolved delegate")
        contexts[identity].delegates.update(
            {name: contexts[target] for name, target in references.items()}
        )

    heights: dict[str, int] = {}

    def visit(context: _Context, stack: tuple[str, ...]) -> int:
        identity = context.declaration.id
        if identity in stack or len(stack) >= 64:
            raise ValueError("growth composition is cyclic or exceeds the reader depth budget")
        if identity not in heights:
            heights[identity] = 1 + max(
                (visit(child, (*stack, identity)) for child in context.delegates.values()),
                default=0,
            )
        if heights[identity] + len(stack) > 64:
            raise ValueError("growth composition exceeds the reader depth budget")
        return heights[identity]

    subjects: set[str] = set()
    for context in contexts.values():
        visit(context, ())
        if subjects.intersection(context.declaration.subjects):
            raise ValueError("growth public subject has multiple declaration owners")
        subjects.update(context.declaration.subjects)
    for identity, context in contexts.items():
        for metric_bound in context.declaration.bounds:
            if (
                _expression_unit(
                    metric_bound.expression, context, (identity,), metric_bound.relation
                )
                != metric_bound.unit
            ):
                raise ValueError("growth projected metric has incompatible units")
    return contexts


def validate_projection(projection: object) -> None:
    """Validate standalone declaration data through the same typed expression model."""

    _projection_contexts(projection)


def validate_sample(
    projection: object,
    declaration_id: str,
    dimensions: Mapping[str, int],
    metrics: Mapping[str, int],
) -> None:
    """Check actual primitive observations; no source, time or success claim is inferred."""

    contexts = _projection_contexts(projection)
    if declaration_id not in contexts:
        raise ValueError("growth sample has no declared operation")
    _validate_sample(contexts[declaration_id], dimensions, metrics)


def _validate_sample(
    context: _Context, dimensions: Mapping[str, int], metrics: Mapping[str, int]
) -> None:
    if set(metrics) != {b.metric for b in context.declaration.bounds} or any(
        type(value) is not int or value < 0 for value in metrics.values()
    ):
        raise ValueError("growth sample has missing or malformed primitive observations")
    for bound in context.declaration.bounds:
        if bound.relation == "big_o":
            raise ValueError(
                "asymptotic arguments require separate proof rather than a numeric sample"
            )
        expected = _evaluate(context, bound.metric, dimensions)
        if (bound.relation == "equal" and metrics[bound.metric] != expected) or (
            bound.relation == "upper" and metrics[bound.metric] > expected
        ):
            raise ValueError("actual primitive observations violate the declared growth bound")


def validate_execution_samples(projection: object, declaration_id: str, samples: object) -> None:
    """Validate complete primitive samples and the actual named delegate invocations.

    Counts establish only the declared successful-call scope. Clean source and
    assertion execution must be verified by the evidence owner separately.
    Each root has its own bounded reader budget; every root is checked.
    """
    contexts = _projection_contexts(projection)
    rows = _fields(projection, {"format", "cost_model", "declarations"})["declarations"]

    def check(identity: str, sample: object, depth: int, budget: list[int]) -> None:
        budget[0] -= 1
        if depth > 64 or budget[0] < 0 or identity not in contexts:
            raise ValueError("growth sample exceeds the reader budget or has no declared owner")
        row = _fields(
            sample, {"declaration", "declaration_sha256", "dimensions", "metrics", "delegates"}
        )
        if (
            row["declaration"] != identity
            or row["declaration_sha256"]
            != hashlib.sha256(canonical_json_bytes(rows[identity])).hexdigest()
        ):
            raise ValueError("growth sample has a stale or mismatched declaration digest")
        if not isinstance(row["dimensions"], Mapping) or not isinstance(row["metrics"], Mapping):
            raise ValueError("growth sample requires actual dimensions and primitive observations")
        context = contexts[identity]
        _validate_sample(context, row["dimensions"], row["metrics"])
        delegates = row["delegates"]
        if not isinstance(delegates, Mapping) or set(delegates) != set(context.delegates):
            raise ValueError("growth sample omits declared delegate execution")
        for name, child in context.delegates.items():
            child_samples = delegates[name]
            if not isinstance(child_samples, list):
                raise ValueError("growth delegate samples require actual invocation records")
            for child_sample in child_samples:
                check(child.declaration.id, child_sample, depth + 1, budget)

        def composition(expression: Expression) -> None:
            if expression.op == "delegate":
                child_samples = delegates[expression.name]
                count = _evaluate_expression(expression.terms[0], context, row["dimensions"])
                arguments = {
                    name: _evaluate_expression(term, context, row["dimensions"])
                    for name, term in expression.arguments
                }
                if len(child_samples) != count or any(
                    sample["dimensions"] != arguments for sample in child_samples
                ):
                    raise ValueError("actual delegate calls contradict the declared composition")
            for term in expression.terms:
                composition(term)
            for _, argument in expression.arguments:
                composition(argument)

        for bound in context.declaration.bounds:
            composition(bound.expression)

    if not isinstance(samples, list) or not samples:
        raise ValueError("growth evidence has no completed primitive samples")
    for sample in samples:
        check(declaration_id, sample, 0, [65536])


def _evaluate(context: _Context, metric: str, dimensions: Mapping[str, int]) -> int:
    if set(dimensions) != {d.name for d in context.declaration.dimensions} or any(
        type(value) is not int or value < 0 for value in dimensions.values()
    ):
        raise ValueError("growth dimensions must be complete nonnegative integer observations")
    bounds = {bound.metric: bound for bound in context.declaration.bounds}
    if metric not in bounds:
        raise ValueError("growth metric is not declared")
    bound = bounds[metric]
    if bound.relation == "big_o":
        raise ValueError("asymptotic arguments are not numerical acceptance budgets")

    return _evaluate_expression(bound.expression, context, dimensions)


def _evaluate_expression(
    expression: Expression, context: _Context, dimensions: Mapping[str, int]
) -> int:
    def evaluate(expression: Expression) -> int:
        if expression.op == "constant":
            return expression.value
        if expression.op == "dimension":
            return dimensions[expression.name]
        values = tuple(evaluate(term) for term in expression.terms)
        if expression.op == "sum":
            return sum(values)
        if expression.op == "product":
            result = 1
            for value in values:
                result *= value
            return result
        if expression.op == "maximum":
            return max(values)
        if expression.op == "log2_ceil":
            if values[0] < 1:
                raise ValueError("growth logarithm requires a positive argument")
            return (values[0] - 1).bit_length()
        child = context.delegates[expression.name]
        arguments = {name: evaluate(term) for name, term in expression.arguments}
        return values[0] * _evaluate(child, expression.metric, arguments)

    return evaluate(expression)


__all__ = [
    "Binding",
    "Bound",
    "COUNT",
    "COST_MODEL",
    "Declaration",
    "Dimension",
    "Expression",
    "FORMAT",
    "OCTETS",
    "OCTETS_PER_COUNT",
    "SCALAR",
    "declare_growth",
    "evaluate_bound",
    "growth_binding",
    "project_bindings",
    "validate_execution_samples",
    "validate_projection",
    "validate_sample",
]

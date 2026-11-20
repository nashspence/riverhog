"""Source-owned, dimensionally checked growth declarations and primitive bounds."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Literal, TypeVar, cast

from riverhog_canonical_json import canonical_json_bytes, parse_identity_json

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


def _product_units(values: tuple[Unit, ...], *, _work: _NumericWork | None = None) -> Unit:
    if _work is not None:
        _work.unit_values(values, "unit_product")
    powers: dict[str, int] = {}
    for value in values:
        for name, power in value:
            if _work is None:
                powers[name] = powers.get(name, 0) + power
            else:
                previous = powers.get(name, 0)
                _work.addition((previous, power))
                powers[name] = previous + power
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

    def project(self, *, _work: _NumericWork | None = None) -> dict[str, object]:
        if _work is not None:
            _work.expression_record(self, "source_projection", source=True)
            _work.admission_nodes += 1
        if self.op == "constant":
            if _work is not None:
                _work.source_record(self, Expression, "source_projection_constant", retain=True)
            return {
                "op": self.op,
                "value": self.value,
                "unit": [list(t) for t in _record_tuple(self, Expression, "unit", _work)],
            }
        if self.op == "dimension":
            if _work is not None:
                _work.source_record(self, Expression, "source_projection_dimension", retain=True)
            return {"op": self.op, "name": self.name}
        if self.op == "delegate":
            return {
                "op": self.op,
                "name": self.name,
                "metric": self.metric,
                "multiplicity": (
                    self.terms[0].project()
                    if _work is None
                    else Expression.project(
                        _record_tuple(self, Expression, "terms", _work)[0], _work=_work
                    )
                ),
                "arguments": {
                    name: term.project() if _work is None else Expression.project(term, _work=_work)
                    for name, term in _record_tuple(self, Expression, "arguments", _work)
                },
            }
        return {
            "op": self.op,
            "terms": [
                term.project() if _work is None else Expression.project(term, _work=_work)
                for term in _record_tuple(self, Expression, "terms", _work)
            ],
        }


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

    def project(self, *, _work: _NumericWork | None = None) -> dict[str, object]:
        if _work is not None:
            _work.declaration_record(self)
        return {
            "id": self.id,
            "authority": self.authority,
            "subjects": list(_record_tuple(self, Declaration, "subjects", _work)),
            "cost_model": COST_MODEL,
            "scope": {"kind": self.scope, "starts_at": self.starts_at, "ends_at": self.ends_at},
            "dimensions": {
                (d.name if _work is None else _dimension_name(d, _work)): {
                    "unit": [list(t) for t in d.unit],
                    "meaning": d.meaning,
                }
                for d in _record_tuple(self, Declaration, "dimensions", _work)
            },
            "bounds": {
                (b.metric if _work is None else _metric_name(b, _work)): {
                    "unit": [list(t) for t in b.unit],
                    "relation": b.relation,
                    "mode": "worst-case",
                    "expression": (
                        b.expression.project()
                        if _work is None
                        else Expression.project(b.expression, _work=_work)
                    ),
                    "meaning": b.meaning,
                }
                for b in _record_tuple(self, Declaration, "bounds", _work)
            },
            "conditions": list(_record_tuple(self, Declaration, "conditions", _work)),
            "invariants": list(_record_tuple(self, Declaration, "invariants", _work)),
            "limitations": list(_record_tuple(self, Declaration, "limitations", _work)),
        }


@dataclass(frozen=True, slots=True)
class Binding:
    declaration: Declaration
    operation: Callable[..., Any]
    delegates: tuple[tuple[str, Callable[..., Any]], ...]

    def project(self, *, _work: _NumericWork | None = None) -> dict[str, object]:
        if _work is not None:
            _work.binding_record(self)
        return {
            **(
                self.declaration.project()
                if _work is None
                else Declaration.project(self.declaration, _work=_work)
            ),
            "delegates": {
                name: {
                    "declaration": growth_binding(operation).declaration.id,
                }
                for name, operation in _record_tuple(self, Binding, "delegates", _work)
            },
        }


def growth_binding(operation: Callable[..., Any]) -> Binding:
    value = getattr(operation, "__riverhog_growth__", None)
    if not isinstance(value, Binding) or value.operation is not operation:
        raise ValueError("operation has no exact source-owned growth binding")
    return value


@dataclass(frozen=True, slots=True)
class NumericWorkPolicy:
    """Caller-owned allowance for one finite growth-reader invocation.

    Integer width is a conservative operand/result reservation, not an
    archive-size limit. Original Mapping operations and metadata admission
    retain their separate input/callback costs. Admission recursion and derived
    arithmetic share this allowance. No numerical default is inferred.
    """

    max_work: int
    max_integer_bits: int

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or value < 1 for value in (self.max_work, self.max_integer_bits)
        ):
            raise ValueError("growth numeric policy requires positive exact integer allowances")


@dataclass(frozen=True, slots=True)
class NumericWorkUsage:
    """Completed numeric accounting; carries no source or sample authority."""

    consumed: int
    payload_work: int
    meter_work: int
    debit_calls: int
    metric_calls: int
    expression_nodes: int
    composition_nodes: int
    derived_environment_entries: int
    original_environment_validations: int
    original_metric_validations: int
    maximum_operand_bits: int
    maximum_reserved_bits: int
    admission_nodes: int
    unit_nodes: int
    source_contexts: int


class NumericWorkRefused(ValueError):
    """The invocation remains unvalidated under its explicit resource policy."""

    def __init__(
        self,
        reason: str,
        *,
        operation: str,
        limit: int,
        consumed: int,
        max_integer_bits: int,
        requested: int = 0,
    ) -> None:
        # Policy integers may themselves be large; never decimal-format them
        # while receiving a refusal or replace it with a formatting failure.
        super().__init__("growth numeric work refused: " + reason)
        self.reason = reason
        self.operation = operation
        self.limit = limit
        self.consumed = consumed
        self.max_integer_bits = max_integer_bits
        self.requested = requested


@dataclass(slots=True)
class _NumericWork:
    limit: int
    max_integer_bits: int
    consumed: int = 0
    payload_work: int = 0
    debit_calls: int = 0
    metric_calls: int = 0
    expression_nodes: int = 0
    composition_nodes: int = 0
    derived_environment_entries: int = 0
    original_environment_validations: int = 0
    original_metric_validations: int = 0
    maximum_operand_bits: int = 0
    maximum_reserved_bits: int = 0
    admission_nodes: int = 0
    unit_nodes: int = 0
    source_contexts: int = 0
    source_records: dict[int, tuple[object, type[object], tuple[str, ...], tuple[object, ...]]] = (
        field(default_factory=dict)
    )
    unit_proof_entries: int = 0
    unit_proof_names: int = 0

    @classmethod
    def from_policy(cls, policy: NumericWorkPolicy) -> _NumericWork:
        if type(policy) is not NumericWorkPolicy:
            raise ValueError("growth numeric validation requires an exact work policy")
        # Copy/revalidate configuration before caller callbacks can run. The
        # invocation never rereads the policy or obtains it from evidence.
        snapshot = NumericWorkPolicy(policy.max_work, policy.max_integer_bits)
        work = cls(snapshot.max_work, snapshot.max_integer_bits)
        work.charge(1, "policy")
        return work

    def refuse(self, reason: str, operation: str, requested: int = 0) -> None:
        raise NumericWorkRefused(
            reason,
            operation=operation,
            limit=self.limit,
            consumed=self.consumed,
            max_integer_bits=self.max_integer_bits,
            requested=requested,
        )

    def charge(self, amount: int, operation: str) -> None:
        # Charge counter arithmetic from its actual prefix/request widths.
        # Comparing a wider limit stops at its digit length; equal-width
        # comparison fits this charge. Larger allowances never increase cost.
        control = 3 + max(self.consumed.bit_length(), amount.bit_length())
        cost = amount + control
        following = self.consumed + cost
        if following > self.limit:
            self.refuse("work_allowance_exhausted", operation, cost)
        self.consumed = following
        self.payload_work += amount
        self.debit_calls += 1

    def operand(self, value: object, operation: str) -> int:
        self.charge(1, operation + ".operand")
        if type(value) is not int:
            self.refuse("unsupported_numeric_operand", operation)
        assert type(value) is int
        width = max(1, int.bit_length(value))
        self.maximum_operand_bits = max(self.maximum_operand_bits, width)
        if width > self.max_integer_bits:
            self.refuse("operand_width", operation, width)
        self.charge(width, operation + ".bits")
        return width

    def reserve(self, width: int, operation: str) -> None:
        self.maximum_reserved_bits = max(self.maximum_reserved_bits, width)
        if width > self.max_integer_bits:
            self.refuse("operation_width", operation, width)

    def comparison(self, left: object, right: object, operation: str) -> None:
        a, b = self.operand(left, operation), self.operand(right, operation)
        self.charge(a + b + 1, operation + ".compare")

    def addition(self, values: tuple[int, ...]) -> None:
        widths = tuple(self.operand(value, "sum") for value in values)
        width = max(widths, default=1) + max(0, (len(values) - 1).bit_length())
        self.reserve(width, "sum")
        self.charge((len(values) + 1) * (width + 1), "sum.arithmetic")

    def multiplication(self, left: object, right: object) -> None:
        a, b = self.operand(left, "product"), self.operand(right, "product")
        self.reserve(a + b, "product")
        self.charge((a + 1) * (b + 1), "product.arithmetic")

    def maximum(self, values: tuple[int, ...]) -> None:
        widths = tuple(self.operand(value, "maximum") for value in values)
        self.charge((len(values) + 1) * (max(widths, default=1) + 1), "maximum.compare")

    def arguments(self, arguments: tuple[tuple[str, Expression], ...]) -> None:
        # Expression entry already prepays this finite metadata-name scan.
        # Arity times total key width conservatively includes collision probes.
        size = len(arguments)
        names = sum(str.__len__(name) for name, _ in arguments)
        self.charge((size + 1) * (names + size + 1), "argument_keys")

    def source_record(
        self, value: object, expected: type[object], operation: str, *, retain: bool = False
    ) -> None:
        if type(value) is not expected:
            self.refuse("unsupported_source_record", operation)
        if retain:
            names = tuple(cast(Any, expected).__dataclass_fields__)
            self.charge(1 + len(names), operation + ".shape")
            fields = tuple(object.__getattribute__(value, name) for name in names)
            # Integer identity keys have fixed host-word width. Charge the
            # fresh table's probing and amortized growth work.
            self.charge(1 + len(self.source_records), operation + ".ledger")
            prior = self.source_records.get(id(value))
            if prior is None:
                self.source_records[id(value)] = value, expected, names, fields
            elif any(old is not current for old, current in zip(prior[3], fields, strict=True)):
                self.refuse("unsupported_changed_source_record", operation)

    def verify_sources(self) -> None:
        for value, expected, names, fields in self.source_records.values():
            self.charge(1 + len(names), "source_shape_currentness")
            if type(value) is not expected or any(
                object.__getattribute__(value, name) is not original
                for name, original in zip(names, fields, strict=True)
            ):
                self.refuse("unsupported_changed_source_record", "source_shape_currentness")

    def unit_values(self, values: tuple[Unit, ...], operation: str) -> None:
        # Typed tuples are reader-owned or explicitly admitted source records.
        # Base string length does not invoke a caller's __len__. Hash/equality
        # callbacks remain the original operations and have separate costs.
        total = 0
        terms = 0
        for value in values:
            self.source_record(value, tuple, operation)
            self.charge(1 + len(value), operation + ".terms")
            terms += len(value)
            for term in value:
                self.source_record(term, tuple, operation)
                if len(term) != 2 or not isinstance(term[0], str):
                    self.refuse("unsupported_source_record", operation)
                width = self.operand(term[1], operation)
                total += 1 + str.__len__(term[0]) + width
        # Products build one powers table across every term, while unit
        # equality builds a set of whole tuples. Cover both populations.
        cardinality = len(values) + terms
        self.charge((cardinality + 1) * (total + cardinality + 1), operation + ".keys")

    def unit_proof(self, key: tuple[str, str], *, insert: bool = False) -> None:
        names = str.__len__(key[0]) + str.__len__(key[1])
        self.charge(1 + self.unit_proof_entries + self.unit_proof_names + names, "unit_proof_table")
        if insert:
            self.unit_proof_entries += 1
            self.unit_proof_names += names

    def expression_record(
        self, expression: Expression, operation: str, *, source: bool = False
    ) -> None:
        self.source_record(expression, Expression, operation, retain=source)
        self.source_record(expression.terms, tuple, operation)
        self.source_record(expression.arguments, tuple, operation)
        self.charge(1 + len(expression.terms) + len(expression.arguments), operation)
        for term in expression.terms:
            self.source_record(term, Expression, operation)
        for argument in expression.arguments:
            self.source_record(argument, tuple, operation)
            if len(argument) != 2 or not isinstance(argument[0], str):
                self.refuse("unsupported_source_record", operation)
            self.source_record(argument[1], Expression, operation)
        self.unit_values((expression.unit,), operation + ".unit")
        self.operand(expression.value, operation + ".constant")
        self.arguments(expression.arguments)

    def declaration_record(self, declaration: Declaration) -> None:
        operation = "source_declaration"
        self.source_record(declaration, Declaration, operation, retain=True)
        for values in (
            declaration.subjects,
            declaration.dimensions,
            declaration.bounds,
            declaration.conditions,
            declaration.invariants,
            declaration.limitations,
        ):
            self.source_record(values, tuple, operation)
            self.charge(1 + len(values), operation)
        for dimension in declaration.dimensions:
            self.source_record(dimension, Dimension, operation, retain=True)
            if not isinstance(dimension.name, str):
                self.refuse("unsupported_source_record", operation)
            self.unit_values((dimension.unit,), operation + ".dimension")
        for bound in declaration.bounds:
            self.source_record(bound, Bound, operation, retain=True)
            if not isinstance(bound.metric, str):
                self.refuse("unsupported_source_record", operation)
            self.source_record(bound.expression, Expression, operation)
            self.unit_values((bound.unit,), operation + ".metric")
        for names in (
            tuple(dimension.name for dimension in declaration.dimensions),
            tuple(bound.metric for bound in declaration.bounds),
        ):
            characters = sum(str.__len__(name) for name in names)
            self.charge((len(names) + 1) * (characters + len(names) + 1), operation + ".keys")

    def binding_record(self, binding: Binding) -> None:
        operation = "source_binding"
        self.source_record(binding, Binding, operation, retain=True)
        self.source_record(binding.declaration, Declaration, operation)
        self.source_record(binding.delegates, tuple, operation)
        self.charge(1 + len(binding.delegates), operation)
        for item in binding.delegates:
            self.source_record(item, tuple, operation)
            if len(item) != 2 or not isinstance(item[0], str):
                self.refuse("unsupported_source_record", operation)

    def prepare(self, context: _Context) -> None:
        if context.source_owned:
            self.declaration_record(context.declaration)
        dimensions, bounds = context.declaration.dimensions, context.declaration.bounds
        self.charge(1 + len(dimensions) + len(bounds) + len(context.delegates), "plan_context")
        dimension_names = sum(str.__len__(dimension.name) for dimension in dimensions)
        metric_names = sum(str.__len__(bound.metric) for bound in bounds)
        delegate_names = sum(str.__len__(name) for name in context.delegates)
        self.charge(dimension_names + metric_names + delegate_names, "plan_names")
        context.numeric_environment_work = (len(dimensions) + 1) * (
            dimension_names + len(dimensions) + 1
        ) + len(dimensions)
        # Underlying fresh-table key work is charged separately from any
        # original hash/equality callback bodies or callback-driven retries.
        context.numeric_metric_work = 2 * (len(bounds) + metric_names + 1)
        context.numeric_metric_validation_work = (len(bounds) + 1) * (
            metric_names + len(bounds) + 1
        )
        context.numeric_dimension_lookup_work = len(dimensions) + dimension_names + 1
        context.numeric_delegate_lookup_work = len(context.delegates) + delegate_names + 1
        context.numeric_work = self

    def attach(self, contexts: dict[str, _Context]) -> None:
        for context in contexts.values():
            self.prepare(context)

    def usage(self) -> NumericWorkUsage:
        return NumericWorkUsage(
            consumed=self.consumed,
            payload_work=self.payload_work,
            meter_work=self.consumed - self.payload_work,
            debit_calls=self.debit_calls,
            metric_calls=self.metric_calls,
            expression_nodes=self.expression_nodes,
            composition_nodes=self.composition_nodes,
            derived_environment_entries=self.derived_environment_entries,
            original_environment_validations=self.original_environment_validations,
            original_metric_validations=self.original_metric_validations,
            maximum_operand_bits=self.maximum_operand_bits,
            maximum_reserved_bits=self.maximum_reserved_bits,
            admission_nodes=self.admission_nodes,
            unit_nodes=self.unit_nodes,
            source_contexts=self.source_contexts,
        )


@dataclass(slots=True)
class _Context:
    declaration: Declaration
    delegates: dict[str, _Context]
    dimension_units: dict[str, Unit] | None = None
    bounds_by_metric: dict[str, Bound] | None = None
    numeric_dimension_names: frozenset[str] | None = None
    numeric_bounds_by_metric: dict[str, Bound] | None = None
    numeric_work: _NumericWork | None = None
    numeric_environment_work: int = 0
    numeric_metric_work: int = 0
    numeric_metric_validation_work: int = 0
    numeric_dimension_lookup_work: int = 0
    numeric_delegate_lookup_work: int = 0
    source_owned: bool = False


def _record_tuple(
    value: object, expected: type[object], name: str, work: _NumericWork | None
) -> Any:
    if work is not None:
        work.source_record(value, expected, "source_container", retain=True)
    result = getattr(value, name)
    if work is not None:
        work.source_record(result, tuple, "source_container")
    return result


def _dimension_name(dimension: Dimension, work: _NumericWork | None) -> str:
    if work is not None:
        work.source_record(dimension, Dimension, "source_dimension_key", retain=True)
    return dimension.name


def _metric_name(bound: Bound, work: _NumericWork | None) -> str:
    if work is not None:
        work.source_record(bound, Bound, "source_metric_key", retain=True)
    return bound.metric


def _dimension_table(context: _Context, *, names_only: bool = False) -> Any:
    work = context.numeric_work
    if work is not None:
        if context.source_owned:
            work.source_record(
                context.declaration, Declaration, "source_dimension_table", retain=True
            )
        work.charge(context.numeric_environment_work, "expected_dimension_table")
    source_work = work if context.source_owned else None
    if names_only:
        return {
            _dimension_name(dimension, source_work) for dimension in context.declaration.dimensions
        }
    return {
        _dimension_name(dimension, source_work): dimension.unit
        for dimension in context.declaration.dimensions
    }


def _metric_table(context: _Context) -> dict[str, Bound]:
    work = context.numeric_work
    if work is not None:
        if context.source_owned:
            work.source_record(context.declaration, Declaration, "source_metric_table", retain=True)
        work.charge(context.numeric_metric_validation_work, "expected_metric_table")
    source_work = work if context.source_owned else None
    return {_metric_name(bound, source_work): bound for bound in context.declaration.bounds}


def _source_context(
    binding: Binding, stack: tuple[str, ...] = (), *, _work: _NumericWork | None = None
) -> _Context:
    if _work is not None:
        _work.charge(1 + len(stack), "source_context")
        _work.source_contexts += 1
        _work.binding_record(binding)
        _work.declaration_record(binding.declaration)
    identity = binding.declaration.id
    if identity in stack:
        raise ValueError("growth composition has cyclic delegates")
    context = _Context(
        binding.declaration,
        {
            name: _source_context(growth_binding(operation), (*stack, identity), _work=_work)
            for name, operation in _record_tuple(binding, Binding, "delegates", _work)
        },
        source_owned=_work is not None,
    )
    if _work is not None:
        _work.prepare(context)
    return context


def _expression_unit(
    expression: Expression, context: _Context, stack: tuple[str, ...], relation: str
) -> Unit:
    work = context.numeric_work
    if work is not None:
        work.expression_record(expression, "unit_expression", source=context.source_owned)
        if context.source_owned:
            work.source_record(context.declaration, Declaration, "source_unit_context", retain=True)
        work.unit_nodes += 1
    dimensions = context.dimension_units
    if dimensions is None:
        dimensions = _dimension_table(context)
    if expression.op == "constant":
        if work is not None and context.source_owned:
            return cast(Unit, _record_tuple(expression, Expression, "unit", work))
        return expression.unit
    if expression.op == "dimension":
        if work is not None:
            if context.source_owned:
                work.source_record(expression, Expression, "source_unit_dimension", retain=True)
            work.charge(2 * context.numeric_dimension_lookup_work, "unit_dimension_lookup")
        if expression.name not in dimensions:
            raise ValueError("growth expression references an unknown dimension")
        return dimensions[expression.name]
    units = tuple(_expression_unit(term, context, stack, relation) for term in expression.terms)
    if work is not None and context.source_owned:
        work.source_record(expression, Expression, "source_unit_dispatch", retain=True)
    if expression.op == "product":
        return _product_units(units) if work is None else _product_units(units, _work=work)
    if expression.op in {"sum", "maximum"}:
        if work is not None:
            work.unit_values(units, "unit_equality")
        if len(set(units)) != 1:
            raise ValueError("growth expression adds incompatible dimensions")
        return units[0]
    if expression.op == "log2_ceil":
        if work is not None:
            work.unit_values(units, "unit_logarithm")
        if units[0] not in (SCALAR, COUNT):
            raise ValueError("growth logarithm requires a scalar or count")
        return SCALAR
    if work is not None:
        if context.source_owned:
            work.source_record(expression, Expression, "source_unit_delegate", retain=True)
        work.charge(2 * context.numeric_delegate_lookup_work + len(stack), "unit_delegate")
        work.unit_values(units, "unit_multiplicity")
    if expression.name not in context.delegates or units != (COUNT,):
        raise ValueError("growth delegate lacks an owned call-count binding")
    if work is not None and context.source_owned:
        work.source_record(expression, Expression, "source_unit_delegate", retain=True)
    child = context.delegates[expression.name]
    if child.declaration.id in stack:
        raise ValueError("growth composition has cyclic delegates")
    if work is not None:
        if context.source_owned:
            work.source_record(expression, Expression, "source_unit_arguments", retain=True)
        work.arguments(expression.arguments)
    arguments = dict(expression.arguments)
    child_dimensions = child.dimension_units
    if child_dimensions is None:
        child_dimensions = _dimension_table(child)
    if work is not None:
        work.charge(child.numeric_environment_work, "unit_argument_validation")
    if set(arguments) != set(child_dimensions) or any(
        _expression_unit(arguments[name], context, stack, "equal") != unit
        for name, unit in child_dimensions.items()
    ):
        raise ValueError("growth delegate dimension mapping is incomplete or incompatible")
    bounds = child.bounds_by_metric
    if bounds is None:
        bounds = _metric_table(child)
    if work is not None:
        if context.source_owned:
            work.source_record(expression, Expression, "source_unit_metric", retain=True)
        work.charge(child.numeric_metric_work, "unit_child_metric_lookup")
    if expression.metric not in bounds:
        raise ValueError("growth delegate metric is not declared")
    if work is not None and context.source_owned:
        work.source_record(expression, Expression, "source_unit_metric", retain=True)
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


def _literal_expression_unit(
    expression: Expression,
    context: _Context,
    relation: str,
    proven_metrics: set[tuple[str, str]],
) -> Unit:
    work = context.numeric_work
    if work is not None:
        work.expression_record(expression, "unit_expression")
        work.unit_nodes += 1
    # Only the private decoded graph uses this route. Its complete delegate
    # graph has already passed cycle/depth checks. Prove each metric body once,
    # but check every incoming use before consulting that invocation's proof.
    declaration = context.declaration
    dimensions = context.dimension_units
    if dimensions is None:
        dimensions = {d.name: d.unit for d in declaration.dimensions}
    if expression.op == "constant":
        return expression.unit
    if expression.op == "dimension":
        if work is not None:
            work.charge(2 * context.numeric_dimension_lookup_work, "unit_dimension_lookup")
        if expression.name not in dimensions:
            raise ValueError("growth expression references an unknown dimension")
        return dimensions[expression.name]
    units = tuple(
        _literal_expression_unit(term, context, relation, proven_metrics)
        for term in expression.terms
    )
    if expression.op == "product":
        return _product_units(units) if work is None else _product_units(units, _work=work)
    if expression.op in {"sum", "maximum"}:
        if work is not None:
            work.unit_values(units, "unit_equality")
        if len(set(units)) != 1:
            raise ValueError("growth expression adds incompatible dimensions")
        return units[0]
    if expression.op == "log2_ceil":
        if work is not None:
            work.unit_values(units, "unit_logarithm")
        if units[0] not in (SCALAR, COUNT):
            raise ValueError("growth logarithm requires a scalar or count")
        return SCALAR
    if work is not None:
        work.charge(2 * context.numeric_delegate_lookup_work, "unit_delegate")
        work.unit_values(units, "unit_multiplicity")
    if expression.name not in context.delegates or units != (COUNT,):
        raise ValueError("growth delegate lacks an owned call-count binding")
    child = context.delegates[expression.name]
    if work is not None:
        work.arguments(expression.arguments)
    arguments = dict(expression.arguments)
    child_dimensions = child.dimension_units
    if child_dimensions is None:
        child_dimensions = {d.name: d.unit for d in child.declaration.dimensions}
    if work is not None:
        work.charge(child.numeric_environment_work, "unit_argument_validation")
    if set(arguments) != set(child_dimensions) or any(
        _literal_expression_unit(arguments[name], context, "equal", proven_metrics) != unit
        for name, unit in child_dimensions.items()
    ):
        raise ValueError("growth delegate dimension mapping is incomplete or incompatible")
    bounds = child.bounds_by_metric
    if bounds is None:
        bounds = {b.metric: b for b in child.declaration.bounds}
    if work is not None:
        work.charge(child.numeric_metric_work, "unit_child_metric_lookup")
    if expression.metric not in bounds:
        raise ValueError("growth delegate metric is not declared")
    bound = bounds[expression.metric]
    if (relation == "equal" and bound.relation != "equal") or (
        relation == "upper" and bound.relation == "big_o"
    ):
        raise ValueError("growth delegate cannot establish the stronger parent relation")
    key = (child.declaration.id, bound.metric)
    if work is not None:
        work.unit_proof(key)
    if key not in proven_metrics:
        if (
            _literal_expression_unit(bound.expression, child, bound.relation, proven_metrics)
            != bound.unit
        ):
            raise ValueError("growth delegate expression contradicts its metric unit")
        if work is not None:
            work.unit_proof(key, insert=True)
        proven_metrics.add(key)
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


def evaluate_bound(
    binding: Binding,
    metric: str,
    dimensions: Mapping[str, int],
    *,
    numeric_policy: NumericWorkPolicy,
) -> int:
    """Evaluate primitive bounds with the same model used for standalone records."""

    work = _NumericWork.from_policy(numeric_policy)
    context = _source_context(binding, _work=work)
    bounds = _metric_table(context)
    if metric not in bounds:
        raise ValueError("growth metric is not declared")
    bound = bounds[metric]
    if (
        _expression_unit(bound.expression, context, (binding.declaration.id,), bound.relation)
        != bound.unit
    ):
        raise ValueError("growth metric expression has incompatible units")
    result = _evaluate(context, metric, dimensions)
    work.verify_sources()
    return result


def project_bindings(
    bindings: tuple[Binding, ...], *, numeric_policy: NumericWorkPolicy
) -> dict[str, object]:
    """Project exact reachable source declarations once, without a second registry."""

    work = _NumericWork.from_policy(numeric_policy)
    work.source_record(bindings, tuple, "source_roots")
    work.charge(1 + len(bindings), "source_roots")
    result: dict[str, object] = {}
    owners: dict[str, Callable[..., Any]] = {}
    pending = list(bindings)
    while pending:
        work.charge(1, "source_projection_binding")
        work.source_contexts += 1
        binding = pending.pop()
        work.binding_record(binding)
        identity = binding.declaration.id
        if identity in owners:
            if owners[identity] is not binding.operation:
                raise ValueError("growth declaration identity has multiple source owners")
            continue
        owners[identity] = binding.operation
        result[identity] = binding.project(_work=work)
        pending.extend(
            growth_binding(operation)
            for _, operation in _record_tuple(binding, Binding, "delegates", work)
        )
    projection: dict[str, object] = {
        "format": FORMAT,
        "cost_model": COST_MODEL,
        "declarations": result,
    }
    _projection_contexts(projection, _work=work)
    work.verify_sources()
    return projection


def project_binding(binding: Binding, *, numeric_policy: NumericWorkPolicy) -> dict[str, object]:
    """Project one current source row under its own explicit finite allowance.

    A consumer projecting multiple rows accounts one invocation per row; this
    result alone grants no source authorization or accepted sample authority.
    """
    work = _NumericWork.from_policy(numeric_policy)
    result = Binding.project(binding, _work=work)
    work.verify_sources()
    return result


def _fields(value: object, expected: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError("growth data has missing, unknown or contradictory fields")
    return value


def _read_unit(value: object, *, _work: _NumericWork | None = None) -> Unit:
    if _work is not None:
        _work.charge(1, "metadata_unit")
    if not isinstance(value, list) or any(not isinstance(t, list) or len(t) != 2 for t in value):
        raise ValueError("growth unit data must contain named integer powers")
    result = _unit(tuple((t[0], t[1]) for t in value))
    if _work is not None:
        _work.unit_values((result,), "metadata_unit")
    return result


def _read_expression(
    value: object, budget: list[int], *, depth: int = 0, _work: _NumericWork | None = None
) -> Expression:
    if _work is not None:
        _work.charge(1, "metadata_expression")
        _work.admission_nodes += 1
    budget[0] -= 1
    if budget[0] < 0 or depth > 64 or not isinstance(value, Mapping):
        raise ValueError("growth expression exceeds the bounded metadata parsing budget")
    op = value.get("op")
    if op == "constant":
        row = _fields(value, {"op", "value", "unit"})
        return Expression("constant", value=row["value"], unit=_read_unit(row["unit"], _work=_work))
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
            terms=(_read_expression(row["multiplicity"], budget, depth=depth + 1, _work=_work),),
            arguments=tuple(
                (name, _read_expression(term, budget, depth=depth + 1, _work=_work))
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
        terms=tuple(
            _read_expression(term, budget, depth=depth + 1, _work=_work) for term in row["terms"]
        ),
    )


def _projection_contexts(
    projection: object, *, _literal_units: bool = False, _work: _NumericWork | None = None
) -> dict[str, _Context]:
    root = _fields(projection, {"format", "cost_model", "declarations"})
    if root["format"] != FORMAT or root["cost_model"] != COST_MODEL:
        raise ValueError("growth projection has an unknown format or cost model")
    rows = root["declarations"]
    if not isinstance(rows, Mapping) or len(rows) > 4096:
        raise ValueError("growth declaration data exceeds the internal metadata budget")
    contexts: dict[str, _Context] = {}
    links: dict[str, dict[str, str]] = {}
    budget = [131072]
    for identity, value in rows.items():
        if _work is not None:
            _work.charge(1, "metadata_declaration")
            _work.admission_nodes += 1
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
            if _work is not None:
                _work.charge(1, "metadata_dimension")
            dimension = _fields(data, {"unit", "meaning"})
            dimensions.append(
                Dimension(name, _read_unit(dimension["unit"], _work=_work), dimension["meaning"])
            )
        bounds = []
        for name, data in row["bounds"].items():
            if _work is not None:
                _work.charge(1, "metadata_metric")
            bound = _fields(data, {"unit", "relation", "mode", "expression", "meaning"})
            if bound["mode"] != "worst-case":
                raise ValueError("growth metric has an unestablished mode")
            bounds.append(
                Bound(
                    name,
                    _read_unit(bound["unit"], _work=_work),
                    bound["relation"],
                    _read_expression(bound["expression"], budget, _work=_work),
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
        dimension_units = {d.name: d.unit for d in declaration.dimensions}
        bounds_by_metric = {b.metric: b for b in declaration.bounds}
        # Custom names can observe hashing and equality during numeric checks.
        contexts[identity] = _Context(
            declaration,
            {},
            dimension_units=dimension_units,
            bounds_by_metric=bounds_by_metric,
            numeric_dimension_names=(
                frozenset(dimension_units)
                if all(type(dimension.name) is str for dimension in declaration.dimensions)
                else None
            ),
            numeric_bounds_by_metric=(
                bounds_by_metric
                if all(type(bound.metric) is str for bound in declaration.bounds)
                else None
            ),
        )
        if not isinstance(row["delegates"], Mapping):
            raise ValueError("growth delegates require exact named declarations")
        references = {}
        for name, data in row["delegates"].items():
            if _work is not None:
                _work.charge(1, "metadata_delegate")
            delegate = _fields(data, {"declaration"})
            if not isinstance(name, str) or not _NAME.fullmatch(name):
                raise ValueError("growth delegate name is invalid")
            references[name] = delegate["declaration"]
        links[identity] = references
    for identity, references in links.items():
        if _work is not None:
            _work.charge(1 + len(references), "metadata_links")
        if any(
            not isinstance(target, str) or target not in contexts for target in references.values()
        ):
            raise ValueError("growth composition has an unresolved delegate")
        contexts[identity].delegates.update(
            {name: contexts[target] for name, target in references.items()}
        )
    if _work is not None:
        _work.attach(contexts)

    heights: dict[str, int] = {}

    # Explicit recursion releases the height map with this compilation.
    def visit(recurse: Callable[..., int], context: _Context, stack: tuple[str, ...]) -> int:
        if _work is not None:
            _work.charge(1 + len(stack) + len(context.delegates), "metadata_graph")
        identity = context.declaration.id
        if identity in stack or len(stack) >= 64:
            raise ValueError("growth composition is cyclic or exceeds the reader depth budget")
        if identity not in heights:
            heights[identity] = 1 + max(
                (
                    recurse(recurse, child, (*stack, identity))
                    for child in context.delegates.values()
                ),
                default=0,
            )
        if heights[identity] + len(stack) > 64:
            raise ValueError("growth composition exceeds the reader depth budget")
        return heights[identity]

    subjects: set[str] = set()
    for context in contexts.values():
        visit(visit, context, ())
        if subjects.intersection(context.declaration.subjects):
            raise ValueError("growth public subject has multiple declaration owners")
        subjects.update(context.declaration.subjects)
    if _literal_units:
        proven_metrics: set[tuple[str, str]] = set()
        for identity, context in contexts.items():
            for metric_bound in context.declaration.bounds:
                key = (identity, metric_bound.metric)
                if _work is not None:
                    _work.unit_proof(key)
                if key not in proven_metrics:
                    if (
                        _literal_expression_unit(
                            metric_bound.expression, context, metric_bound.relation, proven_metrics
                        )
                        != metric_bound.unit
                    ):
                        raise ValueError("growth projected metric has incompatible units")
                    if _work is not None:
                        _work.unit_proof(key, insert=True)
                    proven_metrics.add(key)
        return contexts
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


def validate_projection(
    projection: object, *, numeric_policy: NumericWorkPolicy
) -> NumericWorkUsage:
    """Validate standalone declaration data through the same typed expression model."""

    work = _NumericWork.from_policy(numeric_policy)
    _projection_contexts(projection, _work=work)
    return work.usage()


def validate_sample(
    projection: object,
    declaration_id: str,
    dimensions: Mapping[str, int],
    metrics: Mapping[str, int],
    *,
    numeric_policy: NumericWorkPolicy,
) -> NumericWorkUsage:
    """Check actual primitive observations; no source, time or success claim is inferred."""

    work = _NumericWork.from_policy(numeric_policy)
    contexts = _projection_contexts(projection, _work=work)
    if declaration_id not in contexts:
        raise ValueError("growth sample has no declared operation")
    _validate_sample(contexts[declaration_id], dimensions, metrics)
    return work.usage()


def _metric_violation(actual: int, expected: int, work: _NumericWork, *, equal: bool) -> bool:
    work.comparison(actual, expected, "metric")
    return actual != expected if equal else actual > expected


def _validate_sample(
    context: _Context, dimensions: Mapping[str, int], metrics: Mapping[str, int]
) -> None:
    work = context.numeric_work
    if work is not None:
        if context.source_owned:
            work.source_record(
                context.declaration, Declaration, "source_numeric_context", retain=True
            )
        work.charge(1 + context.numeric_metric_validation_work, "original_metric_boundary")
        work.original_metric_validations += 1
    # Keep original container operations and callbacks. Their input scans are
    # separate from the private derived-work allowance.
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
        if work is None:
            # Keep short-circuit truth testing for callback-bearing results.
            if (bound.relation == "equal" and metrics[bound.metric] != expected) or (
                bound.relation == "upper" and metrics[bound.metric] > expected
            ):
                raise ValueError("actual primitive observations violate the declared growth bound")
            continue
        if (
            bound.relation == "equal"
            and _metric_violation(metrics[bound.metric], expected, work, equal=True)
        ) or (
            bound.relation == "upper"
            and _metric_violation(metrics[bound.metric], expected, work, equal=False)
        ):
            raise ValueError("actual primitive observations violate the declared growth bound")


def _execution_root_checker(
    contexts: dict[str, _Context],
    rows: Mapping[str, Any],
    declaration_digest: Callable[[str], str] | None = None,
) -> Callable[[str, object, int, list[int]], None]:
    # The returned callable owns this walker; the walker must not own itself.
    def check(
        recurse: Callable[..., None], identity: str, sample: object, depth: int, budget: list[int]
    ) -> None:
        budget[0] -= 1
        if depth > 64 or budget[0] < 0 or identity not in contexts:
            raise ValueError("growth sample exceeds the reader budget or has no declared owner")
        row = _fields(
            sample, {"declaration", "declaration_sha256", "dimensions", "metrics", "delegates"}
        )
        if row["declaration"] != identity or row["declaration_sha256"] != (
            hashlib.sha256(canonical_json_bytes(rows[identity])).hexdigest()
            if declaration_digest is None
            else declaration_digest(identity)
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
                recurse(recurse, child.declaration.id, child_sample, depth + 1, budget)

        # A completed walk must not retain the original sample in a self-cycle.
        def composition(recurse: Callable[..., None], expression: Expression) -> None:
            work = context.numeric_work
            if work is not None:
                work.charge(1 + len(expression.terms) + len(expression.arguments), "composition")
                work.composition_nodes += 1
            if expression.op == "delegate":
                if work is not None:
                    work.charge(context.numeric_delegate_lookup_work, "composition_delegate")
                child_samples = delegates[expression.name]
                count = _evaluate_expression(expression.terms[0], context, row["dimensions"])
                if work is not None:
                    work.arguments(expression.arguments)
                arguments = {
                    name: _evaluate_expression(term, context, row["dimensions"])
                    for name, term in expression.arguments
                }
                actual_count = len(child_samples)
                if work is not None:
                    work.comparison(actual_count, count, "composition_count")
                    # Original child mappings still supply their own live
                    # equality/callback work; this prepays the finite prefix.
                    if actual_count == count:
                        work.charge(actual_count, "original_child_comparisons")
                if actual_count != count or any(
                    sample["dimensions"] != arguments for sample in child_samples
                ):
                    raise ValueError("actual delegate calls contradict the declared composition")
            for term in expression.terms:
                recurse(recurse, term)
            for _, argument in expression.arguments:
                recurse(recurse, argument)

        for bound in context.declaration.bounds:
            composition(composition, bound.expression)

    return partial(check, check)


def validate_execution_samples(
    projection: object,
    declaration_id: str,
    samples: object,
    *,
    numeric_policy: NumericWorkPolicy,
) -> NumericWorkUsage:
    """Validate complete primitive samples and the actual named delegate invocations.

    Counts establish only the declared successful-call scope. Clean source and
    assertion execution must be verified by the evidence owner separately.
    Each root has its own bounded reader budget; every root is checked.
    """
    work = _NumericWork.from_policy(numeric_policy)
    contexts = _projection_contexts(projection, _work=work)
    rows = _fields(projection, {"format", "cost_model", "declarations"})["declarations"]
    check = _execution_root_checker(contexts, rows)
    if not isinstance(samples, list) or not samples:
        raise ValueError("growth evidence has no completed primitive samples")
    for sample in samples:
        check(declaration_id, sample, 0, [65536])
    return work.usage()


def validate_execution_roots(
    projection_json: bytes, groups: object, *, numeric_policy: NumericWorkPolicy
) -> NumericWorkUsage:
    """Check independent roots against one private canonical metadata closure.

    Complete metadata is validated before the finite ordered group schedule.
    Each group is an exact (declaration identity, nonempty sample list) tuple.
    Snapshot only that schedule; original sample values remain live and each
    occurrence gets its own depth/node budget. No acceptance or compiled
    context survives this invocation. Failure has no partial-success result;
    an original traceback can retain its local state until received.

    The mandatory caller policy meters admission recursion and private numeric
    expansion for the whole finite invocation. Original Mapping scans/callbacks
    and metadata byte decoding retain their separate input costs.
    """
    work = _NumericWork.from_policy(numeric_policy)
    projection = parse_identity_json(projection_json)
    if canonical_json_bytes(projection) != projection_json:
        raise ValueError("growth metadata input must be canonical JSON bytes")
    contexts = _projection_contexts(projection, _literal_units=True, _work=work)
    rows = _fields(projection, {"format", "cost_model", "declarations"})["declarations"]
    if type(groups) is not list or not groups:
        raise ValueError("growth evidence requires nonempty ordered root groups")
    schedule = []
    for group in groups:
        if (
            type(group) is not tuple
            or len(group) != 2
            or type(group[0]) is not str
            or type(group[1]) is not list
            or not group[1]
        ):
            raise ValueError("growth root group requires an identity and completed sample list")
        schedule.append((group[0], tuple(group[1])))
    # Only this invocation owns the decoded literal rows. Reuse their identity
    # representation; each original sample still supplies a fresh digest/value.
    digests: dict[str, str] = {}

    def declaration_digest(identity: str) -> str:
        if identity not in digests:
            digests[identity] = hashlib.sha256(canonical_json_bytes(rows[identity])).hexdigest()
        return digests[identity]

    check = _execution_root_checker(contexts, rows, declaration_digest)
    for identity, samples in schedule:
        for sample in samples:
            check(identity, sample, 0, [65536])
    return work.usage()


def _evaluate(
    context: _Context, metric: str, dimensions: Mapping[str, int], *, _derived: bool = False
) -> int:
    work = context.numeric_work
    if work is not None:
        if context.source_owned:
            work.source_record(
                context.declaration, Declaration, "source_numeric_context", retain=True
            )
        work.charge(1 + context.numeric_metric_work, "metric_dispatch")
        work.metric_calls += 1
        if _derived:
            # Only the evaluator creates this private dictionary. An original
            # exact dict never acquires this authority by its outer type.
            work.charge(context.numeric_environment_work, "derived_environment")
            work.derived_environment_entries += len(context.declaration.dimensions)
        else:
            work.original_environment_validations += 1
    # Observations may be mutable mappings, so every evaluation checks them.
    if set(dimensions) != (
        context.numeric_dimension_names
        if context.numeric_dimension_names is not None
        else _dimension_table(context, names_only=True)
    ) or any(type(value) is not int or value < 0 for value in dimensions.values()):
        raise ValueError("growth dimensions must be complete nonnegative integer observations")
    bounds = context.numeric_bounds_by_metric
    if bounds is None:
        bounds = _metric_table(context)
    if metric not in bounds:
        raise ValueError("growth metric is not declared")
    bound = bounds[metric]
    if bound.relation == "big_o":
        raise ValueError("asymptotic arguments are not numerical acceptance budgets")

    return _evaluate_expression(bound.expression, context, dimensions)


def _evaluate_expression(
    expression: Expression, context: _Context, dimensions: Mapping[str, int]
) -> int:
    work = context.numeric_work

    # A self-capturing local function would retain contexts and dimensions
    # until cyclic collection, even after successful evaluation returns.
    def evaluate(recurse: Callable[..., int], expression: Expression) -> int:
        if work is not None:
            if context.source_owned:
                work.expression_record(expression, "source_numeric_expression", source=True)
            work.charge(1 + len(expression.terms) + len(expression.arguments), "expression")
            work.expression_nodes += 1
        if expression.op == "constant":
            if work is not None:
                if context.source_owned:
                    work.source_record(
                        expression, Expression, "source_numeric_constant", retain=True
                    )
                work.operand(expression.value, "constant")
            return expression.value
        if expression.op == "dimension":
            if work is not None:
                if context.source_owned:
                    work.source_record(
                        expression, Expression, "source_numeric_dimension", retain=True
                    )
                work.charge(context.numeric_dimension_lookup_work, "dimension_lookup")
            value = dimensions[expression.name]
            if work is not None:
                work.operand(value, "dimension")
            return value
        values = tuple(recurse(recurse, term) for term in expression.terms)
        if work is not None and context.source_owned:
            work.source_record(expression, Expression, "source_numeric_dispatch", retain=True)
        if expression.op == "sum":
            if work is not None:
                work.addition(values)
            return sum(values)
        if expression.op == "product":
            result = 1
            for value in values:
                if work is not None:
                    work.multiplication(result, value)
                result *= value
            return result
        if expression.op == "maximum":
            if work is not None:
                work.maximum(values)
            return max(values)
        if expression.op == "log2_ceil":
            if work is not None:
                work.comparison(values[0], 1, "log_domain")
            if values[0] < 1:
                raise ValueError("growth logarithm requires a positive argument")
            if work is not None:
                work.charge(work.operand(values[0], "logarithm") + 1, "logarithm.arithmetic")
            return (values[0] - 1).bit_length()
        if work is not None:
            if context.source_owned:
                work.source_record(expression, Expression, "source_numeric_delegate", retain=True)
            work.charge(context.numeric_delegate_lookup_work, "delegate_lookup")
        child = context.delegates[expression.name]
        if work is not None:
            if context.source_owned:
                work.source_record(expression, Expression, "source_numeric_arguments", retain=True)
            work.arguments(expression.arguments)
        arguments = {name: recurse(recurse, term) for name, term in expression.arguments}
        if work is not None and context.source_owned:
            work.source_record(expression, Expression, "source_numeric_metric", retain=True)
        multiplier = values[0]
        if work is None:
            return multiplier * _evaluate(child, expression.metric, arguments)
        result = _evaluate(child, expression.metric, arguments, _derived=True)
        work.multiplication(multiplier, result)
        return multiplier * result

    return evaluate(evaluate, expression)


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
    "NumericWorkPolicy",
    "NumericWorkRefused",
    "NumericWorkUsage",
    "SCALAR",
    "declare_growth",
    "evaluate_bound",
    "growth_binding",
    "project_bindings",
    "project_binding",
    "validate_execution_roots",
    "validate_execution_samples",
    "validate_projection",
    "validate_sample",
]

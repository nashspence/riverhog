"""Concrete monotone numeric envelopes for Core-owned growth composition."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from riverhog_growth_contracts import Expression, growth_binding


def compact_expression(expression: Expression) -> Expression:
    """Fold exact arithmetic in already dimension-checked declarations."""
    terms = tuple(compact_expression(term) for term in expression.terms)
    if expression.op not in {"sum", "product"}:
        return replace(expression, terms=terms)
    flattened = tuple(
        child for term in terms for child in (term.terms if term.op == expression.op else (term,))
    )
    constants = [term for term in flattened if term.op == "constant"]
    values = [term for term in flattened if term.op != "constant"]
    if constants:
        if expression.op == "sum":
            value = sum(term.value for term in constants)
            unit = constants[0].unit
        else:
            value = 1
            powers: dict[str, int] = {}
            for term in constants:
                value *= term.value
                for name, power in term.unit:
                    powers[name] = powers.get(name, 0) + power
            unit = tuple(sorted((name, power) for name, power in powers.items() if power))
        if (
            not values
            or expression.op == "sum"
            and value
            or expression.op == "product"
            and (value != 1 or unit)
        ):
            values.append(Expression("constant", value=value, unit=unit))
    if len(values) == 1:
        return values[0]
    return replace(expression, terms=tuple(values))


def bound_envelope(
    operation: Callable[..., Any],
    metric: str,
    arguments: Mapping[str, Expression],
) -> Expression:
    binding = growth_binding(operation)
    bound = next(bound for bound in binding.declaration.bounds if bound.metric == metric)
    if bound.relation == "big_o":
        raise ValueError("concrete owner envelopes require numeric provider bounds")

    def expand(expression: Expression) -> Expression:
        if expression.op == "dimension":
            return arguments[expression.name]
        if expression.op == "constant":
            return expression
        if expression.op == "delegate":
            child = dict(binding.delegates)[expression.name]
            return Expression(
                "product",
                terms=(
                    Expression("constant", value=1, unit=(("count", -1),)),
                    expand(expression.terms[0]),
                    bound_envelope(
                        child,
                        expression.metric,
                        {name: expand(value) for name, value in expression.arguments},
                    ),
                ),
            )
        return replace(expression, terms=tuple(expand(term) for term in expression.terms))

    return compact_expression(expand(bound.expression))

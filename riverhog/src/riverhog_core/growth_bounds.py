"""Concrete monotone numeric envelopes for Core-owned growth composition."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from riverhog_growth_contracts import Expression, growth_binding


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

    return expand(bound.expression)

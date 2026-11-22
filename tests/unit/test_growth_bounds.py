from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

import pytest
import riverhog_core
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.growth_bounds import bound_envelope, compact_expression
from riverhog_core.services.upload_journal_validation_owner import UploadJournalValidationOwner
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    evaluate_bound,
    growth_binding,
)

from scripts.growth_contract import source_bindings
from scripts.validation_numeric import numeric_policy_from_environment


@pytest.mark.parametrize("value", [1, 17, 1025])
def test_compacted_envelopes_preserve_complete_actual_owner_bounds(value):
    original = growth_binding(UploadJournalValidationOwner.advance)
    arguments = {
        item.name: Expression("dimension", name=item.name)
        for item in original.declaration.dimensions
    }
    envelope = replace(
        original.declaration,
        id="fixture.compacted-owner.growth/v1",
        subjects=("fixture.compacted_owner",),
        bounds=tuple(
            replace(
                bound,
                expression=bound_envelope(original.operation, bound.metric, arguments),
            )
            for bound in original.declaration.bounds
        ),
    )

    def operation():
        pass

    declare_growth(envelope)(operation)
    current = growth_binding(operation)
    dimensions = {name: value for name in arguments}
    for bound in original.declaration.bounds:
        assert evaluate_bound(
            current,
            bound.metric,
            dimensions,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) == evaluate_bound(
            original,
            bound.metric,
            dimensions,
            numeric_policy=numeric_policy_from_environment(os.environ),
        )


def test_compaction_preserves_typed_zero_and_unit_cancellation():
    count = (("count", 1),)
    inverse = (("count", -1),)
    octets = (("octets", 1),)
    zero = Expression("constant", value=0, unit=octets)
    assert compact_expression(Expression("sum", terms=(zero, zero))) == zero
    body = Expression("dimension", name="bytes")
    nested = Expression(
        "product",
        terms=(
            Expression("constant", value=1, unit=count),
            Expression(
                "product",
                terms=(Expression("constant", value=1, unit=inverse), body),
            ),
        ),
    )
    assert compact_expression(nested) == body
    assert compact_expression(Expression("sum", terms=(zero, body))) == body
    typed = Expression(
        "product",
        terms=(Expression("constant", value=1, unit=count), body),
    )
    compacted = compact_expression(typed)
    assert compacted != body and set(compacted.terms) == set(typed.terms)


@pytest.mark.parametrize("operator", ("delegate", "delegate_envelope"))
@pytest.mark.parametrize("calls, octets", ((0, 0), (1, 17), (4, 1025)))
def test_core_expands_heterogeneous_and_exact_numeric_bounds_through_nested_calls(
    operator, calls, octets
):
    def bind(name, dimensions, expression, delegates=()):
        def operation():
            pass

        declaration = Declaration(
            id=f"fixture.expansion-{name}.growth/v1",
            authority="fixture",
            subjects=(f"fixture.expansion_{name}",),
            scope="test numeric expansion only",
            starts_at="receive declared dimensions",
            ends_at="return a numeric expression",
            dimensions=dimensions,
            bounds=(Bound("scanned", OCTETS, "upper", expression, "Declared numeric ceiling."),),
            conditions=("Dimensions are exact nonnegative source inputs.",),
            invariants=("The original nested leaf binding remains owned.",),
            limitations=("Original invocation evidence is checked separately.",),
        )
        declare_growth(declaration, delegates=delegates)(operation)
        return operation

    leaf = bind(
        "leaf",
        (Dimension("bytes", OCTETS, "Actual leaf input size."),),
        Expression("dimension", name="bytes"),
    )
    child = bind(
        "child",
        (Dimension("chunk", OCTETS, "Exact nested leaf size."),),
        Expression(
            "delegate",
            name="leaf",
            metric="scanned",
            terms=(Expression("constant", value=1, unit=COUNT),),
            arguments=(("bytes", Expression("dimension", name="chunk")),),
        ),
        (("leaf", leaf),),
    )
    parent = bind(
        "parent",
        (
            Dimension("calls", COUNT, "Actual required child cardinality."),
            Dimension("ceiling", OCTETS, "Parent-derived child input ceiling."),
        ),
        Expression(
            operator,
            name="child",
            metric="scanned",
            terms=(Expression("dimension", name="calls"),),
            arguments=(("chunk", Expression("dimension", name="ceiling")),),
        ),
        (("child", child),),
    )
    arguments = {
        "calls": Expression("dimension", name="count"),
        "ceiling": Expression("dimension", name="width"),
    }
    expanded = bind(
        "expanded",
        (
            Dimension("count", COUNT, "Original cardinality input."),
            Dimension("width", OCTETS, "Original parent ceiling."),
        ),
        bound_envelope(parent, "scanned", arguments),
    )
    policy = numeric_policy_from_environment(os.environ)
    expected = evaluate_bound(
        growth_binding(parent),
        "scanned",
        {"calls": calls, "ceiling": octets},
        numeric_policy=policy,
    )
    assert expected == calls * octets
    assert growth_binding(expanded).delegates == ()
    assert (
        evaluate_bound(
            growth_binding(expanded),
            "scanned",
            {"count": calls, "width": octets},
            numeric_policy=policy,
        )
        == expected
    )
    assert growth_binding(parent).declaration.bounds[0].expression.op == operator


def test_archive_source_declarations_enter_repository_contract_discovery():
    bindings = source_bindings({}, packages={"riverhog_core": Path(riverhog_core.__file__)})
    operations = {binding.operation for binding in bindings}
    assert {
        ArchiveJournalSource.__init__,
        ArchiveJournalSource.verify_current,
        ArchiveJournalSource.read_window,
        ArchiveJournalSource.read_exact,
    } <= operations

from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_client import hash_raw_source_chunks
from riverhog_protocol.growth import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    evaluate_bound,
    growth_binding,
)
from riverhog_protocol.raw_ingress import advance_raw_part_commitment


def declaration(expression: Expression, *, unit=OCTETS, relation="equal") -> Declaration:
    return Declaration(
        id="fixture.operation.growth/v1",
        authority="fixture",
        subjects=("fixture.operation",),
        scope="successful-call-primitive-work",
        starts_at="accept input",
        ends_at="return exact output",
        dimensions=(Dimension("bytes", OCTETS, "Consumed input bytes."),),
        bounds=(Bound("processed_bytes", unit, relation, expression, "Processed input bytes."),),
        conditions=("Finite valid input.",),
        invariants=("Complete exact output.",),
        limitations=("No numerical elapsed-time promise.",),
    )


def attach(value: Declaration):
    def operation():
        return "exact-output"

    return declare_growth(value)(operation)


@pytest.mark.parametrize("value", [True, -1, 1.0])
def test_constants_reject_noninteger_or_negative_values(value):
    with pytest.raises(ValueError):
        Expression("constant", value=value)


@pytest.mark.parametrize(
    "expression",
    [
        lambda: Expression("eval", name="arbitrary()"),
        lambda: Expression("constant", value=1, name="hidden"),
        lambda: Expression("dimension", name="bytes", value=1),
        lambda: Expression("sum", terms=(Expression("constant"),)),
        lambda: Expression("log2_ceil", terms=()),
    ],
)
def test_expression_grammar_is_closed(expression):
    with pytest.raises(ValueError):
        expression()


def test_declared_units_reject_byte_count_addition_and_unknown_dimensions():
    invalid = Expression(
        "sum",
        terms=(Expression("dimension", name="bytes"), Expression("constant", value=1, unit=COUNT)),
    )
    with pytest.raises(ValueError, match="incompatible"):
        attach(declaration(invalid))
    with pytest.raises(ValueError, match="unknown dimension"):
        attach(declaration(Expression("dimension", name="absent")))
    with pytest.raises(ValueError, match="metric unit"):
        attach(declaration(Expression("dimension", name="bytes"), unit=COUNT))


def test_binding_preserves_exact_function_identity_and_single_execution():
    calls = []

    def operation():
        calls.append("executed")
        return "exact-output"

    result = declare_growth(declaration(Expression("dimension", name="bytes")))(operation)
    assert result is operation
    assert result() == "exact-output"
    assert calls == ["executed"]
    assert growth_binding(result).operation is operation
    with pytest.raises(ValueError, match="duplicate"):
        declare_growth(growth_binding(result).declaration)(operation)


@pytest.mark.parametrize(
    "dimensions",
    [{}, {"bytes": True}, {"bytes": -1}, {"bytes": 1.0}, {"bytes": 1, "extra": 2}],
)
def test_evaluation_requires_complete_actual_integer_dimensions(dimensions):
    operation = attach(declaration(Expression("dimension", name="bytes")))
    with pytest.raises(ValueError, match="nonnegative integer"):
        evaluate_bound(growth_binding(operation), "processed_bytes", dimensions)


def test_asymptotic_expression_is_not_an_absolute_acceptance_budget():
    operation = attach(declaration(Expression("dimension", name="bytes"), relation="big_o"))
    with pytest.raises(ValueError, match="not numerical acceptance"):
        evaluate_bound(growth_binding(operation), "processed_bytes", {"bytes": 100})


def test_count_to_octet_conversion_has_explicit_units():
    expression = Expression(
        "product",
        terms=(
            Expression("constant", value=72, unit=OCTETS_PER_COUNT),
            Expression("constant", value=2, unit=COUNT),
        ),
    )
    operation = attach(declaration(expression))
    assert evaluate_bound(growth_binding(operation), "processed_bytes", {"bytes": 0}) == 144


@pytest.mark.parametrize("parts", [1, 2, 17, 1024])
def test_real_source_declaration_composes_its_exact_commitment_delegate(parts):
    binding = growth_binding(hash_raw_source_chunks)
    assert dict(binding.delegates)["commitment"] is advance_raw_part_commitment
    dimensions = {"payload_octets": parts * 65536 - 7, "input_chunks": parts + 3, "parts": parts}
    assert (
        evaluate_bound(binding, "hash_input_octets", dimensions)
        == 2 * dimensions["payload_octets"] + 72 * parts
    )
    assert evaluate_bound(binding, "digest_spool_octets", dimensions) == 32 * parts
    assert evaluate_bound(binding, "iterator_pulls", dimensions) == dimensions["input_chunks"] + 1
    child = growth_binding(advance_raw_part_commitment)
    assert evaluate_bound(child, "hash_input_octets", {"parts": parts}) == 72 * parts
    projected = binding.project()
    assert projected["delegates"]["commitment"]["declaration"] == child.declaration.id
    assert "source_sha" not in projected and "tests" not in projected


def test_declaration_rejects_duplicate_dimension_and_metric_names():
    value = declaration(Expression("dimension", name="bytes"))
    with pytest.raises(ValueError, match="unique"):
        replace(value, dimensions=(*value.dimensions, *value.dimensions))
    with pytest.raises(ValueError, match="unique"):
        replace(value, bounds=(*value.bounds, *value.bounds))


@pytest.mark.parametrize("defect", ["missing", "metric", "unit", "multiplicity"])
def test_delegate_composition_rejects_incomplete_or_contradictory_mappings(defect):
    expression = Expression(
        "delegate",
        name="commitment",
        metric="hash_input_octets" if defect != "metric" else "absent",
        terms=(
            Expression("constant", value=1, unit=COUNT if defect != "multiplicity" else OCTETS),
        ),
        arguments=()
        if defect == "missing"
        else (
            ("parts", Expression("constant", value=1, unit=COUNT if defect != "unit" else OCTETS)),
        ),
    )
    with pytest.raises(ValueError):
        declare_growth(
            declaration(expression), delegates=(("commitment", advance_raw_part_commitment),)
        )(lambda: None)


def test_standalone_reader_rejects_unused_cyclic_delegate_edges():
    from riverhog_protocol.growth import project_bindings, validate_projection

    operation = attach(declaration(Expression("dimension", name="bytes")))
    projection = project_bindings((growth_binding(operation),))
    projection["declarations"]["fixture.operation.growth/v1"]["delegates"]["unused"] = {
        "declaration": "fixture.operation.growth/v1"
    }
    with pytest.raises(ValueError, match="cyclic"):
        validate_projection(projection)


@pytest.mark.parametrize("meaning", [True, 1, ["ambiguous"]])
def test_dimension_and_metric_meanings_require_text(meaning):
    with pytest.raises(ValueError):
        Dimension("bytes", OCTETS, meaning)
    with pytest.raises(ValueError):
        Bound("bytes", OCTETS, "equal", Expression("dimension", name="bytes"), meaning)

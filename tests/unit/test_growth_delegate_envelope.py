from __future__ import annotations

import copy
import hashlib
import os
from dataclasses import replace

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes

from scripts.validation_numeric import numeric_policy_from_environment


def _policy():
    return numeric_policy_from_environment(os.environ)


def _dimension(name):
    return growth.Expression("dimension", name=name)


def _constant(value, unit=growth.COUNT):
    return growth.Expression("constant", value=value, unit=unit)


def _declaration(name, dimensions, bounds):
    return growth.Declaration(
        id=f"fixture.envelope_{name}.growth/v1",
        authority="fixture",
        subjects=(f"fixture.envelope_{name}",),
        scope="successful-call-primitive-work",
        starts_at="accept the original input and its work allowance",
        ends_at="receive every original child and return complete output",
        dimensions=dimensions,
        bounds=bounds,
        conditions=("Finite complete observations of the original invocation.",),
        invariants=("Parent work limits do not come from observed child costs.",),
        limitations=("Synthetic algebra witness, not a Native workload result.",),
    )


def _bind(declaration, delegates=()):
    def operation():
        return None

    return growth.growth_binding(growth.declare_growth(declaration, delegates=delegates)(operation))


def _sample(binding, dimensions, metrics, delegates=None):
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(canonical_json_bytes(binding.project())).hexdigest(),
        "dimensions": dimensions,
        "metrics": metrics,
        "delegates": {} if delegates is None else delegates,
    }


def _family(*, op="delegate_envelope"):
    leaf = _bind(
        _declaration(
            "leaf",
            (growth.Dimension("bytes", growth.OCTETS, "Exact bytes supplied to this child."),),
            (
                growth.Bound(
                    "scanned", growth.OCTETS, "equal", _dimension("bytes"), "Actual scanned bytes."
                ),
            ),
        )
    )
    child = _bind(
        _declaration(
            "child",
            (
                growth.Dimension(
                    "byte_budget", growth.OCTETS, "This call's remaining byte budget."
                ),
                growth.Dimension(
                    "phase", growth.COUNT, "This phase's actual leaf invocation count."
                ),
            ),
            (
                growth.Bound(
                    "scanned",
                    growth.OCTETS,
                    "equal",
                    growth.Expression(
                        "delegate",
                        name="read",
                        metric="scanned",
                        terms=(_dimension("phase"),),
                        arguments=(("bytes", _dimension("byte_budget")),),
                    ),
                    "Exact composed scans for this call's phase and byte budget.",
                ),
            ),
        ),
        (("read", leaf.operation),),
    )
    parent = _bind(
        _declaration(
            "parent",
            (
                growth.Dimension("calls", growth.COUNT, "Actual original child invocations."),
                growth.Dimension(
                    "transition_limit", growth.COUNT, "Admitted transition allowance."
                ),
                growth.Dimension("byte_limit", growth.OCTETS, "Parent input bound for each child."),
                growth.Dimension(
                    "aggregate_limit", growth.OCTETS, "Admitted total byte allowance."
                ),
            ),
            (
                growth.Bound(
                    "scanned",
                    growth.OCTETS,
                    "upper",
                    growth.Expression(
                        op,
                        name="advance",
                        metric="scanned",
                        terms=(_dimension("calls"),),
                        arguments=(
                            ("byte_budget", _dimension("byte_limit")),
                            ("phase", _constant(1)),
                        ),
                    ),
                    "Declared conservative composition, independent of observed child costs.",
                ),
                growth.Bound(
                    "aggregate_scanned",
                    growth.OCTETS,
                    "upper",
                    _dimension("aggregate_limit"),
                    "Same actual byte counter checked against the admitted aggregate limit.",
                ),
                growth.Bound(
                    "actual_calls",
                    growth.COUNT,
                    "upper",
                    _dimension("transition_limit"),
                    "Actual calls cannot expand the admitted transition allowance.",
                ),
            ),
        ),
        (("advance", child.operation),),
    )
    return parent, child, leaf


def _invocation(
    family,
    arguments,
    *,
    byte_limit,
    aggregate_limit,
    transition_limit,
    declared_calls=None,
):
    parent, child, leaf = family
    children = []
    scanned = 0
    for budget, phase in arguments:
        reads = [_sample(leaf, {"bytes": budget}, {"scanned": budget}) for _ in range(phase)]
        scanned += budget * phase
        children.append(
            _sample(
                child,
                {"byte_budget": budget, "phase": phase},
                {"scanned": budget * phase},
                {"read": reads},
            )
        )
    return _sample(
        parent,
        {
            "calls": len(children) if declared_calls is None else declared_calls,
            "transition_limit": transition_limit,
            "byte_limit": byte_limit,
            "aggregate_limit": aggregate_limit,
        },
        {"scanned": scanned, "aggregate_scanned": scanned, "actual_calls": len(children)},
        {"advance": children},
    )


def _validate(parent, sample, *, reader="samples"):
    projection = growth.project_bindings((parent,), numeric_policy=_policy())
    if reader == "samples":
        return growth.validate_execution_samples(
            projection, parent.declaration.id, [sample], numeric_policy=_policy()
        )
    return growth.validate_execution_roots(
        canonical_json_bytes(projection),
        [(parent.declaration.id, [sample])],
        numeric_policy=_policy(),
    )


@pytest.mark.parametrize("reader", ["samples", "roots"])
def test_heterogeneous_calls_preserve_actual_phase_budget_and_nested_exact_arguments(reader):
    family = _family()
    sample = _invocation(
        family,
        [(33_582_160, 0), (33_582_096, 1), (64, 1)],
        byte_limit=33_582_160,
        aggregate_limit=33_582_160,
        transition_limit=3,
    )
    usage = _validate(family[0], sample, reader=reader)
    assert usage.original_metric_validations == 6
    assert usage.composition_nodes > 0
    assert sample["delegates"]["advance"][1]["dimensions"]["byte_budget"] == 33_582_096


@pytest.mark.parametrize("count", [0, 1, 4, 16])
def test_cardinality_is_exact_even_when_child_envelopes_are_zero(count):
    family = _family()
    sample = _invocation(
        family,
        [(0, index % 2) for index in range(count)],
        byte_limit=0,
        aggregate_limit=0,
        transition_limit=count,
    )
    _validate(family[0], sample)


@pytest.mark.parametrize("declared_calls", [0, 1, 3])
def test_wrong_actual_child_cardinality_is_rejected(declared_calls):
    family = _family()
    sample = _invocation(
        family,
        [(2, 0), (1, 0)],
        byte_limit=2,
        aggregate_limit=2,
        transition_limit=3,
        declared_calls=declared_calls,
    )
    with pytest.raises(ValueError, match="actual delegate calls contradict"):
        _validate(family[0], sample)


def test_child_above_envelope_fails_even_when_its_phase_performs_no_work():
    family = _family()
    sample = _invocation(family, [(65, 0)], byte_limit=64, aggregate_limit=64, transition_limit=1)
    with pytest.raises(ValueError, match="exceeds the declared envelope"):
        _validate(family[0], sample)


def test_each_nested_exact_delegate_rejects_a_predicted_envelope_in_place_of_actual_input():
    family = _family()
    sample = _invocation(
        family,
        [(33_582_096, 1)],
        byte_limit=33_582_160,
        aggregate_limit=33_582_160,
        transition_limit=1,
    )
    nested = sample["delegates"]["advance"][0]["delegates"]["read"][0]
    nested["dimensions"]["bytes"] = 33_582_160
    nested["metrics"]["scanned"] = 33_582_160
    with pytest.raises(ValueError, match="actual delegate calls contradict"):
        _validate(family[0], sample)


def test_child_primitive_excess_is_not_hidden_by_a_larger_parent_envelope():
    family = _family()
    sample = _invocation(family, [(7, 1)], byte_limit=100, aggregate_limit=100, transition_limit=1)
    sample["delegates"]["advance"][0]["delegates"]["read"][0]["metrics"]["scanned"] = 8
    with pytest.raises(ValueError, match="primitive observations violate"):
        _validate(family[0], sample)


def test_parent_aggregate_bound_rejects_repeated_whole_resource_work():
    family = _family()
    sample = _invocation(
        family, [(100, 1)] * 4, byte_limit=100, aggregate_limit=120, transition_limit=4
    )
    parent = family[0]
    assert (
        growth.evaluate_bound(parent, "scanned", sample["dimensions"], numeric_policy=_policy())
        == 400
    )
    with pytest.raises(ValueError, match="primitive observations violate"):
        _validate(parent, sample)


def test_observed_call_count_cannot_enlarge_admitted_quantum():
    family = _family()
    sample = _invocation(family, [(0, 0)] * 5, byte_limit=0, aggregate_limit=0, transition_limit=4)
    with pytest.raises(ValueError, match="primitive observations violate"):
        _validate(family[0], sample)


def test_parent_bound_does_not_follow_changed_child_costs():
    family = _family()
    first = _invocation(
        family, [(1, 1), (2, 1)], byte_limit=10, aggregate_limit=20, transition_limit=2
    )
    second = _invocation(
        family, [(8, 1), (9, 1)], byte_limit=10, aggregate_limit=20, transition_limit=2
    )
    assert first["dimensions"] == second["dimensions"]
    for sample in (first, second):
        assert (
            growth.evaluate_bound(
                family[0], "scanned", sample["dimensions"], numeric_policy=_policy()
            )
            == 20
        )
        _validate(family[0], sample)


@pytest.mark.parametrize("budget", [9, 11])
def test_existing_exact_delegate_still_rejects_smaller_and_larger_child_arguments(budget):
    parent, child, leaf = _family(op="delegate")
    composed = parent.declaration.bounds[0]
    parent = _bind(
        replace(
            parent.declaration,
            bounds=(
                replace(
                    composed,
                    expression=growth.Expression(
                        "sum", terms=(composed.expression, _constant(10, growth.OCTETS))
                    ),
                ),
                *parent.declaration.bounds[1:],
            ),
        ),
        parent.delegates,
    )
    family = parent, child, leaf
    sample = _invocation(
        family, [(budget, 1)], byte_limit=10, aggregate_limit=20, transition_limit=1
    )
    with pytest.raises(ValueError, match="actual delegate calls contradict"):
        _validate(family[0], sample)


@pytest.mark.parametrize("relation", ["equal", "big_o"])
def test_envelope_does_not_establish_an_exact_or_asymptotic_relation(relation):
    parent, child, _ = _family()
    changed = replace(
        parent.declaration,
        bounds=(replace(parent.declaration.bounds[0], relation=relation),),
    )
    with pytest.raises(ValueError, match="envelope requires an upper bound"):
        _bind(changed, (("advance", child.operation),))


@pytest.mark.parametrize("defect", ["unit", "missing", "count"])
def test_envelope_preserves_complete_argument_and_unit_validation(defect):
    parent, child, _ = _family()
    expression = parent.declaration.bounds[0].expression
    if defect == "unit":
        expression = replace(
            expression, arguments=(("byte_budget", _constant(1)), ("phase", _constant(1)))
        )
    elif defect == "missing":
        expression = replace(expression, arguments=expression.arguments[:1])
    else:
        expression = replace(expression, terms=(_constant(1, growth.OCTETS),))
    changed = replace(
        parent.declaration, bounds=(replace(parent.declaration.bounds[0], expression=expression),)
    )
    with pytest.raises(ValueError, match="incompatible|incomplete|call-count"):
        _bind(changed, (("advance", child.operation),))


def test_envelope_rejects_an_asymptotic_child_metric():
    parent, child, _ = _family()
    changed_child = _bind(
        replace(
            child.declaration,
            bounds=(replace(child.declaration.bounds[0], relation="big_o"),),
        ),
        child.delegates,
    )
    with pytest.raises(ValueError, match="stronger parent relation"):
        _bind(parent.declaration, (("advance", changed_child.operation),))


def test_arithmetic_and_positive_logarithm_envelopes_are_monotone():
    base, _, _ = _family()
    child = _bind(
        _declaration(
            "monotone",
            (growth.Dimension("items", growth.COUNT, "Actual positive-domain input."),),
            (
                growth.Bound(
                    "scanned",
                    growth.COUNT,
                    "upper",
                    growth.Expression(
                        "product",
                        terms=(
                            growth.Expression("maximum", terms=(_dimension("items"), _constant(2))),
                            growth.Expression(
                                "log2_ceil",
                                terms=(
                                    growth.Expression(
                                        "sum", terms=(_dimension("items"), _constant(1))
                                    ),
                                ),
                            ),
                        ),
                    ),
                    "Monotone product, maximum, sum and positive logarithm.",
                ),
            ),
        )
    )
    parent = _bind(
        replace(
            base.declaration,
            dimensions=(growth.Dimension("limit", growth.COUNT, "Admitted input ceiling."),),
            bounds=(
                growth.Bound(
                    "scanned",
                    growth.COUNT,
                    "upper",
                    growth.Expression(
                        "delegate_envelope",
                        name="work",
                        metric="scanned",
                        terms=(_constant(1),),
                        arguments=(("items", _dimension("limit")),),
                    ),
                    "The declared monotone bound at the parent input ceiling.",
                ),
            ),
        ),
        (("work", child.operation),),
    )
    for limit in range(9):
        ceiling = growth.evaluate_bound(
            parent, "scanned", {"limit": limit}, numeric_policy=_policy()
        )
        for value in range(limit + 1):
            actual = growth.evaluate_bound(
                child, "scanned", {"items": value}, numeric_policy=_policy()
            )
            assert actual <= ceiling
            _validate(
                parent,
                _sample(
                    parent,
                    {"limit": limit},
                    {"scanned": actual},
                    {"work": [_sample(child, {"items": value}, {"scanned": actual})]},
                ),
            )


def test_unknown_nonmonotone_operator_cannot_enter_a_projected_envelope():
    parent, _, _ = _family()
    projection = growth.project_bindings((parent,), numeric_policy=_policy())
    changed = copy.deepcopy(projection)
    expression = changed["declarations"][parent.declaration.id]["bounds"]["scanned"]["expression"]
    expression["arguments"]["byte_budget"] = {"op": "subtract", "terms": []}
    with pytest.raises(ValueError, match="unknown operator"):
        growth.validate_projection(changed, numeric_policy=_policy())

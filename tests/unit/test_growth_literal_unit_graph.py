from __future__ import annotations

import copy
import hashlib
import json
import os
from collections import Counter

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes

from scripts.validation_numeric import numeric_policy_from_environment
from tests.unit.test_growth_numeric_evaluation import (
    NUMERIC_CALLBACK_EXPECTATIONS,
    numeric_callback_outcome,
)


def _identity(name):
    return f"fixture.literal_{name}.growth/v1"


def _dimension(name="items"):
    return {"op": "dimension", "name": name}


def _constant(unit=growth.COUNT, value=1):
    return {"op": "constant", "value": value, "unit": [list(term) for term in unit]}


def _call(name, metric="visits", *, argument=None):
    return {
        "op": "delegate",
        "name": name,
        "metric": metric,
        "multiplicity": _constant(),
        "arguments": {"items": _dimension() if argument is None else argument},
    }


def _row(name, *, delegates=None, bounds=None):
    declaration = growth.Declaration(
        id=_identity(name),
        authority="fixture",
        subjects=(f"fixture.literal_{name}",),
        scope="successful-call-primitive-work",
        starts_at="accept actual input",
        ends_at="return exact output",
        dimensions=(growth.Dimension("items", growth.COUNT, "Actual input items."),),
        bounds=(
            growth.Bound(
                "visits",
                growth.COUNT,
                "equal",
                growth.Expression("dimension", name="items"),
                "Actual primitive work.",
            ),
        ),
        conditions=("Finite valid input.",),
        invariants=("Complete exact output.",),
        limitations=("No numerical elapsed-time promise.",),
    )
    row = declaration.project()
    row["delegates"] = {
        edge: {"declaration": _identity(target)} for edge, target in (delegates or {}).items()
    }
    if bounds is not None:
        row["bounds"] = {
            metric: {
                "unit": [list(term) for term in unit],
                "relation": relation,
                "mode": "worst-case",
                "expression": expression,
                "meaning": "Actual primitive work.",
            }
            for metric, (unit, relation, expression) in bounds.items()
        }
    return row


def _projection(*rows):
    sentinel = _row("sentinel")
    return {
        "format": growth.FORMAT,
        "cost_model": growth.COST_MODEL,
        "declarations": {row["id"]: row for row in (*rows, sentinel)},
    }


def _groups(projection):
    identity = _identity("sentinel")
    return [
        (
            identity,
            [
                {
                    "declaration": identity,
                    "declaration_sha256": hashlib.sha256(
                        canonical_json_bytes(projection["declarations"][identity])
                    ).hexdigest(),
                    "dimensions": {"items": 3},
                    "metrics": {"visits": 3},
                    "delegates": {},
                }
            ],
        )
    ]


def _validate(projection):
    growth.validate_execution_roots(
        canonical_json_bytes(projection),
        _groups(projection),
        numeric_policy=numeric_policy_from_environment(os.environ),
    )


def _graph(shape, size):
    rows = []

    def append(name, children):
        expressions = [_call(edge) for edge in children]
        expression = (
            _dimension()
            if not expressions
            else expressions[0]
            if len(expressions) == 1
            else {"op": "sum", "terms": expressions}
        )
        rows.append(
            _row(
                name,
                delegates=children,
                bounds={"visits": (growth.COUNT, "equal", expression)},
            )
        )

    if shape in {"chain", "shared"}:
        append("node_0", {})
        for index in range(1, size):
            edges = ("left", "right") if shape == "shared" else ("child",)
            append(f"node_{index}", {edge: f"node_{index - 1}" for edge in edges})
    elif shape == "diamond":
        append("node_0", {})
        for index in range(1, size + 1):
            append(f"left_{index}", {"child": f"node_{index - 1}"})
            append(f"right_{index}", {"child": f"node_{index - 1}"})
            append(f"node_{index}", {"left": f"left_{index}", "right": f"right_{index}"})
    elif shape == "disjoint":

        def branch(name, remaining):
            children = {}
            if remaining:
                children = {"left": f"{name}_left", "right": f"{name}_right"}
                for child in children.values():
                    branch(child, remaining - 1)
            append(name, children)

        branch("node", size)
    elif shape == "multimetric":
        specifications = {
            "exact": (growth.COUNT, "equal"),
            "octets": (growth.OCTETS, "equal"),
            "upper": (growth.COUNT, "upper"),
            "asymptotic": (growth.SCALAR, "big_o"),
        }
        for index in range(size):
            bounds = {}
            for metric, (unit, relation) in specifications.items():
                if index:
                    expression = {
                        "op": "sum",
                        "terms": [_call(edge, metric) for edge in ("left", "right")],
                    }
                elif unit == growth.OCTETS:
                    expression = {
                        "op": "product",
                        "terms": [_dimension(), _constant(growth.OCTETS_PER_COUNT)],
                    }
                elif unit == growth.SCALAR:
                    expression = {"op": "log2_ceil", "terms": [_dimension()]}
                else:
                    expression = _dimension()
                bounds[metric] = unit, relation, expression
            rows.append(
                _row(
                    f"node_{index}",
                    delegates=(
                        {edge: f"node_{index - 1}" for edge in ("left", "right")} if index else {}
                    ),
                    bounds=bounds,
                )
            )
    else:
        raise AssertionError(shape)
    return _projection(*rows)


def _stored_expression_nodes(projection):
    def nodes(expression):
        return (
            1
            + sum(nodes(term) for term in expression.get("terms", []))
            + sum(nodes(term) for term in expression.get("arguments", {}).values())
            + (nodes(expression["multiplicity"]) if "multiplicity" in expression else 0)
        )

    return sum(
        nodes(bound["expression"])
        for row in projection["declarations"].values()
        for bound in row["bounds"].values()
    )


def _count_unit_work(monkeypatch):
    original = growth._literal_expression_unit
    expressions, metrics = Counter(), Counter()

    def check(expression, context, relation, proven_metrics, *, _allow_envelope=True):
        expressions[context.declaration.id, id(expression), relation] += 1
        for bound in context.declaration.bounds:
            if expression is bound.expression:
                metrics[context.declaration.id, bound.metric] += 1
        return original(
            expression, context, relation, proven_metrics, _allow_envelope=_allow_envelope
        )

    monkeypatch.setattr(growth, "_literal_expression_unit", check)
    return expressions, metrics


@pytest.mark.parametrize(
    "shape, size",
    [
        ("chain", 1),
        ("chain", 4),
        ("chain", 13),
        ("shared", 1),
        ("shared", 4),
        ("shared", 13),
        ("diamond", 1),
        ("diamond", 4),
        ("disjoint", 1),
        ("disjoint", 4),
        ("multimetric", 1),
        ("multimetric", 7),
    ],
)
def test_unit_validation_visits_stored_graph_not_its_expanded_paths(shape, size, monkeypatch):
    projection = _graph(shape, size)
    expressions, metrics = _count_unit_work(monkeypatch)
    _validate(projection)
    assert sum(expressions.values()) == _stored_expression_nodes(projection)
    assert set(expressions.values()) == {1}
    assert metrics == {
        (identity, metric): 1
        for identity, row in projection["declarations"].items()
        for metric in row["bounds"]
    }
    if shape == "shared":
        # Each added declaration stores two named calls to the preceding child.
        assert _stored_expression_nodes(projection) == 2 + 7 * (size - 1)


def _same_metadata_rejection(projection, monkeypatch, *, match=None):
    raw = canonical_json_bytes(projection)
    # Canonical decoding gives both readers exactly the same declaration order.
    with pytest.raises(ValueError, match=match) as original:
        growth.validate_projection(
            json.loads(raw), numeric_policy=numeric_policy_from_environment(os.environ)
        )

    def no_sample(*_args):
        raise AssertionError("sample validation preceded complete metadata validation")

    with monkeypatch.context() as patch:
        patch.setattr(growth, "_validate_sample", no_sample)
        with pytest.raises(type(original.value)) as literal:
            growth.validate_execution_roots(
                raw, _groups(projection), numeric_policy=numeric_policy_from_environment(os.environ)
            )
    assert str(literal.value) == str(original.value)


@pytest.mark.parametrize(
    "defect",
    [
        "unknown-dimension",
        "unknown-metric",
        "missing-argument",
        "argument-unit",
        "multiplicity-unit",
        "unbound-delegate",
        "unresolved-unused-edge",
        "unused-invalid-metric",
        "duplicate-subject",
        "unused-cycle",
    ],
)
def test_every_unused_graph_obligation_remains_validated(defect, monkeypatch):
    child = _row("z_child")
    parent = _row(
        "a_parent",
        delegates={"child": "z_child"},
        bounds={
            "a_valid": (growth.COUNT, "equal", _call("child")),
            "z_checked": (growth.COUNT, "equal", _call("child")),
        },
    )
    expression = parent["bounds"]["z_checked"]["expression"]
    if defect == "unknown-dimension":
        expression["arguments"]["items"] = _dimension("absent")
    elif defect == "unknown-metric":
        expression["metric"] = "absent"
    elif defect == "missing-argument":
        expression["arguments"] = {}
    elif defect == "argument-unit":
        expression["arguments"]["items"] = _constant(growth.OCTETS)
    elif defect == "multiplicity-unit":
        expression["multiplicity"] = _constant(growth.SCALAR)
    elif defect == "unbound-delegate":
        expression["name"] = "absent"
    elif defect == "unresolved-unused-edge":
        child["delegates"] = {"unused": {"declaration": _identity("absent")}}
    elif defect == "unused-invalid-metric":
        child["bounds"]["unused"] = {
            **copy.deepcopy(child["bounds"]["visits"]),
            "expression": _dimension("absent"),
        }
    elif defect == "duplicate-subject":
        child["subjects"] = list(parent["subjects"])
    elif defect == "unused-cycle":
        child["delegates"] = {"unused": {"declaration": parent["id"]}}
    else:
        raise AssertionError(defect)
    _same_metadata_rejection(_projection(parent, child), monkeypatch)


@pytest.mark.parametrize("child_first", [False, True])
def test_invalid_child_unit_keeps_original_exception_precedence(child_first, monkeypatch):
    child_name, parent_name = ("a_child", "z_parent") if child_first else ("z_child", "a_parent")
    child = _row(child_name)
    child["bounds"]["visits"]["unit"] = [["octets", 1]]
    parent = _row(
        parent_name,
        delegates={"child": child_name},
        bounds={"visits": (growth.OCTETS, "equal", _call("child"))},
    )
    _same_metadata_rejection(_projection(parent, child), monkeypatch)


@pytest.mark.parametrize("parent_relation", ["equal", "upper", "big_o"])
@pytest.mark.parametrize("child_relation", ["equal", "upper", "big_o"])
def test_each_edge_establishes_its_own_parent_relation(
    parent_relation, child_relation, monkeypatch
):
    child = _row("z_child", bounds={"visits": (growth.COUNT, child_relation, _dimension())})
    # A weaker incoming edge is valid and appears before the challenged edge.
    parent = _row(
        "a_parent",
        delegates={"child": "z_child"},
        bounds={
            "a_weak": (growth.COUNT, "big_o", _call("child")),
            "z_checked": (growth.COUNT, parent_relation, _call("child")),
        },
    )
    projection = _projection(parent, child)
    allowed = (
        parent_relation == "big_o"
        or child_relation == "equal"
        or (parent_relation == child_relation == "upper")
    )
    if allowed:
        _validate(projection)
    else:
        _same_metadata_rejection(projection, monkeypatch, match="stronger parent relation")


def test_argument_expressions_require_equal_relations_even_in_weak_parents(monkeypatch):
    target = _row("z_target")
    weak = _row("z_weak", bounds={"visits": (growth.COUNT, "upper", _dimension())})
    parent = _row(
        "a_parent",
        delegates={"target": "z_target", "weak": "z_weak"},
        bounds={
            "a_weak": (growth.COUNT, "upper", _call("weak")),
            "z_checked": (
                growth.COUNT,
                "big_o",
                _call("target", argument=_call("weak")),
            ),
        },
    )
    _same_metadata_rejection(
        _projection(parent, target, weak), monkeypatch, match="stronger parent relation"
    )


@pytest.mark.parametrize("depth", [64, 65])
@pytest.mark.parametrize("kind", ["declaration", "expression"])
def test_private_literal_compilation_retains_depth_budgets(kind, depth, monkeypatch):
    if kind == "declaration":
        rows = [
            _row(
                f"node_{index}",
                delegates={"unused": f"node_{index + 1}"} if index + 1 < depth else {},
            )
            for index in range(depth)
        ]
        projection = _projection(*rows)
    else:
        expression = _dimension()
        for _ in range(depth):
            expression = {"op": "log2_ceil", "terms": [expression]}
        projection = _projection(
            _row("deep", bounds={"visits": (growth.SCALAR, "equal", expression)})
        )
    if depth == 64:
        _validate(projection)
    else:
        _same_metadata_rejection(projection, monkeypatch, match="budget")


@pytest.mark.parametrize("nodes", [131072, 131073])
def test_private_literal_compilation_retains_complete_expression_node_budget(nodes, monkeypatch):
    projection = _projection(
        _row(
            "wide",
            bounds={
                "visits": (
                    growth.COUNT,
                    "equal",
                    {"op": "sum", "terms": [_dimension()] * (nodes - 2)},
                )
            },
        )
    )
    assert _stored_expression_nodes(projection) == nodes
    if nodes == 131072:
        _validate(projection)
    else:
        _same_metadata_rejection(projection, monkeypatch, match="bounded metadata parsing budget")


def test_mixed_graph_and_expression_depth_keeps_existing_recursion_headroom():
    rows = []
    for index in range(31):
        children = {"child": f"node_{index + 1:02d}"} if index < 30 else {}
        expression = _call("child") if children else _dimension()
        for _ in range(14):
            expression = {"op": "product", "terms": [_constant(growth.SCALAR), expression]}
        rows.append(
            _row(
                f"node_{index:02d}",
                delegates=children,
                bounds={"visits": (growth.COUNT, "equal", expression)},
            )
        )
    # Canonical order reaches the entire mixed-depth path before any child proof.
    projection = _projection(*rows)
    raw = canonical_json_bytes(projection)
    growth.validate_projection(
        json.loads(raw), numeric_policy=numeric_policy_from_environment(os.environ)
    )
    growth.validate_execution_roots(
        raw, _groups(projection), numeric_policy=numeric_policy_from_environment(os.environ)
    )


def test_literal_proofs_do_not_survive_an_invocation(monkeypatch):
    projection = _graph("shared", 4)
    raw = canonical_json_bytes(projection)
    groups = _groups(projection)
    expressions, metrics = _count_unit_work(monkeypatch)
    growth.validate_execution_roots(
        raw, groups, numeric_policy=numeric_policy_from_environment(os.environ)
    )
    first_count = sum(expressions.values())
    first_metrics = metrics.copy()
    growth.validate_execution_roots(
        raw, groups, numeric_policy=numeric_policy_from_environment(os.environ)
    )
    assert sum(expressions.values()) == first_count * 2
    assert metrics == {key: count * 2 for key, count in first_metrics.items()}
    changed = projection["declarations"][_identity("node_0")]["bounds"]["visits"]
    changed["expression"] = _dimension("absent")
    _same_metadata_rejection(projection, monkeypatch, match="unknown dimension")
    growth.validate_execution_roots(
        raw, groups, numeric_policy=numeric_policy_from_environment(os.environ)
    )


@pytest.mark.parametrize(
    "entrypoint", ["validate_projection", "validate_sample", "validate_execution_samples"]
)
def test_mapping_apis_never_use_private_literal_proofs(entrypoint, monkeypatch):
    projection = _graph("shared", 3)
    identity, samples = _groups(projection)[0]

    def forbidden(*_args):
        raise AssertionError("mapping metadata entered the private literal proof path")

    monkeypatch.setattr(growth, "_literal_expression_unit", forbidden)
    if entrypoint == "validate_projection":
        growth.validate_projection(
            projection, numeric_policy=numeric_policy_from_environment(os.environ)
        )
    elif entrypoint == "validate_sample":
        growth.validate_sample(
            projection,
            identity,
            samples[0]["dimensions"],
            samples[0]["metrics"],
            numeric_policy=numeric_policy_from_environment(os.environ),
        )
    else:
        growth.validate_execution_samples(
            projection,
            identity,
            samples,
            numeric_policy=numeric_policy_from_environment(os.environ),
        )


@pytest.mark.parametrize("name, expected", NUMERIC_CALLBACK_EXPECTATIONS)
def test_callback_apis_never_borrow_private_literal_proofs(name, expected, monkeypatch):
    def forbidden(*_args):
        raise AssertionError("observable callbacks entered the private literal proof path")

    monkeypatch.setattr(growth, "_literal_expression_unit", forbidden)
    outcome = numeric_callback_outcome(growth, name)
    if isinstance(expected, str):
        assert outcome == {
            "kind": "error",
            "type": "NumericWorkRefused"
            if expected.startswith("growth numeric work refused:")
            else "ValueError",
            "message": expected,
            "source_restored": True,
        }
    else:
        assert outcome == {"kind": "return", "value": expected, "source_restored": True}

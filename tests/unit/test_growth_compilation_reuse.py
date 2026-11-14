from __future__ import annotations

import copy
import hashlib
from collections import Counter
from dataclasses import replace

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes
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
    project_bindings,
    validate_execution_samples,
    validate_projection,
)


def graph_binding(name, *bounds, delegates=()):
    declaration = Declaration(
        id=f"fixture.{name}.growth/v1",
        authority="fixture",
        subjects=(f"fixture.{name}",),
        scope="successful-call-primitive-work",
        starts_at="accept actual input",
        ends_at="return exact output",
        dimensions=(Dimension("items", COUNT, "Actual input items."),),
        bounds=bounds,
        conditions=("Finite valid input.",),
        invariants=("Complete exact output.",),
        limitations=("No numerical elapsed-time promise.",),
    )

    def operation():
        return "exact-output"

    declare_growth(declaration, delegates=delegates)(operation)
    return growth_binding(operation)


def graph_bound(expression, *, metric="visits", relation="equal"):
    return Bound(metric, COUNT, relation, expression, "Actually observed primitive work.")


def graph_call(name, *, metric="visits", calls=1, argument=None):
    return Expression(
        "delegate",
        name=name,
        metric=metric,
        terms=(Expression("constant", value=calls, unit=COUNT),),
        arguments=(("items", argument or Expression("dimension", name="items")),),
    )


def graph_sample(binding, items, *, delegates=None):
    dimensions = {"items": items}
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(canonical_json_bytes(binding.project())).hexdigest(),
        "dimensions": dimensions,
        "metrics": {
            bound.metric: evaluate_bound(binding, bound.metric, dimensions)
            for bound in binding.declaration.bounds
        },
        "delegates": {} if delegates is None else delegates,
    }


def shared_graph(*, weak="upper", strong="equal", argument=False, strict_first=False):
    leaf = graph_binding("leaf", graph_bound(Expression("dimension", name="items")))
    loose = graph_binding(
        "loose",
        graph_bound(graph_call("leaf"), relation=weak),
        delegates=(("leaf", leaf.operation),),
    )
    strict = graph_binding(
        "strict",
        graph_bound(
            graph_call("leaf", argument=graph_call("leaf") if argument else None),
            relation=strong,
        ),
        delegates=(("leaf", leaf.operation),),
    )
    terms = (graph_call("loose"), graph_call("strict"))
    root = graph_binding(
        "root",
        graph_bound(
            Expression("sum", terms=tuple(reversed(terms)) if strict_first else terms),
            relation=weak,
        ),
        delegates=(("loose", loose.operation), ("strict", strict.operation)),
    )
    return root, loose, strict, leaf


@pytest.mark.parametrize("reverse", [False, True])
def test_projection_visits_each_shared_bound_body_once(monkeypatch, reverse):
    markers = {}

    def marked(name, metric, marker, *terms):
        markers[(f"fixture.{name}.growth/v1", marker)] = 1
        return graph_bound(
            Expression("sum", terms=(*terms, Expression("constant", value=marker, unit=COUNT))),
            metric=metric,
        )

    leaf = graph_binding(
        "leaf",
        marked("leaf", "visits", 101, Expression("dimension", name="items")),
        marked("leaf", "checks", 103, Expression("dimension", name="items")),
    )
    left = graph_binding(
        "left",
        marked("left", "visits", 107, graph_call("leaf")),
        marked("left", "checks", 109, graph_call("leaf", metric="checks")),
        delegates=(("leaf", leaf.operation),),
    )
    right = graph_binding(
        "right",
        marked("right", "visits", 113, graph_call("leaf"), graph_call("leaf", metric="checks")),
        delegates=(("leaf", leaf.operation),),
    )
    root = graph_binding(
        "root",
        marked(
            "root",
            "visits",
            127,
            graph_call("left"),
            graph_call("right"),
            graph_call("left", metric="checks"),
        ),
        delegates=(("left", left.operation), ("right", right.operation)),
    )
    projection = project_bindings((root,))
    if reverse:
        projection["declarations"] = dict(reversed(tuple(projection["declarations"].items())))
    before = canonical_json_bytes(projection)
    visited = Counter()
    original = growth._expression_unit

    def observed(expression, context, *args, **kwargs):
        key = (context.declaration.id, expression.value)
        if expression.op == "constant" and key in markers:
            visited[key] += 1
        return original(expression, context, *args, **kwargs)

    monkeypatch.setattr(growth, "_expression_unit", observed)
    for _ in range(2):
        visited.clear()
        validate_projection(projection)
        assert visited == markers
    assert canonical_json_bytes(projection) == before


@pytest.mark.parametrize(
    ("child_relation", "weak", "strong"),
    [("upper", "upper", "equal"), ("big_o", "big_o", "equal"), ("big_o", "big_o", "upper")],
)
@pytest.mark.parametrize("strict_first", [False, True])
def test_shared_bound_reuse_keeps_each_parent_relation(child_relation, weak, strong, strict_first):
    root, _, _, leaf = shared_graph(weak=weak, strong=strong, strict_first=strict_first)
    projection = project_bindings((root,))
    projection["declarations"][leaf.declaration.id]["bounds"]["visits"]["relation"] = child_relation
    with pytest.raises(ValueError, match="stronger parent relation"):
        validate_projection(projection)


@pytest.mark.parametrize("relation", ["equal", "upper", "big_o"])
def test_shared_bound_reuse_accepts_supported_relations(relation):
    root, _, _, leaf = shared_graph(weak=relation, strong=relation)
    projection = project_bindings((root,))
    projection["declarations"][leaf.declaration.id]["bounds"]["visits"]["relation"] = relation
    validate_projection(projection)


@pytest.mark.parametrize("strict_first", [False, True])
def test_shared_bound_reuse_keeps_exact_delegate_argument_relation(strict_first):
    root, _, _, leaf = shared_graph(
        weak="upper", strong="upper", argument=True, strict_first=strict_first
    )
    projection = project_bindings((root,))
    projection["declarations"][leaf.declaration.id]["bounds"]["visits"]["relation"] = "upper"
    with pytest.raises(ValueError, match="stronger parent relation"):
        validate_projection(projection)


@pytest.mark.parametrize(
    "defect", ["missing-argument", "argument-unit", "multiplicity-unit", "metric"]
)
@pytest.mark.parametrize("strict_first", [False, True])
def test_shared_bound_reuse_keeps_each_delegate_mapping(defect, strict_first):
    root, _, strict, _ = shared_graph(strict_first=strict_first)
    projection = project_bindings((root,))
    expression = projection["declarations"][strict.declaration.id]["bounds"]["visits"]["expression"]
    if defect == "missing-argument":
        expression["arguments"] = {}
    elif defect == "argument-unit":
        expression["arguments"]["items"] = Expression("constant", value=1, unit=OCTETS).project()
    elif defect == "multiplicity-unit":
        expression["multiplicity"] = Expression("constant", value=1, unit=OCTETS).project()
    else:
        expression["metric"] = "absent"
    with pytest.raises(ValueError):
        validate_projection(projection)


def execution_graph():
    leaf = graph_binding("leaf", graph_bound(Expression("dimension", name="items")))
    branch = graph_binding(
        "branch",
        graph_bound(graph_call("leaf", calls=2)),
        delegates=(("leaf", leaf.operation),),
    )
    root = graph_binding(
        "root",
        graph_bound(graph_call("branch", calls=2)),
        delegates=(("branch", branch.operation),),
    )
    child = graph_sample(leaf, 3)
    parent = graph_sample(
        branch, 3, delegates={"leaf": [copy.deepcopy(child), copy.deepcopy(child)]}
    )
    sample = graph_sample(
        root, 3, delegates={"branch": [copy.deepcopy(parent), copy.deepcopy(parent)]}
    )
    return root, branch, leaf, project_bindings((root,)), sample


@pytest.mark.parametrize(
    "defect",
    [
        "bound",
        "missing-metric",
        "extra-metric",
        "bool-metric",
        "float-metric",
        "negative-metric",
        "missing-dimension",
        "extra-dimension",
        "bool-dimension",
        "float-dimension",
        "negative-dimension",
        "missing-field",
        "extra-field",
        "wrong-declaration",
        "digest",
        "missing-delegate",
        "extra-delegate",
        "child-not-list",
        "count",
        "mapping",
    ],
)
def test_execution_batch_checks_last_nested_sample(defect):
    root, _, _, projection, sample = execution_graph()
    validate_execution_samples(projection, root.declaration.id, [sample, copy.deepcopy(sample)])
    final = copy.deepcopy(sample)
    branch = final["delegates"]["branch"][-1]
    child = branch["delegates"]["leaf"][-1]
    if defect == "bound":
        child["metrics"]["visits"] += 1
    elif defect == "missing-metric":
        child["metrics"] = {}
    elif defect == "extra-metric":
        child["metrics"]["extra"] = 0
    elif defect.endswith("metric"):
        child["metrics"]["visits"] = {
            "bool-metric": True,
            "float-metric": 3.0,
            "negative-metric": -1,
        }[defect]
    elif defect == "missing-dimension":
        child["dimensions"] = {}
    elif defect == "extra-dimension":
        child["dimensions"]["extra"] = 0
    elif defect.endswith("dimension"):
        child["dimensions"]["items"] = {
            "bool-dimension": True,
            "float-dimension": 3.0,
            "negative-dimension": -1,
        }[defect]
    elif defect == "missing-field":
        child.pop("metrics")
    elif defect == "extra-field":
        child["extra"] = 0
    elif defect == "wrong-declaration":
        child["declaration"] = root.declaration.id
    elif defect == "digest":
        child["declaration_sha256"] = "0" * 64
    elif defect == "missing-delegate":
        branch["delegates"] = {}
    elif defect == "extra-delegate":
        branch["delegates"]["extra"] = []
    elif defect == "child-not-list":
        branch["delegates"]["leaf"] = tuple(branch["delegates"]["leaf"])
    elif defect == "count":
        branch["delegates"]["leaf"].pop()
    else:
        child["dimensions"]["items"] = child["metrics"]["visits"] = 4
    with pytest.raises(ValueError):
        validate_execution_samples(projection, root.declaration.id, [sample, final])


@pytest.mark.parametrize("defect", ["expression", "metadata", "extra-field", "unresolved", "cycle"])
def test_repeated_execution_validation_rechecks_mutated_projection(defect):
    root, _, leaf, projection, sample = execution_graph()
    validate_execution_samples(projection, root.declaration.id, [sample])
    row = projection["declarations"][leaf.declaration.id]
    if defect == "expression":
        row["bounds"]["visits"]["expression"] = Expression(
            "constant", value=3, unit=COUNT
        ).project()
    elif defect == "metadata":
        row["invariants"] = ["Complete exact output and retained input."]
    elif defect == "extra-field":
        row["extra"] = 0
    elif defect == "unresolved":
        row["delegates"]["unused"] = {"declaration": "fixture.absent.growth/v1"}
    else:
        row["delegates"]["unused"] = {"declaration": root.declaration.id}
    with pytest.raises(ValueError):
        validate_execution_samples(projection, root.declaration.id, [sample])


def test_repeated_projection_validation_rechecks_relation_change():
    root, _, _, leaf = shared_graph()
    projection = project_bindings((root,))
    validate_projection(projection)
    projection["declarations"][leaf.declaration.id]["bounds"]["visits"]["relation"] = "upper"
    with pytest.raises(ValueError, match="stronger parent relation"):
        validate_projection(projection)


def test_new_projection_observes_replaced_source_binding(monkeypatch):
    root, _, leaf, before, sample = execution_graph()
    validate_execution_samples(before, root.declaration.id, [sample])
    changed = replace(
        leaf,
        declaration=replace(
            leaf.declaration,
            bounds=(graph_bound(Expression("constant", value=3, unit=COUNT)),),
        ),
    )
    monkeypatch.setattr(leaf.operation, "__riverhog_growth__", changed)
    after = project_bindings((root,))
    assert before["declarations"][root.declaration.id] == after["declarations"][root.declaration.id]
    assert before["declarations"][leaf.declaration.id] != after["declarations"][leaf.declaration.id]
    assert growth_binding(leaf.operation) is changed
    with pytest.raises(ValueError, match="stale or mismatched declaration digest"):
        validate_execution_samples(after, root.declaration.id, [sample])


def test_new_projection_rejects_replaced_delegate_owner(monkeypatch):
    root, _, _, leaf = shared_graph()
    project_bindings((root,))
    monkeypatch.setattr(
        leaf.operation, "__riverhog_growth__", replace(leaf, operation=lambda: None)
    )
    with pytest.raises(ValueError, match="exact source-owned"):
        project_bindings((root,))


def test_projection_reuse_keeps_duplicate_ownership_rejection():
    first = graph_binding("leaf", graph_bound(Expression("dimension", name="items")))
    second = graph_binding("leaf", graph_bound(Expression("dimension", name="items")))
    assert len(project_bindings((first, first))["declarations"]) == 1
    with pytest.raises(ValueError, match="multiple source owners"):
        project_bindings((first, second))
    other = graph_binding("other", graph_bound(Expression("dimension", name="items")))
    projection = project_bindings((first, other))
    projection["declarations"][other.declaration.id]["subjects"] = list(first.declaration.subjects)
    with pytest.raises(ValueError, match="multiple declaration owners"):
        validate_projection(projection)


@pytest.mark.parametrize("count", [64, 65])
def test_shared_graph_preserves_longest_path_depth_budget(count):
    leaf = graph_binding("leaf", graph_bound(Expression("dimension", name="items")))
    projection = project_bindings((leaf,))
    template = projection["declarations"][leaf.declaration.id]
    identities = [f"fixture.chain-{index:02d}.growth/v1" for index in range(count)]
    rows = {}
    for index, identity in enumerate(identities):
        row = copy.deepcopy(template)
        row["id"] = identity
        row["subjects"] = [f"fixture.chain_{index:02d}"]
        row["delegates"] = {}
        if index == 0:
            row["delegates"]["shortcut"] = {"declaration": identities[-1]}
        if index + 1 < count:
            row["delegates"]["next"] = {"declaration": identities[index + 1]}
        rows[identity] = row
    projection["declarations"] = rows
    if count == 65:
        with pytest.raises(ValueError, match="depth budget"):
            validate_projection(projection)
    else:
        validate_projection(projection)

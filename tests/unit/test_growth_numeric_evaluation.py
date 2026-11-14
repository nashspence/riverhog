from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from dataclasses import replace

import pytest
import riverhog_growth_contracts as growth

from tests.unit.test_growth_context_lookups import (
    _action,
    _case,
    _projection,
    _sample,
    apply_context_changes,
    invoke_context_action,
    lookup_bindings,
)

DIMENSIONS_ERROR = "growth dimensions must be complete nonnegative integer observations"
METRICS_ERROR = "growth sample has missing or malformed primitive observations"
BOUND_ERROR = "actual primitive observations violate the declared growth bound"
SAMPLE_ASYMPTOTIC_ERROR = "asymptotic arguments require separate proof rather than a numeric sample"
SOURCE_ASYMPTOTIC_ERROR = "asymptotic arguments are not numerical acceptance budgets"
LOG_ERROR = "growth logarithm requires a positive argument"


def _observations(
    projection, identity, dimensions, metrics, *, expected="accept", error=None, delegates=None
):
    actions = (
        _action(
            "validate_sample",
            expected,
            declaration_id=identity,
            dimensions=dimensions,
            metrics=metrics,
        ),
        _action(
            "validate_execution_samples",
            expected,
            declaration_id=identity,
            samples=[_sample(projection, identity, dimensions, metrics, delegates=delegates)],
        ),
    )
    if error is not None:
        for action in actions:
            action["expected_error"] = error
    return actions


def numeric_evaluation_cases():
    """JSON observations with literal outcomes, separate from declaration parsing cases."""
    parent, leaf = lookup_bindings()
    parent_id, leaf_id = parent.declaration.id, leaf.declaration.id
    projection = _projection(parent, leaf)
    leaf_projection = _projection(leaf)
    dimensions = {"items": 3, "width": 4, "spare": 0}
    metrics = {"visits": 3, "octets": 12, "steps": 2}

    shared = copy.deepcopy(projection)
    root = shared["declarations"][parent_id]
    child = shared["declarations"][leaf_id]
    root["dimensions"] = {"spare": copy.deepcopy(child["dimensions"]["spare"])}
    for tag in ("left", "right"):
        for name in ("items", "width"):
            root["dimensions"][tag + "_" + name] = copy.deepcopy(child["dimensions"][name])
    root["delegates"] = {tag: {"declaration": leaf_id} for tag in ("left", "right")}
    root["bounds"] = copy.deepcopy(child["bounds"])
    for metric, bound in root["bounds"].items():
        bound["expression"] = {
            "op": "sum",
            "terms": [
                {
                    "op": "delegate",
                    "name": tag,
                    "metric": metric,
                    "multiplicity": {"op": "constant", "value": 1, "unit": [["count", 1]]},
                    "arguments": {
                        "items": {"op": "dimension", "name": tag + "_items"},
                        "width": {"op": "dimension", "name": tag + "_width"},
                        "spare": {"op": "dimension", "name": "spare"},
                    },
                }
                for tag in ("left", "right")
            ],
        }
    shared_dimensions = {
        "left_items": 3,
        "left_width": 4,
        "right_items": 5,
        "right_width": 2,
        "spare": 0,
    }
    shared_metrics = {"visits": 8, "octets": 22, "steps": 5}
    for reverse in (False, True):
        current = copy.deepcopy(shared)
        root = current["declarations"][parent_id]
        if reverse:
            root["bounds"] = dict(reversed(tuple(root["bounds"].items())))
            root["delegates"] = dict(reversed(tuple(root["delegates"].items())))
            for bound in root["bounds"].values():
                bound["expression"]["terms"].reverse()
        children = {
            "left": [_sample(current, leaf_id, dimensions, metrics)],
            "right": [
                _sample(
                    current,
                    leaf_id,
                    {"items": 5, "width": 2, "spare": 0},
                    {"visits": 5, "octets": 10, "steps": 3},
                )
            ],
        }
        yield _case(
            f"shared-child-distinct-numeric-invocations-{reverse}",
            current,
            *_observations(
                current, parent_id, shared_dimensions, shared_metrics, delegates=children
            ),
        )
        last = next(reversed(root["delegates"]))
        children[last][0]["metrics"]["steps"] += 1
        yield _case(
            f"shared-child-last-observation-{reverse}",
            current,
            _action(
                "validate_execution_samples",
                "reject",
                declaration_id=parent_id,
                samples=[
                    _sample(
                        current, parent_id, shared_dimensions, shared_metrics, delegates=children
                    )
                ],
                expected_error=BOUND_ERROR,
            ),
        )

    literal_rows = (
        ({"items": 1, "width": 2, "spare": 0}, {"visits": 1, "octets": 2, "steps": 0}),
        (dimensions, metrics),
        ({"items": 5, "width": 2, "spare": 0}, {"visits": 5, "octets": 10, "steps": 3}),
    )
    yield _case(
        "each-root-uses-its-own-numeric-observations",
        leaf_projection,
        _action(
            "validate_execution_samples",
            declaration_id=leaf_id,
            samples=[
                _sample(leaf_projection, leaf_id, values, observed)
                for values, observed in literal_rows
            ],
        ),
    )
    first = _sample(leaf_projection, leaf_id, *literal_rows[0])
    for field, defect, value in (
        ("dimensions", "bool", True),
        ("dimensions", "float", 0.0),
        ("dimensions", "negative", -1),
        ("dimensions", "missing", None),
        ("dimensions", "extra", 0),
        ("metrics", "bool", True),
        ("metrics", "float", 0.0),
        ("metrics", "negative", -1),
        ("metrics", "missing", None),
        ("metrics", "extra", 0),
    ):
        last = copy.deepcopy(first)
        name = (
            "spare"
            if field == "dimensions"
            else {
                "bool": "visits",
                "float": "steps",
                "negative": "octets",
                "missing": "steps",
                "extra": "extra",
            }[defect]
        )
        if defect == "missing":
            last[field].pop(name)
        else:
            last[field]["extra" if defect == "extra" else name] = value
        yield _case(
            f"late-{field}-{defect}",
            leaf_projection,
            _action(
                "validate_execution_samples",
                "reject",
                declaration_id=leaf_id,
                samples=[first, last],
                expected_error=DIMENSIONS_ERROR if field == "dimensions" else METRICS_ERROR,
            ),
        )

    for name, asymptotic, actual_dimensions, actual_metrics, error in (
        (
            "metrics-before-relations-and-dimensions",
            "visits",
            {**dimensions, "spare": True},
            {**metrics, "steps": False},
            METRICS_ERROR,
        ),
        (
            "first-asymptotic-bound-before-dimensions",
            "visits",
            {**dimensions, "spare": True},
            metrics,
            SAMPLE_ASYMPTOTIC_ERROR,
        ),
        (
            "dimensions-before-later-asymptotic-bound",
            "steps",
            {**dimensions, "spare": True},
            metrics,
            DIMENSIONS_ERROR,
        ),
        (
            "first-violation-before-later-asymptotic-bound",
            "steps",
            dimensions,
            {**metrics, "visits": 4},
            BOUND_ERROR,
        ),
    ):
        current = copy.deepcopy(leaf_projection)
        current["declarations"][leaf_id]["bounds"][asymptotic]["relation"] = "big_o"
        yield _case(
            name,
            current,
            *_observations(
                current, leaf_id, actual_dimensions, actual_metrics, expected="reject", error=error
            ),
        )

    logarithms = (
        (1, 4, 0),
        (2, 8, 1),
        (3, 12, 2),
        (4, 16, 2),
        (8, 32, 3),
        (9, 36, 4),
        (1208925819614629174706176, 4835703278458516698824704, 80),
        (1208925819614629174706177, 4835703278458516698824708, 81),
    )
    yield _case(
        "integer-arithmetic-and-logarithm-boundaries",
        leaf_projection,
        _action(
            "validate_execution_samples",
            declaration_id=leaf_id,
            samples=[
                _sample(
                    leaf_projection,
                    leaf_id,
                    {"items": items, "width": 4, "spare": 0},
                    {"visits": items, "octets": octets, "steps": steps},
                )
                for items, octets, steps in logarithms
            ],
        ),
    )
    for name, actual_metrics, expected in (
        ("upper-below", {**metrics, "octets": 0}, "accept"),
        ("upper-equal", metrics, "accept"),
        ("upper-above", {**metrics, "octets": 13}, "reject"),
        ("equal-below", {**metrics, "visits": 2}, "reject"),
        ("equal-above", {**metrics, "visits": 4}, "reject"),
    ):
        yield _case(
            name,
            leaf_projection,
            *_observations(
                leaf_projection,
                leaf_id,
                dimensions,
                actual_metrics,
                expected=expected,
                error=BOUND_ERROR if expected == "reject" else None,
            ),
        )

    zero_dimensions = {"items": 0, "width": 4, "spare": 0}
    zero_metrics = {"visits": 0, "octets": 0, "steps": 0}
    yield _case(
        "zero-logarithm-is-not-a-numeric-budget",
        leaf_projection,
        *_observations(
            leaf_projection,
            leaf_id,
            zero_dimensions,
            zero_metrics,
            expected="reject",
            error=LOG_ERROR,
        ),
    )
    for operator, constant, positive in (("product", 0, 0), ("maximum", 17, 17)):
        current = copy.deepcopy(leaf_projection)
        expression = current["declarations"][leaf_id]["bounds"]["steps"]["expression"]
        current["declarations"][leaf_id]["bounds"]["steps"]["expression"] = {
            "op": operator,
            "terms": [{"op": "constant", "value": constant, "unit": []}, expression],
        }
        yield _case(
            operator + "-still-evaluates-all-operands",
            current,
            *_observations(
                current, leaf_id, zero_dimensions, zero_metrics, expected="reject", error=LOG_ERROR
            ),
            *_observations(
                current,
                leaf_id,
                {"items": 1, "width": 4, "spare": 0},
                {"visits": 1, "octets": 4, "steps": positive},
            ),
        )

    zero_delegate = copy.deepcopy(projection)
    root = zero_delegate["declarations"][parent_id]
    expression = copy.deepcopy(root["bounds"]["visits"]["expression"])
    expression["metric"] = "steps"
    bound = copy.deepcopy(zero_delegate["declarations"][leaf_id]["bounds"]["steps"])
    bound["expression"] = expression
    root["bounds"] = {"steps": bound}
    yield _case(
        "zero-multiplicity-still-evaluates-child-numeric-domain",
        zero_delegate,
        *_observations(
            zero_delegate,
            parent_id,
            {**zero_dimensions, "calls": 0},
            {"steps": 0},
            expected="reject",
            error=LOG_ERROR,
            delegates={"leaf": []},
        ),
        *_observations(
            zero_delegate,
            parent_id,
            {"items": 1, "width": 4, "spare": 0, "calls": 0},
            {"steps": 0},
            delegates={"leaf": []},
        ),
    )


@pytest.mark.parametrize("case", tuple(numeric_evaluation_cases()), ids=lambda case: case["name"])
def test_numeric_evaluation_preserves_each_public_invocation(case):
    projection = copy.deepcopy(case["projection"])
    for action in case["actions"]:
        apply_context_changes(projection, action)
        before = json.dumps([projection, action], sort_keys=True, allow_nan=False)
        if action["expected"] == "reject":
            with pytest.raises(ValueError) as caught:
                invoke_context_action(growth, projection, action)
            if "expected_error" in action:
                assert str(caught.value) == action["expected_error"]
        else:
            assert invoke_context_action(growth, projection, action) is None
        assert json.dumps([projection, action], sort_keys=True, allow_nan=False) == before


class _CallbackMapping(Mapping):
    def __init__(self, values, *, on_get=None, on_values=None):
        self.data, self.on_get, self.on_values = values, on_get, on_values

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def __getitem__(self, name):
        result = self.data[name]
        if self.on_get is not None:
            self.on_get(name)
        return result

    def values(self):
        if self.on_values is not None:
            self.on_values()
        return self.data.values()


class _CallbackDict(dict):
    def __init__(self, values, *, on_get):
        super().__init__(values)
        self.on_get = on_get

    def __getitem__(self, name):
        result = super().__getitem__(name)
        self.on_get(name)
        return result


class _HashName(str):
    def __new__(cls, value, on_hash):
        result = super().__new__(cls, value)
        result.on_hash = on_hash
        return result

    def __hash__(self):
        self.on_hash()
        return str.__hash__(self)


class _ComparedName(str):
    def __new__(cls, value, on_compare):
        result = super().__new__(cls, value)
        result.on_compare = on_compare
        return result

    __hash__ = str.__hash__

    def __eq__(self, other):
        self.on_compare()
        return str.__eq__(self, other)


def numeric_callback_outcome(reader, name):
    """Run public callback controls with fresh reader types and restore every source edit."""
    parent, leaf = lookup_bindings(reader)
    projection = _projection(leaf)
    dimensions = {"items": 3, "width": 4, "spare": 0}
    metrics = {"visits": 3, "octets": 12, "steps": 2}
    original = leaf.declaration.bounds
    doubled = replace(
        original[0],
        expression=reader.Expression(
            "sum",
            terms=(
                reader.Expression("dimension", name="items"),
                reader.Expression("dimension", name="items"),
            ),
        ),
    )
    changed = (doubled, *original[1:])

    def replace_bounds():
        object.__setattr__(leaf.declaration, "bounds", changed)

    def invalidate_unused(trigger):
        def mutate(key):
            if key == trigger:
                dimensions["spare"] = True

        return mutate

    def invoke():
        if name.startswith("metrics-"):
            kind = _CallbackDict if name == "metrics-dict-mutation" else _CallbackMapping
            observed = kind(metrics, on_get=invalidate_unused("visits"))
            return reader.validate_sample(projection, leaf.declaration.id, dimensions, observed)
        if name.startswith("dimensions-"):
            kind = _CallbackDict if name == "dimensions-dict-mutation" else _CallbackMapping
            observed = kind(dimensions, on_get=invalidate_unused("items"))
            if kind is _CallbackDict:

                def mutate(key):
                    if key == "items":
                        observed["spare"] = True

                observed.on_get = mutate
            return reader.validate_sample(projection, leaf.declaration.id, observed, metrics)
        if name in {
            "metadata-dimension-name-hash",
            "metadata-metric-name-hash",
            "metadata-string-subclasses-accepted",
            "observation-name-comparison",
        }:
            armed = False

            def arm():
                nonlocal armed
                armed = True

            def change_dimension():
                if armed:
                    dimensions["spare"] = True

            def change_metric():
                if armed:
                    metrics["visits"] = 4

            def unchanged():
                return None

            declaration = projection["declarations"][leaf.declaration.id]
            if name == "metadata-dimension-name-hash":
                data = declaration["dimensions"].pop("spare")
                declaration["dimensions"][_HashName("spare", change_dimension)] = data
            elif name == "metadata-metric-name-hash":
                data = declaration["bounds"].pop("steps")
                declaration["bounds"][_HashName("steps", change_metric)] = data
            elif name == "metadata-string-subclasses-accepted":
                for field, key in (("dimensions", "spare"), ("bounds", "steps")):
                    data = declaration[field].pop(key)
                    declaration[field][_HashName(key, unchanged)] = data
            else:
                value = dimensions.pop("spare")
                dimensions[_ComparedName("spare", change_metric)] = value
            return reader.validate_sample(
                projection,
                leaf.declaration.id,
                dimensions,
                _CallbackMapping(metrics, on_values=arm),
            )
        if name in {"source-bound-replacement", "source-child-bound-replacement"}:
            selected = parent if name == "source-child-bound-replacement" else leaf
            values = {**dimensions, "calls": 2} if selected is parent else dimensions
            observed = _CallbackMapping(values, on_values=replace_bounds)
            return reader.evaluate_bound(selected, "visits", observed)
        if name == "source-bound-removal":

            def remove_bound():
                object.__setattr__(leaf.declaration, "bounds", original[1:])

            return reader.evaluate_bound(
                leaf, "visits", _CallbackMapping(dimensions, on_values=remove_bound)
            )
        if name == "source-metric-before-dimensions":
            return reader.evaluate_bound(leaf, "absent", {**dimensions, "spare": True})
        if name in {"source-big-o-invalid-dimensions", "source-big-o-valid-dimensions"}:
            object.__setattr__(
                leaf.declaration, "bounds", (replace(original[0], relation="big_o"), *original[1:])
            )
            values = (
                {**dimensions, "spare": True}
                if name == "source-big-o-invalid-dimensions"
                else dimensions
            )
            return reader.evaluate_bound(leaf, "visits", values)
        if name == "source-between-calls":
            before = reader.evaluate_bound(leaf, "visits", dimensions)
            replace_bounds()
            after = reader.evaluate_bound(leaf, "visits", dimensions)
            reader.validate_sample(projection, leaf.declaration.id, dimensions, metrics)
            object.__setattr__(leaf.declaration, "bounds", original)
            restored = reader.evaluate_bound(leaf, "visits", dimensions)
            return [before, after, restored]
        raise AssertionError(name)

    try:
        try:
            outcome = {"kind": "return", "value": invoke()}
        except Exception as error:
            outcome = {"kind": "error", "type": type(error).__name__, "message": str(error)}
    finally:
        object.__setattr__(leaf.declaration, "bounds", original)
    outcome["source_restored"] = leaf.declaration.bounds is original
    return outcome


NUMERIC_CALLBACK_EXPECTATIONS = (
    ("metrics-mapping-mutation", DIMENSIONS_ERROR),
    ("metrics-dict-mutation", DIMENSIONS_ERROR),
    ("dimensions-mapping-mutation", DIMENSIONS_ERROR),
    ("dimensions-dict-mutation", DIMENSIONS_ERROR),
    ("metadata-dimension-name-hash", DIMENSIONS_ERROR),
    ("metadata-metric-name-hash", BOUND_ERROR),
    ("metadata-string-subclasses-accepted", None),
    ("observation-name-comparison", BOUND_ERROR),
    ("source-bound-replacement", 6),
    ("source-child-bound-replacement", 12),
    ("source-bound-removal", "growth metric is not declared"),
    ("source-metric-before-dimensions", "growth metric is not declared"),
    ("source-big-o-invalid-dimensions", DIMENSIONS_ERROR),
    ("source-big-o-valid-dimensions", SOURCE_ASYMPTOTIC_ERROR),
    ("source-between-calls", [3, 6, 3]),
)


@pytest.mark.parametrize("name, expected", NUMERIC_CALLBACK_EXPECTATIONS)
def test_numeric_callbacks_keep_live_source_and_validation_order(name, expected):
    outcome = numeric_callback_outcome(growth, name)
    if isinstance(expected, str):
        assert outcome == {
            "kind": "error",
            "type": "ValueError",
            "message": expected,
            "source_restored": True,
        }
    else:
        assert outcome == {"kind": "return", "value": expected, "source_restored": True}

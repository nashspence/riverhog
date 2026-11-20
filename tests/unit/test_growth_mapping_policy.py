from __future__ import annotations

from collections.abc import Mapping

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes

from tests.unit.test_growth_numeric_budget import LiveMapping, leaf, policy, shared_graph
from tests.unit.test_growth_observer_batch import (
    graph_binding,
    graph_bound,
    graph_call,
    leaf_binding,
)


@pytest.mark.parametrize(
    "entry",
    [
        "validate_projection",
        "validate_sample",
        "validate_execution_samples",
        "validate_execution_roots",
        "project_binding",
        "project_bindings",
        "evaluate_bound",
    ],
)
def test_every_post_registration_reader_requires_owned_configuration(entry):
    data, groups = leaf()
    identity, originals = groups[0]
    original = originals[0]
    binding = leaf_binding()
    args = {
        "validate_projection": (data,),
        "validate_sample": (data, identity, original["dimensions"], original["metrics"]),
        "validate_execution_samples": (data, identity, originals),
        "validate_execution_roots": (canonical_json_bytes(data), groups),
        "project_binding": (binding,),
        "project_bindings": ((binding,),),
        "evaluate_bound": (binding, "visits", {"items": 3}),
    }[entry]
    with pytest.raises(TypeError, match="numeric_policy"):
        getattr(growth, entry)(*args)
    with pytest.raises(ValueError, match="exact work policy"):
        getattr(growth, entry)(*args, numeric_policy=None)


def test_mapping_unit_expansion_is_charged_from_the_stored_graph():
    data, _groups = shared_graph(3)
    usage = growth.validate_projection(data, numeric_policy=policy())
    # One leaf plus three stored seven-node bodies. The old Mapping unit
    # traversal visits T(0)=1; T(n)=7+2*T(n-1), for every declaration root.
    assert usage.admission_nodes == 22 + 4
    assert usage.unit_nodes == 1 + 9 + 25 + 57
    assert usage.metric_calls == 0
    assert usage.source_contexts == 0
    assert usage.consumed == usage.payload_work + usage.meter_work


def test_mapping_unit_budget_refuses_before_original_observation():
    data, groups = shared_graph(5)
    identity, originals = groups[0]
    events = []
    originals[0]["dimensions"] = LiveMapping({"items": 1}, events)
    with pytest.raises(growth.NumericWorkRefused) as caught:
        growth.validate_execution_samples(
            data, identity, originals, numeric_policy=policy(max_work=5000)
        )
    assert caught.value.reason == "work_allowance_exhausted"
    assert caught.value.operation.startswith("unit_") or caught.value.operation == "argument_keys"
    assert caught.value.consumed <= 5000
    assert events == []


def _mapping_name_outcome(kind, *, metered):
    events = []
    armed = False
    data, groups = leaf()
    identity, originals = groups[0]
    original = originals[0]
    row = data["declarations"][identity]

    class Name(str):
        def __len__(self):
            raise AssertionError("meter introduced an original name length callback")

        def __hash__(self):
            events.append("hash:" + str(self))
            if armed and kind == "dimension":
                original["dimensions"]["items"] = True
            if armed and kind == "metric":
                original["metrics"].data["visits"] = 4
            return str.__hash__(self)

        def __eq__(self, other):
            events.append("equal:" + str(self))
            return str.__eq__(self, other)

    class ObservedMetrics(LiveMapping):
        def values(self):
            nonlocal armed
            armed = True
            return super().values()

    for field, key in (("dimensions", "items"), ("bounds", "visits")):
        value = row[field].pop(key)
        row[field][Name(key)] = value
    original["metrics"] = ObservedMetrics({"visits": 3}, events, label="metrics")
    events.clear()
    try:
        if metered:
            growth.validate_sample(
                data,
                identity,
                original["dimensions"],
                original["metrics"],
                numeric_policy=policy(),
            )
        else:
            context = growth._projection_contexts(data)[identity]
            growth._validate_sample(context, original["dimensions"], original["metrics"])
        outcome = "accepted"
    except ValueError as error:
        outcome = type(error).__name__, str(error)
    return outcome, events


@pytest.mark.parametrize("kind", ["unchanged", "dimension", "metric"])
def test_mapping_names_retain_original_hash_equality_and_mutation_order(kind):
    original = _mapping_name_outcome(kind, metered=False)
    metered = _mapping_name_outcome(kind, metered=True)
    assert metered == original
    assert metered[1]
    if kind == "unchanged":
        assert metered[0] == "accepted"
    elif kind == "dimension":
        assert metered[0] == (
            "ValueError",
            "growth dimensions must be complete nonnegative integer observations",
        )
    else:
        assert metered[0] == (
            "ValueError",
            "actual primitive observations violate the declared growth bound",
        )


@pytest.mark.parametrize(
    "relation, expected",
    [
        ("equal", [("relation", "equal"), ("metric", "visits"), ("relation", "upper")]),
        ("upper", [("relation", "equal"), ("relation", "upper"), ("metric", "visits")]),
    ],
)
def test_mapping_relation_callbacks_keep_original_metric_read_order(relation, expected):
    data, groups = leaf()
    identity, originals = groups[0]
    original = originals[0]
    events = []
    armed = False

    class Relation(str):
        __hash__ = str.__hash__

        def __eq__(self, other):
            if armed:
                events.append(("relation", other))
            return str.__eq__(self, other)

    class Dimensions(dict):
        def __getitem__(self, key):
            nonlocal armed
            value = super().__getitem__(key)
            armed = True
            return value

    class Metrics(dict):
        def __getitem__(self, key):
            events.append(("metric", key))
            return super().__getitem__(key)

    data["declarations"][identity]["bounds"]["visits"]["relation"] = Relation(relation)
    growth.validate_sample(
        data,
        identity,
        Dimensions(original["dimensions"]),
        Metrics(original["metrics"]),
        numeric_policy=policy(),
    )
    assert events == expected


def test_mapping_upper_relation_keeps_its_original_exception_before_metric_read():
    data, groups = leaf()
    identity, originals = groups[0]
    original = originals[0]
    armed = False
    events = []

    class OriginalStop(BaseException):
        pass

    failure = OriginalStop("original upper-relation equality")

    class Relation(str):
        __hash__ = str.__hash__

        def __eq__(self, other):
            if armed:
                events.append(("relation", other))
                if other == "upper":
                    raise failure
            return str.__eq__(self, other)

    class Dimensions(dict):
        def __getitem__(self, key):
            nonlocal armed
            value = super().__getitem__(key)
            armed = True
            return value

    class Metrics(dict):
        def __getitem__(self, key):
            events.append(("metric", key))
            return super().__getitem__(key)

    data["declarations"][identity]["bounds"]["visits"]["relation"] = Relation("upper")
    with pytest.raises(OriginalStop) as caught:
        growth.validate_sample(
            data,
            identity,
            Dimensions(original["dimensions"]),
            Metrics(original["metrics"]),
            numeric_policy=policy(),
        )
    assert caught.value is failure
    assert events == [("relation", "equal"), ("relation", "upper")]


def test_mapping_unit_exponents_have_an_explicit_width_boundary():
    data, groups = leaf()
    row = data["declarations"][groups[0][0]]
    row["dimensions"]["items"]["unit"] = [["count", 4096]]
    row["bounds"]["visits"]["unit"] = [["count", 4096]]
    usage = growth.validate_projection(data, numeric_policy=policy())
    assert usage.maximum_operand_bits == 13
    with pytest.raises(growth.NumericWorkRefused) as caught:
        growth.validate_projection(data, numeric_policy=policy(max_integer_bits=8))
    assert caught.value.reason == "operand_width"
    assert caught.value.operation == "metadata_unit"


@pytest.mark.parametrize("shape", ["binding", "dimensions"])
def test_unsupported_source_shapes_refuse_without_new_attribute_or_iterator_callbacks(shape):
    events = []
    binding = leaf_binding()

    class BindingSubclass(growth.Binding):
        def __getattribute__(self, name):
            events.append(name)
            return super().__getattribute__(name)

    class Dimensions(tuple):
        def __iter__(self):
            events.append("iterate")
            return super().__iter__()

        def __len__(self):
            events.append("length")
            return super().__len__()

    original = binding.declaration.dimensions
    if shape == "binding":
        binding = BindingSubclass(binding.declaration, binding.operation, binding.delegates)
    else:
        object.__setattr__(binding.declaration, "dimensions", Dimensions(original))
    events.clear()
    try:
        with pytest.raises(growth.NumericWorkRefused) as caught:
            growth.evaluate_bound(binding, "visits", {"items": 3}, numeric_policy=policy())
    finally:
        if shape == "dimensions":
            object.__setattr__(binding.declaration, "dimensions", original)
    assert caught.value.reason == "unsupported_source_record"
    assert events == []


def test_source_context_expansion_refuses_before_original_numeric_callbacks():
    binding = leaf_binding()
    for level in range(4):
        binding = graph_binding(
            f"source_policy_{level}",
            graph_bound(
                growth.Expression(
                    "sum", terms=(graph_call("left", calls=0), graph_call("right", calls=0))
                )
            ),
            delegates=(("left", binding.operation), ("right", binding.operation)),
        )
    events = []
    observations = LiveMapping({"items": 3}, events)
    assert isinstance(observations, Mapping)
    with pytest.raises(growth.NumericWorkRefused) as caught:
        growth.evaluate_bound(binding, "visits", observations, numeric_policy=policy(max_work=2000))
    assert caught.value.reason == "work_allowance_exhausted"
    assert caught.value.operation.startswith(("source_", "plan_"))
    assert events == []

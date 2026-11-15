from __future__ import annotations

import copy
import os
from collections import Counter

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes

from scripts.validation_numeric import numeric_policy_from_environment
from tests.unit.test_growth_context_lookups import apply_context_changes, context_lookup_cases
from tests.unit.test_growth_numeric_evaluation import numeric_evaluation_cases
from tests.unit.test_growth_observer_batch import (
    SampleBatch,
    assert_released,
    graph_binding,
    graph_bound,
    graph_call,
    leaf_binding,
    literal_sample,
    parent_bindings,
    projection_bindings,
)


def metadata(*bindings):
    return canonical_json_bytes(
        growth.project_bindings(
            bindings, numeric_policy=numeric_policy_from_environment(os.environ)
        )
    )


def test_each_original_occurrence_and_group_order_is_checked(monkeypatch):
    parent, child = parent_bindings()
    leaf = literal_sample(child)
    root = literal_sample(parent, children={"leaf": [leaf]})
    original = growth._validate_sample
    checked = []

    def validate(context, dimensions, metrics):
        checked.append((context.declaration.id, id(dimensions)))
        original(context, dimensions, metrics)

    monkeypatch.setattr(growth, "_validate_sample", validate)
    growth.validate_execution_roots(
        metadata(parent),
        [(child.declaration.id, [leaf, leaf]), (parent.declaration.id, [root])],
        numeric_policy=numeric_policy_from_environment(os.environ),
    )
    assert checked == [
        (child.declaration.id, id(leaf["dimensions"])),
        (child.declaration.id, id(leaf["dimensions"])),
        (parent.declaration.id, id(root["dimensions"])),
        (child.declaration.id, id(leaf["dimensions"])),
    ]


def test_schedule_is_fixed_but_original_later_values_remain_live(monkeypatch):
    leaf = leaf_binding()
    first, second, appended = (literal_sample(leaf) for _ in range(3))
    groups = [(leaf.declaration.id, [first]), (leaf.declaration.id, [second])]
    original = growth._validate_sample

    def validate(context, dimensions, metrics):
        if dimensions is first["dimensions"]:
            # These schedule changes cannot drop or add reader obligations.
            groups[1][1].clear()
            groups.append((leaf.declaration.id, [appended]))
            second["metrics"]["visits"] = 4
        original(context, dimensions, metrics)

    monkeypatch.setattr(growth, "_validate_sample", validate)
    with pytest.raises(ValueError, match="declared growth bound"):
        growth.validate_execution_roots(
            metadata(leaf), groups, numeric_policy=numeric_policy_from_environment(os.environ)
        )


@pytest.mark.parametrize(
    "groups",
    [None, (), [], [None], [("id",)], [("id", [])], [["id", [{}]]], [(True, [{}])]],
)
def test_schedule_is_finite_explicit_and_nonempty(groups):
    with pytest.raises(ValueError, match="root group"):
        growth.validate_execution_roots(
            metadata(leaf_binding()),
            groups,
            numeric_policy=numeric_policy_from_environment(os.environ),
        )


@pytest.mark.parametrize("kind", ["mapping", "text", "bytearray", "space", "duplicate", "number"])
def test_metadata_requires_strict_canonical_literal_bytes(kind):
    raw = metadata(leaf_binding())
    value = (
        {}
        if kind == "mapping"
        else raw.decode()
        if kind == "text"
        else bytearray(raw)
        if kind == "bytearray"
        else raw + b" "
        if kind == "space"
        else b'{"format":0,"format":1}'
        if kind == "duplicate"
        else b'{"bad":NaN}'
    )
    with pytest.raises(ValueError):
        growth.validate_execution_roots(
            value, [("id", [{}])], numeric_policy=numeric_policy_from_environment(os.environ)
        )


def test_unused_metadata_is_checked_before_any_root(monkeypatch):
    parent, child, unused = projection_bindings()
    projection = growth.project_bindings(
        (parent,), numeric_policy=numeric_policy_from_environment(os.environ)
    )
    projection["declarations"][unused.declaration.id]["bounds"]["visits"]["unit"] = [["octets", 1]]

    def forbidden(*_args):
        raise AssertionError("root validation started before full metadata validation")

    monkeypatch.setattr(growth, "_validate_sample", forbidden)
    with pytest.raises(ValueError, match="incompatible units"):
        growth.validate_execution_roots(
            canonical_json_bytes(projection),
            [(child.declaration.id, [literal_sample(child)])],
            numeric_policy=numeric_policy_from_environment(os.environ),
        )


def test_each_call_freshly_reads_changed_metadata_and_private_contexts(monkeypatch):
    leaf = leaf_binding()
    projection = growth.project_bindings(
        (leaf,), numeric_policy=numeric_policy_from_environment(os.environ)
    )
    raw = canonical_json_bytes(projection)
    original = growth._projection_contexts
    decoded = []

    def compile_projection(value, **kwargs):
        assert all(value is not prior for prior in decoded)
        contexts = original(value, **kwargs)
        decoded.append(value)
        return contexts

    monkeypatch.setattr(growth, "_projection_contexts", compile_projection)
    groups = [(leaf.declaration.id, [literal_sample(leaf)])]
    growth.validate_execution_roots(
        raw, groups, numeric_policy=numeric_policy_from_environment(os.environ)
    )
    decoded[-1]["format"] = "changed after first invocation"
    growth.validate_execution_roots(
        raw, groups, numeric_policy=numeric_policy_from_environment(os.environ)
    )
    projection["declarations"][leaf.declaration.id]["subjects"] = []
    with pytest.raises(ValueError):
        growth.validate_execution_roots(
            canonical_json_bytes(projection),
            groups,
            numeric_policy=numeric_policy_from_environment(os.environ),
        )
    assert len(decoded) == 2


def test_each_root_gets_the_full_node_budget_without_deduplicating_children():
    child = leaf_binding()
    count = 65535
    parent = graph_binding(
        "root_budget",
        graph_bound(graph_call("leaf", calls=count)),
        delegates=(("leaf", child.operation),),
    )
    leaf = literal_sample(child)
    root = literal_sample(parent, visits=3 * count, children={"leaf": [leaf] * count})
    raw = metadata(parent)
    # Two independently checked roots exceed a shared 65,536-node budget.
    growth.validate_execution_roots(
        raw,
        [(parent.declaration.id, [root, root])],
        numeric_policy=numeric_policy_from_environment(os.environ),
    )
    root["delegates"]["leaf"].append(leaf)
    with pytest.raises(ValueError, match="reader budget"):
        growth.validate_execution_roots(
            raw,
            [(parent.declaration.id, [root])],
            numeric_policy=numeric_policy_from_environment(os.environ),
        )


def test_failed_multi_root_flush_keeps_original_exception_and_receives_scope(monkeypatch):
    class Stop(BaseException):
        pass

    failure = Stop("original receiving interrupted")
    binding = leaf_binding()
    first, last = literal_sample(binding), literal_sample(binding)
    original = growth._validate_sample

    def validate(context, dimensions, metrics):
        if dimensions is last["dimensions"]:
            raise failure
        original(context, dimensions, metrics)

    monkeypatch.setattr(growth, "_validate_sample", validate)
    with pytest.raises(Stop) as caught:
        with SampleBatch(
            (binding.operation,),
            limit=2,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
            batch.receive(binding.operation, first)
            batch.receive(binding.operation, last)
    assert caught.value is failure
    assert batch.validation_attempts == 1
    assert batch.validated_samples == batch.validation_calls == batch.validation_groups == 0
    assert_released(batch)


@pytest.mark.parametrize(
    "case",
    [
        case
        for case in [*context_lookup_cases(), *numeric_evaluation_cases()]
        if any(action["entrypoint"] == "validate_execution_samples" for action in case["actions"])
    ],
    ids=lambda case: case["name"],
)
def test_literal_execution_controls_keep_existing_results(case):
    data = copy.deepcopy(case)
    projection = data["projection"]
    for action in data["actions"]:
        apply_context_changes(projection, action)
        if action["entrypoint"] != "validate_execution_samples":
            continue
        accepted = action["expected"] == "accept"
        groups = [(action["declaration_id"], action["samples"])]
        if accepted:
            growth.validate_execution_roots(
                canonical_json_bytes(projection),
                groups,
                numeric_policy=numeric_policy_from_environment(os.environ),
            )
        else:
            with pytest.raises(ValueError):
                growth.validate_execution_roots(
                    canonical_json_bytes(projection),
                    groups,
                    numeric_policy=numeric_policy_from_environment(os.environ),
                )


def test_literal_row_identity_work_depends_on_distinct_rows_per_invocation(monkeypatch):
    parent, child = parent_bindings()
    leaf = literal_sample(child)
    root = literal_sample(parent, children={"leaf": [leaf]})
    raw = metadata(parent)
    groups = [(child.declaration.id, [leaf, leaf]), (parent.declaration.id, [root, root])]
    encoded = Counter()
    checked = []
    original_encode = growth.canonical_json_bytes
    original_validate = growth._validate_sample

    def encode(value):
        if isinstance(value, dict) and "bounds" in value:
            encoded[value["id"]] += 1
        return original_encode(value)

    def validate(context, dimensions, metrics):
        checked.append(context.declaration.id)
        return original_validate(context, dimensions, metrics)

    monkeypatch.setattr(growth, "canonical_json_bytes", encode)
    monkeypatch.setattr(growth, "_validate_sample", validate)
    for _ in range(2):
        growth.validate_execution_roots(
            raw, groups, numeric_policy=numeric_policy_from_environment(os.environ)
        )
    assert encoded == {child.declaration.id: 2, parent.declaration.id: 2}
    assert (
        checked
        == [
            child.declaration.id,
            child.declaration.id,
            parent.declaration.id,
            child.declaration.id,
            parent.declaration.id,
            child.declaration.id,
        ]
        * 2
    )


@pytest.mark.parametrize("change", ["digest", "metrics", "delegates"])
def test_later_root_acceptance_stays_live_after_literal_row_identity(monkeypatch, change):
    leaf = leaf_binding()
    first, second = literal_sample(leaf), literal_sample(leaf)
    original = growth._validate_sample

    def validate(context, dimensions, metrics):
        if dimensions is first["dimensions"]:
            if change == "digest":
                second["declaration_sha256"] = "0" * 64
            elif change == "metrics":
                second["metrics"]["visits"] = 4
            else:
                second["delegates"]["unexpected"] = []
        return original(context, dimensions, metrics)

    monkeypatch.setattr(growth, "_validate_sample", validate)
    with pytest.raises(ValueError):
        growth.validate_execution_roots(
            metadata(leaf),
            [(leaf.declaration.id, [first, second])],
            numeric_policy=numeric_policy_from_environment(os.environ),
        )


@pytest.mark.parametrize("literal", [False, True])
def test_declaration_identity_is_fresh_after_metadata_changes(monkeypatch, literal):
    leaf = leaf_binding()
    projection = growth.project_bindings(
        (leaf,), numeric_policy=numeric_policy_from_environment(os.environ)
    )
    first, second = literal_sample(leaf), literal_sample(leaf)
    original = growth._validate_sample

    def validate(context, dimensions, metrics):
        if dimensions is first["dimensions"]:
            projection["declarations"][leaf.declaration.id]["subjects"] = ["fixture.changed"]
        return original(context, dimensions, metrics)

    monkeypatch.setattr(growth, "_validate_sample", validate)
    if literal:
        # Caller metadata aliases cannot change an invocation's private literal rows.
        growth.validate_execution_roots(
            canonical_json_bytes(projection),
            [(leaf.declaration.id, [first, second])],
            numeric_policy=numeric_policy_from_environment(os.environ),
        )
        with pytest.raises(ValueError, match="declaration digest"):
            growth.validate_execution_roots(
                canonical_json_bytes(projection),
                [(leaf.declaration.id, [second])],
                numeric_policy=numeric_policy_from_environment(os.environ),
            )
    else:
        # The mapping API still observes metadata changes between original roots.
        with pytest.raises(ValueError, match="declaration digest"):
            growth.validate_execution_samples(
                projection,
                leaf.declaration.id,
                [first, second],
                numeric_policy=numeric_policy_from_environment(os.environ),
            )

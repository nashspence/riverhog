from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter
from contextlib import closing
from dataclasses import replace

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
    project_bindings,
    validate_execution_samples,
)

from tests.support import growth_batch
from tests.support.growth_batch import SampleBatch


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


def leaf_binding():
    return graph_binding("batch_leaf", graph_bound(Expression("dimension", name="items")))


def parent_bindings():
    child = leaf_binding()
    parent = graph_binding(
        "batch_parent",
        graph_bound(graph_call("leaf")),
        delegates=(("leaf", child.operation),),
    )
    return parent, child


def literal_sample(binding, *, items=3, visits=3, children=None):
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(canonical_json_bytes(binding.project())).hexdigest(),
        "dimensions": {"items": items},
        "metrics": {"visits": visits},
        "delegates": {} if children is None else children,
    }


def assert_released(batch):
    assert batch.state == "closed"
    assert not batch._pending
    assert not batch._source
    assert not batch._roots
    assert not batch._digests
    assert batch._projection_bytes is None


def observed_validations(monkeypatch):
    calls = []
    original = growth_batch.validate_execution_samples

    def validate(projection, identity, samples):
        original(projection, identity, samples)
        calls.append((identity, tuple(samples)))

    monkeypatch.setattr(growth_batch, "validate_execution_samples", validate)
    return calls


def test_bounded_batch_checks_every_root_without_compiling_each_emission(monkeypatch):
    binding = leaf_binding()
    calls = observed_validations(monkeypatch)
    compilations = []
    original = growth._projection_contexts

    def compile_projection(projection):
        compilations.append(projection)
        return original(projection)

    monkeypatch.setattr(growth, "_projection_contexts", compile_projection)
    with SampleBatch((binding.operation,), limit=3) as batch:
        assert len(compilations) == 1
        for items in range(1, 9):
            batch.sample(binding.operation, {"items": items}, {"visits": items})
            assert len(compilations) == 1 + items // 3
            assert len(batch._pending) == items % 3
            assert batch.validated_samples == items - items % 3
    assert [len(samples) for _, samples in calls] == [3, 3, 2]
    assert [sample["metrics"]["visits"] for _, samples in calls for sample in samples] == list(
        range(1, 9)
    )
    assert len(compilations) == 4
    assert batch.validated_samples == 8 and batch.validation_calls == 3
    assert_released(batch)


def test_each_flush_uses_a_fresh_projection_from_admitted_bytes(monkeypatch):
    binding = leaf_binding()
    original = growth_batch.validate_execution_samples
    received = []

    def validate(projection, identity, samples):
        assert all(projection is not previous for previous in received)
        original(projection, identity, samples)
        received.append(projection)
        projection["format"] = "changed after this reader call"

    monkeypatch.setattr(growth_batch, "validate_execution_samples", validate)
    with SampleBatch((binding.operation,), limit=1) as batch:
        assert isinstance(batch._projection_bytes, bytes)
        batch.sample(binding.operation, {"items": 2}, {"visits": 2})
        batch.sample(binding.operation, {"items": 3}, {"visits": 3})
    assert len(received) == 2
    assert batch.validated_samples == 2
    assert_released(batch)


def test_completed_child_and_parent_are_independently_received(monkeypatch):
    parent, child = parent_bindings()
    calls = observed_validations(monkeypatch)
    with SampleBatch((parent.operation, child.operation), limit=4) as batch:
        observed_child = batch.sample(child.operation, {"items": 3}, {"visits": 3})
        observed_parent = batch.sample(
            parent.operation, {"items": 3}, {"visits": 3}, {"leaf": [observed_child]}
        )
    assert [(identity, len(samples)) for identity, samples in calls] == [
        (child.declaration.id, 1),
        (parent.declaration.id, 1),
    ]
    assert observed_parent["delegates"]["leaf"] == [observed_child]
    assert batch.validated_samples == 2
    assert_released(batch)


def test_completed_child_is_checked_when_parent_dies_before_emitting(monkeypatch):
    parent, child = parent_bindings()
    calls = observed_validations(monkeypatch)
    failure = RuntimeError("parent did not complete")
    with pytest.raises(RuntimeError) as caught:
        with SampleBatch((parent.operation, child.operation), limit=4) as batch:
            observed = batch.sample(child.operation, {"items": 3}, {"visits": 3})
            raise failure
    assert caught.value is failure
    assert calls == [(child.declaration.id, (observed,))]
    assert batch.validated_samples == 1
    assert_released(batch)


def test_invalid_orphan_is_rejected_and_retains_parent_failure_context():
    parent, child = parent_bindings()
    failure = RuntimeError("parent did not complete")
    with pytest.raises(ValueError, match="declared growth bound") as caught:
        with SampleBatch((parent.operation, child.operation), limit=4) as batch:
            batch.sample(child.operation, {"items": 3}, {"visits": 4})
            raise failure
    assert caught.value.__context__ is failure
    assert batch.validated_samples == 0
    assert_released(batch)


def test_later_invalid_root_is_checked_after_an_earlier_flush():
    binding = leaf_binding()
    with pytest.raises(ValueError, match="declared growth bound"):
        with SampleBatch((binding.operation,), limit=2) as batch:
            batch.sample(binding.operation, {"items": 1}, {"visits": 1})
            batch.sample(binding.operation, {"items": 2}, {"visits": 2})
            assert batch.validated_samples == 2
            batch.sample(binding.operation, {"items": 3}, {"visits": 4})
    assert batch.validated_samples == 2
    assert_released(batch)


@pytest.mark.parametrize("selected", ["parent", "child"])
def test_semantically_identical_source_binding_replacement_is_rejected(monkeypatch, selected):
    parent, child = parent_bindings()
    binding = parent if selected == "parent" else child
    replacement = replace(binding)
    assert replacement == binding and replacement is not binding
    with pytest.raises(ValueError, match="source binding"):
        with SampleBatch((parent.operation,), limit=4) as batch:
            batch.sample(
                parent.operation,
                {"items": 3},
                {"visits": 3},
                {"leaf": [literal_sample(child)]},
            )
            monkeypatch.setattr(binding.operation, "__riverhog_growth__", replacement)
    assert_released(batch)


def test_same_identity_delegate_substitution_is_rejected():
    parent, child = parent_bindings()
    other = leaf_binding()
    assert other.operation is not child.operation
    assert other.project() == child.project()
    delegates = parent.delegates
    before = canonical_json_bytes(parent.project())
    try:
        with pytest.raises(ValueError, match="source binding"):
            with SampleBatch((parent.operation,), limit=4) as batch:
                batch.sample(
                    parent.operation,
                    {"items": 3},
                    {"visits": 3},
                    {"leaf": [literal_sample(child)]},
                )
                object.__setattr__(parent, "delegates", (("leaf", other.operation),))
                assert canonical_json_bytes(parent.project()) == before
    finally:
        object.__setattr__(parent, "delegates", delegates)
    assert_released(batch)


@pytest.mark.parametrize("defect", ["declaration-object", "metadata"])
def test_forced_frozen_source_mutation_is_rejected(defect):
    parent, child = parent_bindings()
    target = parent if defect == "declaration-object" else child.declaration
    field = "declaration" if defect == "declaration-object" else "invariants"
    original = getattr(target, field)
    value = replace(original) if defect == "declaration-object" else ("Changed completion scope.",)
    try:
        with pytest.raises(ValueError, match="source binding"):
            with SampleBatch((parent.operation,), limit=4) as batch:
                batch.sample(
                    parent.operation,
                    {"items": 3},
                    {"visits": 3},
                    {"leaf": [literal_sample(child)]},
                )
                object.__setattr__(target, field, value)
    finally:
        object.__setattr__(target, field, original)
    assert_released(batch)


@pytest.mark.parametrize("defect", ["observations", "nested-observations", "delegate-list"])
def test_completed_sample_mutation_is_rejected_before_receiving(defect):
    parent, child = parent_bindings()
    with pytest.raises(ValueError, match="completed growth sample changed"):
        with SampleBatch((parent.operation,), limit=4) as batch:
            leaf = literal_sample(child)
            sample = batch.sample(parent.operation, {"items": 3}, {"visits": 3}, {"leaf": [leaf]})
            if defect == "observations":
                sample["dimensions"]["items"] = sample["metrics"]["visits"] = 4
            elif defect == "nested-observations":
                leaf["dimensions"]["items"] = leaf["metrics"]["visits"] = 4
            else:
                sample["delegates"]["leaf"].append(copy.deepcopy(leaf))
    assert batch.validated_samples == 0
    assert_released(batch)


def test_capture_owns_primitive_observations():
    binding = leaf_binding()
    dimensions, metrics = {"items": 3}, {"visits": 3}
    with SampleBatch((binding.operation,), limit=4) as batch:
        sample = batch.sample(binding.operation, dimensions, metrics)
        dimensions["items"] = metrics["visits"] = 4
    assert sample["dimensions"] == {"items": 3}
    assert sample["metrics"] == {"visits": 3}
    assert batch.validated_samples == 1


@pytest.mark.parametrize("defect", ["missing-metric", "source", "unadmitted-owner"])
def test_caught_admission_failure_cannot_complete_the_scope(monkeypatch, defect):
    binding = leaf_binding()
    other = graph_binding("unadmitted", graph_bound(Expression("dimension", name="items")))
    with pytest.raises(ValueError, match="previously failed scope"):
        with SampleBatch((binding.operation,), limit=4) as batch:
            with monkeypatch.context() as patch:
                if defect == "source":
                    patch.setattr(binding.operation, "__riverhog_growth__", replace(binding))
                with pytest.raises((KeyError, ValueError)):
                    batch.sample(
                        other.operation if defect == "unadmitted-owner" else binding.operation,
                        {"items": 3},
                        {} if defect == "missing-metric" else {"visits": 3},
                    )
            assert batch.state == "failed"
    assert_released(batch)


@pytest.mark.parametrize("field", ["dimensions", "metrics"])
def test_malformed_observation_mappings_are_rejected(field):
    binding = leaf_binding()
    dimensions = [("items", 3)] if field == "dimensions" else {"items": 3}
    metrics = [("visits", 3)] if field == "metrics" else {"visits": 3}
    with pytest.raises(ValueError):
        with SampleBatch((binding.operation,), limit=4) as batch:
            batch.sample(binding.operation, dimensions, metrics)
    assert_released(batch)


def test_caught_validation_failure_cannot_complete_the_scope():
    binding = leaf_binding()
    with pytest.raises(ValueError, match="previously failed scope"):
        with SampleBatch((binding.operation,), limit=1) as batch:
            with pytest.raises(ValueError, match="declared growth bound"):
                batch.sample(binding.operation, {"items": 3}, {"visits": 4})
    assert_released(batch)


@pytest.mark.parametrize("limit", [True, 0, -1, 1.0])
def test_batch_requires_positive_integer_admission(limit):
    with pytest.raises(ValueError, match="positive root admission"):
        SampleBatch((leaf_binding().operation,), limit=limit)


def test_batch_requires_fixed_distinct_operations():
    operation = leaf_binding().operation
    for operations in ((), [operation], (operation, operation)):
        with pytest.raises(ValueError, match="distinct source operations"):
            SampleBatch(operations, limit=1)


def test_failed_entry_releases_its_source_closure():
    first, duplicate = leaf_binding(), leaf_binding()
    batch = SampleBatch((first.operation, duplicate.operation), limit=1)
    with pytest.raises(ValueError, match="multiple source owners"):
        with batch:
            pytest.fail("duplicate source owners entered the batch")
    assert_released(batch)


def test_nested_entry_and_reentry_are_rejected():
    binding = leaf_binding()
    batch = SampleBatch((binding.operation,), limit=4)
    with batch:
        with pytest.raises(ValueError, match="original scope"):
            with batch:
                pytest.fail("nested batch entry succeeded")
        batch.sample(binding.operation, {"items": 3}, {"visits": 3})
    assert batch.validated_samples == 1
    with pytest.raises(ValueError, match="original scope"):
        with batch:
            pytest.fail("closed batch reentered")
    assert_released(batch)


@pytest.mark.parametrize("state", ["new", "closed"])
@pytest.mark.parametrize("action", ["sample", "flush"])
def test_batch_rejects_work_outside_its_active_scope(state, action):
    binding = leaf_binding()
    batch = SampleBatch((binding.operation,), limit=4)
    if state == "closed":
        with batch:
            pass
    with pytest.raises(ValueError, match="active"):
        if action == "sample":
            batch.sample(binding.operation, {"items": 3}, {"visits": 3})
        else:
            batch.flush()
    assert batch.state == state


def test_serialized_samples_remain_independently_validated_after_close():
    parent, child = parent_bindings()
    with SampleBatch((parent.operation, child.operation), limit=4) as batch:
        leaf = batch.sample(child.operation, {"items": 3}, {"visits": 3})
        sample = batch.sample(parent.operation, {"items": 3}, {"visits": 3}, {"leaf": [leaf]})
    retained = json.loads(canonical_json_bytes([sample]))
    projection = project_bindings((parent,))
    validate_execution_samples(projection, parent.declaration.id, retained)
    retained[0]["delegates"]["leaf"][0]["metrics"]["visits"] = 4
    with pytest.raises(ValueError, match="declared growth bound"):
        validate_execution_samples(projection, parent.declaration.id, retained)
    assert_released(batch)


def test_actual_caller_and_unit_turns_share_a_bounded_observer_batch(tmp_path, monkeypatch):
    from tests.unit import test_upload_journal_service_caller_growth as callers
    from tests.unit import test_upload_journal_service_coordination_growth as coordination
    from tests.unit import test_upload_journal_service_unit_growth as units

    calls = observed_validations(monkeypatch)
    cases = units.real_case(tmp_path / "caller", unrelated=0)
    roots = []
    with closing(cases):
        case, current = next(cases)
        with SampleBatch((callers.ROOT, units.ROOT), limit=4) as batch:
            recorder = callers.Recorder(case, current, batch=batch)
            for _ in range(3):
                result, sample = recorder.measure()
                assert type(result) is bool
                assert len(sample["delegates"]["unit"]) == 1
                roots.append(sample)
                assert case.engine.pool.checkedout() == 0
        assert batch.validated_samples == 6
        assert batch.validation_calls == 4
        expected = {
            growth.growth_binding(callers.ROOT).declaration.id: 3,
            growth.growth_binding(units.ROOT).declaration.id: 3,
        }
        assert Counter(identity for identity, samples in calls for _ in samples) == expected
        received = {id(sample) for _, samples in calls for sample in samples}
        assert received == {
            id(sample) for root in roots for sample in (root, root["delegates"]["unit"][0])
        }
        for root in roots:
            for operation, sample in (
                (callers.ROOT, root),
                (units.ROOT, root["delegates"]["unit"][0]),
            ):
                independent = coordination.sample(
                    operation, sample["dimensions"], sample["metrics"], sample["delegates"]
                )
                assert canonical_json_bytes(sample) == canonical_json_bytes(independent)
        assert_released(batch)
        assert case.engine.pool.checkedout() == 0
    assert cases.gi_frame is None
    assert case.engine.pool.checkedout() == 0

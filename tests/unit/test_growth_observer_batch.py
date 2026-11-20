from __future__ import annotations

import copy
import hashlib
import json
import os
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

from scripts.validation_numeric import numeric_policy_from_environment
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


def projection_bindings():
    child = leaf_binding()
    unused = graph_binding("batch_unused", graph_bound(Expression("dimension", name="items")))
    parent = graph_binding(
        "batch_projection",
        graph_bound(graph_call("leaf")),
        delegates=(("leaf", child.operation), ("unused", unused.operation)),
    )
    return parent, child, unused


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
    assert not batch._sources
    assert not batch._roots
    assert not batch._digests
    assert not batch._declaration_bytes
    assert batch._projection_header_bytes is None


def observed_validations(monkeypatch):
    calls = []
    original = growth_batch.validate_execution_roots

    def validate(projection, groups, *, numeric_policy):
        original(projection, groups, numeric_policy=numeric_policy)
        calls.extend((identity, tuple(samples)) for identity, samples in groups)

    monkeypatch.setattr(growth_batch, "validate_execution_roots", validate)
    return calls


def test_bounded_batch_checks_every_root_without_compiling_each_emission(monkeypatch):
    binding = leaf_binding()
    calls = observed_validations(monkeypatch)
    compilations = []
    original = growth._projection_contexts

    def compile_projection(projection, **kwargs):
        compilations.append(projection)
        return original(projection, **kwargs)

    monkeypatch.setattr(growth, "_projection_contexts", compile_projection)
    with SampleBatch(
        (binding.operation,), limit=3, numeric_policy=numeric_policy_from_environment(os.environ)
    ) as batch:
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


@pytest.mark.parametrize("parent_first", [False, True])
def test_each_flush_uses_one_private_complete_projection_from_admitted_bytes(
    monkeypatch, parent_first
):
    parent, child, unused = projection_bindings()
    expected = canonical_json_bytes(
        project_bindings((parent,), numeric_policy=numeric_policy_from_environment(os.environ))
    )
    original = growth_batch.validate_execution_roots
    compile_original = growth._projection_contexts
    compilations, received = [], []

    def compile_projection(projection, **kwargs):
        compilations.append(projection)
        return compile_original(projection, **kwargs)

    def containers(value):
        if isinstance(value, dict):
            yield value
            for item in value.values():
                yield from containers(item)
        elif isinstance(value, list):
            yield value
            for item in value:
                yield from containers(item)

    def validate(projection, groups, *, numeric_policy):
        assert type(projection) is bytes and projection == expected
        before = len(compilations)
        original(projection, groups, numeric_policy=numeric_policy)
        assert len(compilations) == before + 1
        current = tuple(containers(compilations[-1]))
        assert {id(item) for item in current}.isdisjoint(
            id(item) for previous in received for item in previous
        )
        received.append(current)
        # Mutating retained private decoded metadata cannot affect another flush.
        for item in current:
            if isinstance(item, dict):
                item["reader_mutation"] = True
            else:
                item.append("changed after this reader call")

    monkeypatch.setattr(growth, "_projection_contexts", compile_projection)
    monkeypatch.setattr(growth_batch, "validate_execution_roots", validate)
    with SampleBatch(
        (parent.operation,),
        limit=2,
        include_descendants=True,
        numeric_policy=numeric_policy_from_environment(os.environ),
    ) as batch:
        assert isinstance(batch._projection_header_bytes, bytes)
        assert set(batch._declaration_bytes) == {
            binding.declaration.id for binding in (parent, child, unused)
        }
        assert all(isinstance(row, bytes) for row in batch._declaration_bytes.values())
        for items in (2, 3):
            leaf = literal_sample(child, items=items, visits=items)
            root = literal_sample(
                parent, items=items, visits=items, children={"leaf": [leaf], "unused": []}
            )
            pending = [(child, leaf), (parent, root)]
            if parent_first:
                pending.reverse()
            for binding, sample in pending:
                batch.receive(binding.operation, sample)
            assert not batch._pending
    assert len(compilations) == 3
    assert len(received) == batch.validation_attempts == batch.validation_calls == 2
    assert batch.validated_samples == batch.validation_groups == 4
    assert_released(batch)


def test_equal_projected_rows_still_require_separate_source_admissions(monkeypatch):
    first, second = leaf_binding(), leaf_binding()
    assert first.operation is not second.operation
    assert first.project() == second.project()
    expected = canonical_json_bytes(
        project_bindings((first,), numeric_policy=numeric_policy_from_environment(os.environ))
    )
    original_project = growth_batch.project_bindings
    original_validate = growth_batch.validate_execution_roots
    admissions, projections = [], []

    def project(bindings, *, numeric_policy):
        admissions.append(bindings)
        return original_project(bindings, numeric_policy=numeric_policy)

    def validate(projection, groups, *, numeric_policy):
        assert type(projection) is bytes and projection == expected
        original_validate(projection, groups, numeric_policy=numeric_policy)
        projections.append(projection)

    monkeypatch.setattr(growth_batch, "project_bindings", project)
    monkeypatch.setattr(growth_batch, "validate_execution_roots", validate)
    for binding in (first, second):
        with SampleBatch(
            (binding.operation,),
            limit=1,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
            assert batch.source(binding.operation)[0] is binding
            batch.receive(binding.operation, literal_sample(binding))
        assert batch.validated_samples == batch.validation_calls == 1
        assert_released(batch)
    assert len(admissions) == len(projections) == 2
    assert admissions[0][0] is first and admissions[1][0] is second


@pytest.mark.parametrize("after_flush", [False, True])
def test_empty_exit_checks_unused_source_metadata_and_releases_projection(after_flush):
    parent, child, unused = projection_bindings()
    previous = unused.declaration.invariants
    try:
        with pytest.raises(ValueError, match="source binding"):
            with SampleBatch(
                (parent.operation,),
                limit=1,
                include_descendants=True,
                numeric_policy=numeric_policy_from_environment(os.environ),
            ) as batch:
                if after_flush:
                    batch.receive(child.operation, literal_sample(child))
                assert not batch._pending
                object.__setattr__(unused.declaration, "invariants", ("Changed unused scope.",))
    finally:
        object.__setattr__(unused.declaration, "invariants", previous)
    assert batch.validated_samples == batch.validation_calls == int(after_flush)
    assert_released(batch)


def test_completed_child_and_parent_are_independently_received(monkeypatch):
    parent, child = parent_bindings()
    calls = observed_validations(monkeypatch)
    with SampleBatch(
        (parent.operation, child.operation),
        limit=4,
        numeric_policy=numeric_policy_from_environment(os.environ),
    ) as batch:
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
        with SampleBatch(
            (parent.operation, child.operation),
            limit=4,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
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
        with SampleBatch(
            (parent.operation, child.operation),
            limit=4,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
            batch.sample(child.operation, {"items": 3}, {"visits": 4})
            raise failure
    assert caught.value.__context__ is failure
    assert batch.validated_samples == 0
    assert_released(batch)


def test_later_invalid_root_is_checked_after_an_earlier_flush():
    binding = leaf_binding()
    with pytest.raises(ValueError, match="declared growth bound"):
        with SampleBatch(
            (binding.operation,),
            limit=2,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
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
        with SampleBatch(
            (parent.operation,), limit=4, numeric_policy=numeric_policy_from_environment(os.environ)
        ) as batch:
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
            with SampleBatch(
                (parent.operation,),
                limit=4,
                numeric_policy=numeric_policy_from_environment(os.environ),
            ) as batch:
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
            with SampleBatch(
                (parent.operation,),
                limit=4,
                numeric_policy=numeric_policy_from_environment(os.environ),
            ) as batch:
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
        with SampleBatch(
            (parent.operation,), limit=4, numeric_policy=numeric_policy_from_environment(os.environ)
        ) as batch:
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
    with SampleBatch(
        (binding.operation,), limit=4, numeric_policy=numeric_policy_from_environment(os.environ)
    ) as batch:
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
        with SampleBatch(
            (binding.operation,),
            limit=4,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
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
        with SampleBatch(
            (binding.operation,),
            limit=4,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
            batch.sample(binding.operation, dimensions, metrics)
    assert_released(batch)


def test_caught_validation_failure_cannot_complete_the_scope():
    binding = leaf_binding()
    with pytest.raises(ValueError, match="previously failed scope"):
        with SampleBatch(
            (binding.operation,),
            limit=1,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
            with pytest.raises(ValueError, match="declared growth bound"):
                batch.sample(binding.operation, {"items": 3}, {"visits": 4})
    assert_released(batch)


@pytest.mark.parametrize("limit", [True, 0, -1, 1.0])
def test_batch_requires_positive_integer_admission(limit):
    with pytest.raises(ValueError, match="positive root admission"):
        SampleBatch(
            (leaf_binding().operation,),
            limit=limit,
            numeric_policy=numeric_policy_from_environment(os.environ),
        )


def test_batch_requires_fixed_distinct_operations():
    operation = leaf_binding().operation
    for operations in ((), [operation], (operation, operation)):
        with pytest.raises(ValueError, match="distinct source operations"):
            SampleBatch(
                operations, limit=1, numeric_policy=numeric_policy_from_environment(os.environ)
            )


def test_failed_entry_releases_its_source_closure():
    first, duplicate = leaf_binding(), leaf_binding()
    batch = SampleBatch(
        (first.operation, duplicate.operation),
        limit=1,
        numeric_policy=numeric_policy_from_environment(os.environ),
    )
    with pytest.raises(ValueError, match="multiple source owners"):
        with batch:
            pytest.fail("duplicate source owners entered the batch")
    assert_released(batch)


def test_nested_entry_and_reentry_are_rejected():
    binding = leaf_binding()
    batch = SampleBatch(
        (binding.operation,), limit=4, numeric_policy=numeric_policy_from_environment(os.environ)
    )
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
    batch = SampleBatch(
        (binding.operation,), limit=4, numeric_policy=numeric_policy_from_environment(os.environ)
    )
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
    with SampleBatch(
        (parent.operation, child.operation),
        limit=4,
        numeric_policy=numeric_policy_from_environment(os.environ),
    ) as batch:
        leaf = batch.sample(child.operation, {"items": 3}, {"visits": 3})
        sample = batch.sample(parent.operation, {"items": 3}, {"visits": 3}, {"leaf": [leaf]})
    retained = json.loads(canonical_json_bytes([sample]))
    projection = project_bindings(
        (parent,), numeric_policy=numeric_policy_from_environment(os.environ)
    )
    validate_execution_samples(
        projection,
        parent.declaration.id,
        retained,
        numeric_policy=numeric_policy_from_environment(os.environ),
    )
    retained[0]["delegates"]["leaf"][0]["metrics"]["visits"] = 4
    with pytest.raises(ValueError, match="declared growth bound"):
        validate_execution_samples(
            projection,
            parent.declaration.id,
            retained,
            numeric_policy=numeric_policy_from_environment(os.environ),
        )
    assert_released(batch)


def _actual_caller_and_unit_turns_share_batch(tmp_path, monkeypatch, *, include_descendants):
    from tests.unit import test_upload_journal_service_caller_growth as callers
    from tests.unit import test_upload_journal_service_coordination_growth as coordination
    from tests.unit import test_upload_journal_service_unit_growth as units

    calls = observed_validations(monkeypatch)
    cases = units.real_case(tmp_path / "caller", unrelated=0)
    roots = []
    with closing(cases):
        case, current = next(cases)
        with SampleBatch(
            (callers.ROOT, units.ROOT),
            limit=64 if include_descendants else 4,
            include_descendants=include_descendants,
            numeric_policy=numeric_policy_from_environment(os.environ),
        ) as batch:
            recorder = callers.Recorder(case, current, batch=batch)
            for _ in range(3):
                result, sample = recorder.measure()
                assert type(result) is bool
                assert len(sample["delegates"]["unit"]) == 1
                roots.append(sample)
                assert case.engine.pool.checkedout() == 0
        assert batch.validated_samples == (122 if include_descendants else 6)
        assert batch.validation_attempts == batch.validation_calls == 2
        assert batch.validation_groups == (46 if include_descendants else 4)
        expected = {
            growth.growth_binding(callers.ROOT).declaration.id: 3,
            growth.growth_binding(units.ROOT).declaration.id: 3,
        }
        counts = Counter(identity for identity, samples in calls for _ in samples)
        assert all(counts[identity] == count for identity, count in expected.items())
        validation_roots = [sample for _, samples in calls for sample in samples]
        received = {id(sample) for sample in validation_roots}
        caller_unit_roots = {
            id(sample) for root in roots for sample in (root, root["delegates"]["unit"][0])
        }
        assert caller_unit_roots <= received
        if include_descendants:

            def nodes(sample):
                return 1 + sum(
                    nodes(child) for children in sample["delegates"].values() for child in children
                )

            assert len(validation_roots) == len(received) == 122
            assert sum(nodes(sample) for sample in validation_roots) == 1078
        else:
            assert counts == expected
            assert received == caller_unit_roots
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


def test_actual_caller_and_unit_turns_share_a_bounded_observer_batch(tmp_path, monkeypatch):
    _actual_caller_and_unit_turns_share_batch(tmp_path, monkeypatch, include_descendants=False)


def test_actual_caller_and_unit_turns_receive_every_descendant(tmp_path, monkeypatch):
    _actual_caller_and_unit_turns_share_batch(tmp_path, monkeypatch, include_descendants=True)

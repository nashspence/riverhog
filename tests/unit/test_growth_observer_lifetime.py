from __future__ import annotations

import copy
import hashlib
import importlib
import json
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import riverhog_growth_contracts as growth
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import OCTETS, Expression, validate_execution_samples

from tests.support import growth_batch
from tests.support.growth_batch import SampleBatch
from tests.unit.test_growth_observer_batch import (
    assert_released,
    graph_binding,
    graph_bound,
    graph_call,
    leaf_binding,
    literal_sample,
    observed_validations,
    parent_bindings,
)


def source_graph():
    child = leaf_binding()
    unused = graph_binding("unused", graph_bound(Expression("dimension", name="items")))
    middle = graph_binding(
        "middle",
        graph_bound(graph_call("leaf")),
        delegates=(("leaf", child.operation), ("unused", unused.operation)),
    )
    root = graph_binding(
        "outer",
        graph_bound(graph_call("middle")),
        delegates=(("middle", middle.operation),),
    )
    return root, middle, child, unused


def test_source_lookup_reuses_exact_metadata_without_compilation_or_codecs(monkeypatch):
    parent, child = parent_bindings()
    expected = {
        binding.operation: (binding, literal_sample(binding)["declaration_sha256"])
        for binding in (parent, child)
    }

    def forbidden(*_args, **_kwargs):
        raise AssertionError("metadata lookup repeated compilation, serialization, or hashing")

    with SampleBatch((parent.operation,), limit=4) as batch:
        with monkeypatch.context() as patch:
            patch.setattr(growth_batch, "project_bindings", forbidden)
            patch.setattr(growth, "_projection_contexts", forbidden)
            patch.setattr(growth_batch, "canonical_json_bytes", forbidden)
            patch.setattr(growth_batch.hashlib, "sha256", forbidden)
            for _ in range(3):
                for operation, (binding, digest) in expected.items():
                    actual, actual_digest = batch.source(operation)
                    assert actual is binding and actual_digest == digest
        assert batch.validated_samples == batch.validation_calls == 0
        assert not batch._pending
    assert_released(batch)


@pytest.mark.parametrize("include_descendants", [False, True])
def test_source_access_and_receiving_authority_are_distinct(include_descendants):
    parent, child = parent_bindings()
    batch = SampleBatch((parent.operation,), limit=4, include_descendants=include_descendants)
    if include_descendants:
        with batch:
            assert batch.source(child.operation)[0] is child
            batch.receive(child.operation, literal_sample(child))
        assert batch.validated_samples == 1
    else:
        with pytest.raises(ValueError):
            with batch:
                assert batch.source(child.operation)[0] is child
                batch.receive(child.operation, literal_sample(child))
        assert batch.validated_samples == 0
    assert_released(batch)


@pytest.mark.parametrize("value", [None, 0, 1, "true"])
def test_descendant_admission_requires_an_explicit_boolean(value):
    with pytest.raises(ValueError):
        SampleBatch((leaf_binding().operation,), limit=4, include_descendants=value)


@pytest.mark.parametrize("action", ["source", "receive"])
def test_same_identity_foreign_operation_is_not_in_the_admitted_closure(action):
    original, foreign = leaf_binding(), leaf_binding()
    assert original.project() == foreign.project()
    with pytest.raises(ValueError):
        with SampleBatch((original.operation,), limit=4, include_descendants=True) as batch:
            if action == "source":
                batch.source(foreign.operation)
            else:
                batch.receive(foreign.operation, literal_sample(foreign))
    assert_released(batch)


def test_receive_preserves_the_complete_supplied_sample(monkeypatch):
    parent, child = parent_bindings()
    calls = observed_validations(monkeypatch)
    sample = literal_sample(parent, children={"leaf": [literal_sample(child)]})
    before = canonical_json_bytes(sample)
    with SampleBatch((parent.operation,), limit=4) as batch:
        batch.receive(parent.operation, sample)
        assert canonical_json_bytes(sample) == before
    assert calls == [(parent.declaration.id, (sample,))]
    assert calls[0][1][0] is sample
    assert canonical_json_bytes(sample) == before
    assert batch.validated_samples == 1
    assert_released(batch)


@pytest.mark.parametrize(
    "defect",
    [
        "unknown-field",
        "missing-field",
        "declaration",
        "digest",
        "dimensions-pairs",
        "metrics-pairs",
        "missing-dimension",
        "extra-dimension",
        "missing-metric",
        "extra-metric",
        "bool-metric",
        "float-metric",
        "negative-metric",
        "bool-dimension",
        "delegates-list",
        "extra-delegate",
        "bound",
    ],
)
def test_receive_rejects_inexact_or_invalid_samples_without_repair(defect):
    binding = leaf_binding()
    sample = literal_sample(binding)
    if defect == "unknown-field":
        sample["unknown"] = 0
    elif defect == "missing-field":
        sample.pop("delegates")
    elif defect == "declaration":
        sample["declaration"] = "fixture.foreign.growth/v1"
    elif defect == "digest":
        sample["declaration_sha256"] = "0" * 64
    elif defect == "dimensions-pairs":
        sample["dimensions"] = [("items", 3)]
    elif defect == "metrics-pairs":
        sample["metrics"] = [("visits", 3)]
    elif defect == "missing-dimension":
        sample["dimensions"] = {}
    elif defect == "extra-dimension":
        sample["dimensions"]["extra"] = 0
    elif defect == "missing-metric":
        sample["metrics"] = {}
    elif defect == "extra-metric":
        sample["metrics"]["extra"] = 0
    elif defect in {"bool-metric", "float-metric", "negative-metric"}:
        sample["metrics"]["visits"] = {
            "bool-metric": True,
            "float-metric": 3.0,
            "negative-metric": -1,
        }[defect]
    elif defect == "bool-dimension":
        sample["dimensions"]["items"] = True
    elif defect == "delegates-list":
        sample["delegates"] = []
    elif defect == "extra-delegate":
        sample["delegates"]["extra"] = []
    else:
        sample["metrics"]["visits"] = 4
    before = copy.deepcopy(sample)
    with pytest.raises(ValueError):
        with SampleBatch((binding.operation,), limit=4) as batch:
            batch.receive(binding.operation, sample)
    assert sample == before
    assert batch.validated_samples == 0
    assert_released(batch)


def test_each_group_reads_its_complete_reachable_projection(monkeypatch):
    root, middle, child, unused = source_graph()
    original = growth_batch.validate_execution_samples
    received = []

    def validate(projection, identity, samples):
        expected = (
            {child.declaration.id}
            if identity == child.declaration.id
            else {middle.declaration.id, child.declaration.id, unused.declaration.id}
        )
        assert set(projection["declarations"]) == expected
        assert all(projection is not prior for prior in received)
        original(projection, identity, samples)
        received.append(projection)

    monkeypatch.setattr(growth_batch, "validate_execution_samples", validate)
    with SampleBatch((root.operation,), limit=4, include_descendants=True) as batch:
        child_sample = literal_sample(child)
        batch.receive(child.operation, child_sample)
        batch.receive(
            middle.operation,
            literal_sample(middle, children={"leaf": [child_sample], "unused": []}),
        )
    assert batch.validation_calls == 2 and batch.validated_samples == 2
    assert len(received) == 2
    assert_released(batch)


def test_later_group_does_not_reuse_a_prior_readers_mutable_projection(monkeypatch):
    root, _, child, _ = source_graph()
    original = growth_batch.validate_execution_samples
    projections = []

    def validate(projection, identity, samples):
        assert all(projection is not prior for prior in projections)
        original(projection, identity, samples)
        projections.append(projection)
        projection["format"] = "changed after validation"

    monkeypatch.setattr(growth_batch, "validate_execution_samples", validate)
    with SampleBatch((root.operation,), limit=1, include_descendants=True) as batch:
        batch.receive(child.operation, literal_sample(child, items=2, visits=2))
        batch.receive(child.operation, literal_sample(child, items=3, visits=3))
    assert len(projections) == batch.validated_samples == 2
    assert_released(batch)


def test_initial_admission_checks_unused_metadata_before_receiving_any_leaf():
    root, _, _, unused = source_graph()
    bound = unused.declaration.bounds[0]
    previous = bound.unit
    batch = SampleBatch((root.operation,), limit=4, include_descendants=True)
    try:
        object.__setattr__(bound, "unit", OCTETS)
        with pytest.raises(ValueError):
            with batch:
                pytest.fail("invalid unused declaration entered the admitted source closure")
    finally:
        object.__setattr__(bound, "unit", previous)
    assert_released(batch)


@pytest.mark.parametrize("during_reader", [False, True])
def test_leaf_receiving_keeps_source_checks_for_unselected_metadata(monkeypatch, during_reader):
    root, _, child, unused = source_graph()
    previous = unused.declaration.invariants
    original = growth_batch.validate_execution_samples

    def mutate():
        object.__setattr__(unused.declaration, "invariants", ("Changed unused completion scope.",))

    def validate(projection, identity, samples):
        assert set(projection["declarations"]) == {child.declaration.id}
        original(projection, identity, samples)
        mutate()

    if during_reader:
        monkeypatch.setattr(growth_batch, "validate_execution_samples", validate)
    try:
        with pytest.raises(ValueError, match="source binding"):
            with SampleBatch((root.operation,), limit=4, include_descendants=True) as batch:
                batch.receive(child.operation, literal_sample(child))
                if not during_reader:
                    mutate()
    finally:
        object.__setattr__(unused.declaration, "invariants", previous)
    assert batch.validated_samples == 0
    assert_released(batch)


@pytest.mark.parametrize("action", ["source", "receive"])
def test_caught_source_or_receiving_failures_cannot_exit_green(monkeypatch, action):
    parent, child = parent_bindings()
    with pytest.raises(ValueError, match="previously failed scope"):
        with SampleBatch((parent.operation,), limit=4, include_descendants=True) as batch:
            with monkeypatch.context() as patch:
                with pytest.raises(ValueError):
                    if action == "source":
                        patch.setattr(child.operation, "__riverhog_growth__", replace(child))
                        batch.source(child.operation)
                    else:
                        malformed = literal_sample(child)
                        malformed["unknown"] = 0
                        batch.receive(child.operation, malformed)
                        batch.flush()
            assert batch.state == "failed"
    assert_released(batch)


@pytest.mark.parametrize("valid", [False, True])
def test_existing_child_obligation_survives_an_unfinished_parent(valid):
    parent, child = parent_bindings()
    failure = RuntimeError("parent did not complete")
    expected = RuntimeError if valid else ValueError
    with pytest.raises(expected) as caught:
        with SampleBatch((parent.operation,), limit=4, include_descendants=True) as batch:
            batch.receive(child.operation, literal_sample(child, visits=3 if valid else 4))
            raise failure
    if valid:
        assert caught.value is failure
        assert batch.validated_samples == 1
    else:
        assert caught.value.__context__ is failure
        assert batch.validated_samples == 0
    assert_released(batch)


def test_later_received_observation_is_not_an_acceptance_cache_hit():
    parent, child = parent_bindings()
    with pytest.raises(ValueError, match="declared growth bound"):
        with SampleBatch((parent.operation,), limit=1, include_descendants=True) as batch:
            batch.receive(child.operation, literal_sample(child))
            assert batch.validated_samples == 1
            batch.receive(child.operation, literal_sample(child, visits=4))
    assert batch.validated_samples == 1
    assert_released(batch)


@pytest.mark.parametrize("action", ["source", "receive"])
@pytest.mark.parametrize("closed", [False, True])
def test_metadata_and_receiving_reject_work_outside_the_owned_scope(action, closed):
    binding = leaf_binding()
    batch = SampleBatch((binding.operation,), limit=4)
    if closed:
        with batch:
            pass
    before = batch.state
    with pytest.raises(ValueError):
        if action == "source":
            batch.source(binding.operation)
        else:
            batch.receive(binding.operation, literal_sample(binding))
    assert batch.state == before


def test_received_samples_keep_independent_serialized_validation_after_close():
    parent, child = parent_bindings()
    sample = literal_sample(parent, children={"leaf": [literal_sample(child)]})
    with SampleBatch((parent.operation,), limit=4) as batch:
        batch.receive(parent.operation, sample)
    retained = json.loads(canonical_json_bytes([sample]))
    projection = growth.project_bindings((parent,))
    validate_execution_samples(projection, parent.declaration.id, retained)
    retained[0]["delegates"]["leaf"][0]["metrics"]["visits"] = 4
    with pytest.raises(ValueError, match="declared growth bound"):
        validate_execution_samples(projection, parent.declaration.id, retained)
    assert_released(batch)


def test_native_metadata_and_completion_do_not_record_observer_work(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "packages/riverhog-provenance/tests"
    monkeypatch.syspath_prepend(str(path))
    native = importlib.import_module("test_entry_replay_growth")
    operation = native.CheckpointSHA256.update_bytes
    binding = growth.growth_binding(operation)
    child = growth.growth_binding(dict(binding.delegates)["compression"])
    expected = {
        owner.operation: hashlib.sha256(canonical_json_bytes(owner.project())).hexdigest()
        for owner in (binding, child)
    }

    class Recording:
        def __init__(self):
            self._recording = True
            self.transitions = []
            self.metrics = {
                "native_hashed_octets": 0,
                "decoded_json_octets": 0,
                "encoded_json_octets": 0,
            }

        @property
        def recording(self):
            return self._recording

        @recording.setter
        def recording(self, value):
            self.transitions.append(value)
            self._recording = value

    def forbidden(*_args, **_kwargs):
        raise AssertionError("observer construction repeated source projection")

    db = Recording()
    with SampleBatch((operation,), limit=4) as batch:
        with monkeypatch.context() as patch:
            patch.setattr(native, "project_bindings", forbidden)
            for _ in range(3):
                dimensions = {item.name: 0 for item in binding.declaration.dimensions}
                with native.observe_replay(
                    db, object(), dimensions, root_operation=operation, batch=batch
                ) as (frame, complete):
                    before = dict(db.metrics)
                    primitive = {bound.metric: 0 for bound in binding.declaration.bounds}
                    sample = complete(frame, primitive)
                    child_frame = {
                        "binding": child,
                        "dimensions": {item.name: 0 for item in child.declaration.dimensions},
                        "delegates": {name: [] for name, _ in child.delegates},
                    }
                    child_sample = complete(
                        child_frame, {bound.metric: 0 for bound in child.declaration.bounds}
                    )
                    assert sample["declaration_sha256"] == expected[operation]
                    assert child_sample["declaration_sha256"] == expected[child.operation]
                    assert db.metrics == before
                    assert db.recording and not db.transitions
                    assert not batch._pending
                complete(frame, primitive)
                assert not batch._pending
        assert batch.validation_calls == batch.validated_samples == 0
    assert_released(batch)


def test_core_receiving_and_automatic_flush_run_with_observers_paused(monkeypatch):
    from tests.unit import test_upload_journal_validation_advance_growth as core

    binding = leaf_binding()
    recorder = object.__new__(core.Recorder)
    recorder.active = recorder.lease_active = True
    recorder.native_recorder = SimpleNamespace(recording=True)
    recorder.failures = []

    class Database:
        closed = False
        recording = True

        @contextmanager
        def pause(self):
            previous, self.recording = self.recording, False
            try:
                yield
            finally:
                self.recording = previous

    db = Database()
    recorder.dbs = [db]
    original = growth_batch.validate_execution_samples
    calls = []

    def validate(projection, identity, samples):
        assert not recorder.active and not recorder.lease_active
        assert not recorder.native_recorder.recording and not db.recording
        original(projection, identity, samples)
        calls.append(identity)

    monkeypatch.setattr(growth_batch, "validate_execution_samples", validate)
    with SampleBatch((binding.operation,), limit=1, include_descendants=True) as batch:
        recorder.batch = batch
        recorder.validate(binding.operation, literal_sample(binding))
        assert recorder.active and recorder.lease_active
        assert recorder.native_recorder.recording and db.recording
    assert calls == [binding.declaration.id]
    assert batch.validated_samples == 1
    assert_released(batch)


def test_core_delayed_failure_retains_batch_scope_and_cannot_exit_green():
    from tests.unit import test_upload_journal_validation_advance_growth as core

    binding = leaf_binding()
    recorder = object.__new__(core.Recorder)
    recorder.active = recorder.lease_active = True
    recorder.native_recorder = None
    recorder.dbs, recorder.failures = [], []
    with pytest.raises(ValueError, match="previously failed scope"):
        with SampleBatch((binding.operation,), limit=2, include_descendants=True) as batch:
            recorder.batch = batch
            recorder.validate(binding.operation, literal_sample(binding, visits=4))
            with pytest.raises(core.EvidenceFailure) as caught:
                recorder.validate(binding.operation, literal_sample(binding, items=2, visits=2))
            assert isinstance(caught.value.__cause__, ValueError)
            assert "declared growth bound" in str(caught.value.__cause__)
            assert recorder.failures == [
                {"scope": "growth-batch-receiving", "error_class": "ValueError"}
            ]
            assert recorder.active and recorder.lease_active
    assert batch.validated_samples == 0
    assert_released(batch)

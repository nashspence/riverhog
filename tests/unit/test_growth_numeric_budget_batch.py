from __future__ import annotations

import pytest
from riverhog_growth_contracts import NumericWorkPolicy, NumericWorkRefused

from tests.support import growth_batch
from tests.support.growth_batch import SampleBatch
from tests.unit.test_growth_observer_batch import assert_released, leaf_binding, literal_sample


def allowance():
    # An explicit allowance for these one-leaf fixtures, not a deployment default.
    return NumericWorkPolicy(max_work=10_000, max_integer_bits=64)


@pytest.mark.parametrize("value", [True, 1, {}, object()])
def test_batch_rejects_nonpolicy_admission_before_entering(value):
    with pytest.raises(ValueError, match="numeric work policy"):
        SampleBatch((leaf_binding().operation,), limit=1, numeric_policy=value)


@pytest.mark.parametrize("field", ["max_work", "max_integer_bits"])
@pytest.mark.parametrize("value", [True, 0, -1, 1.0])
def test_batch_revalidates_policy_configuration_at_admission(field, value):
    policy = allowance()
    object.__setattr__(policy, field, value)
    with pytest.raises(ValueError):
        SampleBatch((leaf_binding().operation,), limit=1, numeric_policy=policy)


def test_configured_flushes_pass_the_owned_policy_and_receive_each_invocation(monkeypatch):
    binding = leaf_binding()
    policy = allowance()
    batch = SampleBatch((binding.operation,), limit=1, numeric_policy=policy)
    owned = batch.numeric_policy
    assert owned is not policy and owned == policy
    original = growth_batch.validate_execution_roots
    receipts = []

    def validate(projection, groups, *, numeric_policy):
        assert numeric_policy is owned
        result = original(projection, groups, numeric_policy=numeric_policy)
        receipts.append(result)
        return result

    monkeypatch.setattr(growth_batch, "validate_execution_roots", validate)
    with batch:
        for _ in range(2):
            batch.receive(binding.operation, literal_sample(binding))
    assert len(receipts) == 2
    assert receipts[0] is not None and receipts[0] == receipts[1]
    assert batch.validation_attempts == batch.validation_calls == batch.validated_samples == 2
    assert_released(batch)


def test_caller_policy_mutation_cannot_enlarge_a_later_flush_allowance():
    binding = leaf_binding()
    policy = NumericWorkPolicy(max_work=10_000, max_integer_bits=8)
    with pytest.raises(NumericWorkRefused):
        with SampleBatch((binding.operation,), limit=1, numeric_policy=policy) as batch:
            batch.receive(binding.operation, literal_sample(binding))
            assert batch.validated_samples == 1
            object.__setattr__(policy, "max_work", 1_000_000)
            object.__setattr__(policy, "max_integer_bits", 64)
            assert policy.max_integer_bits == 64
            assert batch.numeric_policy.max_integer_bits == 8
            batch.receive(binding.operation, literal_sample(binding, items=256, visits=256))
    assert batch.validation_calls == batch.validated_samples == 1
    assert_released(batch)


def test_batch_requires_an_explicit_policy_before_source_admission():
    binding = leaf_binding()
    with pytest.raises(TypeError, match="numeric_policy"):
        SampleBatch((binding.operation,), limit=1)
    with pytest.raises(ValueError, match="numeric work policy"):
        SampleBatch((binding.operation,), limit=1, numeric_policy=None)


def test_source_admission_refusal_releases_before_product_work():
    batch = SampleBatch(
        (leaf_binding().operation,),
        limit=1,
        numeric_policy=NumericWorkPolicy(max_work=1, max_integer_bits=64),
    )
    with pytest.raises(NumericWorkRefused):
        with batch:
            pytest.fail("refused source admission reached product work")
    assert batch.validation_attempts == batch.validated_samples == 0
    assert_released(batch)


def test_caught_numeric_refusal_cannot_make_batch_exit_successfully():
    binding = leaf_binding()
    policy = NumericWorkPolicy(max_work=10_000, max_integer_bits=8)
    with pytest.raises(ValueError, match="previously failed scope"):
        with SampleBatch((binding.operation,), limit=1, numeric_policy=policy) as batch:
            with pytest.raises(NumericWorkRefused):
                batch.receive(binding.operation, literal_sample(binding, items=256, visits=256))
            assert batch.state == "failed"
            assert batch.validation_attempts == 1
            assert batch.validation_calls == batch.validation_groups == batch.validated_samples == 0
    assert_released(batch)


def test_numeric_refusal_keeps_completed_product_fact_and_original_sample():
    binding = leaf_binding()
    sample = literal_sample(binding, items=256, visits=256)
    completed = []
    with pytest.raises(NumericWorkRefused):
        with SampleBatch(
            (binding.operation,),
            limit=1,
            numeric_policy=NumericWorkPolicy(max_work=10_000, max_integer_bits=8),
        ) as batch:
            completed.append("original operation completed")
            batch.receive(binding.operation, sample)
    assert completed == ["original operation completed"]
    assert sample == literal_sample(binding, items=256, visits=256)
    assert batch.validated_samples == 0
    assert_released(batch)


def test_later_refusal_does_not_turn_prior_flush_into_complete_batch_evidence():
    binding = leaf_binding()
    with pytest.raises(NumericWorkRefused):
        with SampleBatch(
            (binding.operation,),
            limit=1,
            numeric_policy=NumericWorkPolicy(max_work=10_000, max_integer_bits=8),
        ) as batch:
            batch.receive(binding.operation, literal_sample(binding))
            assert batch.validated_samples == 1
            batch.receive(binding.operation, literal_sample(binding, items=1 << 8, visits=1 << 8))
    assert batch.validation_attempts == 2
    assert batch.validation_calls == batch.validation_groups == batch.validated_samples == 1
    assert_released(batch)


def test_pending_refusal_preserves_original_body_failure_as_context():
    binding = leaf_binding()
    original = RuntimeError("original body failure")
    with pytest.raises(NumericWorkRefused) as caught:
        with SampleBatch(
            (binding.operation,),
            limit=2,
            numeric_policy=NumericWorkPolicy(max_work=10_000, max_integer_bits=8),
        ) as batch:
            batch.receive(binding.operation, literal_sample(binding, items=256, visits=256))
            raise original
    assert caught.value.__context__ is original
    assert batch.validated_samples == 0
    assert_released(batch)


def test_original_baseexception_is_received_and_helper_released(monkeypatch):
    binding = leaf_binding()

    class OriginalStop(BaseException):
        pass

    original = OriginalStop("original numeric callback")

    def validate(_projection, _groups, *, numeric_policy):
        assert numeric_policy == allowance()
        raise original

    monkeypatch.setattr(growth_batch, "validate_execution_roots", validate)
    with pytest.raises(OriginalStop) as caught:
        with SampleBatch((binding.operation,), limit=1, numeric_policy=allowance()) as batch:
            batch.receive(binding.operation, literal_sample(binding))
    assert caught.value is original
    assert batch.validation_attempts == 1
    assert batch.validation_calls == batch.validation_groups == batch.validated_samples == 0
    assert_released(batch)


def test_core_recorder_receives_distinguished_refusal_as_original_cause():
    from tests.unit import test_upload_journal_validation_advance_growth as core

    binding = leaf_binding()
    recorder = object.__new__(core.Recorder)
    recorder.active = recorder.lease_active = True
    recorder.native_recorder = None
    recorder.dbs, recorder.failures = [], []
    with pytest.raises(core.EvidenceFailure) as caught:
        with SampleBatch(
            (binding.operation,),
            limit=1,
            numeric_policy=NumericWorkPolicy(max_work=10_000, max_integer_bits=8),
        ) as batch:
            recorder.batch = batch
            recorder.validate(binding.operation, literal_sample(binding, items=256, visits=256))
    assert isinstance(caught.value.__cause__, NumericWorkRefused)
    assert recorder.failures == [
        {"scope": "growth-batch-receiving", "error_class": "NumericWorkRefused"}
    ]
    assert recorder.active and recorder.lease_active
    assert batch.validated_samples == 0
    assert_released(batch)


def test_numeric_success_does_not_authorize_changed_source(monkeypatch):
    binding = leaf_binding()
    previous = binding.declaration.invariants
    original = growth_batch.validate_execution_roots

    def validate(projection, groups, *, numeric_policy):
        usage = original(projection, groups, numeric_policy=numeric_policy)
        object.__setattr__(binding.declaration, "invariants", ("Changed source after reading.",))
        return usage

    monkeypatch.setattr(growth_batch, "validate_execution_roots", validate)
    try:
        with pytest.raises(ValueError, match="source binding"):
            with SampleBatch((binding.operation,), limit=1, numeric_policy=allowance()) as batch:
                batch.receive(binding.operation, literal_sample(binding))
    finally:
        object.__setattr__(binding.declaration, "invariants", previous)
    assert batch.validation_attempts == batch.validation_calls == 1
    assert batch.validated_samples == 0
    assert_released(batch)


def test_configured_policy_keeps_pending_original_identity_seal():
    binding = leaf_binding()
    sample = literal_sample(binding)
    with pytest.raises(ValueError, match="changed before batch receiving"):
        with SampleBatch((binding.operation,), limit=2, numeric_policy=allowance()) as batch:
            batch.receive(binding.operation, sample)
            sample["dimensions"]["items"] = 4
    assert batch.validation_attempts == batch.validated_samples == 0
    assert_released(batch)


def test_empty_configured_scope_does_not_invent_a_numeric_invocation():
    with SampleBatch(
        (leaf_binding().operation,),
        limit=1,
        numeric_policy=allowance(),
    ) as batch:
        pass
    assert batch.validation_attempts == batch.validation_calls == batch.validated_samples == 0
    assert_released(batch)

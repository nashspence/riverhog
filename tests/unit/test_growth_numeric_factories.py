from __future__ import annotations

import importlib
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import riverhog_growth_contracts as growth

from scripts import ci_timing
from scripts.validation_numeric import numeric_environment, numeric_policy_from_environment
from tests.support import growth_batch
from tests.support.growth_batch import SampleBatch
from tests.unit.test_growth_observer_batch import assert_released, projection_bindings

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(params=("checkpoint", "advance"))
def maintained_factory(request, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "packages/riverhog-provenance/tests"))
    if request.param == "checkpoint":
        module = importlib.import_module("test_journal_session_growth")
        return SimpleNamespace(
            module=module,
            factory=module.checkpoint_samples,
            inner="_checkpoint_samples",
            operation=module.JournalValidationSession.advance,
            args=(object(), object(), object()),
            kwargs={
                "operation": module.JournalValidationSession.advance,
                "samples": [],
                "journal_constructions": object(),
            },
        )
    module = importlib.import_module("test_entry_replay_advance_growth")
    return SimpleNamespace(
        module=module,
        factory=module.advance_samples,
        inner="_advance_samples",
        operation=module.EntryReplay.advance,
        args=(object(), object(), object(), object()),
        kwargs={"samples": []},
    )


def test_maintained_factory_gives_owned_policy_to_its_original_four_root_scope(
    maintained_factory, monkeypatch
):
    factory = maintained_factory
    policy = growth.NumericWorkPolicy(123_456, 37)
    events = []
    admitted = object()

    class ObservedBatch:
        def __init__(self, operations, *, limit, numeric_policy):
            assert operations == (factory.operation,)
            assert limit == 4
            assert numeric_policy is policy
            events.append("source admission")

        def __enter__(self):
            events.append("scope entered")
            return admitted

        def __exit__(self, kind, error, traceback):
            assert kind is error is traceback is None
            events.append("scope received")

    def product_body(*args, **kwargs):
        assert args == factory.args
        assert kwargs["batch"] is admitted
        assert kwargs["samples"] is factory.kwargs["samples"]
        events.append("original product body")

    monkeypatch.setattr(factory.module, "SampleBatch", ObservedBatch)
    monkeypatch.setattr(factory.module, factory.inner, product_body)
    factory.factory(*factory.args, **factory.kwargs, numeric_policy=policy)

    assert events == [
        "source admission",
        "scope entered",
        "original product body",
        "scope received",
    ]


def test_maintained_factory_requires_policy_before_starting_product_work(
    maintained_factory, monkeypatch
):
    factory = maintained_factory

    def unexpected(*_args, **_kwargs):
        pytest.fail("missing numeric authority reached source admission or product work")

    monkeypatch.setattr(factory.module, "SampleBatch", unexpected)
    monkeypatch.setattr(factory.module, factory.inner, unexpected)
    with pytest.raises(TypeError, match="numeric_policy"):
        factory.factory(*factory.args, **factory.kwargs)


def test_maintained_factory_receives_original_admission_refusal_before_product_work(
    maintained_factory, monkeypatch
):
    factory = maintained_factory
    policy = growth.NumericWorkPolicy(1, 37)
    refusal = growth.NumericWorkRefused(
        "work_allowance_exhausted",
        operation="source_projection",
        limit=policy.max_work,
        consumed=0,
        max_integer_bits=policy.max_integer_bits,
        requested=5,
    )

    class RefusedBatch:
        def __init__(self, operations, *, limit, numeric_policy):
            assert operations == (factory.operation,)
            assert limit == 4
            assert numeric_policy is policy

        def __enter__(self):
            raise refusal

        def __exit__(self, *_args):
            pytest.fail("a refused source admission entered its product scope")

    def unexpected(*_args, **_kwargs):
        pytest.fail("source admission refusal reached original product work")

    monkeypatch.setattr(factory.module, "SampleBatch", RefusedBatch)
    monkeypatch.setattr(factory.module, factory.inner, unexpected)
    with pytest.raises(growth.NumericWorkRefused) as caught:
        factory.factory(*factory.args, **factory.kwargs, numeric_policy=policy)
    assert caught.value is refusal


@pytest.mark.parametrize("field", ["max_work", "max_integer_bits"])
def test_fixture_refuses_valid_environment_drift_after_producer_admission(field, monkeypatch):
    for key in tuple(os.environ):
        if key.startswith("RIVERHOG_VALIDATION_NUMERIC_"):
            monkeypatch.delenv(key)
    admitted = growth.NumericWorkPolicy(123_456, 37)
    for key, value in numeric_environment(admitted).items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(
        ci_timing, "source_identity", lambda _: {"source_sha": "a" * 40, "source_clean": True}
    )
    recorder = ci_timing.PytestTiming(SimpleNamespace(rootpath=ROOT))
    changed = growth.NumericWorkPolicy(
        admitted.max_work + (1 if field == "max_work" else 0),
        admitted.max_integer_bits + (1 if field == "max_integer_bits" else 0),
    )
    for key, value in numeric_environment(changed).items():
        monkeypatch.setenv(key, value)
    assert numeric_policy_from_environment(os.environ) == changed

    spec = importlib.util.spec_from_file_location(
        "numeric_rollout_root_fixture", ROOT / "conftest.py"
    )
    assert spec is not None and spec.loader is not None
    root_fixtures = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(root_fixtures)
    request = SimpleNamespace(
        config=SimpleNamespace(pluginmanager=SimpleNamespace(get_plugin=lambda _: recorder)),
        node=SimpleNamespace(get_closest_marker=lambda _: None),
    )
    with pytest.raises(ValueError, match="changed after producer admission"):
        root_fixtures.numeric_policy.__wrapped__(request)
    assert recorder.numeric_policy == admitted


def test_currentness_refuses_oversized_unused_source_before_private_validation(monkeypatch):
    parent, _child, unused = projection_bindings()
    original = unused.declaration.bounds[0].expression
    policy = growth.NumericWorkPolicy(1_000_000, 32)

    def unexpected(*_args, **_kwargs):
        pytest.fail("changed oversized source reached private sample validation")

    monkeypatch.setattr(growth_batch, "validate_execution_roots", unexpected)
    try:
        with pytest.raises(growth.NumericWorkRefused) as caught:
            with SampleBatch((parent.operation,), limit=4, numeric_policy=policy) as batch:
                object.__setattr__(
                    unused.declaration.bounds[0],
                    "expression",
                    growth.Expression("constant", value=1 << 32, unit=growth.COUNT),
                )
                batch.flush()
    finally:
        object.__setattr__(unused.declaration.bounds[0], "expression", original)
    assert caught.value.reason == "operand_width"
    assert batch.validation_attempts == batch.validated_samples == 0
    assert_released(batch)

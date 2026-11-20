from __future__ import annotations

import importlib
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_growth_contracts import NumericWorkPolicy


def _metadata(monkeypatch, modules, metrics, calls):
    binding = SimpleNamespace(
        declaration=SimpleNamespace(
            id="fixture.numeric-observer.growth/v1",
            dimensions=(),
            bounds=tuple(SimpleNamespace(metric=name) for name in metrics),
        ),
        delegates=(),
    )

    def project(_bindings, *, numeric_policy):
        calls.append(numeric_policy)
        return {"declarations": {binding.declaration.id: {}}}

    def validate(_projection, _identity, _samples, *, numeric_policy):
        calls.append(numeric_policy)

    for module in modules:
        monkeypatch.setattr(module, "growth_binding", lambda _operation: binding)
        monkeypatch.setattr(module, "project_bindings", project)
        monkeypatch.setattr(module, "validate_execution_samples", validate)
    return binding


@pytest.mark.parametrize("kind", ["lifecycle", "release", "metadata", "discovery", "edge", "sweep"])
def test_nested_observer_validation_reuses_preobserved_numeric_policy(kind, monkeypatch):
    monkeypatch.syspath_prepend(
        str(Path(__file__).resolve().parents[2] / "packages/riverhog-provenance/tests")
    )
    names = {
        "lifecycle": "test_journal_session_lifecycle_growth",
        "release": "tests.unit.test_upload_journal_validation_release_growth",
        "metadata": "tests.unit.test_canonical_discovery_metadata_growth",
        "discovery": "tests.unit.test_discovery_validation_owner_growth",
        "edge": "tests.unit.test_upload_journal_certificate_edge_growth",
        "sweep": "tests.unit.test_upload_journal_validation_sweep_growth",
    }
    module = importlib.import_module(names[kind])
    modules = (module, module.release) if kind == "sweep" else (module,)
    resolver_owner = module.release if kind == "sweep" else module
    policy = NumericWorkPolicy(123_456, 84)
    observing, resolutions, calls = False, [], []

    def resolve(_environment):
        assert not observing, "numeric reader identity was hashed inside a parent observer"
        resolutions.append(policy)
        return policy

    monkeypatch.setattr(resolver_owner, "numeric_policy_from_environment", resolve)
    metrics = (
        ("native_close_calls", "lease_descriptor_closes", "closed_databases")
        if kind in {"release", "sweep"}
        else ("hashed_octets",)
    )
    _metadata(monkeypatch, modules, metrics, calls)

    if kind == "lifecycle":
        recorder = module.LifecycleRecorder()

        @contextmanager
        def replay(*_args, **_kwargs):
            yield {"delegates": {}}, lambda _frame, _primitive: {}

        monkeypatch.setattr(module, "observe_replay", replay)
        monkeypatch.setattr(module, "observed_work", lambda *_args: {})

        def invoke():
            return recorder.measured(lambda: None, None, {}, lambda: "completed")
    elif kind == "release":
        monkeypatch.setattr(module, "CLOSE", lambda _current: None)
        recorder = module.Recorder()
        current = SimpleNamespace(_slots={}, concurrency=1)

        def invoke():
            return recorder.measure(current, module.CLOSE)
    elif kind == "metadata":
        recorder = module.Recorder()
        recorder.catalog.metrics = defaultdict(int)
        implementation = SimpleNamespace(operation=lambda: None)

        def invoke():
            return recorder.sample("operation", {}, implementation=implementation)
    elif kind == "discovery":
        recorder = module.Recorder()
        dimensions = {
            "control_octets": 0,
            "native_binding_octets": 0,
            "native_checkpoint_octets": 0,
            "active_slots": 0,
        }

        def invoke():
            return recorder.sample(lambda: None, dimensions)
    elif kind == "edge":
        recorder = module.EdgeRecorder({})

        def invoke():
            return recorder.primitive(lambda: None, {})
    else:
        monkeypatch.setattr(module, "DROP", lambda *_args: None)
        monkeypatch.setattr(module.release, "DROP", module.DROP)
        recorder = module.Recorder()
        recorder.values = dict.fromkeys(metrics, 0)
        current = SimpleNamespace(_slots={}, concurrency=1)

        def invoke():
            return recorder.invoke_drop(current, "key", object())

    assert resolutions == [policy]
    observing = True
    invoke()
    assert resolutions == [policy]
    assert calls and all(value is recorder.numeric_policy for value in calls)


def test_source_port_receiving_uses_the_policy_captured_before_observation(monkeypatch):
    module = importlib.import_module("tests.unit.test_upload_journal_source_ports_growth")
    policy = NumericWorkPolicy(123_456, 84)
    observing, calls, resolutions = False, [], []

    def resolve(_environment):
        assert not observing, "numeric reader identity was hashed inside a parent observer"
        resolutions.append(policy)
        return policy

    _metadata(monkeypatch, (module,), ("hashed_octets",), calls)
    monkeypatch.setattr(module, "numeric_policy_from_environment", resolve)
    # Keep the real recorder construction, invoke and receiving path. Product
    # I/O and its independent observer are irrelevant to this carrier boundary.
    real_source = module.UploadJournalValidationSource

    class Source:
        verify_current = real_source.verify_current

        def __init__(self, *_args, **_kwargs):
            self._page = None

    monkeypatch.setattr(module, "UploadJournalValidationSource", Source)
    monkeypatch.setattr(module, "selected_pages", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(module, "source_snapshot", lambda *_args: {})
    monkeypatch.setattr(module, "read_dimensions", lambda *_args, **_kwargs: {"deferred_window": 0})
    monkeypatch.setattr(module, "expected_read", lambda *_args, **_kwargs: (None, b""))

    @contextmanager
    def observe(_recorder):
        nonlocal observing
        observing = True
        yield
        # The child's observer ended, but a parent observer remains active.

    monkeypatch.setattr(module.ReadRecorder, "observe", observe)
    case = SimpleNamespace(
        factory=object(), work=SimpleNamespace(cursor=SimpleNamespace(byte_offset=0))
    )
    module.measured_read(None, case, lambda *_args: b"", 0, 0, "cold")
    assert resolutions == [policy]
    assert calls == [policy, policy]


def test_nested_sweep_source_and_scheduler_pass_the_existing_policy(monkeypatch):
    sweep = importlib.import_module("tests.unit.test_upload_journal_validation_sweep_growth")
    scheduler = importlib.import_module("tests.unit.test_upload_journal_service_scheduler_growth")
    policy = NumericWorkPolicy(123_456, 84)
    calls = []
    _metadata(
        monkeypatch,
        (sweep, sweep.release),
        ("native_close_calls", "lease_descriptor_closes", "closed_databases"),
        calls,
    )

    def forbidden(_environment):
        pytest.fail("a nested recorder resolved the numeric policy inside its parent observer")

    monkeypatch.setattr(sweep.release, "numeric_policy_from_environment", forbidden)
    recorder = sweep.Recorder(numeric_policy=policy)

    class NestedAdmission(Exception):
        pass

    def source(_operation, _inputs, *, numeric_policy):
        assert numeric_policy is recorder.numeric_policy
        raise NestedAdmission

    monkeypatch.setattr(sweep.fences, "Recorder", source)
    with pytest.raises(NestedAdmission):
        recorder.measure(
            SimpleNamespace(),
            SimpleNamespace(concurrency=1, _slots={}),
            [],
            {},
            cold=False,
            busy=False,
            borrowed=(),
        )

    monkeypatch.setattr(
        scheduler.callers, "Recorder", lambda *_args: SimpleNamespace(numeric_policy=policy)
    )
    parent = scheduler.Recorder(
        SimpleNamespace(),
        SimpleNamespace(_journal_validation_retirement=SimpleNamespace(root=object())),
    )

    def child(*, numeric_policy):
        assert numeric_policy is parent.numeric_policy is policy
        raise NestedAdmission

    monkeypatch.setattr(scheduler.sweeps, "Recorder", child)
    with pytest.raises(NestedAdmission):
        parent.sweep(object())
    assert parent.depth == 0

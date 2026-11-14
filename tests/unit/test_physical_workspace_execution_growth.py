from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services import physical_execution as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.support.catalog_work import octets
from tests.support.workspace_reclamation import execution_sample
from tests.unit.test_physical_workspace_execution import key


def width(value):
    if is_dataclass(value):
        return octets(asdict(value))
    return octets(value)


@contextmanager
def observed(pool, monkeypatch):
    metrics = dict.fromkeys(
        (
            "mapping_steps",
            "mapping_key_octets",
            "mapping_enumerations",
            "workers_constructed",
            "workers_started",
            "worker_liveness_calls",
            "worker_join_calls",
            "results_received",
            "payload_octets",
        ),
        0,
    )

    active = False

    def record(key=None):
        if not active:
            return
        metrics["mapping_steps"] += 1
        if key is not None:
            metrics["mapping_key_octets"] += width(key)

    class Counted:
        def __getitem__(self, key):
            record(key)
            return super().__getitem__(key)

        def __setitem__(self, key, value):
            record(key)
            return super().__setitem__(key, value)

        def __delitem__(self, key):
            record(key)
            return super().__delitem__(key)

        def __len__(self):
            record()
            return super().__len__()

        def get(self, key, default=None):
            record(key)
            return super().get(key, default)

        def setdefault(self, key, default=None):
            record(key)
            return super().setdefault(key, default)

        def popitem(self, *args, **kwargs):
            record()
            return super().popitem(*args, **kwargs)

        def __iter__(self):
            if active:
                metrics["mapping_enumerations"] += 1
                raise AssertionError("coordination enumerated retained slots")
            return super().__iter__()

        def keys(self):
            if active:
                metrics["mapping_enumerations"] += 1
                raise AssertionError("coordination enumerated retained keys")
            return super().keys()

        def values(self):
            if active:
                metrics["mapping_enumerations"] += 1
                raise AssertionError("coordination enumerated retained values")
            return super().values()

        def items(self):
            if active:
                metrics["mapping_enumerations"] += 1
                raise AssertionError("coordination enumerated retained entries")
            return super().items()

    class CountedDict(Counted, dict):
        pass

    class CountedOrder(Counted, OrderedDict):
        pass

    actual_thread = owner.Thread

    class CountedThread(actual_thread):
        def __init__(self, *args, **kwargs):
            metrics["workers_constructed"] += 1
            super().__init__(*args, **kwargs)

        def start(self):
            metrics["workers_started"] += 1
            return super().start()

    originals = {name: getattr(pool, name) for name in ("_slots", "_poll", "_counts", "_targets")}
    try:
        for name, value in originals.items():
            if name == "_poll":
                value = {k: CountedOrder(v) for k, v in value.items()}
            setattr(pool, name, CountedDict(value))
        with monkeypatch.context() as patch:
            patch.setattr(owner, "OrderedDict", CountedOrder)
            patch.setattr(owner, "Thread", CountedThread)
            # Existing actual workers are joined. Count only the observed
            # pool's zero-wait liveness/join primitives, never fixture waits.
            for slot in originals["_slots"].values():
                actual_alive, actual_join = slot.worker.is_alive, slot.worker.join

                def alive(_actual=actual_alive):
                    metrics["worker_liveness_calls"] += 1
                    return _actual()

                def joined(timeout=None, _actual=actual_join):
                    metrics["worker_join_calls"] += 1
                    assert timeout == 0
                    return _actual(timeout=timeout)

                patch.setattr(slot.worker, "is_alive", alive)
                patch.setattr(slot.worker, "join", joined)
            active = True
            yield metrics
    finally:
        active = False
        # Restore ordinary owning mappings without invoking observed iteration.
        for name in originals:
            current = getattr(pool, name)
            copied = dict.copy(current)
            if name == "_poll":
                copied = {k: OrderedDict(v.copy()) for k, v in copied.items()}
            setattr(pool, name, copied)


def seed(pool, count, target, receiver, *, start=1):
    for number in range(start, start + count):
        identity = object()
        assert (
            pool.try_submit_workspace(
                key(number),
                target_incarnation=target,
                owner=receiver,
                identity=identity,
                work=lambda: None,
            )
            == "submitted"
        )
        pool._slots[key(number)].worker.join(timeout=5)
        assert not pool._slots[key(number)].worker.is_alive()


def sample(binding, projection, dimensions, metrics):
    value = execution_sample(binding, projection, dimensions, metrics, {})
    validate_execution_samples(projection, binding.declaration.id, [value])
    return value


@pytest.mark.growth_witness(
    "physical-workspace-submit/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.physical_execution.PhysicalExecutionOwner.try_submit_workspace",
)
def test_measured_workspace_submission(tmp_path, monkeypatch, record_property):
    binding = growth_binding(owner.PhysicalExecutionOwner.try_submit_workspace)
    projection = project_bindings((binding,))
    samples = []
    for retained in (1, 17, 1025):
        for control in (1, 17, 1025):
            for variant in ("submitted", "retained", "capacity", "busy", "closed"):
                target = "local-" + "x" * control
                tuning = ArchiveExecutionTuning(
                    terminal_workers=retained + (0 if variant == "capacity" else 2),
                    terminal_workers_per_target=retained + 2,
                )
                pool = owner.PhysicalExecutionOwner(tuning)
                receiver, foreign, original = object(), object(), object()
                try:
                    seed(pool, retained, target, foreign)
                    selected = key(retained + 1)
                    if variant == "retained":
                        assert (
                            pool.try_submit_workspace(
                                selected,
                                target_incarnation=target,
                                owner=receiver,
                                identity=original,
                                work=lambda: None,
                            )
                            == "submitted"
                        )
                        pool._slots[selected].worker.join(timeout=5)
                    if variant == "closed":
                        pool.close()
                    if variant == "busy":
                        pool._lock.acquire()
                    try:
                        with observed(pool, monkeypatch) as metrics:
                            result = pool.try_submit_workspace(
                                selected,
                                target_incarnation=target,
                                owner=receiver,
                                identity=original,
                                work=lambda: None,
                            )
                    finally:
                        if variant == "busy":
                            pool._lock.release()
                    assert result == variant
                    samples.append(
                        sample(
                            binding,
                            projection,
                            {
                                "retained_slots": retained + int(variant == "retained"),
                                "quantum": 1,
                                "control_octets_per_step": width(
                                    (selected, target, id(receiver), True)
                                ),
                            },
                            metrics,
                        )
                    )
                finally:
                    pool.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "physical-workspace-collect/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.physical_execution.PhysicalExecutionOwner.collect_workspace",
)
def test_measured_workspace_receiving(monkeypatch, record_property):
    binding = growth_binding(owner.PhysicalExecutionOwner.collect_workspace)
    projection = project_bindings((binding,))
    samples = []
    for retained in (1, 17, 1025):
        for control in (1, 17, 1025):
            for quantum in (1, 3):
                target = "local-" + "x" * control
                pool = owner.PhysicalExecutionOwner(
                    ArchiveExecutionTuning(
                        terminal_workers=retained + 3,
                        terminal_workers_per_target=retained + 3,
                    )
                )
                receiver, foreign = object(), object()
                try:
                    seed(pool, retained, target, foreign)
                    seed(pool, 3, target, receiver, start=retained + 1)
                    with observed(pool, monkeypatch) as metrics:
                        results = pool.collect_workspace(limit=quantum, owner=receiver)
                        metrics["results_received"] = len(results)
                    assert len(results) == quantum
                    assert all(
                        value.owner is receiver and value.result() is None for value in results
                    )
                    samples.append(
                        sample(
                            binding,
                            projection,
                            {
                                "retained_slots": retained + 3,
                                "quantum": quantum,
                                "control_octets_per_step": width(
                                    (key(retained + 1), target, id(receiver), True)
                                ),
                            },
                            metrics,
                        )
                    )
                    assert all(
                        pool.status(key(n)) == "awaiting-join" for n in range(1, retained + 1)
                    )
                finally:
                    pool.close()
    record_property("growth_samples", samples)
    record_property(
        "coordination_conditions",
        "Independent retained slots1/17/1025 and actual complete target widths1/17/1025; "
        "one/three-result receiving quanta. Real serially started/joined threads remain "
        "charged until receiving. All actual mapping primitives and full keys are counted, "
        "whole-pool enumeration is forbidden, foreign originals remain charged and every "
        "observed join is zero-wait. Worker bodies remain outside coordination.",
    )

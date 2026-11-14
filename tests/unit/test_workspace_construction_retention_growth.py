from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services import archive_construction_retention as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.support.catalog_work import octets
from tests.support.workspace_reclamation import execution_sample
from tests.unit.test_workspace_construction_retention import key, retain


@contextmanager
def measured(ledger):
    metrics = dict.fromkeys(
        (
            "mapping_steps",
            "mapping_enumerations",
            "mapping_key_octets",
            "reservations_created",
            "reservations_released",
            "payload_octets",
        ),
        0,
    )
    active = False

    class Observed(dict):
        label = ""

        def record(self, key=None):
            if active:
                metrics["mapping_steps"] += 1
                if key is not None:
                    metrics["mapping_key_octets"] += octets(key)

        def __getitem__(self, key):
            self.record(key)
            return super().__getitem__(key)

        def get(self, key, default=None):
            self.record(key)
            return super().get(key, default)

        def __setitem__(self, key, value):
            self.record(key)
            if active and self.label == "_reservations":
                metrics["reservations_created"] += 1
            return super().__setitem__(key, value)

        def __delitem__(self, key):
            self.record(key)
            if active and self.label == "_reservations":
                metrics["reservations_released"] += 1
            return super().__delitem__(key)

        def __iter__(self):
            if active:
                metrics["mapping_enumerations"] += 1
                pytest.fail("retention enumerated other originals")
            return super().__iter__()

        def values(self):
            if active:
                metrics["mapping_enumerations"] += 1
                pytest.fail("retention enumerated other originals")
            return super().values()

        def items(self):
            if active:
                metrics["mapping_enumerations"] += 1
                pytest.fail("retention enumerated other originals")
            return super().items()

        def keys(self):
            if active:
                metrics["mapping_enumerations"] += 1
                pytest.fail("retention enumerated other originals")
            return super().keys()

    names = ("_reservations", "_counts", "_targets", "_capacity", "_per_target")
    try:
        for name in names:
            original = getattr(ledger, name)
            observed = Observed(original)
            observed.label = name
            setattr(ledger, name, observed)
        active = True
        yield metrics
    finally:
        active = False
        for name in names:
            setattr(ledger, name, dict.copy(getattr(ledger, name)))


def exercise(record_property, *, retire):
    operation = (
        owner.ArchiveConstructionRetention.retire
        if retire
        else owner.ArchiveConstructionRetention.try_retain_workspace
    )
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    samples = []
    for retained in (1, 17, 1025):
        for control in (1, 17, 1025):
            variants = (
                ("original", "foreign", "copy", "busy")
                if retire
                else (
                    "admitted",
                    "global-capacity",
                    "target-capacity",
                    "busy",
                )
            )
            for variant in variants:
                capacity = retained if variant == "global-capacity" else retained + 2
                per_target = retained if variant == "target-capacity" else retained + 2
                ledger = owner.ArchiveConstructionRetention(
                    ArchiveExecutionTuning(
                        retained_constructions=capacity,
                        retained_constructions_per_target=per_target,
                    )
                )
                receiver = object()
                target = "local-" + "x" * control
                for index in range(1, retained + 1):
                    assert not isinstance(retain(ledger, object(), index, target), str)
                original = None
                if retire:
                    original = retain(ledger, receiver, retained + 1, target)
                    assert not isinstance(original, str)
                if variant == "busy":
                    ledger._lock.acquire()
                try:
                    with measured(ledger) as metrics:
                        if retire:
                            if variant in {"copy", "foreign"}:
                                with pytest.raises(owner.Conflict):
                                    ledger.retire(
                                        replace(original) if variant == "copy" else original,
                                        owner=object() if variant == "foreign" else receiver,
                                    )
                            else:
                                assert ledger.retire(original, owner=receiver) is (
                                    variant != "busy"
                                )
                        else:
                            result = retain(ledger, receiver, retained + 1, target)
                            expected = {
                                "global-capacity": "retention-capacity",
                                "target-capacity": "target-retention-capacity",
                                "busy": "busy",
                            }.get(variant)
                            assert (
                                isinstance(result, owner.WorkspaceConstructionReservation)
                                if expected is None
                                else result == expected
                            )
                finally:
                    if variant == "busy":
                        ledger._lock.release()
                sample = execution_sample(
                    binding,
                    projection,
                    {
                        "retained_slots": retained + int(retire),
                        "control_octets_per_step": octets((key(), target, id(receiver), False)),
                    },
                    metrics,
                    {},
                )
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
    record_property("growth_samples", samples)
    record_property(
        "retention_conditions",
        "Complete observed mapping primitives/keys, independent retained originals and "
        "full target widths1/17/1025, admission, both capacity refusals, busy contention, "
        "exact withdrawal, foreign receiver and copied reservation. No original census "
        "or resource body inspection; ordinary and reserved terminal lanes stay separate.",
    )


@pytest.mark.growth_witness(
    "workspace-construction-admission/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.archive_construction_retention.ArchiveConstructionRetention.try_retain_workspace",
)
def test_measured_shared_workspace_admission(record_property):
    exercise(record_property, retire=False)


@pytest.mark.growth_witness(
    "original-construction-withdrawal/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.archive_construction_retention.ArchiveConstructionRetention.retire",
)
def test_measured_exact_original_withdrawal(record_property):
    exercise(record_property, retire=True)

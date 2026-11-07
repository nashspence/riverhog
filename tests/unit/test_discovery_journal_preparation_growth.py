from __future__ import annotations

import pytest
from riverhog_core import canonical_discovery_journal_preparation as preparation
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationWork
from riverhog_provenance import JournalValidationBudget

from tests.unit.test_discovery_journal_owner import prepared, quantum
from tests.unit.test_discovery_journal_owner_growth import observe, seed_independent_registrations
from tests.unit.test_journal_validation_workspace_reference_growth import sample

PREPARE = DiscoveryValidationWork.prepare_journal
ADVANCE = DiscoveryValidationWork.advance_journal


def measure(work, framing, operation, values, monkeypatch, samples):
    with observe(operation, monkeypatch) as metrics:
        result = (
            operation(work, framing)
            if operation is PREPARE
            else operation(
                work,
                framing,
                budget=JournalValidationBudget(rows=8),
            )
        )
    samples.append(sample(operation, values, metrics))
    return result


def exercise(tmp_path, monkeypatch, record_property, operation):
    samples = []
    for retained in (1, 17, 1025):
        with prepared(tmp_path / str(retained)) as case:
            with quantum(case) as (work, framing):
                seed_independent_registrations(work, framing, retained)
                values = {"retained_journals": retained + 1}
                before = 0
                if operation is ADVANCE:
                    assert measure(work, framing, operation, values, monkeypatch, samples) is None
                while True:
                    provider = (
                        measure(work, framing, operation, values, monkeypatch, samples)
                        if operation is PREPARE
                        else work.prepare_journal(framing)
                    )
                    if provider is not None:
                        break
                    with work.metadata.read() as (db, key):
                        progress = preparation.discovery_journal_preparation_progress(
                            db,
                            key,
                            work._slot.preparation,
                        )
                    assert progress.version > before
                    before = progress.version
                for _retry in range(3):
                    result = measure(work, framing, operation, values, monkeypatch, samples)
                    assert result is not None
                    if operation is PREPARE:
                        assert result is provider
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-preparation-dispatch/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.prepare_journal"
    ),
)
def test_actual_original_preparation_dispatch_and_cached_evidence(
    tmp_path,
    monkeypatch,
    record_property,
):
    exercise(tmp_path, monkeypatch, record_property, PREPARE)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-advance-dispatch/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.advance_journal"
    ),
)
def test_actual_pending_open_and_warm_native_advance_dispatch(
    tmp_path,
    monkeypatch,
    record_property,
):
    exercise(tmp_path, monkeypatch, record_property, ADVANCE)

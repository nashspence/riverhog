from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services.archive_construction_retention import (
    ArchiveConstructionReservation,
    ArchiveConstructionRetention,
)
from riverhog_core.services.physical_execution import PhysicalExecutionKey
from riverhog_protocol.errors import Conflict


def _key(number=1):
    return PhysicalExecutionKey(number, "a" * 64)


def test_reconstructed_foreign_and_stale_reservations_cannot_retire_an_original():
    retention = ArchiveConstructionRetention(ArchiveExecutionTuning(retained_constructions=1))
    owner = object()
    original = retention.try_retain(_key(), target_incarnation="target-a", owner=owner)
    assert isinstance(original, ArchiveConstructionReservation)
    with pytest.raises(Conflict, match="original owner"):
        retention.retire(replace(original), owner=owner)
    with pytest.raises(Conflict, match="original owner"):
        retention.retire(original, owner=object())
    assert (
        retention.try_retain(_key(2), target_incarnation="target-b", owner=owner)
        == "retention-capacity"
    )
    assert retention.retire(original, owner=owner)
    replacement = retention.try_retain(_key(), target_incarnation="target-a", owner=owner)
    assert isinstance(replacement, ArchiveConstructionReservation)
    with pytest.raises(Conflict, match="original owner"):
        retention.retire(original, owner=owner)
    assert (
        retention.try_retain(_key(2), target_incarnation="target-b", owner=owner)
        == "retention-capacity"
    )
    assert retention.retire(replacement, owner=owner)


def test_contended_retirement_preserves_the_charge_without_waiting():
    retention = ArchiveConstructionRetention(ArchiveExecutionTuning(retained_constructions=1))
    owner = object()
    original = retention.try_retain(_key(), target_incarnation="target-a", owner=owner)
    assert isinstance(original, ArchiveConstructionReservation)
    with retention._lock:
        assert not retention.retire(original, owner=owner)
        assert retention.try_retain(_key(2), target_incarnation="target-b", owner=owner) == "busy"
    assert (
        retention.try_retain(_key(2), target_incarnation="target-b", owner=owner)
        == "retention-capacity"
    )
    assert retention.retire(original, owner=owner)


def test_last_shared_retention_slot_cannot_be_spent_twice():
    retention = ArchiveConstructionRetention(ArchiveExecutionTuning(retained_constructions=1))
    start = Barrier(2)
    owners = (object(), object())

    def reserve(number):
        start.wait(timeout=5)
        return retention.try_retain(
            _key(number + 1), target_incarnation="target-a", owner=owners[number]
        )

    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(reserve, number) for number in range(2)]
        results = [future.result(timeout=10) for future in futures]
    originals = [result for result in results if isinstance(result, ArchiveConstructionReservation)]
    assert len(originals) == 1
    assert all(
        isinstance(result, ArchiveConstructionReservation)
        or result in {"busy", "retention-capacity"}
        for result in results
    )
    assert (
        retention.try_retain(_key(3), target_incarnation="target-b", owner=object())
        == "retention-capacity"
    )
    original = originals[0]
    assert retention.retire(original, owner=original.owner)


def test_foreign_reservation_cannot_invoke_equality_inside_retirement():
    class ForeignReservation(ArchiveConstructionReservation):
        def __hash__(self):
            pytest.fail("retirement must not invoke foreign hashing")

        def __eq__(self, other):
            pytest.fail("retirement must not invoke foreign equality")

    retention = ArchiveConstructionRetention(ArchiveExecutionTuning(retained_constructions=1))
    owner = object()
    original = retention.try_retain(_key(), target_incarnation="target-a", owner=owner)
    assert isinstance(original, ArchiveConstructionReservation)
    foreign = ForeignReservation(original.key, original.target_incarnation, owner)
    with pytest.raises(Conflict, match="original owner"):
        retention.retire(foreign, owner=owner)
    assert (
        retention.try_retain(_key(2), target_incarnation="target-b", owner=owner)
        == "retention-capacity"
    )
    assert retention.retire(original, owner=owner)

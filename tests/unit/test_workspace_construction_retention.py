from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services.archive_construction_retention import (
    ArchiveConstructionRetention,
    WorkspaceConstructionReservation,
)
from riverhog_core.services.physical_execution import (
    PhysicalExecutionKey,
    PhysicalWorkspaceExecutionKey,
)
from riverhog_protocol.errors import Conflict


def key(number=1):
    return PhysicalWorkspaceExecutionKey(f"{number:032x}", "a" * 64)


def retain(ledger, receiver, number=1, target="local"):
    return ledger.try_retain_workspace(key(number), target_incarnation=target, owner=receiver)


def test_workspace_shares_ordinary_capacity_and_preserves_terminal_reserve():
    ledger = ArchiveConstructionRetention(
        ArchiveExecutionTuning(
            retained_constructions=1,
            retained_constructions_per_target=1,
            retained_terminals=1,
            retained_terminals_per_target=1,
        )
    )
    receiver = object()
    original = retain(ledger, receiver)
    assert isinstance(original, WorkspaceConstructionReservation) and not original.terminal
    assert (
        ledger.try_retain(
            PhysicalExecutionKey(1, "b" * 64),
            target_incarnation="archive",
            owner=object(),
        )
        == "retention-capacity"
    )
    terminal = ledger.try_retain(
        PhysicalExecutionKey(1, "b" * 64),
        target_incarnation="archive",
        owner=object(),
        terminal=True,
    )
    assert not isinstance(terminal, str) and terminal.terminal
    assert ledger.retire(original, owner=receiver)
    assert (
        ledger.try_retain(
            PhysicalExecutionKey(1, "b" * 64),
            target_incarnation="archive",
            owner=object(),
        )
        != "retention-capacity"
    )
    assert ledger._counts == {False: 1, True: 1}


def test_archive_retention_prevents_workspace_overcommit():
    ledger = ArchiveConstructionRetention(ArchiveExecutionTuning(retained_constructions=1))
    archived = ledger.try_retain(
        PhysicalExecutionKey(1, "b" * 64),
        target_incarnation="archive",
        owner=object(),
    )
    assert not isinstance(archived, str)
    assert retain(ledger, object()) == "retention-capacity"


def test_workspace_target_limit_keeps_other_original_targets_available():
    ledger = ArchiveConstructionRetention(
        ArchiveExecutionTuning(
            retained_constructions=3,
            retained_constructions_per_target=1,
        )
    )
    receiver = object()
    original = retain(ledger, receiver)
    assert not isinstance(original, str)
    assert retain(ledger, object(), 2) == "target-retention-capacity"
    other = retain(ledger, object(), 2, "other")
    assert isinstance(other, WorkspaceConstructionReservation)
    assert ledger.retire(original, owner=receiver)
    assert isinstance(retain(ledger, receiver, 3), WorkspaceConstructionReservation)


def test_only_actual_original_reservation_and_receiver_can_release_charge():
    ledger = ArchiveConstructionRetention(ArchiveExecutionTuning(retained_constructions=1))
    receiver = object()
    original = retain(ledger, receiver)
    assert not isinstance(original, str)
    for value, recipient in ((replace(original), receiver), (original, object())):
        with pytest.raises(Conflict, match="original owner"):
            ledger.retire(value, owner=recipient)
        assert ledger._counts[False] == 1 and retain(ledger, object(), 2) == "retention-capacity"
    assert ledger.retire(original, owner=receiver)
    with pytest.raises(Conflict):
        ledger.retire(original, owner=receiver)


def test_busy_ledger_keeps_original_charge_and_defers_retirement():
    ledger = ArchiveConstructionRetention(ArchiveExecutionTuning())
    receiver = object()
    original = retain(ledger, receiver)
    assert not isinstance(original, str)
    with ledger._lock:
        assert retain(ledger, object(), 2) == "busy"
        assert not ledger.retire(original, owner=receiver)
    assert ledger._counts[False] == 1
    assert ledger.retire(original, owner=receiver)


def test_equal_receivers_cannot_adopt_originals():
    class EqualReceiver:
        def __eq__(self, other):
            pytest.fail("retention uses receiver identity")

    ledger = ArchiveConstructionRetention(ArchiveExecutionTuning())
    receiver = EqualReceiver()
    original = retain(ledger, receiver)
    assert not isinstance(original, str)
    with pytest.raises(Conflict):
        ledger.retire(original, owner=EqualReceiver())
    assert ledger.retire(original, owner=receiver)


@pytest.mark.parametrize("target", ("",))
def test_workspace_requires_original_target(target):
    with pytest.raises(ValueError):
        retain(ArchiveConstructionRetention(ArchiveExecutionTuning()), object(), target=target)


def test_workspace_cannot_use_collection_submission_or_terminal_retention_lane():
    ledger = ArchiveConstructionRetention(ArchiveExecutionTuning())
    with pytest.raises(ValueError):
        ledger.try_retain(key(), target_incarnation="local", owner=object(), terminal=True)
    with pytest.raises(TypeError):
        ledger.try_retain_workspace(
            key(), target_incarnation="local", owner=object(), terminal=True
        )
    assert ledger._counts == {False: 0, True: 0}

"""Synchronous test driving over the maintained bounded archive coordinator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from riverhog_core.services.archive_finalizer_registry import ArchiveFinalizerTurn

if TYPE_CHECKING:
    from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService


def drive_finalization_turn(service: SqlAlchemyCollectionUploadService, *, limit: int = 1) -> int:
    """Join test-owned workers outside SQL; the maintained owner receives results."""
    progressed = service.process_due_finalizations(limit=limit)
    for slot in tuple(service._archive_execution._slots.values()):
        # CPU work such as provenance validation may outlast a short poll.
        # This test-owned wait still requires actual exit before receiving.
        slot.worker.join(timeout=60)
        assert not slot.worker.is_alive(), "test-owned archive worker did not return"
    return progressed + service.process_due_archive_uploads(limit=limit)


def drive_custody_turn(service: SqlAlchemyCollectionUploadService, *, limit: int = 1) -> bool:
    """Observe actual semantic outcomes after test-owned physical worker joins."""
    service.process_due_custody_receipts(limit=limit)
    slots = tuple(service._archive_execution._slots.values())
    progressed = False
    for slot in slots:
        slot.worker.join(timeout=60)
        assert not slot.worker.is_alive(), "test-owned custody worker did not return"
        outcome = slot.future.result()
        if isinstance(outcome, ArchiveFinalizerTurn):
            progressed |= outcome.progressed
    service.process_due_archive_uploads(limit=max(limit, len(slots)))
    return progressed

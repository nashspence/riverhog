"""Synchronous test driving over the maintained bounded archive coordinator."""

from __future__ import annotations

from typing import TYPE_CHECKING

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

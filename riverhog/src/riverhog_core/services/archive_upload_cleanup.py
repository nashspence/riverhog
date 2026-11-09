"""Archive cleanup requires the composing owner's current global disposal decision."""

from __future__ import annotations

import json
from collections.abc import Callable

from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteEffectRecord,
)
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.object_cleanup_receiving import CleanupScope, ObjectCleanupReceiving


class ArchiveUploadCleanup(ObjectCleanupReceiving):
    def _scope_id(self, operation_id: str) -> str:
        return operation_id

    def _context(
        self,
        session: Session,
        collection_id: int,
        operation_id: str,
        *,
        admit: bool = False,
        guard: Callable[[Session], None] | None = None,
    ) -> CleanupScope:
        if admit and not callable(guard):
            raise ValueError("cleanup admission requires a current disposal guard")
        control, terminal = lock_terminal_selection(
            session, collection_id, operation_id, require_upload=admit, guard=guard
        )
        if admit:
            if terminal.phase not in {"cleanup-ready", "cleaning"}:
                raise Conflict("upload terminal has no retained cleanup-ready decision")
            for model in (
                ArchiveWriteEffectRecord,
                CachePopulationBeginRecord,
                CachePopulationWriteEffectRecord,
            ):
                if (
                    session.scalar(
                        select(model.id)
                        .where(model.collection_id == collection_id, model.unresolved.is_(True))
                        .limit(1)
                    )
                    is not None
                ):
                    raise Conflict("upload terminal still has unclosed forward records")
        construction = json.loads(control.construction_json)
        return CleanupScope(
            operation_id,
            operation_id,
            construction["archive_incarnation_id"],
            construction["archive_storage_prefix"],
        )

    def _page_selected(self, session: Session, scope: CleanupScope) -> None:
        terminal = session.get(ArchiveUploadTerminalRecord, scope.operation_id)
        if terminal is None:
            raise Conflict("original archive cleanup terminal is unavailable")
        terminal.phase = "cleaning"

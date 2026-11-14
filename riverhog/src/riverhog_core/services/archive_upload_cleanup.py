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
from riverhog_core.catalog_object_cleanup_models import (
    ObjectCleanupAttemptRecord,
    ObjectCleanupPageRecord,
)
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.archive_upload_terminal_control import require_prepared_terminal_result
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector
from riverhog_core.services.object_cleanup_receiving import (
    CleanupScope,
    ObjectCleanupReceiving,
    RetainedCleanupPage,
)


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
            require_prepared_terminal_result(
                terminal, load_terminal_tag_selector(session, operation_id)
            )
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

    def require_exhausted(
        self,
        session: Session,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
    ) -> RetainedCleanupPage:
        """Require exact current cleanup proof; retain it for catalog teardown.

        An empty listing or an exhausted page cannot close unknown original
        attempts. This validates only this archive scope, leaving independent
        cache cleanup, registration and the confirmed terminal result untouched.
        """
        scope = self._context(session, collection_id, operation_id, admit=True, guard=guard)
        record = session.get(ObjectCleanupPageRecord, scope.scope_id)
        if record is None:
            raise Conflict("archive cleanup has no retained inventory proof")
        page = self._page(record, operation_id, scope)
        if not page.complete or not page.request.inventory.exhausted:
            raise Conflict("archive cleanup inventory proof is incomplete")
        if (
            session.scalar(
                select(ObjectCleanupAttemptRecord.id)
                .where(ObjectCleanupAttemptRecord.scope_id == scope.scope_id)
                .limit(1)
            )
            is not None
        ):
            raise Conflict("archive cleanup still has original cleanup receiving")
        return page

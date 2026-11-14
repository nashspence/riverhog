"""Resolve one proposed workflow claim before selecting semantic constructors."""

from __future__ import annotations

from collections.abc import Callable
from threading import Event
from typing import TYPE_CHECKING

from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.services.archive_write_admission import _writer

if TYPE_CHECKING:
    from riverhog_core.services.archive_finalization_pipeline import ArchiveFinalizationStage


class OriginalFinalizationAdmission:
    """A fixed-size source retained by the original pipeline and physical worker.

    Local registration precedes the proposed claim's transaction exit. Only that
    registering owner releases ready, in finally after its transaction finishes.
    The worker waits outside SQL, then resolves the exact durable workflow nonce
    under its parent writer before selecting any semantic constructor. An
    unresolved observation retains the original selection for retry.

    This decision selects this proposed workflow turn. Each semantic child keeps
    its own current construction, permission and provider admission checks.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        claim_id: str,
        ready: Event,
    ) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._claim_id = claim_id
        self._ready = ready
        self._admitted: bool | None = None

    def select(
        self, select_source: Callable[[], Callable[[], ArchiveFinalizationStage] | None]
    ) -> Callable[[], ArchiveFinalizationStage] | None:
        # Called only by this original pipeline's serialized physical worker.
        if self._admitted is None:
            self._ready.wait()
            with session_scope(self._factory) as session:
                _writer(session, defer_if_busy=True)
                current = session.execute(
                    select(
                        CollectionUploadRecord.state,
                        CollectionUploadRecord.archive_phase,
                        CollectionUploadRecord.archive_claim_id,
                    )
                    .where(CollectionUploadRecord.collection_id == self._collection_id)
                    .with_for_update(nowait=True)
                ).one_or_none()
                if current != ("finalizing", "finalizing", self._claim_id):
                    admitted = False
                else:
                    gate = session.scalar(
                        select(ArchiveWriteControlRecord.gate)
                        .where(ArchiveWriteControlRecord.collection_id == self._collection_id)
                        .with_for_update(nowait=True)
                    )
                    if gate is None:
                        raise Conflict("proposed finalization construction control is unavailable")
                    admitted = gate == "open"
            # A failed read/transaction never caches a no-effect decision. This
            # assignment precedes the first possible semantic constructor.
            self._admitted = admitted
        return select_source() if self._admitted else None

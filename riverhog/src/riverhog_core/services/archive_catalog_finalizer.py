"""Retain one bounded finalization transaction through its original commit reply."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.services.collection_upload_authority import (
    require_archive_write_control,
    require_open_upload_writes,
)

CatalogFinalizationKind = Literal["member-histories", "provenance-closure", "artifact-tree"]

_CURSORS: dict[CatalogFinalizationKind, tuple[str, ...]] = {
    "member-histories": (
        "provenance_history_after_artifact_id",
        "provenance_histories_sealed",
    ),
    "provenance-closure": (
        "provenance_validation_after_artifact_id",
        "provenance_validation_next_artifact_order",
        "provenance_closure_validated",
    ),
    "artifact-tree": (
        "archive_tree_after_artifact_id",
        "archive_tree_next_artifact_order",
        "archive_tree_hash_state",
        "archive_tree_sha256",
    ),
}


class ArchiveCatalogFinalizer:
    """One exact catalog turn, without provider work or global closure authority.

    The application supplies its bounded transaction body. Its result and cursor
    are captured before commit; acknowledgement loss retains both. A later turn
    can receive the exact committed result or retry the same rolled-back source,
    never silently advance a fresh batch. Completed owners remain single use.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        claim_id: str,
        kind: CatalogFinalizationKind,
        advance: Callable[[Session, CollectionUploadRecord], bool],
    ) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._claim_id = claim_id
        self._fields = _CURSORS[kind]
        self._advance = advance
        self._lock = Lock()
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)
        self._construction: str | None = None
        self._epoch: str | None = None
        self._before: tuple[object, ...] | None = None
        self._after: tuple[object, ...] | None = None
        self._result: bool | None = None
        self._completed = False

    @property
    def has_pending(self) -> bool:
        return self._before is not None and not self._completed

    def _cursor(self, upload: CollectionUploadRecord) -> tuple[object, ...]:
        return tuple(getattr(upload, field) for field in self._fields)

    def advance(self) -> bool:
        with self._lock:
            self._terminal.require_forward()
            if self._completed:
                assert self._result is not None
                return self._result
            with session_scope(self._factory) as session:
                _writer(session)
                upload = session.scalar(
                    select(CollectionUploadRecord)
                    .where(CollectionUploadRecord.collection_id == self._collection_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if (
                    upload is None
                    or upload.state != "finalizing"
                    or upload.archive_phase != "finalizing"
                    or upload.archive_claim_id != self._claim_id
                ):
                    raise Conflict("original catalog finalization claim is unavailable")
                control = require_archive_write_control(session, upload)
                if (
                    self._construction is not None
                    and control.construction_json != self._construction
                ):
                    raise Conflict("original catalog finalization construction changed")
                cursor = self._cursor(upload)
                if self._after is not None and cursor == self._after:
                    # Receiving an already committed catalog result creates no
                    # new effect and needs no fresh open provider admission.
                    assert self._result is not None
                else:
                    require_open_upload_writes(session, upload)
                    if self._before is None:
                        self._construction = control.construction_json
                        self._epoch = control.epoch
                        self._before = cursor
                    elif cursor != self._before or control.epoch != self._epoch:
                        raise Conflict("original catalog finalization cursor changed")
                    result = self._advance(session, upload)
                    session.flush()
                    self._after = self._cursor(upload)
                    self._result = result
            # A commit exception leaves the original preimage, postimage and
            # result available to this owner, rather than selecting another page.
            self._completed = True
            assert self._result is not None
            return self._result

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            # Catalog transactions hold the construction lock. Once joined,
            # there is no outside writer to cancel; retained facts stay owned.
            return self._terminal.drain_original(operation_id, None)
        finally:
            self._lock.release()

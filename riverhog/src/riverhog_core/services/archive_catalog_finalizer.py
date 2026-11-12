"""Retain one bounded finalization transaction through its original commit reply."""

from __future__ import annotations

import json
from collections.abc import Callable
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict
from riverhog_provenance import JournalConstruction
from sqlalchemy import inspect, select
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionRecord, CollectionUploadRecord
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.services.collection_upload_authority import (
    require_archive_write_control,
    require_open_upload_writes,
)
from riverhog_core.services.upload_journal_result_receiving import UploadJournalResultResources

CatalogFinalizationKind = Literal[
    "member-histories", "provenance-closure", "artifact-tree", "catalog-projection"
]

_PUBLICATION = (
    "creation_identity_sha256",
    "creation_archive_store",
    "archive_generation",
    "delivery_context_id",
    "artifact_set_identity",
    "encryption_format",
    "passphrase_id",
    "provenance_identity",
    "inventory_identity",
    "archive_root_sha256",
    "artifact_count",
    "artifact_bytes",
)

_CURSORS: dict[CatalogFinalizationKind, tuple[str, ...]] = {
    "catalog-projection": (
        "catalog_phase",
        "catalog_cursor_json",
        "catalog_hash_state",
        "catalog_artifact_set_identity",
        "catalog_inventory_identity",
        "final_authority_json",
        "provenance_identity",
        "description_revision",
        "description_identity",
        "tag_revision",
        "tag_root_sha256",
        "tag_set_identity",
        "tag_head_identity",
        "completion_receipt_json",
        "artifact_count",
        "artifact_bytes",
        "archive_tree_sha256",
        "archive_ordered_volume_sha256",
    ),
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
        construction_factory: Callable[[], JournalConstruction],
        advance: Callable[[Session, CollectionUploadRecord], bool],
    ) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._claim_id = claim_id
        self._kind = kind
        self._fields = _CURSORS[kind]
        self._advance = advance
        self._lock = Lock()
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)
        self._construction: str | None = None
        self._epoch: str | None = None
        self._before: tuple[object, ...] | None = None
        self._after: tuple[object, ...] | None = None
        self._result: bool | None = None
        self._published_after: tuple[object, ...] | None = None
        self._completed = False
        self.result_resources = UploadJournalResultResources(construction_factory)

    @property
    def has_pending(self) -> bool:
        return (self._before is not None and not self._completed) or self.result_resources.pending

    def _cursor(self, upload: CollectionUploadRecord) -> tuple[object, ...]:
        return tuple(getattr(upload, field) for field in self._fields)

    def advance(self) -> bool:
        with self._lock:
            self.result_resources.close()
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
                if upload is None:
                    self._receive_publication(session)
                else:
                    self._advance_current(session, upload)
            # A commit exception leaves the original preimage, postimage and
            # result available to this owner, rather than selecting another page.
            self.result_resources.close()
            self._completed = True
            assert self._result is not None
            return self._result

    def _advance_current(self, session: Session, upload: CollectionUploadRecord) -> None:
        if (
            upload.state != "finalizing"
            or upload.archive_phase != "finalizing"
            or upload.archive_claim_id != self._claim_id
        ):
            raise Conflict("original catalog finalization claim is unavailable")
        control = require_archive_write_control(session, upload)
        if self._construction is not None and control.construction_json != self._construction:
            raise Conflict("original catalog finalization construction changed")
        cursor = self._cursor(upload)
        if self._after is not None and cursor == self._after:
            # Original receiving creates no effect or fresh admission.
            assert self._result is not None
            return
        require_open_upload_writes(session, upload)
        if self._before is None:
            self._construction = control.construction_json
            self._epoch = control.epoch
            self._before = cursor
        elif cursor != self._before or control.epoch != self._epoch:
            raise Conflict("original catalog finalization cursor changed")
        result = self._advance(session, upload)
        session.flush()
        if inspect(upload).deleted:
            published = self._publication(session)
            if (
                self._kind != "catalog-projection"
                or result is not True
                or published != self._expected_publication(upload)
            ):
                raise Conflict("original catalog publication has no receiving proof")
            self._published_after = published
            self._after = None
        else:
            self._after = self._cursor(upload)
        self._result = result

    @staticmethod
    def _expected_publication(upload: CollectionUploadRecord) -> tuple[object, ...]:
        if upload.final_authority_json is None:
            raise Conflict("original catalog archive authority is unavailable")
        root_sha256 = json.loads(upload.final_authority_json)["root"]["plaintext_sha256"]
        return (
            upload.creation_identity_sha256,
            upload.archive_store,
            upload.archive_generation,
            upload.delivery_context_id,
            upload.catalog_artifact_set_identity,
            upload.encryption_format,
            upload.passphrase_id,
            upload.provenance_identity,
            upload.catalog_inventory_identity,
            root_sha256,
            upload.artifact_count,
            upload.artifact_bytes,
        )

    def _publication(self, session: Session) -> tuple[object, ...] | None:
        collection = session.get(CollectionRecord, self._collection_id)
        if collection is None or not collection.is_published:
            return None
        return tuple(getattr(collection, field) for field in _PUBLICATION)

    def _receive_publication(self, session: Session) -> None:
        # Parent absence alone proves no outcome. Only this owner's captured
        # original publication can be acknowledged against its durable root.
        if self._published_after is None or self._result is not True:
            raise Conflict("original catalog finalization claim is unavailable")
        control = session.get(ArchiveWriteControlRecord, self._collection_id)
        if control is None or control.construction_json != self._construction:
            raise Conflict("original catalog finalization construction changed")
        if self._publication(session) != self._published_after:
            raise Conflict("original catalog publication receiving proof changed")

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            # Catalog transactions hold the construction lock. Once joined,
            # there is no outside writer to cancel; retained facts stay owned.
            self.result_resources.close()
            return self._terminal.drain_original(operation_id, None)
        finally:
            self._lock.release()

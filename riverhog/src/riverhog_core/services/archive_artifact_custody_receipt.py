"""Receive one verified artifact custody receipt without repeating dependency work."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock

from riverhog_protocol import CollectionUploadArtifactCustodyReceiptDocument
from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionArtifactRecord, CollectionRecord
from riverhog_core.catalog_models import CollectionUploadArtifactProvenanceBindingRecord as Binding
from riverhog_core.catalog_models import CollectionUploadArtifactRecord as Artifact
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.services.collection_upload_authority import (
    require_archive_write_control,
    require_open_upload_writes,
)


def _matches_primary(
    binding: Binding, receipt: CollectionUploadArtifactCustodyReceiptDocument
) -> bool:
    primary = receipt.primary
    anchor = primary.journal
    through = anchor.through
    return (
        binding.journal_id == anchor.journal_id
        and binding.through_entry_id == through.entry_id
        and binding.through_sequence == int(through.sequence)
        and binding.through_json_sha256 == through.json_sha256
        and binding.prefix_sha256 == anchor.prefix_sha256
        and binding.prefix_bytes == int(anchor.prefix_bytes)
        and binding.delivery_association_id == primary.delivery_association_id
    )


@dataclass(frozen=True, slots=True)
class ArchiveArtifactCustodyReceiptSource:
    """The application verifies dependencies before capturing this original source.

    This acknowledgement records that already verified member and its exact
    accepted provenance/requirement. It grants no provider execution, lease renewal,
    terminal disposition or independent reconstruction of operational custody.
    """

    collection_id: int
    artifact_id: str
    construction_json: str = field(repr=False)
    epoch: str = field(repr=False)
    receipt_json: str = field(repr=False)
    history_imports_ref_json: str | None = field(repr=False)
    completion_requirement_json: str | None = field(repr=False)
    completion_output_id: str | None = field(repr=False)

    @classmethod
    def capture(
        cls,
        session: Session,
        upload: Upload,
        artifact: Artifact,
        binding: Binding,
        *,
        receipt: CollectionUploadArtifactCustodyReceiptDocument,
        completion_output_id: str | None,
        grants: ArchiveWriteGrants,
    ) -> ArchiveArtifactCustodyReceiptSource:
        control = require_open_upload_writes(session, upload)
        source = cls(
            upload.collection_id,
            artifact.artifact_id,
            control.construction_json,
            control.epoch,
            receipt.model_dump_json(),
            binding.history_imports_ref_json,
            upload.completion_requirement_json,
            completion_output_id,
        )
        if source.reserved_source_bytes > grants.metadata_preparation_bytes:
            raise Conflict("artifact custody source exceeds its preparation grant")
        source._require_source(upload, artifact, binding)
        return source

    @property
    def reserved_source_bytes(self) -> int:
        return sum(
            len(value.encode())
            for value in (
                str(self.collection_id),
                self.artifact_id,
                self.construction_json,
                self.epoch,
                self.receipt_json,
                self.history_imports_ref_json or "",
                self.completion_requirement_json or "",
                self.completion_output_id or "",
            )
        )

    def _document(self) -> CollectionUploadArtifactCustodyReceiptDocument:
        receipt = CollectionUploadArtifactCustodyReceiptDocument.model_validate_json(
            self.receipt_json
        )
        if (
            int(receipt.collection_id) != self.collection_id
            or str(receipt.artifact_id) != self.artifact_id
        ):
            raise Conflict("original artifact custody receipt identity changed")
        return receipt

    def _require_source(self, upload: Upload, artifact: Artifact, binding: Binding) -> None:
        receipt = self._document()
        if (
            upload.collection_id != self.collection_id
            or artifact.collection_id != self.collection_id
            or artifact.artifact_id != self.artifact_id
            or binding.collection_id != self.collection_id
            or binding.artifact_id != self.artifact_id
            or artifact.payload_sealed_at is None
            or artifact.bytes != int(receipt.bytes)
            or artifact.sha256 != receipt.sha256
            or not _matches_primary(binding, receipt)
            or upload.completion_requirement_json != self.completion_requirement_json
            or binding.history_imports_ref_json != self.history_imports_ref_json
            or binding.completion_output_id not in {None, self.completion_output_id}
        ):
            raise Conflict("original artifact custody source changed")

    def _control(self, session: Session) -> ArchiveWriteControlRecord:
        control = session.get(ArchiveWriteControlRecord, self.collection_id)
        if control is None or control.construction_json != self.construction_json:
            raise Conflict("original artifact custody construction changed")
        return control

    def _verify_published(self, session: Session) -> None:
        self._control(session)
        collection = session.get(CollectionRecord, self.collection_id)
        artifact = session.get(CollectionArtifactRecord, (self.collection_id, self.artifact_id))
        receipt = self._document()
        construction = json.loads(self.construction_json)
        immutable_fields = {
            "creation_identity": "creation_identity_sha256",
            "idempotency_key": "creation_idempotency_key",
            "delivery_context_id": "delivery_context_id",
            "archive_generation": "archive_generation",
            "archive_store": "creation_archive_store",
            "encryption_format": "encryption_format",
            "passphrase_id": "passphrase_id",
        }
        if (
            collection is None
            or not collection.is_published
            or collection.archive_root_sha256 is None
            or any(
                getattr(collection, field) != construction[key]
                for key, field in immutable_fields.items()
            )
            or artifact is None
            or artifact.bytes != int(receipt.bytes)
            or artifact.sha256 != receipt.sha256
            or artifact.custody_receipt_json != self.receipt_json
        ):
            raise Conflict("original artifact custody publication is unavailable")

    def apply(self, session: Session) -> None:
        _writer(session)
        upload = session.scalar(
            select(Upload)
            .where(Upload.collection_id == self.collection_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if upload is None:
            self._verify_published(session)
            return
        control = self._control(session)
        artifact = session.get(
            Artifact, (self.collection_id, self.artifact_id), populate_existing=True
        )
        binding = session.get(
            Binding, (self.collection_id, self.artifact_id), populate_existing=True
        )
        if artifact is None or binding is None:
            raise Conflict("original artifact custody source is unavailable")
        require_archive_write_control(session, upload)
        self._require_source(upload, artifact, binding)
        if artifact.custody_receipt_json is not None:
            if (
                artifact.custody_receipt_json != self.receipt_json
                or binding.completion_output_id != self.completion_output_id
            ):
                raise Conflict("original artifact custody acknowledgement differs")
            return
        # A committed original result can be received after fencing. A fresh
        # logical acknowledgement still needs the exact selected forward epoch.
        require_open_upload_writes(session, upload)
        if control.epoch != self.epoch or upload.state not in {
            "open",
            "closing",
            "uploading",
            "finalizing",
        }:
            raise Conflict("original artifact custody admission changed")
        artifact.custody_receipt_json = self.receipt_json
        binding.completion_output_id = self.completion_output_id
        upload.safe_release_artifact_count += 1
        upload.safe_release_artifact_bytes += artifact.bytes


class ArchiveArtifactCustodyReceiptFinalizer:
    """Retain one application-selected receipt through its commit acknowledgement.

    Dependency selection and provider work belong to their retained semantic
    owners. This finalizer performs one bounded catalog acknowledgement only.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        select_source: Callable[[], ArchiveArtifactCustodyReceiptSource | None],
    ) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._select_source = select_source
        self._source: ArchiveArtifactCustodyReceiptSource | None = None
        self._result: bool | None = None
        self._lock = Lock()
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)

    @property
    def has_pending(self) -> bool:
        return self._source is not None and self._result is None

    def advance(self) -> bool:
        with self._lock:
            self._terminal.require_forward()
            if self._result is not None:
                return self._result
            if self._source is None:
                self._source = self._select_source()
                if self._source is None:
                    self._result = False
                    return False
            if self._source.collection_id != self._collection_id:
                raise Conflict("artifact custody selection has another construction")
            with session_scope(self._factory) as session:
                self._source.apply(session)
            self._result = True
            return True

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            return self._terminal.drain_original(operation_id, None)
        finally:
            self._lock.release()

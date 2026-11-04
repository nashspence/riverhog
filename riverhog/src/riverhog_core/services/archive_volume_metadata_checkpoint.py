"""Own exact volume metadata receiving facts and ordered publication progress."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from riverhog_archive_contracts import (
    CollectionArchiveTerminalDocument,
    CollectionArchiveVolumeDocument,
    parse_archive_sequence,
    update_archive_sequence_commitment,
)
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    ImmutableObjectReceipt as AdapterImmutableObjectReceipt,
)
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.archive_manifest import (
    build_collection_archive_terminal_document,
    build_collection_archive_volume_document,
)
from riverhog_core.archive_root import (
    ArchiveVolumeMetadataPublication,
    SealedArchiveVolumeMetadata,
)
from riverhog_core.archive_volume_receipts import parse_sealed_pack, parse_sealed_raw
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.domain.archive import SealedPackVolume, SealedRawVolume
from riverhog_core.immutable_archive_identity import require_immutable_archive_receipt
from riverhog_core.pack_volume import parse_pack_volume_plan
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _parent_identity(upload: CollectionUploadRecord) -> dict[str, object]:
    return {
        "collection_id": upload.collection_id,
        "archive_store": upload.archive_store,
        "archive_incarnation_id": upload.archive_incarnation_id,
        "archive_generation": upload.archive_generation,
        "archive_storage_prefix": upload.archive_storage_prefix,
        "archive_tree_sha256": upload.archive_tree_sha256,
        "planner_next_sequence": upload.planner_next_sequence,
    }


def _volume_identity(record: CollectionArchiveObjectUploadRecord | None) -> object:
    if record is None:
        return None
    return {
        "object_id": record.object_id,
        "sequence": record.sequence,
        "kind": record.kind,
        "state": record.state,
        "plan_json": record.plan_json,
        "sealed_receipt_json": record.sealed_receipt_json,
    }


def _receipt_json(receipt: SealedArchiveVolumeMetadata) -> str:
    return _json(
        {
            "sequence": receipt.sequence,
            "object_path": receipt.object_path,
            "relative_path": receipt.relative_path,
            "revision": receipt.revision,
            "plaintext_bytes": receipt.plaintext_bytes,
            "plaintext_sha256": receipt.plaintext_sha256,
            "stored_bytes": receipt.stored_bytes,
            "stored_sha256": receipt.stored_sha256,
            "completed_at": receipt.completed_at,
        }
    )


@dataclass(frozen=True, slots=True)
class ArchiveVolumeMetadataSource:
    """Immutable source facts; current admission and durable receiving stay separate."""

    collection_id: int
    object_id: str | None
    identity_json: str = field(repr=False)
    publication: ArchiveVolumeMetadataPublication = field(repr=False)

    @classmethod
    def capture(
        cls,
        *,
        upload: CollectionUploadRecord,
        record: CollectionArchiveObjectUploadRecord | None,
        document: CollectionArchiveVolumeDocument | CollectionArchiveTerminalDocument,
        total_volumes: int,
    ) -> ArchiveVolumeMetadataSource:
        publication = ArchiveVolumeMetadataPublication.from_document(
            archive_storage_prefix=upload.archive_storage_prefix, document=document
        )
        terminal = isinstance(document, CollectionArchiveTerminalDocument)
        if (
            upload.archive_tree_sha256 != document.artifact_set_sha256
            or upload.archive_generation != document.archive_generation
            or upload.planner_next_sequence != total_volumes
            or (terminal and (record is not None or publication.sequence != total_volumes))
            or (
                not terminal
                and (
                    record is None
                    or record.collection_id != upload.collection_id
                    or record.sequence != publication.sequence
                    or record.sequence >= total_volumes
                    or record.state != "sealed"
                    or record.sealed_receipt_json is None
                )
            )
        ):
            raise Conflict("archive volume metadata source differs from its construction")
        return cls(
            upload.collection_id,
            record.object_id if record is not None else None,
            _json({"construction": _parent_identity(upload), "volume": _volume_identity(record)}),
            publication,
        )

    @classmethod
    def load(
        cls, session: Session, collection_id: int, sequence: int
    ) -> ArchiveVolumeMetadataSource:
        """Rebuild one original document from indexed, source-owned facts."""
        upload = session.scalar(
            select(CollectionUploadRecord)
            .where(CollectionUploadRecord.collection_id == collection_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if upload is None:
            raise NotFound("archive volume metadata receiving source is unavailable")
        if upload.archive_tree_sha256 is None:
            raise Conflict("archive volume metadata tree identity is unavailable")
        document: CollectionArchiveVolumeDocument | CollectionArchiveTerminalDocument
        total = upload.planner_next_sequence
        if sequence == total:
            record = None
            document = build_collection_archive_terminal_document(
                archive_generation=upload.archive_generation,
                artifact_set_sha256=upload.archive_tree_sha256,
                sequence=sequence,
            )
        elif 0 <= sequence < total:
            record = session.scalar(
                select(CollectionArchiveObjectUploadRecord)
                .where(
                    CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                    CollectionArchiveObjectUploadRecord.sequence == sequence,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if record is None or record.state != "sealed" or record.sealed_receipt_json is None:
                raise Conflict("archive volume metadata original source is not sealed")
            receipt: SealedPackVolume | SealedRawVolume
            if record.kind == "pack":
                plan = parse_pack_volume_plan(record.plan_json)
                receipt = parse_sealed_pack(record.sealed_receipt_json)
            elif record.kind == "segment":
                plan = None
                receipt = parse_sealed_raw(record.sealed_receipt_json)
            else:
                raise Conflict("archive volume metadata original source kind is invalid")
            document = build_collection_archive_volume_document(
                archive_generation=upload.archive_generation,
                artifact_set_sha256=upload.archive_tree_sha256,
                plan=plan,
                receipt=receipt,
            )
        else:
            raise Conflict("archive volume metadata original sequence is outside its construction")
        return cls.capture(upload=upload, record=record, document=document, total_volumes=total)

    @classmethod
    def from_prepared(
        cls, session: Session, prepared: PreparedImmutableArchiveObject
    ) -> ArchiveVolumeMetadataSource:
        """Route original metadata facts without acquiring fresh forward authority."""
        request = prepared.operation.request
        try:
            sequence = parse_archive_sequence(
                request.required_identity_assertions.get("riverhog-volume-sequence")
            )
        except ValueError as exc:
            raise Conflict("archive volume metadata original sequence is invalid") from exc
        source = cls.load(session, prepared.collection_id, sequence)
        source._require_prepared(prepared)
        return source

    def _require_prepared(self, prepared: PreparedImmutableArchiveObject) -> None:
        if (
            prepared.collection_id != self.collection_id
            or prepared.object_id
            != ImmutableArchivePreparation.object_id(self.publication.object_path)
            or prepared._value()["target_incarnation"]
            != json.loads(self.identity_json)["construction"]["archive_incarnation_id"]
        ):
            raise Conflict("archive volume metadata preparation belongs to another construction")
        try:
            self.publication.require_request(prepared.operation.request)
        except ValueError as exc:
            raise Conflict(str(exc)) from exc

    def _prepared_receipt(
        self, prepared: PreparedImmutableArchiveObject, receipt: AdapterImmutableObjectReceipt
    ) -> SealedArchiveVolumeMetadata:
        self._require_prepared(prepared)
        # Compatible observation may name different randomized ciphertext. The
        # admission owner, rather than this projection, proves execution closure.
        require_immutable_archive_receipt(prepared.operation.request, receipt)
        return self.publication.sealed_receipt(
            ImmutableObjectReceipt(
                object_path=receipt.object_path,
                revision=receipt.revision,
                entity_token=receipt.entity_token,
                stored_bytes=receipt.stored_bytes,
                stored_sha256=receipt.stored_sha256,
                completed_at=receipt.completed_at,
            )
        )

    def apply_prepared_receipt(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
    ) -> None:
        self.apply_receipt(session, self._prepared_receipt(prepared, receipt))

    def verify_prepared_receipt(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
    ) -> None:
        self.verify_receipt(session, self._prepared_receipt(prepared, receipt))

    @property
    def document(self) -> CollectionArchiveVolumeDocument | CollectionArchiveTerminalDocument:
        if self.object_id is None:
            return CollectionArchiveTerminalDocument.from_json_bytes(self.publication.plaintext)
        return CollectionArchiveVolumeDocument.from_json_bytes(self.publication.plaintext)

    def _parent(self, session: Session) -> CollectionUploadRecord:
        upload = session.scalar(
            select(CollectionUploadRecord)
            .where(CollectionUploadRecord.collection_id == self.collection_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if upload is None:
            raise NotFound("archive volume metadata receiving source is unavailable")
        if _parent_identity(upload) != json.loads(self.identity_json)["construction"]:
            raise Conflict("archive volume metadata original construction changed")
        return upload

    def _source(
        self, session: Session
    ) -> tuple[CollectionUploadRecord, CollectionArchiveObjectUploadRecord | None]:
        upload = self._parent(session)
        record = (
            session.get(
                CollectionArchiveObjectUploadRecord,
                (self.collection_id, self.object_id),
                with_for_update=True,
                populate_existing=True,
            )
            if self.object_id is not None
            else None
        )
        if _volume_identity(record) != json.loads(self.identity_json)["volume"]:
            raise Conflict("archive volume metadata original sealed source changed")
        return upload, record

    def guard(self, session: Session) -> None:
        """Require the already accepted finalizer claim without renewing producer custody."""
        upload = self._parent(session)
        if (
            upload.state != "finalizing"
            or upload.archive_phase != "finalizing"
            or upload.archive_volume_next_sequence != self.publication.sequence
            or (self.object_id is None and upload.archive_terminal_receipt_json is not None)
        ):
            raise Conflict("archive volume metadata finalizer claim is not current")

    def validate_metadata(
        self, session: Session, upload: CollectionUploadRecord, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id:
            raise Conflict("archive volume metadata belongs to another construction")
        self.guard(session)
        self._source(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict(str(exc)) from exc

    def _projection(self, receipt: SealedArchiveVolumeMetadata) -> str:
        try:
            expected = self.publication.sealed_receipt(
                ImmutableObjectReceipt(
                    object_path=receipt.object_path,
                    revision=receipt.revision,
                    entity_token=None,
                    stored_bytes=receipt.stored_bytes,
                    stored_sha256=receipt.stored_sha256,
                    completed_at=receipt.completed_at,
                )
            )
        except RuntimeError as exc:
            raise Conflict("archive volume metadata receipt differs from its source") from exc
        if receipt != expected:
            raise Conflict("archive volume metadata receipt differs from its source")
        return _receipt_json(receipt)

    def apply_receipt(self, session: Session, receipt: SealedArchiveVolumeMetadata) -> None:
        self._receive(session, receipt, apply=True)

    def verify_receipt(self, session: Session, receipt: SealedArchiveVolumeMetadata) -> None:
        self._receive(session, receipt, apply=False)

    def _receive(
        self, session: Session, receipt: SealedArchiveVolumeMetadata, *, apply: bool
    ) -> None:
        encoded = self._projection(receipt)
        upload, record = self._source(session)
        sequence = self.publication.sequence
        if record is None:
            existing = upload.archive_terminal_receipt_json
            if existing is not None:
                if (
                    existing != encoded
                    or upload.archive_ordered_volume_sha256 is None
                    or upload.archive_volume_hash_state is not None
                    or upload.archive_volume_next_sequence != sequence
                ):
                    raise Conflict("archive terminal metadata receiving proof changed")
                return
        else:
            existing = record.metadata_receipt_json
            if existing is not None:
                if existing != encoded or upload.archive_volume_next_sequence <= sequence:
                    raise Conflict("archive volume metadata receiving proof changed")
                return
        if not apply:
            raise Conflict("archive volume metadata receipt is not durably received")
        if upload.archive_volume_next_sequence != sequence:
            raise Conflict("archive volume metadata receiving sequence changed")
        if sequence > 0 and upload.archive_volume_hash_state is None:
            raise Conflict("archive volume metadata ordered commitment is missing")
        digest = (
            CheckpointSHA256.from_state(upload.archive_volume_hash_state)
            if upload.archive_volume_hash_state is not None
            else CheckpointSHA256()
        )
        update_archive_sequence_commitment(digest, self.document)
        if record is None:
            upload.archive_terminal_receipt_json = encoded
            upload.archive_ordered_volume_sha256 = digest.hexdigest()
            upload.archive_volume_hash_state = None
        else:
            record.metadata_receipt_json = encoded
            upload.archive_volume_next_sequence = sequence + 1
            upload.archive_volume_hash_state = digest.export_state()
        session.flush()

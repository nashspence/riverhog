"""Source-owned original root and recovery receiving checkpoints."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from riverhog_archive_contracts import CollectionEncryptionBinding
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    ImmutableObjectReceipt as AdapterImmutableObjectReceipt,
)
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.archive_manifest import build_collection_archive_root_manifest
from riverhog_core.archive_recovery_descriptor import (
    ArchiveRecoveryDescriptorPublication,
    SealedRecoveryDescriptor,
)
from riverhog_core.archive_root import ArchiveRootPublication, SealedArchiveRoot
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.collection_construction import upload_construction_fields
from riverhog_core.domain.archive import SealedProvenanceObject
from riverhog_core.immutable_archive_identity import require_immutable_archive_receipt
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _identity(upload: CollectionUploadRecord) -> dict[str, object]:
    return {
        **upload_construction_fields(upload),
        "artifact_count": upload.artifact_count,
        "artifact_bytes": upload.artifact_bytes,
        "archive_tree_sha256": upload.archive_tree_sha256,
        "archive_ordered_volume_sha256": upload.archive_ordered_volume_sha256,
        "archive_terminal_receipt_json": upload.archive_terminal_receipt_json,
        "provenance_identity": upload.provenance_identity,
        "provenance_archive_root_receipt_json": upload.provenance_archive_root_receipt_json,
    }


def _upload(session: Session, collection_id: int) -> CollectionUploadRecord:
    upload = session.scalar(
        select(CollectionUploadRecord)
        .where(CollectionUploadRecord.collection_id == collection_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if upload is None:
        raise NotFound("archive authority receiving source is unavailable")
    return upload


def _claimed(session: Session, upload: CollectionUploadRecord) -> None:
    if (
        upload.state != "finalizing"
        or upload.archive_phase != "finalizing"
        or upload.final_authority_json is not None
    ):
        raise Conflict("archive authority finalizer claim is not current")
    ArchiveWriteAdmission.require_no_unresolved_calls(session, upload.collection_id)


def _root_value(receipt: SealedArchiveRoot) -> dict[str, object]:
    return {
        "object_path": receipt.object_path,
        "relative_path": receipt.relative_path,
        "revision": receipt.revision,
        "plaintext_bytes": receipt.plaintext_bytes,
        "plaintext_sha256": receipt.plaintext_sha256,
        "stored_bytes": receipt.stored_bytes,
        "stored_sha256": receipt.stored_sha256,
        "artifact_set_sha256": receipt.artifact_set_sha256,
        "artifact_count": receipt.artifact_count,
        "artifact_bytes": receipt.artifact_bytes,
        "completed_at": receipt.completed_at,
    }


def _object_receipt(receipt: AdapterImmutableObjectReceipt) -> ImmutableObjectReceipt:
    return ImmutableObjectReceipt(
        object_path=receipt.object_path,
        revision=receipt.revision,
        entity_token=receipt.entity_token,
        stored_bytes=receipt.stored_bytes,
        stored_sha256=receipt.stored_sha256,
        completed_at=receipt.completed_at,
    )


def _require_prepared(
    collection_id: int,
    publication: ArchiveRootPublication | ArchiveRecoveryDescriptorPublication,
    identity_json: str,
    prepared: PreparedImmutableArchiveObject,
) -> None:
    if (
        prepared.collection_id != collection_id
        or prepared.object_id != ImmutableArchivePreparation.object_id(publication.object_path)
        or prepared._value()["target_incarnation"]
        != json.loads(identity_json)["archive_incarnation_id"]
    ):
        raise Conflict("archive authority preparation belongs to another construction")
    try:
        publication.require_request(prepared.operation.request)
    except ValueError as exc:
        raise Conflict(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class ArchiveRootSource:
    """Original canonical root inputs, independently received before recovery."""

    collection_id: int
    identity_json: str = field(repr=False)
    publication: ArchiveRootPublication = field(repr=False)
    forward_guard: Guard | None = field(default=None, repr=False, compare=False)

    @classmethod
    def capture(
        cls, upload: CollectionUploadRecord, *, forward_guard: Guard | None = None
    ) -> ArchiveRootSource:
        if (
            upload.archive_tree_sha256 is None
            or upload.archive_ordered_volume_sha256 is None
            or upload.archive_terminal_receipt_json is None
            or upload.provenance_archive_root_receipt_json is None
        ):
            raise Conflict("archive authority source checkpoints are incomplete")
        value = json.loads(upload.provenance_archive_root_receipt_json)
        if value["identity"] != upload.provenance_identity:
            raise Conflict("archive authority provenance identity changed")
        root = SealedProvenanceObject(**value["root"])
        if root.plaintext_sha256 != upload.provenance_identity:
            raise Conflict("archive authority provenance root identity changed")
        manifest = build_collection_archive_root_manifest(
            archive_generation=upload.archive_generation,
            artifact_set={
                "count": int(upload.artifact_count),
                "bytes": int(upload.artifact_bytes),
                "sha256": upload.archive_tree_sha256,
            },
            ordered_volume_sha256=upload.archive_ordered_volume_sha256,
            provenance_identity=upload.provenance_identity,
            provenance_objects=(root,),
        )
        return cls(
            upload.collection_id,
            _json(_identity(upload)),
            ArchiveRootPublication(upload.archive_storage_prefix, manifest),
            forward_guard,
        )

    @classmethod
    def load(
        cls, session: Session, collection_id: int, *, forward_guard: Guard | None = None
    ) -> ArchiveRootSource:
        return cls.capture(_upload(session, collection_id), forward_guard=forward_guard)

    def _parent(self, session: Session) -> CollectionUploadRecord:
        upload = _upload(session, self.collection_id)
        if _identity(upload) != json.loads(self.identity_json):
            raise Conflict("archive authority original construction changed")
        return upload

    @property
    def archive_store(self) -> str:
        return str(json.loads(self.identity_json)["archive_store"])

    @property
    def archive_incarnation_id(self) -> str:
        return str(json.loads(self.identity_json)["archive_incarnation_id"])

    @property
    def passphrase_id(self) -> str:
        return str(json.loads(self.identity_json)["passphrase_id"])

    def guard(self, session: Session) -> None:
        if self.forward_guard is not None:
            self.forward_guard(session)
        upload = self._parent(session)
        _claimed(session, upload)
        if upload.archive_root_receipt_json is not None:
            raise Conflict("archive root is already durably received")

    def validate_metadata(
        self, session: Session, upload: CollectionUploadRecord, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id:
            raise Conflict("archive authority belongs to another construction")
        self.guard(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict(str(exc)) from exc

    def _projection(self, receipt: SealedArchiveRoot) -> str:
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
            raise Conflict("archive root receipt differs from its source") from exc
        if receipt != expected:
            raise Conflict("archive root receipt differs from its source")
        return _json(_root_value(receipt))

    def _receive(self, session: Session, receipt: SealedArchiveRoot, *, apply: bool) -> None:
        encoded = self._projection(receipt)
        upload = self._parent(session)
        existing = upload.archive_root_receipt_json
        if existing is not None:
            if existing != encoded:
                raise Conflict("archive root receiving proof changed")
            return
        if not apply:
            raise Conflict("archive root receipt is not durably received")
        upload.archive_root_receipt_json = encoded
        session.flush()

    def apply_receipt(self, session: Session, receipt: SealedArchiveRoot) -> None:
        self._receive(session, receipt, apply=True)

    def verify_receipt(self, session: Session, receipt: SealedArchiveRoot) -> None:
        self._receive(session, receipt, apply=False)

    def received(self, session: Session) -> SealedArchiveRoot | None:
        upload = self._parent(session)
        if upload.archive_root_receipt_json is None:
            return None
        value = json.loads(upload.archive_root_receipt_json)
        receipt = self.publication.sealed_receipt(
            ImmutableObjectReceipt(
                object_path=value["object_path"],
                revision=value["revision"],
                entity_token=None,
                stored_bytes=value["stored_bytes"],
                stored_sha256=value["stored_sha256"],
                completed_at=value["completed_at"],
            )
        )
        if self._projection(receipt) != upload.archive_root_receipt_json:
            raise Conflict("archive root receiving proof changed")
        return receipt

    @classmethod
    def from_prepared(
        cls, session: Session, prepared: PreparedImmutableArchiveObject
    ) -> ArchiveRootSource:
        source = cls.load(session, prepared.collection_id)
        _require_prepared(source.collection_id, source.publication, source.identity_json, prepared)
        return source

    def apply_prepared_receipt(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=True)

    def verify_prepared_receipt(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=False)

    def _receive_prepared(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
        *,
        apply: bool,
    ) -> None:
        _require_prepared(self.collection_id, self.publication, self.identity_json, prepared)
        require_immutable_archive_receipt(prepared.operation.request, receipt)
        self._receive(
            session, self.publication.sealed_receipt(_object_receipt(receipt)), apply=apply
        )


@dataclass(frozen=True, slots=True)
class ArchiveRecoverySource:
    """Exact stored root ciphertext is an input, never a newly encrypted substitute."""

    root_source: ArchiveRootSource
    root_receipt_json: str = field(repr=False)
    publication: ArchiveRecoveryDescriptorPublication = field(repr=False)

    @property
    def collection_id(self) -> int:
        return self.root_source.collection_id

    @classmethod
    def load(
        cls, session: Session, collection_id: int, *, forward_guard: Guard | None = None
    ) -> ArchiveRecoverySource:
        source = ArchiveRootSource.load(session, collection_id, forward_guard=forward_guard)
        root = source.received(session)
        if root is None:
            raise Conflict("archive recovery original root is not durably received")
        identity = json.loads(source.identity_json)
        return cls(
            source,
            _json(_root_value(root)),
            ArchiveRecoveryDescriptorPublication.from_root(
                archive_storage_prefix=source.publication.archive_storage_prefix,
                root=root,
                encryption=CollectionEncryptionBinding(
                    format=str(identity["encryption_format"]),
                    passphrase_id=str(identity["passphrase_id"]),
                ),
            ),
        )

    def _parent(self, session: Session) -> CollectionUploadRecord:
        upload = self.root_source._parent(session)
        if upload.archive_root_receipt_json != self.root_receipt_json:
            raise Conflict("archive recovery original stored root changed")
        return upload

    def guard(self, session: Session) -> None:
        if self.root_source.forward_guard is not None:
            self.root_source.forward_guard(session)
        _claimed(session, self._parent(session))

    def validate_metadata(
        self, session: Session, upload: CollectionUploadRecord, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id:
            raise Conflict("archive authority belongs to another construction")
        self.guard(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict(str(exc)) from exc

    def _receive(self, session: Session, receipt: SealedRecoveryDescriptor, *, apply: bool) -> None:
        try:
            expected = self.publication.sealed_receipt(
                ImmutableObjectReceipt(
                    object_path=receipt.object_path,
                    revision=receipt.revision,
                    entity_token=None,
                    stored_bytes=receipt.bytes,
                    stored_sha256=receipt.sha256,
                    completed_at=receipt.completed_at,
                )
            )
        except RuntimeError as exc:
            raise Conflict("archive recovery receipt differs from its source") from exc
        if expected != receipt:
            raise Conflict("archive recovery receipt differs from its source")
        upload = self._parent(session)
        encoded = _json(
            {
                "root": json.loads(self.root_receipt_json),
                "recovery": {
                    "object_path": receipt.object_path,
                    "relative_path": receipt.relative_path,
                    "revision": receipt.revision,
                    "bytes": receipt.bytes,
                    "sha256": receipt.sha256,
                    "completed_at": receipt.completed_at,
                },
            }
        )
        if upload.final_authority_json is not None:
            if upload.final_authority_json != encoded:
                raise Conflict("archive authority receiving proof changed")
            return
        if not apply:
            raise Conflict("archive recovery receipt is not durably received")
        upload.final_authority_json = encoded
        session.flush()

    def apply_receipt(self, session: Session, receipt: SealedRecoveryDescriptor) -> None:
        self._receive(session, receipt, apply=True)

    def verify_receipt(self, session: Session, receipt: SealedRecoveryDescriptor) -> None:
        self._receive(session, receipt, apply=False)

    def received(self, session: Session) -> SealedRecoveryDescriptor | None:
        upload = self._parent(session)
        if upload.final_authority_json is None:
            return None
        try:
            value = json.loads(upload.final_authority_json)["recovery"]
            receipt = self.publication.sealed_receipt(
                ImmutableObjectReceipt(
                    object_path=value["object_path"],
                    revision=value["revision"],
                    entity_token=None,
                    stored_bytes=value["bytes"],
                    stored_sha256=value["sha256"],
                    completed_at=value["completed_at"],
                )
            )
        except (KeyError, TypeError, ValueError, RuntimeError) as exc:
            raise Conflict("archive authority receiving proof changed") from exc
        self.verify_receipt(session, receipt)
        return receipt

    @classmethod
    def from_prepared(
        cls, session: Session, prepared: PreparedImmutableArchiveObject
    ) -> ArchiveRecoverySource:
        source = cls.load(session, prepared.collection_id)
        _require_prepared(
            source.collection_id,
            source.publication,
            source.root_source.identity_json,
            prepared,
        )
        return source

    def apply_prepared_receipt(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=True)

    def verify_prepared_receipt(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=False)

    def _receive_prepared(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
        *,
        apply: bool,
    ) -> None:
        _require_prepared(
            self.collection_id, self.publication, self.root_source.identity_json, prepared
        )
        require_immutable_archive_receipt(prepared.operation.request, receipt)
        self._receive(
            session, self.publication.sealed_receipt(_object_receipt(receipt)), apply=apply
        )

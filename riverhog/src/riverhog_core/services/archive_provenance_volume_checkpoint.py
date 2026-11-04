"""Original corpus volume staging and independent physical receipt receiving."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal

from riverhog_archive_contracts import (
    PROVENANCE_BINDING_PAGE_MEMBERS_MAX,
    PROVENANCE_BINDINGS_FORMAT,
    PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
    ProvenanceVolumeDocument,
    parse_archive_sequence,
    validate_member_history_binding_page,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import ImmutableObjectReceipt as AdapterReceipt
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core import provenance_volume_source as volumes
from riverhog_core.archive_provenance import ArchiveProvenancePublication
from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_models import CollectionUploadMemberHistoryRecord as History
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceVolumePublicationRecord as Volume,
)
from riverhog_core.collection_construction import upload_construction
from riverhog_core.domain.archive import SealedProvenanceObject
from riverhog_core.immutable_archive_identity import require_immutable_archive_receipt
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.services.archive_provenance_journal_checkpoint import (
    ArchiveProvenanceJournalSource,
    ProvenanceJournalRange,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, _writer
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)

Role = Literal["bindings", "metadata"]


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _upload(session: Session, collection_id: int) -> Upload:
    upload = session.scalar(
        select(Upload)
        .where(Upload.collection_id == collection_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if upload is None:
        raise NotFound("provenance volume receiving parent is unavailable")
    return upload


def _claim(upload: Upload, sequence: int) -> None:
    if (
        upload.state != "finalizing"
        or upload.archive_phase != "finalizing"
        or not upload.provenance_closure_validated
        or upload.archive_tree_sha256 is None
        or upload.provenance_archive_next_sequence != sequence
        or upload.provenance_archive_terminal_receipt_json is not None
    ):
        raise Conflict("provenance volume finalizer claim is not current")


def _bindings(
    session: Session, collection_id: int, after: str | None
) -> tuple[bytes, list[History]]:
    statement = select(History).where(History.collection_id == collection_id)
    if after is not None:
        statement = statement.where(History.artifact_id > after)
    rows = list(
        session.scalars(
            statement.order_by(History.artifact_id).limit(PROVENANCE_BINDING_PAGE_MEMBERS_MAX)
        )
    )
    bindings = [volumes.member_history_binding_row(row) for row in rows]
    if any(
        (row.artifact_id, row.history_sha256, row.history_bytes)
        != (binding.artifact_id, binding.history_sha256, binding.history_bytes)
        for row, binding in zip(rows, bindings, strict=True)
    ):
        raise Conflict("provenance volume original binding row changed")
    values = [binding.to_mapping() for binding in bindings]
    validate_member_history_binding_page(values, max_members=PROVENANCE_BINDING_PAGE_MEMBERS_MAX)
    return canonical_json_bytes({"format": PROVENANCE_BINDINGS_FORMAT, "bindings": values}), rows


def _context(upload: Upload) -> dict[str, Any]:
    return {
        "construction_sha256": _sha(upload_construction(upload)),
        "archive_incarnation_id": upload.archive_incarnation_id,
        "artifact_set_sha256": upload.archive_tree_sha256,
        "artifact_count": upload.artifact_count,
        "after_artifact_id": upload.provenance_archive_after_artifact_id,
        "next_artifact_order": upload.provenance_archive_next_artifact_order,
        "last_journal_id": upload.provenance_archive_last_journal_id,
        "current_journal_id": upload.provenance_archive_current_journal_id,
        "current_journal_offset": upload.provenance_archive_current_journal_offset,
    }


@dataclass(frozen=True, slots=True)
class ArchiveProvenanceVolumeSource:
    """Each role owns one physical object; receipt data grants no closure or next step."""

    collection_id: int
    sequence: int
    role: Role
    source_json: str = field(repr=False)
    document_json: str = field(repr=False)
    publication: ArchiveProvenancePublication = field(repr=False)

    @classmethod
    def stage_next(
        cls, session: Session, collection_id: int, *, grants: ArchiveWriteGrants
    ) -> ArchiveProvenanceVolumeSource | None:
        _writer(session)
        upload = _upload(session, collection_id)
        sequence = upload.provenance_archive_next_sequence
        _claim(upload, sequence)
        assert upload.archive_tree_sha256 is not None
        ArchiveWriteAdmission.require_no_unresolved_calls(session, collection_id)
        control = ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        if control.gate != "open":
            raise Conflict("provenance volume admission is closed")
        existing = session.get(Volume, (collection_id, sequence), with_for_update=True)
        if existing is None:
            if upload.provenance_archive_next_artifact_order < upload.artifact_count:
                payload, rows = _bindings(
                    session, collection_id, upload.provenance_archive_after_artifact_id
                )
                if not rows or len(rows) > (
                    upload.artifact_count - upload.provenance_archive_next_artifact_order
                ):
                    raise Conflict("provenance volume bindings do not cover the selected page")
                document = volumes.provenance_volume_document(
                    archive_generation=upload.archive_generation,
                    artifact_set_sha256=upload.archive_tree_sha256,
                    sequence=sequence,
                    payload=payload,
                    first_artifact_id=rows[0].artifact_id,
                    last_artifact_id=rows[-1].artifact_id,
                    binding_count=len(rows),
                )
            else:
                if upload.provenance_archive_next_artifact_order != upload.artifact_count:
                    raise Conflict("provenance volume binding cursor exceeds its construction")
                journal = volumes.next_provenance_publication_journal(session, upload)
                if journal is None:
                    return None
                selected, payload = ProvenanceJournalRange.capture(
                    session,
                    collection_id,
                    journal.journal_id,
                    offset=upload.provenance_archive_current_journal_offset,
                    size=min(
                        PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
                        journal.bytes - upload.provenance_archive_current_journal_offset,
                    ),
                )
                document = volumes.provenance_volume_document(
                    archive_generation=upload.archive_generation,
                    artifact_set_sha256=upload.archive_tree_sha256,
                    sequence=sequence,
                    payload=payload,
                    journal=journal,
                    journal_offset=selected.offset,
                )
            source_json = _json(_context(upload))
            session.add(
                Volume(
                    collection_id=collection_id,
                    sequence=sequence,
                    document_json=document.to_json_bytes().decode(),
                    source_json=source_json,
                    source_sha256=_sha(source_json),
                    binding_payload_sha256=document.payload.sha256
                    if document.payload.kind == "bindings"
                    else None,
                )
            )
            session.flush()
        source = cls.load(session, collection_id, sequence, role="metadata")
        if source.source_json != _json(_context(upload)):
            raise Conflict("provenance volume original cursor changed")
        source.write_grants(grants)
        return source

    @classmethod
    def load(
        cls, session: Session, collection_id: int, sequence: int, *, role: Role
    ) -> ArchiveProvenanceVolumeSource:
        upload = _upload(session, collection_id)
        row = session.get(
            Volume, (collection_id, sequence), with_for_update=True, populate_existing=True
        )
        if row is None:
            raise NotFound("provenance volume original source is unavailable")
        context: dict[str, Any] = json.loads(row.source_json)
        if (
            set(context) != set(_context(upload))
            or _json(context) != row.source_json
            or row.source_sha256 != _sha(row.source_json)
            or context["construction_sha256"] != _sha(upload_construction(upload))
            or context["artifact_set_sha256"] != upload.archive_tree_sha256
            or context["artifact_count"] != upload.artifact_count
            or context["archive_incarnation_id"] != upload.archive_incarnation_id
        ):
            raise Conflict("provenance volume original construction changed")
        try:
            document = ProvenanceVolumeDocument.from_json_bytes(row.document_json.encode())
        except ValueError as exc:
            raise Conflict("provenance volume original document changed") from exc
        if (
            document.sequence != sequence
            or document.archive_generation != upload.archive_generation
            or document.artifact_set_sha256 != upload.archive_tree_sha256
            or document.to_json_bytes().decode() != row.document_json
        ):
            raise Conflict("provenance volume original document changed")
        if document.payload.kind == "bindings":
            payload, rows = _bindings(session, collection_id, context["after_artifact_id"])
            if (
                not rows
                or document.first_artifact_id != rows[0].artifact_id
                or document.last_artifact_id != rows[-1].artifact_id
                or document.binding_count != len(rows)
                or row.binding_payload_sha256 != document.payload.sha256
                or context["next_artifact_order"] + len(rows) > context["artifact_count"]
            ):
                raise Conflict("provenance volume original bindings changed")
        else:
            if row.binding_payload_sha256 is not None:
                raise Conflict("provenance journal volume has a binding payload locator")
            assert document.journal_id is not None and document.journal_offset is not None
            selected, payload = ProvenanceJournalRange.capture(
                session,
                collection_id,
                document.journal_id,
                offset=document.journal_offset,
                size=document.payload.bytes,
            )
            if (
                selected.journal_bytes != document.journal_bytes
                or selected.journal_sha256 != document.journal_sha256
                or context["next_artifact_order"] != context["artifact_count"]
                or selected.offset != context["current_journal_offset"]
                or context["current_journal_id"] not in {None, document.journal_id}
            ):
                raise Conflict("provenance volume original journal range changed")
        try:
            payload_publication = ArchiveProvenancePublication.from_volume_payload(
                archive_storage_prefix=upload.archive_storage_prefix,
                document=document,
                payload=payload,
            )
        except ValueError as exc:
            raise Conflict("provenance volume original payload changed") from exc
        if role == "bindings":
            if document.payload.kind != "bindings":
                raise Conflict("journal volume payload retains its original journal owner")
            publication = payload_publication
        elif role == "metadata":
            publication = ArchiveProvenancePublication.from_volume_metadata(
                archive_storage_prefix=upload.archive_storage_prefix, document=document
            )
        else:
            raise Conflict("unsupported provenance volume physical source role")
        return cls(collection_id, sequence, role, row.source_json, row.document_json, publication)

    @property
    def document(self) -> ProvenanceVolumeDocument:
        return ProvenanceVolumeDocument.from_json_bytes(self.document_json.encode())

    @property
    def reserved_source_bytes(self) -> int:
        document = self.document
        return (
            len(self.source_json.encode())
            + len(self.document_json.encode())
            + 64
            + len(str(self.collection_id).encode())
            + 64
            + (64 if document.payload.kind == "bindings" else 0)
        )

    def write_grants(self, grants: ArchiveWriteGrants) -> ArchiveWriteGrants:
        remaining = grants.metadata_preparation_bytes - self.reserved_source_bytes
        if remaining < 1:
            raise Conflict("provenance volume source exceeds its preparation grant")
        return replace(grants, metadata_preparation_bytes=remaining)

    def _source(self, session: Session) -> tuple[Upload, Volume]:
        current = self.load(session, self.collection_id, self.sequence, role=self.role)
        if current != self:
            raise Conflict("provenance volume original source changed")
        upload = _upload(session, self.collection_id)
        row = session.get(Volume, (self.collection_id, self.sequence))
        assert row is not None
        return upload, row

    def _forward(self, session: Session) -> Volume:
        upload, row = self._source(session)
        _claim(upload, self.sequence)
        if self.source_json != _json(_context(upload)):
            raise Conflict("provenance volume original cursor changed")
        ArchiveWriteAdmission.require_no_unresolved_calls(session, self.collection_id)
        return row

    def guard_journal_payload(self, session: Session) -> None:
        row = self._forward(session)
        if self.document.payload.kind != "journal" or row.payload_receipt_json is not None:
            raise Conflict("provenance volume journal payload cannot start writes")

    def stage_journal_payload(
        self, session: Session, *, grants: ArchiveWriteGrants
    ) -> ArchiveProvenanceJournalSource:
        document = self.document
        if document.payload.kind != "journal":
            raise Conflict("binding payload has its own original physical receiver")
        assert document.journal_id is not None and document.journal_offset is not None
        return ArchiveProvenanceJournalSource.stage(
            session,
            self.collection_id,
            journal_id=document.journal_id,
            offset=document.journal_offset,
            size=document.payload.bytes,
            grants=self.write_grants(grants),
            forward_guard=self.guard_journal_payload,
        )

    def guard(self, session: Session) -> None:
        row = self._forward(session)
        if self.role == "metadata":
            self._require_payload(session, row)
        if self._encoded_receipt(row) is not None:
            raise Conflict("provenance volume original receipt is already received")

    def validate_metadata(
        self, session: Session, upload: Upload, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id:
            raise Conflict("provenance volume belongs to another construction")
        self._source(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict("provenance volume request differs from its source") from exc

    def _require_prepared(self, prepared: PreparedImmutableArchiveObject) -> None:
        value = prepared._value()
        if (
            prepared.collection_id != self.collection_id
            or prepared.object_id
            != ImmutableArchivePreparation.object_id(self.publication.object_path)
            or value["construction_sha256"] != json.loads(self.source_json)["construction_sha256"]
            or value["target_incarnation"] != json.loads(self.source_json)["archive_incarnation_id"]
        ):
            raise Conflict("provenance volume preparation belongs to another original source")
        try:
            self.publication.require_request(prepared.operation.request)
        except ValueError as exc:
            raise Conflict("provenance volume request differs from its source") from exc

    @classmethod
    def from_prepared(
        cls, session: Session, prepared: PreparedImmutableArchiveObject, *, role: Role
    ) -> ArchiveProvenanceVolumeSource:
        upload = _upload(session, prepared.collection_id)
        if role == "bindings":
            digest = prepared.operation.request.required_identity_assertions.get(
                "riverhog-plaintext-sha256"
            )
            row = (
                session.scalar(
                    select(Volume)
                    .where(
                        Volume.collection_id == prepared.collection_id,
                        Volume.binding_payload_sha256 == digest,
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if digest is not None
                else None
            )
            if row is None:
                raise NotFound("provenance binding original physical source is unavailable")
            sequence = row.sequence
        else:
            path = prepared.operation.request.object_path
            prefix = upload.archive_storage_prefix + "/provenance/metadata/volume-"
            if not path.startswith(prefix) or not path.endswith(".json.age"):
                raise Conflict("provenance volume original metadata path changed")
            try:
                sequence = parse_archive_sequence(path[len(prefix) : -len(".json.age")])
            except ValueError as exc:
                raise Conflict("provenance volume original metadata sequence changed") from exc
        source = cls.load(session, prepared.collection_id, sequence, role=role)
        source._require_prepared(prepared)
        return source

    def _encoded_receipt(self, row: Volume) -> str | None:
        return row.payload_receipt_json if self.role == "bindings" else row.metadata_receipt_json

    def _projection(self, receipt: SealedProvenanceObject) -> str:
        expected = self.publication.sealed_receipt(
            ImmutableObjectReceipt(
                object_path=self.publication.object_path,
                revision=receipt.revision,
                entity_token=None,
                stored_bytes=receipt.stored_bytes,
                stored_sha256=receipt.stored_sha256,
                completed_at=receipt.completed_at,
            )
        )
        if receipt != expected:
            raise Conflict("provenance volume receipt differs from its source")
        return _json(asdict(receipt))

    def _require_payload(self, session: Session, row: Volume) -> SealedProvenanceObject:
        if row.payload_receipt_json is None:
            raise Conflict("provenance volume original payload is not received")
        receipt = SealedProvenanceObject(**json.loads(row.payload_receipt_json))
        if self.document.payload.kind == "bindings":
            source = self.load(session, self.collection_id, self.sequence, role="bindings")
            if source._projection(receipt) != row.payload_receipt_json:
                raise Conflict("provenance volume original payload proof changed")
        else:
            document = self.document
            assert document.journal_id is not None and document.journal_offset is not None
            source_journal = ArchiveProvenanceJournalSource.load(
                session, self.collection_id, document.payload.sha256
            )
            selected, _ = ProvenanceJournalRange.capture(
                session,
                self.collection_id,
                document.journal_id,
                offset=document.journal_offset,
                size=document.payload.bytes,
            )
            if source_journal.received_association(session, selected) != receipt:
                raise Conflict("provenance volume original journal payload proof changed")
        return receipt

    def receive_journal_payload(self, session: Session) -> SealedProvenanceObject:
        _, row = self._source(session)
        document = self.document
        if document.payload.kind != "journal":
            raise Conflict("binding payload has its own original physical receiver")
        assert document.journal_id is not None and document.journal_offset is not None
        source = ArchiveProvenanceJournalSource.load(
            session, self.collection_id, document.payload.sha256
        )
        selected, _ = ProvenanceJournalRange.capture(
            session,
            self.collection_id,
            document.journal_id,
            offset=document.journal_offset,
            size=document.payload.bytes,
        )
        receipt = source.receive_association(session, selected)
        encoded = _json(asdict(receipt))
        if row.payload_receipt_json not in {None, encoded}:
            raise Conflict("provenance volume original journal payload proof changed")
        row.payload_receipt_json = encoded
        session.flush()
        return self._require_payload(session, row)

    def _receive(self, session: Session, receipt: SealedProvenanceObject, *, apply: bool) -> None:
        encoded = self._projection(receipt)
        _, row = self._source(session)
        if self.role == "metadata":
            self._require_payload(session, row)
        existing = self._encoded_receipt(row)
        if existing is not None:
            if existing != encoded:
                raise Conflict("provenance volume original receiving proof changed")
        elif apply:
            if self.role == "bindings":
                row.payload_receipt_json = encoded
            else:
                row.metadata_receipt_json = encoded
        else:
            raise Conflict("provenance volume original receipt is not received")
        session.flush()

    def received(self, session: Session) -> SealedProvenanceObject | None:
        _, row = self._source(session)
        encoded = self._encoded_receipt(row)
        if encoded is None:
            return None
        receipt = SealedProvenanceObject(**json.loads(encoded))
        self._receive(session, receipt, apply=False)
        return receipt

    def _receive_prepared(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterReceipt,
        *,
        apply: bool,
    ) -> None:
        self._require_prepared(prepared)
        require_immutable_archive_receipt(prepared.operation.request, receipt)
        projected = self.publication.sealed_receipt(
            ImmutableObjectReceipt(
                object_path=receipt.object_path,
                revision=receipt.revision,
                entity_token=receipt.entity_token,
                stored_bytes=receipt.stored_bytes,
                stored_sha256=receipt.stored_sha256,
                completed_at=receipt.completed_at,
            )
        )
        self._receive(session, projected, apply=apply)

    def apply_prepared_receipt(
        self, session: Session, prepared: PreparedImmutableArchiveObject, receipt: AdapterReceipt
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=True)

    def verify_prepared_receipt(
        self, session: Session, prepared: PreparedImmutableArchiveObject, receipt: AdapterReceipt
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=False)

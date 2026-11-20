"""Original journal publication and bounded logical custody association receiving."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from typing import Any

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    ImmutableObjectReceipt as AdapterImmutableObjectReceipt,
)
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.archive_provenance import ArchiveProvenancePublication
from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceCustodyObjectRecord as Custody,
)
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceJournalPublicationRecord as Publication,
)
from riverhog_core.collection_construction import upload_construction
from riverhog_core.domain.archive import SealedProvenanceObject
from riverhog_core.immutable_archive_identity import (
    require_immutable_archive_receipt,
)
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard, _writer
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)
from riverhog_core.upload_journal_ranges import upload_journal_range_bytes


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
        raise NotFound("provenance journal receiving parent is unavailable")
    return upload


@dataclass(frozen=True, slots=True)
class ProvenanceJournalRange:
    journal_id: str
    journal_bytes: int
    journal_sha256: str
    offset: int
    size: int
    plaintext_sha256: str

    @classmethod
    def capture(
        cls, session: Session, collection_id: int, journal_id: str, *, offset: int, size: int
    ) -> tuple[ProvenanceJournalRange, bytes]:
        journal = session.get(
            Journal, (collection_id, journal_id), with_for_update=True, populate_existing=True
        )
        if journal is None:
            raise NotFound("provenance journal original source is unavailable")
        if (
            journal.state != "sealed"
            or journal.accepted_bytes != journal.bytes
            or type(offset) is not int
            or offset < 0
            or type(size) is not int
            or size < 1
            or offset + size > journal.bytes
        ):
            raise Conflict("provenance journal source range is not sealed")
        content = upload_journal_range_bytes(
            session, collection_id, journal_id, offset=offset, size=size
        )
        return cls(
            journal_id,
            journal.bytes,
            journal.sha256,
            offset,
            size,
            hashlib.sha256(content).hexdigest(),
        ), content

    def require_current(self, session: Session, collection_id: int) -> bytes:
        current, content = self.capture(
            session, collection_id, self.journal_id, offset=self.offset, size=self.size
        )
        if current != self:
            raise Conflict("provenance journal original source changed")
        return content


@dataclass(frozen=True, slots=True)
class ArchiveProvenanceJournalSource:
    """The first physical origin is retained independently of later associations."""

    collection_id: int
    source_json: str = field(repr=False)
    publication: ArchiveProvenancePublication = field(repr=False)
    forward_guard: Guard | None = field(default=None, repr=False, compare=False)

    def _value(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self.source_json)
        if (
            set(value)
            != {
                "collection_id",
                "construction_sha256",
                "archive_incarnation_id",
                "range",
            }
            or value["collection_id"] != self.collection_id
            or _json(value) != self.source_json
        ):
            raise Conflict("provenance journal original source context changed")
        return value

    @property
    def journal_range(self) -> ProvenanceJournalRange:
        return ProvenanceJournalRange(**self._value()["range"])

    @property
    def reserved_source_bytes(self) -> int:
        # Retained source values are charged alongside the original preparation;
        # database index/page costs remain with the database resource owner.
        return (
            len(self.source_json.encode())
            + len(_sha(self.source_json).encode())
            + len(self.publication.plaintext_sha256.encode())
            + len(str(self.collection_id).encode())
        )

    def write_grants(self, grants: ArchiveWriteGrants) -> ArchiveWriteGrants:
        remaining = grants.metadata_preparation_bytes - self.reserved_source_bytes
        if remaining < 1:
            raise Conflict("provenance journal source exceeds its preparation grant")
        return replace(grants, metadata_preparation_bytes=remaining)

    @classmethod
    def stage(
        cls,
        session: Session,
        collection_id: int,
        *,
        journal_id: str,
        offset: int,
        size: int,
        grants: ArchiveWriteGrants,
        forward_guard: Guard,
        expected_range: ProvenanceJournalRange | None = None,
    ) -> ArchiveProvenanceJournalSource:
        _writer(session)
        forward_guard(session)
        upload = _upload(session, collection_id)
        if upload.state not in {"open", "closing", "uploading", "finalizing"}:
            raise Conflict("provenance journal construction cannot stage writes")
        ArchiveWriteAdmission.require_no_unresolved_calls(session, collection_id)
        control = ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        if control.gate != "open":
            raise Conflict("provenance journal admission is closed")
        selected, content = ProvenanceJournalRange.capture(
            session, collection_id, journal_id, offset=offset, size=size
        )
        if expected_range is not None and selected != expected_range:
            raise Conflict("verified artifact custody journal selection changed")
        row = session.get(
            Publication,
            (collection_id, selected.plaintext_sha256),
            with_for_update=True,
            populate_existing=True,
        )
        if row is None:
            context = _json(
                {
                    "collection_id": collection_id,
                    "construction_sha256": _sha(upload_construction(upload)),
                    "archive_incarnation_id": upload.archive_incarnation_id,
                    "range": asdict(selected),
                }
            )
            publication = ArchiveProvenancePublication.from_journal_segment(
                archive_storage_prefix=upload.archive_storage_prefix, content=content
            )
            source = cls(collection_id, context, publication, forward_guard)
            source.write_grants(grants)
            row = Publication(
                collection_id=collection_id,
                plaintext_sha256=selected.plaintext_sha256,
                source_json=context,
                source_sha256=_sha(context),
            )
            session.add(row)
            session.flush()
            return source
        # The existing first origin stays authoritative even when another
        # journal/position references identical bytes.
        source = cls.load(
            session, collection_id, selected.plaintext_sha256, forward_guard=forward_guard
        )
        if len(source.publication.plaintext) != size:
            raise Conflict("provenance journal shared plaintext identity changed")
        source.write_grants(grants)
        return source

    @classmethod
    def load(
        cls,
        session: Session,
        collection_id: int,
        plaintext_sha256: str,
        *,
        forward_guard: Guard | None = None,
    ) -> ArchiveProvenanceJournalSource:
        upload = _upload(session, collection_id)
        row = session.get(
            Publication,
            (collection_id, plaintext_sha256),
            with_for_update=True,
            populate_existing=True,
        )
        if row is None:
            raise NotFound("provenance journal original publication is unavailable")
        if row.source_sha256 != _sha(row.source_json):
            raise Conflict("provenance journal original source context changed")
        value = json.loads(row.source_json)
        if (
            value["construction_sha256"] != _sha(upload_construction(upload))
            or value["archive_incarnation_id"] != upload.archive_incarnation_id
        ):
            raise Conflict("provenance journal original construction changed")
        selected = ProvenanceJournalRange(**value["range"])
        if selected.plaintext_sha256 != plaintext_sha256:
            raise Conflict("provenance journal original range identity changed")
        content = selected.require_current(session, collection_id)
        source = cls(
            collection_id,
            row.source_json,
            ArchiveProvenancePublication.from_journal_segment(
                archive_storage_prefix=upload.archive_storage_prefix, content=content
            ),
            forward_guard,
        )
        source._value()
        return source

    def _source(self, session: Session) -> tuple[Upload, Publication]:
        current = self.load(session, self.collection_id, self.publication.plaintext_sha256)
        if current.source_json != self.source_json or current.publication != self.publication:
            raise Conflict("provenance journal original publication source changed")
        upload = _upload(session, self.collection_id)
        row = session.get(Publication, (self.collection_id, self.publication.plaintext_sha256))
        assert row is not None
        return upload, row

    def guard(self, session: Session) -> None:
        if self.forward_guard is None:
            raise Conflict("provenance journal current forward authority is required")
        self.forward_guard(session)
        upload, row = self._source(session)
        if upload.state not in {"open", "closing", "uploading", "finalizing"}:
            raise Conflict("provenance journal construction cannot start writes")
        ArchiveWriteAdmission.require_no_unresolved_calls(session, self.collection_id)
        if row.receipt_json is not None:
            raise Conflict("provenance journal original receipt is already received")

    def validate_metadata(
        self, session: Session, upload: Upload, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id:
            raise Conflict("provenance journal belongs to another construction")
        self._source(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict("provenance journal request differs from its source") from exc

    def _require_prepared(self, prepared: PreparedImmutableArchiveObject) -> None:
        value = prepared._value()
        if (
            prepared.collection_id != self.collection_id
            or prepared.object_id
            != ImmutableArchivePreparation.object_id(self.publication.object_path)
            or value["construction_sha256"] != self._value()["construction_sha256"]
            or value["target_incarnation"] != self._value()["archive_incarnation_id"]
        ):
            raise Conflict("provenance journal preparation belongs to another original source")
        try:
            self.publication.require_request(prepared.operation.request)
        except ValueError as exc:
            raise Conflict("provenance journal request differs from its source") from exc

    @classmethod
    def from_prepared(
        cls, session: Session, prepared: PreparedImmutableArchiveObject
    ) -> ArchiveProvenanceJournalSource:
        digest = prepared.operation.request.required_identity_assertions.get(
            "riverhog-plaintext-sha256"
        )
        if digest is None:
            raise Conflict("provenance journal original plaintext identity is unavailable")
        source = cls.load(session, prepared.collection_id, digest)
        source._require_prepared(prepared)
        return source

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
            raise Conflict("provenance journal receipt differs from its source")
        return _json(asdict(receipt))

    def _association(
        self,
        session: Session,
        selected: ProvenanceJournalRange,
        encoded: str,
        *,
        apply: bool,
    ) -> None:
        selected.require_current(session, self.collection_id)
        if selected.plaintext_sha256 != self.publication.plaintext_sha256 or selected.size != len(
            self.publication.plaintext
        ):
            raise Conflict("provenance journal association does not identify original bytes")
        key = (
            self.collection_id,
            selected.journal_id,
            selected.offset,
            selected.plaintext_sha256,
        )
        row = session.get(Custody, key, with_for_update=True, populate_existing=True)
        if row is None:
            if not apply:
                raise Conflict("provenance journal original association is not received")
            session.add(
                Custody(
                    collection_id=self.collection_id,
                    journal_id=selected.journal_id,
                    byte_offset=selected.offset,
                    plaintext_bytes=selected.size,
                    plaintext_sha256=selected.plaintext_sha256,
                    relative_path=self.publication.relative_path,
                    receipt_json=encoded,
                )
            )
        elif (
            row.plaintext_bytes != selected.size
            or row.relative_path != self.publication.relative_path
            or row.receipt_json != encoded
        ):
            raise Conflict("provenance journal original association proof changed")

    def _receive(self, session: Session, receipt: SealedProvenanceObject, *, apply: bool) -> None:
        encoded = self._projection(receipt)
        _, row = self._source(session)
        if row.receipt_json is not None:
            if row.receipt_json != encoded:
                raise Conflict("provenance journal original receiving proof changed")
        elif apply:
            row.receipt_json = encoded
        else:
            raise Conflict("provenance journal original receipt is not received")
        self._association(session, self.journal_range, encoded, apply=apply)
        session.flush()

    def apply_receipt(self, session: Session, receipt: SealedProvenanceObject) -> None:
        self._receive(session, receipt, apply=True)

    def verify_receipt(self, session: Session, receipt: SealedProvenanceObject) -> None:
        self._receive(session, receipt, apply=False)

    def received(self, session: Session) -> SealedProvenanceObject | None:
        _, row = self._source(session)
        if row.receipt_json is None:
            return None
        receipt = SealedProvenanceObject(**json.loads(row.receipt_json))
        self._receive(session, receipt, apply=False)
        return receipt

    def received_association(
        self, session: Session, selected: ProvenanceJournalRange
    ) -> SealedProvenanceObject | None:
        """Only an exact retained physical and logical proof completes this position."""
        receipt = self.received(session)
        row = session.get(
            Custody,
            (
                self.collection_id,
                selected.journal_id,
                selected.offset,
                selected.plaintext_sha256,
            ),
            with_for_update=True,
            populate_existing=True,
        )
        if row is None:
            return None
        if receipt is None:
            raise Conflict("provenance journal association has no physical receiving proof")
        self._association(session, selected, self._projection(receipt), apply=False)
        return receipt

    def receive_association(
        self, session: Session, selected: ProvenanceJournalRange
    ) -> SealedProvenanceObject:
        receipt = self.received(session)
        if receipt is None:
            raise Conflict("provenance journal physical receipt is not received")
        self._association(session, selected, self._projection(receipt), apply=True)
        session.flush()
        return receipt

    def _receive_prepared(
        self,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        receipt: AdapterImmutableObjectReceipt,
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

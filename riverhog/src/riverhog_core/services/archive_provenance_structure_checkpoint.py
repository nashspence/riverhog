"""Original staged provenance facts and exact receipt receiving."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace

from riverhog_archive_contracts import (
    MemberHistoryBinding,
    MemberHistoryDocument,
    RecordPage,
    SourceMemberHistoryBindingProof,
    provenance_structure_identity,
    provenance_structure_object_id,
)
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
    CollectionUploadProvenanceStructureRecord as Structure,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.collection_construction import upload_construction, upload_construction_fields
from riverhog_core.domain.archive import SealedProvenanceObject
from riverhog_core.immutable_archive_identity import require_immutable_archive_receipt
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard, _writer
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _upload(session: Session, collection_id: int) -> Upload:
    upload = session.scalar(
        select(Upload)
        .where(Upload.collection_id == collection_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if upload is None:
        raise NotFound("provenance structure receiving parent is unavailable")
    return upload


def _structure(session: Session, collection_id: int, object_id: str) -> Structure:
    row = session.get(
        Structure, (collection_id, object_id), with_for_update=True, populate_existing=True
    )
    if row is None:
        raise NotFound("provenance structure receiving source is unavailable")
    return row


def _publication(upload: Upload, row: Structure) -> ArchiveProvenancePublication:
    identity = provenance_structure_identity(row.content)
    if row.collection_id != upload.collection_id or (
        row.object_id,
        row.kind,
        row.relative_path,
    ) != (identity.object_id, identity.kind, identity.relative_path):
        raise Conflict("provenance structure staged identity changed")
    prefix = upload.archive_storage_prefix
    if row.kind == "history":
        history = MemberHistoryDocument.from_json_bytes(row.content)
        return ArchiveProvenancePublication.from_member_history(
            archive_storage_prefix=prefix,
            binding=MemberHistoryBinding(
                history.artifact_id,
                history.bytes,
                history.sha256,
                history.identity,
                len(row.content),
            ),
            content=row.content,
        )
    if row.kind == "record-page":
        return ArchiveProvenancePublication.from_record_page(
            archive_storage_prefix=prefix, page=RecordPage.from_json_bytes(row.content)
        )
    if row.kind == "source-proof":
        return ArchiveProvenancePublication.from_source_binding_proof(
            archive_storage_prefix=prefix,
            proof=SourceMemberHistoryBindingProof.from_json_bytes(row.content),
        )
    raise Conflict("unsupported staged provenance structure")


@dataclass(frozen=True, slots=True)
class ArchiveProvenanceStructureSource:
    """Original receiving facts carry no live key, claim or lease authority."""

    collection_id: int
    object_id: str
    construction_json: str = field(repr=False)
    construction_sha256: str
    publication: ArchiveProvenancePublication = field(repr=False)
    forward_guard: Guard | None = field(default=None, repr=False, compare=False)

    @classmethod
    def capture(
        cls, upload: Upload, row: Structure, *, forward_guard: Guard | None = None
    ) -> ArchiveProvenanceStructureSource:
        return cls(
            upload.collection_id,
            row.object_id,
            _json(upload_construction_fields(upload)),
            hashlib.sha256(upload_construction(upload).encode()).hexdigest(),
            _publication(upload, row),
            forward_guard,
        )

    @classmethod
    def load(
        cls,
        session: Session,
        collection_id: int,
        object_id: str,
        *,
        forward_guard: Guard | None = None,
    ) -> ArchiveProvenanceStructureSource:
        upload = _upload(session, collection_id)
        return cls.capture(
            upload, _structure(session, collection_id, object_id), forward_guard=forward_guard
        )

    @classmethod
    def select_current(
        cls,
        session: Session,
        collection_id: int,
        *,
        forward_guard: Guard,
        grants: ArchiveWriteGrants,
        selected_object_id: str | None = None,
    ) -> ArchiveProvenanceStructureSource | None:
        """Select one pending source under the caller's current key or claim."""
        _writer(session)
        forward_guard(session)
        upload = _upload(session, collection_id)
        if upload.state not in {"open", "closing", "uploading", "finalizing"}:
            raise Conflict("provenance structure construction cannot select writes")
        ArchiveWriteAdmission.require_no_unresolved_calls(session, collection_id)
        require_closure = selected_object_id is None
        if require_closure and not upload.provenance_closure_validated:
            return None
        # The maintained partial index contains only pending objects, so repeated
        # selection does not scan the already published prefix.
        statement = select(Structure).where(
            Structure.collection_id == collection_id,
            Structure.receipt_json.is_(None),
        )
        if selected_object_id is not None:
            statement = statement.where(Structure.object_id == selected_object_id)
        row = session.scalar(statement.order_by(Structure.object_id).limit(1).with_for_update())
        if row is None:
            return None

        def current(session: Session) -> None:
            forward_guard(session)
            if require_closure and not _upload(session, collection_id).provenance_closure_validated:
                raise Conflict("provenance structure closure is no longer validated")

        source = cls.capture(upload, row, forward_guard=current)
        source.write_grants(grants)
        return source

    @property
    def reserved_source_bytes(self) -> int:
        # Staged content and original context remain alongside preparation.
        # Database index/page costs belong to the database resource owner.
        return (
            len(str(self.collection_id).encode())
            + len(self.object_id.encode())
            + len(self.publication.relative_path.encode())
            + len(self.publication.kind.removeprefix("provenance-").encode())
            + len(self.publication.plaintext)
            + len(self.construction_json.encode())
            + len(self.construction_sha256.encode())
        )

    def write_grants(self, grants: ArchiveWriteGrants) -> ArchiveWriteGrants:
        remaining = grants.metadata_preparation_bytes - self.reserved_source_bytes
        if remaining < 1:
            raise Conflict("provenance structure source exceeds its preparation grant")
        return replace(grants, metadata_preparation_bytes=remaining)

    def _source(self, session: Session) -> tuple[Upload, Structure]:
        upload = _upload(session, self.collection_id)
        if (
            _json(upload_construction_fields(upload)) != self.construction_json
            or hashlib.sha256(upload_construction(upload).encode()).hexdigest()
            != self.construction_sha256
        ):
            raise Conflict("provenance structure original construction changed")
        row = _structure(session, self.collection_id, self.object_id)
        if _publication(upload, row) != self.publication:
            raise Conflict("provenance structure original staged source changed")
        return upload, row

    def guard(self, session: Session) -> None:
        if self.forward_guard is None:
            raise Conflict("provenance structure current forward authority is required")
        # The caller's existing owner checks its key/claim/lease before the
        # construction lock. Receiving never calls or renews that authority.
        self.forward_guard(session)
        upload, row = self._source(session)
        if upload.state not in {"open", "closing", "uploading", "finalizing"}:
            raise Conflict("provenance structure construction cannot start writes")
        ArchiveWriteAdmission.require_no_unresolved_calls(session, self.collection_id)
        if row.receipt_json is not None:
            raise Conflict("provenance structure original receipt is already received")

    def validate_metadata(
        self, session: Session, upload: Upload, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id:
            raise Conflict("provenance structure belongs to another construction")
        self._source(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict("provenance structure request differs from its source") from exc

    def _require_prepared(self, prepared: PreparedImmutableArchiveObject) -> None:
        value = prepared._value()
        if (
            prepared.collection_id != self.collection_id
            or prepared.object_id
            != ImmutableArchivePreparation.object_id(self.publication.object_path)
            or value["construction_sha256"] != self.construction_sha256
            or value["target_incarnation"]
            != json.loads(self.construction_json)["archive_incarnation_id"]
        ):
            raise Conflict("provenance structure preparation belongs to another construction")
        try:
            self.publication.require_request(prepared.operation.request)
        except ValueError as exc:
            raise Conflict("provenance structure request differs from its source") from exc

    @classmethod
    def from_prepared(
        cls, session: Session, prepared: PreparedImmutableArchiveObject
    ) -> ArchiveProvenanceStructureSource:
        upload = _upload(session, prepared.collection_id)
        prefix = upload.archive_storage_prefix + "/"
        path = prepared.operation.request.object_path
        if not path.startswith(prefix):
            raise Conflict("provenance structure original prefix changed")
        try:
            object_id = provenance_structure_object_id(path.removeprefix(prefix))
        except ValueError as exc:
            raise Conflict("provenance structure original path is invalid") from exc
        source = cls.capture(upload, _structure(session, prepared.collection_id, object_id))
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
            raise Conflict("provenance structure receipt differs from its source")
        return _json(asdict(receipt))

    def _receive(self, session: Session, receipt: SealedProvenanceObject, *, apply: bool) -> None:
        encoded = self._projection(receipt)
        _, row = self._source(session)
        if row.receipt_json is not None:
            if row.receipt_json != encoded:
                raise Conflict("provenance structure original receiving proof changed")
        elif apply:
            row.receipt_json = encoded
            session.flush()
        else:
            raise Conflict("provenance structure original receipt is not received")

    def apply_receipt(self, session: Session, receipt: SealedProvenanceObject) -> None:
        self._receive(session, receipt, apply=True)

    def verify_receipt(self, session: Session, receipt: SealedProvenanceObject) -> None:
        self._receive(session, receipt, apply=False)

    def received(self, session: Session) -> SealedProvenanceObject | None:
        _, row = self._source(session)
        if row.receipt_json is None:
            return None
        receipt = SealedProvenanceObject(**json.loads(row.receipt_json))
        if self._projection(receipt) != row.receipt_json:
            raise Conflict("provenance structure original receiving proof changed")
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

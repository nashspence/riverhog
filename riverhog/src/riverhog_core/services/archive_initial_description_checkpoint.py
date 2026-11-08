"""Original initial-description facts, physical receiving and separate progress."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from typing import Any

from riverhog_protocol import CollectionDescriptionDocument, collection_description_identity
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import ImmutableObjectReceipt as AdapterReceipt
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy.orm import Session

from riverhog_core.archive_collection_metadata import ArchiveCollectionMetadataPublication
from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_initial_metadata_models import (
    CollectionUploadInitialDescriptionRecord as Description,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.immutable_archive_identity import require_immutable_archive_receipt
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.ports.archive_store import CollectionDescriptionReceipt
from riverhog_core.services.archive_initial_metadata_basis import (
    current_initial_metadata_basis,
    require_original_metadata_basis,
)
from riverhog_core.services.archive_initial_metadata_basis import (
    initial_metadata_parent as _upload,
)
from riverhog_core.services.archive_initial_metadata_basis import (
    require_initial_metadata_claim as _claim,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard, _writer
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _context(session: Session, upload: Upload) -> dict[str, Any]:
    return {**current_initial_metadata_basis(session, upload), "description": upload.description}


def _publication(original: dict[str, Any]) -> ArchiveCollectionMetadataPublication | None:
    if original["description"] is None:
        return None
    document = CollectionDescriptionDocument.seal(
        archive_root_sha256=original["archive_root_sha256"],
        revision=1,
        description=original["description"],
    )
    return ArchiveCollectionMetadataPublication(
        original["archive_storage_prefix"],
        "description",
        document.to_json_bytes(),
        original["passphrase_id"],
    )


@dataclass(frozen=True, slots=True)
class ArchiveInitialDescriptionSource:
    """Original facts grant no current claim, new dispatch or global quiescence."""

    collection_id: int
    source_json: str = field(repr=False)
    document_json: str | None = field(repr=False)
    _publication: ArchiveCollectionMetadataPublication | None = field(repr=False)
    forward_guard: Guard | None = field(default=None, repr=False, compare=False)

    @property
    def has_publication(self) -> bool:
        return self._publication is not None

    @property
    def publication(self) -> ArchiveCollectionMetadataPublication:
        if self._publication is None:
            raise Conflict("absent initial description has no provider write")
        return self._publication

    @classmethod
    def stage_next(
        cls,
        session: Session,
        collection_id: int,
        *,
        grants: ArchiveWriteGrants,
        forward_guard: Guard | None = None,
    ) -> ArchiveInitialDescriptionSource | None:
        _writer(session)
        if forward_guard is not None:
            forward_guard(session)
        upload = _upload(session, collection_id)
        ArchiveWriteAdmission.require_no_unresolved_calls(session, collection_id)
        if (
            upload.description_identity is not None
            or upload.description_publication_receipt_json is not None
        ):
            cls.load(session, collection_id).verify_progress(session)
            return None
        _claim(session, upload)
        control = ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        if control.gate != "open":
            raise Conflict("initial description admission is closed")
        original = _context(session, upload)
        publication = _publication(original)
        row = session.get(Description, collection_id, with_for_update=True)
        if row is None:
            encoded = _json(original)
            row = Description(
                collection_id=collection_id,
                document_json=None if publication is None else publication.plaintext.decode(),
                source_json=encoded,
                source_sha256=_sha(encoded),
            )
            session.add(row)
            session.flush()
        source = cls.load(session, collection_id, forward_guard=forward_guard)
        if source.source_json != _json(original):
            raise Conflict("initial description original inputs changed")
        source.write_grants(grants)
        return source

    @classmethod
    def load(
        cls, session: Session, collection_id: int, *, forward_guard: Guard | None = None
    ) -> ArchiveInitialDescriptionSource:
        upload = _upload(session, collection_id)
        row = session.get(Description, collection_id, with_for_update=True, populate_existing=True)
        if row is None:
            raise NotFound("initial description original source is unavailable")
        try:
            original = json.loads(row.source_json)
            if (
                set(original)
                != {
                    "construction_sha256",
                    "archive_incarnation_id",
                    "archive_storage_prefix",
                    "passphrase_id",
                    "archive_root_sha256",
                    "archive_root_receipt_sha256",
                    "description",
                }
                or _json(original) != row.source_json
                or row.source_sha256 != _sha(row.source_json)
            ):
                raise Conflict("initial description original construction changed")
            require_original_metadata_basis(session, upload, original)
            publication = _publication(original)
            expected = None if publication is None else publication.plaintext.decode()
            if row.document_json != expected:
                raise Conflict("initial description original document changed")
            # Revision-zero identity validates the root even when there is no
            # physical description document to parse.
            collection_description_identity(
                archive_root_sha256=original["archive_root_sha256"],
                revision=0 if publication is None else 1,
                description=original["description"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise Conflict("initial description original source is invalid") from exc
        return cls(collection_id, row.source_json, row.document_json, publication, forward_guard)

    def _source(self, session: Session) -> tuple[Upload, Description]:
        source = self.load(session, self.collection_id)
        if source != self:
            raise Conflict("initial description original source changed")
        upload = _upload(session, self.collection_id)
        row = session.get(Description, self.collection_id)
        assert row is not None
        return upload, row

    @property
    def reserved_source_bytes(self) -> int:
        return (
            len(str(self.collection_id).encode())
            + len(self.source_json.encode())
            + 64
            + (0 if self.document_json is None else len(self.document_json.encode()))
        )

    def write_grants(self, grants: ArchiveWriteGrants) -> ArchiveWriteGrants:
        remaining = grants.metadata_preparation_bytes - self.reserved_source_bytes
        if remaining < 1:
            raise Conflict("initial description source exceeds its preparation grant")
        return replace(grants, metadata_preparation_bytes=remaining)

    def guard(self, session: Session) -> None:
        upload, row = self._source(session)
        if self.forward_guard is not None:
            self.forward_guard(session)
        _claim(session, upload)
        if self.source_json != _json(_context(session, upload)):
            raise Conflict("initial description original inputs changed")
        if (
            row.receipt_json is not None
            or row.progress_receipt_json is not None
            or upload.description_identity is not None
            or upload.description_publication_receipt_json is not None
        ):
            raise Conflict("initial description original progress is already received")
        if self._publication is None:
            raise Conflict("absent initial description has no provider write")

    def validate_metadata(
        self, session: Session, upload: Upload, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id or self._publication is None:
            raise Conflict("initial description does not own this publication")
        self._source(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict("initial description request differs from its source") from exc

    def _require_prepared(self, prepared: PreparedImmutableArchiveObject) -> None:
        original = json.loads(self.source_json)
        if (
            self._publication is None
            or prepared.collection_id != self.collection_id
            or prepared.object_id
            != ImmutableArchivePreparation.object_id(self.publication.object_path)
            or prepared._value()["construction_sha256"] != original["construction_sha256"]
            or prepared._value()["target_incarnation"] != original["archive_incarnation_id"]
        ):
            raise Conflict("initial description preparation belongs to another source")
        try:
            self.publication.require_request(prepared.operation.request)
        except ValueError as exc:
            raise Conflict("initial description preparation differs from its source") from exc

    @classmethod
    def from_prepared(
        cls, session: Session, prepared: PreparedImmutableArchiveObject
    ) -> ArchiveInitialDescriptionSource:
        source = cls.load(session, prepared.collection_id)
        source._require_prepared(prepared)
        return source

    def _projection(self, receipt: CollectionDescriptionReceipt) -> str:
        if (
            self._publication is None
            or receipt.object_path != self.publication.object_path
            or receipt.stored_bytes <= 0
            or not receipt.stored_sha256
        ):
            raise Conflict("initial description receipt differs from its source")
        return _json(asdict(receipt))

    def received(self, session: Session) -> CollectionDescriptionReceipt | None:
        _, row = self._source(session)
        if row.receipt_json is None:
            return None
        try:
            receipt = CollectionDescriptionReceipt(**json.loads(row.receipt_json))
            if self._projection(receipt) != row.receipt_json:
                raise Conflict("initial description original receiving proof changed")
            return receipt
        except (TypeError, ValueError) as exc:
            raise Conflict("initial description original receiving proof is invalid") from exc

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
        assert self._publication is not None
        projected = self.publication.receipt(
            ImmutableObjectReceipt(
                object_path=receipt.object_path,
                revision=receipt.revision,
                entity_token=receipt.entity_token,
                stored_bytes=receipt.stored_bytes,
                stored_sha256=receipt.stored_sha256,
                completed_at=receipt.completed_at,
            )
        )
        assert isinstance(projected, CollectionDescriptionReceipt)
        encoded = self._projection(projected)
        _, row = self._source(session)
        if row.receipt_json is None:
            if not apply:
                raise Conflict("initial description receipt is not durably received")
            row.receipt_json = encoded
        elif row.receipt_json != encoded:
            raise Conflict("initial description original receipt changed")
        session.flush()

    def apply_prepared_receipt(
        self, session: Session, prepared: PreparedImmutableArchiveObject, receipt: AdapterReceipt
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=True)

    def verify_prepared_receipt(
        self, session: Session, prepared: PreparedImmutableArchiveObject, receipt: AdapterReceipt
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=False)

    def _progress_value(self, session: Session) -> dict[str, Any]:
        original = json.loads(self.source_json)
        revision = 0 if self._publication is None else 1
        receipt = self.received(session)
        if self._publication is not None and receipt is None:
            raise Conflict("initial description original physical receipt is unavailable")
        return {
            "source_sha256": _sha(self.source_json),
            "projection": {
                "description_revision": revision,
                "description_identity": collection_description_identity(
                    archive_root_sha256=original["archive_root_sha256"],
                    revision=revision,
                    description=original["description"],
                ),
                "description_publication_receipt_json": (
                    None if receipt is None else self._projection(receipt)
                ),
            },
        }

    def verify_progress(self, session: Session) -> None:
        upload, row = self._source(session)
        if row.progress_receipt_json is None:
            raise Conflict("initial description original progress proof is missing")
        expected = self._progress_value(session)
        if row.progress_receipt_json != _json(expected) or any(
            getattr(upload, name) != value for name, value in expected["projection"].items()
        ):
            raise Conflict("initial description original progress proof changed")

    def commit_progress(self, session: Session) -> bool:
        _writer(session)
        upload, row = self._source(session)
        if row.progress_receipt_json is not None:
            self.verify_progress(session)
            return False
        if self.forward_guard is not None:
            self.forward_guard(session)
        _claim(session, upload)
        if self.source_json != _json(_context(session, upload)):
            raise Conflict("initial description original inputs changed")
        control = ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        if control.gate != "open":
            raise Conflict("initial description progress admission is closed")
        if (
            upload.description_identity is not None
            or upload.description_publication_receipt_json is not None
        ):
            raise Conflict("initial description has no original progress proof")
        expected = self._progress_value(session)
        for name, value in expected["projection"].items():
            setattr(upload, name, value)
        row.progress_receipt_json = _json(expected)
        session.flush()
        self.verify_progress(session)
        return True

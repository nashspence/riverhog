"""Original provenance terminal/root sources, physical receiving and separate progress."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal

from riverhog_archive_contracts import (
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    update_provenance_commitment,
)
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import ImmutableObjectReceipt as AdapterReceipt
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core import provenance_volume_source as volumes
from riverhog_core.archive_provenance import ArchiveProvenancePublication
from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceAuthorityPublicationRecord as Authority,
)
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceVolumePublicationRecord as Volume,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.collection_construction import upload_construction
from riverhog_core.domain.archive import SealedProvenanceObject
from riverhog_core.immutable_archive_identity import require_immutable_archive_receipt
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.services.archive_provenance_volume_checkpoint import (
    ArchiveProvenanceVolumeSource,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, _writer
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)

Role = Literal["terminal", "root"]


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
        raise NotFound("provenance authority original parent is unavailable")
    return upload


def _claim(upload: Upload) -> None:
    if (
        upload.state != "finalizing"
        or upload.archive_phase != "finalizing"
        or not upload.provenance_closure_validated
        or upload.archive_tree_sha256 is None
        or upload.final_authority_json is not None
    ):
        raise Conflict("provenance authority finalizer claim is not current")


def _tail_sha(session: Session, collection_id: int, sequence: int) -> str:
    if sequence < 1:
        raise Conflict("provenance authority has no committed corpus volume")
    row = session.get(
        Volume, (collection_id, sequence - 1), with_for_update=True, populate_existing=True
    )
    if row is None or row.progress_receipt_json is None:
        raise Conflict("provenance authority original corpus progress is unavailable")
    return _sha(row.progress_receipt_json)


def _basis(upload: Upload) -> dict[str, Any]:
    return {
        "construction_sha256": _sha(upload_construction(upload)),
        "archive_generation": upload.archive_generation,
        "archive_incarnation_id": upload.archive_incarnation_id,
        "archive_storage_prefix": upload.archive_storage_prefix,
        "artifact_set_sha256": upload.archive_tree_sha256,
        "artifact_count": upload.artifact_count,
        "delivery_context_id": upload.delivery_context_id,
        "next_sequence": upload.provenance_archive_next_sequence,
        "after_artifact_id": upload.provenance_archive_after_artifact_id,
        "next_artifact_order": upload.provenance_archive_next_artifact_order,
        "last_journal_id": upload.provenance_archive_last_journal_id,
        "current_journal_id": upload.provenance_archive_current_journal_id,
        "current_journal_offset": upload.provenance_archive_current_journal_offset,
        "binding_tree_state": upload.provenance_archive_binding_tree_state,
        "journal_count": upload.provenance_archive_journal_count,
        "hash_state": upload.provenance_archive_hash_state,
        "ordered_sha256": upload.provenance_archive_ordered_sha256,
        "terminal_receipt_json": upload.provenance_archive_terminal_receipt_json,
    }


def _prior_progress_sha(session: Session, collection_id: int, role: Role) -> str | None:
    if role == "terminal":
        return None
    row = session.get(
        Authority, (collection_id, "terminal"), with_for_update=True, populate_existing=True
    )
    if row is None or row.progress_receipt_json is None:
        raise Conflict("provenance root original terminal progress is unavailable")
    return _sha(row.progress_receipt_json)


def _context(session: Session, upload: Upload, role: Role) -> dict[str, Any]:
    return {
        **_basis(upload),
        "prior_progress_sha256": _prior_progress_sha(session, upload.collection_id, role),
        "last_progress_sha256": _tail_sha(
            session, upload.collection_id, upload.provenance_archive_next_sequence
        ),
    }


def _publication(role: Role, original: dict[str, Any]) -> ArchiveProvenancePublication:
    checkpoint = volumes.provenance_binding_checkpoint(
        state=original["binding_tree_state"],
        count=original["artifact_count"],
        after_artifact_id=original["after_artifact_id"],
    )
    if (
        original["next_artifact_order"] != original["artifact_count"]
        or original["current_journal_id"] is not None
        or original["current_journal_offset"] != 0
        or type(original["journal_count"]) is not int
        or not 1 <= original["journal_count"] <= original["next_sequence"]
        or original["last_journal_id"] is None
    ):
        raise Conflict("provenance authority original corpus inputs are incomplete")
    prefix = original["archive_storage_prefix"]
    if role == "terminal":
        if original["terminal_receipt_json"] is not None or original["ordered_sha256"] is not None:
            raise Conflict("provenance terminal original source already has terminal progress")
        try:
            CheckpointSHA256.from_state(original["hash_state"])
        except (TypeError, ValueError) as exc:
            raise Conflict("provenance terminal original commitment is invalid") from exc
        return ArchiveProvenancePublication.from_terminal(
            archive_storage_prefix=prefix,
            terminal=ProvenanceTerminalDocument(
                archive_generation=original["archive_generation"],
                artifact_set_sha256=original["artifact_set_sha256"],
                sequence=original["next_sequence"],
            ),
        )
    if role != "root":
        raise Conflict("unsupported provenance authority source role")
    if (
        original["terminal_receipt_json"] is None
        or original["hash_state"] is None
        or original["ordered_sha256"] is None
    ):
        raise Conflict("provenance root original terminal progress is incomplete")
    return ArchiveProvenancePublication.from_root(
        archive_storage_prefix=prefix,
        root=ProvenanceRootDocument(
            archive_generation=original["archive_generation"],
            artifact_set_sha256=original["artifact_set_sha256"],
            delivery_context_id=original["delivery_context_id"],
            binding_count=original["artifact_count"],
            binding_tree_sha256=checkpoint.root_sha256,
            journal_count=original["journal_count"],
            ordered_volume_sha256=original["ordered_sha256"],
        ),
    )


@dataclass(frozen=True, slots=True)
class ArchiveProvenanceAuthoritySource:
    """Original facts grant no current claim, new dispatch or global quiescence."""

    collection_id: int
    role: Role
    source_json: str = field(repr=False)
    document_json: str = field(repr=False)
    publication: ArchiveProvenancePublication = field(repr=False)

    @classmethod
    def stage_next(
        cls, session: Session, collection_id: int, *, grants: ArchiveWriteGrants
    ) -> ArchiveProvenanceAuthoritySource | None:
        _writer(session)
        upload = _upload(session, collection_id)
        ArchiveWriteAdmission.require_no_unresolved_calls(session, collection_id)
        if upload.provenance_archive_root_receipt_json is not None:
            cls.load(session, collection_id, role="root").verify_progress(session)
            return None
        _claim(upload)
        if upload.provenance_identity is not None:
            raise Conflict("provenance root has no original progress proof")
        control = ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        if control.gate != "open":
            raise Conflict("provenance authority admission is closed")
        if (
            upload.provenance_archive_next_artifact_order != upload.artifact_count
            or volumes.next_provenance_publication_journal(session, upload) is not None
        ):
            raise Conflict("provenance authority corpus traversal is incomplete")
        # Initial terminal selection verifies the last actual bounded volume
        # proof. Later original receiving uses its exact retained point reference.
        role: Role = "terminal"
        if upload.provenance_archive_terminal_receipt_json is None:
            _tail_sha(session, collection_id, upload.provenance_archive_next_sequence)
            ArchiveProvenanceVolumeSource.load(
                session, collection_id, upload.provenance_archive_next_sequence - 1, role="metadata"
            ).verify_progress(session)
        else:
            cls.load(session, collection_id, role="terminal").verify_progress(session)
            role = "root"
        original = _context(session, upload, role)
        publication = _publication(role, original)
        row = session.get(Authority, (collection_id, role), with_for_update=True)
        if row is None:
            encoded = _json(original)
            row = Authority(
                collection_id=collection_id,
                role=role,
                document_json=publication.plaintext.decode(),
                source_json=encoded,
                source_sha256=_sha(encoded),
            )
            session.add(row)
            session.flush()
        source = cls.load(session, collection_id, role=role)
        if source.source_json != _json(original):
            raise Conflict("provenance authority original cursor changed")
        source.write_grants(grants)
        return source

    @classmethod
    def load(
        cls, session: Session, collection_id: int, *, role: Role
    ) -> ArchiveProvenanceAuthoritySource:
        upload = _upload(session, collection_id)
        row = session.get(
            Authority, (collection_id, role), with_for_update=True, populate_existing=True
        )
        if row is None:
            raise NotFound("provenance authority original source is unavailable")
        try:
            original: dict[str, Any] = json.loads(row.source_json)
            current = _basis(upload)
            immutable = (
                "construction_sha256",
                "archive_generation",
                "archive_incarnation_id",
                "archive_storage_prefix",
                "artifact_set_sha256",
                "artifact_count",
                "delivery_context_id",
            )
            if (
                set(original) != set(current) | {"last_progress_sha256", "prior_progress_sha256"}
                or _json(original) != row.source_json
                or row.source_sha256 != _sha(row.source_json)
                or any(original[k] != current[k] for k in immutable)
                or original["last_progress_sha256"]
                != _tail_sha(session, collection_id, original["next_sequence"])
                or original["prior_progress_sha256"]
                != _prior_progress_sha(session, collection_id, role)
            ):
                raise Conflict("provenance authority original construction changed")
            if role == "root":
                terminal = cls.load(session, collection_id, role="terminal")
                terminal_row = session.get(Authority, (collection_id, "terminal"))
                assert terminal_row is not None
                progress = terminal._progress_value(session, terminal_row)
                if terminal_row.progress_receipt_json != _json(progress) or original != {
                    **json.loads(terminal.source_json),
                    **progress["projection"],
                    "prior_progress_sha256": _sha(terminal_row.progress_receipt_json or ""),
                }:
                    raise Conflict("provenance root original terminal progress changed")
            publication = _publication(role, original)
            if publication.plaintext.decode() != row.document_json:
                raise Conflict("provenance authority original document changed")
        except (KeyError, TypeError, ValueError) as exc:
            raise Conflict("provenance authority original source is invalid") from exc
        return cls(collection_id, role, row.source_json, row.document_json, publication)

    def _source(self, session: Session) -> tuple[Upload, Authority]:
        current = self.load(session, self.collection_id, role=self.role)
        if current != self:
            raise Conflict("provenance authority original source changed")
        upload = _upload(session, self.collection_id)
        row = session.get(Authority, (self.collection_id, self.role))
        assert row is not None
        return upload, row

    @property
    def reserved_source_bytes(self) -> int:
        return (
            len(str(self.collection_id).encode())
            + len(self.role.encode())
            + len(self.document_json.encode())
            + len(self.source_json.encode())
            + 64
        )

    def write_grants(self, grants: ArchiveWriteGrants) -> ArchiveWriteGrants:
        remaining = grants.metadata_preparation_bytes - self.reserved_source_bytes
        if remaining < 1:
            raise Conflict("provenance authority source exceeds its preparation grant")
        return replace(grants, metadata_preparation_bytes=remaining)

    def guard(self, session: Session) -> None:
        upload, row = self._source(session)
        _claim(upload)
        ArchiveWriteAdmission.require_no_unresolved_calls(session, self.collection_id)
        if self.source_json != _json(_context(session, upload, self.role)):
            raise Conflict("provenance authority original cursor changed")
        if row.receipt_json is not None:
            raise Conflict("provenance authority original receipt is already received")
        if (
            upload.provenance_archive_root_receipt_json is not None
            or upload.provenance_identity is not None
        ):
            raise Conflict("provenance authority root progress is already committed")

    def validate_metadata(
        self, session: Session, upload: Upload, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id:
            raise Conflict("provenance authority belongs to another construction")
        self._source(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict("provenance authority request differs from its source") from exc

    def _require_prepared(self, prepared: PreparedImmutableArchiveObject) -> None:
        original = json.loads(self.source_json)
        if (
            prepared.collection_id != self.collection_id
            or prepared.object_id
            != ImmutableArchivePreparation.object_id(self.publication.object_path)
            or prepared._value()["construction_sha256"] != original["construction_sha256"]
            or prepared._value()["target_incarnation"] != original["archive_incarnation_id"]
        ):
            raise Conflict("provenance authority preparation belongs to another construction")
        try:
            self.publication.require_request(prepared.operation.request)
        except ValueError as exc:
            raise Conflict("provenance authority preparation differs from its source") from exc

    @classmethod
    def from_prepared(
        cls, session: Session, prepared: PreparedImmutableArchiveObject, *, role: Role
    ) -> ArchiveProvenanceAuthoritySource:
        source = cls.load(session, prepared.collection_id, role=role)
        source._require_prepared(prepared)
        return source

    def _projection(self, receipt: SealedProvenanceObject) -> str:
        if (
            receipt.object_id != self.publication.object_id
            or receipt.kind != self.publication.kind
            or receipt.relative_path != self.publication.relative_path
            or receipt.plaintext_bytes != len(self.publication.plaintext)
            or receipt.plaintext_sha256 != self.publication.plaintext_sha256
        ):
            raise Conflict("provenance authority receipt differs from its source")
        return _json(asdict(receipt))

    def received(self, session: Session) -> SealedProvenanceObject | None:
        _, row = self._source(session)
        if row.receipt_json is None:
            return None
        try:
            receipt = SealedProvenanceObject(**json.loads(row.receipt_json))
            if self._projection(receipt) != row.receipt_json:
                raise Conflict("provenance authority original receiving proof changed")
        except (TypeError, ValueError) as exc:
            raise Conflict("provenance authority original receiving proof is invalid") from exc
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
        _, row = self._source(session)
        require_immutable_archive_receipt(prepared.operation.request, receipt)
        projected = self._projection(
            self.publication.sealed_receipt(
                ImmutableObjectReceipt(
                    object_path=receipt.object_path,
                    revision=receipt.revision,
                    entity_token=receipt.entity_token,
                    stored_bytes=receipt.stored_bytes,
                    stored_sha256=receipt.stored_sha256,
                    completed_at=receipt.completed_at,
                )
            )
        )
        if row.receipt_json is None:
            if not apply:
                raise Conflict("provenance authority original receipt is not received")
            row.receipt_json = projected
        elif row.receipt_json != projected:
            raise Conflict("provenance authority original receipt changed")
        # The cold drainer verifies in the same transaction through fresh row
        # reads. Persist this owner's fact before that independent verification.
        session.flush()

    def apply_prepared_receipt(
        self, session: Session, prepared: PreparedImmutableArchiveObject, receipt: AdapterReceipt
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=True)

    def verify_prepared_receipt(
        self, session: Session, prepared: PreparedImmutableArchiveObject, receipt: AdapterReceipt
    ) -> None:
        self._receive_prepared(session, prepared, receipt, apply=False)

    def _progress_value(self, session: Session, row: Authority) -> dict[str, Any]:
        receipt = self.received(session)
        if receipt is None:
            raise Conflict("provenance authority original physical receipt is unavailable")
        original = json.loads(self.source_json)
        projection: dict[str, Any]
        if self.role == "terminal":
            digest = CheckpointSHA256.from_state(original["hash_state"])
            update_provenance_commitment(
                digest, ProvenanceTerminalDocument.from_json_bytes(self.publication.plaintext)
            )
            projection = {
                "terminal_receipt_json": self._projection(receipt),
                # Keep the exact committed volume-prefix checkpoint available
                # for late volume acknowledgements; terminal commitment is separate.
                "ordered_sha256": digest.hexdigest(),
                "hash_state": original["hash_state"],
            }
        else:
            projection = {
                "identity": self.publication.plaintext_sha256,
                "root_receipt_json": _json(
                    {
                        "identity": self.publication.plaintext_sha256,
                        "root": asdict(receipt),
                    }
                ),
            }
        assert row.receipt_json is not None
        return {
            "source_sha256": _sha(self.source_json),
            "document_sha256": _sha(self.document_json),
            "receipt_sha256": _sha(row.receipt_json),
            "projection": projection,
        }

    def verify_progress(self, session: Session) -> None:
        upload, row = self._source(session)
        expected = self._progress_value(session, row)
        if row.progress_receipt_json != _json(expected):
            raise Conflict("provenance authority original progress proof changed")
        original = json.loads(self.source_json)
        if self.role == "terminal":
            current = _context(session, upload, self.role)
            if current != {**original, **expected["projection"]}:
                raise Conflict("provenance terminal original progress changed")
        elif (
            _context(session, upload, self.role) != original
            or upload.provenance_identity != expected["projection"]["identity"]
            or upload.provenance_archive_root_receipt_json
            != expected["projection"]["root_receipt_json"]
        ):
            raise Conflict("provenance root original progress changed")

    def commit_progress(self, session: Session) -> bool:
        """Commit new progress under current authority, or acknowledge its exact old fact."""
        _writer(session)
        upload, row = self._source(session)
        if row.progress_receipt_json is not None:
            self.verify_progress(session)
            return False
        _claim(upload)
        ArchiveWriteAdmission.require_no_unresolved_calls(session, self.collection_id)
        if self.source_json != _json(_context(session, upload, self.role)):
            raise Conflict("provenance authority original cursor changed")
        control = ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        if control.gate != "open":
            raise Conflict("provenance authority progress admission is closed")
        expected = self._progress_value(session, row)
        value = expected["projection"]
        if self.role == "terminal":
            upload.provenance_archive_terminal_receipt_json = value["terminal_receipt_json"]
            upload.provenance_archive_ordered_sha256 = value["ordered_sha256"]
            upload.provenance_archive_hash_state = value["hash_state"]
        else:
            self.load(session, self.collection_id, role="terminal").verify_progress(session)
            if (
                upload.provenance_archive_root_receipt_json is not None
                or upload.provenance_identity is not None
            ):
                raise Conflict("provenance root has no original progress proof")
            upload.provenance_identity = value["identity"]
            upload.provenance_archive_root_receipt_json = value["root_receipt_json"]
        row.progress_receipt_json = _json(expected)
        session.flush()
        self.verify_progress(session)
        return True

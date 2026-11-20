"""Original initial tag state, expansion and physical publication checkpoints."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal, cast

from riverhog_protocol import (
    CollectionTagHeadDocument,
    CollectionTagSetRoot,
    collection_tag_node_digest,
    decode_collection_tag_node,
)
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import ImmutableObjectReceipt as AdapterReceipt
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.archive_collection_metadata import ArchiveCollectionMetadataPublication
from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_initial_metadata_models import (
    CollectionUploadInitialTagPublicationRecord as Tag,
)
from riverhog_core.catalog_models import (
    CollectionTagNodeRecord as Node,
)
from riverhog_core.catalog_models import (
    CollectionUploadRecord as Upload,
)
from riverhog_core.catalog_models import (
    CollectionUploadTagPublicationFrontierRecord as Frontier,
)
from riverhog_core.immutable_archive_identity import require_immutable_archive_receipt
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.ports.archive_store import CollectionTagObjectReceipt
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
from riverhog_core.services.collection_upload_tag_frontier import pending_initial_tag_query
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)

Role = Literal["state", "expand", "node", "head"]


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _key(role: Role, digest: str | None = None) -> str:
    if role in {"state", "head"}:
        return "tag-" + role
    if role not in {"expand", "node"} or digest is None:
        raise Conflict("initial tag source role is invalid")
    return "tag-" + role + "-" + digest


def _head(original: dict[str, Any]) -> CollectionTagHeadDocument:
    expected = CollectionTagSetRoot.seal(original["tag_staging_root_sha256"])
    if expected.tag_set_identity != original["tag_staging_set_identity"]:
        raise Conflict("initial tag original staged identity changed")
    return CollectionTagHeadDocument.seal(
        archive_root_sha256=original["archive_root_sha256"],
        revision=1,
        root_sha256=original["tag_staging_root_sha256"],
    )


def _head_fields(head: CollectionTagHeadDocument) -> dict[str, Any]:
    return {
        "tag_revision": head.revision,
        "tag_root_sha256": head.root_sha256,
        "tag_set_identity": head.tag_set_identity,
        "tag_head_identity": head.head_identity,
    }


def _node(session: Session, digest: str) -> bytes:
    row = session.get(Node, digest, populate_existing=True)
    if row is None:
        raise Conflict("initial tag original node is unavailable")
    decode_collection_tag_node(row.encoded)
    if collection_tag_node_digest(row.encoded) != digest:
        raise Conflict("initial tag original node digest changed")
    return row.encoded


def _initial_context(session: Session, upload: Upload) -> dict[str, Any]:
    original = {
        **current_initial_metadata_basis(session, upload),
        "role": "state",
        "object_id": _key("state"),
        "tag_staging_root_sha256": upload.tag_staging_root_sha256,
        "tag_staging_set_identity": upload.tag_staging_set_identity,
    }
    head = _head(original)
    if head.tag_set_identity != upload.initial_tag_set_identity:
        raise Conflict("initial tag accepted construction identity changed")
    return original


def _any_frontier(session: Session, collection_id: int) -> bool:
    return (
        session.scalar(
            select(Frontier.node_digest).where(Frontier.collection_id == collection_id).limit(1)
        )
        is not None
    )


@dataclass(frozen=True, slots=True)
class ArchiveInitialTagSource:
    """Original facts grant no fresh dispatch, current claim or global quiescence."""

    collection_id: int
    object_id: str
    role: Role
    source_json: str = field(repr=False)
    document_bytes: bytes = field(repr=False)
    _publication: ArchiveCollectionMetadataPublication | None = field(repr=False)
    forward_guard: Guard | None = field(default=None, repr=False, compare=False)

    @property
    def has_publication(self) -> bool:
        return self._publication is not None

    @property
    def publication(self) -> ArchiveCollectionMetadataPublication:
        if self._publication is None:
            raise Conflict("logical initial tag step has no provider write")
        return self._publication

    @classmethod
    def _context(
        cls, session: Session, upload: Upload, role: Role, digest: str | None
    ) -> dict[str, Any]:
        current = _initial_context(session, upload)
        if role == "state":
            return current
        state = cls.load(session, upload.collection_id, _key("state"))
        if state.source_json != _json(current):
            raise Conflict("initial tag original selection changed")
        state.verify_progress(session)
        row = session.get(Tag, (upload.collection_id, state.object_id))
        assert row is not None and row.progress_receipt_json is not None
        result = {
            **current,
            "role": role,
            "object_id": _key(role, digest),
            "initial_progress_sha256": _sha(row.progress_receipt_json),
        }
        if role in {"expand", "node"}:
            result["node_digest"] = digest
        return result

    @classmethod
    def stage_next(
        cls,
        session: Session,
        collection_id: int,
        *,
        grants: ArchiveWriteGrants,
        forward_guard: Guard | None = None,
    ) -> ArchiveInitialTagSource | None:
        _writer(session)
        if forward_guard is not None:
            forward_guard(session)
        upload = _upload(session, collection_id)
        ArchiveWriteAdmission.require_no_unresolved_calls(session, collection_id)
        if upload.tag_publication_receipt_json is not None:
            cls.load(session, collection_id, _key("head")).verify_progress(session)
            return None
        _claim(session, upload)
        control = ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        if control.gate != "open":
            raise Conflict("initial tag admission is closed")
        role: Role = "state"
        digest = None
        if upload.tag_set_identity is not None:
            cls.load(session, collection_id, _key("state")).verify_progress(session)
            frontier = session.scalar(pending_initial_tag_query(collection_id, work="expand"))
            if frontier is not None:
                role, digest = "expand", frontier.node_digest
            else:
                frontier = session.scalar(pending_initial_tag_query(collection_id, work="publish"))
                if frontier is not None:
                    role, digest = "node", frontier.node_digest
                else:
                    role = "head"
        original = cls._context(session, upload, role, digest)
        document = _node(session, digest) if digest is not None else _head(original).to_json_bytes()
        object_id = _key(role, digest)
        row = session.get(Tag, (collection_id, object_id), with_for_update=True)
        if row is None:
            row = Tag(
                collection_id=collection_id,
                object_id=object_id,
                role=role,
                document_bytes=document,
                source_json=_json(original),
                source_sha256=_sha(_json(original)),
            )
            session.add(row)
            session.flush()
        source = cls.load(session, collection_id, object_id, forward_guard=forward_guard)
        if source.source_json != _json(original):
            raise Conflict("initial tag original selection changed")
        source.write_grants(grants)
        return source

    @classmethod
    def load(
        cls,
        session: Session,
        collection_id: int,
        object_id: str,
        *,
        forward_guard: Guard | None = None,
    ) -> ArchiveInitialTagSource:
        upload = _upload(session, collection_id)
        row = session.get(
            Tag, (collection_id, object_id), with_for_update=True, populate_existing=True
        )
        if row is None:
            raise NotFound("initial tag original source is unavailable")
        if row.role not in {"state", "expand", "node", "head"}:
            raise Conflict("initial tag source role is invalid")
        role = cast(Role, row.role)
        try:
            original = json.loads(row.source_json)
            common = {
                "construction_sha256",
                "archive_incarnation_id",
                "archive_storage_prefix",
                "passphrase_id",
                "archive_root_sha256",
                "archive_root_receipt_sha256",
                "role",
                "object_id",
                "tag_staging_root_sha256",
                "tag_staging_set_identity",
            }
            extras = set() if row.role == "state" else {"initial_progress_sha256"}
            if row.role in {"expand", "node"}:
                extras.add("node_digest")
            if (
                set(original) != common | extras
                or _json(original) != row.source_json
                or row.source_sha256 != _sha(row.source_json)
                or row.role != original["role"]
                or row.object_id != original["object_id"]
                or object_id != _key(role, original.get("node_digest"))
            ):
                raise Conflict("initial tag original source changed")
            require_original_metadata_basis(session, upload, original)
            head = _head(original)
            if head.tag_set_identity != upload.initial_tag_set_identity:
                raise Conflict("initial tag original accepted identity changed")
            if row.role != "state":
                state = cls.load(session, collection_id, _key("state"))
                state_row = session.get(Tag, (collection_id, state.object_id))
                assert state_row is not None
                if (
                    state_row.progress_receipt_json != _json(state._progress_value(session))
                    or original["initial_progress_sha256"]
                    != _sha(state_row.progress_receipt_json or "")
                    or {
                        k: v
                        for k, v in original.items()
                        if k not in {"role", "object_id", "initial_progress_sha256", "node_digest"}
                    }
                    != {
                        k: v
                        for k, v in json.loads(state.source_json).items()
                        if k not in {"role", "object_id"}
                    }
                ):
                    raise Conflict("initial tag original initialization proof changed")
            if role in {"expand", "node"}:
                if collection_tag_node_digest(row.document_bytes) != original["node_digest"]:
                    raise Conflict("initial tag original document changed")
                decode_collection_tag_node(row.document_bytes)
            elif row.document_bytes != head.to_json_bytes():
                raise Conflict("initial tag original document changed")
            publication = (
                ArchiveCollectionMetadataPublication(
                    original["archive_storage_prefix"],
                    "tag-node" if row.role == "node" else "tag-head",
                    row.document_bytes,
                    original["passphrase_id"],
                )
                if row.role in {"node", "head"}
                else None
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise Conflict("initial tag original source is invalid") from exc
        return cls(
            collection_id,
            object_id,
            role,
            row.source_json,
            row.document_bytes,
            publication,
            forward_guard,
        )

    def _source(self, session: Session) -> tuple[Upload, Tag]:
        if self.load(session, self.collection_id, self.object_id) != self:
            raise Conflict("initial tag original source changed")
        upload = _upload(session, self.collection_id)
        row = session.get(Tag, (self.collection_id, self.object_id))
        assert row is not None
        return upload, row

    @property
    def reserved_source_bytes(self) -> int:
        return (
            len(str(self.collection_id).encode())
            + len(self.object_id.encode())
            + len(self.role.encode())
            + len(self.source_json.encode())
            + len(self.document_bytes)
            + 64
        )

    def write_grants(self, grants: ArchiveWriteGrants) -> ArchiveWriteGrants:
        remaining = grants.metadata_preparation_bytes - self.reserved_source_bytes
        if remaining < 1:
            raise Conflict("initial tag source exceeds its preparation grant")
        return replace(grants, metadata_preparation_bytes=remaining)

    def _fresh(self, session: Session, upload: Upload) -> None:
        if self.forward_guard is not None:
            self.forward_guard(session)
        _claim(session, upload)
        original = json.loads(self.source_json)
        if self.source_json != _json(
            self._context(session, upload, self.role, original.get("node_digest"))
        ):
            raise Conflict("initial tag original selection changed")
        if upload.tag_publication_receipt_json is not None:
            raise Conflict("initial tag head progress is already received")
        if self.role == "state":
            if upload.tag_set_identity is not None or _any_frontier(session, self.collection_id):
                raise Conflict("initial tag state has no original progress proof")
        elif self.role in {"expand", "node"}:
            frontier = session.get(
                Frontier, (self.collection_id, original["node_digest"]), populate_existing=True
            )
            if frontier is None or frontier.published or frontier.expanded != (self.role == "node"):
                raise Conflict("initial tag original frontier changed")
        elif self.role == "head":
            if any(
                session.scalar(pending_initial_tag_query(self.collection_id, work=work)) is not None
                for work in ("expand", "publish")
            ):
                raise Conflict("initial tag frontier is not completely published")
        else:
            raise Conflict("initial tag source role is invalid")

    def guard(self, session: Session) -> None:
        upload, row = self._source(session)
        self._fresh(session, upload)
        if row.receipt_json is not None or row.progress_receipt_json is not None:
            raise Conflict("initial tag original receiving is already recorded")
        if not self.has_publication:
            raise Conflict("logical initial tag step has no provider write")

    def validate_metadata(
        self, session: Session, upload: Upload, operation: PutObjectOperation
    ) -> None:
        if upload.collection_id != self.collection_id or not self.has_publication:
            raise Conflict("initial tag source does not own this publication")
        self._source(session)
        try:
            self.publication.require_request(operation.request)
        except ValueError as exc:
            raise Conflict("initial tag request differs from its source") from exc

    def _require_prepared(self, prepared: PreparedImmutableArchiveObject) -> None:
        original = json.loads(self.source_json)
        if (
            not self.has_publication
            or prepared.collection_id != self.collection_id
            or prepared.object_id
            != ImmutableArchivePreparation.object_id(self.publication.object_path)
            or prepared._value()["construction_sha256"] != original["construction_sha256"]
            or prepared._value()["target_incarnation"] != original["archive_incarnation_id"]
        ):
            raise Conflict("initial tag preparation belongs to another source")
        try:
            self.publication.require_request(prepared.operation.request)
        except ValueError as exc:
            raise Conflict("initial tag preparation differs from its source") from exc

    @classmethod
    def from_prepared(
        cls,
        session: Session,
        prepared: PreparedImmutableArchiveObject,
        *,
        role: Literal["node", "head"],
    ) -> ArchiveInitialTagSource:
        digest = (
            prepared.operation.request.required_identity_assertions.get("riverhog-tag-node-sha256")
            if role == "node"
            else None
        )
        source = cls.load(session, prepared.collection_id, _key(role, digest))
        source._require_prepared(prepared)
        return source

    def _projection(self, receipt: CollectionTagObjectReceipt) -> str:
        if (
            not self.has_publication
            or receipt.object_path != self.publication.object_path
            or receipt.stored_bytes <= 0
            or not receipt.stored_sha256
        ):
            raise Conflict("initial tag receipt differs from its source")
        return _json(asdict(receipt))

    def received(self, session: Session) -> CollectionTagObjectReceipt | None:
        _, row = self._source(session)
        if row.receipt_json is None:
            return None
        try:
            receipt = CollectionTagObjectReceipt(**json.loads(row.receipt_json))
            if self._projection(receipt) != row.receipt_json:
                raise Conflict("initial tag original receiving proof changed")
            return receipt
        except (TypeError, ValueError) as exc:
            raise Conflict("initial tag original receiving proof is invalid") from exc

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
        assert isinstance(projected, CollectionTagObjectReceipt)
        encoded = self._projection(projected)
        _, row = self._source(session)
        if row.receipt_json is None:
            if not apply:
                raise Conflict("initial tag receipt is not durably received")
            row.receipt_json = encoded
        elif row.receipt_json != encoded:
            raise Conflict("initial tag original receipt changed")
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
        if self.role == "state":
            projection = _head_fields(_head(original))
        elif self.role == "expand":
            node = decode_collection_tag_node(self.document_bytes)
            projection = {
                "node_digest": original["node_digest"],
                "children": [child.digest for child in node.children],
            }
        else:
            receipt = self.received(session)
            if receipt is None:
                raise Conflict("initial tag original physical receipt is unavailable")
            physical = {
                "object_path": receipt.object_path,
                "provider_revision": receipt.revision,
                "stored_bytes": receipt.stored_bytes,
                "stored_sha256": receipt.stored_sha256,
                "published_at": receipt.published_at,
            }
            if self.role == "node":
                projection = {
                    "node_digest": original["node_digest"],
                    "frontier": {"published": True, **physical},
                }
            else:
                projection = {
                    **_head_fields(_head(original)),
                    "tag_publication_receipt_json": _json(physical),
                }
        return {"source_sha256": _sha(self.source_json), "projection": projection}

    def verify_progress(self, session: Session) -> None:
        upload, row = self._source(session)
        if row.progress_receipt_json is None:
            raise Conflict("initial tag original progress proof is missing")
        expected = self._progress_value(session)
        projection = expected["projection"]
        valid = row.progress_receipt_json == _json(expected)
        if self.role in {"state", "head"}:
            valid = valid and all(getattr(upload, k) == v for k, v in projection.items())
            root = json.loads(self.source_json)["tag_staging_root_sha256"]
            if root is None:
                valid = valid and not _any_frontier(session, self.collection_id)
            else:
                valid = (
                    valid
                    and session.scalar(
                        select(Frontier.node_digest).where(
                            Frontier.collection_id == self.collection_id,
                            Frontier.node_digest == root,
                        )
                    )
                    is not None
                )
        else:
            frontier = session.get(
                Frontier, (self.collection_id, projection["node_digest"]), populate_existing=True
            )
            valid = valid and frontier is not None
            if frontier is not None and self.role == "node":
                valid = valid and all(
                    getattr(frontier, k) == v for k, v in projection["frontier"].items()
                )
            elif frontier is not None:
                valid = valid and frontier.expanded
                for digest in projection["children"]:
                    valid = (
                        valid
                        and session.scalar(
                            select(Frontier.node_digest).where(
                                Frontier.collection_id == self.collection_id,
                                Frontier.node_digest == digest,
                            )
                        )
                        is not None
                    )
        if not valid:
            raise Conflict("initial tag original progress proof changed")

    def commit_progress(self, session: Session) -> bool:
        _writer(session)
        upload, row = self._source(session)
        if row.progress_receipt_json is not None:
            self.verify_progress(session)
            return False
        self._fresh(session, upload)
        control = ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        if control.gate != "open":
            raise Conflict("initial tag progress admission is closed")
        expected = self._progress_value(session)
        projection = expected["projection"]
        if self.role in {"state", "head"}:
            for k, v in projection.items():
                setattr(upload, k, v)
            if self.role == "state" and upload.tag_root_sha256 is not None:
                session.add(
                    Frontier(collection_id=self.collection_id, node_digest=upload.tag_root_sha256)
                )
        elif self.role == "expand":
            for digest in projection["children"]:
                if session.scalar(select(Node.digest).where(Node.digest == digest)) is None:
                    raise Conflict("initial tag original child node is unavailable")
                if (
                    session.scalar(
                        select(Frontier.node_digest).where(
                            Frontier.collection_id == self.collection_id,
                            Frontier.node_digest == digest,
                        )
                    )
                    is None
                ):
                    session.add(Frontier(collection_id=self.collection_id, node_digest=digest))
            frontier = session.get(Frontier, (self.collection_id, projection["node_digest"]))
            assert frontier is not None
            frontier.expanded = True
        else:
            frontier = session.get(Frontier, (self.collection_id, projection["node_digest"]))
            for k, v in projection["frontier"].items():
                setattr(frontier, k, v)
        row.progress_receipt_json = _json(expected)
        session.flush()
        self.verify_progress(session)
        return True

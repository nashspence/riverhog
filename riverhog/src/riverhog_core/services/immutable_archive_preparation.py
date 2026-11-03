"""Retain original opaque metadata bytes under their archive construction."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy.orm import Session

from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord,
    ArchiveWritePreparationRecord,
)
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.immutable_archive_identity import immutable_archive_identity
from riverhog_core.services.archive_write_admission import (
    ArchiveWriteAdmission,
    Guard,
    _json,
    _sha,
    _writer,
)
from riverhog_core.storage_incarnations import require_storage_incarnation

ValidateMetadata = Callable[[Session, CollectionUploadRecord, PutObjectOperation], None]


@dataclass(frozen=True, slots=True)
class PreparedImmutableArchiveObject:
    collection_id: int
    object_id: str
    epoch: str
    preparation_json: str

    def _value(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self.preparation_json)
        if (
            value["collection_id"] != self.collection_id
            or value["object_id"] != self.object_id
            or value["epoch"] != self.epoch
            or type(value["capacity_bytes"]) is not int
            or value["capacity_bytes"] < 1
            or len(self.preparation_json.encode()) > value["capacity_bytes"]
        ):
            raise Conflict("immutable archive original preparation changed")
        return value

    @property
    def operation(self) -> PutObjectOperation:
        return PutObjectOperation.model_validate(self._value()["operation"])

    @property
    def content(self) -> bytes:
        value = self._value()
        content = base64.b64decode(value["content_base64"], validate=True)
        operation = PutObjectOperation.model_validate(value["operation"])
        if (
            len(content) != operation.request.stored_bytes
            or hashlib.sha256(content).hexdigest() != operation.request.stored_sha256
        ):
            raise Conflict("immutable archive original ciphertext commitment changed")
        return content


class ImmutableArchivePreparation:
    """Receive exact encrypted bytes before any original provider dispatch.

    Stored preparations are immutable facts. Reading one grants neither current
    mutation authority nor a live provider dispatch capability.
    """

    def __init__(self, factory: SessionFactory) -> None:
        self._factory = factory

    @staticmethod
    def object_id(object_path: str) -> str:
        return "immutable-" + hashlib.sha256(object_path.encode()).hexdigest()

    @staticmethod
    def _original(
        collection_id: int,
        object_id: str,
        control: ArchiveWriteControlRecord,
        record: ArchiveWritePreparationRecord,
    ) -> PreparedImmutableArchiveObject:
        prepared = PreparedImmutableArchiveObject(
            collection_id,
            object_id,
            json.loads(record.preparation_json)["epoch"],
            record.preparation_json,
        )
        value = prepared._value()
        operation = prepared.operation
        if (
            len(record.identity_json.encode()) + len(record.preparation_json.encode())
            > value["capacity_bytes"]
            or value["construction_sha256"] != _sha(control.construction_json)
            or value["target_incarnation"]
            != json.loads(control.construction_json)["archive_incarnation_id"]
            or record.identity_json != _json(immutable_archive_identity(operation.request))
            or object_id != ImmutableArchivePreparation.object_id(operation.request.object_path)
        ):
            raise Conflict("immutable archive original construction or identity changed")
        # Validate the retained opaque payload without trusting a caller's copy.
        _ = prepared.content
        return prepared

    def prepare(
        self,
        collection_id: int,
        *,
        operation: PutObjectOperation,
        content: bytes,
        epoch: str,
        grants: ArchiveWriteGrants,
        guard: Guard,
        validate_metadata: ValidateMetadata,
    ) -> PreparedImmutableArchiveObject:
        # Retain an independent validated snapshot before permission or catalog
        # waits. Neither the caller nor its validation callback can edit it.
        operation = PutObjectOperation.model_validate_json(operation.model_dump_json())
        if operation.request.mode != "create_only" or (
            operation.request.expected_current_stored_sha256 is not None
        ):
            raise Conflict("immutable archive preparation requires create-only metadata")
        if (
            not isinstance(content, bytes)
            or len(content) != operation.request.stored_bytes
            or hashlib.sha256(content).hexdigest() != operation.request.stored_sha256
        ):
            raise Conflict("immutable archive ciphertext differs from its original request")
        object_id = self.object_id(operation.request.object_path)
        identity_json = _json(immutable_archive_identity(operation.request))
        with session_scope(self._factory) as session:
            _writer(session)
            guard(session)
            upload = ArchiveWriteAdmission._upload(session, collection_id)
            control = ArchiveWriteAdmission._control(session, upload)
            ArchiveWriteAdmission._require_epoch(control, epoch)
            ArchiveWriteAdmission._terminal_lock(session, control)
            if upload.state not in {"open", "closing", "uploading", "finalizing"}:
                raise Conflict("immutable archive construction cannot prepare metadata")
            prefix = upload.archive_storage_prefix
            if not prefix or not operation.request.object_path.startswith(prefix + "/"):
                raise Conflict("immutable archive object is outside its original construction")
            if (
                require_storage_incarnation(session, "archive", upload.archive_store)
                != upload.archive_incarnation_id
            ):
                raise Conflict("immutable archive original target incarnation changed")
            record = session.get(
                ArchiveWritePreparationRecord,
                (collection_id, object_id),
                with_for_update=True,
                populate_existing=True,
            )
            if record is not None:
                if record.identity_json != identity_json:
                    raise Conflict("immutable archive prepared metadata identity changed")
                prepared = self._original(collection_id, object_id, control, record)
                validate_metadata(session, upload, prepared.operation)
                return prepared
            validate_metadata(session, upload, operation.model_copy(deep=True))
            # This is a per-object preparation grant, independent of total
            # collection extent and the bounded retained provider-call grants.
            if len(content) > grants.preparation_bytes:
                raise Conflict("immutable archive preparation exceeds its byte grant")
            encoded = _json(
                {
                    "collection_id": collection_id,
                    "object_id": object_id,
                    "epoch": control.epoch,
                    "construction_sha256": _sha(control.construction_json),
                    "target_incarnation": upload.archive_incarnation_id,
                    "capacity_bytes": grants.preparation_bytes,
                    "operation": operation.model_dump(mode="json"),
                    "content_base64": base64.b64encode(content).decode("ascii"),
                }
            )
            if len(identity_json.encode()) + len(encoded.encode()) > grants.preparation_bytes:
                raise Conflict("immutable archive preparation exceeds its byte grant")
            record = ArchiveWritePreparationRecord(
                collection_id=collection_id,
                object_id=object_id,
                identity_json=identity_json,
                preparation_json=encoded,
            )
            session.add(record)
            return PreparedImmutableArchiveObject(collection_id, object_id, control.epoch, encoded)

    def load(
        self, collection_id: int, object_path: str, *, epoch: str
    ) -> PreparedImmutableArchiveObject | None:
        """Read original facts after actor loss, without recreating dispatch rights."""
        object_id = self.object_id(object_path)
        with session_scope(self._factory) as session:
            control = session.get(ArchiveWriteControlRecord, collection_id)
            if control is None:
                raise NotFound("immutable archive original construction is unavailable")
            record = session.get(ArchiveWritePreparationRecord, (collection_id, object_id))
            if record is None:
                return None
            original = self._original(collection_id, object_id, control, record)
            if original.epoch != epoch:
                raise Conflict("immutable archive original preparation epoch changed")
            return original

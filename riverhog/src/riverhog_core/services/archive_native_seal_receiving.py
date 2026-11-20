"""Retain one completed native checkpoint through exact logical seal receiving."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from threading import Lock
from typing import Protocol

from riverhog_protocol.errors import Conflict
from sqlalchemy import LargeBinary, Select, cast, exists, func, literal, literal_column, or_, select
from sqlalchemy.orm import InstrumentedAttribute, Session
from sqlalchemy.sql.elements import ColumnElement
from state_schema import read_snapshot

from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.domain.archive import (
    PackVolumePlan,
    RawVolumePlan,
    SealedPackVolume,
    SealedRawVolume,
)
from riverhog_core.pack_upload import PackUploadCheckpoint, PackVolumeUploader
from riverhog_core.pack_volume import parse_pack_volume_plan
from riverhog_core.ports.archive_objects import ArchiveResumableObjectStore
from riverhog_core.raw_upload import RawUploadCheckpoint, RawVolumeUploader
from riverhog_core.raw_volume import parse_raw_volume_plan
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)
from riverhog_core.services.collection_upload_authority import require_archive_write_control


def native_seal_receipts_query(
    collection_id: int, *, limit: int
) -> Select[tuple[str, str, str, str | None]]:
    """Select pending native seal facts without revisiting other volume history."""
    if type(limit) is not int or limit < 1:
        raise ValueError("native seal receiving limit must be positive")
    return (
        select(
            CollectionArchiveObjectUploadRecord.object_id,
            CollectionArchiveObjectUploadRecord.kind,
            CollectionArchiveObjectUploadRecord.plan_json,
            CollectionArchiveObjectUploadRecord.checkpoint_json,
        )
        .where(
            CollectionArchiveObjectUploadRecord.collection_id == collection_id,
            CollectionArchiveObjectUploadRecord.state == literal_column("'sealed'"),
            CollectionArchiveObjectUploadRecord.sealed_receipt_json.is_(None),
        )
        .order_by(CollectionArchiveObjectUploadRecord.sequence)
        .limit(limit)
        .execution_options(include_unpublished_registration=True)
    )


@dataclass(frozen=True, slots=True)
class NativeArchiveSealSource:
    collection_id: int
    construction_json: str
    object_id: str
    kind: str
    sequence: int
    object_path: str
    relative_path: str
    plan_sha256: str
    plan_json: str
    checkpoint_json: str
    reserved_bytes: int

    @classmethod
    def load(
        cls, factory: SessionFactory, collection_id: int, grants: ArchiveWriteGrants
    ) -> NativeArchiveSealSource | None:
        with read_snapshot(factory) as session:
            parent = session.execute(
                select(
                    CollectionUploadRecord.collection_id,
                    exists(
                        select(ArchiveWriteControlRecord.collection_id).where(
                            ArchiveWriteControlRecord.collection_id == collection_id
                        )
                    ),
                ).where(CollectionUploadRecord.collection_id == collection_id)
            ).one_or_none()
            if parent is None:
                return None
            if not parent[1]:
                raise Conflict("native seal construction control is unavailable")
            volume = CollectionArchiveObjectUploadRecord
            control = ArchiveWriteControlRecord
            strings = (
                control.construction_json,
                volume.object_id,
                volume.kind,
                volume.object_path,
                volume.relative_path,
                volume.plan_sha256,
                volume.plan_json,
                volume.checkpoint_json,
            )

            def encoded_length(
                column: InstrumentedAttribute[str] | InstrumentedAttribute[str | None],
            ) -> ColumnElement[int]:
                # SQLite's native BLOB length avoids materializing the value in
                # its application-defined octet_length callback before admission.
                if session.get_bind().dialect.name == "sqlite":
                    return func.length(cast(column, LargeBinary))
                return func.octet_length(column)

            query = native_seal_receipts_query(collection_id, limit=1).join(
                control, control.collection_id == volume.collection_id
            )
            size: ColumnElement[int] = literal(0)
            for column in strings:
                size = size + func.coalesce(encoded_length(column), 0)
            selected = session.execute(
                query.with_only_columns(volume.object_id, size)
            ).one_or_none()
            if selected is None:
                return None
            object_id, source_bytes = selected
            # Reserve the encoded source plus parse/receipt serialization overlap.
            # Escaping non-ASCII receipt strings can expand their UTF-8 source;
            # eight source copies cover that expansion and fixed receipt fields.
            # This is serialized control accounting, not a whole-process heap cap.
            reserved = 8 * source_bytes
            if reserved > grants.native_receipt_bytes:
                raise Conflict("native seal source exceeds its configured receiving byte grant")
            row = session.execute(
                select(*strings, volume.sequence)
                .select_from(volume)
                .join(control, control.collection_id == volume.collection_id)
                .where(volume.collection_id == collection_id, volume.object_id == object_id)
                .execution_options(include_unpublished_registration=True)
            ).one()
            construction, selected_id, kind, path, relative, digest, plan, checkpoint, sequence = (
                row
            )
            if checkpoint is None:
                raise Conflict("sealed native volume has no completed checkpoint")
            return cls(
                collection_id,
                construction,
                selected_id,
                kind,
                sequence,
                path,
                relative,
                digest,
                plan,
                checkpoint,
                reserved,
            )

    def require_original(
        self,
        session: Session,
        upload: CollectionUploadRecord,
        encoded_receipt: str,
    ) -> None:
        volume = CollectionArchiveObjectUploadRecord
        # Compare bounded original preimages in SQL before materializing any
        # replacement plan, checkpoint or receipt. The caller holds the parent.
        match = session.scalar(
            select(volume.object_id)
            .join(
                ArchiveWriteControlRecord,
                ArchiveWriteControlRecord.collection_id == volume.collection_id,
            )
            .where(
                volume.collection_id == self.collection_id,
                volume.object_id == self.object_id,
                volume.kind == self.kind,
                volume.sequence == self.sequence,
                volume.object_path == self.object_path,
                volume.relative_path == self.relative_path,
                volume.plan_sha256 == self.plan_sha256,
                volume.plan_json == self.plan_json,
                volume.checkpoint_json == self.checkpoint_json,
                volume.state == "sealed",
                or_(
                    volume.sealed_receipt_json.is_(None),
                    volume.sealed_receipt_json == encoded_receipt,
                ),
                ArchiveWriteControlRecord.construction_json == self.construction_json,
            )
            .execution_options(include_unpublished_registration=True)
        )
        if upload.collection_id != self.collection_id or match != self.object_id:
            raise Conflict("original native seal source changed")
        require_archive_write_control(session, upload)

    def require_receipt_identity(
        self, collection_id: int, receipt: SealedPackVolume | SealedRawVolume
    ) -> None:
        kind = "pack" if isinstance(receipt, SealedPackVolume) else "segment"
        if (
            collection_id != self.collection_id
            or receipt.volume_id != self.object_id
            or receipt.sequence != self.sequence
            or receipt.relative_path != self.relative_path
            or kind != self.kind
        ):
            raise Conflict("native seal receipt differs from its original source")

    def require_receipt_capacity(self, encoded: str) -> None:
        if len(encoded.encode()) > self.reserved_bytes - self.reserved_bytes // 8:
            raise Conflict("native seal receipt exceeds its original receiving byte reservation")

    def checkpoint(self) -> PackUploadCheckpoint | RawUploadCheckpoint:
        plan: PackVolumePlan | RawVolumePlan
        if self.kind == "pack":
            checkpoint: PackUploadCheckpoint | RawUploadCheckpoint = PackUploadCheckpoint.from_json(
                self.checkpoint_json
            )
            pack_plan = parse_pack_volume_plan(self.plan_json)
            plan = pack_plan
            digest = pack_plan.plan_sha256
        elif self.kind == "segment":
            checkpoint = RawUploadCheckpoint.from_json(self.checkpoint_json)
            plan = parse_raw_volume_plan(self.plan_json)
            digest = hashlib.sha256(self.plan_json.encode()).hexdigest()
        else:
            raise Conflict("unsupported native archive volume kind")
        if (
            checkpoint.collection_id != self.collection_id
            or checkpoint.volume_id != self.object_id
            or checkpoint.object_path != self.object_path
            or checkpoint.relative_path != self.relative_path
            or plan.volume_id != self.object_id
            or plan.sequence != self.sequence
            or digest != self.plan_sha256
            or checkpoint.completed is None
        ):
            raise Conflict("completed native checkpoint differs from its selected source")
        return checkpoint


class NativeArchiveSealService(Protocol):
    _session_factory: SessionFactory
    _archive_stores: ArchiveStoreRegistry
    _config: RuntimeConfig

    def _pack_uploader(
        self, object_store: ArchiveResumableObjectStore, *, passphrase_id: str
    ) -> PackVolumeUploader: ...

    def _raw_uploader(
        self, object_store: ArchiveResumableObjectStore, *, passphrase_id: str
    ) -> RawVolumeUploader: ...

    def _record_sealed_volume(
        self,
        collection_id: int,
        receipt: SealedPackVolume | SealedRawVolume,
        *,
        source: NativeArchiveSealSource | None = None,
    ) -> None: ...


class OriginalNativeArchiveSeal:
    """A lightweight original; construction precedes key/format callbacks.

    One indexed positive checkpoint is selected within a configured byte grant.
    The original source, uploader and validated receipt survive lost replies.
    This owner never dispatches provider work or grants global closure authority.
    """

    def __init__(self, service: NativeArchiveSealService, collection_id: int) -> None:
        self._service = service
        self._collection_id = collection_id
        self._source: NativeArchiveSealSource | None = None
        self._checkpoint: PackUploadCheckpoint | RawUploadCheckpoint | None = None
        self._uploader: PackVolumeUploader | RawVolumeUploader | None = None
        self._construction_failure = False
        self._receipt: SealedPackVolume | SealedRawVolume | None = None
        self._result: bool | None = None
        self._lock = Lock()
        self._terminal = ArchiveFinalizerTerminal(service._session_factory, collection_id)

    @property
    def has_pending(self) -> bool:
        return self._source is not None and self._result is None

    def advance(self) -> bool:
        with self._lock:
            self._terminal.require_forward()
            if self._result is not None:
                return self._result
            if self._source is None:
                self._source = NativeArchiveSealSource.load(
                    self._service._session_factory,
                    self._collection_id,
                    self._service._config.archive_write,
                )
                if self._source is None:
                    self._result = False
                    return False
            if self._construction_failure:
                raise Conflict("original native receipt constructor remains unresolved")
            source = self._source
            if self._checkpoint is None:
                self._checkpoint = source.checkpoint()
            if self._uploader is None:
                construction = json.loads(source.construction_json)
                binding = self._service._archive_stores.require_incarnation(
                    construction["archive_store"], construction["archive_incarnation_id"]
                )
                self._construction_failure = True
                if source.kind == "pack":
                    self._uploader = self._service._pack_uploader(
                        binding.resumable_objects, passphrase_id=construction["passphrase_id"]
                    )
                else:
                    self._uploader = self._service._raw_uploader(
                        binding.resumable_objects, passphrase_id=construction["passphrase_id"]
                    )
                self._construction_failure = False
            if self._receipt is None:
                if isinstance(self._uploader, PackVolumeUploader):
                    assert isinstance(self._checkpoint, PackUploadCheckpoint)
                    self._receipt = self._uploader.sealed_receipt(
                        plan=parse_pack_volume_plan(source.plan_json), checkpoint=self._checkpoint
                    )
                else:
                    assert isinstance(self._checkpoint, RawUploadCheckpoint)
                    self._receipt = self._uploader.sealed_receipt(
                        self._checkpoint, plan=parse_raw_volume_plan(source.plan_json)
                    )
            self._receive_original()
            return True

    def _receive_original(self) -> None:
        assert self._source is not None and self._receipt is not None
        self._service._record_sealed_volume(self._collection_id, self._receipt, source=self._source)
        self._result = True

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            self._terminal.drain_original(operation_id, None)
            if self.has_pending:
                if self._receipt is None:
                    return "pending"
                self._receive_original()
            return "closed"
        finally:
            self._lock.release()

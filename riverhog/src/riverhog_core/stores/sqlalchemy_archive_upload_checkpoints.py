from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session, load_only, object_session

from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import SessionFactory, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
)
from riverhog_core.domain.archive import StoredArchivePart
from riverhog_core.pack_upload import PackUploadCheckpoint, merge_pack_upload_checkpoints
from riverhog_core.raw_upload import RawUploadCheckpoint, merge_raw_upload_checkpoints
from riverhog_core.runtime_config import RuntimeConfig


class _ArchiveUploadCheckpoint(Protocol):
    @property
    def collection_id(self) -> int: ...

    @property
    def volume_id(self) -> str: ...

    @property
    def archive_parts(self) -> Sequence[StoredArchivePart]: ...

    @property
    def completed(self) -> object | None: ...

    def to_json(self) -> str: ...


_CheckpointT = TypeVar("_CheckpointT", bound=_ArchiveUploadCheckpoint)


class SqlAlchemyArchiveUploadCheckpointStore:
    """Persist resumable-write progress in the upload row that owns an immutable volume plan."""

    def __init__(
        self,
        config: RuntimeConfig,
        *,
        session_factory: SessionFactory | None = None,
    ) -> None:
        self._session_factory = session_factory or make_session_factory(config.database_url)

    def load_pack_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> str | None:
        return self._load(collection_id=collection_id, volume_id=volume_id, kind="pack")

    def merge_pack_upload_checkpoint(
        self, *, collection_id: int, volume_id: str, checkpoint_json: str
    ) -> str:
        return self._merge(
            collection_id=collection_id,
            volume_id=volume_id,
            kind="pack",
            checkpoint_json=checkpoint_json,
            parse=PackUploadCheckpoint.from_json,
            merge=merge_pack_upload_checkpoints,
        )

    def delete_pack_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> None:
        self._delete(collection_id=collection_id, volume_id=volume_id, kind="pack")

    def load_raw_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> str | None:
        return self._load(collection_id=collection_id, volume_id=volume_id, kind="segment")

    def merge_raw_upload_checkpoint(
        self, *, collection_id: int, volume_id: str, checkpoint_json: str
    ) -> str:
        return self._merge(
            collection_id=collection_id,
            volume_id=volume_id,
            kind="segment",
            checkpoint_json=checkpoint_json,
            parse=RawUploadCheckpoint.from_json,
            merge=merge_raw_upload_checkpoints,
        )

    def delete_raw_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> None:
        self._delete(collection_id=collection_id, volume_id=volume_id, kind="segment")

    def _load(self, *, collection_id: int, volume_id: str, kind: str) -> str | None:
        with session_scope(self._session_factory) as session:
            record = session.get(
                CollectionArchiveObjectUploadRecord,
                (collection_id, volume_id),
                options=(
                    load_only(
                        CollectionArchiveObjectUploadRecord.kind,
                        CollectionArchiveObjectUploadRecord.checkpoint_json,
                    ),
                ),
            )
            if record is None or record.kind != kind:
                return None
            return record.checkpoint_json

    def apply_pack_upload_checkpoint(
        self,
        *,
        session: Session,
        upload: CollectionUploadRecord,
        record: CollectionArchiveObjectUploadRecord,
        checkpoint_json: str,
    ) -> str:
        return self._apply(
            session=session,
            upload=upload,
            record=record,
            kind="pack",
            incoming=PackUploadCheckpoint.from_json(checkpoint_json),
            parse=PackUploadCheckpoint.from_json,
            merge=merge_pack_upload_checkpoints,
        )

    def apply_raw_upload_checkpoint(
        self,
        *,
        session: Session,
        upload: CollectionUploadRecord,
        record: CollectionArchiveObjectUploadRecord,
        checkpoint_json: str,
    ) -> str:
        return self._apply(
            session=session,
            upload=upload,
            record=record,
            kind="segment",
            incoming=RawUploadCheckpoint.from_json(checkpoint_json),
            parse=RawUploadCheckpoint.from_json,
            merge=merge_raw_upload_checkpoints,
        )

    @staticmethod
    def _lock(
        session: Session,
        *,
        collection_id: int,
        volume_id: str,
    ) -> tuple[CollectionUploadRecord | None, CollectionArchiveObjectUploadRecord | None]:
        if session.get_bind().dialect.name == "sqlite":
            session.connection(execution_options={"sqlite_immediate_writer": True})
        upload = session.scalar(
            select(CollectionUploadRecord)
            .options(load_only(CollectionUploadRecord.collection_id))
            .where(CollectionUploadRecord.collection_id == collection_id)
            .with_for_update()
        )
        if upload is None:
            return None, None
        terminal_id = session.scalar(
            select(ArchiveWriteControlRecord.terminal_id).where(
                ArchiveWriteControlRecord.collection_id == collection_id
            )
        )
        if terminal_id is not None:
            terminal = session.scalar(
                select(ArchiveUploadTerminalRecord.id)
                .where(ArchiveUploadTerminalRecord.id == terminal_id)
                .with_for_update()
            )
            if terminal is None:
                raise RuntimeError("archive upload terminal intent is missing")
        record = session.scalar(
            select(CollectionArchiveObjectUploadRecord)
            .options(
                load_only(
                    CollectionArchiveObjectUploadRecord.collection_id,
                    CollectionArchiveObjectUploadRecord.object_id,
                    CollectionArchiveObjectUploadRecord.kind,
                    CollectionArchiveObjectUploadRecord.checkpoint_json,
                )
            )
            .where(
                CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                CollectionArchiveObjectUploadRecord.object_id == volume_id,
            )
            .with_for_update()
        )
        return upload, record

    def _merge(
        self,
        *,
        collection_id: int,
        volume_id: str,
        kind: str,
        checkpoint_json: str,
        parse: Callable[[str], _CheckpointT],
        merge: Callable[[_CheckpointT, _CheckpointT], _CheckpointT],
    ) -> str:
        incoming = parse(checkpoint_json)
        with session_scope(self._session_factory) as session:
            upload, record = self._lock(session, collection_id=collection_id, volume_id=volume_id)
            if upload is None or record is None:
                raise LookupError(f"archive upload volume not found: {volume_id}")
            return self._apply(
                session=session,
                upload=upload,
                record=record,
                kind=kind,
                incoming=incoming,
                parse=parse,
                merge=merge,
            )

    def _apply(
        self,
        *,
        session: Session,
        upload: CollectionUploadRecord,
        record: CollectionArchiveObjectUploadRecord,
        kind: str,
        incoming: _CheckpointT,
        parse: Callable[[str], _CheckpointT],
        merge: Callable[[_CheckpointT, _CheckpointT], _CheckpointT],
    ) -> str:
        """Apply inside the caller's existing upload/terminal/volume write locks."""
        if (
            object_session(upload) is not session
            or object_session(record) is not session
            or session.get_bind() is not self._session_factory.kw.get("bind")
        ):
            raise ValueError("archive checkpoint application requires its owning catalog session")
        if (
            record.collection_id != upload.collection_id
            or incoming.collection_id != upload.collection_id
            or incoming.volume_id != record.object_id
            or record.kind != kind
        ):
            raise ValueError("archive checkpoint construction or volume identity changed")
        if (
            session.get_bind().dialect.name == "sqlite"
            and not session.connection()
            .get_execution_options()
            .get("sqlite_immediate_writer", False)
        ):
            raise ValueError("archive checkpoint application requires the early writer fence")
        previous = parse(record.checkpoint_json) if record.checkpoint_json else None
        merged = merge(previous or incoming, incoming)
        previous_payload_bytes = (
            sum(item.plaintext_bytes for item in previous.archive_parts)
            if previous is not None
            else 0
        )
        added_payload_bytes = (
            sum(item.plaintext_bytes for item in merged.archive_parts) - previous_payload_bytes
        )
        encoded = merged.to_json()
        record.checkpoint_json = encoded
        record.uploaded_bytes = sum(item.stored_bytes for item in merged.archive_parts)
        record.uploaded_units = len(merged.archive_parts)
        record.state = "sealed" if merged.completed is not None else "uploading"
        if added_payload_bytes:
            upload.uploaded_payload_bytes = (
                CollectionUploadRecord.uploaded_payload_bytes + added_payload_bytes
            )
        return encoded

    def _delete(self, *, collection_id: int, volume_id: str, kind: str) -> None:
        with session_scope(self._session_factory) as session:
            _, record = self._lock(session, collection_id=collection_id, volume_id=volume_id)
            if record is not None and record.kind == kind:
                record.checkpoint_json = None
                record.uploaded_bytes = 0
                record.uploaded_units = 0
                record.state = "planned"

"""Retain one actual payload unit before preparation and provider callbacks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Lock
from typing import Literal, Protocol

from riverhog_age import UploadState
from riverhog_protocol.errors import BadRequest, Conflict, NotFound, ServiceUnavailable
from riverhog_storage_adapter_protocol import WriteExecutionResult, WriteOperation
from sqlalchemy.exc import OperationalError

from riverhog_core.app_permissions import Principal
from riverhog_core.archive_store_registry import ArchiveStoreBinding, ArchiveStoreRegistry
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.domain.archive import (
    PackVolumePlan,
    RawVolumePlan,
    SealedPackVolume,
    SealedRawVolume,
)
from riverhog_core.pack_upload import PackUploadCheckpoint, PackVolumeUploader
from riverhog_core.pack_volume import pack_unit_descriptors, parse_pack_volume_plan
from riverhog_core.ports.archive_objects import ArchiveResumableObjectStore
from riverhog_core.ports.archive_upload_checkpoints import (
    PackUploadCheckpointStore,
    RawUploadCheckpointStore,
)
from riverhog_core.provider_write_dispatch import ProviderWriteAdmissionRefused
from riverhog_core.raw_upload import RawUploadCheckpoint, RawVolumeUploader
from riverhog_core.raw_volume import parse_raw_volume_plan
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_cache_families import (
    ArchivePayloadMirror,
    ArchivePayloadMirrorFactory,
    CacheFamilyNotAdmitted,
)
from riverhog_core.services.archive_payload_input import ArchivePayloadInput
from riverhog_core.services.archive_upload_drain import inspect_terminal_write_records
from riverhog_core.services.archive_upload_execution import ArchiveUploadExecutionKey
from riverhog_core.services.archive_upload_writes import ArchiveUploadWrites
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, Guard
from riverhog_core.services.cache_population_publication import CachePopulationPublication
from riverhog_core.services.mirrored_archive_upload_writes import MirroredArchiveUploadWrites
from riverhog_core.stores.executing_archive_objects import ExecutingArchiveResumableObjectStore
from riverhog_core.stores.sqlalchemy_archive_upload_checkpoints import (
    SqlAlchemyArchiveUploadCheckpointStore,
)
from riverhog_core.upload_continuation import UploadContinuation

type _Checkpoint = PackUploadCheckpoint | RawUploadCheckpoint
type _Writes = ArchiveUploadWrites | MirroredArchiveUploadWrites


class ArchivePayloadService(Protocol):
    """The collection owner's capabilities used by one retained payload unit."""

    _config: RuntimeConfig
    _archive_stores: ArchiveStoreRegistry
    _session_factory: SessionFactory
    _checkpoints: SqlAlchemyArchiveUploadCheckpointStore

    def _payload_write_guard(self, collection_id: int, principal: Principal) -> Guard: ...

    def _pack_uploader(
        self,
        object_store: ArchiveResumableObjectStore,
        *,
        passphrase_id: str,
        checkpoint_store: PackUploadCheckpointStore | None = None,
    ) -> PackVolumeUploader: ...

    def _raw_uploader(
        self,
        object_store: ArchiveResumableObjectStore,
        *,
        passphrase_id: str,
        checkpoint_store: RawUploadCheckpointStore | None = None,
    ) -> RawVolumeUploader: ...

    def _raw_volume_digests(self, collection_id: int, plan: RawVolumePlan) -> tuple[str, ...]: ...

    def _retain_volume_preparation(
        self,
        *,
        admission: ArchiveWriteAdmission,
        collection_id: int,
        volume_id: str,
        epoch: str,
        identity: Mapping[str, str],
        guard: Guard,
        load: Callable[[], UploadState | None],
        prepare: Callable[[], UploadState],
    ) -> UploadState: ...

    def _record_sealed_volume(
        self, collection_id: int, receipt: SealedPackVolume | SealedRawVolume
    ) -> None: ...

    def get_unit(self, collection_id: int, volume_id: str, unit: int) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class _Volume:
    store: str
    passphrase_id: str
    kind: str
    object_path: str
    relative_path: str
    plan_json: str
    unit_plaintext_bytes: int
    use_cache: bool


class _PayloadExecutor:
    """Check current target selection only for new provider operations."""

    def __init__(
        self,
        service: ArchivePayloadService,
        store: str,
        target: str,
        writes: _Writes,
    ) -> None:
        self._service, self._store, self._target, self._writes = service, store, target, writes

    def execute_write_operation(
        self, operation: WriteOperation, *, content: bytes = b""
    ) -> WriteExecutionResult:
        try:
            self._service._archive_stores.require_incarnation(self._store, self._target)
        except (ServiceUnavailable, ValueError) as error:
            raise ProviderWriteAdmissionRefused("current archive target is unavailable") from error
        return self._writes.execute_write_operation(operation, content=content)


class ArchivePayloadUnit:
    """A lightweight original receiver; advance belongs in its physical worker.

    Installation precedes key access, encryption preparation, uploader creation
    and provider work. A preparation candidate is retained before its durable
    acknowledgement. Open, unit, and sealed-receipt receiving each keep their
    exact original state. Local drainage grants no global teardown authority.
    """

    def __init__(
        self,
        service: ArchivePayloadService,
        *,
        key: ArchiveUploadExecutionKey,
        target_incarnation: str,
        volume_id: str,
        unit: int,
        plan_sha256: str,
        payload: ArchivePayloadInput,
        principal: Principal,
        mirror: ArchivePayloadMirrorFactory | None,
    ) -> None:
        self._service = service
        self._key = key
        self._target = target_incarnation
        self._volume_id = volume_id
        self._unit_number = unit
        self._plan_sha256 = plan_sha256
        request = payload.request
        if (
            request.key != key
            or request.target_incarnation != target_incarnation
            or request.volume_id != volume_id
            or request.unit != unit
            or request.plan_sha256 != plan_sha256
            or request.principal != principal
        ):
            raise Conflict("payload actor differs from the original input")
        self._input = payload
        payload.bind_actor(self)
        self._principal = principal
        self._mirror = mirror
        self._mirror_unresolved = False
        self._mirror_original: object | None = None
        self._mirror_binding: ArchivePayloadMirror | None = None
        self._archive: ArchiveUploadWrites | None = None
        self._writes: _Writes | None = None
        self._guard = service._payload_write_guard(key.collection_id, principal)
        self._admission = ArchiveWriteAdmission(service._session_factory)
        self._lock = Lock()
        self._volume: _Volume | None = None
        self._binding: ArchiveStoreBinding | None = None
        self._epoch = payload.request.epoch
        self._epoch_verified = False
        self._pack: PackVolumeUploader | None = None
        self._raw: RawVolumeUploader | None = None
        self._pack_plan: PackVolumePlan | None = None
        self._raw_plan: RawVolumePlan | None = None
        self._digests: tuple[str, ...] = ()
        self._candidate: UploadState | None = None
        self._preparation: UploadState | None = None
        self._opening: (
            UploadContinuation[PackUploadCheckpoint]
            | UploadContinuation[RawUploadCheckpoint]
            | None
        ) = None
        self._uploading: (
            UploadContinuation[PackUploadCheckpoint]
            | UploadContinuation[RawUploadCheckpoint]
            | None
        ) = None
        self._opened: _Checkpoint | None = None
        self._checkpoint: _Checkpoint | None = None
        self._receipt: SealedPackVolume | SealedRawVolume | None = None
        self._receipt_recorded = False
        self._result: dict[str, object] | None = None
        self._terminal_id: str | None = None
        self._locally_closed = False

    @property
    def has_pending(self) -> bool:
        return (
            self._mirror_unresolved
            or self._writes is not None
            and (
                self._mirror_binding.has_pending
                if self._mirror_binding is not None
                else self._writes.has_pending
            )
            or self._receipt is not None
            and not self._receipt_recorded
            or self._result is None
            and not self._locally_closed
        )

    def _select(self) -> _Volume:
        service = self._service
        with session_scope(service._session_factory) as session:
            if session.get_bind().dialect.name == "sqlite":
                session.connection(execution_options={"sqlite_immediate_writer": True})
            self._guard(session)
            upload = session.get(CollectionUploadRecord, self._key.collection_id)
            record = session.get(
                CollectionArchiveObjectUploadRecord,
                (self._key.collection_id, self._volume_id),
            )
            if upload is None or record is None:
                raise NotFound("collection upload volume not found")
            if record.plan_sha256 != self._plan_sha256:
                raise Conflict("archive upload unit plan identity changed")
            if self._unit_number < 0 or self._unit_number >= record.total_units:
                raise BadRequest("archive upload unit number is outside the plan")
            return _Volume(
                upload.archive_store,
                upload.passphrase_id,
                record.kind,
                record.object_path,
                record.relative_path,
                record.plan_json,
                record.unit_plaintext_bytes,
                upload.use_cache,
            )

    def _require_construction(self) -> None:
        with session_scope(self._service._session_factory) as session:
            control = session.get(ArchiveWriteControlRecord, self._key.collection_id)
            if control is None or (
                hashlib.sha256(control.construction_json.encode()).hexdigest()
                != self._key.construction_sha256
                or json.loads(control.construction_json)["archive_incarnation_id"] != self._target
            ):
                raise Conflict("original payload construction or target changed")

    def _initialize(self) -> None:
        if self._mirror_unresolved:
            raise Conflict("original payload cache constructor outcome is unresolved")
        if self._pack is not None or self._raw is not None:
            return
        if self._volume is None:
            self._volume = self._select()
        volume = self._volume
        if not self._epoch_verified:
            if (
                self._admission.bind_epoch(self._key.collection_id, guard=self._guard)
                != self._epoch
            ):
                raise Conflict("payload input admission epoch changed")
            self._epoch_verified = True
        self._require_construction()
        current = self._service._archive_stores.require_incarnation(volume.store, self._target)
        if self._binding is None:
            self._binding = current
        binding = self._binding
        if self._archive is None:
            self._archive = ArchiveUploadWrites(
                self._service._session_factory,
                admission=self._admission,
                checkpoints=self._service._checkpoints,
                executor=binding.require_resumable_writes(),
                collection_id=self._key.collection_id,
                object_id=self._volume_id,
                epoch=self._epoch,
                grants=self._service._config.archive_write,
                guard=self._guard,
            )
        if self._writes is None:
            if volume.use_cache:
                if self._mirror is None:
                    raise Conflict("accepted retrieval cache requires its original write factory")
                self._mirror_unresolved = True
                try:
                    self._mirror_original = self._mirror(self._archive, self._guard)
                except CacheFamilyNotAdmitted:
                    self._mirror_unresolved = False
                    raise
                if not isinstance(self._mirror_original, ArchivePayloadMirror):
                    raise Conflict("original payload cache constructor returned no receiver")
                if self._mirror_original.writes.archive is not self._archive:
                    raise Conflict("payload cache write owner has another original archive")
                self._mirror_binding = self._mirror_original
                self._writes = self._mirror_binding.writes
                self._mirror_unresolved = False
            else:
                if self._mirror is not None:
                    raise Conflict("payload cache factory differs from the accepted selection")
                self._writes = self._archive
        store = ExecutingArchiveResumableObjectStore(
            binding.resumable_objects,
            _PayloadExecutor(self._service, volume.store, self._target, self._writes),
            placement_policy="archive_default",
        )
        content_bytes = self._input.request.plaintext_bytes
        if volume.kind == "pack":
            plan = parse_pack_volume_plan(volume.plan_json)
            if content_bytes != pack_unit_descriptors(plan)[self._unit_number].payload_bytes:
                raise BadRequest("pack upload unit payload length mismatch")
            self._pack_plan = plan
            self._pack = self._service._pack_uploader(
                store, passphrase_id=volume.passphrase_id, checkpoint_store=self._writes
            )
        elif volume.kind == "segment":
            raw_plan = parse_raw_volume_plan(volume.plan_json)
            expected = min(
                volume.unit_plaintext_bytes,
                raw_plan.plaintext_bytes - self._unit_number * volume.unit_plaintext_bytes,
            )
            if content_bytes != expected:
                raise BadRequest("raw upload unit payload length mismatch")
            self._raw_plan = raw_plan
            self._digests = self._service._raw_volume_digests(self._key.collection_id, raw_plan)
            self._raw = self._service._raw_uploader(
                store, passphrase_id=volume.passphrase_id, checkpoint_store=self._writes
            )
        else:
            raise Conflict("unsupported archive volume kind")

    def _load_candidate(self) -> UploadState | None:
        if self._candidate is not None:
            return self._candidate
        volume = self._volume
        assert volume is not None
        if self._pack is not None:
            assert self._pack_plan is not None
            candidate = self._pack.load_preparation(
                collection_id=self._key.collection_id,
                plan=self._pack_plan,
                object_path=volume.object_path,
                relative_path=volume.relative_path,
            )
        else:
            assert self._raw is not None and self._raw_plan is not None
            candidate = self._raw.load_preparation(
                collection_id=self._key.collection_id,
                plan=self._raw_plan,
                object_path=volume.object_path,
                relative_path=volume.relative_path,
                target_part_plaintext_bytes=volume.unit_plaintext_bytes,
                expected_part_sha256s=self._digests,
            )
        self._candidate = candidate
        return candidate

    def _prepare_once(self) -> UploadState:
        if self._candidate is None:
            if self._pack is not None:
                assert self._pack_plan is not None
                self._candidate = self._pack.prepare(plan=self._pack_plan)
            else:
                assert self._raw is not None and self._raw_plan is not None
                self._candidate = self._raw.prepare(plan=self._raw_plan)
        return self._candidate

    def _prepare(self) -> None:
        if self._preparation is not None:
            return
        volume = self._volume
        assert volume is not None and self._epoch is not None
        prepared = self._service._retain_volume_preparation(
            admission=self._admission,
            collection_id=self._key.collection_id,
            volume_id=self._volume_id,
            epoch=self._epoch,
            identity={
                "kind": volume.kind,
                "object_path": volume.object_path,
                "plan_sha256": self._plan_sha256,
            },
            guard=self._guard,
            load=self._load_candidate,
            prepare=self._prepare_once,
        )
        if self._candidate is not None and (
            prepared.to_json_bytes() != self._candidate.to_json_bytes()
        ):
            raise Conflict("original payload encryption preparation changed")
        self._preparation = prepared

    def _open(self) -> None:
        if self._opened is not None:
            return
        volume = self._volume
        assert volume is not None and self._preparation is not None
        if self._opening is None:
            if self._pack is not None:
                assert self._pack_plan is not None
                self._opening = self._pack.retain_open(
                    collection_id=self._key.collection_id,
                    plan=self._pack_plan,
                    object_path=volume.object_path,
                    relative_path=volume.relative_path,
                    prepared_state=self._preparation,
                )
            else:
                assert self._raw is not None and self._raw_plan is not None
                self._opening = self._raw.retain_open(
                    collection_id=self._key.collection_id,
                    plan=self._raw_plan,
                    object_path=volume.object_path,
                    relative_path=volume.relative_path,
                    target_part_plaintext_bytes=volume.unit_plaintext_bytes,
                    expected_part_sha256s=self._digests,
                    prepared_state=self._preparation,
                )
        self._opened = self._opening.run()

    def _upload(self) -> None:
        if self._checkpoint is not None:
            return
        assert self._opened is not None
        if self._uploading is None:
            if self._pack is not None:
                assert self._pack_plan is not None and isinstance(
                    self._opened, PackUploadCheckpoint
                )
                self._uploading = self._pack.retain_unit(
                    plan=self._pack_plan,
                    checkpoint=self._opened,
                    unit_number=self._unit_number,
                    payload_chunks=(self._input.content(),),
                )
            else:
                assert (
                    self._raw is not None
                    and self._raw_plan is not None
                    and isinstance(self._opened, RawUploadCheckpoint)
                )
                self._uploading = self._raw.retain_unit(
                    plan=self._raw_plan,
                    checkpoint=self._opened,
                    unit_number=self._unit_number + 1,
                    plaintext=self._input.content(),
                )
        self._checkpoint = self._uploading.run()

    def _receive_receipt(self, *, terminal: bool = False) -> None:
        if self._receipt is None:
            if self._checkpoint is None:
                original = self._uploading or self._opening
                if original is not None:
                    if terminal:
                        received = original.receive_checkpoint()
                        if isinstance(received, (PackUploadCheckpoint, RawUploadCheckpoint)):
                            self._checkpoint = received
                    else:
                        try:
                            self._checkpoint = original.result()
                        except Conflict:
                            return
            checkpoint = self._checkpoint
            if checkpoint is None or checkpoint.completed is None:
                return
            if self._pack is not None:
                assert self._pack_plan is not None and isinstance(checkpoint, PackUploadCheckpoint)
                self._receipt = self._pack.sealed_receipt(
                    plan=self._pack_plan, checkpoint=checkpoint
                )
            else:
                assert (
                    self._raw is not None
                    and self._raw_plan is not None
                    and isinstance(checkpoint, RawUploadCheckpoint)
                )
                self._receipt = self._raw.sealed_receipt(checkpoint, plan=self._raw_plan)
        if not self._receipt_recorded:
            self._service._record_sealed_volume(self._key.collection_id, self._receipt)
            self._receipt_recorded = True
        if self._mirror_binding is not None:
            self._mirror_binding.receive_complete(
                CachePopulationPublication(self._service._session_factory)
            )

    def advance(self) -> dict[str, object]:
        if not self._lock.acquire(blocking=False):
            raise Conflict("original payload unit is running")
        try:
            if self._terminal_id is not None:
                raise Conflict("original payload unit is fenced for its terminal request")
            if self._result is not None:
                self._input.consumed(self)
                return self._result
            self._input.require_actor(self)
            self._initialize()
            self._prepare()
            self._open()
            self._upload()
            self._receive_receipt()
            self._result = self._service.get_unit(
                self._key.collection_id, self._volume_id, self._unit_number
            )
            self._input.consumed(self)
            return self._result
        finally:
            self._lock.release()

    def drain_terminal(self, operation_id: str) -> Literal["busy", "pending", "closed"]:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._terminal_id not in {None, operation_id}:
                raise Conflict("original payload unit has another terminal request")
            try:
                with session_scope(self._service._session_factory) as session:
                    inspect_terminal_write_records(
                        session, self._key.collection_id, operation_id, nowait=True
                    )
                self._require_construction()
            except OperationalError as error:
                if catalog_write_contention(error):
                    return "busy"
                raise
            self._terminal_id = operation_id
            self._receive_receipt(terminal=True)
            for original in (self._opening, self._uploading):
                if original is not None and not original.close():
                    return "busy"
            self._input.consumed(self)
            self._candidate = None
            self._locally_closed = True
            writes = self._writes or self._archive
            if writes is None:
                return "pending" if self._mirror_unresolved else "closed"
            drain = (
                self._mirror_binding.drain_terminal
                if self._mirror_binding is not None
                else writes.drain_terminal
            )
            status = drain(operation_id)
            if status != "busy":
                writes.receive_terminal_parts(operation_id)
                status = drain(operation_id)
            return "pending" if self._mirror_unresolved and status != "busy" else status
        finally:
            self._lock.release()

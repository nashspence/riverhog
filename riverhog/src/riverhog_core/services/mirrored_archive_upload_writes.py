"""Pair original archive/cache writes without combining their identities."""

from __future__ import annotations

import secrets
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    BeginWriteOperation,
    CompleteWriteOperation,
    WriteCompleteRequest,
    WriteCompletionPrecondition,
    WriteExecutionResult,
    WriteOperation,
    WritePartOperation,
    WriteSegmentRequest,
)
from sqlalchemy.orm import Session

from riverhog_core.app_permissions import Principal
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.domain.retrieval_cache import ClosedCachePopulation, RetrievalCacheReceipt
from riverhog_core.ports.archive_objects import WriteSegmentCursor
from riverhog_core.ports.archive_objects import WriteSession as ArchiveSession
from riverhog_core.provider_write_dispatch import (
    ArmedProviderWrite,
    ProviderWriteAdmissionRefused,
    WithdrawnProviderWrite,
)
from riverhog_core.services.archive_upload_writes import ArchiveUploadWrites
from riverhog_core.services.archive_write_admission import Guard
from riverhog_core.services.cache_population_publication import (
    CachePopulationPublication,
    PublishedCachePopulation,
)
from riverhog_core.services.cache_population_writes import CachePopulationWriteAdmission
from riverhog_core.services.mirrored_archive_begin import MirroredArchiveBegin
from riverhog_core.stores.storage_adapter_retrieval_cache import StorageAdapterRetrievalCache


@dataclass
class _CacheEffect:
    ticket: ArmedProviderWrite
    kind: str
    response: WriteExecutionResult | None = None
    withdrawal: WithdrawnProviderWrite | None = None
    reported: bool = False
    archive_confirmed: bool = False
    retirement_uncertain: bool = False


class MirroredArchiveUploadWrites:
    """Execute sibling intents, retiring parts only after both owners confirm.

    This live actor retains exact returned results through acknowledgement loss.
    Cache completion stays independently owned and does not publish placement.
    An uncertain cache effect stops further optional mirroring without releasing
    its population, data reservation or original effect.
    """

    def __init__(
        self,
        *,
        archive: ArchiveUploadWrites,
        begin: MirroredArchiveBegin,
        cache: CachePopulationWriteAdmission,
        candidate: StorageAdapterRetrievalCache,
        source_store: str,
        owner: str,
        principal: Principal,
        grants: CacheWriteGrants,
        guard: Guard,
        validate_population: Callable[[Session, CollectionUploadRecord, str, int], None],
    ) -> None:
        self._archive = archive
        self._begin = begin
        self._cache = cache
        self._candidate = candidate
        self._source_store = source_store
        self._owner = owner
        self._principal = principal
        self._grants = grants
        self._guard = guard
        self._validate_population = validate_population
        self._executor_id = secrets.token_hex(32)
        self._lock = threading.RLock()
        self._effects: dict[str, _CacheEffect] = {}
        self._reports: deque[_CacheEffect] = deque()
        self._retire: deque[_CacheEffect] = deque()
        self._checkpoint_pending: list[_CacheEffect] = []
        self._cache_writable = True
        self._cache_admission_pending = False
        self._terminal_operation_id: str | None = None
        self._checkpoint_input: tuple[str, int, str, str] | None = None
        self._closed: ClosedCachePopulation | None = None
        self._closure_acknowledged = False
        self._publication: PublishedCachePopulation | None = None
        self._publication_acknowledged = False
        self._publication_retirement_uncertain = False

    @property
    def has_pending(self) -> bool:
        return (
            self._archive.has_pending
            or self._begin.has_pending
            or self._cache_admission_pending
            or bool(self._effects)
            or bool(self._checkpoint_pending)
            or self._checkpoint_input is not None
            or self._publication is not None
            and not self._publication_acknowledged
        )

    def drain_terminal(self, operation_id: str) -> Literal["busy", "pending", "closed"]:
        """Receive only original child work; a cache population keeps its owner."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if (
                self._terminal_operation_id is not None
                and self._terminal_operation_id != operation_id
            ):
                raise Conflict("joint write actor has another terminal request")
            status = self._begin.drain_terminal(operation_id)
            if status == "busy":
                return "busy"
            self._terminal_operation_id = operation_id
            if self._checkpoint_input is not None:
                kind, collection_id, volume_id, checkpoint = self._checkpoint_input
                self._merge(
                    kind,
                    collection_id=collection_id,
                    volume_id=volume_id,
                    checkpoint_json=checkpoint,
                )
            self.flush_cache_reports()
            return "pending" if self.has_pending or status == "pending" else "closed"
        finally:
            self._lock.release()

    def _cache_operation(
        self, operation: WritePartOperation | CompleteWriteOperation
    ) -> WritePartOperation | CompleteWriteOperation | None:
        original = self._begin.archive_response
        if original is None or operation.request.session != original.result:
            raise Conflict("joint write does not use its original archive session")
        session = self._begin.cache_session
        if not self._cache_writable or session is None:
            return None
        if isinstance(operation, WritePartOperation):
            return WritePartOperation(
                request=WriteSegmentRequest(
                    session=session,
                    number=operation.request.number,
                    stored_bytes=operation.request.stored_bytes,
                ),
                stored_sha256=operation.stored_sha256,
            )
        collection_id, object_id, _ = self._archive.scope
        reader = self._candidate.resumable_object_store(
            source_store=self._source_store,
            collection_id=collection_id,
            object_id=object_id,
        )
        cursor = WriteSegmentCursor()
        while True:
            page = reader.list_segments(
                session=ArchiveSession(
                    session.object_path, session.write_token, session.expected_bytes
                ),
                cursor=cursor,
            )
            if page.next_cursor is None:
                completion = page.completion
                if completion is None:
                    raise Conflict(
                        "joint cache completion has no independently issued precondition"
                    )
                break
            cursor = page.next_cursor
        expected = operation.request.completion
        if (completion.segment_count, completion.stored_bytes) != (
            expected.segment_count,
            expected.stored_bytes,
        ):
            raise Conflict("joint cache completion differs from the original archive extent")
        request = self._candidate.population_write_request(
            source_store=self._source_store,
            collection_id=collection_id,
            object_id=object_id,
            expected_bytes=session.expected_bytes,
        )
        return CompleteWriteOperation(
            request=WriteCompleteRequest(
                session=session,
                completion=WriteCompletionPrecondition(
                    segment_count=completion.segment_count,
                    stored_bytes=completion.stored_bytes,
                    state_token=completion.state_token,
                ),
                expected_bytes=request.expected_bytes,
                expected_content_type=request.content_type,
                required_identity_assertions=request.required_identity_assertions,
                expected_placement_policy=request.placement_policy,
            )
        )

    def execute_write_operation(
        self, operation: WriteOperation, *, content: bytes = b""
    ) -> WriteExecutionResult:
        with self._lock:
            if self._terminal_operation_id is not None:
                raise Conflict("joint write actor was stopped for its terminal request")
            if self._cache_admission_pending:
                raise Conflict("original joint cache admission remains unconfirmed")
            if self._checkpoint_input is not None:
                raise Conflict("original joint archive checkpoint remains unconfirmed")
            self.flush_cache_reports()
            if isinstance(operation, BeginWriteOperation):
                if content:
                    raise Conflict("joint begin cannot accept payload bytes")
                return self._begin.begin(operation)
            if not isinstance(operation, (WritePartOperation, CompleteWriteOperation)):
                return self._archive.execute_write_operation(operation, content=content)
            sibling = self._cache_operation(operation)
            if sibling is None:
                return self._archive.execute_write_operation(operation, content=content)
            begin_call_id = self._begin.cache_begin_call_id
            if begin_call_id is None:
                raise Conflict("joint cache write has no original begin identity")
            admitted = self._archive.arm_write_operation(operation)
            collection_id, object_id, epoch = self._archive.scope
            self._cache_admission_pending = True
            try:
                ticket = self._cache.arm(
                    collection_id,
                    object_id,
                    begin_call_id=begin_call_id,
                    operation=sibling,
                    owner=self._owner,
                    principal=self._principal,
                    epoch=epoch,
                    executor_id=self._executor_id,
                    candidate=self._candidate,
                    grants=self._grants,
                    guard=self._guard,
                    validate_population=self._validate_population,
                )
                self._cache_admission_pending = False
            except BaseException as error:
                if isinstance(error, ProviderWriteAdmissionRefused):
                    self._cache_admission_pending = False
                try:
                    self._archive.withdraw_admitted_write(admitted)
                except Exception:
                    if isinstance(error, Exception):
                        raise
                raise error from None
            effect = _CacheEffect(ticket=ticket, kind=sibling.kind)
            self._effects[ticket.call_id] = effect
            try:
                result = self._archive.execute_admitted_write(admitted, content=content)
            except BaseException:
                try:
                    self._archive.withdraw_admitted_write(admitted)
                except Exception:
                    # A started archive invocation cannot prove unused withdrawal.
                    # Any original minted proof remains with its archive owner.
                    pass
                self._withdraw_preserving_failure(effect)
                raise
            self._checkpoint_pending.append(effect)
            try:
                effect.response = self._cache.execute(
                    ticket, candidate=self._candidate, content=content
                )
            except BaseException as error:
                try:
                    effect.withdrawal = ticket.withdraw()
                except Conflict:
                    self._cache_writable = False
                    if isinstance(error, Exception):
                        return result
                    raise error from None
                self._reports.append(effect)
                self._flush_preserving_failure()
                raise
            self._reports.append(effect)
            expected_outcome = "part" if sibling.kind == "part" else "completed"
            if effect.response.execution != "closed" or effect.response.outcome != expected_outcome:
                self._cache_writable = False
            self._flush_preserving_failure()
            return result

    def _withdraw_preserving_failure(self, effect: _CacheEffect) -> None:
        try:
            effect.withdrawal = effect.ticket.withdraw()
            self._reports.append(effect)
            self.flush_cache_reports()
        except Exception:
            # The admitted unused ticket or its exact proof remains owned here.
            pass

    def _flush_preserving_failure(self) -> None:
        try:
            self.flush_cache_reports()
        except Exception:
            pass

    def flush_cache_reports(self) -> None:
        """Retry retained original reports/proofs, never provider execution."""
        with self._lock:
            self._begin.flush_cache_report()
            while self._reports:
                effect = self._reports[0]
                if effect.withdrawal is not None:
                    self._cache.settle_withdrawn(effect.withdrawal)
                else:
                    assert effect.response is not None
                    self._cache.record_response(effect.ticket, effect.response)
                effect.reported = True
                self._reports.popleft()
                if effect.withdrawal is not None or self._closed_confirmed_part(effect):
                    self._retire.append(effect)
            while self._retire:
                effect = self._retire[0]
                try:
                    self._cache.acknowledge_report(effect.ticket)
                except NotFound:
                    if not effect.retirement_uncertain:
                        raise
                except Exception:
                    effect.retirement_uncertain = True
                    raise
                self._retire.popleft()
                self._effects.pop(effect.ticket.call_id)

    @staticmethod
    def _closed_confirmed_part(effect: _CacheEffect) -> bool:
        return (
            effect.kind == "part"
            and effect.archive_confirmed
            and effect.reported
            and effect.response is not None
            and effect.response.execution == "closed"
            and effect.response.outcome != "unknown"
        )

    def _merge(self, kind: str, *, collection_id: int, volume_id: str, checkpoint_json: str) -> str:
        with self._lock:
            original = (kind, collection_id, volume_id, checkpoint_json)
            if self._checkpoint_input is not None and self._checkpoint_input != original:
                raise Conflict("original joint archive checkpoint report changed")
            self._checkpoint_input = original
            merge = (
                self._archive.merge_pack_upload_checkpoint
                if kind == "pack"
                else self._archive.merge_raw_upload_checkpoint
            )
            result = merge(
                collection_id=collection_id, volume_id=volume_id, checkpoint_json=checkpoint_json
            )
            # The exact original archive reports and checkpoint are confirmed.
            for effect in self._checkpoint_pending:
                effect.archive_confirmed = True
                if self._closed_confirmed_part(effect):
                    self._retire.append(effect)
            self._checkpoint_pending.clear()
            self._checkpoint_input = None
            self._flush_preserving_failure()
            return result

    def load_pack_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> str | None:
        return self._archive.load_pack_upload_checkpoint(
            collection_id=collection_id, volume_id=volume_id
        )

    def load_raw_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> str | None:
        return self._archive.load_raw_upload_checkpoint(
            collection_id=collection_id, volume_id=volume_id
        )

    def merge_pack_upload_checkpoint(
        self, *, collection_id: int, volume_id: str, checkpoint_json: str
    ) -> str:
        return self._merge(
            "pack",
            collection_id=collection_id,
            volume_id=volume_id,
            checkpoint_json=checkpoint_json,
        )

    def merge_raw_upload_checkpoint(
        self, *, collection_id: int, volume_id: str, checkpoint_json: str
    ) -> str:
        return self._merge(
            "segment",
            collection_id=collection_id,
            volume_id=volume_id,
            checkpoint_json=checkpoint_json,
        )

    def delete_pack_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> None:
        raise Conflict("forward joint write actor cannot dispose checkpoints")

    def delete_raw_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> None:
        raise Conflict("forward joint write actor cannot dispose checkpoints")

    def transfer_cache(self, publisher: CachePopulationPublication) -> ClosedCachePopulation | None:
        """Receive closed bytes durably before releasing the original call slots."""
        with self._lock:
            if self._closure_acknowledged:
                return self._closed
            self.flush_cache_reports()
            if self._begin.cache_begin_call_id is None:
                return None
            if self._closed is None:
                completed = [
                    effect
                    for effect in self._effects.values()
                    if effect.kind == "complete"
                    and effect.reported
                    and effect.archive_confirmed
                    and effect.response is not None
                    and effect.response.outcome == "completed"
                    and effect.response.execution == "closed"
                ]
                if len(completed) != 1:
                    raise Conflict("joint cache population has no confirmed original completion")
                self._closed = publisher.closure.accept(completed[0].ticket)
            publisher.closure.acknowledge(self._closed)
            self._begin.acknowledge_transfer(self._closed)
            value = publisher.closure._value(self._closed)
            self._effects.pop(value["completion_call_id"], None)
            self._closure_acknowledged = True
            return self._closed

    def publish_cache(self, publisher: CachePopulationPublication) -> RetrievalCacheReceipt | None:
        """Publish retained closure only after accepted archive authority."""
        with self._lock:
            if self._publication_acknowledged:
                assert self._publication is not None
                return self._publication.receipt
            closed = self.transfer_cache(publisher)
            if closed is None:
                return None
            if self._publication is None:
                self._publication = publisher.publish(closed)
            try:
                publisher.acknowledge(self._publication)
            except NotFound:
                if not self._publication_retirement_uncertain:
                    raise
            except Exception:
                self._publication_retirement_uncertain = True
                raise
            self._publication_acknowledged = True
            return self._publication.receipt

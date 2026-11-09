"""Commit independent archive/cache begin intents before physical execution."""

from __future__ import annotations

import json
import secrets
import threading
from collections.abc import Callable
from typing import Literal

from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import (
    BeginWriteOperation,
    WriteExecutionResult,
    WriteSession,
)
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.app_permissions import Principal
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_models import (
    CollectionArchiveObjectUploadRecord,
    CollectionUploadRecord,
    RetrievalCachePopulationRecord,
)
from riverhog_core.domain.retrieval_cache import ClosedCachePopulation
from riverhog_core.provider_write_dispatch import (
    ArmedProviderWrite,
    ProviderWriteAdmissionRefused,
    WithdrawnProviderWrite,
)
from riverhog_core.runtime_config import RetrievalCacheStoreRegistration
from riverhog_core.services.archive_upload_writes import ArchiveUploadWrites
from riverhog_core.services.archive_write_admission import Guard
from riverhog_core.services.cache_population_begin import (
    CacheDataReservationUnavailable,
    CachePopulationBeginAdmission,
)
from riverhog_core.stores.storage_adapter_retrieval_cache import StorageAdapterRetrievalCache


class MirroredArchiveBegin:
    """Retain the original archive result independently of optional cache progress.

    One live actor attempts one joint begin. Configured cache candidates are tried
    in order only after confirmed data-capacity refusal; uncertain admission or
    execution never selects another target. The archive workflow owns its result
    and checkpoint. The cache retains its own result, reservation and report;
    this coordinator neither publishes a ready placement nor releases ownership.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        archive: ArchiveUploadWrites,
        cache: CachePopulationBeginAdmission,
        candidates: tuple[
            tuple[StorageAdapterRetrievalCache, RetrievalCacheStoreRegistration], ...
        ],
        owner: str,
        principal: Principal,
        grants: CacheWriteGrants,
        guard: Guard,
        validate_population: Callable[[Session, CollectionUploadRecord, str, int], None],
    ) -> None:
        self._factory = factory
        self._archive = archive
        self._cache = cache
        self._candidates = tuple(candidates)
        if len({candidate.name for candidate, _ in self._candidates}) != len(self._candidates):
            raise ValueError("joint cache candidates require unique configured names")
        self._candidate: StorageAdapterRetrievalCache | None = None
        self._owner = owner
        self._principal = principal
        self._grants = grants
        self._guard = guard
        self._validate_population = validate_population
        self._executor_id = secrets.token_hex(32)
        self._lock = threading.RLock()
        self._running = False
        self._cache_admission_pending = False
        self._terminal_operation_id: str | None = None
        self._transfer_receipt: ClosedCachePopulation | None = None
        self._entered = False
        self._cache_ticket: ArmedProviderWrite | None = None
        self._cache_response: WriteExecutionResult | None = None
        self._cache_reported = False
        self._cache_withdrawn_settled = False
        self._withdrawal: WithdrawnProviderWrite | None = None
        self._archive_response: WriteExecutionResult | None = None

    @property
    def has_pending(self) -> bool:
        """A settled begin still owns its population until durable transfer."""
        return (
            self._running
            or self._cache_admission_pending
            or self._cache_ticket is not None
            and not self._cache_withdrawn_settled
            and self._transfer_receipt is None
        )

    @property
    def has_unclosed_execution(self) -> bool:
        """Closed forward calls can still retain an owned cache population."""
        return (
            self._running
            or self._cache_admission_pending
            or self._cache_ticket is not None
            and not self._cache_withdrawn_settled
            and (
                self._cache_response is None
                or self._cache_response.execution != "closed"
                or not self._cache_reported
            )
        )

    def drain_terminal(self, operation_id: str) -> Literal["busy", "pending", "closed"]:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._running:
                return "busy"
            if (
                self._terminal_operation_id is not None
                and self._terminal_operation_id != operation_id
            ):
                raise Conflict("joint begin has another terminal request")
            archive = self._archive.drain_terminal(operation_id)
            if archive == "busy":
                return "busy"
            self._terminal_operation_id = operation_id
            if (
                self._cache_ticket is not None
                and self._cache_response is None
                and self._withdrawal is None
                and not self._cache_withdrawn_settled
                and self._transfer_receipt is None
            ):
                try:
                    self._withdrawal = self._cache_ticket.withdraw()
                except Conflict:
                    # A started or uncertain call has no unused-capability proof.
                    return "pending"
            self.flush_cache_report()
            return "pending" if self.has_unclosed_execution or archive == "pending" else "closed"
        finally:
            self._lock.release()

    def acknowledge_transfer(self, receipt: ClosedCachePopulation) -> None:
        """Receive an exact existing durable handoff, never a new provider right."""
        with self._lock:
            if self._transfer_receipt is not None:
                if self._transfer_receipt != receipt:
                    raise Conflict("joint begin original receiving acknowledgement changed")
                return
            ticket, response = self._cache_ticket, self._cache_response
            collection_id, object_id, _ = self._archive.scope
            if (
                ticket is None
                or response is None
                or not self._cache_reported
                or response.execution != "closed"
                or response.outcome != "session"
                or (receipt.collection_id, receipt.object_id) != (collection_id, object_id)
            ):
                raise Conflict("joint begin has no original closed population to transfer")
            value = json.loads(receipt.receipt_json)
            if (
                value["source_store"] != receipt.source_store
                or value["collection_id"] != collection_id
                or value["object_id"] != object_id
                or value["population_generation"] != receipt.generation
                or value["begin_call_id"] != ticket.call_id
                or value["begin_request_sha256"] != ticket.request_sha256
                or value["cache_incarnation_id"] != ticket.target_incarnation
            ):
                raise Conflict("joint begin receiving acknowledgement has another original call")
            with read_snapshot(self._factory) as session:
                source = session.get(
                    CollectionArchiveObjectUploadRecord, (collection_id, object_id)
                )
                population = session.get(
                    RetrievalCachePopulationRecord,
                    (receipt.source_store, collection_id, object_id),
                )
                source_matches = (
                    source is not None and source.cache_receipt_json == receipt.receipt_json
                )
                receiver_matches = (
                    population is not None
                    and population.generation == receipt.generation
                    and population.closed_receipt_json == receipt.receipt_json
                )
                if not source_matches and not receiver_matches:
                    raise Conflict("joint begin receiving acknowledgement is not durable")
                if (
                    population is not None
                    and population.generation == receipt.generation
                    and not receiver_matches
                ):
                    raise Conflict("joint begin durable receiving evidence changed")
            self._transfer_receipt = receipt

    @property
    def candidate(self) -> StorageAdapterRetrievalCache | None:
        """The original admitted cache target, without selection or provider I/O."""
        return self._candidate

    @property
    def archive_response(self) -> WriteExecutionResult | None:
        return self._archive_response

    @property
    def cache_begin_call_id(self) -> str | None:
        return self._cache_ticket.call_id if self._cache_ticket is not None else None

    @property
    def cache_session(self) -> WriteSession | None:
        response = self._cache_response
        if (
            self._cache_reported
            and response is not None
            and response.execution == "closed"
            and isinstance(response.result, WriteSession)
        ):
            return response.result.model_copy(deep=True)
        return None

    def begin(self, operation: BeginWriteOperation) -> WriteExecutionResult:
        if not isinstance(operation, BeginWriteOperation):
            raise TypeError("mirror begin requires an original archive begin operation")
        if not self._lock.acquire(blocking=False):
            raise Conflict("joint archive/cache begin was already attempted")
        entered_here = False
        try:
            if self._terminal_operation_id is not None:
                raise Conflict("joint begin was stopped for its terminal request")
            if self._entered:
                raise Conflict("joint archive/cache begin was already attempted")
            self._entered = True
            self._running = True
            entered_here = True
            admitted = self._archive.arm_write_operation(operation)
            collection_id, object_id, epoch = self._archive.scope
            try:
                for candidate, registration in self._candidates:
                    self._cache_admission_pending = True
                    try:
                        ticket = self._cache.arm(
                            collection_id,
                            object_id,
                            owner=self._owner,
                            principal=self._principal,
                            expected_bytes=operation.request.expected_bytes,
                            epoch=epoch,
                            executor_id=self._executor_id,
                            candidate=candidate,
                            registration=registration,
                            grants=self._grants,
                            guard=self._guard,
                            validate_population=self._validate_population,
                        )
                        if not isinstance(ticket, ArmedProviderWrite) or (
                            ticket.collection_id != collection_id
                            or ticket.object_id != object_id
                            or ticket.executor_id != self._executor_id
                            or ticket.target_incarnation != candidate.incarnation_id
                        ):
                            raise Conflict(
                                "original cache admission returned no exact dispatch capability"
                            )
                        self._cache_ticket = ticket
                    except CacheDataReservationUnavailable:
                        # Confirmed refusal committed no cache effect or reservation.
                        # Only this known absence permits trying the next preference.
                        self._cache_admission_pending = False
                        continue
                    self._candidate = candidate
                    self._cache_admission_pending = False
                    break
            except BaseException as error:
                if isinstance(error, ProviderWriteAdmissionRefused):
                    self._cache_admission_pending = False
                self._archive.withdraw_admitted_write(admitted)
                raise
            if self._cache_ticket is None:
                self._archive_response = self._archive.execute_admitted_write(admitted)
                return self._archive_response
            try:
                self._archive_response = self._archive.execute_admitted_write(admitted)
            except BaseException:
                self._withdrawal = self._cache_ticket.withdraw()
                self._flush_preserving_failure()
                raise
            try:
                assert self._cache_ticket is not None and self._candidate is not None
                self._cache_response = self._cache.execute(
                    self._cache_ticket, candidate=self._candidate
                )
            except BaseException as error:
                try:
                    self._withdrawal = self._cache_ticket.withdraw()
                except Conflict:
                    # Started execution cannot prove withdrawal. Its original
                    # independently retained effect remains charged and unresolved.
                    if isinstance(error, Exception):
                        return self._archive_response
                    raise error from None
                self._flush_preserving_failure()
                raise
            # A failed cache report keeps this same returned response for retry.
            # It cannot replace or reissue the independently retained archive result.
            self._flush_preserving_failure()
            return self._archive_response
        finally:
            if entered_here:
                self._running = False
            self._lock.release()

    def _flush_preserving_failure(self) -> None:
        try:
            self.flush_cache_report()
        except Exception:
            # The original live response/proof stays owned until acknowledgement.
            pass

    def flush_cache_report(self) -> None:
        """Retry only the original cache result or unused-capability proof."""
        with self._lock:
            if self._withdrawal is not None:
                self._cache.settle_withdrawn(self._withdrawal)
                self._cache_withdrawn_settled = True
                self._withdrawal = None
            if self._cache_response is not None and not self._cache_reported:
                assert self._cache_ticket is not None
                self._cache.record_response(self._cache_ticket, self._cache_response)
                self._cache_reported = True

"""Commit independent archive/cache begin intents before physical execution."""

from __future__ import annotations

import secrets
import threading
from collections.abc import Callable

from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import (
    BeginWriteOperation,
    WriteExecutionResult,
    WriteSession,
)
from sqlalchemy.orm import Session

from riverhog_core.app_permissions import Principal
from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.provider_write_dispatch import ArmedProviderWrite, WithdrawnProviderWrite
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

    One live actor attempts one joint begin. The archive workflow owns its result
    and checkpoint. The cache retains its own result, reservation and report;
    this coordinator neither publishes a ready placement nor releases ownership.
    """

    def __init__(
        self,
        *,
        archive: ArchiveUploadWrites,
        cache: CachePopulationBeginAdmission,
        candidate: StorageAdapterRetrievalCache,
        registration: RetrievalCacheStoreRegistration,
        owner: str,
        principal: Principal,
        grants: CacheWriteGrants,
        guard: Guard,
        validate_population: Callable[[Session, CollectionUploadRecord, str, int], None],
    ) -> None:
        self._archive = archive
        self._cache = cache
        self._candidate = candidate
        self._registration = registration
        self._owner = owner
        self._principal = principal
        self._grants = grants
        self._guard = guard
        self._validate_population = validate_population
        self._executor_id = secrets.token_hex(32)
        self._entry_lock = threading.Lock()
        self._report_lock = threading.Lock()
        self._entered = False
        self._cache_ticket: ArmedProviderWrite | None = None
        self._cache_response: WriteExecutionResult | None = None
        self._cache_reported = False
        self._withdrawal: WithdrawnProviderWrite | None = None
        self._archive_response: WriteExecutionResult | None = None

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
        with self._entry_lock:
            if self._entered:
                raise Conflict("joint archive/cache begin was already attempted")
            self._entered = True
        admitted = self._archive.arm_write_operation(operation)
        collection_id, object_id, epoch = self._archive.scope
        try:
            self._cache_ticket = self._cache.arm(
                collection_id,
                object_id,
                owner=self._owner,
                principal=self._principal,
                expected_bytes=operation.request.expected_bytes,
                epoch=epoch,
                executor_id=self._executor_id,
                candidate=self._candidate,
                registration=self._registration,
                grants=self._grants,
                guard=self._guard,
                validate_population=self._validate_population,
            )
        except CacheDataReservationUnavailable:
            # The owner has checked current authority after its accounting wait.
            self._archive_response = self._archive.execute_admitted_write(admitted)
            return self._archive_response
        except BaseException:
            self._archive.withdraw_admitted_write(admitted)
            raise
        try:
            self._archive_response = self._archive.execute_admitted_write(admitted)
        except BaseException:
            self._withdrawal = self._cache_ticket.withdraw()
            self._flush_preserving_failure()
            raise
        try:
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
                raise
            self._flush_preserving_failure()
            raise
        # A failed cache report keeps this same returned response for retry.
        # It cannot replace or reissue the independently retained archive result.
        self._flush_preserving_failure()
        return self._archive_response

    def _flush_preserving_failure(self) -> None:
        try:
            self.flush_cache_report()
        except Exception:
            # The original live response/proof stays owned until acknowledgement.
            pass

    def flush_cache_report(self) -> None:
        """Retry only the original cache result or unused-capability proof."""
        with self._report_lock:
            if self._withdrawal is not None:
                self._cache.settle_withdrawn(self._withdrawal)
                self._withdrawal = None
            if self._cache_response is not None and not self._cache_reported:
                assert self._cache_ticket is not None
                self._cache.record_response(self._cache_ticket, self._cache_response)
                self._cache_reported = True

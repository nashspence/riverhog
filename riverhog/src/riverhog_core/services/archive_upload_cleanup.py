"""Durable selected-page receiving for a separately adjudicated upload terminal.

The composing service owns global writer quiescence, current disposal permission,
target/global physical slots and scheduling. This owner requires its retained
cleanup-ready decision; catalog absence cannot create that decision. Its grants
bound retained per-terminal control state and complete outstanding attempt charges.
Provider calls never run in these catalog transactions.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from riverhog_canonical_json import canonical_json_bytes, format_scalar, parse_scalar
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    CleanupExecutionResult,
    CleanupInventoryProgress,
    CleanupInventoryRequest,
    CleanupInventoryStepRequest,
    CleanupInvocation,
    CleanupProgress,
    CleanupRequest,
    CleanupStepRequest,
    WriteCallIdentity,
    cleanup_step_sha256,
    unknown_cleanup_execution,
    validate_cleanup_execution,
    validate_cleanup_inventory_response,
)
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.archive_cleanup_grants import ArchiveCleanupGrants
from riverhog_core.catalog_archive_write_models import (
    ArchiveCleanupAttemptRecord,
    ArchiveCleanupPageRecord,
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
)
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.ports.archive_objects import ArchiveObjectCleanupPort
from riverhog_core.services.archive_upload_drain import lock_terminal_selection


def _json(value: object) -> str:
    return canonical_json_bytes(value).decode()


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _charge(value: str) -> int:
    result = parse_scalar("nonnegative", value)
    if result < 1:
        raise Conflict("retained cleanup reservation is invalid")
    return result


@dataclass(frozen=True, slots=True)
class RetainedCleanupPage:
    request_sha256: str
    request: CleanupStepRequest = field(repr=False)
    disposed: tuple[int, ...]

    @property
    def complete(self) -> bool:
        return len(self.disposed) == len(self.request.inventory.candidates)


class RetainedCleanupAttempt:
    """The original live one-use right; cold catalog rows cannot recreate it."""

    def __init__(
        self,
        collection_id: int,
        operation_id: str,
        invocation: CleanupInvocation,
        nonce: str,
    ) -> None:
        self.collection_id = collection_id
        self.operation_id = operation_id
        self.invocation = invocation
        self._nonce = nonce
        self._lock = threading.Lock()
        self._started = False
        self._response: CleanupExecutionResult | None = None

    def _dispatch(self, target: ArchiveObjectCleanupPort) -> CleanupExecutionResult:
        with self._lock:
            if self._response is not None:
                return CleanupExecutionResult.model_validate_json(
                    self._response.model_dump_json(exclude_none=True)
                )
            if self._started:
                raise Conflict("original cleanup attempt is still running or unaccounted")
            self._started = True
        try:
            response = target.execute_cleanup(self.invocation)
            response = CleanupExecutionResult.model_validate_json(
                response.model_dump_json(exclude_none=True)
            )
            validate_cleanup_execution(self.invocation, response)
        except Exception:
            response = unknown_cleanup_execution(self.invocation)
        with self._lock:
            self._response = response
        return response


class ArchiveUploadCleanup:
    def __init__(self, factory: SessionFactory, *, grants: ArchiveCleanupGrants) -> None:
        self._factory = factory
        self._grants = grants

    @staticmethod
    def _context(
        session: Session,
        collection_id: int,
        operation_id: str,
        *,
        admit: bool = False,
        guard: Callable[[Session], None] | None = None,
    ) -> tuple[ArchiveWriteControlRecord, ArchiveUploadTerminalRecord]:
        if admit and not callable(guard):
            raise ValueError("cleanup admission requires a current disposal guard")
        control, terminal = lock_terminal_selection(
            session,
            collection_id,
            operation_id,
            require_upload=admit,
            guard=guard,
        )
        if admit:
            if terminal.phase not in {"cleanup-ready", "cleaning"}:
                raise Conflict("upload terminal has no retained cleanup-ready decision")
            for model in (
                ArchiveWriteEffectRecord,
                CachePopulationBeginRecord,
                CachePopulationWriteEffectRecord,
            ):
                if (
                    session.scalar(
                        select(model.id)
                        .where(model.collection_id == collection_id, model.unresolved.is_(True))
                        .limit(1)
                    )
                    is not None
                ):
                    raise Conflict("upload terminal still has unclosed forward records")
        return control, terminal

    @staticmethod
    def _require_scope(
        request: CleanupRequest, control: ArchiveWriteControlRecord, operation_id: str
    ) -> None:
        construction = json.loads(control.construction_json)
        if (
            request.selected_terminal_sha256 != operation_id
            or request.inventory.object_prefix != construction["archive_storage_prefix"]
            or request.inventory.target_incarnation != construction["archive_incarnation_id"]
        ):
            raise Conflict("cleanup selection differs from its original archive scope")

    @staticmethod
    def _page_bytes(
        operation_id: str, request: CleanupStepRequest, inventory_request_json: str
    ) -> int:
        # Header names conservatively cover the per-row identity overhead too.
        return len(
            _json(
                {
                    "terminal_id": operation_id,
                    "request_sha256": cleanup_step_sha256(request),
                    "inventory_call_id": "0" * 64,
                    "inventory_request": json.loads(inventory_request_json),
                    "request": request.model_dump(mode="json"),
                    "disposed": list(range(len(request.inventory.candidates))),
                }
            ).encode()
        )

    @staticmethod
    def _attempt_bytes(invocation: CleanupInvocation, operation_id: str) -> int:
        request = invocation.request
        # Persist invocation/response JSON with the same omission policy as
        # their protocol grant; optional nulls must not escape byte accounting.
        reservation = request.maximum_control_bytes
        while True:
            header = {
                "id": invocation.call.call_id,
                "terminal_id": operation_id,
                "page_sha256": "0" * 64,
                "dispatch_sha256": "0" * 64,
                "unresolved": False,
                "control_bytes": format_scalar("nonnegative", reservation),
                "provider_requests": format_scalar(
                    "nonnegative", request.maximum_provider_requests
                ),
                "in_flight": format_scalar("nonnegative", request.maximum_in_flight),
            }
            total = request.maximum_control_bytes + len(_json(header).encode())
            if total == reservation:
                return total
            reservation = total

    @staticmethod
    def _page(
        record: ArchiveCleanupPageRecord, operation_id: str, control: ArchiveWriteControlRecord
    ) -> RetainedCleanupPage:
        request = CleanupStepRequest.model_validate_json(record.request_json)
        ArchiveUploadCleanup._require_scope(request, control, operation_id)
        disposed = json.loads(record.disposed_json)
        if (
            record.terminal_id != operation_id
            or request.selected_terminal_sha256 != operation_id
            or cleanup_step_sha256(request) != record.request_sha256
            or not isinstance(disposed, list)
            or any(type(index) is not int for index in disposed)
            or disposed != sorted(set(disposed))
            or any(not 0 <= index < len(request.inventory.candidates) for index in disposed)
        ):
            raise Conflict("retained cleanup page identity changed")
        return RetainedCleanupPage(record.request_sha256, request, tuple(disposed))

    def _reserve(
        self,
        session: Session,
        operation_id: str,
        *,
        page_bytes: int,
        invocation: CleanupInvocation | None = None,
    ) -> None:
        # The parent terminal lock serializes reservations. This indexed query
        # reads only bounded outstanding charges, never completed retry history.
        rows = session.execute(
            select(
                ArchiveCleanupAttemptRecord.control_bytes,
                ArchiveCleanupAttemptRecord.provider_requests,
                ArchiveCleanupAttemptRecord.in_flight,
            )
            .where(ArchiveCleanupAttemptRecord.terminal_id == operation_id)
            .order_by(ArchiveCleanupAttemptRecord.id)
            .limit(self._grants.calls + 1)
        ).all()
        controls = page_bytes + sum(_charge(row[0]) for row in rows)
        requests = sum(_charge(row[1]) for row in rows)
        in_flight = sum(_charge(row[2]) for row in rows)
        calls = len(rows)
        if invocation is not None:
            calls += 1
            controls += self._attempt_bytes(invocation, operation_id)
            requests += invocation.request.maximum_provider_requests
            in_flight += invocation.request.maximum_in_flight
        if (
            calls > self._grants.calls
            or controls > self._grants.control_bytes
            or requests > self._grants.provider_requests
            or in_flight > self._grants.in_flight
        ):
            raise Conflict("cleanup outstanding reservations exhaust the retention grant")

    @staticmethod
    def _predecessor(
        record: ArchiveCleanupPageRecord | None,
        inventory_request: CleanupInventoryRequest,
        after: str | None,
        operation_id: str,
        control: ArchiveWriteControlRecord,
    ) -> None:
        if record is None:
            if after is not None or inventory_request.continuation is not None:
                raise Conflict("cleanup first selection has no retained predecessor")
            return
        prior = ArchiveUploadCleanup._page(record, operation_id, control)
        if (
            after != prior.request_sha256
            or not prior.complete
            or prior.request.inventory.exhausted
            or inventory_request.continuation != prior.request.inventory.continuation
        ):
            raise Conflict("cleanup cannot advance an incomplete or different page")

    def arm_inventory(
        self,
        collection_id: int,
        operation_id: str,
        *,
        request: CleanupInventoryStepRequest,
        after: str | None,
        dispatch_owner: str,
        guard: Callable[[Session], None],
    ) -> RetainedCleanupAttempt:
        """Retain the original read and all grants before any provider access."""
        request = CleanupInventoryStepRequest.model_validate_json(request.model_dump_json())
        invocation = CleanupInvocation(
            call=WriteCallIdentity(
                call_id=secrets.token_hex(32),
                exact_request_sha256=cleanup_step_sha256(request),
                target_incarnation=request.inventory.target_incarnation,
                dispatch_owner=dispatch_owner,
            ),
            request=request,
        )
        nonce = secrets.token_hex(32)
        with session_scope(self._factory) as session:
            control, _ = self._context(
                session, collection_id, operation_id, admit=True, guard=guard
            )
            self._require_scope(request, control, operation_id)
            page_record = session.get(ArchiveCleanupPageRecord, operation_id)
            self._predecessor(page_record, request.inventory, after, operation_id, control)
            page_bytes = (
                self._page_bytes(
                    operation_id,
                    self._page(page_record, operation_id, control).request,
                    page_record.inventory_request_json,
                )
                if page_record is not None
                else 0
            )
            self._reserve(session, operation_id, page_bytes=page_bytes, invocation=invocation)
            session.add(
                ArchiveCleanupAttemptRecord(
                    id=invocation.call.call_id,
                    terminal_id=operation_id,
                    page_sha256=after,
                    dispatch_sha256=_sha(nonce),
                    invocation_json=invocation.model_dump_json(exclude_none=True),
                    unresolved=True,
                    control_bytes=format_scalar(
                        "nonnegative", self._attempt_bytes(invocation, operation_id)
                    ),
                    provider_requests=format_scalar(
                        "nonnegative", request.maximum_provider_requests
                    ),
                    in_flight=format_scalar("nonnegative", request.maximum_in_flight),
                )
            )
        return RetainedCleanupAttempt(collection_id, operation_id, invocation, nonce)

    def select_page(
        self,
        collection_id: int,
        operation_id: str,
        *,
        inventory_call_id: str,
        inventory_request: CleanupInventoryRequest,
        request: CleanupStepRequest,
        after: str | None,
        guard: Callable[[Session], None],
    ) -> RetainedCleanupPage:
        """Select only a retained original read, before any destructive attempt."""
        inventory_request = CleanupInventoryRequest.model_validate_json(
            inventory_request.model_dump_json()
        )
        request = CleanupStepRequest.model_validate_json(request.model_dump_json())
        validate_cleanup_inventory_response(inventory_request, request.inventory)
        request_sha256 = cleanup_step_sha256(request)
        # Fixed-width synthetic correlation validates reservation shape only;
        # it creates no live invocation or execution evidence.
        CleanupInvocation(
            call=WriteCallIdentity(
                call_id="0" * 64,
                exact_request_sha256=request_sha256,
                target_incarnation=request.inventory.target_incarnation,
                dispatch_owner="0" * 64,
            ),
            request=request,
        )
        encoded = request.model_dump_json()
        inventory_encoded = inventory_request.model_dump_json()
        with session_scope(self._factory) as session:
            control, terminal = self._context(
                session, collection_id, operation_id, admit=True, guard=guard
            )
            self._require_scope(request, control, operation_id)
            record = session.get(ArchiveCleanupPageRecord, operation_id)
            if record is not None:
                prior = self._page(record, operation_id, control)
                if record.inventory_call_id == inventory_call_id:
                    if (
                        prior.request_sha256 != request_sha256
                        or record.request_json != encoded
                        or record.inventory_request_json != inventory_encoded
                    ):
                        raise Conflict("cleanup selection preimage changed")
                    return prior
            self._predecessor(record, inventory_request, after, operation_id, control)
            observed = session.get(ArchiveCleanupAttemptRecord, inventory_call_id)
            if observed is None or observed.terminal_id != operation_id:
                raise NotFound("original cleanup inventory attempt is unavailable")
            if observed.response_json is None:
                raise Conflict("cleanup inventory has no retained original observation")
            response = CleanupExecutionResult.model_validate_json(observed.response_json)
            original, invocation = self._attempt(session, operation_id, response)
            if (
                original.id != inventory_call_id
                or not isinstance(invocation.request, CleanupInventoryStepRequest)
                or not isinstance(response.result, CleanupInventoryProgress)
                or observed.page_sha256 != after
                or invocation.request.inventory != inventory_request
                or response.result.inventory != request.inventory
            ):
                raise Conflict("cleanup selection differs from its original inventory read")
            # Reserve the largest disposed-index encoding before any effect.
            page_bytes = self._page_bytes(operation_id, request, inventory_encoded)
            self._reserve(session, operation_id, page_bytes=page_bytes)
            if record is None:
                record = ArchiveCleanupPageRecord(terminal_id=operation_id)
                session.add(record)
            record.request_sha256 = request_sha256
            record.inventory_call_id = inventory_call_id
            record.inventory_request_json = inventory_encoded
            record.request_json = encoded
            record.disposed_json = "[]"
            terminal.phase = "cleaning"
            return self._page(record, operation_id, control)

    def current(self, collection_id: int, operation_id: str) -> RetainedCleanupPage:
        """Observe original bounded progress; this grants no new provider work."""
        with session_scope(self._factory) as session:
            control, _ = self._context(session, collection_id, operation_id)
            record = session.get(ArchiveCleanupPageRecord, operation_id)
            if record is None:
                raise NotFound("upload terminal cleanup page is unavailable")
            return self._page(record, operation_id, control)

    def arm(
        self,
        collection_id: int,
        operation_id: str,
        *,
        page_sha256: str,
        dispatch_owner: str,
        maximum_provider_requests: int,
        maximum_control_bytes: int,
        maximum_in_flight: int,
        guard: Callable[[Session], None],
    ) -> RetainedCleanupAttempt:
        """Commit a fresh exact remaining-item request and independent current grants.

        A new attempt may have different grants without rewriting the original
        selection or releasing any earlier uncertain physical reservation.
        """
        nonce = secrets.token_hex(32)
        with session_scope(self._factory) as session:
            control, _ = self._context(
                session, collection_id, operation_id, admit=True, guard=guard
            )
            record = session.get(ArchiveCleanupPageRecord, operation_id)
            if record is None:
                raise NotFound("upload terminal cleanup page is unavailable")
            page = self._page(record, operation_id, control)
            if page.request_sha256 != page_sha256 or page.complete:
                raise Conflict("cleanup selection changed or is already complete")
            inventory = page.request.inventory
            remaining = tuple(
                item
                for index, item in enumerate(inventory.candidates)
                if index not in page.disposed
            )
            request = page.request.model_copy(
                update={
                    "inventory": inventory.model_copy(update={"candidates": remaining}),
                    "maximum_provider_requests": maximum_provider_requests,
                    "maximum_control_bytes": maximum_control_bytes,
                    "maximum_in_flight": maximum_in_flight,
                }
            )
            invocation = CleanupInvocation(
                call=WriteCallIdentity(
                    call_id=secrets.token_hex(32),
                    exact_request_sha256=cleanup_step_sha256(request),
                    target_incarnation=inventory.target_incarnation,
                    dispatch_owner=dispatch_owner,
                ),
                request=request,
            )
            page_bytes = self._page_bytes(operation_id, page.request, record.inventory_request_json)
            self._reserve(session, operation_id, page_bytes=page_bytes, invocation=invocation)
            session.add(
                ArchiveCleanupAttemptRecord(
                    id=invocation.call.call_id,
                    terminal_id=operation_id,
                    page_sha256=page.request_sha256,
                    dispatch_sha256=_sha(nonce),
                    invocation_json=invocation.model_dump_json(exclude_none=True),
                    unresolved=True,
                    control_bytes=format_scalar(
                        "nonnegative", self._attempt_bytes(invocation, operation_id)
                    ),
                    provider_requests=format_scalar(
                        "nonnegative", request.maximum_provider_requests
                    ),
                    in_flight=format_scalar("nonnegative", request.maximum_in_flight),
                )
            )
        # A failed/uncertain commit never returns a capability. Its row, if
        # committed, remains charged; a later attempt always has a new call ID.
        return RetainedCleanupAttempt(collection_id, operation_id, invocation, nonce)

    def execute(
        self, ticket: RetainedCleanupAttempt, target: ArchiveObjectCleanupPort
    ) -> CleanupExecutionResult:
        """Check the original capability, then execute outside the catalog writer."""
        with session_scope(self._factory) as session:
            self._context(session, ticket.collection_id, ticket.operation_id)
            record = session.get(ArchiveCleanupAttemptRecord, ticket.invocation.call.call_id)
            if (
                record is None
                or record.terminal_id != ticket.operation_id
                or record.dispatch_sha256 != _sha(ticket._nonce)
                or record.invocation_json != ticket.invocation.model_dump_json(exclude_none=True)
            ):
                raise Conflict("original live cleanup dispatch capability changed")
        return ticket._dispatch(target)

    @staticmethod
    def _attempt(
        session: Session,
        operation_id: str,
        response: CleanupExecutionResult,
    ) -> tuple[ArchiveCleanupAttemptRecord, CleanupInvocation]:
        record = session.get(ArchiveCleanupAttemptRecord, response.call.call_id)
        if record is None or record.terminal_id != operation_id:
            raise NotFound("original cleanup attempt is unavailable")
        invocation = CleanupInvocation.model_validate_json(record.invocation_json)
        if isinstance(invocation.request, CleanupStepRequest) and record.page_sha256 is None:
            raise Conflict("retained disposal attempt has no original selection")
        if invocation.call.call_id != record.id:
            raise Conflict("retained cleanup attempt identity changed")
        validate_cleanup_execution(invocation, response)
        return record, invocation

    def record_response(
        self, collection_id: int, operation_id: str, response: CleanupExecutionResult
    ) -> None:
        """Receive the trusted original response after fencing, without redispatch."""
        response = CleanupExecutionResult.model_validate_json(
            response.model_dump_json(exclude_none=True)
        )
        encoded = response.model_dump_json(exclude_none=True)
        with session_scope(self._factory) as session:
            control, _ = self._context(session, collection_id, operation_id)
            record, invocation = self._attempt(session, operation_id, response)
            self._require_scope(invocation.request, control, operation_id)
            if record.response_json is not None:
                prior = CleanupExecutionResult.model_validate_json(record.response_json)
                if (
                    (prior.execution == "closed" and prior != response)
                    or (prior.result is not None and prior.result != response.result)
                    or (prior.result is not None and response.outcome != prior.outcome)
                ):
                    raise Conflict("cleanup response conflicts with its retained original")
            page_record = session.get(ArchiveCleanupPageRecord, operation_id)
            if (
                isinstance(invocation.request, CleanupStepRequest)
                and page_record is not None
                and page_record.request_sha256 == record.page_sha256
            ):
                page = self._page(page_record, operation_id, control)
                disposed = set(page.disposed)
                if isinstance(response.result, CleanupProgress):
                    positions = {
                        (item.object_path, item.kind, item.token): index
                        for index, item in enumerate(page.request.inventory.candidates)
                    }
                    for item in response.result.items:
                        if item.outcome == "disposed":
                            disposed.add(
                                positions[
                                    (
                                        item.candidate.object_path,
                                        item.candidate.kind,
                                        item.candidate.token,
                                    )
                                ]
                            )
                page_record.disposed_json = _json(sorted(disposed))
            record.response_json = encoded
            record.unresolved = response.execution != "closed"

    def acknowledge_response(
        self, collection_id: int, operation_id: str, response: CleanupExecutionResult
    ) -> None:
        """Retire only the exact closed response received by its independent owner."""
        response = CleanupExecutionResult.model_validate_json(
            response.model_dump_json(exclude_none=True)
        )
        with session_scope(self._factory) as session:
            control, _ = self._context(session, collection_id, operation_id)
            record, invocation = self._attempt(session, operation_id, response)
            self._require_scope(invocation.request, control, operation_id)
            if record.unresolved or record.response_json != response.model_dump_json(
                exclude_none=True
            ):
                raise Conflict("cleanup attempt has no acknowledged exact closed response")
            session.delete(record)

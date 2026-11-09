"""Bounded original provider receiving shared by independently authorized scopes."""

from __future__ import annotations

import secrets
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    CleanupExecutionResult,
    CleanupInventoryProgress,
    CleanupInventoryRequest,
    CleanupInventoryStepRequest,
    CleanupStepRequest,
)
from sqlalchemy.orm import Session

from riverhog_core.cleanup_execution_tuning import CleanupExecutionTuning
from riverhog_core.ports.object_cleanup import ObjectCleanupPort
from riverhog_core.services.cleanup_attempts import RetainedCleanupAttempt
from riverhog_core.services.object_cleanup_receiving import (
    ObjectCleanupReceiving,
    ReceivedCleanupResponse,
)

type CleanupTurnProgress = Literal["pending", "inventory-exhausted", "busy"]


@dataclass(slots=True)
class _Pending:
    ticket: RetainedCleanupAttempt
    after: str | None
    response: CleanupExecutionResult | None = None
    received: ReceivedCleanupResponse | None = None
    selection: CleanupStepRequest | None = None
    selected: bool = False


class ObjectCleanupTurn:
    """One bounded provider call with exact original reply/page/ack receiving.

    The semantic scope owner supplies current authority and exact admission.
    Inventory exhaustion is intermediate evidence, never a disposition or
    global terminal result. This unit owns no scheduler, physical worker or
    independent resource allowance.
    """

    def __init__(
        self,
        owner: ObjectCleanupReceiving,
        target: ObjectCleanupPort,
        *,
        collection_id: int,
        operation_id: str,
        target_incarnation: str,
        object_prefix: str,
        guard: Callable[[Session], None],
        tuning: CleanupExecutionTuning,
    ) -> None:
        if not callable(guard):
            raise ValueError("cleanup execution requires a current disposal guard")
        self._collection_id = collection_id
        self._operation = operation_id
        self._target_incarnation = target_incarnation
        self._path = object_prefix
        self._owner = owner
        self._target = target
        self._guard = guard
        self._tuning = tuning
        self._dispatch_owner = secrets.token_hex(32)
        self._pending: _Pending | None = None
        self._lock = Lock()

    @property
    def has_pending(self) -> bool:
        return self._pending is not None

    def advance(self) -> CleanupTurnProgress:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._pending is not None:
                self._receive()
                return "pending"
            try:
                page = self._owner.current(self._collection_id, self._operation)
            except NotFound:
                page = None
            if page is not None and page.complete and page.request.inventory.exhausted:
                return "inventory-exhausted"
            tuning = self._tuning
            if page is None or page.complete:
                after = page.request_sha256 if page is not None else None
                continuation = page.request.inventory.continuation if page is not None else None
                ticket = self._owner.arm_inventory(
                    self._collection_id,
                    self._operation,
                    request=CleanupInventoryStepRequest(
                        selected_terminal_sha256=self._operation,
                        inventory=CleanupInventoryRequest(
                            object_prefix=self._path,
                            target_incarnation=self._target_incarnation,
                            maximum_items=tuning.inventory_items,
                            maximum_response_bytes=tuning.inventory_bytes,
                            continuation=continuation,
                        ),
                        maximum_provider_requests=tuning.provider_requests,
                        maximum_control_bytes=tuning.control_bytes,
                        maximum_in_flight=tuning.in_flight,
                    ),
                    after=after,
                    dispatch_owner=self._dispatch_owner,
                    guard=self._guard,
                )
            else:
                after = page.request_sha256
                ticket = self._owner.arm(
                    self._collection_id,
                    self._operation,
                    page_sha256=after,
                    dispatch_owner=self._dispatch_owner,
                    maximum_provider_requests=tuning.provider_requests,
                    maximum_control_bytes=tuning.control_bytes,
                    maximum_in_flight=tuning.in_flight,
                    guard=self._guard,
                )
            self._pending = _Pending(ticket, after)
            self._receive()
            return "pending"
        finally:
            self._lock.release()

    def _receive(self) -> None:
        pending = self._pending
        assert pending is not None
        if pending.response is None:
            # The original ticket retains even an ambiguous local dispatch failure.
            pending.response = self._owner.execute(pending.ticket, self._target)
        if pending.received is None:
            pending.received = self._owner.record_response(
                self._collection_id, self._operation, pending.response
            )
        if isinstance(pending.response.result, CleanupInventoryProgress) and not pending.selected:
            if pending.selection is None:
                tuning = self._tuning
                pending.selection = CleanupStepRequest(
                    selected_terminal_sha256=self._operation,
                    inventory=pending.response.result.inventory,
                    maximum_provider_requests=tuning.provider_requests,
                    maximum_control_bytes=tuning.control_bytes,
                    maximum_in_flight=tuning.in_flight,
                )
            request = pending.ticket.invocation.request
            assert isinstance(request, CleanupInventoryStepRequest)
            try:
                self._owner.select_page(
                    self._collection_id,
                    self._operation,
                    inventory_call_id=pending.ticket.invocation.call.call_id,
                    inventory_request=request.inventory,
                    request=pending.selection,
                    after=pending.after,
                    guard=self._guard,
                )
            except Conflict as error:
                try:
                    current = self._owner.current(self._collection_id, self._operation)
                except NotFound:
                    raise error from None
                if current.request_sha256 == pending.after:
                    raise
                # Another original inventory already selected a positive retained
                # page in this exact scope. Receive this read independently, then
                # follow that page through fresh current admission next quantum.
            pending.selected = True
        if pending.response.execution == "closed":
            self._owner.acknowledge_response(pending.received)
        # UNKNOWN remains durably charged. A later fresh monotone attempt cannot
        # close it, and finish still requires every original receiving row gone.
        self._pending = None

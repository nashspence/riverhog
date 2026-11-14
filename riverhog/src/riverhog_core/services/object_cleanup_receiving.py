"""Bounded original receiving inside an independently authorized cleanup scope.

Each concrete owner validates its current rights, original terminal and target
before admission. Shared storage and grants convey no cross-scope authority.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field, replace

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
    validate_cleanup_execution,
    validate_cleanup_inventory_response,
)
from sqlalchemy.orm import Session

from riverhog_core.archive_cleanup_grants import ArchiveCleanupGrants
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_object_cleanup_models import (
    ObjectCleanupAttemptRecord,
    ObjectCleanupPageRecord,
)
from riverhog_core.ports.object_cleanup import ObjectCleanupPort
from riverhog_core.services.cleanup_attempts import (
    RetainedCleanupAttempt,
    cleanup_attempt_bytes,
)
from riverhog_core.services.terminal_cleanup_capacity import (
    CleanupCharge,
    change_cleanup_capacity,
)


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
class CleanupScope:
    scope_id: str
    operation_id: str
    target_incarnation: str
    object_prefix: str
    exact_object_path: str | None = None


@dataclass(frozen=True, slots=True)
class RetainedCleanupPage:
    request_sha256: str
    request: CleanupStepRequest = field(repr=False)
    disposed: tuple[int, ...]

    @property
    def complete(self) -> bool:
        return len(self.disposed) == len(self.request.inventory.candidates)


@dataclass(frozen=True, slots=True)
class ReceivedCleanupResponse:
    """Live evidence of exact committed receiving, never a dispatch capability.

    Its authenticator is private to one receiver instance and binds the complete
    original invocation and response. A cold owner must receive the original
    response against retained state; serialized fields cannot recreate evidence.
    """

    collection_id: int
    operation_id: str
    scope_id: str
    invocation_json: str = field(repr=False)
    response_json: str = field(repr=False)
    _signature: bytes = field(repr=False)


class ObjectCleanupReceiving(ABC):
    def __init__(self, factory: SessionFactory, *, grants: ArchiveCleanupGrants) -> None:
        self._factory = factory
        self._grants = grants
        self._receiving_key = secrets.token_bytes(32)

    @abstractmethod
    def _scope_id(self, operation_id: str) -> str: ...

    @abstractmethod
    def _context(
        self,
        session: Session,
        collection_id: int,
        operation_id: str,
        *,
        admit: bool = False,
        guard: Callable[[Session], None] | None = None,
    ) -> CleanupScope: ...

    @abstractmethod
    def _page_selected(self, session: Session, scope: CleanupScope) -> None: ...

    def _require_scope(
        self, request: CleanupRequest, scope: CleanupScope, operation_id: str
    ) -> None:
        if (
            scope.scope_id != self._scope_id(operation_id)
            or scope.operation_id != operation_id
            or request.selected_terminal_sha256 != operation_id
            or request.inventory.object_prefix != scope.object_prefix
            or request.inventory.target_incarnation != scope.target_incarnation
            or (
                isinstance(request, CleanupStepRequest)
                and scope.exact_object_path is not None
                and any(
                    item.object_path != scope.exact_object_path
                    for item in request.inventory.candidates
                )
            )
        ):
            raise Conflict("cleanup selection differs from its original target scope")

    def _page_bytes(
        self, operation_id: str, request: CleanupStepRequest, inventory_request_json: str
    ) -> int:
        # Header names conservatively cover the per-row identity overhead too.
        return len(
            _json(
                {
                    "terminal_id": operation_id,
                    "scope_id": self._scope_id(operation_id),
                    "request_sha256": cleanup_step_sha256(request),
                    "inventory_call_id": "0" * 64,
                    "inventory_request": json.loads(inventory_request_json),
                    "request": request.model_dump(mode="json"),
                    "disposed": list(range(len(request.inventory.candidates))),
                }
            ).encode()
        )

    def _page(
        self, record: ObjectCleanupPageRecord, operation_id: str, scope: CleanupScope
    ) -> RetainedCleanupPage:
        request = CleanupStepRequest.model_validate_json(record.request_json)
        self._require_scope(request, scope, operation_id)
        disposed = json.loads(record.disposed_json)
        if (
            (record.terminal_id != operation_id or record.scope_id != self._scope_id(operation_id))
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
        page = session.get(ObjectCleanupPageRecord, self._scope_id(operation_id))
        previous_bytes = (
            self._page_bytes(
                operation_id,
                CleanupStepRequest.model_validate_json(page.request_json),
                page.inventory_request_json,
            )
            if page is not None
            else 0
        )
        change_cleanup_capacity(
            session,
            operation_id,
            grants=self._grants,
            removed=CleanupCharge(control_bytes=previous_bytes),
            added=CleanupCharge(
                calls=int(invocation is not None),
                control_bytes=page_bytes
                + (
                    cleanup_attempt_bytes(
                        invocation, operation_id, scope_id=self._scope_id(operation_id)
                    )
                    if invocation is not None
                    else 0
                ),
                provider_requests=(
                    invocation.request.maximum_provider_requests if invocation is not None else 0
                ),
                in_flight=invocation.request.maximum_in_flight if invocation is not None else 0,
            ),
        )

    def _predecessor(
        self,
        record: ObjectCleanupPageRecord | None,
        inventory_request: CleanupInventoryRequest,
        after: str | None,
        operation_id: str,
        scope: CleanupScope,
    ) -> None:
        if record is None:
            if after is not None or inventory_request.continuation is not None:
                raise Conflict("cleanup first selection has no retained predecessor")
            return
        prior = self._page(record, operation_id, scope)
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
            scope = self._context(session, collection_id, operation_id, admit=True, guard=guard)
            self._require_scope(request, scope, operation_id)
            page_record = session.get(ObjectCleanupPageRecord, self._scope_id(operation_id))
            self._predecessor(page_record, request.inventory, after, operation_id, scope)
            page_bytes = (
                self._page_bytes(
                    operation_id,
                    self._page(page_record, operation_id, scope).request,
                    page_record.inventory_request_json,
                )
                if page_record is not None
                else 0
            )
            self._reserve(session, operation_id, page_bytes=page_bytes, invocation=invocation)
            session.add(
                ObjectCleanupAttemptRecord(
                    id=invocation.call.call_id,
                    terminal_id=operation_id,
                    scope_id=self._scope_id(operation_id),
                    page_sha256=after,
                    dispatch_sha256=_sha(nonce),
                    invocation_json=invocation.model_dump_json(exclude_none=True),
                    unresolved=True,
                    control_bytes=format_scalar(
                        "nonnegative",
                        cleanup_attempt_bytes(
                            invocation, operation_id, scope_id=self._scope_id(operation_id)
                        ),
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
            scope = self._context(session, collection_id, operation_id, admit=True, guard=guard)
            self._require_scope(request, scope, operation_id)
            record = session.get(ObjectCleanupPageRecord, self._scope_id(operation_id))
            if record is not None:
                prior = self._page(record, operation_id, scope)
                if record.inventory_call_id == inventory_call_id:
                    if (
                        prior.request_sha256 != request_sha256
                        or record.request_json != encoded
                        or record.inventory_request_json != inventory_encoded
                    ):
                        raise Conflict("cleanup selection preimage changed")
                    return prior
            self._predecessor(record, inventory_request, after, operation_id, scope)
            observed = session.get(ObjectCleanupAttemptRecord, inventory_call_id)
            if observed is None or (
                observed.terminal_id != operation_id
                or observed.scope_id != self._scope_id(operation_id)
            ):
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
                record = ObjectCleanupPageRecord(
                    scope_id=self._scope_id(operation_id), terminal_id=operation_id
                )
                session.add(record)
            record.request_sha256 = request_sha256
            record.inventory_call_id = inventory_call_id
            record.inventory_request_json = inventory_encoded
            record.request_json = encoded
            record.disposed_json = "[]"
            self._page_selected(session, scope)
            return self._page(record, operation_id, scope)

    def current(self, collection_id: int, operation_id: str) -> RetainedCleanupPage:
        """Observe original bounded progress; this grants no new provider work."""
        with session_scope(self._factory) as session:
            scope = self._context(session, collection_id, operation_id)
            record = session.get(ObjectCleanupPageRecord, self._scope_id(operation_id))
            if record is None:
                raise NotFound("upload terminal cleanup page is unavailable")
            return self._page(record, operation_id, scope)

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
            scope = self._context(session, collection_id, operation_id, admit=True, guard=guard)
            record = session.get(ObjectCleanupPageRecord, self._scope_id(operation_id))
            if record is None:
                raise NotFound("upload terminal cleanup page is unavailable")
            page = self._page(record, operation_id, scope)
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
                ObjectCleanupAttemptRecord(
                    id=invocation.call.call_id,
                    terminal_id=operation_id,
                    scope_id=self._scope_id(operation_id),
                    page_sha256=page.request_sha256,
                    dispatch_sha256=_sha(nonce),
                    invocation_json=invocation.model_dump_json(exclude_none=True),
                    unresolved=True,
                    control_bytes=format_scalar(
                        "nonnegative",
                        cleanup_attempt_bytes(
                            invocation, operation_id, scope_id=self._scope_id(operation_id)
                        ),
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
        self, ticket: RetainedCleanupAttempt, target: ObjectCleanupPort
    ) -> CleanupExecutionResult:
        """Check the original capability, then execute outside the catalog writer."""
        with session_scope(self._factory) as session:
            scope = self._context(session, ticket.collection_id, ticket.operation_id)
            self._require_scope(ticket.invocation.request, scope, ticket.operation_id)
            record = session.get(ObjectCleanupAttemptRecord, ticket.invocation.call.call_id)
            if (
                record is None
                or record.terminal_id != ticket.operation_id
                or record.scope_id != self._scope_id(ticket.operation_id)
                or record.dispatch_sha256 != _sha(ticket._nonce)
                or record.invocation_json != ticket.invocation.model_dump_json(exclude_none=True)
            ):
                raise Conflict("original live cleanup dispatch capability changed")
        return ticket._dispatch(target)

    def _attempt(
        self,
        session: Session,
        operation_id: str,
        response: CleanupExecutionResult,
    ) -> tuple[ObjectCleanupAttemptRecord, CleanupInvocation]:
        record = session.get(ObjectCleanupAttemptRecord, response.call.call_id)
        if record is None or (
            record.terminal_id != operation_id or record.scope_id != self._scope_id(operation_id)
        ):
            raise NotFound("original cleanup attempt is unavailable")
        invocation = CleanupInvocation.model_validate_json(record.invocation_json)
        if isinstance(invocation.request, CleanupStepRequest) and record.page_sha256 is None:
            raise Conflict("retained disposal attempt has no original selection")
        if invocation.call.call_id != record.id:
            raise Conflict("retained cleanup attempt identity changed")
        validate_cleanup_execution(invocation, response)
        return record, invocation

    def _receiving_signature(self, received: ReceivedCleanupResponse) -> bytes:
        return hmac.digest(
            self._receiving_key,
            canonical_json_bytes(
                {
                    "collection_id": received.collection_id,
                    "operation_id": received.operation_id,
                    "scope_id": received.scope_id,
                    "invocation_json": received.invocation_json,
                    "response_json": received.response_json,
                }
            ),
            "sha256",
        )

    def record_response(
        self, collection_id: int, operation_id: str, response: CleanupExecutionResult
    ) -> ReceivedCleanupResponse:
        """Commit the exact trusted response before issuing live receiving evidence."""
        response = CleanupExecutionResult.model_validate_json(
            response.model_dump_json(exclude_none=True)
        )
        encoded = response.model_dump_json(exclude_none=True)
        with session_scope(self._factory) as session:
            scope = self._context(session, collection_id, operation_id)
            record, invocation = self._attempt(session, operation_id, response)
            self._require_scope(invocation.request, scope, operation_id)
            if record.response_json is not None:
                prior = CleanupExecutionResult.model_validate_json(record.response_json)
                if (
                    (prior.execution == "closed" and prior != response)
                    or (prior.result is not None and prior.result != response.result)
                    or (prior.result is not None and response.outcome != prior.outcome)
                ):
                    raise Conflict("cleanup response conflicts with its retained original")
            page_record = session.get(ObjectCleanupPageRecord, self._scope_id(operation_id))
            if (
                isinstance(invocation.request, CleanupStepRequest)
                and page_record is not None
                and page_record.request_sha256 == record.page_sha256
            ):
                page = self._page(page_record, operation_id, scope)
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

        # Construct the receipt only after the receiving transaction committed.
        received = ReceivedCleanupResponse(
            collection_id,
            operation_id,
            self._scope_id(operation_id),
            invocation.model_dump_json(exclude_none=True),
            encoded,
            b"",
        )
        return replace(received, _signature=self._receiving_signature(received))

    def acknowledge_response(self, received: ReceivedCleanupResponse) -> None:
        """Retire only positively received original closure, including lost replies."""
        if not isinstance(received, ReceivedCleanupResponse) or not hmac.compare_digest(
            received._signature, self._receiving_signature(received)
        ):
            raise Conflict("cleanup response has no original live receiving evidence")
        collection_id, operation_id = received.collection_id, received.operation_id
        response = CleanupExecutionResult.model_validate_json(received.response_json)
        original = CleanupInvocation.model_validate_json(received.invocation_json)
        validate_cleanup_execution(original, response)
        if response.execution != "closed":
            raise Conflict("cleanup attempt has no acknowledged exact closed response")
        with session_scope(self._factory) as session:
            scope = self._context(session, collection_id, operation_id)
            if received.scope_id != scope.scope_id:
                raise Conflict("cleanup receiving evidence has another original scope")
            self._require_scope(original.request, scope, operation_id)
            record = session.get(ObjectCleanupAttemptRecord, response.call.call_id)
            if record is None:
                # This live receipt already proves exact closed receiving. Absence
                # alone supplies neither that fact nor authority to release a charge.
                return
            record, invocation = self._attempt(session, operation_id, response)
            if (
                invocation != original
                or record.unresolved
                or record.response_json != received.response_json
            ):
                raise Conflict("cleanup attempt has no acknowledged exact closed response")
            change_cleanup_capacity(
                session,
                operation_id,
                grants=self._grants,
                removed=CleanupCharge(
                    calls=1,
                    control_bytes=_charge(record.control_bytes),
                    provider_requests=_charge(record.provider_requests),
                    in_flight=_charge(record.in_flight),
                ),
            )
            session.delete(record)

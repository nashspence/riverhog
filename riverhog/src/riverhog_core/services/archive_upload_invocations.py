"""Retain original payload invocations separately from their physical workers."""

from __future__ import annotations

import hashlib
import json
import re
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from threading import Lock
from typing import Literal, Protocol

from riverhog_protocol.errors import Conflict
from sqlalchemy.exc import OperationalError

from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.services.archive_construction_retention import (
    ArchiveConstructionReservation,
    ArchiveConstructionRetention,
)
from riverhog_core.services.archive_finalizer_terminal import TerminalDrainStatus
from riverhog_core.services.archive_payload_input import ArchivePayloadInput
from riverhog_core.services.archive_upload_drain import inspect_terminal_write_records
from riverhog_core.services.archive_upload_execution import (
    ArchiveExecutionOccupied,
    ArchiveUploadExecutionCompletion,
    ArchiveUploadExecutionKey,
    ArchiveUploadExecutionOwner,
    Submission,
)

InvocationSubmission = Submission | Literal["retention-capacity", "target-retention-capacity"]
InvocationTerminalSubmission = InvocationSubmission | Literal["untracked", "drained", "pending"]


class RetainedArchiveUploadInvocation(Protocol):
    @property
    def has_pending(self) -> bool: ...

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus: ...


@dataclass(frozen=True, slots=True)
class ArchiveUploadInvocationCompletion:
    request_sha256: str
    execution: ArchiveUploadExecutionCompletion = field(repr=False, compare=False)

    def result(self) -> object:
        return self.execution.result()


@dataclass(slots=True)
class _PinLifetime:
    registry: ArchiveUploadInvocationRegistry | None
    owner: object | None


@dataclass(frozen=True, slots=True, eq=False)
class ArchiveUploadInvocationPin:
    """Retain one original invocation; this grants no provider or closure right."""

    key: ArchiveUploadExecutionKey
    target_incarnation: str
    epoch: str
    volume_id: str
    request_sha256: str
    _lifetime: _PinLifetime = field(repr=False, compare=False)

    @property
    def active(self) -> bool:
        return self._lifetime.registry is not None

    def release(self, *, owner: object) -> bool:
        registry = self._lifetime.registry
        if registry is None:
            return True
        return registry._release_pin(self, owner=owner)


@dataclass(slots=True)
class _Construction:
    key: ArchiveUploadExecutionKey
    target: str
    requests: deque[str] = field(default_factory=deque)
    terminal_id: str | None = None
    remaining: int = 0
    terminal_turn: bool = True


@dataclass(slots=True)
class _Invocation:
    request_sha256: str
    construction: _Construction
    reservation: ArchiveConstructionReservation
    create: Callable[[], RetainedArchiveUploadInvocation] = field(repr=False)
    advance: Callable[[RetainedArchiveUploadInvocation], object] = field(repr=False)
    payload: ArchivePayloadInput | None = field(default=None, repr=False)
    input_token: object = field(default_factory=object, repr=False)
    lock: Lock = field(default_factory=Lock)
    owner: RetainedArchiveUploadInvocation | None = None
    construction_failure: BaseException | None = None
    received: ArchiveUploadExecutionCompletion | None = None
    terminal_received: ArchiveUploadExecutionCompletion | None = None
    succeeded: bool = False
    drained: bool = False
    family_pin: ArchiveUploadInvocationPin | None = None


class ArchiveUploadInvocationRegistry:
    """Bound original constructors, arguments and receiving with the shared budget.

    The service supplies a commitment to the exact semantic request and retains
    its current admission checks in the original actor. These labels grant no
    provider authority. One construction has one physical turn at a time;
    independently retained units and metadata owners spend the same retention
    pool. This serial physical profile permits independent constructions to run.
    """

    def __init__(
        self,
        factory: SessionFactory,
        execution: ArchiveUploadExecutionOwner,
        retention: ArchiveConstructionRetention,
    ) -> None:
        self._input_owner = object()
        self._factory = factory
        self._execution = execution
        self._retention = retention
        self._lock = Lock()
        self._closed = False
        self._constructions: dict[int, _Construction] = {}
        self._invocations: OrderedDict[str, _Invocation] = OrderedDict()

    def _construction(self, key: ArchiveUploadExecutionKey, target: str) -> _Construction | None:
        prior = self._constructions.get(key.collection_id)
        if prior is not None and (prior.key != key or prior.target != target):
            raise Conflict("retained upload has another original construction or target")
        return prior

    def submit(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        request_sha256: str,
        create: Callable[[], RetainedArchiveUploadInvocation],
        advance: Callable[[RetainedArchiveUploadInvocation], object],
        payload: ArchivePayloadInput | None = None,
    ) -> InvocationSubmission:
        """Transfer the exact input, or withdraw only this unused incoming body."""
        try:
            if payload is not None:
                payload.require_submission(
                    self._input_owner, key, target_incarnation, request_sha256
                )
            return self._submit(
                key,
                target_incarnation=target_incarnation,
                request_sha256=request_sha256,
                create=create,
                advance=advance,
                payload=payload,
            )
        finally:
            if payload is not None:
                payload.discard_unretained(self._input_owner)

    def _submit(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        request_sha256: str,
        create: Callable[[], RetainedArchiveUploadInvocation],
        advance: Callable[[RetainedArchiveUploadInvocation], object],
        payload: ArchivePayloadInput | None,
    ) -> InvocationSubmission:
        if not target_incarnation or re.fullmatch(r"[a-f0-9]{64}", request_sha256) is None:
            raise ValueError("upload invocation requires exact target and request identities")
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._closed:
                return "closed"
            construction = self._construction(key, target_incarnation)
            if construction is not None and construction.terminal_id is not None:
                raise Conflict("retained upload construction is fenced for its terminal request")
            context = self._invocations.get(request_sha256)
            if context is not None and context.construction is not construction:
                raise Conflict("upload invocation identity belongs to another construction")
            if context is None and payload is not None and payload.released:
                raise Conflict("a retired payload input cannot construct another invocation")
            fresh = context is None
            if context is None:
                if self._execution.status(key) != "untracked":
                    return "busy"
                reservation = self._retention.try_retain(
                    key, target_incarnation=target_incarnation, owner=self
                )
                if isinstance(reservation, str):
                    return reservation
                if construction is None:
                    construction = _Construction(key, target_incarnation)
                    self._constructions[key.collection_id] = construction
                context = _Invocation(
                    request_sha256, construction, reservation, create, advance, payload
                )
                if payload is not None:
                    payload.retain(self._input_owner, context.input_token)
                self._invocations[request_sha256] = context
                construction.requests.append(request_sha256)
                construction.remaining += 1
            elif context.succeeded and context.received is not None:
                return "retained"
            selected = context
            try:
                outcome = self._execution.try_submit(
                    key,
                    target_incarnation=target_incarnation,
                    terminal=False,
                    work=lambda: self._advance(selected),
                    owner=self,
                    identity=selected,
                )
            except ArchiveExecutionOccupied:
                if fresh:
                    self._remove(selected)
                raise
            except BaseException:
                if fresh and self._execution.status(key) == "untracked":
                    self._remove(selected)
                raise
            if outcome == "submitted":
                context.received = None
                context.succeeded = False
            elif fresh and outcome not in {"retained"}:
                self._remove(context)
            return outcome
        finally:
            self._lock.release()

    def _advance(self, context: _Invocation) -> object:
        with context.lock:
            with self._lock:
                if self._closed or context.construction.terminal_id is not None:
                    raise Conflict("upload invocation cannot start forward work")
            if context.construction_failure is not None:
                raise Conflict("original upload constructor outcome is unresolved")
            if context.owner is None:
                try:
                    original_owner: RetainedArchiveUploadInvocation | None = context.create()
                    if original_owner is None:
                        raise ValueError("original upload constructor returned no receiver")
                    context.owner = original_owner
                except BaseException as error:
                    context.construction_failure = error
                    raise
            with self._lock:
                if self._closed or context.construction.terminal_id is not None:
                    raise Conflict("upload invocation was fenced during construction")
            return context.advance(context.owner)

    def collect(self, *, limit: int) -> tuple[ArchiveUploadInvocationCompletion, ...]:
        if type(limit) is not int or limit < 1:
            raise ValueError("upload invocation receiving limit must be positive")
        if not self._lock.acquire(blocking=False):
            return ()
        try:
            outcomes = self._execution.collect(limit=limit, owner=self)
            received = []
            for outcome in outcomes:
                context = outcome.identity
                if (
                    not isinstance(context, _Invocation)
                    or self._invocations.get(context.request_sha256) is not context
                    or context.construction.key != outcome.key
                    or context.construction.target != outcome.target_incarnation
                ):
                    raise Conflict("upload execution returned another original invocation")
                if context.payload is not None:
                    outcome.release_exception_frames()
                if outcome.terminal:
                    context.terminal_received = outcome
                else:
                    context.received = outcome
                    try:
                        outcome.result()
                    except BaseException:
                        context.succeeded = False
                    else:
                        context.succeeded = True
                if context.payload is not None:
                    context.payload.receive_joined(context.input_token, context.owner)
                received.append(ArchiveUploadInvocationCompletion(context.request_sha256, outcome))
            return tuple(received)
        finally:
            self._lock.release()

    def completed(
        self, request_sha256: str
    ) -> ArchiveUploadInvocationCompletion | Literal["busy"] | None:
        """Point receiving only; it creates neither work nor a live capability."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            context = self._invocations.get(request_sha256)
            if context is None or context.received is None:
                return None
            return ArchiveUploadInvocationCompletion(request_sha256, context.received)
        finally:
            self._lock.release()

    def pin_family(
        self,
        request_sha256: str,
        *,
        key: ArchiveUploadExecutionKey,
        target_incarnation: str,
        epoch: str,
        volume_id: str,
        owner: object,
    ) -> ArchiveUploadInvocationPin | Literal["busy", "untracked"]:
        """Pin the first original payload before constructing its cache family."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            context = self._invocations.get(request_sha256)
            if context is None:
                return "untracked"
            if self._closed or context.construction.terminal_id is not None or context.drained:
                raise Conflict("upload invocation cannot acquire a fresh family pin")
            if context.payload is None:
                raise Conflict("family pin requires the exact original payload input")
            request = context.payload.request
            if (
                context.construction.key != key
                or context.construction.target != target_incarnation
                or request.epoch != epoch
                or request.volume_id != volume_id
            ):
                raise Conflict("family pin differs from its original payload scope")
            if context.family_pin is not None:
                if context.family_pin._lifetime.owner is not owner:
                    raise Conflict("family pin belongs to another original owner")
                return context.family_pin
            pin = ArchiveUploadInvocationPin(
                key,
                target_incarnation,
                epoch,
                volume_id,
                request_sha256,
                _PinLifetime(self, owner),
            )
            context.family_pin = pin
            return pin
        finally:
            self._lock.release()

    def _release_pin(self, pin: ArchiveUploadInvocationPin, *, owner: object) -> bool:
        if not self._lock.acquire(blocking=False):
            return False
        try:
            if pin._lifetime.registry is None:
                return True
            context = self._invocations.get(pin.request_sha256)
            if (
                context is None
                or context.family_pin is not pin
                or pin._lifetime.registry is not self
                or pin._lifetime.owner is not owner
            ):
                raise Conflict("family pin is not this original receiving owner")
            if self._execution.status(context.construction.key) != "untracked":
                return False
            context.family_pin = None
            pin._lifetime.owner = None
            pin._lifetime.registry = None
            return True
        finally:
            self._lock.release()

    def retire_idle(self, completion: ArchiveUploadInvocationCompletion) -> bool:
        if not self._lock.acquire(blocking=False):
            return False
        try:
            context = self._invocations.get(completion.request_sha256)
            if context is None or context.received is not completion.execution:
                raise Conflict("upload retirement is not its original received completion")
            return self._retire_received(context)
        finally:
            self._lock.release()

    def retire_ready(self, *, limit: int) -> int:
        """Retire only exact joined successful originals, in bounded fair turns."""
        if type(limit) is not int or limit < 1:
            raise ValueError("upload invocation retirement limit must be positive")
        if not self._lock.acquire(blocking=False):
            return 0
        try:
            retired = 0
            for _ in range(min(limit, len(self._invocations))):
                request, context = self._invocations.popitem(last=False)
                self._invocations[request] = context
                if context.received is not None:
                    retired += self._retire_received(context)
            return retired
        finally:
            self._lock.release()

    def _retire_received(self, context: _Invocation) -> bool:
        if (
            self._closed
            or context.construction.terminal_id is not None
            or not context.succeeded
            or context.family_pin is not None
            or self._execution.status(context.construction.key) != "untracked"
            or not context.lock.acquire(blocking=False)
        ):
            return False
        try:
            if (
                context.construction_failure is not None
                or context.owner is None
                or context.owner.has_pending
                or context.payload is not None
                and not context.payload.released
            ):
                return False
            return self._remove(context)
        finally:
            context.lock.release()

    def terminal_turn(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        operation_id: str,
    ) -> Literal["busy", "untracked", "selected", "yielded"]:
        """Alternate service work kinds using the charged original group only."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            group = self._construction(key, target_incarnation)
            if group is None:
                return "untracked"
            if group.terminal_id != operation_id:
                raise Conflict("upload terminal turn differs from the selected original request")
            selected = group.terminal_turn
            group.terminal_turn = not selected
            return "selected" if selected else "yielded"
        finally:
            self._lock.release()

    def retire_unstarted(self, request_sha256: str) -> bool:
        """Release only a retained refusal whose constructor never ran."""
        if not self._lock.acquire(blocking=False):
            return False
        try:
            context = self._invocations.get(request_sha256)
            if (
                context is None
                or self._closed
                or context.family_pin is not None
                or context.construction.terminal_id is not None
                or self._execution.status(context.construction.key) != "untracked"
                or not context.lock.acquire(blocking=False)
            ):
                return False
            try:
                if (
                    context.owner is not None
                    or context.construction_failure is not None
                    or context.received is not None
                ):
                    return False
                return self._remove(context)
            finally:
                context.lock.release()
        finally:
            self._lock.release()

    def _remove(self, context: _Invocation) -> bool:
        if context.family_pin is not None:
            return False
        if not self._retention.retire(context.reservation, owner=self):
            return False
        if context.payload is not None and not context.payload.released:
            assert context.owner is None and context.construction_failure is None
            context.payload.retire_unstarted(context.input_token)
        del self._invocations[context.request_sha256]
        group = context.construction
        group.requests.remove(context.request_sha256)
        group.remaining -= not context.drained
        if not group.requests:
            del self._constructions[group.key.collection_id]
        return True

    def fence_terminal(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        operation_id: str,
        defer_if_busy: bool = False,
    ) -> Literal["busy", "untracked", "fenced", "drained"]:
        try:
            with session_scope(self._factory) as session:
                inspect_terminal_write_records(
                    session, key.collection_id, operation_id, nowait=defer_if_busy
                )
                control = session.get(ArchiveWriteControlRecord, key.collection_id)
                assert control is not None
                if (
                    hashlib.sha256(control.construction_json.encode()).hexdigest()
                    != key.construction_sha256
                    or json.loads(control.construction_json)["archive_incarnation_id"]
                    != target_incarnation
                ):
                    raise Conflict("retained upload terminal construction changed")
        except OperationalError as error:
            if defer_if_busy and catalog_write_contention(error):
                return "busy"
            raise
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            group = self._construction(key, target_incarnation)
            if group is None:
                return "untracked"
            if group.terminal_id not in {None, operation_id}:
                raise Conflict("retained upload has another terminal request")
            group.terminal_id = operation_id
            if group.remaining == 0 and self._execution.status(key) == "untracked":
                return "drained"
            return "fenced"
        finally:
            self._lock.release()

    def submit_terminal(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        operation_id: str,
        limit: int = 1,
        defer_if_busy: bool = False,
    ) -> InvocationTerminalSubmission:
        if type(limit) is not int or limit < 1:
            raise ValueError("upload invocation drainage limit must be positive")
        fenced = self.fence_terminal(
            key,
            target_incarnation=target_incarnation,
            operation_id=operation_id,
            defer_if_busy=defer_if_busy,
        )
        if fenced in {"busy", "untracked", "drained"}:
            return fenced
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._closed:
                return "closed"
            group = self._construction(key, target_incarnation)
            assert group is not None
            if self._execution.status(key) != "untracked":
                return "busy"
            for _ in range(min(limit, len(group.requests))):
                request = group.requests.popleft()
                group.requests.append(request)
                context = self._invocations[request]
                if context.drained:
                    continue
                selected = context
                return self._execution.try_submit(
                    key,
                    target_incarnation=target_incarnation,
                    terminal=True,
                    work=partial(self._drain, selected, operation_id),
                    owner=self,
                    identity=selected,
                )
            return "pending"
        finally:
            self._lock.release()

    def _drain(self, context: _Invocation, operation_id: str) -> TerminalDrainStatus:
        with context.lock:
            if context.construction_failure is not None:
                return "pending"
            result = (
                "closed" if context.owner is None else context.owner.drain_terminal(operation_id)
            )
            if type(result) is not str or result not in {"busy", "pending", "closed"}:
                raise ValueError("original upload actor returned invalid drainage evidence")
            with self._lock:
                if result == "closed" and context.family_pin is not None:
                    result = "pending"
                if result == "closed" and not context.drained:
                    context.drained = True
                    context.construction.remaining -= 1
            return result

    def close(self) -> None:
        with self._lock:
            self._closed = True
        self._execution.close()

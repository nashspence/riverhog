"""Bound service-local finalizer retention independently of physical workers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
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
from riverhog_core.services.archive_upload_drain import inspect_terminal_write_records
from riverhog_core.services.archive_upload_execution import (
    ArchiveExecutionOccupied,
    ArchiveUploadExecutionCompletion,
    ArchiveUploadExecutionKey,
    ArchiveUploadExecutionOwner,
    Submission,
)

FinalizerSubmission = Submission | Literal["retention-capacity", "target-retention-capacity"]
TerminalSubmission = FinalizerSubmission | Literal["untracked", "drained"]


class RetainedArchiveFinalizer(Protocol):
    @property
    def has_pending(self) -> bool: ...

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus: ...


@dataclass(frozen=True, slots=True)
class ArchiveFinalizerTurn:
    selection: str
    progressed: bool


@dataclass(slots=True)
class _Context:
    key: ArchiveUploadExecutionKey
    target: str
    reservation: ArchiveConstructionReservation
    lock: Lock = field(default_factory=Lock)
    owner: RetainedArchiveFinalizer | None = None
    selection: str | None = None
    advance: Callable[[], bool] | None = None
    terminal_id: str | None = None
    drained: bool = False
    construction_failure: BaseException | None = None


class ArchiveFinalizerRegistry:
    """One retained original finalizer per construction, with explicit capacity.

    The submitting service owns construction/target/selection labels and supplies
    its qualified finalizer factory. Labels grant no catalog or provider rights:
    each semantic owner still validates its exact source and admission authority.
    A construction is registered before invoking any constructor or work callback.
    Pending originals remain charged after worker results have been received.
    """

    def __init__(
        self,
        factory: SessionFactory,
        execution: ArchiveUploadExecutionOwner,
        retention: ArchiveConstructionRetention,
    ) -> None:
        self._factory = factory
        self._execution = execution
        self._retention = retention
        self._lock = Lock()
        self._closed = False
        self._contexts: dict[int, _Context] = {}

    def _existing(self, key: ArchiveUploadExecutionKey, target: str) -> _Context | None:
        value = self._contexts.get(key.collection_id)
        if value is not None and (value.key != key or value.target != target):
            raise Conflict("retained finalizer has another original construction or target")
        return value

    def submit(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        selection: str,
        create: Callable[[], RetainedArchiveFinalizer],
        advance: Callable[[RetainedArchiveFinalizer], bool],
    ) -> FinalizerSubmission:
        if not selection or not target_incarnation:
            raise ValueError("retained finalizer requires its exact target and selection")
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._closed:
                return "closed"
            context = self._existing(key, target_incarnation)
            fresh = context is None
            if context is None:
                # Do not adopt an execution owned by another service work kind.
                if self._execution.status(key) != "untracked":
                    return "busy"
                reservation = self._retention.try_retain(
                    key, target_incarnation=target_incarnation, owner=self
                )
                if isinstance(reservation, str):
                    return reservation
                context = _Context(key, target_incarnation, reservation)
                self._contexts[key.collection_id] = context
            if context.terminal_id is not None:
                raise Conflict("retained finalizer is fenced for its terminal request")
            selected = context
            try:
                result = self._execution.try_submit(
                    key,
                    target_incarnation=target_incarnation,
                    terminal=False,
                    work=lambda: self._advance(selected, selection, create, advance),
                    owner=self,
                )
            except ArchiveExecutionOccupied:
                if fresh:
                    self._remove(selected)
                raise
            except BaseException:
                if fresh and self._execution.status(key) == "untracked":
                    self._remove(selected)
                raise
            if fresh and result not in {"submitted", "retained"}:
                self._remove(selected)
            return result
        finally:
            self._lock.release()

    def _advance(
        self,
        context: _Context,
        selection: str,
        create: Callable[[], RetainedArchiveFinalizer],
        advance: Callable[[RetainedArchiveFinalizer], bool],
    ) -> ArchiveFinalizerTurn:
        # Only the bounded execution worker acquires this potentially busy lock.
        with context.lock:
            with self._lock:
                if context.terminal_id is not None or self._closed:
                    raise Conflict("retained finalizer cannot start forward work")
            if context.construction_failure is not None:
                raise Conflict("original finalizer constructor outcome is unresolved")
            if context.owner is None or not context.owner.has_pending:
                context.selection = selection
                context.advance = None
                try:
                    original_owner: RetainedArchiveFinalizer | None = create()
                    if original_owner is None:
                        raise ValueError("original finalizer constructor returned no receiver")
                    context.owner = original_owner
                except BaseException as error:
                    context.construction_failure = error
                    raise
                original = context.owner
                context.advance = lambda: advance(original)
            # Constructor acknowledgement precedes execution, including a
            # terminal selection that raced with the constructor itself.
            with self._lock:
                if context.terminal_id is not None or self._closed:
                    raise Conflict("retained finalizer was fenced during construction")
            assert context.advance is not None and context.selection is not None
            # A pending original ignores fresh factories/arguments. Its original
            # selected continuation runs before another source is considered.
            return ArchiveFinalizerTurn(context.selection, context.advance())

    def collect(self, *, limit: int) -> tuple[ArchiveUploadExecutionCompletion, ...]:
        return self._execution.collect(limit=limit, owner=self)

    def fence_terminal(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        operation_id: str,
        defer_if_busy: bool = False,
    ) -> Literal["busy", "untracked", "fenced", "drained"]:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            context = self._existing(key, target_incarnation)
            if context is not None and context.terminal_id is not None:
                if context.terminal_id != operation_id:
                    raise Conflict("retained finalizer has another terminal request")
                # A semantic drain can return before its original worker exits.
                # Report drainage only after physical receiving removed that slot.
                return (
                    "drained"
                    if context.drained and self._execution.status(key) == "untracked"
                    else "fenced"
                )
        finally:
            self._lock.release()
        # Validate the exact selected request before installing a local fence.
        # No registry lock or provider IO is held during this bounded transaction.
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
                    raise Conflict("retained finalizer terminal construction changed")
        except OperationalError as exc:
            if defer_if_busy and catalog_write_contention(exc):
                return "busy"
            raise
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            context = self._existing(key, target_incarnation)
            if context is None:
                return "untracked"
            if context.terminal_id not in {None, operation_id}:
                raise Conflict("retained finalizer has another terminal request")
            context.terminal_id = operation_id
            return "fenced"
        finally:
            self._lock.release()

    def submit_terminal(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        operation_id: str,
        defer_if_busy: bool = False,
    ) -> TerminalSubmission:
        fenced = self.fence_terminal(
            key,
            target_incarnation=target_incarnation,
            operation_id=operation_id,
            defer_if_busy=defer_if_busy,
        )
        if fenced in {"busy", "untracked"}:
            return fenced
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._closed:
                return "closed"
            context = self._existing(key, target_incarnation)
            assert context is not None
            # Receiving has not joined the previous original worker yet.
            if self._execution.status(key) != "untracked":
                return "busy"
            if context.drained:
                return "drained"
            return self._execution.try_submit(
                key,
                target_incarnation=target_incarnation,
                terminal=True,
                work=lambda: self._drain(context, operation_id),
                owner=self,
            )
        finally:
            self._lock.release()

    def _drain(self, context: _Context, operation_id: str) -> TerminalDrainStatus:
        with context.lock:
            if context.construction_failure is not None:
                return "pending"
            result = (
                "closed" if context.owner is None else context.owner.drain_terminal(operation_id)
            )
            with self._lock:
                context.drained = result == "closed"
            return result

    def retire_idle(self, key: ArchiveUploadExecutionKey, *, target_incarnation: str) -> bool:
        """Release only an idle local owner after physical outcome receiving.

        Fenced or pending originals retain all source/crypto/input ownership.
        This does not dispose durable preparations or adjudicate a terminal.
        """
        if not self._lock.acquire(blocking=False):
            return False
        try:
            if self._closed:
                return False
            context = self._existing(key, target_incarnation)
            if context is None:
                return False
            if context.terminal_id is not None or self._execution.status(key) != "untracked":
                return False
            if not context.lock.acquire(blocking=False):
                return False
            try:
                if (
                    context.construction_failure is not None
                    or (context.owner is None and context.selection is not None)
                    or (context.owner is not None and context.owner.has_pending)
                ):
                    return False
                return self._remove(context)
            finally:
                context.lock.release()
        finally:
            self._lock.release()

    def _remove(self, context: _Context) -> bool:
        if not self._retention.retire(context.reservation, owner=self):
            return False
        del self._contexts[context.key.collection_id]
        return True

    def close(self) -> None:
        with self._lock:
            self._closed = True
        self._execution.close()

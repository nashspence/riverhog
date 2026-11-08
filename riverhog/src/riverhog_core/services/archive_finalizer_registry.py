"""Bound service-local finalizer retention independently of physical workers."""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from threading import Lock
from time import monotonic
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
from riverhog_core.services.archive_upload_drain import (
    LocalTerminalStatus,
    inspect_terminal_write_records,
)
from riverhog_core.services.archive_upload_execution import (
    ArchiveExecutionOccupied,
    ArchiveUploadExecutionCompletion,
    ArchiveUploadExecutionKey,
    ArchiveUploadExecutionOwner,
    Submission,
)

FinalizerSubmission = (
    Submission | Literal["retention-capacity", "target-retention-capacity", "original-pending"]
)
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
    resume_after: float = 0


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
        self._contexts: OrderedDict[int, _Context] = OrderedDict()

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
        resume_original: bool = True,
    ) -> FinalizerSubmission:
        """Register work, or explicitly defer a fresh turn behind an original.

        A caller with a new workflow claim uses resume_original=False. No old
        factory, callback, constructor or unreceived physical turn can then be
        adopted as that claim's registration. Existing originals keep their
        separate resume and terminal receiving paths.
        """
        if type(resume_original) is not bool:
            raise ValueError("finalizer original resumption selection must be boolean")
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
            if not resume_original:
                if self._execution.status(key) != "untracked":
                    if fresh:
                        self._remove(context)
                    return "busy"
                if not context.lock.acquire(blocking=False):
                    if fresh:
                        self._remove(context)
                    return "busy"
                try:
                    if (
                        context.construction_failure is not None
                        or (context.owner is None and context.selection is not None)
                        or (context.owner is not None and context.owner.has_pending)
                    ):
                        return "original-pending"
                finally:
                    context.lock.release()
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
                if not resume_original:
                    return "busy"
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

    def resume(
        self, key: ArchiveUploadExecutionKey, *, target_incarnation: str
    ) -> FinalizerSubmission:
        """Submit only the retained original; this path cannot install a constructor."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._closed:
                return "closed"
            context = self._existing(key, target_incarnation)
            if context is None:
                raise Conflict("original archive finalizer is unavailable")
            if context.terminal_id is not None:
                raise Conflict("retained finalizer is fenced for its terminal request")
            return self._execution.try_submit(
                key,
                target_incarnation=target_incarnation,
                terminal=False,
                work=lambda: self._resume_original(context),
                owner=self,
            )
        finally:
            self._lock.release()

    def _resume_original(self, context: _Context) -> ArchiveFinalizerTurn:
        with context.lock:
            with self._lock:
                if context.terminal_id is not None or self._closed:
                    raise Conflict("retained finalizer cannot start forward work")
            if context.construction_failure is not None:
                raise Conflict("original finalizer constructor outcome is unresolved")
            if (
                context.owner is None
                or context.advance is None
                or context.selection is None
                or not context.owner.has_pending
            ):
                raise Conflict("original archive finalizer is unavailable")
            return ArchiveFinalizerTurn(context.selection, context.advance())

    def process_ready(
        self,
        *,
        limit: int,
        retry_after: timedelta,
        retire: Callable[[ArchiveUploadExecutionKey, str], bool],
    ) -> int:
        """Resume pending originals and request idle retirement in bounded fair visits.

        The existing maintenance owner sets the retry cadence and interlocks
        retirement with current catalog admission. Its callback runs outside the
        registry lock, preserving parent-before-registry lock order. This map is
        already charged against construction retention; it is not a task queue.
        Actual physical receiving must precede another original continuation.
        Fenced contexts and unknown constructors retain their separate owners.
        """
        interval = retry_after.total_seconds()
        if type(limit) is not int or limit < 1 or interval <= 0:
            raise ValueError("finalizer resumption requires positive limits and retry interval")
        if not self._lock.acquire(blocking=False):
            return 0
        try:
            budget = min(limit, len(self._contexts))
        finally:
            self._lock.release()
        progressed = 0
        now = monotonic()
        for _ in range(budget):
            if not self._lock.acquire(blocking=False):
                return progressed
            retiring: tuple[ArchiveUploadExecutionKey, str] | None = None
            try:
                if self._closed or not self._contexts:
                    return progressed
                identity, context = self._contexts.popitem(last=False)
                self._contexts[identity] = context
                if (
                    context.terminal_id is not None
                    or context.construction_failure is not None
                    or self._execution.status(context.key) != "untracked"
                    or not context.lock.acquire(blocking=False)
                ):
                    continue
                try:
                    pending = (
                        context.owner is not None
                        and context.advance is not None
                        and context.selection is not None
                        and context.owner.has_pending
                    )
                finally:
                    context.lock.release()
                if not pending:
                    retiring = context.key, context.target
                elif context.resume_after <= now:
                    try:
                        result = self._execution.try_submit(
                            context.key,
                            target_incarnation=context.target,
                            terminal=False,
                            work=partial(self._resume_original, context),
                            owner=self,
                        )
                    except ArchiveExecutionOccupied:
                        # A separately owned payload may have acquired the shared
                        # slot since the status probe. Adopt no foreign work.
                        continue
                    if result == "submitted":
                        context.resume_after = now + interval
                        progressed += 1
            finally:
                self._lock.release()
            if retiring is not None:
                progressed += retire(*retiring)
        return progressed

    def collect(self, *, limit: int) -> tuple[ArchiveUploadExecutionCompletion, ...]:
        return self._execution.collect(limit=limit, owner=self)

    def local_terminal_status(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        operation_id: str,
    ) -> LocalTerminalStatus:
        """Observe local ownership without SQL, waiting, fencing or global authority.

        The composing owner validates the exact closed catalog selection
        separately. A missing context grants no global quiescence or disposal.
        """
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            context = self._existing(key, target_incarnation)
            if context is None:
                return "untracked"
            if context.terminal_id is None:
                return "unfenced"
            if context.terminal_id != operation_id:
                raise Conflict("retained finalizer has another terminal request")
            if not context.drained or context.construction_failure is not None:
                return "fenced"
            execution = self._execution.status(key)
            if execution == "busy":
                return "busy"
            return "drained" if execution == "untracked" else "fenced"
        finally:
            self._lock.release()

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
            return context is not None and self._retire_context_if_idle(context)
        finally:
            self._lock.release()

    def _retire_context_if_idle(self, context: _Context) -> bool:
        # The caller retains the registry lock; semantic and physical receiving
        # remain independent prerequisites for releasing this original charge.
        if context.terminal_id is not None or self._execution.status(context.key) != "untracked":
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

    def _remove(self, context: _Context) -> bool:
        if not self._retention.retire(context.reservation, owner=self):
            return False
        del self._contexts[context.key.collection_id]
        return True

    def close(self) -> None:
        with self._lock:
            self._closed = True
        self._execution.close()

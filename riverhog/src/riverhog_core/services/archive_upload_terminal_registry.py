"""Retain original native terminal turns in shared, bounded execution owners."""

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from threading import Lock
from time import monotonic

from riverhog_protocol.errors import Conflict

from riverhog_core.services.archive_construction_retention import (
    ArchiveConstructionReservation,
    ArchiveConstructionRetention,
)
from riverhog_core.services.archive_upload_terminal_turn import (
    ArchiveUploadTerminalTurn,
    TerminalTurnProgress,
)
from riverhog_core.services.physical_execution import (
    PhysicalExecutionKey,
    PhysicalExecutionOccupied,
    PhysicalExecutionOwner,
)

type TerminalTurnCreate = Callable[[int, str], ArchiveUploadTerminalTurn]

_LOG = logging.getLogger("riverhog_core.archive_upload_terminal_registry")


@dataclass(slots=True)
class _Context:
    reservation: ArchiveConstructionReservation
    entered: bool = False
    original: object | None = None
    actor: ArchiveUploadTerminalTurn | None = None
    received: bool = False
    progress: TerminalTurnProgress | None = None
    resume_after: float = 0
    lock: Lock = field(default_factory=Lock)


class ArchiveUploadTerminalRegistry:
    """Retain one original actor and its current-authority guard across turns.

    This receiver owns neither a scheduler nor a physical pool. The upload
    maintenance owner owns bounded scheduling opportunities. Terminal
    reservations precede construction, independently of the original forward
    work's reservations. Intermediate progress never releases an entered actor
    or supplies a confirmed public outcome.
    """

    def __init__(
        self,
        execution: PhysicalExecutionOwner,
        retention: ArchiveConstructionRetention,
        create: TerminalTurnCreate,
    ) -> None:
        self._execution = execution
        self._retention = retention
        self._create = create
        self._lock = Lock()
        self._closed = False
        self._contexts: OrderedDict[PhysicalExecutionKey, _Context] = OrderedDict()

    def close(self) -> None:
        """Stop scheduling; the shared physical owner performs actual joins."""
        with self._lock:
            self._closed = True

    @staticmethod
    def _limits(limit: int, retry_after: timedelta) -> None:
        if type(limit) is not int or limit < 1 or retry_after.total_seconds() <= 0:
            raise ValueError("terminal execution scheduling limits must be positive")

    def collect(self, *, limit: int) -> int:
        """Receive actual joined originals without promoting intermediate results."""
        if type(limit) is not int or limit < 1:
            raise ValueError("terminal execution receiving limit must be positive")
        if not self._lock.acquire(blocking=False):
            return 0
        progressed = 0
        try:
            for outcome in self._execution.collect(limit=limit, owner=self):
                context = self._contexts.get(outcome.key)
                if context is None or outcome.identity is not context:
                    raise Conflict("terminal execution has another original physical receiver")
                if outcome.target_incarnation != context.reservation.target_incarnation:
                    raise Conflict("terminal execution original physical target changed")
                context.received = True
                progressed += 1
                try:
                    progress = outcome.result()
                    if not isinstance(progress, TerminalTurnProgress):
                        raise Conflict("terminal execution returned another progress type")
                    context.progress = progress
                except BaseException as error:
                    _LOG.warning(
                        "terminal execution worker failed: collection_id=%s error_type=%s",
                        outcome.key.collection_id,
                        type(error).__name__,
                    )
                finally:
                    outcome.release_exception_frames()
            for _ in range(min(limit, len(self._contexts))):
                key, context = self._contexts.popitem(last=False)
                self._contexts[key] = context
                # Positive non-entry, not a missing actor or successful phase,
                # permits withdrawal of an unused local reservation.
                if context.received and not context.entered:
                    if self._retention.retire(context.reservation, owner=self):
                        del self._contexts[key]
                        progressed += 1
            return progressed
        finally:
            self._lock.release()

    def _submit(
        self, context: _Context, *, work_limit: int, clock: float, retry_after: timedelta
    ) -> int:
        key = context.reservation.key
        previous_received = context.received
        context.received = False
        try:
            result = self._execution.try_submit(
                key,
                target_incarnation=context.reservation.target_incarnation,
                terminal=True,
                work=partial(self._run, context, work_limit=work_limit),
                owner=self,
                identity=context,
            )
        except PhysicalExecutionOccupied:
            context.received = previous_received or not context.entered
            return 0
        except BaseException:
            # The registry serializes its own submission and receiving. Only
            # positive withdrawal by the physical owner proves no callback ran;
            # a started worker with a lost acknowledgement remains charged.
            if self._execution.status(key) == "untracked":
                context.received = previous_received or not context.entered
            raise
        if result == "submitted":
            context.resume_after = clock + retry_after.total_seconds()
            return 1
        context.received = previous_received
        if result != "retained" and not context.entered:
            context.received = True
        return 0

    def submit(
        self,
        key: PhysicalExecutionKey,
        *,
        target_incarnation: str,
        work_limit: int,
        retry_after: timedelta,
    ) -> int:
        """Admit one exact selection, constructing only inside its actual worker."""
        self._limits(work_limit, retry_after)
        if not isinstance(target_incarnation, str) or not target_incarnation:
            raise ValueError("terminal execution requires its exact original target")
        if not self._lock.acquire(blocking=False):
            return 0
        try:
            if self._closed:
                return 0
            clock = monotonic()
            context = self._contexts.get(key)
            if context is None:
                reservation = self._retention.try_retain(
                    key, target_incarnation=target_incarnation, owner=self, terminal=True
                )
                if isinstance(reservation, str):
                    return 0
                context = _Context(reservation)
                self._contexts[key] = context
            else:
                if context.reservation.target_incarnation != target_incarnation:
                    raise Conflict("terminal execution retained original target changed")
                if (
                    not context.received
                    or context.resume_after > clock
                    or (context.entered and context.actor is None)
                ):
                    return 0
            return self._submit(
                context, work_limit=work_limit, clock=clock, retry_after=retry_after
            )
        finally:
            self._lock.release()

    def resume_ready(self, *, limit: int, work_limit: int, retry_after: timedelta) -> int:
        """Offer bounded retained originals a turn without replacing their factory."""
        self._limits(limit, retry_after)
        self._limits(work_limit, retry_after)
        if not self._lock.acquire(blocking=False):
            return 0
        try:
            if self._closed:
                return 0
            submitted = 0
            clock = monotonic()
            for _ in range(min(limit, len(self._contexts))):
                key, context = self._contexts.popitem(last=False)
                self._contexts[key] = context
                if (
                    not context.received
                    or context.resume_after > clock
                    or (context.entered and context.actor is None)
                ):
                    continue
                submitted += self._submit(
                    context, work_limit=work_limit, clock=clock, retry_after=retry_after
                )
            return submitted
        finally:
            self._lock.release()

    def _run(self, context: _Context, *, work_limit: int) -> TerminalTurnProgress:
        # Only the charged physical worker may enter or wait for this actor.
        with context.lock:
            key = context.reservation.key
            if context.actor is None:
                if context.entered:
                    raise Conflict("original terminal constructor is unresolved")
                context.entered = True
                context.original = self._create(key.collection_id, key.scope_sha256)
                actor = context.original
                if (
                    not isinstance(actor, ArchiveUploadTerminalTurn)
                    or actor.key != key
                    or actor.target_incarnation != context.reservation.target_incarnation
                ):
                    raise Conflict("terminal constructor returned another original receiver")
                context.actor = actor
            return context.actor.advance(limit=work_limit)

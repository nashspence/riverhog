"""Retain independent original workspace cleanup through actual receiving."""

from __future__ import annotations

import logging
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from threading import Lock
from time import monotonic
from typing import Protocol

from riverhog_protocol.errors import Conflict

from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services.archive_construction_retention import (
    ArchiveConstructionRetention,
    WorkspaceConstructionReservation,
)
from riverhog_core.services.physical_execution import (
    PhysicalExecutionOccupied,
    PhysicalExecutionOwner,
    PhysicalWorkspaceExecutionCompletion,
    PhysicalWorkspaceExecutionKey,
    release_exception_graph,
)

_LOG = logging.getLogger("riverhog_core.workspace_retirement_registry")


class WorkspaceJob(Protocol):
    @property
    def workspace_id(self) -> str: ...


class WorkspaceRetirementActor[ProgressT](Protocol):
    @property
    def received(self) -> bool: ...

    def advance(self, *, octet_budget: int) -> ProgressT: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class _Closed:
    """Only actual local handle receiving; no catalog or terminal outcome."""


@dataclass(frozen=True, slots=True)
class _ReceivingPending:
    """A joined close attempt is not a positive resource/diagnostic receipt."""

    error_type: str


@dataclass(slots=True)
class _Context[JobT, ProgressT]:
    job: JobT
    reservation: WorkspaceConstructionReservation
    octet_budget: int
    entered: bool = False
    original: object | None = None
    actor: WorkspaceRetirementActor[ProgressT] | None = None
    received: bool = False
    closing: bool = False
    closed: bool = False
    progress: ProgressT | None = None
    unknown: PhysicalWorkspaceExecutionCompletion | None = None
    pending_diagnostic: BaseException | None = None
    diagnostic_failure: BaseException | None = None
    resume_after: float = 0
    lock: Lock = field(default_factory=Lock)


class WorkspaceRetirementRegistry[JobT: WorkspaceJob, ProgressT]:
    """One original per workspace, using shared retention and physical budgets.

    Ordinary retention precedes construction; terminal physical capacity executes
    one bounded quantum or an original close-receiving retry. Actual worker join
    and original receiving precede reservation withdrawal. A failed constructor
    is never reconstructed, and uncertain resource closure retains the original.
    Independent irreversible cleanup neither acquires nor confirms a semantic
    upload terminal. Its scheduler supplies detached catalog jobs separately.
    """

    def __init__(
        self,
        execution: PhysicalExecutionOwner,
        retention: ArchiveConstructionRetention,
    ) -> None:
        self._execution, self._retention = execution, retention
        self._lock = Lock()
        self._closed = False
        self._contexts: OrderedDict[str, _Context[JobT, ProgressT]] = OrderedDict()

    @staticmethod
    def _limits(limit: int, retry_after: timedelta) -> None:
        if type(limit) is not int or limit < 1 or retry_after.total_seconds() <= 0:
            raise ValueError("workspace receiving limits must be positive")

    def close(self) -> None:
        """Stop scheduling; actual shared-worker joins and receiving stay required."""
        with self._lock:
            self._closed = True

    def retained(self, workspace_id: str) -> bool:
        if not self._lock.acquire(blocking=False):
            return True
        try:
            return workspace_id in self._contexts
        finally:
            self._lock.release()

    def collect(self, *, limit: int) -> int:
        if type(limit) is not int or limit < 1:
            raise ValueError("workspace receiving limit must be positive")
        if not self._lock.acquire(blocking=False):
            return 0
        progressed = 0
        try:
            for outcome in self._execution.collect_workspace(limit=limit, owner=self):
                context = self._contexts.get(outcome.key.workspace_id)
                if (
                    context is None
                    or outcome.identity is not context
                    or outcome.key != context.reservation.key
                    or outcome.target_incarnation != context.reservation.target_incarnation
                    or not outcome.terminal
                ):
                    raise Conflict("workspace receiving has another original physical outcome")
                context.received = True
                progressed += 1
                try:
                    result = outcome.result()
                    if isinstance(result, _ReceivingPending):
                        if not context.closing or context.closed:
                            raise Conflict("pending receiving lacks its original context")
                    elif isinstance(result, _Closed):
                        if not context.closing or not context.closed:
                            raise Conflict("workspace close receiving lacks its original proof")
                    else:
                        if context.actor is None or context.closing or not context.actor.received:
                            raise Conflict("workspace progress lacks original resource receiving")
                        context.progress = self._accept_progress(context.job, result)
                        context.closed = True
                except BaseException as error:
                    context.closing = True
                    context.closed = False
                    if context.unknown is None:
                        context.unknown = outcome
                    _LOG.warning(
                        "workspace receiving failed: workspace_id=%s error_type=%s",
                        outcome.key.workspace_id,
                        type(error).__name__,
                    )
            for _ in range(min(limit, len(self._contexts))):
                workspace, context = self._contexts.popitem(last=False)
                self._contexts[workspace] = context
                if context.received and context.closed:
                    if self._retention.retire(context.reservation, owner=self):
                        del self._contexts[workspace]
                        progressed += 1
            return progressed
        finally:
            self._lock.release()

    def _submit(
        self, context: _Context[JobT, ProgressT], *, clock: float, retry_after: timedelta
    ) -> int:
        previous = context.received
        context.received = False
        try:
            result = self._execution.try_submit_workspace(
                context.reservation.key,
                target_incarnation=context.reservation.target_incarnation,
                work=partial(self._run, context),
                owner=self,
                identity=context,
            )
        except PhysicalExecutionOccupied:
            context.received = previous or not context.entered
            if not context.entered:
                context.closed = True
            return 0
        except BaseException:
            # A started worker with a lost startup reply stays charged.
            if self._execution.status(context.reservation.key) == "untracked":
                context.received = previous or not context.entered
                context.closed = not context.entered
            raise
        if result == "submitted":
            context.resume_after = clock + retry_after.total_seconds()
            return 1
        context.received = previous
        if result != "retained" and not context.entered:
            context.received = context.closed = True
        return 0

    def submit(
        self,
        job: JobT,
        *,
        octet_budget: int,
        metadata_budget: int,
        retry_after: timedelta,
    ) -> int:
        """Admit a full original control record before reserving and constructing."""
        self._limits(octet_budget, retry_after)
        if type(metadata_budget) is not int or metadata_budget < 1:
            raise ValueError("workspace metadata admission must be positive")
        if (
            not isinstance(job.workspace_id, str)
            or len(job.workspace_id) != 32
            or re.fullmatch(r"[a-f0-9]{32}", job.workspace_id) is None
        ):
            raise ValueError("workspace execution requires an exact original namespace")
        if not self._lock.acquire(blocking=False):
            return 0
        try:
            if self._closed:
                return 0
            context = self._contexts.get(job.workspace_id)
            if context is not None:
                # The original may already have committed a later catalog phase.
                # Its actual receiving must finish before a new cold turn.
                return 0
            key, target, required = self._scope(job, metadata_budget)
            if key.workspace_id != job.workspace_id:
                raise Conflict("workspace scope has another original namespace")
            if required > metadata_budget:
                raise VolumeEmissionCapacity(
                    dimension="metadata_bytes",
                    required=required,
                    admitted=metadata_budget,
                )
            reservation = self._retention.try_retain_workspace(
                key,
                target_incarnation=target,
                owner=self,
            )
            if isinstance(reservation, str):
                return 0
            context = _Context[JobT, ProgressT](job, reservation, octet_budget)
            self._contexts[job.workspace_id] = context
            return self._submit(context, clock=monotonic(), retry_after=retry_after)
        finally:
            self._lock.release()

    def resume_ready(self, *, limit: int, retry_after: timedelta) -> int:
        """Retry only actual closure of retained originals, never failed work."""
        self._limits(limit, retry_after)
        if not self._lock.acquire(blocking=False):
            return 0
        try:
            if self._closed:
                return 0
            submitted = 0
            clock = monotonic()
            for _ in range(min(limit, len(self._contexts))):
                workspace, context = self._contexts.popitem(last=False)
                self._contexts[workspace] = context
                if (
                    not context.received
                    or context.resume_after > clock
                    or context.closed
                    or not context.closing
                    or context.actor is None
                ):
                    continue
                submitted += self._submit(context, clock=clock, retry_after=retry_after)
            return submitted
        finally:
            self._lock.release()

    def _scope(
        self, job: JobT, metadata_budget: int
    ) -> tuple[PhysicalWorkspaceExecutionKey, str, int]:
        raise NotImplementedError

    def _accept_progress(self, job: JobT, result: object) -> ProgressT:
        raise NotImplementedError

    def _create(self, job: JobT) -> WorkspaceRetirementActor[ProgressT]:
        raise NotImplementedError

    def _run(self, context: _Context[JobT, ProgressT]) -> ProgressT | _Closed | _ReceivingPending:
        with context.lock:
            if context.closing:
                if context.actor is None:
                    raise Conflict("original workspace constructor remains unknown")
                # Retain secondary receiving exceptions before frame disposal.
                if context.pending_diagnostic is not None:
                    try:
                        release_exception_graph(context.pending_diagnostic)
                    except BaseException as error:
                        context.diagnostic_failure = error
                        return _ReceivingPending(type(error).__name__)
                    context.pending_diagnostic = None
                    context.diagnostic_failure = None
                try:
                    context.actor.close()
                    if not context.actor.received:
                        raise Conflict("original workspace receiving remains unconfirmed")
                    if context.unknown is not None:
                        context.unknown.release_exception_frames()
                        context.unknown = None
                except BaseException as error:
                    context.pending_diagnostic = error
                    try:
                        release_exception_graph(error)
                    except BaseException as failure:
                        context.diagnostic_failure = failure
                    else:
                        context.pending_diagnostic = None
                        context.diagnostic_failure = None
                    return _ReceivingPending(type(error).__name__)
                context.closed = True
                return _Closed()
            if context.entered:
                raise Conflict("original workspace work cannot be reconstructed")
            context.entered = True
            actor = self._create(context.job)
            context.original = context.actor = actor
            result = actor.advance(octet_budget=context.octet_budget)
            if not actor.received:
                raise Conflict("workspace work returned before original resource receiving")
            return result

    def receive_stopped(self) -> bool:
        """Receive local handles after shutdown and actual original worker joins.

        This explicit shutdown step may wait in a handle close. Normal collection
        and scheduling never do. Missing/unknown original construction remains
        retained; the caller keeps shared dependencies open until receiving.
        """
        with self._lock:
            if not self._closed:
                raise Conflict("workspace receiving requires stopped admission")
            for workspace, context in tuple(self._contexts.items()):
                if not context.received:
                    return False
                if context.entered and context.actor is None:
                    return False
                context.closed = False
                if context.actor is not None:
                    context.actor.close()
                    if not context.actor.received:
                        return False
                if context.pending_diagnostic is not None:
                    release_exception_graph(context.pending_diagnostic)
                    context.pending_diagnostic = None
                    context.diagnostic_failure = None
                if context.unknown is not None:
                    context.unknown.release_exception_frames()
                    context.unknown = None
                context.closed = True
                if self._retention.retire(context.reservation, owner=self):
                    del self._contexts[workspace]
            return not self._contexts

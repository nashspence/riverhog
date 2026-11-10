"""Retain bounded physical execution through actual worker joins and receiving."""

from __future__ import annotations

import re
import traceback
from collections import OrderedDict
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from threading import Lock, Thread, current_thread
from typing import Literal, overload

from riverhog_protocol.errors import Conflict

from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning
from riverhog_core.services.physical_workspace_growth import declare_workspace_execution_growth

Submission = Literal["submitted", "retained", "capacity", "target-capacity", "busy", "closed"]


class PhysicalExecutionOccupied(Conflict):
    """An original turn belongs to a different scope, lane or local owner."""


@dataclass(frozen=True, slots=True)
class PhysicalExecutionKey:
    collection_id: int
    scope_sha256: str

    def __post_init__(self) -> None:
        if type(self.collection_id) is not int or self.collection_id < 1:
            raise ValueError("physical execution collection identity must be positive")
        if re.fullmatch(r"[a-f0-9]{64}", self.scope_sha256) is None:
            raise ValueError("physical execution requires the original scope digest")


@dataclass(frozen=True, slots=True)
class PhysicalWorkspaceExecutionKey:
    """Route an independent local workspace without adopting collection authority."""

    workspace_id: str
    scope_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.workspace_id, str)
            or re.fullmatch(r"[a-f0-9]{32}", self.workspace_id) is None
        ):
            raise ValueError("physical workspace execution requires an exact workspace identity")
        if (
            not isinstance(self.scope_sha256, str)
            or re.fullmatch(r"[a-f0-9]{64}", self.scope_sha256) is None
        ):
            raise ValueError("physical workspace execution requires the original scope digest")


_ExecutionKey = PhysicalExecutionKey | PhysicalWorkspaceExecutionKey


class _PhysicalExecutionResult:
    __slots__ = ()

    _future: Future[object]

    def result(self) -> object:
        return self._future.result()

    def release_exception_frames(self) -> None:
        """Drop joined worker locals while preserving its original exception."""
        error = self._future.exception()
        pending = [error] if error is not None else []
        seen: set[int] = set()
        while pending:
            current = pending.pop()
            if id(current) in seen:
                continue
            seen.add(id(current))
            traceback.clear_frames(current.__traceback__)
            for chained in (current.__cause__, current.__context__):
                if chained is not None:
                    pending.append(chained)
            if isinstance(current, BaseExceptionGroup):
                pending.extend(current.exceptions)


@dataclass(frozen=True, slots=True)
class PhysicalExecutionCompletion(_PhysicalExecutionResult):
    """Original collection outcome after actual worker join; no disposal authority."""

    key: PhysicalExecutionKey
    target_incarnation: str
    terminal: bool
    owner: object | None = field(repr=False, compare=False)
    identity: object | None = field(repr=False, compare=False)
    _future: Future[object] = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class PhysicalWorkspaceExecutionCompletion(_PhysicalExecutionResult):
    """Independent original workspace result after actual worker join.

    The exact receiver still owns retained handles, metadata and semantic
    receiving. Joining this worker never supplies those outcomes.
    """

    key: PhysicalWorkspaceExecutionKey
    target_incarnation: str
    terminal: bool
    owner: object = field(repr=False, compare=False)
    identity: object = field(repr=False, compare=False)
    _future: Future[object] = field(repr=False, compare=False)


@dataclass(slots=True)
class _Slot:
    key: _ExecutionKey
    target_incarnation: str
    terminal: bool
    owner: object | None
    identity: object | None
    future: Future[object]
    worker: Thread


def _run(work: Callable[[], object], future: Future[object]) -> None:
    try:
        result = work()
    except BaseException as exc:
        future.set_exception(exc)
    else:
        future.set_result(result)


class PhysicalExecutionOwner:
    """Service-owned physical execution; it never admits a provider effect.

    Reserve before invoking a constructor or a work callback. There is no queued
    work beyond the configured slots, and one scope has at most one local
    turn. Polling never waits for a provider, worker, callback or another poll.
    Shutdown joins outside the registry lock before shared dependencies close.
    """

    def __init__(self, tuning: ArchiveExecutionTuning) -> None:
        self._tuning = tuning
        self._lock = Lock()
        self._closed = False
        self._slots: dict[_ExecutionKey, _Slot] = {}
        self._poll: dict[int, OrderedDict[_ExecutionKey, None]] = {}
        self._counts = {False: 0, True: 0}
        self._targets: dict[tuple[bool, str], int] = {}

    def has_unreceived_work(self) -> bool:
        """Hint that short receiving polls may help, without waiting for a lock.

        Slots include running workers and physically joined results that still
        need their original receiver. A busy observation stays conservative.
        Neither value supplies semantic or global closure authority.
        """
        if not self._lock.acquire(blocking=False):
            return True
        try:
            return bool(self._slots)
        finally:
            self._lock.release()

    def try_submit(
        self,
        key: PhysicalExecutionKey,
        *,
        target_incarnation: str,
        terminal: bool,
        work: Callable[[], object],
        owner: object | None = None,
        identity: object | None = None,
    ) -> Submission:
        if not isinstance(key, PhysicalExecutionKey):
            raise ValueError("collection execution requires its exact collection key")
        return self._try_submit(
            key,
            target_incarnation=target_incarnation,
            terminal=terminal,
            work=work,
            owner=owner,
            identity=identity,
        )

    def try_submit_workspace(
        self,
        key: PhysicalWorkspaceExecutionKey,
        *,
        target_incarnation: str,
        work: Callable[[], object],
        owner: object,
        identity: object,
    ) -> Submission:
        """Reserve shared terminal capacity before independent workspace work.

        The exact receiver retains its resource owner through failed receiving.
        This routes only local execution and grants no archive/provider authority.
        """
        if not isinstance(key, PhysicalWorkspaceExecutionKey) or owner is None or identity is None:
            raise ValueError("workspace execution requires its exact key, receiver and original")
        return self._try_submit(
            key,
            target_incarnation=target_incarnation,
            terminal=True,
            work=work,
            owner=owner,
            identity=identity,
        )

    def _try_submit(
        self,
        key: _ExecutionKey,
        *,
        target_incarnation: str,
        terminal: bool,
        work: Callable[[], object],
        owner: object | None,
        identity: object | None,
    ) -> Submission:
        if type(terminal) is not bool or not target_incarnation:
            raise ValueError("physical execution requires an exact target and work lane")
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._closed:
                return "closed"
            prior = self._slots.get(key)
            if prior is not None:
                if (
                    prior.key != key
                    or prior.target_incarnation != target_incarnation
                    or prior.terminal != terminal
                    or prior.owner is not owner
                    or prior.identity is not identity
                ):
                    raise PhysicalExecutionOccupied(
                        "physical execution already retains another original turn"
                    )
                return "retained"
            capacity = self._tuning.terminal_workers if terminal else self._tuning.forward_workers
            per_target = (
                self._tuning.terminal_workers_per_target
                if terminal
                else self._tuning.forward_workers_per_target
            )
            target = (terminal, target_incarnation)
            if self._counts[terminal] >= capacity:
                return "capacity"
            if self._targets.get(target, 0) >= per_target:
                return "target-capacity"
            future: Future[object] = Future()
            worker = Thread(
                target=_run, args=(work, future), name="physical-execution", daemon=False
            )
            slot = _Slot(key, target_incarnation, terminal, owner, identity, future, worker)
            self._slots[key] = slot
            self._poll.setdefault(id(owner), OrderedDict())[key] = None
            self._counts[terminal] += 1
            self._targets[target] = self._targets.get(target, 0) + 1
            try:
                worker.start()
            except BaseException:
                # A started worker remains charged even if startup acknowledgement
                # failed. Only an original thread that never started is withdrawn.
                if worker.ident is None:
                    self._remove(slot)
                raise
            return "submitted"
        finally:
            self._lock.release()

    def status(
        self, key: _ExecutionKey
    ) -> Literal["busy", "untracked", "running", "awaiting-join", "outcome-missing"]:
        """Point observation of this registry, never a global closure proof."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            slot = self._slots.get(key)
            if slot is None:
                return "untracked"
            if slot.key != key:
                raise Conflict("physical execution retains another original scope")
            if slot.worker.is_alive():
                return "running"
            return "awaiting-join" if slot.future.done() else "outcome-missing"
        finally:
            self._lock.release()

    def collect(
        self, *, limit: int, owner: object | None
    ) -> tuple[PhysicalExecutionCompletion, ...]:
        return self._collect(limit=limit, owner=owner, workspace=False)

    def collect_workspace(
        self, *, limit: int, owner: object
    ) -> tuple[PhysicalWorkspaceExecutionCompletion, ...]:
        """Receive only this workspace owner's actual joined outcomes."""
        if owner is None:
            raise ValueError("workspace receiving requires its exact owner")
        return self._collect(limit=limit, owner=owner, workspace=True)

    @overload
    def _collect(
        self, *, limit: int, owner: object | None, workspace: Literal[False]
    ) -> tuple[PhysicalExecutionCompletion, ...]: ...

    @overload
    def _collect(
        self, *, limit: int, owner: object | None, workspace: Literal[True]
    ) -> tuple[PhysicalWorkspaceExecutionCompletion, ...]: ...

    def _collect(
        self, *, limit: int, owner: object | None, workspace: bool
    ) -> tuple[PhysicalExecutionCompletion, ...] | tuple[PhysicalWorkspaceExecutionCompletion, ...]:
        """Inspect at most limit slots for the exact local receiving owner.

        Each receiver rotates only its own charged slots. Interleaved polls by
        another receiver cannot reset that progress or consume its outcomes.
        Buckets exist only while physical work is retained, including joined
        results that have not been received. Owner identity never uses equality.
        """
        if type(limit) is not int or limit < 1:
            raise ValueError("upload execution collection limit must be positive")
        if not self._lock.acquire(blocking=False):
            return ()
        try:
            completed: list[PhysicalExecutionCompletion] = []
            workspace_completed: list[PhysicalWorkspaceExecutionCompletion] = []
            poll = self._poll.get(id(owner))
            if poll is None:
                return ()
            for _ in range(min(limit, len(poll))):
                key, _ = poll.popitem(last=False)
                poll[key] = None
                slot = self._slots[key]
                assert slot.owner is owner
                if isinstance(slot.key, PhysicalWorkspaceExecutionKey) != workspace:
                    raise PhysicalExecutionOccupied("physical receiver has another execution kind")
                if slot.worker.is_alive():
                    continue
                # Future.done alone is insufficient: callbacks, thread-local
                # destructors and original frames may outlive result publication.
                slot.worker.join(timeout=0)
                if not slot.future.done():
                    # Missing original evidence stays charged. A dead local
                    # thread cannot supply an invented successful outcome.
                    continue
                if isinstance(slot.key, PhysicalExecutionKey):
                    completed.append(
                        PhysicalExecutionCompletion(
                            slot.key,
                            slot.target_incarnation,
                            slot.terminal,
                            slot.owner,
                            slot.identity,
                            slot.future,
                        )
                    )
                else:
                    assert slot.owner is not None and slot.identity is not None
                    workspace_completed.append(
                        PhysicalWorkspaceExecutionCompletion(
                            slot.key,
                            slot.target_incarnation,
                            slot.terminal,
                            slot.owner,
                            slot.identity,
                            slot.future,
                        )
                    )
                self._remove(slot)
            return tuple(workspace_completed) if workspace else tuple(completed)
        finally:
            self._lock.release()

    def _remove(self, slot: _Slot) -> None:
        receiver = id(slot.owner)
        poll = self._poll[receiver]
        del poll[slot.key]
        if not poll:
            del self._poll[receiver]
        del self._slots[slot.key]
        self._counts[slot.terminal] -= 1
        target = (slot.terminal, slot.target_incarnation)
        remaining = self._targets[target] - 1
        if remaining:
            self._targets[target] = remaining
        else:
            del self._targets[target]

    def close(self) -> None:
        """Stop submission and join physical workers without dropping outcomes."""
        with self._lock:
            workers = tuple(slot.worker for slot in self._slots.values())
            if any(worker is current_thread() for worker in workers):
                raise RuntimeError("upload worker cannot join its own execution owner")
            self._closed = True
        for worker in workers:
            worker.join()


declare_workspace_execution_growth(PhysicalExecutionOwner.try_submit_workspace, collect=False)
declare_workspace_execution_growth(PhysicalExecutionOwner.collect_workspace, collect=True)

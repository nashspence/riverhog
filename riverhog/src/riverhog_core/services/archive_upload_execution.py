"""Retain bounded upload execution through actual worker joins and receiving."""

from __future__ import annotations

import re
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from threading import Lock, Thread, current_thread
from typing import Literal

from riverhog_protocol.errors import Conflict

from riverhog_core.archive_execution_tuning import ArchiveExecutionTuning

Submission = Literal["submitted", "retained", "capacity", "target-capacity", "busy", "closed"]


class ArchiveExecutionOccupied(Conflict):
    """An original turn belongs to a different construction, lane or local owner."""


@dataclass(frozen=True, slots=True)
class ArchiveUploadExecutionKey:
    collection_id: int
    construction_sha256: str

    def __post_init__(self) -> None:
        if type(self.collection_id) is not int or self.collection_id < 1:
            raise ValueError("upload execution collection identity must be positive")
        if re.fullmatch(r"[a-f0-9]{64}", self.construction_sha256) is None:
            raise ValueError("upload execution requires the original construction digest")


@dataclass(frozen=True, slots=True)
class ArchiveUploadExecutionCompletion:
    """The exact local outcome after the original physical worker has joined.

    Receiving this result grants no provider closure, preparation disposal or
    global quiescence. Those facts remain with the original semantic owners.
    """

    key: ArchiveUploadExecutionKey
    target_incarnation: str
    terminal: bool
    _future: Future[object] = field(repr=False, compare=False)

    def result(self) -> object:
        return self._future.result()


@dataclass(slots=True)
class _Slot:
    key: ArchiveUploadExecutionKey
    target_incarnation: str
    terminal: bool
    owner: object | None
    future: Future[object]
    worker: Thread


def _run(work: Callable[[], object], future: Future[object]) -> None:
    try:
        result = work()
    except BaseException as exc:
        future.set_exception(exc)
    else:
        future.set_result(result)


class ArchiveUploadExecutionOwner:
    """Service-owned physical execution; it never admits a provider effect.

    Reserve before invoking a constructor or a work callback. There is no queued
    work beyond the configured slots, and one construction has at most one local
    turn. Polling never waits for a provider, worker, callback or another poll.
    Shutdown joins outside the registry lock before shared dependencies close.
    """

    def __init__(self, tuning: ArchiveExecutionTuning) -> None:
        self._tuning = tuning
        self._lock = Lock()
        self._closed = False
        self._slots: dict[int, _Slot] = {}
        self._poll: deque[int] = deque()
        self._counts = {False: 0, True: 0}
        self._targets: dict[tuple[bool, str], int] = {}

    def try_submit(
        self,
        key: ArchiveUploadExecutionKey,
        *,
        target_incarnation: str,
        terminal: bool,
        work: Callable[[], object],
        owner: object | None = None,
    ) -> Submission:
        if type(terminal) is not bool or not target_incarnation:
            raise ValueError("upload execution requires an exact target and work lane")
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._closed:
                return "closed"
            prior = self._slots.get(key.collection_id)
            if prior is not None:
                if (
                    prior.key != key
                    or prior.target_incarnation != target_incarnation
                    or prior.terminal != terminal
                    or prior.owner is not owner
                ):
                    raise ArchiveExecutionOccupied(
                        "upload execution already retains another original turn"
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
                target=_run, args=(work, future), name="archive-upload-execution", daemon=False
            )
            slot = _Slot(key, target_incarnation, terminal, owner, future, worker)
            self._slots[key.collection_id] = slot
            self._poll.append(key.collection_id)
            self._counts[terminal] += 1
            self._targets[target] = self._targets.get(target, 0) + 1
            try:
                worker.start()
            except BaseException:
                # A started worker remains charged even if startup acknowledgement
                # failed. Only an original thread that never started is withdrawn.
                if worker.ident is None:
                    self._poll.remove(key.collection_id)
                    self._remove(slot)
                raise
            return "submitted"
        finally:
            self._lock.release()

    def status(
        self, key: ArchiveUploadExecutionKey
    ) -> Literal["busy", "untracked", "running", "awaiting-join", "outcome-missing"]:
        """Point observation of this registry, never a global closure proof."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            slot = self._slots.get(key.collection_id)
            if slot is None:
                return "untracked"
            if slot.key != key:
                raise Conflict("upload execution retains another original construction")
            if slot.worker.is_alive():
                return "running"
            return "awaiting-join" if slot.future.done() else "outcome-missing"
        finally:
            self._lock.release()

    def collect(self, *, limit: int) -> tuple[ArchiveUploadExecutionCompletion, ...]:
        """Inspect at most limit slots; return only joined original outcomes."""
        if type(limit) is not int or limit < 1:
            raise ValueError("upload execution collection limit must be positive")
        if not self._lock.acquire(blocking=False):
            return ()
        try:
            completed = []
            for _ in range(min(limit, len(self._poll))):
                collection_id = self._poll.popleft()
                slot = self._slots[collection_id]
                if slot.worker.is_alive():
                    self._poll.append(collection_id)
                    continue
                # Future.done alone is insufficient: callbacks, thread-local
                # destructors and original frames may outlive result publication.
                slot.worker.join(timeout=0)
                if not slot.future.done():
                    # Missing original evidence stays charged. A dead local
                    # thread cannot supply an invented successful outcome.
                    self._poll.append(collection_id)
                    continue
                completed.append(
                    ArchiveUploadExecutionCompletion(
                        slot.key, slot.target_incarnation, slot.terminal, slot.future
                    )
                )
                self._remove(slot)
            return tuple(completed)
        finally:
            self._lock.release()

    def _remove(self, slot: _Slot) -> None:
        del self._slots[slot.key.collection_id]
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

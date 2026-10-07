"""Volatile bounded dispatch mechanics; each component owns its typed durable jobs."""

from __future__ import annotations

import logging
import math
import secrets
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from stove0_extension_support import (
    ExclusiveStateOwner,
    ExecutionAdmission,
    ExecutionOwner,
    ExecutionPermit,
    ImmediateExecutionAdmission,
)
from stove0_extension_support.consumers import consumer_scope

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExecutionDispatch:
    """Transient callbacks over one component-owned exact invocation.

    Authorize performs bounded read-only checks. Prepare rechecks durable state
    and cancellation, persists the start boundary, and returns the payload. It
    must never execute payload or wait for resources. No job store or wire result
    is defined here; terminal evidence stays with its owning component.
    """

    invocation_id: str
    attempt: int
    authorize: Callable[[float], None]
    prepare: Callable[[threading.Event, ExecutionPermit], Callable[[], object] | None]
    deferred: Callable[[], None]
    failed: Callable[[BaseException], None]
    finished: Callable[[], None]


class BoundedExecutionDispatcher:
    def __init__(
        self,
        *,
        maximum_workers: int = 1,
        state_owner: ExclusiveStateOwner | None = None,
        admission: ExecutionAdmission | None = None,
        probe_seconds: float = 1.0,
        retry_seconds: float = 1.0,
    ) -> None:
        if isinstance(maximum_workers, bool) or maximum_workers < 1:
            raise ValueError("extension execution concurrency must be positive")
        if any(
            isinstance(value, bool) or not math.isfinite(value) or value <= 0
            for value in (probe_seconds, retry_seconds)
        ):
            raise ValueError("extension admission timing must be finite and positive")
        self.state_owner = state_owner
        self.maximum_workers = maximum_workers
        self.probe_seconds = probe_seconds
        self.retry_seconds = retry_seconds
        self.admission = admission or ImmediateExecutionAdmission()
        self.pool = ThreadPoolExecutor(
            max_workers=maximum_workers, thread_name_prefix="stove0-extension"
        )
        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._closing = False
        self._process_id = secrets.token_hex(16)
        self._pending: dict[str, ExecutionDispatch] = {}
        self._owners: dict[str, ExecutionOwner] = {}
        self._due: dict[str, float] = {}
        self._cancellation: dict[str, threading.Event] = {}
        self._probing: set[str] = set()
        self._permits: dict[str, ExecutionPermit] = {}
        self._futures: dict[str, Future[object]] = {}
        self._finishing: set[str] = set()
        self._releases: dict[ExecutionOwner, ExecutionPermit] = {}
        self._withdrawals: dict[ExecutionOwner, float] = {}
        self._errors: dict[str, str] = {}
        self._thread = threading.Thread(
            target=self._loop, name="stove0-extension-dispatch", daemon=True
        )
        self._thread.start()

    @property
    def active_keys(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._futures.keys() | self._permits.keys() | self._finishing)

    @property
    def payload_count(self) -> int:
        with self._lock:
            return len(self._futures)

    def enqueue(self, key: str, dispatch: ExecutionDispatch) -> None:
        with self._lock:
            if self._closing:
                raise RuntimeError("extension dispatcher is closing")
            existing = self._pending.get(key)
            if existing is not None and (
                existing.invocation_id != dispatch.invocation_id
                or existing.attempt != dispatch.attempt
            ):
                raise ValueError("pending extension invocation changed")
            self._pending[key] = dispatch
            self._wake.set()

    def cancel(self, key: str) -> None:
        with self._lock:
            self._pending.pop(key, None)
            event = self._cancellation.get(key)
            if event is not None:
                event.set()
            if key not in self.active_keys and key not in self._probing:
                self._withdraw(key)
            self._wake.set()

    def close(self) -> None:
        with self._lock:
            self._closing = True
            for event in self._cancellation.values():
                event.set()
            self._wake.set()
        self._thread.join()
        self.pool.shutdown(wait=True, cancel_futures=False)
        with self._lock:
            for key in tuple(self._owners):
                self._withdraw(key)
            self._pending.clear()
            # A submit failure can be ambiguous. Shutdown has now joined every
            # executor consumer, so its retained reservation can be released.
            permits = tuple(self._permits.values())
            self._permits.clear()
        for permit in permits:
            self._release(permit)
        with self._lock:
            cleanup_count = len(self._releases) + len(self._withdrawals)
        for _ in range(cleanup_count):
            self._cleanup(force=True)
        with self._lock:
            if self._releases or self._withdrawals:
                raise RuntimeError("extension resource cleanup remains unresolved")

    def _loop(self) -> None:
        while True:
            self._wake.wait(self.retry_seconds)
            self._wake.clear()
            self._cleanup()
            with self._lock:
                if self._closing:
                    return
                key = None
                capacity = len(self.active_keys) + len(self._probing) + len(self._releases)
                if capacity < self.maximum_workers:
                    for candidate, dispatch in tuple(self._pending.items()):
                        if (
                            candidate in self.active_keys
                            or self._due.get(candidate, 0) > time.monotonic()
                        ):
                            continue
                        key = candidate
                        owner = self._owners.setdefault(
                            key,
                            ExecutionOwner(
                                dispatch.invocation_id,
                                dispatch.attempt,
                                self._process_id,
                                secrets.token_hex(16),
                            ),
                        )
                        self._cancellation.setdefault(key, threading.Event())
                        self._probing.add(key)
                        self._pending.pop(key)
                        self._pending[key] = dispatch
                        break
            if key is not None:
                self._probe(key, owner)
                self._wake.set()

    def _probe(self, key: str, owner: ExecutionOwner) -> None:
        permit = None
        preparing: ExecutionDispatch | None = None
        try:
            with self._lock:
                dispatch = self._pending.get(key)
                cancellation = self._cancellation[key]
            if dispatch is None or cancellation.is_set():
                return
            dispatch.authorize(time.monotonic() + self.probe_seconds)
            deadline = time.monotonic() + self.probe_seconds
            permit = self.admission.probe(owner, deadline=deadline)
            if permit is not None and permit.owner != owner:
                permit = None
                raise ValueError("foreign extension admission permit")
            if permit is None:
                dispatch.deferred()
                return
            with self._lock:
                fresh = self._pending.get(key)
            if (
                self._closing
                or fresh is None
                or cancellation.is_set()
                or time.monotonic() >= deadline
            ):
                return
            fresh.authorize(time.monotonic() + self.probe_seconds)
            deadline = time.monotonic() + self.probe_seconds
            permit.activate(cancellation, deadline=deadline)
            with self._lock:
                if (
                    self._closing
                    or self._pending.get(key) is not fresh
                    or cancellation.is_set()
                    or time.monotonic() >= deadline
                ):
                    return
                self._permits[key] = permit
            preparing = fresh
            payload = fresh.prepare(cancellation, permit)
            if payload is None:
                with self._lock:
                    if self._pending.get(key) is fresh:
                        self._pending.pop(key, None)
                    self._permits.pop(key, None)
                return
            with self._lock:
                self._pending.pop(key, None)
                owned = permit
                permit = None
                try:
                    future = self.pool.submit(self._execute, key, fresh, payload, owned)
                except BaseException as exc:
                    # Retain a may-have-started reservation until pool shutdown
                    # proves stopped consumers. Completion still wins at its owner.
                    cancellation.set()
                    launch_error: BaseException | None = exc
                else:
                    launch_error = None
                    self._futures[key] = future
                    self._errors.pop(key, None)
            # No component callback runs under the dispatcher lock: control
            # handlers acquire their own durable-state lock before queue calls.
            if launch_error is not None:
                raise launch_error
            future.add_done_callback(lambda completed: self._forget(key, fresh, completed))
        except Exception as exc:
            if preparing is not None:
                preparing.failed(exc)
            with self._lock:
                if self._errors.get(key) != type(exc).__name__:
                    _LOG.warning(
                        "extension admission unavailable invocation=%s cause=%s",
                        key,
                        type(exc).__name__,
                    )
                    self._errors[key] = type(exc).__name__
        finally:
            if permit is not None:
                self._release(permit)
                with self._lock:
                    if self._permits.get(key) is permit:
                        self._permits.pop(key)
            with self._lock:
                self._probing.discard(key)
                self._due[key] = time.monotonic() + self.retry_seconds
                if key not in self._pending and key not in self.active_keys:
                    self._withdraw(key)

    def _execute(
        self,
        key: str,
        dispatch: ExecutionDispatch,
        payload: Callable[[], object],
        permit: ExecutionPermit,
    ) -> object:
        try:
            with consumer_scope(self.state_owner, self._cancellation[key]):
                return payload()
        except BaseException as exc:
            dispatch.failed(exc)
            raise
        finally:
            self._release(permit)
            with self._lock:
                if self._permits.get(key) is permit:
                    self._permits.pop(key)

    def _forget(self, key: str, dispatch: ExecutionDispatch, future: Future[object]) -> None:
        with self._lock:
            if self._futures.get(key) is not future:
                return
            self._futures.pop(key)
            self._finishing.add(key)
            self._withdraw(key)
        try:
            dispatch.finished()
        finally:
            with self._lock:
                self._finishing.discard(key)
                self._wake.set()

    def _withdraw(self, key: str) -> None:
        owner = self._owners.pop(key, None)
        if owner is not None:
            self._withdrawals[owner] = 0
        self._cancellation.pop(key, None)
        self._due.pop(key, None)
        self._errors.pop(key, None)

    def _release(self, permit: ExecutionPermit) -> None:
        try:
            permit.release(deadline=time.monotonic() + self.probe_seconds)
        except Exception as exc:
            with self._lock:
                self._releases[permit.owner] = permit
            _LOG.warning("extension resource release unresolved cause=%s", type(exc).__name__)
        else:
            with self._lock:
                self._releases.pop(permit.owner, None)

    def _cleanup(self, *, force: bool = False) -> None:
        with self._lock:
            releases = tuple(self._releases.values())[:1]
            withdrawals = tuple(
                owner
                for owner, due in self._withdrawals.items()
                if force or due <= time.monotonic()
            )[:1]
        for permit in releases:
            self._release(permit)
        for owner in withdrawals:
            try:
                self.admission.withdraw(owner, deadline=time.monotonic() + self.probe_seconds)
            except Exception as exc:
                with self._lock:
                    self._withdrawals[owner] = time.monotonic() + self.retry_seconds
                _LOG.warning(
                    "extension admission withdrawal unresolved cause=%s", type(exc).__name__
                )
            else:
                with self._lock:
                    self._withdrawals.pop(owner, None)


__all__ = ["BoundedExecutionDispatcher", "ExecutionDispatch"]

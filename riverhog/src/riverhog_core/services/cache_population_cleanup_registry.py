"""Bounded native cache cleanup in the upload service's existing execution owner."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from threading import Lock
from time import monotonic

from riverhog_protocol.errors import Conflict
from sqlalchemy import Select, select
from sqlalchemy.orm import load_only
from time_formats import format_utc_timestamp, utc_now

from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import RetrievalCachePopulationRecord as Population
from riverhog_core.domain.retrieval_cache import AbandonedCachePopulation
from riverhog_core.services.archive_construction_retention import (
    ArchiveConstructionReservation,
    ArchiveConstructionRetention,
)
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.services.cache_population_abandonment import CachePopulationAbandonment
from riverhog_core.services.cache_population_cleanup_turn import (
    CacheCleanupNotAdmitted,
    CachePopulationCleanupTurn,
)
from riverhog_core.services.physical_execution import (
    PhysicalExecutionKey,
    PhysicalExecutionOccupied,
    PhysicalExecutionOwner,
)

type CacheCleanupCreate = Callable[[AbandonedCachePopulation], CachePopulationCleanupTurn]

_LOG = logging.getLogger("riverhog_core.cache_population_cleanup_registry")


def _due_query(before: str, limit: int) -> Select[tuple[Population]]:
    return (
        select(Population)
        .options(
            load_only(
                Population.source_store,
                Population.collection_id,
                Population.object_id,
                Population.generation,
                Population.abandonment_receipt_json,
                Population.updated_at,
            )
        )
        .where(
            Population.state == "abandoning",
            Population.abandonment_receipt_json.is_not(None),
            Population.updated_at <= before,
        )
        .order_by(
            Population.updated_at,
            Population.source_store,
            Population.collection_id,
            Population.object_id,
        )
        .with_for_update(skip_locked=True)
        .limit(limit)
    )


@dataclass(slots=True)
class _Context:
    receipt: AbandonedCachePopulation
    reservation: ArchiveConstructionReservation
    entered: bool = False
    original: object | None = None
    actor: CachePopulationCleanupTurn | None = None
    received: bool = False
    resume_after: float = 0
    lock: Lock = field(default_factory=Lock)


class CachePopulationCleanupRegistry:
    """Retain bounded originals; the maintenance owner supplies scheduling.

    All physical and retained-construction grants are shared with archive work.
    A due timestamp carries no execution, receiving or disposal authority.
    """

    def __init__(
        self,
        factory: SessionFactory,
        execution: PhysicalExecutionOwner,
        retention: ArchiveConstructionRetention,
        create: CacheCleanupCreate,
    ) -> None:
        self._factory = factory
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

    def collect(self, *, limit: int) -> int:
        if type(limit) is not int or limit < 1:
            raise ValueError("cache cleanup receiving limit must be positive")
        if not self._lock.acquire(blocking=False):
            return 0
        progressed = 0
        try:
            for outcome in self._execution.collect(limit=limit, owner=self):
                context = self._contexts.get(outcome.key)
                if context is None or outcome.identity is not context:
                    raise Conflict("cache cleanup has another original physical receiver")
                if outcome.target_incarnation != context.reservation.target_incarnation:
                    raise Conflict("cache cleanup original physical target changed")
                context.received = True
                progressed += 1
                try:
                    outcome.result()
                except BaseException as error:
                    _LOG.warning(
                        "cache cleanup worker failed: collection_id=%s error_type=%s",
                        outcome.key.collection_id,
                        type(error).__name__,
                    )
                finally:
                    outcome.release_exception_frames()
            for _ in range(min(limit, len(self._contexts))):
                key, context = self._contexts.popitem(last=False)
                self._contexts[key] = context
                if not context.received or not context.lock.acquire(blocking=False):
                    continue
                try:
                    if not context.entered or (
                        context.actor is not None and context.actor.outcome is not None
                    ):
                        if self._retention.retire(context.reservation, owner=self):
                            del self._contexts[key]
                            progressed += 1
                finally:
                    context.lock.release()
            return progressed
        finally:
            self._lock.release()

    def _submit(self, context: _Context, *, now: float, retry_after: timedelta) -> int:
        key = context.reservation.key
        previous_received = context.received
        context.received = False
        try:
            result = self._execution.try_submit(
                key,
                target_incarnation=context.reservation.target_incarnation,
                terminal=True,
                work=partial(self._run, context),
                owner=self,
                identity=context,
            )
        except PhysicalExecutionOccupied:
            # This positive refusal entered no callback owned by this context.
            if not context.entered:
                context.received = True
            return 0
        except BaseException:
            # This receiver serializes submission and collect. The shared owner
            # removes a failed startup only when its thread never started; no
            # other receiver can consume this bucket while this lock is held.
            if not context.entered and self._execution.status(key) == "untracked":
                context.received = True
            raise
        if result == "submitted":
            context.received = False
            context.resume_after = now + retry_after.total_seconds()
            return 1
        context.received = previous_received
        if result != "retained" and not context.entered:
            context.received = True
        return 0

    def submit_due(self, *, limit: int, retry_after: timedelta) -> int:
        if type(limit) is not int or limit < 1 or retry_after.total_seconds() <= 0:
            raise ValueError("cache cleanup scheduling limits must be positive")
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
                    or (context.actor is not None and context.actor.outcome is not None)
                ):
                    continue
                # Retained originals resume even after another worker or a lost
                # finish reply removed their population scheduling row.
                submitted += self._submit(context, now=clock, retry_after=retry_after)
            if submitted == limit:
                return submitted
            now = utc_now()
            with session_scope(self._factory) as session:
                _writer(session, defer_if_busy=True)
                rows = list(
                    session.scalars(
                        _due_query(format_utc_timestamp(now - retry_after), limit - submitted)
                    )
                )
                selected = []
                for row in rows:
                    assert row.abandonment_receipt_json is not None
                    value = json.loads(row.abandonment_receipt_json)
                    receipt = AbandonedCachePopulation(
                        row.source_store,
                        row.collection_id,
                        row.object_id,
                        row.generation,
                        value["selected_terminal_sha256"],
                        row.abandonment_receipt_json,
                    )
                    CachePopulationAbandonment._value(receipt)
                    selected.append((receipt, value["cache_incarnation_id"]))
                    row.updated_at = format_utc_timestamp(now)
            for receipt, target in selected:
                key = PhysicalExecutionKey(
                    receipt.collection_id, hashlib.sha256(receipt.receipt_json.encode()).hexdigest()
                )
                selected_context = self._contexts.get(key)
                if selected_context is None:
                    reservation = self._retention.try_retain(
                        key, target_incarnation=target, owner=self
                    )
                    if isinstance(reservation, str):
                        continue
                    selected_context = _Context(receipt, reservation)
                    self._contexts[key] = selected_context
                else:
                    if (
                        selected_context.receipt != receipt
                        or selected_context.reservation.target_incarnation != target
                    ):
                        raise Conflict("cache cleanup retained original scope changed")
                    if not selected_context.received or selected_context.resume_after > clock:
                        continue
                    if selected_context.entered and selected_context.actor is None:
                        continue
                    if (
                        selected_context.actor is not None
                        and selected_context.actor.outcome is not None
                    ):
                        continue
                submitted += self._submit(selected_context, now=clock, retry_after=retry_after)
            return submitted
        finally:
            self._lock.release()

    def _run(self, context: _Context) -> str:
        # Only the physical worker may wait for or enter the original actor.
        with context.lock:
            if context.actor is None:
                if context.entered:
                    raise Conflict("original cache cleanup constructor is unresolved")
                context.entered = True
                try:
                    context.original = self._create(context.receipt)
                except CacheCleanupNotAdmitted:
                    context.entered = False
                    raise
                actor = context.original
                if (
                    not isinstance(actor, CachePopulationCleanupTurn)
                    or actor.key != context.reservation.key
                    or actor.target_incarnation != context.reservation.target_incarnation
                ):
                    raise Conflict("cache cleanup constructor returned another original receiver")
                context.actor = actor
            return context.actor.run()

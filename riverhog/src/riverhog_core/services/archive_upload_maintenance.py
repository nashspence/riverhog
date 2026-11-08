"""Bounded receiving and terminal drainage for service-owned upload work."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import timedelta
from threading import Lock

from riverhog_protocol.errors import Conflict
from sqlalchemy import Select, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only
from time_formats import format_utc_timestamp, utc_now

from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.services.archive_cache_families import ArchiveCacheFamilies
from riverhog_core.services.archive_finalizer_registry import ArchiveFinalizerRegistry
from riverhog_core.services.archive_upload_execution import ArchiveUploadExecutionKey
from riverhog_core.services.archive_upload_invocations import ArchiveUploadInvocationRegistry
from riverhog_core.services.archive_write_admission import _writer

_LOG = logging.getLogger("riverhog_core.archive_upload_maintenance")


def _due_query(now: str, limit: int) -> Select[tuple[ArchiveUploadTerminalRecord]]:
    return (
        select(ArchiveUploadTerminalRecord)
        .options(
            load_only(
                ArchiveUploadTerminalRecord.id,
                ArchiveUploadTerminalRecord.collection_id,
                ArchiveUploadTerminalRecord.drain_next_attempt_at,
            )
        )
        .where(
            ArchiveUploadTerminalRecord.phase == "draining",
            ArchiveUploadTerminalRecord.drain_next_attempt_at <= now,
        )
        .order_by(
            ArchiveUploadTerminalRecord.drain_next_attempt_at,
            ArchiveUploadTerminalRecord.id,
        )
        .with_for_update(skip_locked=True)
        .limit(limit)
    )


class ArchiveUploadMaintenance:
    """Receive original payload/metadata work and submit bounded terminal drains.

    This owner never adjudicates a terminal, disposes a preparation or reports
    global quiescence. A due time only controls scheduling; it cannot close an
    admitted invocation or release a physical worker. Original receiving runs
    in each retained owner before the worker publishes its outcome.
    """

    def __init__(
        self,
        factory: SessionFactory,
        finalizers: ArchiveFinalizerRegistry,
        invocations: ArchiveUploadInvocationRegistry,
        cache_families: ArchiveCacheFamilies,
        *,
        retry_after: timedelta,
    ) -> None:
        if retry_after.total_seconds() <= 0:
            raise ValueError("archive upload retry interval must be positive")
        self._factory = factory
        self._finalizers = finalizers
        self._invocations = invocations
        self._cache_families = cache_families
        self._retry_after = retry_after
        self._lock = Lock()

    def process_due(self, *, limit: int) -> int:
        if type(limit) is not int or limit < 1:
            raise ValueError("archive upload maintenance limit must be positive")
        if not self._lock.acquire(blocking=False):
            return 0
        progressed = 0
        try:
            completed = self._finalizers.collect(limit=limit)
            payloads = self._invocations.collect(limit=limit)
            progressed = len(completed) + len(payloads)
            for outcome in (*completed, *(value.execution for value in payloads)):
                try:
                    outcome.result()
                except BaseException as exc:
                    # The original actor/constructor failure remains retained.
                    # Receiving a failed physical result grants no disposal.
                    _LOG.warning(
                        "archive upload worker failed: collection_id=%s error_type=%s",
                        outcome.key.collection_id,
                        type(exc).__name__,
                    )
            progressed += self._cache_families.retire_ready(limit=limit)
            progressed += self._invocations.retire_ready(limit=limit)
            now = utc_now()
            next_attempt = format_utc_timestamp(now + self._retry_after)
            with session_scope(self._factory) as session:
                _writer(session, defer_if_busy=True)
                rows = list(session.scalars(_due_query(format_utc_timestamp(now), limit)))
                selected = [(row.collection_id, row.id) for row in rows]
                for row in rows:
                    row.drain_next_attempt_at = next_attempt
            for collection_id, operation_id in selected:
                with session_scope(self._factory) as session:
                    control = session.get(ArchiveWriteControlRecord, collection_id)
                    if control is None:
                        raise RuntimeError("selected terminal construction is unavailable")
                    construction = control.construction_json
                key = ArchiveUploadExecutionKey(
                    collection_id, hashlib.sha256(construction.encode()).hexdigest()
                )
                target = json.loads(construction)["archive_incarnation_id"]
                finalizers = self._finalizers.fence_terminal(
                    key, target_incarnation=target, operation_id=operation_id, defer_if_busy=True
                )
                invocations = self._invocations.fence_terminal(
                    key, target_incarnation=target, operation_id=operation_id, defer_if_busy=True
                )
                if "busy" in (finalizers, invocations):
                    continue
                owners: tuple[ArchiveFinalizerRegistry | ArchiveUploadInvocationRegistry, ...] = (
                    self._finalizers,
                    self._invocations,
                )
                if finalizers == invocations == "fenced":
                    turn = self._invocations.terminal_turn(
                        key, target_incarnation=target, operation_id=operation_id
                    )
                    if turn in {"busy", "untracked"}:
                        continue
                    if turn == "selected":
                        owners = (self._invocations, self._finalizers)
                for owner in owners:
                    submission = owner.submit_terminal(
                        key,
                        target_incarnation=target,
                        operation_id=operation_id,
                        defer_if_busy=True,
                    )
                    if submission == "submitted":
                        progressed += 1
                        break
            progressed += self._finalizers.process_ready(
                limit=limit, retry_after=self._retry_after, retire=self._retire_finalizer
            )
            return progressed
        except OperationalError as exc:
            if catalog_write_contention(exc):
                return progressed
            raise
        finally:
            self._lock.release()

    def _retire_finalizer(self, key: ArchiveUploadExecutionKey, target: str) -> bool:
        """Keep parent/control admission locked through the local idle release.

        A lost terminal scheduling reply can leave a closed gate with an
        unfenced local owner. Gate closure alone must not release that original
        before terminal drainage, even if its last workflow turn was idle.
        """
        try:
            with session_scope(self._factory) as session:
                _writer(session, defer_if_busy=True)
                session.scalar(
                    select(CollectionUploadRecord.collection_id)
                    .where(CollectionUploadRecord.collection_id == key.collection_id)
                    .with_for_update(nowait=True)
                )
                control = session.get(
                    ArchiveWriteControlRecord,
                    key.collection_id,
                    with_for_update={"nowait": True},
                    populate_existing=True,
                )
                if (
                    control is None
                    or hashlib.sha256(control.construction_json.encode()).hexdigest()
                    != key.construction_sha256
                    or json.loads(control.construction_json)["archive_incarnation_id"] != target
                ):
                    raise Conflict("retained finalizer retirement construction changed")
                if control.gate != "open":
                    return False
                return self._finalizers.retire_idle(key, target_incarnation=target)
        except OperationalError as exc:
            if catalog_write_contention(exc):
                return False
            raise

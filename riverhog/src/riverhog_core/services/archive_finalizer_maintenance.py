"""Bounded receiving and terminal drainage for service-owned finalizers."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import timedelta
from threading import Lock

from sqlalchemy import Select, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only
from time_formats import format_utc_timestamp, utc_now

from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.services.archive_finalizer_registry import ArchiveFinalizerRegistry
from riverhog_core.services.archive_upload_execution import ArchiveUploadExecutionKey
from riverhog_core.services.archive_write_admission import _writer

_LOG = logging.getLogger("riverhog_core.archive_finalizer_maintenance")


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


class ArchiveFinalizerMaintenance:
    """Poll original completions and submit due local terminal drains.

    This owner never adjudicates a terminal, disposes a preparation or reports
    global quiescence. A due time only controls scheduling; it cannot close an
    admitted invocation or release a physical worker. Original receiving runs
    in the retained finalizer before the worker publishes its outcome.
    """

    def __init__(
        self,
        factory: SessionFactory,
        registry: ArchiveFinalizerRegistry,
        *,
        retry_after: timedelta,
    ) -> None:
        if retry_after.total_seconds() <= 0:
            raise ValueError("archive finalizer retry interval must be positive")
        self._factory = factory
        self._registry = registry
        self._retry_after = retry_after
        self._lock = Lock()

    def process_due(self, *, limit: int) -> int:
        if type(limit) is not int or limit < 1:
            raise ValueError("archive finalizer maintenance limit must be positive")
        if not self._lock.acquire(blocking=False):
            return 0
        progressed = 0
        try:
            completed = self._registry.collect(limit=limit)
            progressed = len(completed)
            for outcome in completed:
                try:
                    outcome.result()
                except BaseException as exc:
                    # The original actor/constructor failure remains retained.
                    # Receiving a failed physical result grants no disposal.
                    _LOG.warning(
                        "archive finalizer worker failed: collection_id=%s error_type=%s",
                        outcome.key.collection_id,
                        type(exc).__name__,
                    )
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
                submission = self._registry.submit_terminal(
                    key,
                    target_incarnation=json.loads(construction)["archive_incarnation_id"],
                    operation_id=operation_id,
                    defer_if_busy=True,
                )
                progressed += submission == "submitted"
            return progressed
        except OperationalError as exc:
            if catalog_write_contention(exc):
                return progressed
            raise
        finally:
            self._lock.release()

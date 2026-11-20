"""Indexed scheduling of exact native terminal operations in the upload owner."""

from __future__ import annotations

import json
from datetime import timedelta

from riverhog_protocol.errors import Conflict
from sqlalchemy import Select, literal_column, select
from sqlalchemy.orm import load_only
from state_schema import read_snapshot
from time_formats import format_utc_timestamp, utc_now

from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord as Terminal,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord as Control,
)
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.services.archive_upload_terminal_registry import ArchiveUploadTerminalRegistry
from riverhog_core.services.archive_write_admission import _writer
from riverhog_core.services.physical_execution import PhysicalExecutionKey


def _due_query(now: str, limit: int) -> Select[tuple[Terminal]]:
    return (
        select(Terminal)
        .options(load_only(Terminal.id, Terminal.collection_id, Terminal.execution_next_attempt_at))
        .where(
            Terminal.execution_next_attempt_at.is_not(None),
            Terminal.phase != literal_column("'done'"),
            Terminal.execution_next_attempt_at <= now,
        )
        .order_by(Terminal.execution_next_attempt_at, Terminal.id)
        .with_for_update(skip_locked=True)
        .limit(limit)
    )


class ArchiveUploadTerminalDue:
    """Offer bounded work to the existing terminal registry, without authority.

    A committed due claim permits scheduling only. The exact original actor
    checks current authority, writer closure and all receiving before changing
    semantic state. Warm originals keep their own receiving and can resume even
    after a lost stopping or scheduling reply changed the durable due field.
    """

    def __init__(self, factory: SessionFactory, registry: ArchiveUploadTerminalRegistry) -> None:
        self._factory = factory
        self._registry = registry

    def collect(self, *, limit: int) -> int:
        return self._registry.collect(limit=limit)

    def submit_due(self, *, limit: int, retry_after: timedelta) -> int:
        if type(limit) is not int or limit < 1 or retry_after.total_seconds() <= 0:
            raise ValueError("terminal due scheduling limits must be positive")
        submitted = self._registry.resume_ready(
            limit=limit, work_limit=limit, retry_after=retry_after
        )
        if submitted == limit:
            return submitted
        now = utc_now()
        with session_scope(self._factory) as session:
            _writer(session, defer_if_busy=True)
            rows = list(session.scalars(_due_query(format_utc_timestamp(now), limit - submitted)))
            selected = [(row.collection_id, row.id) for row in rows]
            for row in rows:
                row.execution_next_attempt_at = format_utc_timestamp(now + retry_after)
        for collection_id, operation_id in selected:
            with read_snapshot(self._factory) as session:
                construction = session.scalar(
                    select(Control.construction_json).where(Control.collection_id == collection_id)
                )
                if construction is None:
                    raise Conflict("terminal due original construction is unavailable")
                target = json.loads(construction)["archive_incarnation_id"]
                if not isinstance(target, str) or not target:
                    raise Conflict("terminal due original target is invalid")
            submitted += self._registry.submit(
                PhysicalExecutionKey(collection_id, operation_id),
                target_incarnation=target,
                work_limit=limit,
                retry_after=retry_after,
            )
        return submitted

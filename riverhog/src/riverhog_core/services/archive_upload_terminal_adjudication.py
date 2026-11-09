"""Adjudicate the service's closed native upload construction before cleanup."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from riverhog_protocol.errors import Conflict
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.services.archive_custody_due import refresh_custody_parent
from riverhog_core.services.archive_upload_terminal_control import require_prepared_terminal_result
from riverhog_core.services.archive_upload_terminal_observation import (
    ArchiveUploadTerminalObservation,
    TerminalResourceObservation,
)
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector

PendingReason = Literal[
    "catalog-busy",
    "forward-effects",
    "original-receiving",
    "local-writers",
    "local-input",
    "local-execution",
    "original-result",
]


@dataclass(frozen=True, slots=True)
class TerminalAdjudication:
    phase: str | None
    pending_reason: PendingReason | None = None


def terminal_pending_reason(observed: TerminalResourceObservation) -> PendingReason | None:
    records = observed.records
    if records.archive_unclosed or records.cache_begin_unclosed or records.cache_write_unclosed:
        return "forward-effects"
    if records.archive_retained or records.cache_begin_retained or records.cache_write_retained:
        return "original-receiving"
    if observed.finalizers not in {"drained", "untracked"} or observed.invocations not in {
        "drained",
        "untracked",
    }:
        return "local-writers"
    if observed.inputs != "untracked":
        return "local-input"
    if observed.execution != "untracked":
        return "local-execution"
    return None


class ArchiveUploadTerminalAdjudication:
    """Compose native writer admission and original receiving into one decision.

    All native provider writers arm through this construction's durable gate.
    Registration and catalog publication share that gate; copy writers require
    published roots and cannot overlap its retained upload. Consequently a closed
    exact gate, no retained forward records, fenced/drained known local owners,
    no retained input and actual original execution receiving form the proof.
    Local absence alone supplies none of it.

    This work kind belongs to the existing upload service and its physical pool.
    It performs bounded indexed catalog work only: no provider IO, join, crypto,
    registration disposal, outcome confirmation or second scheduler. Destructive
    batches still need current authority and their own exact cleanup receiving.
    """

    def __init__(
        self, factory: SessionFactory, observation: ArchiveUploadTerminalObservation
    ) -> None:
        self._factory = factory
        self._observation = observation

    def adjudicate(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
    ) -> TerminalAdjudication:
        if not callable(guard):
            raise ValueError("terminal adjudication requires current authority")
        try:
            with session_scope(self._factory) as session:
                observed = self._observation.observe(
                    session, collection_id, operation_id, guard=guard
                )
                terminal = session.get(ArchiveUploadTerminalRecord, operation_id)
                assert terminal is not None
                if terminal.phase != "draining":
                    return TerminalAdjudication(terminal.phase)
                reason = terminal_pending_reason(observed)
                if reason is not None:
                    return TerminalAdjudication(terminal.phase, reason)
                if terminal.kind == "cancel" and observed.records.payload_sealed:
                    terminal.phase = "refused-sealed"
                    return TerminalAdjudication(terminal.phase)
                if terminal.kind == "discard" and (
                    observed.records.custody_revision != observed.records.selected_custody_revision
                ):
                    terminal.phase = "needs-reconfirmation"
                    return TerminalAdjudication(terminal.phase)
                upload = session.get(CollectionUploadRecord, collection_id)
                assert upload is not None
                if terminal.kind == "discard" and upload.state != "orphaned":
                    raise Conflict("upload terminal discard requires its original orphan")
                if terminal.original_result_json is None:
                    return TerminalAdjudication(terminal.phase, "original-result")
                require_prepared_terminal_result(
                    terminal, load_terminal_tag_selector(session, operation_id)
                )
                terminal.phase = "cleanup-ready"
                upload.state = "discarding"
                upload.archive_phase = "discarding"
                upload.archive_phase_updated_at = utc_timestamp_now()
                upload.archive_next_attempt_at = None
                refresh_custody_parent(session, upload)
                return TerminalAdjudication(terminal.phase)
        except OperationalError as exc:
            if catalog_write_contention(exc):
                return TerminalAdjudication(None, "catalog-busy")
            raise

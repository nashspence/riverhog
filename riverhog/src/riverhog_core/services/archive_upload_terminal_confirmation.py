"""Confirm one original upload-terminal result after fresh complete cleanup proof."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, cast

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import delete, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord as Terminal,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteEffectRecord as Effect,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWritePreparationRecord as Preparation,
)
from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import (
    CollectionRecord as Collection,
)
from riverhog_core.catalog_models import (
    CollectionUploadRecord as Upload,
)
from riverhog_core.incremental_emission import VolumeEmissionCapacity, VolumeEmissionLimits
from riverhog_core.services.archive_upload_catalog_cleanup import (
    UPLOAD_CHILD_TABLES,
    validate_upload_cleanup_routes,
)
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.archive_upload_terminal_adjudication import _pending
from riverhog_core.services.archive_upload_terminal_control import (
    require_prepared_terminal_result,
    require_terminal_control_capacity,
    terminal_control_bytes,
)
from riverhog_core.services.archive_upload_terminal_observation import (
    ArchiveUploadTerminalObservation,
)
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector
from riverhog_core.services.catalog_retirement import metadata_row_bytes

Guard = Callable[[Session], None]


def _confirmed(session: Session, terminal: Terminal, collection_id: int) -> str | None:
    if terminal.phase != "done":
        return None
    if terminal.original_result_json is not None or terminal.result_json is None:
        raise Conflict("confirmed upload terminal has inconsistent original receiving")
    if (
        session.scalar(
            select(1).select_from(Upload).where(Upload.collection_id == collection_id).limit(1)
        )
        is not None
    ):
        raise Conflict("confirmed upload terminal still retains its upload")
    value = terminal.result_json
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, dict) or canonical_json_bytes(parsed).decode() != value:
            raise ValueError("noncanonical confirmed result")
    except (TypeError, ValueError):
        raise Conflict("confirmed upload terminal result is invalid") from None
    require_terminal_control_capacity(terminal, load_terminal_tag_selector(session, terminal.id))
    return value


class ArchiveUploadTerminalConfirmation:
    """Transfer exact reserved result bytes atomically with an empty upload removal.

    Current read rights authorize only already confirmed readback. A new confirmation
    also requires current disposal rights, fresh original resource closure, positive
    archive inventory proof and indexed absence of every required cleanup child.
    Independent capacity, original selectors and receiving records remain retained.
    """

    def __init__(
        self,
        factory: SessionFactory,
        cleanup: ArchiveUploadCleanup,
        observation: ArchiveUploadTerminalObservation,
    ) -> None:
        validate_upload_cleanup_routes()
        self._factory = factory
        self._cleanup = cleanup
        self._observation = observation

    def confirmed(self, collection_id: int, operation_id: str, *, guard: Guard) -> str | None:
        if not callable(guard):
            raise ValueError("confirmed terminal readback requires current authority")
        with session_scope(self._factory) as session:
            _, terminal = lock_terminal_selection(
                session, collection_id, operation_id, require_upload=False, nowait=True, guard=guard
            )
            return _confirmed(session, terminal, collection_id)

    def confirm(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Guard,
        replay_guard: Guard,
        limits: VolumeEmissionLimits,
    ) -> str:
        if not callable(guard) or not callable(replay_guard):
            raise ValueError(
                "terminal confirmation requires current disposal and readback authority"
            )
        with session_scope(self._factory) as session:
            try:
                _, terminal = lock_terminal_selection(
                    session, collection_id, operation_id, nowait=True, guard=guard
                )
            except (NotFound, Conflict):
                _, terminal = lock_terminal_selection(
                    session,
                    collection_id,
                    operation_id,
                    require_upload=False,
                    nowait=True,
                    guard=replay_guard,
                )
                replayed = _confirmed(session, terminal, collection_id)
                if replayed is None:
                    raise
                return replayed
            _confirmed(session, terminal, collection_id)
            observed = self._observation.observe(session, collection_id, operation_id, guard=guard)
            reason = _pending(observed)
            if (
                reason is not None
                or observed.finalizers != "untracked"
                or observed.invocations != "untracked"
            ):
                raise Conflict("terminal confirmation still has original resource custody")
            self._cleanup.require_exhausted(session, collection_id, operation_id, guard=guard)
            upload = session.get(Upload, collection_id)
            assert upload is not None
            if upload.active_registration_id is not None:
                raise Conflict("terminal confirmation still has hidden registration")
            for name in (
                *UPLOAD_CHILD_TABLES,
                Effect.__tablename__,
                Preparation.__tablename__,
                Job.__tablename__,
            ):
                table = Base.metadata.tables[name]
                if (
                    session.scalar(
                        select(1)
                        .select_from(table)
                        .where(table.c.collection_id == collection_id)
                        .limit(1)
                    )
                    is not None
                ):
                    raise Conflict("terminal confirmation still has required cleanup: " + name)
            intents = Base.metadata.tables["collection_upload_copy_intents"]
            if (
                session.scalar(
                    select(1)
                    .select_from(intents)
                    .where(intents.c.collection_id == collection_id, intents.c.state != "canceled")
                    .limit(1)
                )
                is not None
            ):
                raise Conflict("terminal confirmation still has original copy-intent work")
            if (
                session.scalar(
                    select(1).select_from(Collection).where(Collection.id == collection_id).limit(1)
                )
                is not None
            ):
                raise Conflict("terminal confirmation still has an unpublished projection")
            selector = load_terminal_tag_selector(session, operation_id)
            original = require_prepared_terminal_result(terminal, selector)
            used = terminal_control_bytes(terminal, selector) + metadata_row_bytes(
                {column.name: getattr(upload, column.name) for column in Upload.__table__.columns}
            )
            if limits.input_transitions < 2:
                raise VolumeEmissionCapacity(
                    dimension="input_transitions", required=2, admitted=limits.input_transitions
                )
            if used > limits.metadata_bytes:
                raise VolumeEmissionCapacity(
                    dimension="metadata_bytes", required=used, admitted=limits.metadata_bytes
                )
            terminal.result_json = original
            terminal.original_result_json = None
            terminal.phase = "done"
            terminal.execution_next_attempt_at = None
            require_terminal_control_capacity(terminal, selector)
            removed = session.execute(
                delete(Base.metadata.tables[Upload.__tablename__]).where(
                    Upload.collection_id == collection_id
                )
            )
            if cast(CursorResult[Any], removed).rowcount != 1:
                raise Conflict("original terminal upload changed during confirmation")
            session.flush()
            return original

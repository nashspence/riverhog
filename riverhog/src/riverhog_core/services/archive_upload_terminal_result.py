"""Receive one bounded original terminal result before destructive cleanup."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.errors import Conflict
from sqlalchemy.orm import Session

from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.archive_upload_terminal_control import require_prepared_terminal_result
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector


class ArchiveUploadTerminalResults:
    """Retain a proposed result, without adjudicating or confirming its outcome.

    The composing terminal actor freezes source-owned original result fields.
    Each exact database retry receives those same bytes, including after a lost
    commit acknowledgement. No provider call, object count, tag walk or cleanup
    runs here. A prepared result is never a confirmed cancellation/discard.
    """

    def __init__(self, factory: SessionFactory) -> None:
        self._factory = factory

    def prepare(
        self,
        collection_id: int,
        operation_id: str,
        result: Mapping[str, object],
        *,
        guard: Callable[[Session], None],
    ) -> str:
        if not callable(guard):
            raise ValueError("terminal result preparation requires current authority")
        encoded = canonical_json_bytes(dict(result)).decode()
        with session_scope(self._factory) as session:
            _, terminal = lock_terminal_selection(
                session, collection_id, operation_id, nowait=True, guard=guard
            )
            if terminal.phase not in {"draining", "cleanup-ready", "cleaning"}:
                raise Conflict("upload terminal result cannot be prepared in this phase")
            if terminal.result_preparation_json is not None:
                raise Conflict("upload terminal original result projection is in progress")
            if terminal.original_result_json is None:
                if terminal.phase != "draining" or terminal.result_json is not None:
                    raise Conflict("upload terminal original result must precede cleanup")
                terminal.original_result_json = encoded
            elif terminal.original_result_json != encoded:
                raise Conflict("upload terminal original result differs")
            return require_prepared_terminal_result(
                terminal, load_terminal_tag_selector(session, operation_id)
            )

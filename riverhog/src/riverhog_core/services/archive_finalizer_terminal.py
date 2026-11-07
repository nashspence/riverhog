"""Fence one retained finalizer while preserving its original inputs."""

from __future__ import annotations

from typing import Literal, Protocol

from riverhog_protocol.errors import Conflict

from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.services.archive_upload_drain import inspect_terminal_write_records

TerminalDrainStatus = Literal["busy", "pending", "closed"]


class OriginalFinalizerWrite(Protocol):
    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus: ...


class ArchiveFinalizerTerminal:
    """Called only under the retained finalizer's own lock.

    Closed fences forward work and acknowledges the original child's drainage.
    It does not acknowledge logical progress or release retained source, crypto,
    worker, preparation or buffer ownership. Global drainage has separate owners.
    """

    def __init__(self, factory: SessionFactory, collection_id: int) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._operation_id: str | None = None
        self._drained = False

    def require_forward(self) -> None:
        if self._operation_id is not None:
            raise Conflict("archive finalizer was stopped for its terminal request")

    def drain_original(
        self, operation_id: str, actor: OriginalFinalizerWrite | None
    ) -> TerminalDrainStatus:
        if self._operation_id is not None:
            if self._operation_id != operation_id:
                raise Conflict("archive finalizer has another terminal request")
            if self._drained:
                return "closed"
        else:
            with session_scope(self._factory) as session:
                inspect_terminal_write_records(session, self._collection_id, operation_id)
            # Fence selection, staging and encryption before child report/proof IO.
            self._operation_id = operation_id
        result: TerminalDrainStatus = (
            actor.drain_terminal(operation_id) if actor is not None else "closed"
        )
        self._drained = result == "closed"
        return result

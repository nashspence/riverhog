"""Reload current authority for an exact retained upload-terminal continuation."""

from __future__ import annotations

from collections.abc import Callable

from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.app_permissions import Principal
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.services.app_keys import (
    KeyAuthorizationFence,
    load_retained_key_principal,
    retain_key_validity,
)
from riverhog_core.services.archive_upload_drain import read_terminal_selection
from riverhog_core.services.collection_upload_authority import retain_processing_upload_principal

Guard = Callable[[Session], None]
GuardFactory = Callable[[str, Principal], Guard]


class ArchiveUploadTerminalAuthority:
    """Retain key -> claim -> upload scope using today's protected authority.

    Construction and invocation belong to one serialized terminal actor. No
    credentials or admission-time grants are persisted. A reusable proof is
    invalidated by the current key generation; revoked or expired rights pause
    work, while a later authorized grant set can resume the original request.
    This owner supplies no orphan eligibility, quiescence or disposal decision.
    """

    def __init__(
        self,
        factory: SessionFactory,
        collection_id: int,
        operation_id: str,
        guard_factory: GuardFactory,
    ) -> None:
        with read_snapshot(factory) as session:
            _, terminal, _ = read_terminal_selection(session, collection_id, operation_id)
            self._principal_id = terminal.principal_id
            self._key_id = terminal.key_id
            self._kind = terminal.kind
            self._request = terminal.request_json
        self._collection_id = collection_id
        self._operation_id = operation_id
        self._guard_factory = guard_factory
        self._key: KeyAuthorizationFence | None = None
        self._guard: Guard | None = None

    def __call__(self, session: Session) -> None:
        """The caller owns an early writer before any parent or terminal lock."""
        if self._key_id is None:
            raise NotFound("upload terminal current key authority is unavailable")
        key = retain_key_validity(session, self._key_id, nowait=True)
        principal: Principal | None
        delegated = self._principal_id.startswith("processing:")
        if delegated:
            if self._kind != "cancel":
                raise NotFound("upload terminal delegated deletion authority is unavailable")
            principal = retain_processing_upload_principal(
                session, self._principal_id, key, nowait=True
            )
        else:
            if key.app != self._principal_id:
                raise NotFound("upload terminal selecting application changed")
            principal = (
                load_retained_key_principal(session, key)
                if self._key != key or self._guard is None
                else None
            )
        if self._key != key or self._guard is None:
            assert principal is not None
            self._guard = self._guard_factory(self._kind, principal)
            self._key = key
        self._guard(session)
        _, terminal, _ = read_terminal_selection(session, self._collection_id, self._operation_id)
        if terminal.request_json != self._request:
            raise Conflict("upload terminal original authorization context changed")

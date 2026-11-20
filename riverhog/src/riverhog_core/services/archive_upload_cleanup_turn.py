"""Bounded archive inventory/disposal receiving for one original terminal."""

from __future__ import annotations

import json
from collections.abc import Callable

from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import CleanupExecutionResult, CleanupInvocation
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.cleanup_execution_tuning import CleanupExecutionTuning
from riverhog_core.ports.object_cleanup import ObjectCleanupPort
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.archive_upload_drain import read_terminal_selection
from riverhog_core.services.object_cleanup_turn import CleanupTurnProgress, ObjectCleanupTurn
from riverhog_core.services.physical_execution import PhysicalExecutionKey


class _ArchiveTarget:
    """Check the current exact binding without replacing an admitted original."""

    def __init__(self, stores: ArchiveStoreRegistry, name: str, incarnation: str) -> None:
        self._stores = stores
        self._name = name
        self._incarnation = incarnation
        self._original: ObjectCleanupPort | None = None

    def before_admission(self) -> None:
        target = self._stores.require_incarnation(
            self._name, self._incarnation
        ).require_object_cleanup()
        if self._original is None:
            self._original = target

    def execute_cleanup(self, invocation: CleanupInvocation) -> CleanupExecutionResult:
        original = self._original
        assert original is not None
        return original.execute_cleanup(invocation)


class ArchiveUploadCleanupTurn:
    """Receive at most one provider invocation; never confirm a terminal outcome."""

    def __init__(
        self,
        factory: SessionFactory,
        collection_id: int,
        operation_id: str,
        *,
        owner: ArchiveUploadCleanup,
        archive_stores: ArchiveStoreRegistry,
        guard: Callable[[Session], None],
        tuning: CleanupExecutionTuning,
    ) -> None:
        self._key = PhysicalExecutionKey(collection_id, operation_id)
        with read_snapshot(factory) as session:
            control, _, _ = read_terminal_selection(session, collection_id, operation_id)
            construction = json.loads(control.construction_json)
            store = construction["archive_store"]
            incarnation = construction["archive_incarnation_id"]
            prefix = construction["archive_storage_prefix"]
            if (
                not isinstance(store, str)
                or not store
                or not isinstance(incarnation, str)
                or not incarnation
                or not isinstance(prefix, str)
                or not prefix
            ):
                raise Conflict("archive cleanup original target scope is invalid")
        self._factory = factory
        self._owner = owner
        self._guard = guard
        self._target_incarnation = incarnation
        target = _ArchiveTarget(archive_stores, store, incarnation)
        self._execution = ObjectCleanupTurn(
            owner,
            target,
            collection_id=collection_id,
            operation_id=operation_id,
            target_incarnation=incarnation,
            object_prefix=prefix,
            guard=guard,
            tuning=tuning,
            before_admission=target.before_admission,
        )

    @property
    def key(self) -> PhysicalExecutionKey:
        return self._key

    @property
    def target_incarnation(self) -> str:
        return self._target_incarnation

    def advance(self) -> CleanupTurnProgress:
        result = self._execution.advance()
        if result == "inventory-exhausted":
            with session_scope(self._factory) as session:
                self._owner.require_exhausted(
                    session, self._key.collection_id, self._key.scope_sha256, guard=self._guard
                )
        return result

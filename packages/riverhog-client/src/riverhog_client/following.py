"""Bounded catalog traversal with application-owned durable progress and effects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, cast

from riverhog_protocol import (
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    CatalogSyncDeparture,
    CatalogSyncDescriptor,
    CatalogSyncUpsert,
)

from riverhog_client._catalog_sync import _start, _step, _SyncBatch, _SyncState

type CatalogFollowPhase = Literal["new", "catalog", "catchup", "following", "reset_required"]
type CatalogFollowKind = Literal["checkpoint", "catalog", "changes", "reset"]


class CatalogFollowApi(Protocol):
    def create_catalog_sync_checkpoint(self) -> CatalogSyncCheckpoint: ...
    def list_catalog_sync_collections(
        self, cursor: str, *, limit: int = 100
    ) -> CatalogSyncCollectionPage: ...
    def list_catalog_sync_changes(
        self, cursor: str, *, limit: int = 100
    ) -> CatalogSyncChangePage: ...


@dataclass(frozen=True)
class CatalogFollowPosition:
    """Typed progress for a caller-owned transaction; no database or blob format is implied."""

    phase: CatalogFollowPhase = "new"
    source_identity: str | None = None
    authorization_view_identity: str | None = None
    cursor: str | None = None
    through_revision: str = "0"
    last_collection_id: int | None = None
    reset_reason: str | None = None

    def __post_init__(self) -> None:
        self._internal()

    def _internal(self) -> _SyncState:
        return _SyncState.model_validate(
            {
                "phase": self.phase,
                "source_identity": self.source_identity,
                "authorization_view_identity": self.authorization_view_identity,
                "cursor": self.cursor,
                "through_revision": self.through_revision,
                "last_collection_id": (
                    str(self.last_collection_id) if self.last_collection_id is not None else None
                ),
                "reset_reason": self.reset_reason,
            }
        )

    @classmethod
    def _from_internal(cls, state: _SyncState) -> CatalogFollowPosition:
        return cls(
            phase=state.phase,
            source_identity=state.source_identity,
            authorization_view_identity=state.authorization_view_identity,
            cursor=state.cursor,
            through_revision=state.through_revision,
            last_collection_id=state.last_collection_id,
            reset_reason=state.reset_reason,
        )


@dataclass(frozen=True)
class CatalogFollowBatch:
    """One proposal; commit accepted observations and after in one caller transaction."""

    kind: CatalogFollowKind
    before: CatalogFollowPosition
    after: CatalogFollowPosition
    collections: tuple[CatalogSyncDescriptor, ...] = ()
    changes: tuple[CatalogSyncUpsert | CatalogSyncDeparture, ...] = ()


def _public_batch(batch: _SyncBatch) -> CatalogFollowBatch:
    if batch.kind == "catalog":
        collections = cast(tuple[CatalogSyncDescriptor, ...], batch.observations)
        changes: tuple[CatalogSyncUpsert | CatalogSyncDeparture, ...] = ()
    elif batch.kind == "changes":
        collections = ()
        changes = cast(tuple[CatalogSyncUpsert | CatalogSyncDeparture, ...], batch.observations)
    else:
        collections = ()
        changes = ()
    return CatalogFollowBatch(
        kind=batch.kind,
        before=CatalogFollowPosition._from_internal(batch.before),
        after=CatalogFollowPosition._from_internal(batch.next_state),
        collections=collections,
        changes=changes,
    )


class CatalogFollower:
    """Fetch one validated page; the application owns fencing, persistence, and retries."""

    def __init__(self, api: CatalogFollowApi) -> None:
        self.api = api

    def start(self, position: CatalogFollowPosition) -> CatalogFollowBatch:
        """Request a fresh checkpoint, including after an explicit rebaseline decision."""

        return _public_batch(_start(self.api, position._internal()))

    def step(self, position: CatalogFollowPosition, *, limit: int = 100) -> CatalogFollowBatch:
        return _public_batch(_step(self.api, position._internal(), limit=limit))


__all__ = [
    "CatalogFollowApi",
    "CatalogFollowBatch",
    "CatalogFollowKind",
    "CatalogFollowPhase",
    "CatalogFollowPosition",
    "CatalogFollower",
]

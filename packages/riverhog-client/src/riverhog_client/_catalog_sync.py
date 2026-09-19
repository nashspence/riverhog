"""Private, persistence-free catalog traversal experiment for #870.

Not a public client API or a supported durable-state format. Neither existing
consumer is migrated here. Each call makes at most one catalog request and
returns observations plus a proposed state; it never accepts its own result.

The caller must atomically apply observations and save next_state, fencing its
own row version/generation against concurrent workers. A batch's before value
is evidence for that check, not a lock. On rollback, retry the persisted state.
Retries need not return identical pages: bootstrap reads current descriptors,
and an unfixed following cursor can see a later horizon. Per-collection revision
reconciliation (including equal-revision conflicts) remains application-owned.

Bootstrap descriptors deliberately remain distinct from change events. Their
revisions are NOT feed watermarks; catchup can replay older/equal observations.
A reset reports lost synchronization authority, not permission to delete data
or automatically rebootstrap. Only an explicit start leaves reset_required.
Tag hydration, projection publication, policies, and external effects are absent.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, Self, cast, get_args

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from riverhog_protocol import (
    CATALOG_SYNC_CURSOR_BYTES_MAX,
    CATALOG_SYNC_PAGE_SIZE_MAX,
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    CatalogSyncCursor,
    CatalogSyncDelete,
    CatalogSyncDescriptor,
    CatalogSyncIdentity,
    CatalogSyncPosition,
    CatalogSyncUpsert,
    CollectionId,
)
from riverhog_protocol.errors import RiverhogError

_ResetReason = Literal[
    "catalog_sync_cursor_expired",
    "catalog_sync_history_expired",
    "catalog_sync_source_changed",
    "catalog_sync_view_changed",
    "unauthorized",
    "forbidden",
]


class _SyncApi(Protocol):
    """Only the three catalog operations, not CatalogSyncApi's tag hydration."""

    def create_catalog_sync_checkpoint(self) -> CatalogSyncCheckpoint: ...
    def list_catalog_sync_collections(
        self, cursor: str, *, limit: int = 100
    ) -> CatalogSyncCollectionPage: ...
    def list_catalog_sync_changes(
        self, cursor: str, *, limit: int = 100
    ) -> CatalogSyncChangePage: ...


class _SyncState(BaseModel):
    """JSON-roundtrippable proposed progress, never mutated by a request."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    phase: Literal["new", "catalog", "catchup", "following", "reset_required"] = "new"
    source_identity: CatalogSyncIdentity | None = None
    authorization_view_identity: CatalogSyncIdentity | None = None
    cursor: CatalogSyncCursor | None = None
    through_revision: CatalogSyncPosition = "0"
    last_collection_id: CollectionId | None = None
    reset_reason: _ResetReason | None = None

    @field_validator("cursor")
    @classmethod
    def validate_cursor(cls, value: str | None) -> str | None:
        if value is not None:
            _check_cursor(value)
        return value

    @model_validator(mode="after")
    def validate_phase(self) -> Self:
        bound = self.source_identity is not None and self.authorization_view_identity is not None
        if (self.source_identity is None) != (self.authorization_view_identity is None):
            raise ValueError("catalog state must bind both source and view")
        if self.phase == "new":
            if bound or self.cursor is not None or self.through_revision != "0":
                raise ValueError("new catalog state cannot contain progress")
        elif self.phase == "reset_required":
            if self.cursor is not None or self.reset_reason is None:
                raise ValueError("reset catalog state must stop traversal and name its reason")
            if not bound and self.through_revision != "0":
                raise ValueError("unbound catalog state cannot contain progress")
        elif not bound or self.cursor is None:
            raise ValueError("active catalog state requires authority and a cursor")
        if self.phase != "reset_required" and self.reset_reason is not None:
            raise ValueError("only reset catalog state can contain a reset reason")
        if self.phase != "catalog" and self.last_collection_id is not None:
            raise ValueError("only catalog traversal can contain a collection position")
        if self.phase == "catalog" and self.through_revision != "0":
            raise ValueError("bootstrap descriptor revisions are not feed progress")
        return self


@dataclass(frozen=True)
class _SyncBatch:
    kind: Literal["checkpoint", "catalog", "changes", "reset"]
    before: _SyncState
    next_state: _SyncState
    observations: tuple[CatalogSyncDescriptor | CatalogSyncUpsert | CatalogSyncDelete, ...] = ()


def _check_cursor(cursor: str) -> None:
    # Opaque: do not decode server tokens or infer revisions/deadlines from them.
    if not 1 <= len(cursor.encode("utf-8")) <= CATALOG_SYNC_CURSOR_BYTES_MAX:
        raise ValueError("catalog cursor is outside its byte bound")


def _reset(state: _SyncState, reason: _ResetReason) -> _SyncBatch:
    return _SyncBatch(
        kind="reset",
        before=state,
        next_state=_SyncState(
            phase="reset_required",
            source_identity=state.source_identity,
            authorization_view_identity=state.authorization_view_identity,
            through_revision=state.through_revision,
            reset_reason=reason,
        ),
    )


def _start(api: _SyncApi, state: _SyncState) -> _SyncBatch:
    """Explicitly propose a fresh bootstrap, even across a reset/authority change."""

    state = _SyncState.model_validate(state.model_dump())
    try:
        checkpoint = api.create_catalog_sync_checkpoint()
    except RiverhogError as exc:
        if exc.code in get_args(_ResetReason):
            return _reset(state, cast(_ResetReason, exc.code))
        raise
    if not isinstance(checkpoint, CatalogSyncCheckpoint):
        raise ValueError("catalog checkpoint response has the wrong document type")
    checkpoint = CatalogSyncCheckpoint.model_validate(checkpoint.model_dump())
    return _SyncBatch(
        kind="checkpoint",
        before=state,
        next_state=_SyncState(
            phase="catalog",
            source_identity=checkpoint.source_identity,
            authorization_view_identity=checkpoint.authorization_view_identity,
            cursor=checkpoint.catalog_cursor,
        ),
    )


def _step(api: _SyncApi, state: _SyncState, *, limit: int = 100) -> _SyncBatch:
    """Propose one bounded page, or checkpoint for new state; never loop/retry."""

    if isinstance(limit, bool) or not isinstance(limit, int) or not (
        1 <= limit <= CATALOG_SYNC_PAGE_SIZE_MAX
    ):
        raise ValueError("catalog page size is outside the protocol bound")
    state = _SyncState.model_validate(state.model_dump())
    if state.phase == "new":
        return _start(api, state)
    if state.phase == "reset_required":
        raise RuntimeError("catalog synchronization requires an explicit start")
    assert state.cursor is not None
    try:
        if state.phase == "catalog":
            page = api.list_catalog_sync_collections(state.cursor, limit=limit)
            if not isinstance(page, CatalogSyncCollectionPage):
                raise ValueError("catalog page response has the wrong document type")
            if len(page.collections) > limit:
                raise ValueError("catalog page exceeded its requested bound")
            # Frozen wire models contain mutable lists. Revalidate and detach the
            # response before proposing observations/progress to the caller.
            page = CatalogSyncCollectionPage.model_validate(page.model_dump())
            return _catalog_page(state, page)
        changes = api.list_catalog_sync_changes(state.cursor, limit=limit)
        if not isinstance(changes, CatalogSyncChangePage):
            raise ValueError("catalog changes response has the wrong document type")
        if len(changes.changes) > limit:
            raise ValueError("catalog changes exceeded their requested bound")
        changes = CatalogSyncChangePage.model_validate(changes.model_dump())
        return _change_page(state, changes)
    except RiverhogError as exc:
        if exc.code in get_args(_ResetReason):
            return _reset(state, cast(_ResetReason, exc.code))
        raise


def _authority_reset(
    state: _SyncState, page: CatalogSyncCollectionPage | CatalogSyncChangePage
) -> _SyncBatch | None:
    if page.source_identity != state.source_identity:
        return _reset(state, "catalog_sync_source_changed")
    if page.authorization_view_identity != state.authorization_view_identity:
        return _reset(state, "catalog_sync_view_changed")
    return None


def _catalog_page(state: _SyncState, page: CatalogSyncCollectionPage) -> _SyncBatch:
    reset = _authority_reset(state, page)
    if reset is not None:
        return reset
    previous = state.last_collection_id or 0
    for item in page.collections:
        if item.collection_id <= previous:
            raise ValueError("catalog collection order did not advance")
        previous = item.collection_id
    cursor = page.next_cursor if page.next_cursor is not None else page.changes_cursor
    assert cursor is not None  # The wire model requires exactly one continuation.
    if cursor == state.cursor:
        raise ValueError("catalog cursor did not advance")
    return _SyncBatch(
        kind="catalog",
        before=state,
        observations=tuple(page.collections),
        next_state=_SyncState(
            phase="catalog" if page.next_cursor is not None else "catchup",
            source_identity=state.source_identity,
            authorization_view_identity=state.authorization_view_identity,
            cursor=cursor,
            last_collection_id=previous if page.next_cursor is not None else None,
        ),
    )


def _change_page(state: _SyncState, page: CatalogSyncChangePage) -> _SyncBatch:
    reset = _authority_reset(state, page)
    if reset is not None:
        return reset
    after = int(state.through_revision)
    through = int(page.through_revision)
    if through < after:
        raise ValueError("catalog through revision moved backward")
    previous = after
    for item in page.changes:
        revision = int(item.revision)
        if revision <= previous:
            raise ValueError("catalog change revisions did not advance")
        previous = revision
    if through < previous:
        raise ValueError("catalog through revision precedes its changes")
    if not page.caught_up and through == after:
        raise ValueError("catalog change page did not advance")
    # Idle polls may reuse a token, or renew it with the same through-position.
    if page.next_cursor == state.cursor and (page.changes or through != after):
        raise ValueError("catalog change cursor did not advance with its revision")
    return _SyncBatch(
        kind="changes",
        before=state,
        observations=tuple(page.changes),
        next_state=_SyncState(
            phase="following" if page.caught_up else state.phase,
            source_identity=state.source_identity,
            authorization_view_identity=state.authorization_view_identity,
            cursor=page.next_cursor,
            through_revision=page.through_revision,
        ),
    )

from __future__ import annotations

import json
import sqlite3
from dataclasses import FrozenInstanceError
from typing import Any, Literal, cast

import pytest
from pydantic import ValidationError
from riverhog_client import _catalog_sync as sync
from riverhog_protocol import (
    CATALOG_SYNC_CURSOR_BYTES_MAX,
    CATALOG_SYNC_PAGE_SIZE_MAX,
    MAX_CATALOG_SYNC_REVISION,
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    CatalogSyncDelete,
    CatalogSyncDescriptor,
    CatalogSyncUpsert,
)
from riverhog_protocol.errors import RiverhogError

_AUTHORITY = {"source_identity": "a" * 64, "authorization_view_identity": "b" * 64}


def _descriptor(collection_id: int = 1, revision: str = "1") -> CatalogSyncDescriptor:
    return CatalogSyncDescriptor(
        collection_id=collection_id,
        revision=revision,
        archive_root_sha256="c" * 64,
        content_identity="d" * 64,
        description=None,
        description_revision=0,
        description_identity="e" * 64,
        tag_revision=1,
        tag_set_identity="f" * 64,
    )


def _catalog(**overrides: Any) -> CatalogSyncCollectionPage:
    return CatalogSyncCollectionPage.model_validate(
        {**_AUTHORITY, "collections": [], "changes_cursor": "changes-1", **overrides}
    )


def _changes(**overrides: Any) -> CatalogSyncChangePage:
    return CatalogSyncChangePage.model_validate(
        {
            **_AUTHORITY,
            "changes": [],
            "next_cursor": "changes-2",
            "through_revision": "0",
            "caught_up": True,
            **overrides,
        }
    )


def _state(**overrides: Any) -> sync._SyncState:
    return sync._SyncState.model_validate(
        {**_AUTHORITY, "phase": "catalog", "cursor": "catalog-1", **overrides}
    )


def _roundtrip(state: sync._SyncState) -> sync._SyncState:
    assert sync._SyncState.model_validate(json.loads(state.model_dump_json())) == state
    restored = sync._SyncState.model_validate_json(state.model_dump_json())
    assert restored == state
    return restored


class _Api:
    """No tag API, database, retries, or hidden cursor advancement."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None, int | None]] = []
        self.checkpoint = CatalogSyncCheckpoint.model_validate(
            {**_AUTHORITY, "catalog_cursor": "catalog-1"}
        )
        self.catalog = _catalog()
        self.changes = _changes()
        self.error: Exception | None = None

    def _record(self, operation: str, cursor: str | None, limit: int | None) -> None:
        self.calls.append((operation, cursor, limit))
        if self.error is not None:
            raise self.error

    def create_catalog_sync_checkpoint(self) -> CatalogSyncCheckpoint:
        self._record("checkpoint", None, None)
        return self.checkpoint

    def list_catalog_sync_collections(
        self, cursor: str, *, limit: int = 100
    ) -> CatalogSyncCollectionPage:
        self._record("catalog", cursor, limit)
        return self.catalog

    def list_catalog_sync_changes(
        self, cursor: str, *, limit: int = 100
    ) -> CatalogSyncChangePage:
        self._record("changes", cursor, limit)
        return self.changes


def test_checkpoint_bootstrap_catchup_following_and_restart_are_separate_steps() -> None:
    api = _Api()
    new = sync._SyncState()
    checkpoint = sync._step(api, _roundtrip(new), limit=2)
    assert checkpoint.kind == "checkpoint"
    assert checkpoint.before == new and checkpoint.observations == ()
    assert api.calls == [("checkpoint", None, None)]
    assert new == sync._SyncState()

    api.catalog = _catalog(
        collections=[_descriptor(1, "90"), _descriptor(2, "3")],
        next_cursor="catalog-2",
        changes_cursor=None,
    )
    first = sync._step(api, _roundtrip(checkpoint.next_state), limit=2)
    assert first.kind == "catalog"
    assert first.observations == tuple(api.catalog.collections)
    assert first.next_state.last_collection_id == 2
    assert first.next_state.through_revision == "0"

    api.catalog = _catalog(collections=[_descriptor(4, "20")])
    final = sync._step(api, _roundtrip(first.next_state), limit=2)
    assert final.next_state.phase == "catchup"
    assert final.next_state.last_collection_id is None
    assert final.next_state.through_revision == "0"

    # Bootstrap can have seen revision 90 for this collection already. The
    # traversal must still deliver change 5; reconciliation is the caller's job.
    old = CatalogSyncUpsert(**_descriptor(1, "5").model_dump())
    api.changes = _changes(changes=[old], through_revision="7", caught_up=False)
    catchup = sync._step(api, _roundtrip(final.next_state), limit=2)
    assert catchup.observations == (old,)
    assert catchup.next_state.phase == "catchup"

    api.changes = _changes(next_cursor="changes-3", through_revision="90")
    caught_up = sync._step(api, _roundtrip(catchup.next_state), limit=2)
    assert caught_up.next_state.phase == "following"
    assert caught_up.next_state.through_revision == "90"

    deleted = CatalogSyncDelete(collection_id=1, revision="92")
    api.changes = _changes(
        changes=[deleted], next_cursor="changes-4", through_revision="92", caught_up=False
    )
    following = sync._step(api, _roundtrip(caught_up.next_state), limit=2)
    assert following.observations == (deleted,)
    assert following.next_state.phase == "following"
    assert api.calls == [
        ("checkpoint", None, None),
        ("catalog", "catalog-1", 2),
        ("catalog", "catalog-2", 2),
        ("changes", "changes-1", 2),
        ("changes", "changes-2", 2),
        ("changes", "changes-3", 2),
    ]


@pytest.mark.parametrize("last_id", [None, 12])
def test_empty_final_bootstrap_is_valid(last_id: int | None) -> None:
    result = sync._step(_Api(), _state(last_collection_id=last_id), limit=1)
    assert result.observations == ()
    assert result.next_state.phase == "catchup"


@pytest.mark.parametrize("ids,last", [([2, 1], None), ([1, 1], None), ([2], 2), ([1], 2)])
def test_bootstrap_order_is_checked_within_and_across_pages(
    ids: list[int], last: int | None
) -> None:
    api = _Api()
    api.catalog = _catalog(collections=[_descriptor(value) for value in ids])
    before = _roundtrip(_state(last_collection_id=last))
    with pytest.raises(ValueError, match="collection order"):
        sync._step(api, before, limit=2)
    assert before.last_collection_id == last
    assert len(api.calls) == 1


@pytest.mark.parametrize("continuation", ["next_cursor", "changes_cursor"])
def test_bootstrap_rejects_reused_cursor(continuation: str) -> None:
    api = _Api()
    if continuation == "next_cursor":
        api.catalog = _catalog(
            collections=[_descriptor()], next_cursor="catalog-1", changes_cursor=None
        )
    else:
        api.catalog = _catalog(changes_cursor="catalog-1")
    with pytest.raises(ValueError, match="cursor did not advance"):
        sync._step(api, _state())


@pytest.mark.parametrize(
    "changes,through,caught_up,cursor,message",
    [
        ([], "9", True, "next", "moved backward"),
        (["10"], "10", True, "next", "revisions did not advance"),
        (["11", "11"], "11", True, "next", "revisions did not advance"),
        (["12", "11"], "12", True, "next", "revisions did not advance"),
        (["12"], "11", True, "next", "precedes its changes"),
        ([], "10", False, "next", "page did not advance"),
        (["11"], "11", True, "changes-1", "cursor did not advance"),
        ([], "11", True, "changes-1", "cursor did not advance"),
    ],
)
def test_change_progress_validation(
    changes: list[str], through: str, caught_up: bool, cursor: str, message: str
) -> None:
    api = _Api()
    api.changes = _changes(
        changes=[CatalogSyncDelete(collection_id=1, revision=value) for value in changes],
        through_revision=through,
        caught_up=caught_up,
        next_cursor=cursor,
    )
    before = _state(phase="following", cursor="changes-1", through_revision="10")
    with pytest.raises(ValueError, match=message):
        sync._step(api, before)
    assert _roundtrip(before).through_revision == "10"
    assert len(api.calls) == 1


@pytest.mark.parametrize("caught_up", [False, True])
def test_invisible_revision_gaps_can_advance_an_empty_page(caught_up: bool) -> None:
    api = _Api()
    api.changes = _changes(through_revision="50", caught_up=caught_up)
    result = sync._step(api, _state(phase="catchup", cursor="changes-1"), limit=1)
    assert result.observations == () and result.next_state.through_revision == "50"
    assert result.next_state.phase == ("following" if caught_up else "catchup")


@pytest.mark.parametrize("cursor", ["changes-1", "renewed-token"])
def test_idle_caught_up_page_may_reuse_or_renew_its_cursor(cursor: str) -> None:
    api = _Api()
    api.changes = _changes(next_cursor=cursor, through_revision="10")
    before = _state(phase="following", cursor="changes-1", through_revision="10")
    result = sync._step(api, before)
    assert result.observations == () and result.next_state.cursor == cursor
    assert result.next_state.through_revision == "10"


def test_revision_upper_bound_is_lossless_and_visible_gaps_are_legal() -> None:
    api = _Api()
    maximum = str(MAX_CATALOG_SYNC_REVISION)
    api.changes = _changes(
        changes=[CatalogSyncDelete(collection_id=1, revision=maximum)],
        through_revision=maximum,
    )
    result = sync._step(api, _state(phase="catchup", cursor="changes-1"), limit=1)
    assert _roundtrip(result.next_state).through_revision == maximum


@pytest.mark.parametrize("phase", ["catalog", "catchup", "following"])
@pytest.mark.parametrize("field", ["source_identity", "authorization_view_identity"])
def test_changed_response_authority_yields_no_observations(phase: str, field: str) -> None:
    api = _Api()
    api.catalog = _catalog(collections=[_descriptor()], **{field: "9" * 64})
    api.changes = _changes(
        changes=[CatalogSyncDelete(collection_id=1, revision="1")],
        through_revision="1",
        **{field: "9" * 64},
    )
    before = _state(phase=phase)
    result = sync._step(api, before)
    assert result.kind == "reset" and result.observations == ()
    assert result.before == before
    assert result.next_state.source_identity == before.source_identity
    assert result.next_state.authorization_view_identity == before.authorization_view_identity
    expected = {
        "source_identity": "catalog_sync_source_changed",
        "authorization_view_identity": "catalog_sync_view_changed",
    }[field]
    assert result.next_state.reset_reason == expected
    reset = _roundtrip(result.next_state)
    assert reset.cursor is None
    with pytest.raises(RuntimeError, match="explicit start"):
        sync._step(api, reset)
    assert len(api.calls) == 1
    api.checkpoint = api.checkpoint.model_copy(update={field: "9" * 64})
    restarted = sync._start(api, reset)
    assert restarted.next_state.phase == "catalog"
    assert getattr(restarted.next_state, field) == "9" * 64
    assert restarted.next_state.through_revision == "0"
    assert restarted.next_state.reset_reason is None


@pytest.mark.parametrize("phase", ["new", "catalog", "catchup", "following"])
@pytest.mark.parametrize(
    "code",
    [
        "catalog_sync_cursor_expired",
        "catalog_sync_history_expired",
        "catalog_sync_source_changed",
        "catalog_sync_view_changed",
        "unauthorized",
        "forbidden",
    ],
)
def test_reset_errors_are_explicit_serializable_results(phase: str, code: str) -> None:
    api = _Api()
    api.error = RiverhogError("remote reset", code=code)
    state = sync._SyncState() if phase == "new" else _state(phase=phase)
    result = sync._step(api, state)
    assert result.kind == "reset" and result.observations == ()
    assert result.before == state
    assert _roundtrip(result.next_state).reset_reason == code
    assert len(api.calls) == 1
    with pytest.raises(RuntimeError, match="explicit start"):
        sync._step(api, result.next_state)
    assert len(api.calls) == 1


@pytest.mark.parametrize("phase", ["new", "catalog", "following"])
@pytest.mark.parametrize("code", ["service_unavailable", "bad_request", "precondition_failed"])
def test_non_sync_errors_are_not_silently_classified_as_resets(phase: str, code: str) -> None:
    api = _Api()
    error = RiverhogError("not a synchronization reset", code=code)
    api.error = error
    state = sync._SyncState() if phase == "new" else _state(phase=phase)
    with pytest.raises(RiverhogError) as caught:
        sync._step(api, state)
    assert caught.value is error
    assert len(api.calls) == 1
    assert state.phase == phase


@pytest.mark.parametrize("phase", ["new", "catalog", "following"])
def test_transport_failure_is_retryable_without_hidden_progress(phase: str) -> None:
    api = _Api()
    api.error = OSError("lost response")
    state = sync._SyncState() if phase == "new" else _state(phase=phase)
    with pytest.raises(OSError, match="lost response"):
        sync._step(api, state)
    api.error = None
    retry = sync._step(api, _roundtrip(state))
    assert retry.before == state and api.calls[0] == api.calls[1]


@pytest.mark.parametrize("limit", [True, False, 0, -1, 1.0, "1", None, 101])
def test_invalid_limits_fail_before_io(limit: object) -> None:
    api = _Api()
    with pytest.raises(ValueError, match="page size"):
        sync._step(api, sync._SyncState(), limit=cast(int, limit))
    assert api.calls == []


@pytest.mark.parametrize("phase", ["catalog", "following"])
def test_response_cannot_exceed_the_requested_page_bound(phase: str) -> None:
    api = _Api()
    api.catalog = _catalog(collections=[_descriptor(1), _descriptor(2)])
    api.changes = _changes(
        changes=[CatalogSyncDelete(collection_id=1, revision=str(value)) for value in (1, 2)],
        through_revision="2",
    )
    with pytest.raises(ValueError, match="requested bound"):
        sync._step(api, _state(phase=phase), limit=1)
    assert len(api.calls) == 1


def test_maximum_page_and_opaque_cursor_are_supported() -> None:
    api = _Api()
    cursor = "x" * CATALOG_SYNC_CURSOR_BYTES_MAX
    api.catalog = _catalog(
        collections=[_descriptor(value) for value in range(1, CATALOG_SYNC_PAGE_SIZE_MAX + 1)],
        next_cursor=cursor,
        changes_cursor=None,
    )
    result = sync._step(api, _state(), limit=CATALOG_SYNC_PAGE_SIZE_MAX)
    assert len(result.observations) == CATALOG_SYNC_PAGE_SIZE_MAX
    assert _roundtrip(result.next_state).cursor == cursor


@pytest.mark.parametrize("phase", ["new", "catalog", "following"])
def test_cursor_byte_bound_applies_to_every_response_kind(phase: str) -> None:
    api = _Api()
    # The wire model bounds characters; the engine also respects the byte bound.
    cursor = "é" * CATALOG_SYNC_CURSOR_BYTES_MAX
    api.checkpoint = api.checkpoint.model_copy(update={"catalog_cursor": cursor})
    api.catalog = _catalog(changes_cursor=cursor)
    api.changes = _changes(next_cursor=cursor)
    state = sync._SyncState() if phase == "new" else _state(phase=phase)
    with pytest.raises(ValueError, match="byte bound"):
        sync._step(api, state)


@pytest.mark.parametrize(
    "overrides",
    [
        {"phase": "unknown"},
        {"phase": "new"},
        {"source_identity": None},
        {"authorization_view_identity": "bad"},
        {"cursor": None},
        {"cursor": ""},
        {"cursor": "x" * (CATALOG_SYNC_CURSOR_BYTES_MAX + 1)},
        {"cursor": "é" * CATALOG_SYNC_CURSOR_BYTES_MAX},
        {"through_revision": 1},
        {"through_revision": True},
        {"through_revision": "01"},
        {"through_revision": "-1"},
        {"through_revision": str(MAX_CATALOG_SYNC_REVISION + 1)},
        {"through_revision": "1"},
        {"last_collection_id": 0},
        {"last_collection_id": True},
        {"last_collection_id": "1"},
        {"phase": "following", "last_collection_id": 1},
        {"reset_reason": "forbidden"},
        {"phase": "reset_required", "cursor": None},
        {"phase": "reset_required", "reset_reason": "forbidden"},
        {"extra": "not part of state"},
    ],
)
def test_persisted_state_fails_closed(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _state(**overrides)


@pytest.mark.parametrize("phase", ["new", "catalog", "following"])
def test_wrong_document_type_is_rejected(phase: str) -> None:
    api = _Api()
    api.checkpoint = cast(CatalogSyncCheckpoint, api.changes)
    api.catalog = cast(CatalogSyncCollectionPage, api.changes)
    api.changes = cast(CatalogSyncChangePage, _catalog())
    before = sync._SyncState() if phase == "new" else _state(phase=phase)
    with pytest.raises(ValueError, match="wrong document type"):
        sync._step(api, before)


@pytest.mark.parametrize(
    "overrides",
    [
        {"changes_cursor": None},
        {"next_cursor": "next"},
        {"next_cursor": "next", "changes_cursor": None},
    ],
)
def test_mutated_wire_continuations_are_revalidated(overrides: dict[str, object]) -> None:
    api = _Api()
    api.catalog = api.catalog.model_copy(update=overrides)
    with pytest.raises(ValueError, match="continuation|contain a collection"):
        sync._step(api, _state())


def test_mutated_wire_revision_is_revalidated() -> None:
    api = _Api()
    api.changes = api.changes.model_copy(update={"through_revision": "01"})
    with pytest.raises(ValidationError):
        sync._step(api, _state(phase="following"))


def test_batch_is_detached_and_replay_does_not_mutate_input() -> None:
    api = _Api()
    api.catalog = _catalog(collections=[_descriptor()])
    before = _state()
    batch = sync._step(api, before)
    assert sync._step(api, _roundtrip(before)) == batch
    api.catalog.collections.clear()
    assert batch.observations == (_descriptor(),)
    with pytest.raises(FrozenInstanceError):
        batch.kind = "reset"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        batch.next_state.phase = "new"
    assert before == _state()


def _accept(
    db: sqlite3.Connection,
    batch: sync._SyncBatch,
    *,
    version: int,
    fail_at: Literal["observations", "state"] | None = None,
) -> None:
    """Test application's transaction/CAS, deliberately outside the engine."""

    with db:
        row = db.execute("SELECT version, state FROM progress").fetchone()
        assert row is not None
        if row[0] != version or sync._SyncState.model_validate_json(row[1]) != batch.before:
            raise RuntimeError("stale application worker")
        for item in batch.observations:
            db.execute(
                "INSERT INTO observations VALUES (?, ?)", (item.collection_id, item.revision)
            )
        if fail_at == "observations":
            raise RuntimeError("application rollback")
        updated = db.execute(
            "UPDATE progress SET version = version + 1, state = ? WHERE version = ?",
            (batch.next_state.model_dump_json(), version),
        )
        if updated.rowcount != 1:
            raise RuntimeError("stale application worker")
        if fail_at == "state":
            raise RuntimeError("application rollback")


@pytest.mark.parametrize("fail_at", ["observations", "state"])
def test_application_transaction_rollback_replay_and_competing_workers(
    fail_at: Literal["observations", "state"],
) -> None:
    api = _Api()
    api.catalog = _catalog(collections=[_descriptor()])
    state = _state()
    db = sqlite3.connect(":memory:")
    try:
        db.executescript(
            "CREATE TABLE progress (version INTEGER, state TEXT);"
            "CREATE TABLE observations (collection_id INTEGER, revision TEXT);"
        )
        with db:
            db.execute("INSERT INTO progress VALUES (0, ?)", (state.model_dump_json(),))
        first = sync._step(api, state)
        competitor = sync._step(api, _roundtrip(state))
        with pytest.raises(RuntimeError, match="rollback"):
            _accept(db, first, version=0, fail_at=fail_at)
        assert db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        assert db.execute("SELECT version FROM progress").fetchone()[0] == 0

        # A retry may see a newer bootstrap descriptor. There is no snapshot or
        # exactly-once side-effect promise, only caller-controlled acceptance.
        api.catalog = _catalog(collections=[_descriptor(revision="2")])
        persisted = sync._SyncState.model_validate_json(
            db.execute("SELECT state FROM progress").fetchone()[0]
        )
        retry = sync._step(api, persisted)
        assert retry.observations != first.observations
        _accept(db, retry, version=0)
        with pytest.raises(RuntimeError, match="stale application worker"):
            _accept(db, competitor, version=0)
        assert db.execute("SELECT * FROM observations").fetchall() == [(1, "2")]
        stored = db.execute("SELECT state FROM progress").fetchone()[0]
        assert stored == retry.next_state.model_dump_json()
    finally:
        db.close()


def test_explicit_refresh_discards_only_traversal_progress() -> None:
    api = _Api()
    before = _state(phase="following", through_revision="100")
    api.checkpoint = api.checkpoint.model_copy(update={"source_identity": "9" * 64})
    result = sync._start(api, before)
    assert result.before == before and result.observations == ()
    assert result.next_state.source_identity == "9" * 64
    assert result.next_state.through_revision == "0"
    assert result.next_state.last_collection_id is None
    assert api.calls == [("checkpoint", None, None)]


def test_bypassed_state_validation_is_caught_before_io() -> None:
    api = _Api()
    invalid = _state().model_copy(update={"cursor": None})
    with pytest.raises(ValidationError):
        sync._step(api, invalid)
    with pytest.raises(ValidationError):
        sync._start(api, invalid)
    assert api.calls == []


def test_change_retry_can_observe_a_later_unfixed_horizon() -> None:
    api = _Api()
    before = _state(phase="following", cursor="changes-1", through_revision="10")
    api.changes = _changes(through_revision="10")
    first = sync._step(api, before)
    api.changes = _changes(
        changes=[CatalogSyncDelete(collection_id=1, revision="11")], through_revision="11"
    )
    retry = sync._step(api, _roundtrip(before))
    assert first.before == retry.before
    assert first.observations == () and len(retry.observations) == 1
    assert retry.next_state.through_revision == "11"
    assert api.calls[0] == api.calls[1]

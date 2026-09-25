from __future__ import annotations

from riverhog_client import CatalogFollower, CatalogFollowPosition
from riverhog_protocol import (
    CatalogSyncChangePage,
    CatalogSyncCheckpoint,
    CatalogSyncCollectionPage,
    CatalogSyncDeparture,
    CatalogSyncDescriptor,
)
from riverhog_protocol.errors import CatalogSyncViewChanged


class _Api:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.view_changed = False

    def create_catalog_sync_checkpoint(self) -> CatalogSyncCheckpoint:
        self.calls.append("checkpoint")
        return CatalogSyncCheckpoint(
            source_identity="a" * 64,
            authorization_view_identity="b" * 64,
            catalog_cursor="baseline",
        )

    def list_catalog_sync_collections(
        self, cursor: str, *, limit: int = 100
    ) -> CatalogSyncCollectionPage:
        self.calls.append(f"catalog:{cursor}:{limit}")
        return CatalogSyncCollectionPage(
            source_identity="a" * 64,
            authorization_view_identity="b" * 64,
            collections=[
                CatalogSyncDescriptor(
                    collection_id="42",
                    archive_root_sha256="c" * 64,
                    content_identity="d" * 64,
                    description=None,
                    description_revision=0,
                    description_identity="e" * 64,
                    tag_revision=1,
                    tag_set_identity="f" * 64,
                    revision="3",
                )
            ],
            changes_cursor="following",
        )

    def list_catalog_sync_changes(self, cursor: str, *, limit: int = 100) -> CatalogSyncChangePage:
        self.calls.append(f"changes:{cursor}:{limit}")
        if self.view_changed:
            raise CatalogSyncViewChanged("the authorization view changed")
        return CatalogSyncChangePage(
            source_identity="a" * 64,
            authorization_view_identity="b" * 64,
            changes=[
                CatalogSyncDeparture(cause="visibility_lost", collection_id="42", revision="4")
            ],
            next_cursor="after-departure",
            caught_up=True,
            through_revision="4",
        )


def test_follower_returns_distinct_bootstrap_and_change_proposals() -> None:
    api = _Api()
    follower = CatalogFollower(api)
    new = CatalogFollowPosition()
    checkpoint = follower.step(new)
    assert checkpoint.kind == "checkpoint"
    assert checkpoint.before == new
    assert new.phase == "new"

    catalog = follower.step(checkpoint.after, limit=1)
    assert catalog.kind == "catalog"
    assert catalog.after.phase == "catchup"
    assert [item.collection_id for item in catalog.collections] == [42]
    assert catalog.changes == ()

    changes = follower.step(catalog.after, limit=1)
    assert changes.kind == "changes"
    assert changes.collections == ()
    assert changes.changes[0].cause == "visibility_lost"
    assert changes.after.phase == "following"
    assert api.calls == ["checkpoint", "catalog:baseline:1", "changes:following:1"]

    api.view_changed = True
    reset = follower.step(changes.after, limit=1)
    assert reset.kind == "reset"
    assert reset.collections == reset.changes == ()
    assert reset.after.phase == "reset_required"
    assert reset.after.reset_reason == "catalog_sync_view_changed"
    assert follower.start(reset.after).after.phase == "catalog"

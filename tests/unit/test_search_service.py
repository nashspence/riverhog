from __future__ import annotations

from pathlib import Path

from riverhog_core.app_permissions import CATALOG_READ, ApplicationAccess, Principal
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArtifactRecord,
    CollectionRecord,
    CollectionTagMembershipRecord,
    CollectionTagRecord,
)
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.search import SqlAlchemySearchService
from riverhog_protocol import collection_tag_sha256

from tests.unit.artifact_scope_fixtures import persisted_artifact_scope
from tests.unit.db_helpers import sqlite_url

TAG = "docs"
TAG_SHA256 = collection_tag_sha256(TAG)


def _seed(path: Path) -> None:
    database_url = sqlite_url(path)
    initialize_db(database_url)
    with session_scope(make_session_factory(database_url)) as session:
        session.add(
            CollectionTagRecord(
                tag_sha256=TAG_SHA256,
                tag=TAG,
                search_text=TAG,
                created_at="2026-01-01T00:00:00Z",
                updated_at="2026-01-01T00:00:00Z",
                collection_count=1,
            )
        )
        session.add(
            CollectionRecord(
                id=1,
                creation_idempotency_key="search-test",
                creation_identity_sha256="1" * 64,
                creation_custody_mode="custody-transfer",
                delivery_context_id="urn:uuid:cabfc827-91d5-4cde-8f0f-81255b75b1c4",
                artifact_set_identity="2" * 64,
                encryption_format="none",
                passphrase_id="test",
                provenance_identity="3" * 64,
                inventory_identity="4" * 64,
                archive_root_sha256="5" * 64,
                description="Receipts for tax filing",
                description_search="receipts for tax filing",
                description_revision=1,
                description_identity="6" * 64,
                created_at="2026-01-01T00:00:00Z",
                artifact_count=3,
                artifact_bytes=68,
            )
        )
        session.add(
            CollectionTagMembershipRecord(
                collection_id=1,
                tag_sha256=TAG_SHA256,
                added_at="2026-01-01T00:00:00Z",
            )
        )
        session.add_all(
            CollectionArtifactRecord(
                collection_id=1,
                artifact_id=member_id * 64,
                bytes=byte_count,
                sha256=digest * 64,
            )
            for member_id, byte_count, digest in (("a", 13, "d"), ("b", 34, "e"), ("c", 21, "f"))
        )


def _service(path: Path) -> SqlAlchemySearchService:
    return SqlAlchemySearchService(RuntimeConfig.for_testing(database_url=sqlite_url(path)))


def test_search_pages_opaque_members_with_stable_identity(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite3"
    _seed(path)
    first = _service(path).search(
        q="docs", page_size=2, position=None, sort="artifact_id", order="asc"
    )
    assert [item["artifact_id"] for item in first["artifacts"]] == ["a" * 64, "b" * 64]
    assert first["_next_position"] is not None
    second = _service(path).search(
        q="docs",
        page_size=2,
        position=first["_next_position"],
        sort="artifact_id",
        order="asc",
    )
    assert second["_next_position"] is None
    assert second["artifacts"] == [
        {
            "artifact_ref": "1/" + "c" * 64,
            "collection_id": "1",
            "artifact_id": "c" * 64,
            "bytes": "21",
            "sha256": "f" * 64,
        }
    ]


def test_search_current_description_and_exact_member_digest(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite3"
    _seed(path)
    assert len(
        _service(path).search(
            q="tax", page_size=25, position=None, sort="artifact_ref", order="asc"
        )["artifacts"]
    ) == 3
    result = _service(path).search(
        q="f" * 16, page_size=25, position=None, sort="artifact_ref", order="asc"
    )
    assert [item["artifact_id"] for item in result["artifacts"]] == ["c" * 64]


def test_search_applies_collection_and_exact_artifact_scope(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite3"
    _seed(path)
    denied = Principal(
        id="reader",
        key_id="reader-key",
        access=frozenset({ApplicationAccess(CATALOG_READ, "tag:other")}),
    )
    assert _service(path).search(
        q=None,
        page_size=25,
        position=None,
        sort="artifact_ref",
        order="asc",
        principal=denied,
    )["artifacts"] == []
    scoped = persisted_artifact_scope(
        sqlite_url(path),
        access=(ApplicationAccess(CATALOG_READ, "collection:1"),),
        artifacts=((1, "b" * 64, 34, "e" * 64),),
    )
    result = _service(path).search(
        q=None,
        page_size=25,
        position=None,
        sort="artifact_ref",
        order="asc",
        principal=scoped,
    )
    assert [item["artifact_ref"] for item in result["artifacts"]] == ["1/" + "b" * 64]


def test_search_streams_all_authorized_members_without_names(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite3"
    _seed(path)
    rows = list(_service(path).iter_artifacts(q=None, sort="bytes", order="asc"))
    assert [row["bytes"] for row in rows] == ["13", "21", "34"]
    assert all(
        set(row) == {"artifact_ref", "collection_id", "artifact_id", "bytes", "sha256"}
        for row in rows
    )

from __future__ import annotations

from pathlib import Path

import pytest
from riverhog_core.app_permissions import (
    CATALOG_READ,
    ApplicationAccess,
    Principal,
    collection_resource,
)
from riverhog_core.catalog_db import (
    create_catalog_engine,
    initialize_db,
    session_scope,
)
from riverhog_core.catalog_models import (
    CollectionArchiveCopyRecord,
    CollectionArchiveObjectRecord,
    CollectionRecord,
)
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.collections import SqlAlchemyCollectionService
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from tests.unit.db_helpers import sqlite_url
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

NOW = "2026-01-01T00:00:00.000000000Z"


def _seed_collections(database: Path, *, count: int) -> tuple[RuntimeConfig, Engine]:
    database_url = sqlite_url(database)
    initialize_db(database_url)
    engine = create_catalog_engine(database_url)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_scope(factory) as session:
        for collection_id in range(1, count + 1):
            session.add(
                CollectionRecord(
                    id=collection_id,
                    creation_idempotency_key=f"fixture-{collection_id}",
                    creation_identity_sha256=f"{collection_id:064x}",
                    creation_custody_mode="producer-retained",
                    artifact_set_identity=f"{collection_id:064x}",
                    delivery_context_id=f"urn:uuid:00000000-0000-4000-8000-{collection_id:012x}",
                    encryption_format="age-v1-scrypt",
                    passphrase_id=f"fixture-archive-key-v{1 if collection_id % 2 else 2}",
                    provenance_identity=f"{collection_id:064x}",
                    inventory_identity=f"{collection_id:064x}",
                    created_by_principal_id="fixture",
                    created_at=NOW,
                )
            )
            session.add(
                CollectionArchiveCopyRecord(
                    collection_id=collection_id,
                    store="archive",
                    incarnation_id=seed_storage_incarnation(session, "archive", "archive"),
                    state="uploaded",
                    archive_storage_prefix=f"archives/{collection_id}",
                    last_uploaded_at=NOW,
                    last_verified_at=NOW,
                )
            )
            for order, object_id in enumerate(
                ("manifest", *(f"segment-{index}" for index in range(8)))
            ):
                session.add(
                    CollectionArchiveObjectRecord(
                        collection_id=collection_id,
                        store="archive",
                        object_id=object_id,
                        object_order=order,
                        kind=object_id if object_id == "manifest" else "segment",
                        object_path=f"archives/{collection_id}/{object_id}",
                        plaintext_bytes=1,
                        stored_bytes=1,
                        sha256="a" * 64,
                        stored_sha256="b" * 64,
                        revision=f"version-{collection_id}-{object_id}",
                        uploaded_at=NOW,
                        verified_at=NOW,
                    )
                )
    return RuntimeConfig.for_testing(database_url=database_url), engine


def test_collection_list_query_count_is_independent_of_page_rows(tmp_path: Path) -> None:
    config, engine = _seed_collections(tmp_path / "catalog.sqlite3", count=12)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    service = SqlAlchemyCollectionService(config, session_factory=factory)
    select_count = 0

    @event.listens_for(engine, "before_cursor_execute")
    def count_selects(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        nonlocal select_count
        if statement.lstrip().upper().startswith("SELECT"):
            select_count += 1

    service.list(page_size=1, position=None, q=None)
    one_row_selects = select_count
    select_count = 0

    loaded_object_ids: list[str] = []

    def record_loaded_object(target: CollectionArchiveObjectRecord, _context: object) -> None:
        loaded_object_ids.append(target.object_id)

    event.listen(CollectionArchiveObjectRecord, "load", record_loaded_object)
    try:
        page = service.list(page_size=12, position=None, q=None)
    finally:
        event.remove(CollectionArchiveObjectRecord, "load", record_loaded_object)
    twelve_row_selects = select_count

    assert len(page.collections) == 12
    assert loaded_object_ids == []
    assert all(current.archive_copy_count == 1 for current in page.collections)
    assert twelve_row_selects == one_row_selects
    engine.dispose()


def test_collection_encryption_filters_preserve_catalog_authorization(tmp_path: Path) -> None:
    config, engine = _seed_collections(tmp_path / "catalog.sqlite3", count=4)
    service = SqlAlchemyCollectionService(config)
    principal = Principal(
        id="scoped-reader",
        key_id="key-1",
        access=frozenset({ApplicationAccess(CATALOG_READ, collection_resource(2))}),
    )

    visible = service.list(
        page_size=25,
        position=None,
        q=None,
        encryption_format="age-v1-scrypt",
        passphrase_id="fixture-archive-key-v2",
        principal=principal,
    )
    hidden = service.list(
        page_size=25,
        position=None,
        q=None,
        passphrase_id="fixture-archive-key-v1",
        principal=principal,
    )

    assert [item.id for item in visible.collections] == [2]
    assert visible.encryption_format == "age-v1-scrypt"
    assert visible.passphrase_id == "fixture-archive-key-v2"
    assert visible.collections[0].encryption_format == "age-v1-scrypt"
    assert visible.collections[0].passphrase_id == "fixture-archive-key-v2"
    assert hidden.collections == []
    assert hidden.next_position is None
    engine.dispose()


@pytest.mark.parametrize("sort", ("files", "bytes"))
def test_collection_count_and_byte_sorts_page_over_current_artifact_totals(
    tmp_path: Path, sort: str
) -> None:
    config, engine = _seed_collections(tmp_path / "catalog.sqlite3", count=3)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_scope(factory) as session:
        for collection_id in range(1, 4):
            collection = session.get(CollectionRecord, collection_id)
            assert collection is not None
            collection.artifact_count = collection_id
            collection.artifact_bytes = collection_id * 10

    service = SqlAlchemyCollectionService(config, session_factory=factory)
    first = service.list(page_size=2, position=None, q=None, sort=sort, order="desc")
    assert [item.id for item in first.collections] == [3, 2]
    assert first.next_position is not None
    second = service.list(
        page_size=2,
        position=first.next_position,
        q=None,
        sort=sort,
        order="desc",
    )
    assert [item.id for item in second.collections] == [1]
    assert second.next_position is None
    engine.dispose()

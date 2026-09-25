from __future__ import annotations

import pytest
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    RetrievalCacheStoreAccountingRecord,
    StorageIncarnationRecord,
)
from riverhog_core.storage_incarnations import (
    StorageBindingConflict,
    reconcile_storage_incarnations,
    require_storage_incarnation,
)
from riverhog_protocol.errors import ServiceUnavailable
from sqlalchemy.exc import IntegrityError

from tests.unit.db_helpers import sqlite_url

_FIRST = "00000000-0000-4000-8000-000000000001"
_SECOND = "00000000-0000-4000-8000-000000000002"


def test_historical_name_is_reserved_across_removal_and_rebinding(tmp_path) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    factory = make_session_factory(database_url)

    assert reconcile_storage_incarnations(factory, {("archive", "primary"): _FIRST}) == {
        ("archive", "primary"): _FIRST
    }
    reconcile_storage_incarnations(factory, {})
    with session_scope(factory) as session:
        row = session.get(StorageIncarnationRecord, ("archive", "primary"))
        assert row is not None and row.state == "disabled"
        with pytest.raises(ServiceUnavailable, match="unavailable"):
            require_storage_incarnation(session, "archive", "primary")
    with pytest.raises(StorageBindingConflict, match="reserved"):
        reconcile_storage_incarnations(factory, {("archive", "primary"): _SECOND})
    assert (
        reconcile_storage_incarnations(factory, {("archive", "primary"): _FIRST})[
            ("archive", "primary")
        ]
        == _FIRST
    )
    with session_scope(factory) as session:
        row = session.get(StorageIncarnationRecord, ("archive", "primary"))
        assert row is not None and row.state == "bound" and row.binding_generation == 3


def test_one_physical_incarnation_may_serve_archive_and_cache_roles(tmp_path) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    factory = make_session_factory(database_url)

    bindings = reconcile_storage_incarnations(
        factory,
        {("archive", "archive"): _FIRST, ("cache", "local"): _FIRST},
    )

    assert bindings == {("archive", "archive"): _FIRST, ("cache", "local"): _FIRST}
    with session_scope(factory) as session:
        assert require_storage_incarnation(session, "archive", "archive") == _FIRST
        assert require_storage_incarnation(session, "cache", "local") == _FIRST


def test_unreachable_binding_does_not_disable_other_storage(tmp_path) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    factory = make_session_factory(database_url)
    reconcile_storage_incarnations(
        factory,
        {("archive", "primary"): _FIRST, ("archive", "second"): _SECOND},
    )
    reservations = reconcile_storage_incarnations(
        factory,
        {("archive", "primary"): None, ("archive", "second"): _SECOND},
    )
    assert reservations == {
        ("archive", "primary"): _FIRST,
        ("archive", "second"): _SECOND,
    }
    with session_scope(factory) as session:
        assert require_storage_incarnation(session, "archive", "second") == _SECOND


def test_catalog_rejects_name_and_incarnation_disagreement(tmp_path) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    factory = make_session_factory(database_url)
    reconcile_storage_incarnations(factory, {("cache", "first"): _FIRST})
    with pytest.raises(IntegrityError), session_scope(factory) as session:
        session.add(
            RetrievalCacheStoreAccountingRecord(
                cache_store="other",
                cache_incarnation_id=_FIRST,
                reserved_bytes=0,
                committed_bytes=0,
                updated_at="2026-01-01T00:00:00.000000000Z",
            )
        )

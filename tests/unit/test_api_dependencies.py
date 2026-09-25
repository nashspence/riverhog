from __future__ import annotations

from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from riverhog_api import deps
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import CollectionArchiveCopyRecord, CollectionRecord
from riverhog_core.runtime_config import (
    TEST_ARCHIVE_PASSPHRASE_ID,
    RuntimeConfig,
    StorageAdapterRegistration,
)
from riverhog_core.storage_incarnations import reconcile_storage_incarnations
from riverhog_storage_adapter_protocol import AdapterDescriptor
from riverhog_storage_adapter_support import StorageAdapterClient

from tests.unit.db_helpers import sqlite_url
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation


def test_degraded_container_keeps_unreachable_historical_store_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    factory = make_session_factory(database_url)
    base = RuntimeConfig.for_testing(database_url=database_url)
    archive = base.archive_store("archive")
    offline = replace(archive, name="offline", base_url="http://127.0.0.2/offline")
    config = replace(
        base,
        archive_stores={"archive": archive, "offline": offline},
        archive_read_order=("archive", "offline"),
    )
    reconcile_storage_incarnations(
        factory,
        {
            ("archive", "archive"): "00000000-0000-4000-8000-000000000001",
            ("archive", "offline"): "00000000-0000-4000-8000-000000000002",
        },
    )
    available = {"archive": True, "offline": False}

    def respond(request: httpx.Request) -> httpx.Response:
        name = "offline" if request.url.host == "127.0.0.2" else "archive"
        if request.url.path.endswith("/health/ready"):
            return httpx.Response(200 if available[name] else 503)
        if request.url.path.endswith("/v1/adapter"):
            descriptor = AdapterDescriptor(
                storage_incarnation_id=(
                    "00000000-0000-4000-8000-000000000002"
                    if name == "offline"
                    else "00000000-0000-4000-8000-000000000001"
                ),
                implementation_id="fixture.storage/v1",
                implementation_version="1.0.0",
                read_mode="immediate",
                minimum_nonfinal_segment_bytes=1,
                maximum_segment_bytes=1024,
                maximum_segment_count=10000,
            )
            return httpx.Response(200, json=descriptor.model_dump())
        raise AssertionError(f"unexpected adapter request: {request.url.path}")

    transport_client = httpx.Client(transport=httpx.MockTransport(respond))
    monkeypatch.setattr(
        deps,
        "_adapter_client",
        lambda registration: StorageAdapterClient(
            registration.base_url,
            token="fixture",
            allow_insecure_http=True,
            client=transport_client,
        ),
    )
    try:
        with ExitStack() as cleanup:
            container = deps._build_default_container(
                config,
                session_factory=factory,
                startup_cleanup=cleanup,
            )
            assert container.storage_readiness is not None
            container.storage_readiness()
            historical = container.archive_stores.get("offline")
            assert historical.incarnation_id == "00000000-0000-4000-8000-000000000002"
            assert historical.configured and not historical.reachable
            available["archive"] = False
            with pytest.raises(Exception, match="archive store is unavailable"):
                container.storage_readiness()
    finally:
        transport_client.close()
        deps.dispose_session_factory(factory)


def test_default_container_closes_startup_resources_after_missing_required_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registration = StorageAdapterRegistration(
        name="archive",
        base_url="http://adapter.example.test",
        token_file=tmp_path / "adapter.token",
        allow_insecure_http=True,
    )
    config = RuntimeConfig.for_testing(
        database_url="sqlite+pysqlite:///:memory:",
        archive_stores={"archive": registration},
    )
    session_factory = object()
    closed: list[str] = []

    class RejectingAdapter:
        def check_readiness(self) -> None:
            return None

        def descriptor(self) -> Any:
            return SimpleNamespace(read_mode="restore_required")

        def refresh_descriptor(self) -> Any:
            return SimpleNamespace(
                read_mode="restore_required",
                storage_incarnation_id="00000000-0000-4000-8000-000000000001",
            )

        def close(self) -> None:
            closed.append("adapter")

    monkeypatch.setattr(deps, "load_runtime_config", lambda: config)
    monkeypatch.setattr(deps, "validate_db", lambda _url: None)
    monkeypatch.setattr(deps, "make_session_factory", lambda _url: session_factory)
    monkeypatch.setattr(deps, "dispose_session_factory", lambda _factory: closed.append("db"))
    monkeypatch.setattr(deps, "_adapter_client", lambda _registration: RejectingAdapter())
    monkeypatch.setattr(
        deps,
        "_require_archive_encryption_bindings",
        lambda *_args, **_kwargs: None,
    )
    deps.default_container.cache_clear()

    with pytest.raises(ValueError, match="require a retrieval cache adapter"):
        deps.default_container()

    assert closed == ["adapter", "db"]
    deps.default_container.cache_clear()


def test_startup_rejects_a_persisted_key_id_without_its_secret(tmp_path: Path) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    factory = make_session_factory(database_url)
    with session_scope(factory) as session:
        session.add(
            CollectionRecord(
                id=1,
                creation_idempotency_key="fixture",
                creation_identity_sha256="e" * 64,
                creation_custody_mode="producer-retained",
                content_identity="a" * 64,
                encryption_format="age-v1-scrypt",
                passphrase_id="removed-archive-key-v1",
                provenance_mode="omitted",
                provenance_identity=None,
                inventory_identity="b" * 64,
                created_by_principal_id="fixture",
                created_at="2026-08-24T00:00:00.000000000Z",
            )
        )

    with pytest.raises(ValueError, match="removed-archive-key-v1"):
        deps._require_archive_encryption_bindings(
            RuntimeConfig.for_testing(database_url=database_url),
            session_factory=factory,
        )


def test_startup_rejects_an_uploaded_copy_without_recovery_descriptor(tmp_path: Path) -> None:
    database_url = sqlite_url(tmp_path / "catalog.sqlite3")
    initialize_db(database_url)
    factory = make_session_factory(database_url)
    with session_scope(factory) as session:
        incarnation_id = seed_storage_incarnation(session, "archive", "archive")
        session.add(
            CollectionRecord(
                id=1,
                creation_idempotency_key="fixture",
                creation_identity_sha256="e" * 64,
                creation_custody_mode="producer-retained",
                content_identity="a" * 64,
                encryption_format="age-v1-scrypt",
                passphrase_id=TEST_ARCHIVE_PASSPHRASE_ID,
                provenance_mode="omitted",
                provenance_identity=None,
                inventory_identity="b" * 64,
                created_by_principal_id="fixture",
                created_at="2026-08-24T00:00:00.000000000Z",
            )
        )
        session.add(
            CollectionArchiveCopyRecord(
                collection_id=1,
                store="archive",
                incarnation_id=incarnation_id,
                state="uploaded",
                archive_storage_prefix="archives/fixture",
                last_uploaded_at="2026-08-24T00:00:00.000000000Z",
                last_verified_at="2026-08-24T00:00:00.000000000Z",
            )
        )

    with pytest.raises(ValueError, match="no recovery descriptor"):
        deps._require_archive_encryption_bindings(
            RuntimeConfig.for_testing(database_url=database_url),
            session_factory=factory,
        )

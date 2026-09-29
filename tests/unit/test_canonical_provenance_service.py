from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest
from riverhog_core.app_permissions import (
    CATALOG_READ,
    PROVENANCE_EXPORT,
    PROVENANCE_READ,
    ApplicationAccess,
    Principal,
)
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    CollectionArtifactProvenanceRecord,
    CollectionArtifactRecord,
    CollectionProvenanceJournalRecord,
    CollectionRecord,
)
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.canonical_provenance import SqlAlchemyCanonicalProvenanceService
from riverhog_protocol.errors import NotFound

from tests.unit.db_helpers import sqlite_url
from tests.unit.test_provenance_archive_read import _archive


def _service(path: Path) -> tuple[SqlAlchemyCanonicalProvenanceService, str]:
    database_url = sqlite_url(path)
    initialize_db(database_url)
    reader, _objects, journal_id, journal_text = _archive()
    with session_scope(make_session_factory(database_url)) as session:
        session.add(
            CollectionRecord(
                id=1,
                creation_idempotency_key="provenance-test",
                creation_identity_sha256="1" * 64,
                creation_custody_mode="custody-transfer",
                delivery_context_id="urn:uuid:cabfc827-91d5-4cde-8f0f-81255b75b1c4",
                artifact_set_identity="2" * 64,
                encryption_format="none",
                passphrase_id="test",
                provenance_identity="3" * 64,
                inventory_identity="4" * 64,
                archive_root_sha256="5" * 64,
                created_at="2026-01-01T00:00:00Z",
            )
        )
        session.flush()
        session.add(
            CollectionArtifactRecord(
                collection_id=1, artifact_id="c" * 64, bytes=7, sha256="f" * 64
            )
        )
        session.add(
            CollectionProvenanceJournalRecord(
                collection_id=1,
                journal_id=journal_id,
                bytes=len(journal_text.encode()),
                sha256="a" * 64,
                entries=1,
                terminal_entry_id="urn:uuid:33333333-3333-4333-8333-333333333333",
                terminal_sequence=0,
                terminal_json_sha256="d" * 64,
            )
        )
        session.flush()
        session.add(
            CollectionArtifactProvenanceRecord(
                collection_id=1,
                artifact_id="c" * 64,
                journal_id=journal_id,
                through_entry_id="urn:uuid:33333333-3333-4333-8333-333333333333",
                through_sequence=0,
                through_json_sha256="d" * 64,
                prefix_sha256="e" * 64,
                prefix_bytes=9,
                delivery_association_id="urn:uuid:22222222-2222-4222-8222-222222222222",
            )
        )
    service = SqlAlchemyCanonicalProvenanceService(
        RuntimeConfig.for_testing(database_url=database_url),
        ArchiveStoreRegistry({}),
    )
    service._archives = Mock()
    service._archives.reader.return_value = reader
    return service, journal_id


def _principal(*permissions: str) -> Principal:
    return Principal(
        id="fixture-reader",
        key_id="fixture-key",
        access=frozenset(
            ApplicationAccess(permission, "collection:1") for permission in permissions
        ),
    )


def test_exact_journal_corpus_is_root_fenced_and_export_authorized(tmp_path: Path) -> None:
    service, journal_id = _service(tmp_path / "catalog.sqlite3")
    exporter = _principal(CATALOG_READ, PROVENANCE_EXPORT)
    page = service.list_journals(1, page_size=1, after_journal_id=None, principal=exporter)
    assert page == {
        "collection_id": "1",
        "archive_root_sha256": "5" * 64,
        "journals": [
            {
                "journal_id": journal_id,
                "bytes": str(len(b"first-frame\nsecond-frame\n")),
                "sha256": service.journal_metadata(1, journal_id, principal=exporter)[1],
            }
        ],
        "next_journal_id": None,
    }
    assert (
        service.list_journals(1, page_size=1, after_journal_id=journal_id, principal=exporter)[
            "journals"
        ]
        == []
    )
    with pytest.raises(NotFound):
        service.list_journals(
            1, page_size=1, after_journal_id=None, principal=_principal(CATALOG_READ)
        )
    with pytest.raises(NotFound):
        service.list_journals(
            1, page_size=1, after_journal_id=None, principal=_principal(PROVENANCE_EXPORT)
        )


def test_member_provenance_requires_root_selected_binding_and_read_permission(
    tmp_path: Path,
) -> None:
    service, journal_id = _service(tmp_path / "catalog.sqlite3")
    reader = _principal(CATALOG_READ, PROVENANCE_READ)
    detail = service.get_artifact(1, "c" * 64, principal=reader)
    assert detail["artifact"]["artifact_id"] == "c" * 64
    assert detail["binding"]["journal"]["journal_id"] == journal_id
    with pytest.raises(NotFound):
        service.get_artifact(1, "c" * 64, principal=_principal(CATALOG_READ))
    with session_scope(service._session_factory) as session:
        projection = session.get(CollectionArtifactProvenanceRecord, (1, "c" * 64))
        assert projection is not None
        projection.prefix_sha256 = "0" * 64
    with pytest.raises(NotFound, match="archive does not confirm"):
        service.get_artifact(1, "c" * 64, principal=reader)

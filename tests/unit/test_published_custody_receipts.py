from __future__ import annotations

import json
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_sha256
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionArtifactRecord,
    CollectionRecord,
    CollectionUploadArtifactRecord,
    CollectionUploadRecord,
)
from riverhog_core.services.collection_uploads import _advance_catalog_artifacts
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_UTF8_BYTES_MAX,
    ArtifactId,
    collection_description_identity,
)
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import select

from tests.unit.test_canonical_upload_custody import _construction, _seal_payload


def _projection(tmp_path: Path):
    f = _construction(tmp_path)
    _seal_payload(f)
    assert f.service.process_due_custody_receipts() == 1
    assert f.service.process_due_custody_receipts() == 1
    with session_scope(f.factory) as session:
        session.add(
            CollectionRecord(
                id=1,
                creation_idempotency_key="fixture",
                creation_identity_sha256="1" * 64,
                creation_custody_mode="custody-transfer",
                delivery_context_id="fixture-context",
                artifact_set_identity="2" * 64,
                encryption_format="age-scrypt/v1",
                passphrase_id=f.config.archive_active_passphrase_id,
                provenance_identity="3" * 64,
                inventory_identity="4" * 64,
                created_at="2026-01-01T00:00:00Z",
                is_published=False,
            )
        )
        upload = session.get(CollectionUploadRecord, 1)
        upload.catalog_phase = "artifacts"
        upload.catalog_cursor_json = "{}"
        original = session.get(CollectionUploadArtifactRecord, (1, f.member_id))
        # Preserve original JSON octets, not a reserialized reconstruction.
        original.custody_receipt_json = json.dumps(
            json.loads(original.custody_receipt_json), indent=2
        )
        f.original = original.custody_receipt_json
    return f


def test_receipt_and_projection_cursor_rollback_and_resume_together(tmp_path: Path) -> None:
    f = _projection(tmp_path)
    with pytest.raises(RuntimeError, match="interrupted"):
        with session_scope(f.factory) as session:
            _advance_catalog_artifacts(session, session.get(CollectionUploadRecord, 1))
            session.flush()
            raise RuntimeError("interrupted before commit")
    with session_scope(f.factory) as session:
        assert session.get(CollectionArtifactRecord, (1, f.member_id)) is None
        assert session.get(CollectionUploadRecord, 1).catalog_cursor_json == "{}"
    with session_scope(f.factory) as session:
        _advance_catalog_artifacts(session, session.get(CollectionUploadRecord, 1))
    # Partial projections are not exposed through finalized readback.
    with pytest.raises(NotFound):
        f.service.get_artifact(1, ArtifactId("b" * 64))
    with session_scope(f.factory) as session:
        projected = session.get(CollectionArtifactRecord, (1, f.member_id))
        assert projected.custody_receipt_json == f.original
        _advance_catalog_artifacts(session, session.get(CollectionUploadRecord, 1))
        assert session.get(CollectionUploadRecord, 1).catalog_phase == "journals"
        session.delete(session.get(CollectionUploadRecord, 1))
    with pytest.raises(NotFound):
        f.service.get_artifact(1, ArtifactId(f.member_id))
    with session_scope(f.factory) as session:
        session.get(CollectionRecord, 1).is_published = True
    member = f.service.get_artifact(1, ArtifactId(f.member_id))
    assert member["custody_receipt"].model_dump(mode="json") == json.loads(f.original)
    assert member["payload_sealed"] is True
    # A recovered member which never possessed operational custody is not invented one.
    with session_scope(f.factory) as session:
        session.get(CollectionArtifactRecord, (1, f.member_id)).custody_receipt_json = None
    with pytest.raises(NotFound):
        f.service.get_artifact(1, ArtifactId(f.member_id))


@pytest.mark.parametrize("mutation", ["missing", "member", "primary", "completion", "self-hash"])
def test_projection_rejects_missing_or_changed_original_receipt(
    tmp_path: Path, mutation: str
) -> None:
    f = _projection(tmp_path)
    with session_scope(f.factory) as session:
        member = session.get(CollectionUploadArtifactRecord, (1, f.member_id))
        if mutation == "missing":
            member.custody_receipt_json = None
        else:
            value = json.loads(member.custody_receipt_json)
            if mutation == "member":
                value["bytes"] = "4"
            elif mutation == "primary":
                value["primary"]["delivery_association_id"] = (
                    "urn:uuid:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
                )
            elif mutation == "completion":
                value["completion_requirement_sha256"] = "f" * 64
            if mutation != "self-hash":
                value["receipt_sha256"] = canonical_json_sha256(
                    {key: item for key, item in value.items() if key != "receipt_sha256"}
                )
            else:
                value["receipt_sha256"] = "f" * 64
            member.custody_receipt_json = json.dumps(value)
    with pytest.raises((ValueError, RuntimeError, Conflict)):
        with session_scope(f.factory) as session:
            _advance_catalog_artifacts(session, session.get(CollectionUploadRecord, 1))
    with session_scope(f.factory) as session:
        assert session.get(CollectionArtifactRecord, (1, f.member_id)) is None
        assert session.get(CollectionUploadRecord, 1).catalog_cursor_json == "{}"
        assert list(session.scalars(select(CollectionUploadArtifactRecord)))


@pytest.mark.growth_witness(
    "riverhog-artifact-custody-readback/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.services.collection_uploads.SqlAlchemyCollectionUploadService.get_artifact"
    ),
)
def test_finalized_receipt_lookup_uses_indexed_points_after_cold_reopen(
    tmp_path: Path, record_property
) -> None:
    from riverhog_core.services import collection_uploads as owner
    from riverhog_growth_contracts import growth_binding
    from sqlalchemy import insert

    from tests.unit.test_journal_validation_workspace_reference_growth import sample
    from tests.unit.test_upload_journal_service_unit_growth import CoreRecorder

    f = _projection(tmp_path)
    with session_scope(f.factory) as session:
        _advance_catalog_artifacts(session, session.get(CollectionUploadRecord, 1))
        session.delete(session.get(CollectionUploadRecord, 1))
        collection = session.get(CollectionRecord, 1)
        collection.is_published = True
        collection.description = "x" * COLLECTION_DESCRIPTION_UTF8_BYTES_MAX
        collection.description_search = collection.description
        collection.description_revision = 1
        collection.archive_root_sha256 = "5" * 64
        collection.description_identity = collection_description_identity(
            archive_root_sha256=collection.archive_root_sha256,
            revision=collection.description_revision,
            description=collection.description,
        )
    engine = f.factory.kw["bind"]
    samples = []
    qualified = []
    actual_validate = owner._validate_member_custody_receipt
    retained = 0
    for population in (1, 17, 1025, 8193):
        with session_scope(f.factory) as session:
            session.execute(
                insert(CollectionArtifactRecord),
                [
                    {
                        "collection_id": 1,
                        "artifact_id": f"{number:064x}",
                        "bytes": 3,
                        "sha256": "b" * 64,
                    }
                    for number in range(retained, population)
                ],
            )
        retained = population
        for padding in (0, 8193):
            with session_scope(f.factory) as session:
                session.get(CollectionArtifactRecord, (1, f.member_id)).custody_receipt_json = (
                    " " * padding + f.original
                )
            for artifact_id, exists in (
                (f.member_id, True),
                (f"{0:064x}", False),
                (f"{population // 2:064x}", False),
                (f"{population - 1:064x}", False),
                ("f" * 64, False),
            ):
                recorder = CoreRecorder()
                validations = {"receipt_validations": 0, "receipt_validation_octets": 0}

                def validated(collection_id, member, original, _counts=validations):
                    _counts["receipt_validations"] += 1
                    _counts["receipt_validation_octets"] += len(original.encode())
                    return actual_validate(collection_id, member, original)

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(owner, "_validate_member_custody_receipt", validated)
                    with recorder.observe(engine):
                        if exists:
                            result = f.service.get_artifact(1, ArtifactId(artifact_id))
                        else:
                            with pytest.raises(NotFound):
                                f.service.get_artifact(1, ArtifactId(artifact_id))
                if exists:
                    assert result["custody_receipt"].primary == f.primary.binding
                metrics = dict(recorder.metrics, **validations)
                for bound in growth_binding(
                    owner.SqlAlchemyCollectionUploadService.get_artifact
                ).declaration.bounds:
                    metrics.setdefault(bound.metric, 0)
                assert 0 < metrics["sql_statements"] <= 5
                assert 0 < metrics["returned_rows"] <= 4
                assert metrics.get("source_payload_octets", 0) == 0
                assert metrics["changed_rows"] == 0
                # Point B-tree seeks; account actual SQLite instructions on cold connections.
                assert metrics["sqlite_vm_instructions"] <= 512 * (population + 2).bit_length()
                control = len(f.original.encode()) + padding
                assert metrics["returned_metadata_octets"] <= 4 * control
                samples.append(
                    {
                        "population": population,
                        "padding": padding,
                        "artifact_id": artifact_id,
                        "exists": exists,
                        "control_octets": control,
                        "metrics": metrics,
                    }
                )
                qualified.append(
                    sample(
                        owner.SqlAlchemyCollectionUploadService.get_artifact,
                        {"retained_members": population + 1, "control_octets": control},
                        metrics,
                    )
                )
    record_property("receipt_lookup_samples", samples)
    record_property("growth_samples", qualified)


def test_finalized_receipt_http_readback_uses_current_creator_and_scoped_delete_authority(
    tmp_path: Path,
) -> None:
    from types import SimpleNamespace

    from http_api_contracts.browse import BrowseTokenCodec
    from riverhog_api.app import create_app
    from riverhog_core.app_permissions import (
        ALL_RESOURCES,
        COLLECTIONS_CREATE,
        COLLECTIONS_DELETE,
        ApplicationAccess,
        Principal,
    )
    from riverhog_protocol.errors import Forbidden, Unauthorized

    from tests.support.upload_api import UploadServiceApi

    f = _projection(tmp_path)
    with session_scope(f.factory) as session:
        _advance_catalog_artifacts(session, session.get(CollectionUploadRecord, 1))
        session.delete(session.get(CollectionUploadRecord, 1))
        session.get(CollectionRecord, 1).is_published = True
    creator = Principal(
        id="riverhog",
        key_id="current-key",
        access=frozenset({ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES)}),
    )
    current = [creator]
    container = SimpleNamespace(
        app_keys=SimpleNamespace(
            authenticate=lambda token: current[0] if token == "fixture-upload-token" else None
        ),
        collection_uploads=f.service,
        browse_tokens=BrowseTokenCodec(
            b"fixture-custody-readback-key-00000", lifetime_seconds=3600
        ),
    )
    api = UploadServiceApi(f.service, creator, app=create_app(container=container))
    try:
        expected = api.get_collection_upload_session_artifact(1, ArtifactId(f.member_id))
        assert expected["custody_receipt"] == json.loads(f.original)
        current[0] = Principal(id="other-creator", key_id="other-key", access=creator.access)
        with pytest.raises(NotFound):
            api.get_collection_upload_session_artifact(1, ArtifactId(f.member_id))
        current[0] = Principal(
            id="deletion-reader",
            key_id="delete-key",
            access=frozenset({ApplicationAccess(COLLECTIONS_DELETE, "collection:1")}),
        )
        assert api.get_collection_upload_session_artifact(1, ArtifactId(f.member_id)) == expected
        current[0] = Principal(
            id="deletion-reader",
            key_id="delete-key",
            access=frozenset({ApplicationAccess(COLLECTIONS_DELETE, "collection:2")}),
        )
        with pytest.raises(NotFound):
            api.get_collection_upload_session_artifact(1, ArtifactId(f.member_id))
        current[0] = Principal(id=creator.id, key_id=creator.key_id, access=frozenset())
        with pytest.raises(Forbidden):
            api.get_collection_upload_session_artifact(1, ArtifactId(f.member_id))
        current[0] = None
        with pytest.raises(Unauthorized):
            api.get_collection_upload_session_artifact(1, ArtifactId(f.member_id))
    finally:
        api.close()


def test_original_receipt_survives_interrupted_and_completed_forward_upgrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import shutil

    from riverhog_core.catalog_db import STATE_MIGRATIONS, Base, initialize_db
    from sqlalchemy import Column, MetaData, Text
    from state_schema import StateSchema, assert_schema_matches_metadata, sqlite_engine

    from tests.unit.db_helpers import sqlite_url

    database = tmp_path / "custody.db"
    initialize_db(sqlite_url(database))
    f = _projection(tmp_path)
    with session_scope(f.factory) as session:
        _advance_catalog_artifacts(session, session.get(CollectionUploadRecord, 1))
        session.delete(session.get(CollectionUploadRecord, 1))
        session.get(CollectionRecord, 1).is_published = True
    migrations = tmp_path / "future-migrations"
    shutil.copytree(STATE_MIGRATIONS, migrations, ignore=shutil.ignore_patterns("__pycache__"))
    (migrations / "versions" / "v1_0002.py").write_text(
        "import os\n"
        "from alembic import op\n"
        "import sqlalchemy as sa\n"
        "revision = 'v1_0002'\n"
        "down_revision = 'v1_0001'\n"
        "branch_labels = None\n"
        "depends_on = None\n"
        "def upgrade():\n"
        "    op.add_column('collection_artifacts', sa.Column('future_label', sa.Text()))\n"
        "    if os.environ.get('RIVERHOG_TEST_RECEIPT_UPGRADE_INTERRUPT'):\n"
        "        op.execute('UPDATE collection_artifacts SET custody_receipt_json = NULL')\n"
        "        raise RuntimeError('interrupted custody upgrade')\n"
        "def downgrade():\n"
        "    raise RuntimeError('forward-only')\n",
        encoding="utf-8",
    )
    metadata = MetaData()
    for table in Base.metadata.sorted_tables:
        table.to_metadata(metadata)
    metadata.tables["collection_artifacts"].append_column(Column("future_label", Text()))

    def verify(connection):
        assert_schema_matches_metadata(connection, metadata, version_table="state_schema_revision")

    schema = StateSchema(
        name="custody upgrade qualification",
        engine_factory=lambda: sqlite_engine(database),
        script_location=migrations,
        verify=verify,
    )
    with monkeypatch.context() as patch:
        patch.setenv("RIVERHOG_TEST_RECEIPT_UPGRADE_INTERRUPT", "1")
        with pytest.raises(RuntimeError, match="interrupted custody upgrade"):
            schema.upgrade()
    assert schema.status().current_revision == "v1_0001"
    with session_scope(f.factory) as session:
        assert session.get(CollectionArtifactRecord, (1, f.member_id)).custody_receipt_json == (
            f.original
        )
    assert schema.upgrade().current_revision == "v1_0002"
    assert schema.validate().condition == "current"
    with session_scope(f.factory) as session:
        assert session.get(CollectionArtifactRecord, (1, f.member_id)).custody_receipt_json == (
            f.original
        )
    assert f.service.get_artifact(1, ArtifactId(f.member_id))["custody_receipt"].primary == (
        f.primary.binding
    )

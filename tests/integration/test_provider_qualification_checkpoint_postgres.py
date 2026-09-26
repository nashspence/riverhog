from __future__ import annotations

import os
import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from riverhog_core.catalog_db import (
    create_catalog_engine,
    initialize_db,
    make_session_factory,
    session_scope,
)
from riverhog_core.catalog_models import (
    AppKeyAccessGrantRecord,
    AppKeyRecord,
    CollectionFileRecord,
    CollectionRecord,
    RetrievalJobRecord,
    RetrievalPlanFileRecord,
    RetrievalPlanRecord,
)
from sqlalchemy import text
from sqlalchemy.engine import make_url

from scripts import provider_qualification_checkpoint as checkpoint
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

pytestmark = pytest.mark.integration
KEY_ID = "a" * 16
PLAN_ETAG = "d" * 64


@pytest.fixture
def isolated_database_url() -> Iterator[str]:
    configured = os.getenv("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not configured:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    schema = f"riverhog_provider_checkpoint_{uuid4().hex}"
    admin = create_catalog_engine(configured)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    url = make_url(configured).update_query_dict({"options": f"-csearch_path={schema},public"})
    try:
        yield url.render_as_string(hide_password=False)
    finally:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def test_snapshot_owner_query_matches_the_current_postgres_schema(
    isolated_database_url: str,
) -> None:
    initialize_db(isolated_database_url)
    now = datetime.now(UTC)
    created = (now - timedelta(hours=1)).isoformat()
    deadline = (now + timedelta(days=3)).isoformat()
    factory = make_session_factory(isolated_database_url)
    with session_scope(factory) as session:
        incarnation_id = seed_storage_incarnation(session, "archive", "archive")
        session.add(
            AppKeyRecord(
                id=KEY_ID,
                app=checkpoint.APP,
                token_sha256="f" * 64,
                monthly_download_quota_bytes=2 * 1024**3,
                created_at=created,
            )
        )
        session.flush()
        session.add(
            AppKeyAccessGrantRecord(
                key_id=KEY_ID,
                permission="*",
                resource="*",
                created_at=created,
            )
        )
        session.add(
            CollectionRecord(
                id=42,
                creation_idempotency_key="provider-qualification-collection",
                creation_identity_sha256="1" * 64,
                creation_custody_mode="copy",
                creation_archive_store="archive",
                content_identity="2" * 64,
                encryption_format="age-scrypt-v1",
                passphrase_id="qualification-key-v1",
                inventory_identity="3" * 64,
                created_by_principal_id=checkpoint.APP,
                created_by_key_id=KEY_ID,
                created_at=created,
            )
        )
        session.flush()
        session.add(
            CollectionFileRecord(
                collection_id=42,
                path="sample.bin",
                bytes=1,
                sha256="4" * 64,
            )
        )
        session.add(
            RetrievalPlanRecord(
                id="plan-42",
                principal_id=checkpoint.APP,
                initiated_by_key_id=KEY_ID,
                idempotency_key="provider-qualification-plan",
                creation_identity_sha256="5" * 64,
                state="consumed",
                request_json="{}",
                lease_seconds=86400,
                restore_policy="allow",
                created_at=created,
                expires_at=deadline,
                file_commitment_sha256="6" * 64,
                segment_commitment_sha256="7" * 64,
                etag=PLAN_ETAG,
            )
        )
        session.flush()
        session.add(
            RetrievalPlanFileRecord(
                plan_id="plan-42",
                file_order=0,
                collection_id=42,
                path="sample.bin",
                bytes=1,
                sha256="4" * 64,
                source_store="archive",
                source_incarnation_id=incarnation_id,
                requires_restore=True,
            )
        )
        session.add(
            RetrievalJobRecord(
                id="job-42",
                plan_id="plan-42",
                principal_id=checkpoint.APP,
                initiated_by_key_id=KEY_ID,
                state="requested",
                plan_etag=PLAN_ETAG,
                lease_seconds=86400,
                created_at=created,
            )
        )

    statement = re.search(r"SELECT json_build_object\([\s\S]*\);", checkpoint.SNAPSHOT_SQL)
    assert statement is not None
    sql = statement.group().removesuffix(";")
    for name in ("key_id", "collection_id", "job_id"):
        sql = sql.replace(f":'{name}'", f":{name}")
    with session_scope(factory) as session:
        snapshot = session.execute(
            text(sql),
            {"key_id": KEY_ID, "collection_id": "42", "job_id": "job-42"},
        ).scalar_one()
    assert isinstance(snapshot, dict)
    assert snapshot["key"]["id"] == KEY_ID
    assert snapshot["collection"]["created_by_key_id"] == KEY_ID
    assert snapshot["job"]["initiated_by_key_id"] == KEY_ID
    assert snapshot["plan"]["collection_ids"] == [42]
    checkpoint.validate_snapshot(
        {
            "phase": "restore-pending",
            "qualification_key_id": KEY_ID,
            "collection_id": 42,
            "retrieval_job_id": "job-42",
            "restore_deadline_at": deadline,
        },
        snapshot,
        now=now,
        pending_timeout_seconds=72 * 60 * 60,
        monthly_download_quota_bytes=2 * 1024**3,
    )

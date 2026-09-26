"""Database operations for the host-driven provider checkpoint PostgreSQL test."""

from __future__ import annotations

import hashlib
import os
import sys
from datetime import UTC, datetime, timedelta

from riverhog_core.app_permissions import QUOTAS_MANAGE, ApplicationAccess, Principal
from riverhog_core.catalog_db import initialize_db, make_session_factory, session_scope
from riverhog_core.catalog_models import (
    AppKeyAccessGrantRecord,
    AppKeyRecord,
    CollectionFileRecord,
    CollectionRecord,
    RetrievalJobRecord,
    RetrievalPlanFileRecord,
    RetrievalPlanRecord,
)
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.app_keys import SqlAlchemyAppKeyService
from time_formats import format_utc_timestamp

from scripts import provider_qualification_checkpoint as checkpoint
from tests.unit.storage_incarnation_fixtures import seed_storage_incarnation

KEY_ID = "a" * 16
PLAN_ETAG = "d" * 64
INITIAL_TOKEN = "rh_app_provider_checkpoint_test"


def _service(database_url: str) -> SqlAlchemyAppKeyService:
    return SqlAlchemyAppKeyService(RuntimeConfig.for_testing(database_url=database_url))


def _seed(database_url: str) -> None:
    initialize_db(database_url)
    now = datetime.now(UTC)
    created = format_utc_timestamp(now - timedelta(hours=1))
    deadline = format_utc_timestamp(now + timedelta(days=4))
    factory = make_session_factory(database_url)
    with session_scope(factory) as session:
        incarnation_id = seed_storage_incarnation(session, "archive", "archive")
        session.add(
            AppKeyRecord(
                id=KEY_ID,
                app=checkpoint.APP,
                token_sha256=hashlib.sha256(INITIAL_TOKEN.encode()).hexdigest(),
                monthly_download_quota_bytes=2 * 1024**3,
                created_at=created,
            )
        )
        session.flush()
        session.add(
            AppKeyAccessGrantRecord(key_id=KEY_ID, permission="*", resource="*", created_at=created)
        )
        session.add(
            CollectionRecord(
                id=42,
                creation_idempotency_key="provider-checkpoint-postgres-test",
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
            CollectionFileRecord(collection_id=42, path="sample.bin", bytes=1, sha256="4" * 64)
        )
        session.add(
            RetrievalPlanRecord(
                id="plan-42",
                principal_id=checkpoint.APP,
                initiated_by_key_id=KEY_ID,
                idempotency_key="provider-checkpoint-postgres-plan",
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
                state="ready",
                plan_etag=PLAN_ETAG,
                lease_seconds=86400,
                created_at=created,
                ready_at=format_utc_timestamp(now),
                expires_at=format_utc_timestamp(now + timedelta(days=1)),
            )
        )


def _rotate(database_url: str) -> None:
    grantor = Principal(
        id="bootstrap",
        key_id=None,
        access=frozenset({ApplicationAccess(QUOTAS_MANAGE)}),
        unrestricted_delegation=True,
    )
    rotated = _service(database_url).rotate(app=checkpoint.APP, key_id=KEY_ID, grantor=grantor)
    assert rotated["id"] == KEY_ID
    assert rotated["token"] != INITIAL_TOKEN
    with session_scope(make_session_factory(database_url)) as session:
        key = session.get(AppKeyRecord, KEY_ID)
        assert key is not None
        assert key.token_sha256 == hashlib.sha256(str(rotated["token"]).encode()).hexdigest()
        assert key.revoked_at is None


def _job_owner(database_url: str, key_id: str) -> None:
    with session_scope(make_session_factory(database_url)) as session:
        job = session.get(RetrievalJobRecord, "job-42")
        assert job is not None
        job.initiated_by_key_id = key_id


def _revoke(database_url: str) -> None:
    revoked = _service(database_url).revoke(app=checkpoint.APP, key_id=KEY_ID)
    assert revoked["id"] == KEY_ID
    assert revoked["status"] == "revoked"


def _assert_revoked(database_url: str) -> None:
    with session_scope(make_session_factory(database_url)) as session:
        key = session.get(AppKeyRecord, KEY_ID)
        job = session.get(RetrievalJobRecord, "job-42")
        assert key is not None and key.revoked_at is not None
        assert job is not None and job.state == "canceled"


def main() -> None:
    database_url = os.environ["RIVERHOG_TEST_POSTGRES_URL"]
    action = sys.argv[1]
    if action == "seed":
        _seed(database_url)
    elif action == "rotate":
        _rotate(database_url)
    elif action == "poison-owner":
        _job_owner(database_url, "e" * 16)
    elif action == "repair-owner":
        _job_owner(database_url, KEY_ID)
    elif action == "revoke":
        _revoke(database_url)
    elif action == "assert-revoked":
        _assert_revoked(database_url)
    else:
        raise ValueError("unknown provider checkpoint test action")


if __name__ == "__main__":
    main()

"""Live key, sealed claim and write-control fences for collection-owned operations.

These references do not grant operation permissions or construction custody.
The composing owner checks those scopes while retaining its construction lock.
"""

from __future__ import annotations

import json

from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import select
from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.app_permissions import Principal
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.collection_construction import upload_construction_fields
from riverhog_core.services.app_keys import (
    KeyAuthorizationFence,
    retain_key_authorization,
    retain_key_validity,
)


def retain_upload_key(
    session: Session, principal: Principal, *, for_update: bool = True, nowait: bool = False
) -> KeyAuthorizationFence | None:
    if principal.id.startswith("processing:"):
        if principal.key_id is None:
            raise NotFound("collection upload execution key is unavailable")
        return retain_key_validity(session, principal.key_id, for_update=for_update, nowait=nowait)
    return retain_key_authorization(session, principal, for_update=for_update, nowait=nowait)


def retain_upload_claim(
    session: Session,
    principal_id: str,
    key_id: str | None,
    *,
    for_update: bool = True,
    nowait: bool = False,
) -> str | None:
    if not principal_id.startswith("processing:"):
        return None
    execution = principal_id.removeprefix("processing:")
    statement = select(
        CollectionProcessingClaimRecord.id,
        CollectionProcessingClaimRecord.state,
        CollectionProcessingClaimRecord.plan_sealed_at,
        CollectionProcessingClaimRecord.expires_at,
        CollectionProcessingClaimRecord.consumer_key_id,
    ).where(CollectionProcessingClaimRecord.execution_id == execution)
    if for_update:
        statement = statement.with_for_update(nowait=nowait)
    claim = session.execute(statement).one_or_none()
    if (
        claim is None
        or claim.state != "active"
        or claim.plan_sealed_at is None
        or claim.expires_at <= utc_timestamp_now()
        or claim.consumer_key_id != key_id
    ):
        raise NotFound("collection upload execution authority is no longer active")
    return str(claim.id)


def require_archive_write_control(
    session: Session, upload: CollectionUploadRecord
) -> ArchiveWriteControlRecord:
    """Read original write authority under the caller's construction write lock."""
    control = session.get(ArchiveWriteControlRecord, upload.collection_id)
    if control is None:
        raise Conflict("archive write admission owner is uninitialized")
    original = json.loads(control.construction_json)
    if not isinstance(original, dict) or "policy" not in original:
        raise RuntimeError("archive write original construction is invalid")
    original.pop("policy")
    if original != upload_construction_fields(upload):
        raise Conflict("archive write construction identity changed")
    return control


def require_open_upload_writes(
    session: Session, upload: CollectionUploadRecord
) -> ArchiveWriteControlRecord:
    """A current permission/lease cannot reopen a selected terminal's write gate."""
    control = require_archive_write_control(session, upload)
    if control.gate != "open":
        raise Conflict("archive write admission is closed")
    return control

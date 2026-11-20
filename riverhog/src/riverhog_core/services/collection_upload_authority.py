"""Live key, sealed claim and write-control fences for collection-owned operations.

These references do not grant operation permissions or construction custody.
The composing owner checks those scopes while retaining its construction lock.
"""

from __future__ import annotations

import json

from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import select
from sqlalchemy.orm import Session
from time_formats import parse_utc_timestamp, utc_epoch_ns_now, utc_timestamp_now

from riverhog_core.app_permissions import COLLECTIONS_CREATE, ApplicationAccess, Principal
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


def retain_processing_upload_principal(
    session: Session, principal_id: str, key: KeyAuthorizationFence, *, nowait: bool = False
) -> Principal:
    """Reload only creator scope for an already selected processing-output cancel.

    The caller retains the live original key before this claim. No capability,
    input-read authority, deletion scope or bearer is reconstructed. The selected
    upload and terminal guards must still bind this execution and original key.
    """
    claim_id = retain_upload_claim(session, principal_id, key.key_id, nowait=nowait)
    if claim_id is None:
        raise NotFound("collection upload execution authority is unavailable")
    claim = session.get(CollectionProcessingClaimRecord, claim_id, populate_existing=True)
    if (
        claim is None
        or claim.consumer_app != key.app
        or claim.result_kind != "collection"
        or claim.execution_id != principal_id.removeprefix("processing:")
    ):
        raise NotFound("collection upload execution authority changed")
    return Principal(
        id=principal_id,
        key_id=key.key_id,
        access=frozenset({ApplicationAccess(COLLECTIONS_CREATE)}),
    )


def retain_inactive_upload_claim(
    session: Session, principal_id: str, *, for_update: bool = True, nowait: bool = False
) -> str | None:
    """Retain the original output's inactive processing claim before its parent.

    The caller supplies the principal from the exact original construction and
    owns the early SQLite writer. A processing output with a missing claim is
    unknown, not an unclaimed output. Retaining this row interlocks renewal for
    the caller's short transaction; it neither prohibits later renewal nor
    supplies orphan selection, current deletion rights or cleanup permission.
    A caller that already retains this exact row can recheck without reacquiring
    the earlier claim lock class after it holds the construction parent.
    """
    if not principal_id.startswith("processing:"):
        return None
    statement = select(
        CollectionProcessingClaimRecord.id,
        CollectionProcessingClaimRecord.state,
        CollectionProcessingClaimRecord.expires_at,
    ).where(
        CollectionProcessingClaimRecord.execution_id == principal_id.removeprefix("processing:")
    )
    if for_update:
        statement = statement.with_for_update(nowait=nowait)
    claim = session.execute(statement).one_or_none()
    if claim is None:
        raise NotFound("collection upload output claim is unavailable")
    if claim.state == "active" and parse_utc_timestamp(claim.expires_at) > utc_epoch_ns_now():
        raise Conflict("owning collection upload processing claim remains active")
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

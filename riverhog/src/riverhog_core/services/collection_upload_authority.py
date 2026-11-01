"""Live key and sealed execution-claim fences for collection-owned operations.

These references do not grant operation permissions or construction custody.
The composing owner checks those scopes while retaining its construction lock.
"""

from __future__ import annotations

from riverhog_protocol.errors import NotFound
from sqlalchemy import select
from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.app_permissions import Principal
from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord
from riverhog_core.services.app_keys import (
    KeyAuthorizationFence,
    retain_key_authorization,
    retain_key_validity,
)


def retain_upload_key(
    session: Session, principal: Principal, *, for_update: bool = True
) -> KeyAuthorizationFence | None:
    if principal.id.startswith("processing:"):
        if principal.key_id is None:
            raise NotFound("collection upload execution key is unavailable")
        return retain_key_validity(session, principal.key_id, for_update=for_update)
    return retain_key_authorization(session, principal, for_update=for_update)


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

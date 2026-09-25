from __future__ import annotations

import hashlib
import uuid

from riverhog_core.catalog_models import StorageIncarnationRecord
from sqlalchemy.orm import Session

_CREATED_AT = "2026-01-01T00:00:00.000000000Z"


def fixture_storage_incarnation_id(kind: str, name: str) -> str:
    digest = hashlib.sha256(f"{kind}/{name}".encode()).digest()
    return str(uuid.UUID(bytes=digest[:16], version=4))


def seed_storage_incarnation(session: Session, kind: str, name: str) -> str:
    incarnation_id = fixture_storage_incarnation_id(kind, name)
    if session.get(StorageIncarnationRecord, (kind, name)) is None:
        session.add(
            StorageIncarnationRecord(
                id=incarnation_id,
                kind=kind,
                name=name,
                state="bound",
                binding_generation=1,
                created_at=_CREATED_AT,
                last_bound_at=_CREATED_AT,
                last_read_mode="immediate",
            )
        )
        session.flush()
    return incarnation_id

"""Bounded retained-call capacity shared by original cache session owners."""

from __future__ import annotations

from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_cache_write_models import (
    CachePopulationBeginRecord,
    CachePopulationWriteEffectRecord,
)
from riverhog_core.catalog_models import RetrievalCachePopulationRecord


def require_cache_write_capacity(
    session: Session, collection_id: int, *, charged: int, grants: CacheWriteGrants
) -> None:
    # The caller retains the parent construction lock while admitting either
    # kind of effect. Acknowledged report retirement can only reduce this charge.
    calls = control_bytes = 0
    for model in (CachePopulationBeginRecord, CachePopulationWriteEffectRecord):
        retained = list(
            session.execute(
                select(model.id, model.control_bytes)
                .where(model.collection_id == collection_id)
                .order_by(model.id)
                .limit(grants.calls)
            )
        )
        calls += len(retained)
        control_bytes += sum(row.control_bytes for row in retained)
    if calls >= grants.calls or control_bytes + charged > grants.control_bytes:
        raise Conflict("cache write retained-effect grant is exhausted")


def retains_cache_write(session: Session, key: tuple[str, int, str]) -> bool:
    for model in (CachePopulationBeginRecord, CachePopulationWriteEffectRecord):
        if (
            session.scalar(
                select(model.id)
                .where(
                    model.source_store == key[0],
                    model.collection_id == key[1],
                    model.object_id == key[2],
                )
                .limit(1)
            )
            is not None
        ):
            return True
    population = session.get(RetrievalCachePopulationRecord, key)
    return population is not None and population.begin_call_id is not None

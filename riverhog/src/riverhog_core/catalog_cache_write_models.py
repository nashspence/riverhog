"""Original cache-population begin effects retained independently of upload rows."""

from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_models import COLLECTION_ID_TYPE


class CachePopulationBeginRecord(Base):
    __tablename__ = "cache_population_begins"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE)
    source_store: Mapped[str] = mapped_column(String)
    object_id: Mapped[str] = mapped_column(String)
    population_generation: Mapped[str] = mapped_column(String(64))
    cache_store: Mapped[str] = mapped_column(String)
    cache_incarnation_id: Mapped[str] = mapped_column(String(36))
    completion_call_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    executor_id: Mapped[str] = mapped_column(String(64))
    dispatch_sha256: Mapped[str] = mapped_column(String(64))
    request_sha256: Mapped[str] = mapped_column(String(64))
    request_json: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String)
    unresolved: Mapped[bool] = mapped_column(Boolean)
    control_bytes: Mapped[int] = mapped_column(BigInteger)
    result_capacity_bytes: Mapped[int] = mapped_column(BigInteger)
    cleanup_capacity_bytes: Mapped[int] = mapped_column(BigInteger)
    observation_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("population_generation", name="uq_cache_population_begins_generation"),
        CheckConstraint(
            "state IN ('armed','uncertain','settled')", name="ck_cache_population_begins_state"
        ),
        CheckConstraint(
            "control_bytes > 0 AND result_capacity_bytes > 0 AND cleanup_capacity_bytes > 0",
            name="ck_cache_population_begins_capacity",
        ),
        CheckConstraint(
            "state = 'settled' AND unresolved IS false AND result_json IS NOT NULL OR "
            "state IN ('armed','uncertain') AND unresolved IS true AND result_json IS NULL",
            name="ck_cache_population_begins_outcome",
        ),
        Index("ix_cache_population_begins_pending", "collection_id", "unresolved", "id"),
        Index("ix_cache_population_begins_capacity", "collection_id", "id"),
        Index("ix_cache_population_begins_object", "source_store", "collection_id", "object_id"),
    )


class CachePopulationWriteEffectRecord(Base):
    __tablename__ = "cache_population_write_effects"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE)
    source_store: Mapped[str] = mapped_column(String)
    object_id: Mapped[str] = mapped_column(String)
    population_generation: Mapped[str] = mapped_column(String(64))
    cache_store: Mapped[str] = mapped_column(String)
    cache_incarnation_id: Mapped[str] = mapped_column(String(36))
    begin_call_id: Mapped[str] = mapped_column(String(64))
    kind: Mapped[str] = mapped_column(String)
    resource: Mapped[str] = mapped_column(String)
    executor_id: Mapped[str] = mapped_column(String(64))
    dispatch_sha256: Mapped[str] = mapped_column(String(64))
    request_sha256: Mapped[str] = mapped_column(String(64))
    request_json: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String)
    unresolved: Mapped[bool] = mapped_column(Boolean)
    control_bytes: Mapped[int] = mapped_column(BigInteger)
    result_capacity_bytes: Mapped[int] = mapped_column(BigInteger)
    cleanup_capacity_bytes: Mapped[int] = mapped_column(BigInteger)
    observation_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint("kind IN ('part','complete')", name="ck_cache_write_effects_kind"),
        CheckConstraint(
            "state IN ('armed','uncertain','settled')", name="ck_cache_write_effects_state"
        ),
        CheckConstraint(
            "control_bytes > 0 AND result_capacity_bytes > 0 AND cleanup_capacity_bytes > 0",
            name="ck_cache_write_effects_capacity",
        ),
        CheckConstraint(
            "state = 'settled' AND unresolved IS false AND result_json IS NOT NULL OR "
            "state IN ('armed','uncertain') AND unresolved IS true AND result_json IS NULL",
            name="ck_cache_write_effects_outcome",
        ),
        Index("ix_cache_write_effects_pending", "collection_id", "unresolved", "id"),
        Index("ix_cache_write_effects_capacity", "collection_id", "id"),
        Index("ix_cache_write_effects_object", "source_store", "collection_id", "object_id"),
        Index("ix_cache_write_effects_session", "begin_call_id", "unresolved", "resource", "id"),
    )

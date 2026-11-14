"""Application custody of disposable, cross-turn primary preparation indexes."""

from __future__ import annotations

from sqlalchemy import CheckConstraint, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_models import COLLECTION_ID_TYPE


class PrimaryFactsJobRecord(Base):
    __tablename__ = "primary_facts_jobs"

    workspace_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE, nullable=False)
    construction: Mapped[str] = mapped_column(Text, nullable=False)
    epoch: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_id: Mapped[str] = mapped_column(String(32), nullable=False)
    root_identity: Mapped[str] = mapped_column(Text, nullable=False)
    namespace_identity: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    failure: Mapped[str | None] = mapped_column(Text, nullable=True)

    # No content foreign key: namespace disposal must remain discoverable after
    # upload/content retirement. The collection key bounds unfinished allocation
    # to one original, including capacity refusals awaiting physical receiving.
    __table_args__ = (
        CheckConstraint(
            "state IN ('allocated','pinned','corpus','memo','directory')",
            name="ck_primary_facts_state",
        ),
        CheckConstraint(
            "state != 'allocated' OR namespace_identity IS NULL",
            name="ck_primary_facts_unconfirmed",
        ),
        CheckConstraint(
            "state != 'pinned' OR namespace_identity IS NOT NULL",
            name="ck_primary_facts_confirmed",
        ),
        *(
            CheckConstraint(
                f"length({column}) = 32 AND lower({column}) = {column} AND "
                + "length("
                + "replace(" * 16
                + column
                + "".join(f",'{value}','')" for value in "0123456789abcdef")
                + ") = 0",
                name="ck_primary_facts_" + column,
            )
            for column in ("workspace_id", "owner_id", "version")
        ),
        Index("uq_primary_facts_collection", "collection_id", unique=True),
        Index("ix_primary_facts_state_workspace", "state", "workspace_id"),
    )

"""Original initial-description facts and their receiving/progress checkpoints."""

from __future__ import annotations

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_models import COLLECTION_ID_TYPE


class CollectionUploadInitialDescriptionRecord(Base):
    __tablename__ = "collection_upload_initial_descriptions"

    collection_id: Mapped[int] = mapped_column(COLLECTION_ID_TYPE, primary_key=True)
    document_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_json: Mapped[str] = mapped_column(Text)
    source_sha256: Mapped[str] = mapped_column(String(64))
    receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    progress_receipt_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["collection_id"], ["collection_uploads.collection_id"], ondelete="CASCADE"
        ),
        CheckConstraint("length(source_json) > 0", name="ck_initial_description_source"),
        CheckConstraint(
            "document_json IS NOT NULL OR receipt_json IS NULL",
            name="ck_initial_description_physical_receipt",
        ),
    )

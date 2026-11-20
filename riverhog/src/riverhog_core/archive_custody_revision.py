"""Freshness of positive archive custody observations under construction locks."""

from __future__ import annotations

from riverhog_canonical_json import format_scalar, parse_scalar
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord


def advance_archive_custody_revision(session: Session, collection_id: int) -> None:
    """Advance with a newly accepted exact result in its owning transaction.

    The caller holds the construction write lock and deduplicates the original
    checkpoint/report before calling. A revision is a conservative freshness
    marker, not a receipt, execution-closure proof or permission to dispose.
    """
    control = session.get(ArchiveWriteControlRecord, collection_id)
    if control is None:
        raise RuntimeError("archive write custody owner is unavailable")
    control.custody_revision = format_scalar(
        "nonnegative", parse_scalar("nonnegative", control.custody_revision) + 1
    )

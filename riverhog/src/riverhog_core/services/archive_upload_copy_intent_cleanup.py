"""Cancel unstarted copy handoffs under the exact original terminal authority."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

from riverhog_protocol.errors import Conflict
from sqlalchemy import bindparam, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadCopyIntentRecord as Intent
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.incremental_emission import VolumeEmissionLimits
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.catalog_retirement import read_catalog_window


@dataclass(frozen=True, slots=True)
class CopyIntentCleanupProgress:
    canceled: int = 0
    metadata_bytes: int = 0
    exhausted: bool = False


class ArchiveUploadCopyIntentCleanup:
    """Retain destination identities and receipts; never execute a copy.

    An accepted intent belongs to the retained upload construction. Only the
    atomic collection-publication transaction may make it pending. A different
    live state therefore blocks this unpublished-upload owner rather than
    granting authority over an independently runnable copy job.
    """

    def __init__(self, factory: SessionFactory, cleanup: ArchiveUploadCleanup) -> None:
        self._factory = factory
        self._cleanup = cleanup

    def advance(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
        limits: VolumeEmissionLimits,
    ) -> CopyIntentCleanupProgress:
        with session_scope(self._factory) as session:
            self._cleanup.require_exhausted(session, collection_id, operation_id, guard=guard)
            upload = session.get(Upload, collection_id)
            assert upload is not None
            if upload.active_registration_id is not None:
                raise Conflict("copy-intent retirement requires completed hidden registration")
            if (
                session.scalar(
                    select(Intent.destination_store)
                    .where(
                        Intent.collection_id == collection_id,
                        Intent.state.in_(("pending", "handed_off", "failed")),
                    )
                    .limit(1)
                )
                is not None
            ):
                raise Conflict("copy-intent retirement found an independently runnable handoff")
            table = Base.metadata.tables[Intent.__tablename__]
            rows, used = read_catalog_window(
                session,
                table,
                (table.c.collection_id == collection_id, table.c.state == "accepted"),
                count=limits.input_transitions,
                metadata_bytes=limits.metadata_bytes,
            )
            if not rows:
                return CopyIntentCleanupProgress(exhausted=True)
            expected = {
                "source_store": upload.archive_store,
                "source_incarnation_id": upload.archive_incarnation_id,
                "initiated_by_app": upload.initiated_by_principal_id,
                "initiated_by_key_id": upload.initiated_by_key_id,
                "event_context_json": upload.event_context_json,
                "use_cache": upload.use_cache,
                "attempts": 0,
                "next_attempt_at": None,
                "handed_off_at": None,
                "job_created": None,
                "failure_code": None,
            }
            for row in rows:
                if any(row.values[name] != value for name, value in expected.items()):
                    raise Conflict("accepted copy intent differs from the original upload")
            statement = (
                update(table)
                .where(
                    table.c.collection_id == bindparam("target_collection"),
                    table.c.destination_store == bindparam("target_destination"),
                    table.c.state == "accepted",
                )
                .values(state="canceled", next_attempt_at=None)
            )
            result = session.execute(
                statement,
                [
                    {
                        "target_collection": row.values["collection_id"],
                        "target_destination": row.values["destination_store"],
                    }
                    for row in rows
                ],
            )
            if cast(CursorResult[Any], result).rowcount != len(rows):
                raise Conflict("accepted copy-intent selection changed")
            return CopyIntentCleanupProgress(len(rows), used)

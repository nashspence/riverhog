"""Retire original encryption/checkpoint preparations after verified archive cleanup."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from riverhog_protocol.errors import Conflict
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteEffectRecord as Effect,
)
from riverhog_core.catalog_archive_write_models import (
    ArchiveWritePreparationRecord as Preparation,
)
from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.incremental_emission import VolumeEmissionLimits
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.catalog_retirement import delete_catalog_window, read_catalog_window


@dataclass(frozen=True, slots=True)
class CheckpointCleanupProgress:
    removed: int = 0
    metadata_bytes: int = 0
    exhausted: bool = False


class ArchiveUploadCheckpointCleanup:
    """The closed construction keeps authority until original preparations retire.

    Provider-dependent original evidence survives until the exact complete
    archive proof. Every later metadata quantum checks current terminal rights
    and refuses any original effect receiving; no independent cache or workspace
    controls, upload parent, terminal or confirmed result are removed.
    """

    def __init__(self, factory: SessionFactory, cleanup: ArchiveUploadCleanup) -> None:
        children = {
            table.name
            for table in Base.metadata.tables.values()
            for constraint in table.foreign_key_constraints
            if constraint.referred_table.name == Preparation.__tablename__
        }
        if children != {Effect.__tablename__}:
            raise RuntimeError("archive preparation retirement routes differ from current schema")
        self._factory = factory
        self._cleanup = cleanup

    def advance(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
        limits: VolumeEmissionLimits,
    ) -> CheckpointCleanupProgress:
        with session_scope(self._factory) as session:
            self._cleanup.require_exhausted(session, collection_id, operation_id, guard=guard)
            upload = session.get(CollectionUploadRecord, collection_id)
            assert upload is not None
            if upload.active_registration_id is not None:
                raise Conflict("checkpoint retirement requires completed hidden registration")
            if (
                session.scalar(
                    select(Effect.id).where(Effect.collection_id == collection_id).limit(1)
                )
                is not None
            ):
                raise Conflict("checkpoint retirement still has original effect receiving")
            table = Base.metadata.tables[Preparation.__tablename__]
            rows, used = read_catalog_window(
                session,
                table,
                (table.c.collection_id == collection_id,),
                count=limits.input_transitions,
                metadata_bytes=limits.metadata_bytes,
            )
            if not rows:
                return CheckpointCleanupProgress(exhausted=True)
            delete_catalog_window(session, table, rows)
            return CheckpointCleanupProgress(len(rows), used)

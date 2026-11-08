"""Indexed readiness hints, separate from verified custody and admission."""

from __future__ import annotations

from threading import Lock

from riverhog_protocol.errors import Conflict
from sqlalchemy import Select, literal_column, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.catalog_models import CollectionUploadArtifactProvenanceBindingRecord as Binding
from riverhog_core.catalog_models import CollectionUploadArtifactRecord as Artifact
from riverhog_core.catalog_models import CollectionUploadRecord as Upload


def _control(session: Session, upload: Upload) -> Control:
    # Callers already retain the construction parent, before this control row.
    control = session.get(Control, upload.collection_id, with_for_update=True)
    if control is None:
        raise Conflict("artifact custody scheduling construction is unavailable")
    return control


def _refresh(upload: Upload, control: Control) -> None:
    control.custody_requires_history = upload.completion_requirement_json is not None
    enabled = upload.state in {"open", "closing", "uploading", "finalizing"} and (
        not upload.initiated_by_principal_id.startswith("processing:")
        or control.custody_requires_history
    )
    control.custody_due_count = (
        (
            control.custody_history_count
            if control.custody_requires_history
            else control.custody_primary_count
        )
        if enabled
        else 0
    )


def refresh_custody_parent(session: Session, upload: Upload) -> None:
    """Change parent eligibility without traversing its member population."""
    _refresh(upload, _control(session, upload))


def refresh_custody_member(session: Session, upload: Upload, artifact_id: str) -> bool:
    """Maintain positive facts in the source mutation's parent transaction.

    Both counts preserve readiness when a completion requirement is accepted.
    They grant no verified custody or write authority. Repeated source and
    receipt acknowledgements apply no second delta.
    """
    control = _control(session, upload)
    session.flush()
    artifact = session.get(Artifact, (upload.collection_id, artifact_id), populate_existing=True)
    if artifact is None:
        return False
    binding = session.get(Binding, (upload.collection_id, artifact_id))
    stage = 0
    if (
        artifact.payload_sealed_at is not None
        and artifact.custody_receipt_json is None
        and binding is not None
    ):
        stage = 2 if binding.history_imports_ref_json is not None else 1
    previous = artifact.custody_schedule_stage
    if previous != stage:
        artifact.custody_schedule_stage = stage
        control.custody_primary_count += int(stage > 0) - int(previous > 0)
        control.custody_history_count += int(stage == 2) - int(previous == 2)
    _refresh(upload, control)
    return previous != stage


def custody_parent_query(
    *, limit: int, after: int | None = None
) -> Select[tuple[int, str | None, bool]]:
    query = select(
        Control.collection_id, Control.custody_after_artifact_id, Control.custody_requires_history
    ).where(
        Control.gate == literal_column("'open'"),
        Control.custody_due_count > literal_column("0"),
    )
    if after is not None:
        query = query.where(Control.collection_id > after)
    return query.order_by(Control.collection_id).limit(limit)


def custody_member_query(
    collection_id: int, *, requires_history: bool, after: str | None = None
) -> Select[tuple[str]]:
    query = select(Artifact.artifact_id).where(Artifact.collection_id == collection_id)
    if requires_history:
        query = query.where(Artifact.custody_schedule_stage == literal_column("2"))
    else:
        query = query.where(Artifact.custody_schedule_stage >= literal_column("1"))
    if after is not None:
        query = query.where(Artifact.artifact_id > after)
    return query.order_by(Artifact.artifact_id).limit(1)


def rotate_custody_member(session: Session, collection_id: int, artifact_id: str) -> None:
    """Record a scheduling cursor under the caller's parent/control locks."""
    control = session.get(Control, collection_id)
    if control is None:
        raise Conflict("artifact custody scheduling construction is unavailable")
    control.custody_after_artifact_id = artifact_id


class ArchiveCustodyDue:
    """Fair parent seeks and one indexed member hint per parent.

    The volatile parent cursor uses constant local state. Each parent's member
    cursor rotates after local execution registration. Losing either hint grants
    no authority, completion or release of retained originals. Selection needs at
    most two parent reads and two child reads per selected parent.
    """

    def __init__(self, factory: SessionFactory) -> None:
        self._factory = factory
        self._after: int | None = None
        self._lock = Lock()

    def select(self, *, limit: int) -> tuple[tuple[int, str], ...]:
        if type(limit) is not int or limit < 1:
            raise ValueError("artifact custody selection limit must be positive")
        if not self._lock.acquire(blocking=False):
            return ()
        try:
            selected = []
            with session_scope(self._factory) as session:
                parents = list(
                    session.execute(custody_parent_query(limit=limit, after=self._after))
                )
                if not parents and self._after is not None:
                    parents = list(session.execute(custody_parent_query(limit=limit)))
                for collection_id, after, history in parents:
                    member = session.scalar(
                        custody_member_query(collection_id, requires_history=history, after=after)
                    )
                    if member is None and after is not None:
                        member = session.scalar(
                            custody_member_query(collection_id, requires_history=history)
                        )
                    if member is not None:
                        selected.append((collection_id, member))
            self._after = parents[-1].collection_id if parents else None
            return tuple(selected)
        except OperationalError as error:
            if catalog_write_contention(error):
                return ()
            raise
        finally:
            self._lock.release()

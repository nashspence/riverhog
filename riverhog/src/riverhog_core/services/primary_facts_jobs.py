"""Exact catalog fences for application-owned primary preparation namespaces."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any
from uuid import uuid4

from riverhog_growth_contracts import declare_growth
from sqlalchemy import delete, insert, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from riverhog_core.catalog_primary_facts_models import PrimaryFactsJobRecord as Job
from riverhog_core.services.primary_facts_catalog_growth import catalog_declaration

LIVE_STATES = ("allocated", "pinned")
RETIRED_STATES = ("corpus", "memo", "directory")


@dataclass(frozen=True, slots=True)
class PrimaryFactsJob:
    workspace_id: str
    collection_id: int
    construction: str
    epoch: str
    owner_id: str
    root_identity: str
    namespace_identity: str | None
    state: str
    version: str
    failure: str | None

    @classmethod
    def read(cls, row: Job) -> PrimaryFactsJob:
        return cls(**{field.name: getattr(row, field.name) for field in fields(cls)})


def _identity(job: PrimaryFactsJob) -> tuple[ColumnElement[bool], ...]:
    """The complete original, including phase and committed namespace pin."""
    return tuple(
        getattr(Job, field.name) == getattr(job, field.name)
        for field in fields(job)
        if field.name != "failure"
    )


def get(session: Session, workspace_id: str) -> PrimaryFactsJob | None:
    row = session.scalar(
        select(Job)
        .where(Job.workspace_id == workspace_id)
        .execution_options(populate_existing=True)
    )
    return None if row is None else PrimaryFactsJob.read(row)


def find_pending(session: Session, collection_id: int) -> PrimaryFactsJob | None:
    row = session.scalar(
        select(Job)
        .where(Job.collection_id == collection_id)
        .execution_options(populate_existing=True)
    )
    return None if row is None else PrimaryFactsJob.read(row)


def current(session: Session, job: PrimaryFactsJob) -> bool:
    return session.scalar(select(Job.workspace_id).where(*_identity(job))) == job.workspace_id


def register(
    session: Session,
    *,
    collection_id: int,
    construction: str,
    epoch: str,
    owner_id: str,
    root_identity: str,
) -> PrimaryFactsJob:
    """Register without any namespace effect; the caller must commit this turn."""
    values = dict(
        workspace_id=uuid4().hex,
        collection_id=collection_id,
        construction=construction,
        epoch=epoch,
        owner_id=owner_id,
        root_identity=root_identity,
        namespace_identity=None,
        state="allocated",
        version=uuid4().hex,
        failure=None,
    )
    row = session.execute(insert(Job).values(**values).returning(Job)).scalar_one()
    return PrimaryFactsJob.read(row)


def _change(session: Session, job: PrimaryFactsJob, **values: Any) -> PrimaryFactsJob | None:
    row = session.execute(
        update(Job)
        .where(*_identity(job))
        .values(**values, version=uuid4().hex, failure=None)
        .returning(Job)
        .execution_options(populate_existing=True, synchronize_session=False)
    ).scalar_one_or_none()
    return None if row is None else PrimaryFactsJob.read(row)


def pin(session: Session, job: PrimaryFactsJob, namespace_identity: str) -> PrimaryFactsJob | None:
    """Record actual custody; no database creation is allowed in this turn."""
    if job.state != "allocated" or not namespace_identity:
        raise ValueError("primary preparation pin requires an unconfirmed allocation")
    return _change(session, job, state="pinned", namespace_identity=namespace_identity)


def retire(session: Session, job: PrimaryFactsJob) -> PrimaryFactsJob | None:
    """The caller has received originals or holds exact exclusive quiescence."""
    if job.state in RETIRED_STATES:
        return job if current(session, job) else None
    if job.state not in LIVE_STATES:
        raise ValueError("unknown primary preparation phase")
    return _change(session, job, state="corpus")


def transition(session: Session, job: PrimaryFactsJob, state: str) -> PrimaryFactsJob | None:
    allowed = {
        "allocated": ("corpus",),
        "pinned": ("corpus",),
        "corpus": ("memo",),
        "memo": ("directory",),
        "directory": (),
    }
    if state not in allowed[job.state]:
        raise ValueError("primary preparation retirement cannot skip or reverse a phase")
    return _change(session, job, state=state)


def finish(session: Session, job: PrimaryFactsJob) -> bool:
    """Forget only the exact directory-phase original after physical receiving."""
    if job.state != "directory":
        raise ValueError("primary preparation finish requires final directory receiving")
    removed = session.execute(
        delete(Job)
        .where(*_identity(job))
        .returning(Job.workspace_id)
        .execution_options(synchronize_session=False)
    ).scalar_one_or_none()
    return removed == job.workspace_id


for _operation in (
    "get",
    "find_pending",
    "current",
    "register",
    "pin",
    "retire",
    "transition",
    "finish",
):
    declare_growth(
        catalog_declaration(
            "riverhog_core.services.primary_facts_jobs." + _operation, "job-" + _operation
        )
    )(globals()[_operation])
del _operation

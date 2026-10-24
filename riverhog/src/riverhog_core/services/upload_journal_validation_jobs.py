"Source-fenced catalog ownership for retained native journal validation."

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any
from uuid import uuid4

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance import JournalValidationProgress
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal

from . import collection_uploads as source_owner


def _job_declaration(mode: str) -> Declaration:
    sql = {
        "select_job": 4,
        "claim_worker": 2,
        "worker_current": 1,
        "publish_progress": 2,
        "creation_current": 1,
        "retire_source": 1,
        "encode_progress": 0,
        "decode_progress": 0,
        "outcome": 0,
    }[mode]
    mutations = {"select_job": 2, "claim_worker": 1, "publish_progress": 1, "retire_source": 1}.get(
        mode, 0
    )
    versions = (
        2
        if mode == "select_job"
        else int(mode in {"claim_worker", "publish_progress", "retire_source"})
    )
    decodes = int(mode in {"decode_progress", "publish_progress", "outcome"})
    encodes = (
        3
        if mode == "publish_progress"
        else int(mode in {"encode_progress", "decode_progress", "outcome"})
    )

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    return Declaration(
        id="riverhog-server.validation-job-" + mode.replace("_", "-") + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.services.upload_journal_validation_jobs." + mode,),
        scope=(
            "exact source/worker catalog ownership and canonical detached progress/outcome metadata"
        ),
        starts_at="accept the selected source/job and complete proposed control values",
        ends_at=("close owned catalog resources and return detached metadata, or reject the call"),
        dimensions=(
            Dimension(
                "retained_rows",
                COUNT,
                "Independent total rows in the retained Job and Journal relations.",
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS,
                (
                    "Largest complete native encoded source/job row in the "
                    "admitted lookup and proposed update inputs, including both "
                    "bindings and all progress/outcome/failure text."
                ),
            ),
            Dimension(
                "control_octets",
                OCTETS,
                (
                    "Sum of complete incoming and previous binding, progress and "
                    "outcome canonical control widths, including rejected raw "
                    "texts."
                ),
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(sql),
                (
                    "Exact source selection and one job lookup/CAS; replacing a "
                    "selected old attempt may flush one orphan and insert one new"
                    " row."
                ),
            ),
            Bound(
                "driver_statements",
                COUNT,
                "upper",
                count(24 if sql else 0),
                (
                    "Includes enabled pool checks, native transaction control, "
                    "foreign-key checks and cold connection setup."
                ),
            ),
            Bound(
                "connection_statements",
                COUNT,
                "upper",
                count(20 if sql else 0),
                (
                    "The owned operation uses one bounded transaction/snapshot "
                    "context with maintenance enabled."
                ),
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(12 if sql else 0),
                (
                    "Native fetched rows include exact returning metadata and "
                    "connection-control results."
                ),
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                scale(12 if sql else 0, dimension("metadata_row_octets")),
                "Returned complete job rows retain full control widths.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                add(
                    scale(32 if sql else 0, dimension("metadata_row_octets")),
                    scale(4 if sql else 0, dimension("control_octets")),
                ),
                (
                    "Source and CAS predicates repeat only the complete admitted "
                    "controls and exact keys."
                ),
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                count(mutations),
                (
                    "The unique selected source or workspace identity bounds all "
                    "explicit row changes."
                ),
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                octets(0),
                (
                    "No documentary source, captured content, schema or native "
                    "database payload is fetched."
                ),
            ),
            Bound(
                "json_decodes",
                COUNT,
                "upper",
                count(decodes),
                (
                    "Only previous progress or the exact requested canonical "
                    "control document is decoded."
                ),
            ),
            Bound(
                "json_encodes",
                COUNT,
                "upper",
                count(encodes),
                (
                    "Previous progress validation, proposed progress and optional"
                    " outcome execute their actual canonical encoders."
                ),
            ),
            Bound(
                "json_decoded_octets",
                OCTETS,
                "upper",
                scale(decodes, dimension("control_octets")),
                "The complete incoming/previous control text reaches its codec.",
            ),
            Bound(
                "json_encoded_octets",
                OCTETS,
                "upper",
                scale(2 * encodes, dimension("control_octets")),
                "Canonical controls retain their complete scalar and string widths.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                octets(16 * versions),
                (
                    "Each new CAS version hashes one actual UUID value; replacing"
                    " an old attempt creates at most two versions."
                ),
            ),
            Bound(
                "uuid_values",
                COUNT,
                "upper",
                count(3 if mode == "select_job" else versions),
                (
                    "At most one new workspace identity and the actual new "
                    "version identities are generated."
                ),
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    count(8192 if sql else 0),
                    Expression(
                        "product",
                        terms=(
                            count(2048 if sql else 0),
                            Expression(
                                "log2_ceil", terms=(add(count(2), dimension("retained_rows")),)
                            ),
                        ),
                    ),
                ),
                (
                    "Fixed schema/transaction work and the maintained indexed "
                    "point queries include actual driver instructions."
                ),
            ),
        ),
        conditions=(
            (
                "The maintained SQLite Job and Journal schema, unique "
                "source/workspace indexes and foreign-key policy are present."
            ),
            (
                "The owning catalog Session factory disables automatic "
                "flushing; caller-owned retirement transactions have no "
                "unrelated pending changes attributed to this operation."
            ),
            (
                "Proposed controls come from the owning source, native "
                "progress or explicit failure input; observed accepted counts"
                " are never fabricated to manufacture a growth regime."
            ),
            (
                "A caller-owned retirement transaction retains its own commit"
                " and coordination costs; the operation does not "
                "independently commit or reopen the caller session."
            ),
        ),
        invariants=(
            (
                "Source, attempt, both bindings, worker epoch and CAS version"
                " stay exact; stale ownership cannot overwrite confirmed "
                "progress."
            ),
            (
                "Monotonicity and complete-outcome checks retain their "
                "existing order and reject before publication when required."
            ),
            (
                "A new attempt or source deletion retains the old "
                "derived-workspace cleanup identity; no native retained state"
                " is silently recreated."
            ),
        ),
        limitations=(
            (
                "Logical row, codec, hash and VM observations exclude "
                "physical I/O, in-kernel B-tree comparisons and "
                "machine-dependent latency."
            ),
            (
                "Native validation, kernel workspace custody, source/provider"
                " callbacks, service coordination and retirement retain their"
                " own composed costs."
            ),
        ),
    )


def _version() -> str:
    return hashlib.sha256(uuid4().bytes).hexdigest()


def encode_progress(progress: JournalValidationProgress) -> str:
    return canonical_json_bytes(
        {
            key: str(value) if type(value) is int else value
            for key, value in asdict(progress).items()
        }
    ).decode()


def decode_progress(raw: str) -> JournalValidationProgress:
    try:
        value = json.loads(raw)
        for field in (
            "worker_epoch",
            "version",
            "accepted_frames",
            "accepted_octets",
            "source_octets",
        ):
            value[field] = int(value[field])
        progress = JournalValidationProgress(**value)
        if type(progress.complete) is not bool or encode_progress(progress) != raw:
            raise ValueError("noncanonical progress")
        return progress
    except (TypeError, ValueError, KeyError) as error:
        raise RuntimeError("invalid retained native validation progress") from error


@dataclass(frozen=True, slots=True)
class ValidationJob:
    workspace_id: str
    collection_id: int | None
    journal_id: str | None
    attempt_id: str
    source_binding_json: str
    certificate_binding_json: str
    worker_epoch: int
    version: str
    state: str
    progress_json: str | None
    outcome_json: str | None
    failure: str | None

    @classmethod
    def read(cls, row: Job) -> ValidationJob:
        return cls(**{field: getattr(row, field) for field in cls.__dataclass_fields__})


def _identity(job: ValidationJob) -> tuple[Any, ...]:
    return (
        Job.workspace_id == job.workspace_id,
        Job.collection_id == job.collection_id,
        Job.journal_id == job.journal_id,
        Job.attempt_id == job.attempt_id,
        Job.source_binding_json == job.source_binding_json,
        Job.certificate_binding_json == job.certificate_binding_json,
        Job.worker_epoch == job.worker_epoch,
    )


def select_job(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    *,
    source_binding: str,
    certificate_binding: str,
) -> ValidationJob | None:
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return None
        row = session.scalar(
            select(Job).where(
                Job.collection_id == source.collection_id,
                Job.journal_id == source.journal_id,
            )
        )
        if row is not None and row.attempt_id != source.attempt_id:
            row.collection_id = row.journal_id = None
            row.state, row.version = "retiring", _version()
            session.flush()
            row = None
        if row is None:
            row = Job(
                workspace_id=uuid4().hex,
                collection_id=source.collection_id,
                journal_id=source.journal_id,
                attempt_id=source.attempt_id,
                source_binding_json=source_binding,
                certificate_binding_json=certificate_binding,
                worker_epoch=0,
                version=_version(),
                state="opening",
            )
            session.add(row)
            session.flush()
        if (row.source_binding_json, row.certificate_binding_json) != (
            source_binding,
            certificate_binding,
        ):
            raise RuntimeError("validation attempt inputs changed without a new owned attempt")
        return ValidationJob.read(row)


def claim_worker(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    job: ValidationJob,
) -> ValidationJob | None:
    "Claim only after acquiring the exact workspace's exclusive kernel lease."
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return None
        changed = session.execute(
            update(Job)
            .where(
                *_identity(job),
                Job.version == job.version,
                Job.progress_json == job.progress_json,
                Job.state.in_(("opening", "active")),
            )
            .values(version=_version(), worker_epoch=job.worker_epoch + 1)
            .returning(Job)
        ).scalar_one_or_none()
        return None if changed is None else ValidationJob.read(changed)


def worker_current(factory: SessionFactory, job: ValidationJob) -> bool:
    with read_snapshot(factory) as session:
        with session.execute(
            select(Job.workspace_id).where(
                *_identity(job),
                Job.state.in_(("opening", "active", "complete")),
            )
        ) as result:
            return result.one_or_none() is not None


def publish_progress(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    job: ValidationJob,
    progress: JournalValidationProgress | None,
    *,
    outcome: source_owner._UploadJournalValidationOutcome | None = None,
    lost: str | None = None,
) -> ValidationJob | None:
    "Publish detached metadata after the native commit under both owner fences."
    if progress is not None:
        if progress.attempt_id != job.workspace_id or progress.worker_epoch != job.worker_epoch:
            raise RuntimeError("native progress differs from the selected worker")
        if job.progress_json is not None:
            previous = decode_progress(job.progress_json)
            if (
                progress.version < previous.version
                or progress.accepted_frames < previous.accepted_frames
                or progress.accepted_octets < previous.accepted_octets
                or progress.source_octets < previous.source_octets
                or (previous.complete and not progress.complete)
                or (
                    progress.accepted_frames == previous.accepted_frames
                    and (progress.accepted_octets, progress.accepted_sha256)
                    != (previous.accepted_octets, previous.accepted_sha256)
                )
            ):
                raise RuntimeError("retained native progress moved behind its confirmed prefix")
    state = "lost" if lost is not None else "active"
    if outcome is not None:
        state = "failed" if outcome.failure is not None else "complete"
        if state == "complete" and (progress is None or not progress.complete):
            raise RuntimeError("successful validation requires complete native progress")
    encoded_outcome = None
    if outcome is not None:
        encoded_outcome = canonical_json_bytes(
            {
                key: str(value) if type(value) is int else value
                for key, value in asdict(outcome).items()
            }
        ).decode()
    with factory() as session, session.begin():
        if (
            session.scalar(
                select(Journal.collection_id)
                .where(*source_owner._upload_journal_validation_fence(source))
                .with_for_update(skip_locked=True)
            )
            is None
        ):
            return None
        changed = session.execute(
            update(Job)
            .where(
                *_identity(job),
                Job.version == job.version,
                Job.progress_json == job.progress_json,
                Job.state.in_(("opening", "active")),
            )
            .values(
                version=_version(),
                state=state,
                progress_json=job.progress_json if progress is None else encode_progress(progress),
                outcome_json=encoded_outcome,
                failure=lost
                if lost is not None
                else (None if outcome is None else outcome.failure),
            )
            .returning(Job)
        ).scalar_one_or_none()
        return None if changed is None else ValidationJob.read(changed)


def outcome(job: ValidationJob) -> source_owner._UploadJournalValidationOutcome:
    if job.state not in {"complete", "failed"} or job.outcome_json is None:
        raise RuntimeError("retained validation has no terminal outcome")
    try:
        value = json.loads(job.outcome_json)
        for field in ("frame_count", "terminal_sequence"):
            if value[field] is not None:
                value[field] = int(value[field])
        result = source_owner._UploadJournalValidationOutcome(**value)
        encoded = canonical_json_bytes(
            {
                key: str(value) if type(value) is int else value
                for key, value in asdict(result).items()
            }
        ).decode()
        if encoded != job.outcome_json:
            raise ValueError("noncanonical outcome")
        return result
    except (TypeError, ValueError, KeyError) as error:
        raise RuntimeError("invalid retained validation outcome") from error


def creation_current(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    job: ValidationJob,
) -> bool:
    query = (
        select(Job.workspace_id)
        .join(
            Journal,
            (Job.collection_id == Journal.collection_id) & (Job.journal_id == Journal.journal_id),
        )
        .where(
            *_identity(job),
            Job.version == job.version,
            Job.state == "opening",
            *source_owner._upload_journal_validation_fence(source),
        )
    )
    with read_snapshot(factory) as session:
        with session.execute(query) as rows:
            return rows.one_or_none() is not None


def retire_source(session: Session, collection_id: int, journal_id: str, attempt_id: str) -> None:
    session.execute(
        update(Job)
        .where(
            Job.collection_id == collection_id,
            Job.journal_id == journal_id,
            Job.attempt_id == attempt_id,
        )
        .values(
            collection_id=None, journal_id=None, state="retiring", version=_version(), failure=None
        )
    ).close()


declare_growth(_job_declaration("select_job"))(select_job)


declare_growth(_job_declaration("claim_worker"))(claim_worker)


declare_growth(_job_declaration("worker_current"))(worker_current)


declare_growth(_job_declaration("publish_progress"))(publish_progress)


declare_growth(_job_declaration("creation_current"))(creation_current)


declare_growth(_job_declaration("retire_source"))(retire_source)


declare_growth(_job_declaration("encode_progress"))(encode_progress)


declare_growth(_job_declaration("decode_progress"))(decode_progress)


declare_growth(_job_declaration("outcome"))(outcome)

"Source-fenced catalog ownership for retained native journal validation."

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from typing import Any, Literal
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
from riverhog_protocol.errors import Conflict
from riverhog_provenance import (
    JournalConstruction,
    JournalImageCapture,
    JournalValidationObservation,
)
from sqlalchemy import select, update
from sqlalchemy.orm import Session, SessionTransaction
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.collection_recovery_state import RecoveryGrant
from riverhog_core.journal_recovery_frontier import (
    JournalGenerationReceipt,
    JournalRecoveryContext,
    JournalResumeFrontier,
)

from . import collection_uploads as source_owner


def _job_declaration(mode: str) -> Declaration:
    sql = {
        "select_job": 4,
        "select_resume_frontier": 3,
        "retain_recovery_owner": 2,
        "retain_recovery_original": 1,
        "receive_recovery_original": 1,
        "claim_worker": 2,
        "activate_worker": 2,
        "worker_current": 2,
        "publish_outcome": 2,
        "creation_current": 2,
        "retire_source": 1,
        "outcome": 0,
    }[mode]
    mutations = {
        "select_job": 2,
        "select_resume_frontier": 1,
        "receive_recovery_original": 1,
        "claim_worker": 1,
        "activate_worker": 1,
        "publish_outcome": 1,
        "retire_source": 1,
    }.get(mode, 0)
    versions = (
        2
        if mode == "select_job"
        else int(
            mode
            in {
                "claim_worker",
                "activate_worker",
                "publish_outcome",
                "retire_source",
                "select_resume_frontier",
            }
        )
    )
    decodes = (
        6
        if mode == "select_resume_frontier"
        else 2
        if mode == "retain_recovery_original"
        else int(mode == "outcome")
    )
    encodes = (
        12
        if mode == "select_resume_frontier"
        else 4
        if mode == "retain_recovery_original"
        else int(mode in {"publish_outcome", "outcome"})
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
        scope=("exact source/worker catalog ownership and canonical mandatory outcome metadata"),
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
                    "bindings and all outcome/failure text."
                ),
            ),
            Dimension(
                "control_octets",
                OCTETS,
                (
                    "Sum of complete binding/outcome text widths and incoming "
                    "live observation scalar widths, including rejected controls."
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
                    " row. Read-only currentness includes one explicit snapshot start."
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
                ("Only exact mandatory outcome or selected Native control metadata is decoded."),
            ),
            Bound(
                "json_encodes",
                COUNT,
                "upper",
                count(encodes),
                (
                    "Publishing or validating a mandatory outcome executes its "
                    "actual canonical encoder; selected Native envelope metadata retains its own "
                    "codec work. Live progress is never encoded."
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
                add(
                    octets(16 * versions),
                    scale(
                        4 if mode in {"select_resume_frontier", "retain_recovery_original"} else 0,
                        dimension("control_octets"),
                    ),
                ),
                (
                    "Native envelope and owner hashes use complete admitted controls. "
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
                "Source, attempt, both bindings, worker epoch and CAS version "
                "stay exact; stale ownership cannot overwrite a confirmed outcome."
            ),
            (
                "Worker identity precedes selection; successful-completion checks precede "
                "terminal publication. "
                "Active rows contain no terminal outcome or volatile processed cursor."
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
    outcome_json: str | None
    failure: str | None
    resume_frontier_json: str | None = None
    recovery_receipt_json: str | None = None

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
                Job.outcome_json == job.outcome_json,
                Job.state.in_(("opening", "active")),
            )
            .values(version=_version(), worker_epoch=job.worker_epoch + 1)
            .returning(Job)
        ).scalar_one_or_none()
        return None if changed is None else ValidationJob.read(changed)


def activate_worker(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    job: ValidationJob,
) -> ValidationJob | None:
    """Receive the original workspace opening; publish no semantic replay cursor."""
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
                Job.state == "opening",
                Job.outcome_json.is_(None),
            )
            .values(version=_version(), state="active")
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


def retain_recovery_owner(
    session: Session,
    source: source_owner._UploadJournalValidationWork,
    job: ValidationJob,
) -> ValidationJob:
    """Retain the exact source and worker after the surviving root lock."""
    if (source.collection_id, source.journal_id, source.attempt_id) != (
        job.collection_id,
        job.journal_id,
        job.attempt_id,
    ):
        raise Conflict("journal source differs from its original job")
    if (
        session.scalar(
            select(Journal.collection_id)
            .where(*source_owner._upload_journal_validation_fence(source))
            .with_for_update(skip_locked=True)
        )
        is None
    ):
        raise Conflict("journal source is no longer current")
    row = session.scalar(select(Job).where(*_identity(job)).with_for_update())
    if row is None or row.state != "active" or row.outcome_json is not None:
        raise Conflict("journal worker is no longer current")
    return ValidationJob.read(row)


def select_resume_frontier(
    session: Session,
    source: source_owner._UploadJournalValidationWork,
    job: ValidationJob,
    grant: RecoveryGrant,
    frontier: JournalResumeFrontier,
    *,
    generation: str,
    control_bytes: int,
) -> ValidationJob:
    """Select only inside the surviving recovery root's control-first transaction.

    Native persistence and resource receiving occur outside this transaction.
    The returned job replaces the caller's CAS snapshot before terminal writes.
    """
    if generation != frontier.generation:
        raise Conflict("journal accounting generation differs from its Native envelope")
    if type(control_bytes) is not int or control_bytes < 1:
        raise ValueError("journal frontier requires positive metadata admission")
    if len(frontier.encoded.encode("utf-8")) > control_bytes:
        raise ValueError("journal resume frontier exceeds metadata admission")
    expected = JournalRecoveryContext.original(frontier.context.binding, job, grant)
    if frontier.context != expected:
        raise Conflict("journal generation differs from its original source/worker")
    if grant.generation is not None and grant.generation != frontier.generation:
        raise Conflict("journal grant selected a different Native envelope")
    current = retain_recovery_owner(session, source, job)
    if grant.generation is not None:
        if current.resume_frontier_json != frontier.encoded:
            if current.resume_frontier_json is None:
                raise Conflict("original selected journal frontier is missing")
            newer = JournalResumeFrontier.from_json(
                current.resume_frontier_json, control_bytes=control_bytes
            )
            before = frontier.context
            after = newer.context
            # Only a positively recorded, newer selection of this exact owner
            # can supersede the original. A lost reply never writes old bytes.
            prefix = before.binding.grant_prefix
            if (
                newer.image.binding != frontier.image.binding
                or after.binding != before.binding
                or after.workspace_id != before.workspace_id
                or after.journal_id != before.journal_id
                or after.attempt_id != before.attempt_id
                or after.source_binding_json != before.source_binding_json
                or after.certificate_binding_json != before.certificate_binding_json
                or after.worker_epoch != before.worker_epoch
                or int(after.grant_identity[len(prefix) :])
                <= int(before.grant_identity[len(prefix) :])
            ):
                raise Conflict("journal selected frontier contradicts the original receipt")
        return current
    changed = session.execute(
        update(Job)
        .where(
            *_identity(job),
            Job.version == job.version,
            Job.resume_frontier_json == job.resume_frontier_json,
            Job.state == "active",
            Job.outcome_json.is_(None),
        )
        .values(version=_version(), resume_frontier_json=frontier.encoded)
        .returning(Job)
        .execution_options(populate_existing=True)
    ).scalar_one_or_none()
    if changed is None:
        raise Conflict("journal frontier changed before selection")
    return ValidationJob.read(changed)


def _outcome_document(
    job: ValidationJob, result: source_owner._UploadJournalValidationOutcome
) -> dict[str, Any]:
    return {
        "workspace_id": job.workspace_id,
        "worker_epoch": str(job.worker_epoch),
        "source_binding_json": job.source_binding_json,
        "certificate_binding_json": job.certificate_binding_json,
        "result": {
            key: str(value) if type(value) is int else value
            for key, value in asdict(result).items()
        },
    }


def publish_outcome(
    factory: SessionFactory,
    source: source_owner._UploadJournalValidationWork,
    job: ValidationJob,
    progress: JournalValidationObservation | None,
    *,
    outcome: source_owner._UploadJournalValidationOutcome | None = None,
    lost: str | None = None,
) -> ValidationJob | None:
    """Publish an exact terminal observation/outcome under both current fences."""
    if (outcome is None) == (lost is None):
        raise RuntimeError("Native publication requires exactly one terminal outcome or loss")
    if job.state in {"opening", "active"} and job.outcome_json is not None:
        raise RuntimeError("active Native work cannot retain a terminal outcome")
    if progress is not None and (
        progress.attempt_id != job.workspace_id or progress.worker_epoch != job.worker_epoch
    ):
        raise RuntimeError("native terminal observation differs from the selected worker")
    if lost is not None and progress is not None:
        raise RuntimeError("unavailable Native work cannot publish a terminal observation")
    if outcome is not None:
        if progress is None:
            raise RuntimeError("Native outcome requires its original terminal observation")
        if outcome.failure is None and not progress.complete:
            raise RuntimeError("successful validation requires complete native progress")
    state = "lost"
    if outcome is not None:
        state = "failed" if outcome.failure is not None else "complete"
    encoded_outcome = None
    if outcome is not None:
        encoded_outcome = canonical_json_bytes(_outcome_document(job, outcome)).decode()
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
                Job.outcome_json == job.outcome_json,
                Job.state.in_(("opening", "active")),
            )
            .values(
                version=_version(),
                state=state,
                outcome_json=encoded_outcome,
                failure=lost
                if lost is not None
                else (None if outcome is None else outcome.failure),
            )
            .returning(Job)
        ).scalar_one_or_none()
        return None if changed is None else ValidationJob.read(changed)


def outcome(job: ValidationJob) -> source_owner._UploadJournalValidationOutcome:
    """Read the canonical mandatory outcome from this exact original producer."""
    if job.state not in {"complete", "failed"} or job.outcome_json is None:
        raise RuntimeError("retained validation has no terminal outcome")
    try:
        receipt = json.loads(job.outcome_json)
        if (
            type(receipt) is not dict
            or set(receipt)
            != {
                "workspace_id",
                "worker_epoch",
                "source_binding_json",
                "certificate_binding_json",
                "result",
            }
            or receipt["workspace_id"] != job.workspace_id
            or receipt["worker_epoch"] != str(job.worker_epoch)
            or receipt["source_binding_json"] != job.source_binding_json
            or receipt["certificate_binding_json"] != job.certificate_binding_json
        ):
            raise ValueError("terminal outcome differs from its original Native producer")
        value = receipt["result"]
        for field in ("frame_count", "terminal_sequence"):
            if value[field] is not None:
                value[field] = int(value[field])
        result = source_owner._UploadJournalValidationOutcome(**value)
        encoded = canonical_json_bytes(_outcome_document(job, result)).decode()
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
declare_growth(_job_declaration("activate_worker"))(activate_worker)


declare_growth(_job_declaration("worker_current"))(worker_current)


declare_growth(_job_declaration("publish_outcome"))(publish_outcome)


declare_growth(_job_declaration("creation_current"))(creation_current)


declare_growth(_job_declaration("retire_source"))(retire_source)


declare_growth(_job_declaration("outcome"))(outcome)

declare_growth(_job_declaration("select_resume_frontier"))(select_resume_frontier)

declare_growth(_job_declaration("retain_recovery_owner"))(retain_recovery_owner)


def retain_recovery_original(
    session: Session,
    receipt: JournalGenerationReceipt,
    grant: RecoveryGrant | None,
    *,
    control_bytes: int,
) -> ValidationJob:
    """Retain receiving truth without granting a historical worker new work.

    Called after the surviving root lock and the caller's current permission
    guard. Source retirement and a newer worker cannot erase original costs.
    """
    if type(control_bytes) is not int or control_bytes < 1:
        raise ValueError("journal receiving requires positive metadata admission")
    if len(receipt.encoded.encode("utf-8")) > control_bytes:
        raise ValueError("journal generation receipt exceeds metadata admission")
    context = receipt.context
    row = session.scalar(
        select(Job).where(Job.workspace_id == context.workspace_id).with_for_update()
    )
    if row is None:
        raise Conflict("original journal receiving owner is unavailable")
    current = ValidationJob.read(row)
    if (
        current.attempt_id != context.attempt_id
        or current.source_binding_json != context.source_binding_json
        or current.certificate_binding_json != context.certificate_binding_json
        or current.collection_id not in (None, context.binding.collection_id)
        or current.journal_id not in (None, context.journal_id)
        or current.worker_epoch < context.worker_epoch
    ):
        raise Conflict("journal receiving differs from its immutable original owner")
    if grant is None:
        if current.recovery_receipt_json != receipt.encoded:
            raise Conflict("journal receiving contradicts its immutable original receipt")
    else:
        historical = replace(
            current,
            collection_id=context.binding.collection_id,
            journal_id=context.journal_id,
            worker_epoch=context.worker_epoch,
        )
        if (
            JournalRecoveryContext.original(context.binding, historical, grant) != context
            or grant.generation != receipt.generation
        ):
            raise Conflict("journal receiving has no matching selected original grant")
        previous = current.recovery_receipt_json
        if previous is not None and previous != receipt.encoded:
            original = JournalGenerationReceipt.from_json(previous, control_bytes=control_bytes)
            before, after = original.context, context
            prefix = context.binding.grant_prefix
            if (
                before.binding != after.binding
                or before.workspace_id != after.workspace_id
                or before.journal_id != after.journal_id
                or before.attempt_id != after.attempt_id
                or before.source_binding_json != after.source_binding_json
                or before.certificate_binding_json != after.certificate_binding_json
                or before.worker_epoch > after.worker_epoch
                or int(before.grant_identity[len(prefix) :])
                >= int(after.grant_identity[len(prefix) :])
            ):
                raise Conflict("journal receiving would replace a conflicting original receipt")
    return current


def receive_recovery_original(
    session: Session,
    current: ValidationJob,
    receipt: JournalGenerationReceipt,
    grant: RecoveryGrant | None,
) -> Literal["published"]:
    """Record one exact receipt in the same transaction as root cost receiving."""
    if current.recovery_receipt_json == receipt.encoded:
        return "published"
    if grant is None:
        raise Conflict("original journal receipt cannot be manufactured after receiving")
    if grant.identity != receipt.context.grant_identity or grant.generation != receipt.generation:
        raise Conflict("journal receipt differs from its retained selected grant")
    changed = session.execute(
        update(Job)
        .where(
            *_identity(current),
            Job.recovery_receipt_json == current.recovery_receipt_json,
        )
        .values(recovery_receipt_json=receipt.encoded)
        .returning(Job.workspace_id)
    ).scalar_one_or_none()
    if changed != current.workspace_id:
        raise Conflict("original journal receiving owner changed")
    # Cost receiving owns this independent receipt CAS. It cannot invalidate a
    # still-live semantic job version or alter its frontier/terminal outcome.
    return "published"


class JournalGenerationReceiving:
    """One exact original callback pair for the surviving root transaction."""

    def __init__(
        self,
        receipt: JournalGenerationReceipt,
        *,
        control_bytes: int,
        capture: JournalImageCapture | None = None,
        construction: JournalConstruction | None = None,
    ) -> None:
        if (capture is None) != (construction is None):
            raise TypeError("journal receiving requires both exact Native originals")
        self.receipt, self.control_bytes = receipt, control_bytes
        self.capture, self.construction = capture, construction
        self._session: Session | None = None
        self._transaction: SessionTransaction | None = None
        self._job: ValidationJob | None = None
        self._grant: RecoveryGrant | None = None

    def retain(self, session: Session, grant: RecoveryGrant | None) -> None:
        self._session = None
        self._transaction = None
        if grant is not None:
            if self.capture is None or self.construction is None:
                raise Conflict("original Native receiving proof is unavailable")
            image = self.capture.received_image(self.construction)
            if (
                image.envelope_sha256 != self.receipt.generation
                or image.owner_context != self.receipt.context.encoded
            ):
                raise Conflict("Native receiving differs from the original Core receipt")
        self._job = retain_recovery_original(
            session, self.receipt, grant, control_bytes=self.control_bytes
        )
        self._grant, self._session = grant, session
        self._transaction = session.get_transaction()

    def receive(
        self, session: Session, identity: str, total_actual_ns: int
    ) -> Literal["published"]:
        try:
            if (
                self._session is not session
                or self._transaction is None
                or session.get_transaction() is not self._transaction
                or not self._transaction.is_active
                or self._job is None
                or identity != self.receipt.context.grant_identity
                or total_actual_ns != self.receipt.total_actual_ns
            ):
                raise Conflict("journal receiving lacks its same-transaction original retention")
            return receive_recovery_original(session, self._job, self.receipt, self._grant)
        finally:
            self._session = self._job = self._grant = None
            self._transaction = None


declare_growth(_job_declaration("retain_recovery_original"))(retain_recovery_original)
declare_growth(_job_declaration("receive_recovery_original"))(receive_recovery_original)

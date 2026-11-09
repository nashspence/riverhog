"""Current catalog custody of a retained discovery build and fixed Native inputs."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, fields, replace
from typing import Any
from uuid import uuid4

from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance import CorpusReplayBinding
from riverhog_provenance_contracts import ContractCatalog, core_contract
from sqlalchemy import Select, exists, select, update
from sqlalchemy.orm import Session

from riverhog_core.canonical_discovery_archive_work import (
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
)
from riverhog_core.canonical_discovery_index import EXTRACTION_CONTRACT_SHA256
from riverhog_core.canonical_discovery_selection import FrozenCorpusSelection
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as UploadControl
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Job
from riverhog_core.catalog_models import CollectionDeletionRecord as Deletion
from riverhog_core.catalog_models import CollectionRecord as Collection
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as State,
)

_CORE_CONTRACT_SHA256 = core_contract().contract_sha256


@dataclass(frozen=True, slots=True)
class DiscoveryJob:
    workspace_id: str
    build_id: str | None
    original_build_id: str
    attempt_id: str
    admission_sha256: str
    archive_generation: str
    archive_root_sha256: str
    provenance_identity: str
    artifact_set_identity: str
    journal_count: str
    build_epoch: int
    policy_sha256: str
    catalog_sha256: str
    worker_epoch: int
    version: str
    state: str
    source_manifest_sha256: str | None
    source_count: str | None
    selection_manifest_sha256: str | None
    selection_count: str | None
    failure: str | None
    reclamation_phase: str | None = None
    storage_state: str = "unconfirmed"
    reclamation_journal_id: str | None = None
    reclamation_directory_id: str | None = None

    @classmethod
    def read(cls, row: Job) -> DiscoveryJob:
        return cls(**{field.name: getattr(row, field.name) for field in fields(cls)})

    @property
    def key(self) -> ArchiveTaskKey:
        if self.build_id is None:
            raise StaleArchiveTask("retired discovery job has no current build")
        return ArchiveTaskKey(self.build_id, self.admission_sha256, self.attempt_id)

    @property
    def storage_key(self) -> ArchiveTaskKey:
        """Original owned storage labels, without current source or disposal authority."""
        return ArchiveTaskKey(self.original_build_id, self.admission_sha256, self.attempt_id)

    @property
    def binding(self) -> CorpusReplayBinding:
        if self.source_manifest_sha256 is None or self.selection_manifest_sha256 is None:
            raise StaleArchiveTask("discovery job has no fixed original Native inputs")
        try:
            counts = [
                parse_scalar("nonnegative", raw)
                for raw in (self.source_count, self.selection_count)
            ]
        except ValueError:
            raise StaleArchiveTask("discovery job has invalid original input counts") from None
        return CorpusReplayBinding(
            self.attempt_id,
            self.workspace_id,
            self.source_manifest_sha256,
            counts[0],
            self.selection_manifest_sha256,
            counts[1],
            self.policy_sha256,
            self.catalog_sha256,
        )


def _version() -> str:
    return hashlib.sha256(uuid4().bytes).hexdigest()


def _build_query(job: DiscoveryJob) -> Select[tuple[str]]:
    return (
        select(Generation.build_id)
        .join(Collection, Collection.id == Generation.collection_id)
        .join(State, State.collection_id == Collection.id)
        .where(
            Generation.build_id == job.build_id,
            Generation.expected_epoch == job.build_epoch,
            Generation.complete.is_(False),
            Generation.archive_generation == job.archive_generation,
            Generation.archive_root_sha256 == job.archive_root_sha256,
            Generation.provenance_identity == job.provenance_identity,
            Generation.core_contract_sha256 == _CORE_CONTRACT_SHA256,
            Generation.extraction_contract_sha256 == job.policy_sha256,
            Collection.archive_generation == job.archive_generation,
            Collection.archive_root_sha256 == job.archive_root_sha256,
            Collection.provenance_identity == job.provenance_identity,
            Collection.artifact_set_identity == job.artifact_set_identity,
            State.pending_build_id == Generation.build_id,
            State.epoch == job.build_epoch,
            ~exists(select(Deletion.collection_id).where(Deletion.collection_id == Collection.id)),
            ~exists(
                select(UploadControl.collection_id).where(
                    UploadControl.collection_id == Collection.id, UploadControl.gate != "open"
                )
            ),
        )
    )


def _current_build(session: Session, job: DiscoveryJob, *, lock: bool) -> bool:
    query = _build_query(job)
    if lock:
        query = query.with_for_update(of=(Collection, State), skip_locked=True)
    with session.execute(query) as rows:
        return rows.one_or_none() is not None


def _identity(job: DiscoveryJob) -> tuple[Any, ...]:
    return tuple(
        getattr(Job, field.name) == getattr(job, field.name)
        for field in fields(job)
        if field.name not in {"version", "state", "failure", "storage_state"}
    )


def _declaration(mode: str) -> Declaration:
    sql = {
        "select_discovery_job": 4,
        "claim_discovery_worker": 2,
        "confirm_discovery_storage": 3,
        "discovery_worker_current": 1,
        "discovery_worker_snapshot": 1,
        "pin_discovery_corpus": 2,
        "activate_discovery_corpus": 2,
        "lose_discovery_corpus": 1,
        "retire_discovery_job": 1,
        "advance_discovery_reclamation": 1,
    }[mode]
    mutates = int(mode not in {"discovery_worker_current", "discovery_worker_snapshot"})
    versions = mutates

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def width(value: int, name: str = "metadata_row_octets") -> Expression:
        return Expression(
            "product",
            terms=(Expression("constant", value=value), Expression("dimension", name=name)),
        )

    return Declaration(
        id="riverhog-server.discovery-job-" + mode.replace("_", "-") + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_corpus_jobs." + mode,),
        scope="one live build/job fence, claimed worker or fixed-input catalog mutation",
        starts_at="receive exact admitted root, selected job and complete proposed controls",
        ends_at="close catalog resources and detach the result or preserve rejection",
        dimensions=(
            Dimension("retained_rows", COUNT, "Rows across relevant catalog and job indexes."),
            Dimension(
                "metadata_row_octets",
                OCTETS,
                "Largest complete native row or proposed control, including original co"
                "unts/failure.",
            ),
            Dimension(
                "control_octets", OCTETS, "Complete original incoming job and proposed values."
            ),
        ),
        bounds=(
            Bound("sql_statements", COUNT, "upper", count(sql), "Fixed original-key reads/CAS."),
            Bound(
                "driver_statements",
                COUNT,
                "upper",
                count(sql + 32),
                "Includes cold native connection, transactions, pool checks and foreign keys.",
            ),
            Bound(
                "connection_statements",
                COUNT,
                "upper",
                count(32),
                "One owned catalog connection and bounded transaction setup/close.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(16),
                "Point currentness and optional complete job RETURNING, including driver controls.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                width(16),
                "Returned metadata retains complete physical row widths.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                Expression("sum", terms=(width(64), width(4, "control_octets"))),
                "Exact original keys and full incoming metadata are repeated a fixed nu"
                "mber of times.",
            ),
            Bound(
                "changed_rows", COUNT, "upper", count(mutates), "At most one job row is changed."
            ),
            Bound(
                "uuid_values",
                COUNT,
                "upper",
                count(3 if mode == "select_discovery_job" else versions),
                "Creation generates two ownership identities and one fresh version.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                octets(16 * versions),
                "Only the new version's UUID bytes are hashed; catalog identity is already sealed.",
            ),
            *(
                Bound(metric, COUNT, "equal", count(0), "No JSON document is loaded or encoded.")
                for metric in ("json_decodes", "json_encodes")
            ),
            *(
                Bound(metric, OCTETS, "equal", octets(0), "No JSON or archived payload is loaded.")
                for metric in (
                    "json_decoded_octets",
                    "json_encoded_octets",
                    "source_payload_octets",
                )
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(32768),
                        Expression(
                            "product",
                            terms=(
                                count(2048),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="retained_rows"),
                                                count(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Point-key joins/CAS plus fixed cold schema setup; no preceding-row traversal.",
            ),
        ),
        conditions=(
            "The maintained current schema and SQLite backend have no added triggers.",
            "Each supplied archive admission retains its original root authentication.",
            "Catalog identity was computed by its constructed sealed ContractCatalog.",
            "Worker claims/input pinning/retirement are called under the exact excl"
            "usive kernel lease.",
            "Pinned inputs come from the independent original-source and selection freeze owners.",
        ),
        invariants=(
            "Catalog archive root, pending build and epoch remain live authority.",
            "Stable attempt identity is separate from each replaceable claimed worker epoch.",
            "Original source count is independent of Native output and cannot change within a pin.",
            "Generation deletion leaves a retained job without current source authority.",
            "Catalog dispositions neither recreate missing state nor delete retained files.",
        ),
        limitations=(
            "SQLite VM excludes internal B-tree comparisons, physical I/O and lock wait.",
            "Kernel lease, archive authentication, Native execution and reclamation"
            " retain their owners.",
            "The catalog job is build custody, not an independent query/publication lifecycle.",
        ),
    )


@declare_growth(_declaration("select_discovery_job"))
def select_discovery_job(
    factory: SessionFactory,
    *,
    build_id: str,
    admission: ArchiveDiscoveryAdmission,
    catalog: ContractCatalog,
) -> DiscoveryJob | None:
    """Select one exact build attempt without granting execution or creating storage."""
    catalog_sha256 = catalog.checkpoint_sha256()
    with factory() as session, session.begin():
        # Selection locks the same collection/state rows as publication and supersession.
        query = (
            select(Generation)
            .join(Collection, Collection.id == Generation.collection_id)
            .join(State, State.collection_id == Collection.id)
            .where(
                Generation.build_id == build_id,
                Generation.complete.is_(False),
                Generation.archive_generation == admission.root.archive_generation,
                Generation.archive_root_sha256 == admission.archive_sha256,
                Generation.provenance_identity == admission.provenance_sha256,
                Generation.core_contract_sha256 == _CORE_CONTRACT_SHA256,
                Generation.extraction_contract_sha256 == EXTRACTION_CONTRACT_SHA256,
                Collection.archive_generation == Generation.archive_generation,
                Collection.archive_root_sha256 == Generation.archive_root_sha256,
                Collection.provenance_identity == Generation.provenance_identity,
                Collection.artifact_set_identity == admission.root.artifact_set_sha256,
                State.pending_build_id == Generation.build_id,
                State.epoch == Generation.expected_epoch,
                ~exists(
                    select(Deletion.collection_id).where(Deletion.collection_id == Collection.id)
                ),
                ~exists(
                    select(UploadControl.collection_id).where(
                        UploadControl.collection_id == Collection.id, UploadControl.gate != "open"
                    )
                ),
            )
            .with_for_update(of=(Collection, State), skip_locked=True)
        )
        build = session.scalar(query)
        if build is None:
            return None
        row = session.scalar(select(Job).where(Job.build_id == build_id))
        if row is None:
            row = Job(
                workspace_id=uuid4().hex,
                build_id=build_id,
                original_build_id=build_id,
                attempt_id=uuid4().hex,
                admission_sha256=admission.identity,
                archive_generation=admission.root.archive_generation,
                archive_root_sha256=admission.archive_sha256,
                provenance_identity=admission.provenance_sha256,
                artifact_set_identity=admission.root.artifact_set_sha256,
                journal_count=str(admission.root.journal_count),
                build_epoch=build.expected_epoch,
                policy_sha256=EXTRACTION_CONTRACT_SHA256,
                catalog_sha256=catalog_sha256,
                worker_epoch=0,
                version=_version(),
                state="preparing",
            )
            session.add(row)
            session.flush()
        result = DiscoveryJob.read(row)
        if (
            result.admission_sha256 != admission.identity
            or result.journal_count != str(admission.root.journal_count)
            or result.catalog_sha256 != catalog_sha256
            or result.policy_sha256 != EXTRACTION_CONTRACT_SHA256
            or not _current_build(session, result, lock=False)
        ):
            raise StaleArchiveTask("discovery attempt inputs changed without a new owned attempt")
        return result


@declare_growth(_declaration("claim_discovery_worker"))
def claim_discovery_worker(factory: SessionFactory, job: DiscoveryJob) -> DiscoveryJob | None:
    """Claim only while holding the selected workspace's exclusive kernel lease."""
    with factory() as session, session.begin():
        if not _current_build(session, job, lock=True):
            return None
        row = session.execute(
            update(Job)
            .where(
                *_identity(job),
                Job.version == job.version,
                Job.state.in_(("preparing", "seeding", "ready", "active")),
            )
            .values(worker_epoch=job.worker_epoch + 1, version=_version())
            .returning(Job)
        ).scalar_one_or_none()
        return None if row is None else DiscoveryJob.read(row)


@declare_growth(_declaration("discovery_worker_current"))
def discovery_worker_current(factory: SessionFactory, job: DiscoveryJob) -> bool:
    """Read the live catalog fence and exact selected worker in one indexed snapshot."""
    query = (
        _build_query(job)
        .join(Job, Job.build_id == Generation.build_id)
        .where(
            *_identity(job),
            Job.state.in_(("preparing", "seeding", "ready", "active")),
        )
    )
    with factory() as session, session.execute(query) as rows:
        return rows.one_or_none() is not None


@declare_growth(_declaration("discovery_worker_snapshot"))
def discovery_worker_snapshot(factory: SessionFactory, job: DiscoveryJob) -> DiscoveryJob | None:
    """Read the actual same-worker disposition before granting storage creation."""
    query = (
        _build_query(job)
        .with_only_columns(Job, maintain_column_froms=True)
        .join(Job, Job.build_id == Generation.build_id)
        .where(
            *_identity(job),
            Job.state.in_(("preparing", "seeding", "ready", "active")),
        )
    )
    with factory() as session:
        row = session.scalar(query)
        return None if row is None else DiscoveryJob.read(row)


@declare_growth(_declaration("confirm_discovery_storage"))
def confirm_discovery_storage(factory: SessionFactory, job: DiscoveryJob) -> DiscoveryJob | None:
    """Confirm validated Core storage under exclusive custody before advancing archive work."""
    if job.worker_epoch < 1:
        raise StaleArchiveTask("Core storage requires a claimed discovery worker")
    with factory() as session, session.begin():
        if not _current_build(session, job, lock=True):
            return None
        current = session.scalar(
            select(Job).where(
                *_identity(job), Job.state.in_(("preparing", "seeding", "ready", "active"))
            )
        )
        if current is None:
            return None
        if current.state != "preparing":
            return DiscoveryJob.read(current)
        if current.version != job.version:
            return None
        row = session.execute(
            update(Job)
            .where(*_identity(job), Job.version == job.version, Job.state == "preparing")
            .values(state="seeding", storage_state="confirmed", version=_version())
            .returning(Job)
        ).scalar_one_or_none()
        return None if row is None else DiscoveryJob.read(row)


@declare_growth(_declaration("pin_discovery_corpus"))
def pin_discovery_corpus(
    factory: SessionFactory, job: DiscoveryJob, manifest: FrozenCorpusSelection
) -> DiscoveryJob | None:
    """Pin original independent counts once, under the current claimed worker and lease."""
    if (
        job.worker_epoch < 1
        or manifest.sources.admission_sha256 != job.admission_sha256
        or format_scalar("nonnegative", manifest.sources.source_count) != job.journal_count
    ):
        raise StaleArchiveTask("original corpus inputs have another claimed archive attempt")
    proposed = replace(
        job,
        source_manifest_sha256=manifest.sources.manifest_sha256,
        source_count=format_scalar("nonnegative", manifest.sources.source_count),
        selection_manifest_sha256=manifest.selection_sha256,
        selection_count=format_scalar("nonnegative", manifest.selection_count),
    )
    _ = proposed.binding
    with factory() as session, session.begin():
        if not _current_build(session, job, lock=True):
            return None
        if job.source_manifest_sha256 is not None:
            if job.binding != proposed.binding:
                raise StaleArchiveTask("fixed original corpus inputs changed within an attempt")
            current = session.scalar(
                select(Job).where(*_identity(job), Job.state.in_(("ready", "active")))
            )
            return None if current is None else DiscoveryJob.read(current)
        row = session.execute(
            update(Job)
            .where(*_identity(job), Job.version == job.version, Job.state == "seeding")
            .values(
                source_manifest_sha256=proposed.source_manifest_sha256,
                source_count=proposed.source_count,
                selection_manifest_sha256=proposed.selection_manifest_sha256,
                selection_count=proposed.selection_count,
                state="ready",
                version=_version(),
            )
            .returning(Job)
        ).scalar_one_or_none()
        return None if row is None else DiscoveryJob.read(row)


@declare_growth(_declaration("activate_discovery_corpus"))
def activate_discovery_corpus(factory: SessionFactory, job: DiscoveryJob) -> DiscoveryJob | None:
    """Record a confirmed Native open; retries preserve the same original binding."""
    _ = job.binding
    with factory() as session, session.begin():
        if not _current_build(session, job, lock=True):
            return None
        row = session.execute(
            update(Job)
            .where(
                *_identity(job),
                Job.version == job.version,
                Job.state.in_(("ready", "active")),
            )
            .values(state="active", version=_version())
            .returning(Job)
        ).scalar_one_or_none()
        return None if row is None else DiscoveryJob.read(row)


@declare_growth(_declaration("lose_discovery_corpus"))
def lose_discovery_corpus(
    factory: SessionFactory, job: DiscoveryJob, *, failure: str
) -> DiscoveryJob | None:
    """Preserve a missing/incompatible workspace disposition without recreating its files."""
    if not isinstance(failure, str) or not failure:
        raise ValueError("lost discovery custody requires its concrete failure")
    with factory() as session, session.begin():
        row = session.execute(
            update(Job)
            .where(
                *_identity(job),
                Job.version == job.version,
                Job.state.in_(("preparing", "seeding", "ready", "active")),
            )
            .values(state="lost", failure=failure, version=_version())
            .returning(Job)
        ).scalar_one_or_none()
        return None if row is None else DiscoveryJob.read(row)


@declare_growth(_declaration("retire_discovery_job"))
def retire_discovery_job(session: Session, job: DiscoveryJob) -> DiscoveryJob | None:
    """Detach the exact catalog fence in the composing owner's transaction.

    Preserve whether the original Core storage was confirmed before retirement.
    A preparing prefix cannot acquire child/corpus disposal authority from this
    transition. Retired stages are absorbing: a retry cannot reset a partially
    reclaimed metadata file or a child whose lease was already unlinked.
    No file is closed, interpreted or removed by this catalog disposition.
    """
    fence = (
        *_identity(job),
        Job.version == job.version,
        Job.state == job.state,
        Job.storage_state == job.storage_state,
    )
    if job.state in {"retiring", "directory"}:
        row = session.execute(select(Job).where(*fence)).scalar_one_or_none()
    else:
        confirmed = job.storage_state == "confirmed"
        row = session.execute(
            update(Job)
            .where(*fence)
            .values(
                build_id=None,
                state="retiring",
                version=_version(),
                reclamation_phase="children" if confirmed else "metadata",
            )
            .returning(Job)
        ).scalar_one_or_none()
    return None if row is None else DiscoveryJob.read(row)


@declare_growth(_declaration("advance_discovery_reclamation"))
def advance_discovery_reclamation(
    session: Session,
    job: DiscoveryJob,
    phase: str,
    *,
    journal_id: str | None = None,
    directory_id: str | None = None,
) -> DiscoveryJob | None:
    """Commit one exact irreversible phase after its owning physical proof.

    The composing caller holds the original exclusive parent custody and supplies
    the actual completed child/extent/root proof. This CAS neither establishes
    that proof nor touches files. The selected child's exact registered identity
    survives process exit between lease unlink, rmdir and routing retirement.
    Core routing stays intact until every registered child and its root complete.
    """
    if job.build_id is not None or job.state != "retiring":
        return None
    permitted = {
        "children": {"child_directory", "corpus"},
        "child_directory": {"children"},
        "corpus": {"journals_root"},
        "journals_root": {"metadata"},
        "metadata": {"directory"},
    }
    if phase not in permitted.get(job.reclamation_phase or "", set()):
        raise ValueError("discovery reclamation requires its next irreversible phase")
    if phase == "child_directory":
        if (
            job.storage_state != "confirmed"
            or not isinstance(journal_id, str)
            or not journal_id
            or not isinstance(directory_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", directory_id) is None
        ):
            raise ValueError("child directory retirement requires its exact registered identity")
    elif journal_id is not None or directory_id is not None:
        raise ValueError("only child directory retirement retains a selected child")
    row = session.execute(
        update(Job)
        .where(
            *_identity(job),
            Job.version == job.version,
            Job.state == "retiring",
            Job.storage_state == job.storage_state,
            Job.build_id.is_(None),
        )
        .values(
            reclamation_phase=phase,
            reclamation_journal_id=journal_id,
            reclamation_directory_id=directory_id,
            state="directory" if phase == "directory" else "retiring",
            version=_version(),
        )
        .returning(Job)
    ).scalar_one_or_none()
    return None if row is None else DiscoveryJob.read(row)

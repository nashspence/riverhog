"""Discover and receive exact application-owned primary preparation originals."""

from __future__ import annotations

import errno
import hashlib
import re
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_protocol.errors import Conflict
from riverhog_provenance import JournalValidationCapacity, StaleCorpusWorkspace
from sqlalchemy import select, update
from state_schema import read_snapshot
from workspace_reclamation import InvalidWorkspaceFiles, WorkspaceReclamationCapacity

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_primary_facts_models import PrimaryFactsJobRecord as Job
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.primary_facts_reclamation import (
    PrimaryFactsReclamationProgress,
    PrimaryFactsRetirementTurn,
)
from riverhog_core.services import primary_facts_jobs as jobs
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.catalog_retirement import metadata_row_bytes
from riverhog_core.services.journal_retirement_registry import format_retirement_blocker
from riverhog_core.services.physical_execution import (
    PhysicalExecutionOwner,
    PhysicalWorkspaceExecutionKey,
)
from riverhog_core.services.primary_facts_catalog_growth import catalog_declaration
from riverhog_core.services.workspace_retirement_registry import (
    WorkspaceRetirementRegistry,
    _Closed,
    _Context,
    _ReceivingPending,
)

_PHASES = ("directory", "memo", "corpus", "allocated", "pinned")


def _coordination_declaration() -> Declaration:
    def admitted(multiplier: int) -> Expression:
        return Expression(
            "product",
            terms=(Expression("constant", value=multiplier), Expression("dimension", name="limit")),
        )

    return Declaration(
        id="riverhog.primary-facts-retirement-coordination.growth/v1",
        authority="riverhog",
        subjects=(
            "riverhog_core.services.upload_primary_facts_retirement.UploadPrimaryFactsRetirement.advance",
        ),
        scope="one admitted primary preparation retirement scheduling call",
        starts_at="receive the caller's scheduling and file-extent admissions",
        ends_at="return bounded bookkeeping events without waiting for a physical worker",
        dimensions=(Dimension("limit", COUNT, "Positive scheduling admission for this call."),),
        bounds=(
            Bound("selection_calls", COUNT, "upper", admitted(3), "Three candidates per unit."),
            Bound("collect_limit", COUNT, "upper", admitted(1), "Receive at most L outcomes."),
            Bound("resume_limit", COUNT, "upper", admitted(1), "Retry at most L original closes."),
            Bound(
                "submit_calls", COUNT, "upper", admitted(3), "Each admitted candidate at most once."
            ),
            Bound(
                "new_submissions", COUNT, "upper", admitted(1), "One submitted candidate per unit."
            ),
            Bound(
                "bookkeeping_events", COUNT, "upper", admitted(4), "At most 2L+L+L registry events."
            ),
        ),
        conditions=(
            "Positive integer scheduling and byte-extent admissions are explicit caller inputs.",
            "The shared registry owns original construction retention and actual worker joining.",
        ),
        invariants=(
            "Scheduling does not execute file disposal, SQLite interpretation or waits inline.",
            "Busy identities advance only volatile seek hints and cannot starve later candidates.",
            "Foreign process identity is only a discovery hint, never proof of quiescence.",
        ),
        limitations=(
            "This scope counts logical coordination atoms. Catalog selection and registry "
            "receiving retain their own work bounds; physical work runs on its original worker.",
        ),
    )


class PrimaryFactsRetirementRegistry(
    WorkspaceRetirementRegistry[jobs.PrimaryFactsJob, PrimaryFactsReclamationProgress]
):
    def __init__(
        self,
        cleanup: UploadPrimaryFactsRetirement,
        execution: PhysicalExecutionOwner,
        retention: ArchiveConstructionRetention,
    ) -> None:
        super().__init__(execution, retention)
        self._cleanup = cleanup

    def _scope(
        self, job: jobs.PrimaryFactsJob, metadata_budget: int
    ) -> tuple[PhysicalWorkspaceExecutionKey, str, int]:
        target = "primary-facts:" + str(self._cleanup.root / job.workspace_id)
        controls = {**asdict(job), "root": str(self._cleanup.root), "target_incarnation": target}
        required = metadata_row_bytes(controls)
        if required > metadata_budget:
            raise VolumeEmissionCapacity(
                dimension="metadata_bytes", required=required, admitted=metadata_budget
            )
        exact = {
            name: str(value) if type(value) is int else value for name, value in controls.items()
        }
        scope = hashlib.sha256(canonical_json_bytes(exact)).hexdigest()
        return PhysicalWorkspaceExecutionKey(job.workspace_id, scope), target, required

    def _create(self, job: jobs.PrimaryFactsJob) -> PrimaryFactsRetirementTurn:
        actor = PrimaryFactsRetirementTurn(self._cleanup, job)
        if actor.job != job or actor.cleanup is not self._cleanup:
            raise Conflict("primary retirement constructor returned another original")
        return actor

    def _accept_progress(
        self, job: jobs.PrimaryFactsJob, result: object
    ) -> PrimaryFactsReclamationProgress:
        if not isinstance(result, PrimaryFactsReclamationProgress):
            raise Conflict("primary retirement returned another original result type")
        if (
            result.job.workspace_id != job.workspace_id
            or result.job.collection_id != job.collection_id
            or result.job.owner_id != job.owner_id
            or result.job.root_identity != job.root_identity
            or result.job.namespace_identity != job.namespace_identity
            or result.job.construction != job.construction
            or result.job.epoch != job.epoch
        ):
            raise Conflict("primary retirement returned another original identity")
        return result

    def _run(
        self, context: _Context[jobs.PrimaryFactsJob, PrimaryFactsReclamationProgress]
    ) -> PrimaryFactsReclamationProgress | _Closed | _ReceivingPending:
        if context.closing:
            # Receiving-only retries preserve the generic original/unknown rules.
            return super()._run(context)
        try:
            return super()._run(context)
        except (
            LostJournalValidationWorkspace,
            StaleCorpusWorkspace,
            InvalidWorkspaceFiles,
            JournalValidationCapacity,
            WorkspaceReclamationCapacity,
            OSError,
        ) as error:
            if isinstance(error, OSError) and error.errno not in {
                errno.ENOENT,
                errno.ENOTDIR,
                errno.ELOOP,
                errno.ENOSPC,
                errno.EDQUOT,
            }:
                raise
            actor = context.actor
            if not isinstance(actor, PrimaryFactsRetirementTurn):
                raise
            # A diagnostic is not a resource receipt. A close refusal propagates
            # so the generic registry retains this exact original and charge.
            actor.close()
            if not actor.received:
                raise Conflict("primary blocker lacks original resource receiving") from error
            self._cleanup.blocked(actor.job, format_retirement_blocker(error))
            return PrimaryFactsReclamationProgress(actor.job, False)


class UploadPrimaryFactsRetirement:
    """Schedule bounded disposal; only an exclusive physical owner may retire.

    A process token chooses candidates. It never proves that another process
    stopped. The worker must acquire the exact original lease and check the
    current catalog, root and namespace before committing retirement. Catalog
    seeks do not enumerate disk or read either disposable database.
    """

    def __init__(
        self,
        factory: SessionFactory,
        root: Path,
        *,
        owner_id: str,
        execution: PhysicalExecutionOwner,
        retention: ArchiveConstructionRetention,
        metadata_budget: int,
        receive_limit: int,
        retry_after: timedelta,
    ) -> None:
        if (
            type(metadata_budget) is not int
            or metadata_budget < 1
            or type(receive_limit) is not int
            or receive_limit < 1
            or retry_after.total_seconds() <= 0
            or re.fullmatch(r"[0-9a-f]{32}", owner_id) is None
        ):
            raise ValueError("primary retirement requires positive original admissions")
        self.factory, self.root, self.owner_id = factory, Path(root), owner_id
        self._metadata_budget, self._receive_limit = metadata_budget, receive_limit
        self._retry_after = retry_after
        self._registry = PrimaryFactsRetirementRegistry(self, execution, retention)
        self._after: dict[str, str | None] = dict.fromkeys(_PHASES)
        self._phase = 0

    def close(self) -> None:
        self._registry.close()

    def receive_stopped(self) -> bool:
        self._registry.collect(limit=self._receive_limit)
        return self._registry.receive_stopped()

    def current(self, job: jobs.PrimaryFactsJob) -> bool:
        with read_snapshot(self.factory) as session:
            return jobs.current(session, job)

    def transition(self, job: jobs.PrimaryFactsJob, state: str) -> jobs.PrimaryFactsJob:
        with self.factory() as session, session.begin():
            # Do not wait behind a catalog writer while holding physical custody.
            session.connection(
                execution_options={"sqlite_immediate_writer": True, "sqlite_defer_if_busy": True}
            )
            changed = jobs.transition(session, job, state)
            if changed is None:
                raise Conflict("primary preparation retirement identity changed")
            return changed

    def finish(self, job: jobs.PrimaryFactsJob) -> bool:
        with self.factory() as session, session.begin():
            session.connection(
                execution_options={"sqlite_immediate_writer": True, "sqlite_defer_if_busy": True}
            )
            if jobs.finish(session, job):
                return True
            # The same original can lose its catalog commit acknowledgement only
            # after it has received physical namespace removal and all handles.
            return jobs.get(session, job.workspace_id) is None

    def blocked(self, job: jobs.PrimaryFactsJob, failure: str) -> bool:
        with self.factory() as session, session.begin():
            session.connection(
                execution_options={"sqlite_immediate_writer": True, "sqlite_defer_if_busy": True}
            )
            changed = session.execute(
                update(Job)
                .where(*jobs._identity(job), Job.failure.is_distinct_from(failure))
                .values(failure=failure)
                .returning(Job.workspace_id)
                .execution_options(synchronize_session=False)
            ).scalar_one_or_none()
            return changed is not None

    def _select(self, excluding: tuple[str, ...]) -> jobs.PrimaryFactsJob | None:
        if len(excluding) > 3:
            raise ValueError("primary retirement selection exceeds its candidate admission")
        for distance in range(len(_PHASES)):
            index = (self._phase + distance) % len(_PHASES)
            phase = _PHASES[index]
            query = select(Job).where(Job.state == phase, Job.workspace_id > "")
            if excluding:
                query = query.where(Job.workspace_id.not_in(excluding))
            query = query.order_by(Job.workspace_id).limit(1)
            with read_snapshot(self.factory) as session:
                row = None
                after = self._after[phase]
                if after is not None:
                    row = session.scalar(query.where(Job.workspace_id > after))
                if row is None:
                    row = session.scalar(query)
                selected = None if row is None else jobs.PrimaryFactsJob.read(row)
            if selected is not None:
                self._after[phase] = selected.workspace_id
                self._phase = (index + 1) % len(_PHASES)
                return selected
        return None

    @declare_growth(_coordination_declaration())
    def advance(
        self, *, limit: int = 1, octet_budget: int = 4 * 1024 * 1024, admit: bool = True
    ) -> int:
        """At most 4L bookkeeping events and 3L candidate admissions per call."""
        if (
            type(octet_budget) is not int
            or octet_budget < 1
            or type(limit) is not int
            or type(admit) is not bool
        ):
            raise ValueError("primary retirement requires integer unit and extent admissions")
        if limit < 1:
            return 0
        processed = self._registry.collect(limit=limit)
        processed += self._registry.resume_ready(limit=limit, retry_after=self._retry_after)
        if not admit:
            return processed
        for _ in range(limit):
            attempted: list[str] = []
            submitted = False
            for _candidate in range(3):
                job = self._select(tuple(attempted))
                if job is None:
                    break
                attempted.append(job.workspace_id)
                if job.owner_id == self.owner_id and job.state in jobs.LIVE_STATES:
                    continue
                if self._registry.retained(job.workspace_id):
                    continue
                if self._registry.submit(
                    job,
                    octet_budget=octet_budget,
                    metadata_budget=self._metadata_budget,
                    retry_after=self._retry_after,
                ):
                    processed += 1
                    submitted = True
                    break
            if not submitted:
                break
        return processed


for _operation in ("_select", "current", "transition", "finish", "blocked"):
    declare_growth(
        catalog_declaration(
            "riverhog_core.services.upload_primary_facts_retirement.UploadPrimaryFactsRetirement."
            + _operation,
            "retirement-" + _operation.strip("_"),
            selecting=_operation == "_select",
        )
    )(getattr(UploadPrimaryFactsRetirement, _operation))
del _operation

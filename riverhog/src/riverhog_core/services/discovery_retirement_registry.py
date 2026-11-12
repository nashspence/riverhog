"""Route original discovery cleanup through shared retained workspace execution."""

from __future__ import annotations

from dataclasses import asdict, replace
from pathlib import Path

from riverhog_protocol.errors import Conflict

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_retirement import (
    DiscoveryRetirementProgress,
    DiscoveryRetirementTurn,
)
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.services.archive_construction_retention import ArchiveConstructionRetention
from riverhog_core.services.catalog_retirement import metadata_row_bytes
from riverhog_core.services.physical_execution import (
    PhysicalExecutionOwner,
    PhysicalWorkspaceExecutionKey,
)
from riverhog_core.services.workspace_retirement_registry import WorkspaceRetirementRegistry


class DiscoveryRetirementRegistry(
    WorkspaceRetirementRegistry[jobs.DiscoveryJob, DiscoveryRetirementProgress]
):
    """Keep original discovery scope and outcomes with their application owner."""

    def __init__(
        self,
        factory: SessionFactory,
        root: Path,
        execution: PhysicalExecutionOwner,
        retention: ArchiveConstructionRetention,
    ) -> None:
        if not root.is_absolute() or root == root.parent:
            raise ValueError("discovery retirement requires its configured absolute root")
        super().__init__(execution, retention)
        self._factory, self._root = factory, root

    def _scope(
        self, job: jobs.DiscoveryJob, metadata_budget: int
    ) -> tuple[PhysicalWorkspaceExecutionKey, str, int]:
        if job.build_id is not None or job.state not in {"retiring", "directory"}:
            raise ValueError("discovery execution requires detached irreversible retirement")
        target = "discovery:" + str(self._root / job.workspace_id)
        required = metadata_row_bytes(
            {**asdict(job), "root": str(self._root), "target_incarnation": target}
        )
        return (
            PhysicalWorkspaceExecutionKey(job.workspace_id, job.admission_sha256),
            target,
            required,
        )

    def _accept_progress(
        self, job: jobs.DiscoveryJob, result: object
    ) -> DiscoveryRetirementProgress:
        if not isinstance(result, DiscoveryRetirementProgress):
            raise Conflict("discovery execution returned another outcome type")
        original = replace(
            result.job,
            version=job.version,
            state=job.state,
            reclamation_phase=job.reclamation_phase,
            reclamation_journal_id=job.reclamation_journal_id,
            reclamation_directory_id=job.reclamation_directory_id,
        )
        if original != job:
            raise Conflict("discovery progress changed its original scope")
        return result

    def _create(self, job: jobs.DiscoveryJob) -> DiscoveryRetirementTurn:
        actor = DiscoveryRetirementTurn(self._factory, job, self._root)
        if actor.job != job or actor.root != self._root or actor.factory is not self._factory:
            raise Conflict("discovery constructor returned another original owner")
        return actor

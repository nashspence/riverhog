"""State queries consumed by compiled planners within Stove0's state owner."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol

from sqlalchemy import Table
from sqlalchemy.engine import Engine
from stove0_protocol import (
    ArtifactSelection,
    ArtifactSelectionRef,
    WorkArtifactSubject,
    WorkIdentity,
)
from stove0_protocol.input_groups import InputGroupSet

from stove0_core.accepted_observations import AcceptedObservationStore
from stove0_core.compiled_planning_state import CompiledPlanningState
from stove0_core.metadata_selections import MetadataSelectionStore
from stove0_core.recipe_definitions import RetainedRecipeStore


class GroupPlanningPort(Protocol):
    @property
    def tables(self) -> Mapping[str, Table]: ...

    def step(self, work: WorkIdentity, group_id: str) -> InputGroupSet | None: ...

    def candidate(
        self, authority: InputGroupSet, primary_id: str
    ) -> ArtifactSelectionRef | None: ...


class CompiledStatePort(Protocol):
    @property
    def engine(self) -> Engine: ...

    @property
    def planning_key(self) -> Callable[[str], str]: ...

    @property
    def recipe_definitions(self) -> RetainedRecipeStore: ...

    @property
    def compiled_planning(self) -> CompiledPlanningState: ...

    @property
    def compiled_groups(self) -> GroupPlanningPort: ...

    @property
    def metadata_selections(self) -> MetadataSelectionStore: ...

    @property
    def accepted_observations(self) -> AcceptedObservationStore: ...

    def load_selection_ref(self, selection_sha256: str) -> ArtifactSelectionRef | None: ...

    def load_selection_artifact(
        self, selection_sha256: str, artifact_id: str
    ) -> WorkArtifactSubject | None: ...

    def selection_artifact_page(
        self, selection_sha256: str, *, continuation: str | None, limit: int
    ) -> tuple[tuple[WorkArtifactSubject, ...], str | None, bool]: ...

    def retain_selection(self, selection: ArtifactSelection) -> None: ...

"""Observation requests/results; no universal path, host or filesystem fields."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Any, Protocol, cast, runtime_checkable

from riverhog_provenance_contracts import PROFILE, ContractCatalog, canonical_document

from .common import software_agent_id


@runtime_checkable
class BinaryReadable(Protocol):
    def read(self, size: int = -1, /) -> bytes:
        """Return at most size bytes, or b'' only at the declared extent boundary."""
        ...


@dataclass(frozen=True, slots=True)
class SourceCapabilities:
    repeatable: bool = False
    seekable: bool = False
    stable_version_selection: bool = False
    native_metadata: bool = False
    persistent_designator: bool = False

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be bool")

    def to_mapping(self) -> dict[str, bool]:
        return {
            "bounded": True,
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        }


@dataclass(frozen=True, slots=True)
class ObservationPolicy:
    hash_chunk_bytes: int = 8 * 1024 * 1024
    maximum_content_bytes: int = (1 << 63) - 1
    second_content_hash: bool = False
    include_sha512: bool = False

    def __post_init__(self) -> None:
        for name in ("hash_chunk_bytes", "maximum_content_bytes"):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= (1 << 63) - 1:
                raise ValueError(f"{name} must be a positive sequence63 integer")
        for name in ("second_content_hash", "include_sha512"):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be bool")


@dataclass(frozen=True, slots=True)
class ObservationRequest:
    observer_agent_id: str = field(default_factory=software_agent_id)
    artifact: Mapping[str, Any] | None = None
    occurrence: Mapping[str, Any] | None = None
    execution_context: Mapping[str, Any] | None = None
    policy: ObservationPolicy = field(default_factory=ObservationPolicy)


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    """Finalized source observations, not inferred capabilities or restorability."""

    consistency: Mapping[str, Any]
    address_status: str = "not_exposed"
    metadata: tuple[Mapping[str, Any], ...] = ()
    profiles: tuple[Mapping[str, Any], ...] = ()
    coverage: tuple[Mapping[str, Any], ...] = ()
    locators: tuple[Mapping[str, Any], ...] = ()
    supporting_assertions: Mapping[str, Any] = field(default_factory=dict)


def one_pass_evidence() -> SourceEvidence:
    return SourceEvidence(
        consistency={
            "level": "one_pass",
            "method_uri": PROFILE + "/methods/bounded-read",
            "limitations": [
                "Only bytes yielded through this source boundary were measured.",
                "No upstream object atomicity, earlier pathname or earlier storage model "
                "is asserted.",
            ],
        }
    )


@dataclass(slots=True)
class ObservationSession:
    """Reader lifetime is owned by the source's context manager, not this record.

    A source may expose additional metadata after reading (for example, final
    access-time or version checks). It reports those through finalize().
    """

    reader: BinaryReadable
    extent: Mapping[str, Any]
    occurrence_kind: str
    capabilities: SourceCapabilities
    expected_length: int | None = None
    source_context: Mapping[str, Any] | None = None
    finalize: Callable[[], SourceEvidence] = one_pass_evidence
    repeat_reader: Callable[[], AbstractContextManager[BinaryReadable]] | None = None


@dataclass(frozen=True, slots=True)
class ObservationResult:
    """Immutable serialized assertions with separately identified referents."""

    artifact_id: str
    occurrence_id: str
    state_id: str
    capture_id: str
    observation_id: str
    observer_agent_id: str
    _graph_json: bytes = field(repr=False)
    _catalog: ContractCatalog = field(repr=False, compare=False)

    @classmethod
    def from_graph(
        cls,
        graph: Mapping[str, Any],
        *,
        observation_id: str,
        observer_agent_id: str | None = None,
        catalog: ContractCatalog | None = None,
    ) -> ObservationResult:
        from riverhog_provenance_contracts import validate_graph_shape

        validate_graph_shape(graph)
        objects = {row["id"]: row for rows in graph.values() for row in rows}
        observation = objects[observation_id]
        state = objects[observation["state"]["object_id"]]
        occurrence = objects[state["occurrence_id"]]
        capture = objects[observation["capture_id"]]
        associated = {a["agent_id"] for a in capture["associations"]}
        measured_by = {
            ev["asserted_by_agent_id"]
            for ev in observation["evidence"]
            if ev["basis"] == "direct_measurement"
        } & associated
        if observer_agent_id is None:
            if len(measured_by) != 1:
                raise ValueError("select an unambiguous directly measuring observer Agent")
            observer_agent_id = next(iter(measured_by))
        if observer_agent_id not in measured_by:
            raise ValueError("selected observer is not an attributed direct-measurement Agent")
        return cls(
            occurrence["artifact_id"],
            occurrence["id"],
            state["id"],
            capture["id"],
            observation_id,
            observer_agent_id,
            canonical_document(graph),
            catalog or ContractCatalog(),
        )

    @property
    def catalog(self) -> ContractCatalog:
        return self._catalog

    def graph_fragment(self) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self._graph_json))

    @property
    def artifact(self) -> dict[str, Any]:
        return next(
            row for row in self.graph_fragment()["artifacts"] if row["id"] == self.artifact_id
        )

    @property
    def occurrence(self) -> dict[str, Any]:
        return next(
            row for row in self.graph_fragment()["occurrences"] if row["id"] == self.occurrence_id
        )

    @property
    def observation(self) -> dict[str, Any]:
        return next(
            row for row in self.graph_fragment()["descriptions"] if row["id"] == self.observation_id
        )

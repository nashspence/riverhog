"""Isolate metadata preparation while durable planning state owns continuations."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Hashable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol, Self

from http_api_contracts.control import control_budget
from http_api_contracts.metadata_exchange import ResumableMetadataCalls
from pydantic import JsonValue
from stove0_protocol import PlanningJobStatus, WorkIdentity, canonical_json_bytes

from stove0_core.planning_progress import PlanningProgress
from stove0_core.work_state import WorkRecord

_PREPARING = ContextVar("stove0_metadata_preparation", default=False)

_LOGGER = logging.getLogger("stove0_core.planning")


@contextmanager
def planning_cost(phase: str, **counts: str | int) -> Iterator[None]:
    started = time.perf_counter()
    outcome = "complete"
    try:
        yield
    except BaseException as error:
        outcome = type(error).__name__
        raise
    finally:
        if _LOGGER.isEnabledFor(logging.INFO):
            _LOGGER.info(
                "stove0-planning-cost %s",
                canonical_json_bytes(
                    {
                        "phase": phase,
                        "seconds": time.perf_counter() - started,
                        "outcome": outcome,
                        **counts,
                    }
                ).decode(),
            )


class CompiledPlanningProcessor(Protocol):
    def step(self, work: WorkIdentity) -> PlanningProgress: ...


def advance_planning(
    planner: CompiledPlanningProcessor,
    work: WorkIdentity,
    *,
    owner_kind: str,
    owner_id: str,
    maximum_steps: int = 64,
    maximum_seconds: float = 0.1,
    deliver: Callable[[PlanningProgress], None] | None = None,
    maximum_questions: int = 4,
) -> PlanningProgress:
    """Drain persisted metadata continuations within one finite worker quantum.

    Synchronous control callers retain a single step. The metadata worker can
    drain inexpensive pending steps without paying a scheduler delay for each
    row. A worker may dispatch/poll a finite burst of independent questions;
    synchronous callers and workers without a delivery callback return the first
    question. Target execution and settlement retain their owning boundaries.
    """
    if maximum_steps < 1 or maximum_seconds <= 0 or maximum_questions < 1:
        raise ValueError("planning worker quantum must be positive")
    if not _PREPARING.get():
        return planner.step(work)
    started = time.perf_counter()
    steps = questions = 0
    for _ in range(maximum_steps):
        steps += 1
        progress = planner.step(work)
        if progress.state == "question" and deliver is not None:
            # Delivery durably queues/polls one exact job; it never waits for
            # observer execution. The persisted task turn selects other ready
            # dependencies even when this resource remains deferred.
            deliver(progress)
            questions += 1
            progress = PlanningProgress("pending", progress.work)
        if (
            progress.state != "pending"
            or questions >= maximum_questions
            or time.perf_counter() - started >= maximum_seconds
        ):
            break
    if _LOGGER.isEnabledFor(logging.INFO):
        _LOGGER.info(
            "stove0-planning-cost %s",
            canonical_json_bytes(
                {
                    "phase": "compiled-continuations",
                    "seconds": time.perf_counter() - started,
                    "steps": steps,
                    "questions": questions,
                    "state": progress.state,
                    "outcome": "complete",
                    "work_id": work.work_id,
                    "owner_kind": owner_kind,
                    "owner_id": owner_id,
                }
            ).decode(),
        )
    return progress


class WorkMetadataProcessor(Protocol):
    def step(self, work_id: str) -> WorkRecord: ...

    def maintain(self, work_id: str) -> WorkRecord: ...


class PreviewMetadataProcessor(Protocol):
    def step(self, job_id: str) -> PlanningJobStatus: ...

    def maintain(self, job_id: str) -> PlanningJobStatus: ...


@dataclass(frozen=True)
class WorkScan:
    work_id: str
    phase: str
    revision: int

    @classmethod
    def from_record(cls, record: WorkRecord) -> Self:
        return cls(record.work_id, record.phase, record.revision)


@dataclass(frozen=True)
class PreviewScan:
    job_id: str
    phase: str
    revision: int


def planning_control_budget(seconds: float = 5.0) -> AbstractContextManager[None]:
    # Whole-document validation may be large; each native HTTP contact still
    # owns its finite control allowance, independently of this computation.
    return nullcontext() if _PREPARING.get() else control_budget(seconds)


@contextmanager
def _preparing() -> Iterator[None]:
    token = _PREPARING.set(True)
    try:
        yield
    finally:
        _PREPARING.reset(token)


class MetadataSteps:
    def __init__(self, *, maximum_steps: int = 16, wait_seconds: float = 0.02) -> None:
        self._steps = ResumableMetadataCalls(maximum_steps)
        self._maintenance = ResumableMetadataCalls(maximum_steps)
        self.wait_seconds = wait_seconds

    def call[T](self, key: Hashable, operation: Callable[[], T], *, maintenance: bool = False) -> T:
        def prepare(_latest: Callable[[], dict[str, JsonValue]]) -> T:
            with _preparing():
                return operation()

        calls = self._maintenance if maintenance else self._steps
        return calls.call(key=key, payload=None, prepare=prepare, maximum_seconds=self.wait_seconds)

    def work(
        self, coordinator: WorkMetadataProcessor, work_id: str, *, maintenance: bool = False
    ) -> WorkScan:
        operation = coordinator.maintain if maintenance else coordinator.step
        return self.call(
            ("work", work_id),
            lambda: WorkScan.from_record(operation(work_id)),
            maintenance=maintenance,
        )

    def preview(
        self, service: PreviewMetadataProcessor, job_id: str, *, maintenance: bool = False
    ) -> str:
        operation = service.maintain if maintenance else service.step

        def prepare() -> str:
            status = operation(job_id)
            # Return only scheduling metadata; the complete result belongs to
            # the persisted preview and its exact inspection interface.
            return status.job_id

        return self.call(("preview", job_id), prepare, maintenance=maintenance)

    def close(self) -> None:
        self._steps.close()
        self._maintenance.close()

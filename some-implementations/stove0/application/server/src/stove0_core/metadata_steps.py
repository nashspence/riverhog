"""Isolate metadata preparation while durable planning state owns continuations."""

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol, Self

from http_api_contracts.control import control_budget
from http_api_contracts.metadata_exchange import ResumableMetadataCalls
from pydantic import JsonValue
from stove0_protocol import PlanningJobStatus

from stove0_core.work_state import WorkRecord

_PREPARING = ContextVar("stove0_metadata_preparation", default=False)


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

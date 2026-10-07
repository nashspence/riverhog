"""Fair advancement of explicitly initiated Stove0 work."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any, Literal, Protocol, cast

from http_api_contracts.control import (
    ControlBudgetExhausted,
    check_control_budget,
    control_budget,
    finite_control_seconds,
)
from http_api_contracts.metadata_exchange import MetadataPreparationPending
from stove0_operator_contracts import AdmissionRun, DepartureRun
from time_formats import format_utc_timestamp, utc_now

from stove0_core.coordinator import Stove0Coordinator
from stove0_core.metadata_steps import MetadataSteps
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.work_state import ConcurrentWorkUpdate

_TERMINAL_PHASES = frozenset({"complete", "no_action", "inapplicable", "failed", "canceled"})
_PRUNE_INTERVAL_SECONDS = 60 * 60
SchedulerRole = Literal["controller", "worker", "combined"]


class ProductionSealProcessor(Protocol):
    def process_due_production_seals(self, *, limit: int = 1) -> int: ...


class AdmissionProcessor(Protocol):
    def advance(self, *, limit: int = 25) -> AdmissionRun: ...


class DepartureProcessor(Protocol):
    def advance(self, *, limit: int = 25) -> DepartureRun: ...


class PreviewProcessor(Protocol):
    def advance(self, *, limit: int = 25) -> dict[str, object]: ...


_CONTROLLER_PHASES = frozenset(
    {
        "eligible",
        "claimed",
        "planning",
        "coordinating",
        "verifying",
        "settled",
        "source_collection_retirement_pending",
        "abandon_pending",
        "no_output_pending",
    }
)
_WORKER_PHASES = frozenset(
    {
        "target_preflight",
        "queued",
        "executing",
        "output_finalizing",
    }
)


class Stove0Scheduler:
    """Advance work admitted through an explicit or configured preview acceptance."""

    def __init__(
        self,
        *,
        coordinator: Stove0Coordinator,
        state: SqlAlchemyStateStore,
        production_seals: ProductionSealProcessor | None = None,
        admission: AdmissionProcessor | None = None,
        departure: DepartureProcessor | None = None,
        previews: PreviewProcessor | None = None,
        operational_state_retention_seconds: int = 30 * 24 * 60 * 60,
        claim_renew_seconds: float = 300.0,
        control_seconds: float = 5.0,
        metadata_steps: MetadataSteps | None = None,
    ) -> None:
        if operational_state_retention_seconds < 1:
            raise ValueError("stove0 operational-state retention must be positive")
        self.coordinator = coordinator
        self.state = state
        self.production_seals = production_seals
        self.admission = admission
        self.departure = departure
        self.previews = previews
        self.operational_state_retention_seconds = operational_state_retention_seconds
        self._prune_lock = threading.Lock()
        self._next_prune = 0.0
        self.claim_renew_seconds = finite_control_seconds(claim_renew_seconds)
        self.control_seconds = finite_control_seconds(control_seconds)
        self.metadata_steps = metadata_steps
        self._next_lane = 0

    def advance(
        self,
        *,
        role: SchedulerRole = "combined",
        limit: int = 25,
        maintenance: bool = True,
    ) -> dict[str, object]:
        with control_budget(self.control_seconds):
            return self._advance(role=role, limit=limit, maintenance=maintenance)

    def _advance(self, *, role: SchedulerRole, limit: int, maintenance: bool) -> dict[str, object]:
        if limit < 1 or limit > 100:
            raise ValueError("stove0 scheduler work limit must be between 1 and 100")
        phases = _phases_for_role(role)
        stream = f"stove0-work-scan/{role}/v1"
        saved = self.state.load_cursor(stream)
        cursor, revision = saved if saved is not None else ("", None)
        failures = (
            self._maintain_work(limit=limit)
            if maintenance and role in {"controller", "combined"}
            else []
        )
        records, _ = self.state.scan_work(
            phases=tuple(sorted(phases)),
            after_work_id=cursor,
            limit=limit,
        )
        progressed: list[str] = []
        next_cursor = cursor
        for record in records:
            if record.phase in _TERMINAL_PHASES or record.phase not in phases:
                continue
            try:
                check_control_budget()
            except ControlBudgetExhausted:
                break
            next_cursor = record.work_id
            try:
                updated = (
                    self.metadata_steps.work(self.coordinator, record.work_id)
                    if self.metadata_steps is not None
                    else self.coordinator.step(record.work_id)
                )
                changed = updated.revision > record.revision
                if changed:
                    progressed.append(record.work_id)
                self.state.record_work_contact(
                    record.work_id,
                    expected_revision=updated.revision,
                    failed=False,
                    progressed=changed,
                )
            except ConcurrentWorkUpdate as exc:
                current = self.state.load_work_scan(record.work_id)
                if current is not None and current.revision > record.revision:
                    # Another scheduler owns the same compare-and-swap
                    # transition. That is convergence, not a work failure.
                    continue
                failures.append(
                    {
                        "work_id": record.work_id,
                        "error": f"{type(exc).__name__}: {exc}"[:1000],
                    }
                )
            except MetadataPreparationPending:
                continue
            except ControlBudgetExhausted:
                break
            except Exception as exc:
                self.state.record_work_contact(
                    record.work_id, expected_revision=record.revision, failed=True, progressed=False
                )
                # A failed item never prevents later work from advancing. The
                # original record remains retryable and exact; operator-visible
                # diagnostics identify the failed step without inventing payload
                # custody or silently changing the state machine.
                failures.append(
                    {
                        "work_id": record.work_id,
                        "error": f"{type(exc).__name__}: {exc}"[:1000],
                    }
                )
        self._advance_cursor(
            stream,
            expected_revision=revision,
            cursor=next_cursor,
            require_exact=False,
        )
        return {
            "role": role,
            "cursor": cursor,
            "next_cursor": next_cursor,
            "progressed": progressed,
            "failures": failures,
        }

    def _maintain_work(self, *, limit: int) -> list[dict[str, str]]:
        stream = "stove0-claim-maintenance/v1"
        saved = self.state.load_cursor(stream)
        cursor, revision = saved if saved is not None else ("", None)
        phases = tuple(
            sorted(
                (_CONTROLLER_PHASES | _WORKER_PHASES)
                - {"eligible", "settled", "source_collection_retirement_pending", "abandon_pending"}
            )
        )
        records, _ = self.state.scan_work(
            phases=phases, after_work_id=cursor, limit=limit, maintenance=True
        )
        failures: list[dict[str, str]] = []
        visited = cursor
        for record in records:
            try:
                check_control_budget()
            except ControlBudgetExhausted:
                break
            visited = record.work_id
            try:
                renewed = (
                    self.metadata_steps.work(self.coordinator, record.work_id, maintenance=True)
                    if self.metadata_steps is not None
                    else self.coordinator.maintain(record.work_id)
                )
                self.state.record_work_maintenance(
                    record.work_id,
                    expected_revision=renewed.revision,
                    interval_seconds=self.claim_renew_seconds,
                )
            except MetadataPreparationPending:
                continue
            except ControlBudgetExhausted:
                break
            except ConcurrentWorkUpdate:
                pass
            except Exception as exc:
                failures.append(
                    {"work_id": record.work_id, "error": f"{type(exc).__name__}: {exc}"[:1000]}
                )
        self._advance_cursor(
            stream, expected_revision=revision, cursor=visited, require_exact=False
        )
        return failures

    def run_once(
        self,
        *,
        role: SchedulerRole = "combined",
        work_limit: int = 25,
    ) -> dict[str, object]:
        if work_limit < 1 or work_limit > 100:
            raise ValueError("stove0 scheduler work limit must be between 1 and 100")
        _phases_for_role(role)
        result: dict[str, object] = {
            "pruning": None,
            "previews": None,
            "admission": None,
            "departure": None,
            "work": {
                "role": role,
                "cursor": "",
                "next_cursor": "",
                "progressed": [],
                "failures": [],
            },
        }
        lanes = ("pruning", "production", "previews", "admission", "departure", "work")
        initial = self._next_lane
        with control_budget(self.control_seconds):
            if role in {"controller", "combined"}:
                cast(dict[str, Any], result["work"])["failures"].extend(
                    self._maintain_work(limit=work_limit)
                )
            for offset in range(len(lanes)):
                lane_index = (initial + offset) % len(lanes)
                try:
                    check_control_budget()
                except ControlBudgetExhausted:
                    self._next_lane = lane_index
                    break
                self._next_lane = (lane_index + 1) % len(lanes)
                try:
                    lane = lanes[lane_index]
                    if lane == "pruning" and role in {"controller", "combined"}:
                        result["pruning"] = self._metadata_lane(
                            "pruning", self._prune_operational_state
                        )
                    elif (
                        lane == "production"
                        and role in {"worker", "combined"}
                        and self.production_seals is not None
                    ):
                        production_seals = self.production_seals

                        def production_step(
                            service: ProductionSealProcessor = production_seals,
                        ) -> int:
                            return service.process_due_production_seals(limit=work_limit)

                        self._metadata_lane("production", production_step)
                    elif (
                        lane == "previews"
                        and role in {"controller", "combined"}
                        and self.previews is not None
                    ):
                        result["previews"] = self.previews.advance(limit=work_limit)
                    elif (
                        lane == "admission"
                        and role in {"controller", "combined"}
                        and self.admission is not None
                    ):
                        admission = self.admission

                        def admission_step(
                            service: AdmissionProcessor = admission,
                        ) -> dict[str, Any]:
                            return service.advance(limit=work_limit).model_dump(mode="python")

                        result["admission"] = self._metadata_lane("admission", admission_step)
                    elif (
                        lane == "departure"
                        and role in {"controller", "combined"}
                        and self.departure is not None
                    ):
                        departure = self.departure

                        def departure_step(
                            service: DepartureProcessor = departure,
                        ) -> dict[str, Any]:
                            return service.advance(limit=work_limit).model_dump(mode="python")

                        result["departure"] = self._metadata_lane("departure", departure_step)
                    elif lane == "work":
                        batch = self.advance(role=role, limit=work_limit, maintenance=False)
                        batch["failures"] = [
                            *cast(dict[str, Any], result["work"])["failures"],
                            *cast(list[dict[str, str]], batch["failures"]),
                        ]
                        result["work"] = batch
                except MetadataPreparationPending:
                    continue
                except ControlBudgetExhausted:
                    break
            else:
                self._next_lane = (initial + 1) % len(lanes)
        return result

    def _metadata_lane[T](self, lane: str, operation: Callable[[], T]) -> T:
        if self.metadata_steps is None:
            return operation()
        return self.metadata_steps.call(("lane", lane), operation)

    def _prune_operational_state(self) -> dict[str, int] | None:
        observed = time.monotonic()
        with self._prune_lock:
            if observed < self._next_prune:
                return None
            cutoff = format_utc_timestamp(
                utc_now() - timedelta(seconds=self.operational_state_retention_seconds)
            )
            result = self.state.prune_operational_state(cutoff=cutoff)
            self._next_prune = observed + _PRUNE_INTERVAL_SECONDS
            return result

    def _advance_cursor(
        self,
        stream: str,
        *,
        expected_revision: int | None,
        cursor: str,
        require_exact: bool,
    ) -> None:
        try:
            self.state.compare_and_swap_cursor(
                stream,
                expected_revision=expected_revision,
                cursor=cursor,
            )
        except ConcurrentWorkUpdate:
            current = self.state.load_cursor(stream)
            if current is not None and current[0] == cursor:
                return
            if require_exact:
                raise


def scheduler_role(value: str) -> SchedulerRole:
    normalized = value.strip().casefold()
    if normalized not in {"controller", "worker", "combined"}:
        raise ValueError("stove0 scheduler role must be controller, worker, or combined")
    return cast(SchedulerRole, normalized)


def _phases_for_role(role: SchedulerRole) -> frozenset[str]:
    if role == "controller":
        return _CONTROLLER_PHASES
    if role == "worker":
        return _WORKER_PHASES
    if role == "combined":
        return _CONTROLLER_PHASES | _WORKER_PHASES
    raise ValueError(f"unsupported stove0 scheduler role: {role}")


__all__ = ["SchedulerRole", "Stove0Scheduler", "scheduler_role"]

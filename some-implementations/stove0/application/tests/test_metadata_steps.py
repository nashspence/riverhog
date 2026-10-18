from __future__ import annotations

from types import SimpleNamespace
from typing import Literal

import pytest
import stove0_core.metadata_steps as metadata
from stove0_core.planning_progress import PlanningProgress
from stove0_protocol import CollectionRootIdentityRef, RecipeIdentityRef, WorkIdentity, WorkPayload


@pytest.fixture
def work() -> WorkIdentity:
    return WorkIdentity.seal(
        WorkPayload(
            recipe=RecipeIdentityRef(id="example.recipe/v1", revision="1", sha256="a" * 64),
            inputs=(
                CollectionRootIdentityRef(
                    collection_id="1",
                    archive_root_sha256="b" * 64,
                    artifact_set_identity="c" * 64,
                ),
            ),
            effective_intent={},
        )
    )


def test_synchronous_control_caller_retains_one_bounded_continuation(work: WorkIdentity) -> None:
    calls = []
    planner = SimpleNamespace(
        step=lambda item: calls.append(item) or PlanningProgress("pending", item)
    )
    result = metadata.advance_planning(planner, work, owner_kind="preview", owner_id="fixture")
    assert result.state == "pending" and calls == [work]


def test_metadata_worker_drains_pending_rows_with_a_finite_step_budget(
    work: WorkIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    monkeypatch.setattr(metadata.time, "perf_counter", lambda: 0)
    planner = SimpleNamespace(
        step=lambda item: calls.append(item) or PlanningProgress("pending", item)
    )
    with metadata._preparing():
        result = metadata.advance_planning(
            planner,
            work,
            owner_kind="work",
            owner_id=work.work_id,
            maximum_steps=7,
        )
    assert result.state == "pending" and calls == [work] * 7


def test_metadata_worker_yields_at_its_physical_time_budget(
    work: WorkIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now, calls = [0.0], []
    monkeypatch.setattr(metadata.time, "perf_counter", lambda: now[0])

    def step(item):
        calls.append(item)
        now[0] += 0.06
        return PlanningProgress("pending", item)

    with metadata._preparing():
        result = metadata.advance_planning(
            SimpleNamespace(step=step),
            work,
            owner_kind="work",
            owner_id=work.work_id,
            maximum_steps=1000,
            maximum_seconds=0.1,
        )
    assert result.state == "pending" and len(calls) == 2


@pytest.mark.parametrize("outcome", ["question", "ready", "inapplicable", "no-output"])
def test_worker_stops_at_delivery_or_a_terminal_planning_result(
    work: WorkIdentity,
    outcome: Literal["question", "ready", "inapplicable", "no-output"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(metadata.time, "perf_counter", lambda: 0)
    states, calls = (
        iter(
            [
                PlanningProgress("pending", work),
                PlanningProgress("pending", work),
                PlanningProgress(outcome, work),
            ]
        ),
        [],
    )

    def step(item):
        calls.append(item)
        return next(states)

    with metadata._preparing():
        result = metadata.advance_planning(
            SimpleNamespace(step=step),
            work,
            owner_kind="preview",
            owner_id="fixture",
        )
    assert result.state == outcome and calls == [work] * 3


@pytest.mark.parametrize("steps,seconds", [(0, 0.1), (64, 0), (-1, 0.1)])
def test_invalid_worker_quantum_stops_before_planning(
    work: WorkIdentity,
    steps: int,
    seconds: float,
) -> None:
    planner = SimpleNamespace(step=lambda _: pytest.fail("must stop"))
    with pytest.raises(ValueError, match="quantum must be positive"):
        metadata.advance_planning(
            planner,
            work,
            owner_kind="work",
            owner_id=work.work_id,
            maximum_steps=steps,
            maximum_seconds=seconds,
        )


def test_worker_dispatches_finite_question_bursts_without_waiting_for_results(work, monkeypatch):
    monkeypatch.setattr(metadata.time, "perf_counter", lambda: 0)
    outcomes = iter([PlanningProgress("question", work)] * 13 + [PlanningProgress("ready", work)])
    planner = SimpleNamespace(step=lambda _: next(outcomes))
    delivered = []
    results = []
    with metadata._preparing():
        for _ in range(4):
            previous = len(delivered)
            result = metadata.advance_planning(
                planner,
                work,
                owner_kind="preview",
                owner_id="fixture",
                deliver=delivered.append,
            )
            assert len(delivered) - previous <= 4
            results.append(result.state)
    assert results == ["pending", "pending", "pending", "ready"]
    assert len(delivered) == 13


def test_sync_caller_does_not_dispatch_a_worker_burst(work):
    delivered = []
    result = metadata.advance_planning(
        SimpleNamespace(step=lambda item: PlanningProgress("question", item)),
        work,
        owner_kind="preview",
        owner_id="fixture",
        deliver=delivered.append,
    )
    assert result.state == "question" and not delivered

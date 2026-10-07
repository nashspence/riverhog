"""Planning projections live with their owners and expire without archive mutation."""

from sqlalchemy import func, select
from stove0_core.persistence import _OBSERVATION_TABLES, _PLANNING_CONTEXTS, _SELECTION_BUILDERS
from stove0_core.preview_state import PreviewRecord
from stove0_core.work_state import Stove0WorkService, WorkRecord
from stove0_protocol import (
    PlanningJobRequest,
    PreviewOutcome,
    WorkflowPreview,
    WorkflowPreviewPayload,
    WorkflowPreviewRequest,
    WorkflowPreviewRequestPayload,
)
from stove0_protocol.planning_jobs import PlanningJobPayload
from test_compiled_source_loss import _planned

CUTOFF = "2099-01-01T00:00:00.000000000Z"


def _count(state, table):
    with state.engine.connect() as connection:
        return connection.scalar(select(func.count()).select_from(table))


def test_active_work_keeps_its_accepted_planning_and_expired_terminal_work_releases_them(tmp_path):
    state, work, _decision, _member = _planned(tmp_path, {"first": True}, True)
    created = state.create(WorkRecord(work=work))
    assert _count(state, _OBSERVATION_TABLES["physical"]) == 1
    assert _count(state, _SELECTION_BUILDERS["builder"]) > 0
    kept = state.prune_operational_state(cutoff=CUTOFF)
    assert kept["work"] == kept["planning_contexts"] == 0
    assert state.accepted_observations.accepted(work.work_id, "first") is not None
    Stove0WorkService(state).cancel(work.work_id, expected_revision=created.revision)
    removed = state.prune_operational_state(cutoff=CUTOFF)
    assert removed["work"] == removed["planning_contexts"] == 1
    assert _count(state, _PLANNING_CONTEXTS) == 0
    assert _count(state, _OBSERVATION_TABLES["physical"]) == 0
    assert _count(state, _OBSERVATION_TABLES["records"]) == 0
    assert _count(state, _SELECTION_BUILDERS["builder"]) == 0


def test_completed_preview_expiry_does_not_remove_an_active_work_context(tmp_path):
    state, work, decision, _member = _planned(tmp_path, {"first": True}, True)
    state.create(WorkRecord(work=work))
    request = WorkflowPreviewRequest.seal(WorkflowPreviewRequestPayload(work=work))
    preview = WorkflowPreview.seal(
        WorkflowPreviewPayload(
            preview_id=request.preview_id,
            work=work,
            state="no_action",
            no_output_decision=decision,
            outcome=PreviewOutcome(
                code=decision.definition.code, message=decision.definition.message
            ),
        )
    )
    job = PlanningJobRequest.seal(PlanningJobPayload(work=work, invocation_id="d" * 64))
    state.create_preview(PreviewRecord(job=job, phase="completed", result=preview))
    preview_state = state.planning_context("preview", job.job_id)
    recipe, _closure = state.recipe_definitions.load(work.recipe)
    preview_state.compiled_planning.ensure(work.work_id, recipe)
    assert _count(state, _PLANNING_CONTEXTS) == 2
    removed = state.prune_operational_state(cutoff=CUTOFF)
    assert removed["previews"] == removed["planning_contexts"] == 1
    assert state.load_preview(job.job_id) is None
    assert _count(state, _PLANNING_CONTEXTS) == 1
    assert state.accepted_observations.accepted(work.work_id, "first") is not None

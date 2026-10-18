"""The maintained media catalog retains its decisions, evidence, and target semantics."""

from pathlib import Path

import pytest
from a_stove0_media_archive_contract_lib import SOURCE_ROLE, XMP_SOURCE_ROLE, MediaProjectionPolicy
from a_stove0_media_archive_lib.projection import resolve_media_archive_preflight_projection
from a_stove0_media_archive_lib.publication import accepted_source_hints
from media_planning_fixture import (
    ROOT,
    BatchMediaObservers,
    plan_until_complete,
    planner_for,
)


@pytest.mark.parametrize("batch_size", (1, 4))
def test_conformance_media_retains_overlapping_calls_exact_groups_and_original_evidence(
    tmp_path: Path,
    batch_size: int,
):
    planner = planner_for(tmp_path, observers=BatchMediaObservers(batch_size))
    work = planner.create_work("stove0.conformance-media/v1", (ROOT,))
    planner, progress, evidence = plan_until_complete(planner, work, restart=True)
    assert progress.state == "ready"
    decision = progress.decision
    actual = {
        branch.branch_id: tuple(
            sorted(
                subject.artifact_id
                for subject in decision.selection_documents[
                    branch.artifact_selection.selection_sha256
                ].artifacts
            )
        )
        for branch in decision.plan.branches
    }
    assert actual == {
        "archive-audio": tuple(f"{id:064x}" for id in (3, 4, 5)),
        "archive-audio-overlap": tuple(f"{id:064x}" for id in (3, 4, 5)),
        "archive-video": tuple(f"{id:064x}" for id in (7, 8)),
    }
    assert decision.plan.source_collection_retirement_policy == "retain"
    for branch in decision.plan.branches:
        request = planner.target_preflight_request(
            branch.workflow_plan,
            decision.selection_documents,
            descriptor=planner.targets.descriptor(branch.workflow_plan.target_registration_id),
        )
        projection = resolve_media_archive_preflight_projection(
            request,
            policy=MediaProjectionPolicy.model_validate(request.intent["metadata_projection"]),
        )
        hints, supports = accepted_source_hints(request)
        selected = decision.selection_documents[branch.artifact_selection.selection_sha256]
        assert set(hints) == {subject.id for subject in selected.artifacts}
        assert supports
        assert {item.input_artifact_id for item in projection.items} == {
            subject.id for subject in selected.artifacts if subject.role == SOURCE_ROLE
        }
        assert {item.input_artifact_id for item in projection.retained_xmp_sidecars} == {
            subject.id for subject in selected.artifacts if subject.role == XMP_SOURCE_ROLE
        }
        assert all(item in evidence for item in request.observations)
    questions = {item.request.task_id for item in evidence}
    assert {"metadata", "streams", "provenance", "hint", "filename"} <= questions
    for item in evidence:
        interface = (
            planner._definition(work)[1]
            .interface(
                id=item.request.interface.id,
                sha256=item.request.interface.sha256,
            )
            .interface
        )
        if interface.partitioning == "independent-subjects":
            assert len(item.request.subjects) <= batch_size
    planner.state.engine.dispose()


def test_changed_accepted_capture_time_changes_plan_without_changing_roles_or_relations(tmp_path):
    decisions = []
    for name, capture_time in (
        ("first", "2025:02:03 04:05:06-08:00"),
        ("changed", "2025:02:03 04:05:07-08:00"),
    ):
        path = tmp_path / name
        path.mkdir()
        planner = planner_for(path, observers=BatchMediaObservers(4, capture_time=capture_time))
        work = planner.create_work("stove0.conformance-media/v1", (ROOT,))
        planner, progress, evidence = plan_until_complete(planner, work, restart=True)
        assert progress.state == "ready"
        decisions.append(progress.decision)
        assert any(
            field["value"] == capture_time
            for item in evidence
            if item.request.task_id == "metadata"
            for artifact in item.result.facts["artifacts"]
            for field in artifact["facts"]
            if field["name"] == "capture-time"
        )
        planner.state.engine.dispose()
    first, changed = decisions
    assert first.plan.parent_work == changed.plan.parent_work
    assert first.plan.branch_set_sha256 != changed.plan.branch_set_sha256
    assert first.selection_documents == changed.selection_documents
    assert [branch.workflow_plan.input_groups for branch in first.plan.branches] == [
        branch.workflow_plan.input_groups for branch in changed.plan.branches
    ]
    assert [branch.workflow_plan.operation for branch in first.plan.branches] == [
        branch.workflow_plan.operation for branch in changed.plan.branches
    ]

"""Installed media stages consume exact controller-accepted named predecessors."""

from pathlib import Path

import pytest
from media_planning_fixture import (
    _PAYLOAD_SHA,
    ROOT,
    BatchMediaObservers,
    CatalogApi,
    plan_until_complete,
    planner_for,
)


@pytest.mark.parametrize("subject_count", (3, 9))
def test_dependent_stage_receives_complete_role_partitions_and_exact_predecessors(
    tmp_path: Path,
    subject_count: int,
):
    class StagedCatalog(CatalogApi):
        def search(self, **kwargs):
            return {
                "artifacts": [
                    {"artifact_id": f"{index:064x}", "bytes": 3, "sha256": _PAYLOAD_SHA}
                    for index in range(1, subject_count + 1)
                ]
            }

    planner = planner_for(tmp_path, observers=BatchMediaObservers(1), api=StagedCatalog())
    work = planner.create_work("stove0.conformance-media/v1", (ROOT,))
    planner, progress, evidence = plan_until_complete(planner, work, restart=True)
    assert progress.state in {"ready", "inapplicable"}
    metadata = [item for item in evidence if item.request.task_id == "metadata"]
    assert len(metadata) == subject_count
    assert (
        len({subject.artifact_id for item in metadata for subject in item.request.subjects})
        == subject_count
    )
    last_metadata = max(evidence.index(item) for item in metadata)
    streams = [item for item in evidence if item.request.task_id == "streams"]
    assert streams and all(evidence.index(item) > last_metadata for item in streams)
    assert all(
        subject.artifact_id not in {f"{index:064x}" for index in (3, 5, 8)}
        for item in streams
        for subject in item.request.subjects
    )
    filename = next(item for item in evidence if item.request.task_id == "filename")
    assert len(filename.request.subjects) == subject_count
    options = filename.request.options
    assert sorted(options["primary_ids"] + options["sidecar_ids"]) == sorted(
        member.id for member in filename.request.subjects
    )
    assert options["provenance_slots"] == [slot.slot for slot in filename.request.evidence_slots]
    delivered = planner.observation_delivery.inputs(filename.request)
    assert len(delivered) == 1
    source = delivered[0].authority.source
    assert source.question.task_id == "provenance"
    assert source.question.scope.artifact_count == subject_count
    accepted = planner.state.accepted_observations.accepted(work.work_id, "provenance")
    assert source == accepted
    original = planner.state.accepted_observations.evidence_page(
        source, authorize=lambda scope: None
    )
    assert {result.result_sha256 for result in original.results} == {
        item.result.result_sha256 for item in evidence if item.request.task_id == "provenance"
    }
    planner.state.engine.dispose()

"""Explicit collection exposure does not replace complete child settlement."""

from __future__ import annotations

import pytest
from stove0_protocol import (
    ArtifactSelection,
    BranchSetPlan,
    JoinSettlement,
    evaluate_branch_set,
    resolve_join_plan,
)
from stove0_protocol.fork_join import BranchCollectionExport
from test_fork_join import (
    artifact,
    branch_set_fixture,
    digest,
    root,
    settled_fixture,
)


def test_single_branch_export_exposes_exact_producer_without_a_dummy_join():
    original, selections, settlements = settled_fixture()
    plan = BranchSetPlan.seal(
        parent_work=original.parent_work,
        decision_sha256=original.decision_sha256,
        branches=original.branches,
        export=BranchCollectionExport(branch="video"),
        selections=selections,
    )
    pending = evaluate_branch_set(
        plan, selections, branch_settlements=(settlements["video"], settlements["audio"])
    )
    assert not pending.branch_set_succeeded and pending.coordination_settlement is None
    result = evaluate_branch_set(plan, selections, branch_settlements=tuple(settlements.values()))
    assert result.branch_set_succeeded and result.coordination_settlement is not None
    coordination, video = result.coordination_settlement, settlements["video"]
    assert len(coordination.children) == 3
    assert coordination.final_join_settlement_sha256 is None
    assert coordination.collection_result.producer_work_id == video.work_id
    assert (
        coordination.collection_result.producer_settlement_sha256
        == video.producer_settlement_sha256
    )
    assert coordination.collection_result.output_collection == video.output_collection
    assert coordination.collection_result.output_selection == video.output_selection
    assert coordination.collection_result.derivation_sha256 == video.derivation_sha256


@pytest.mark.parametrize("export", [None, "join"])
def test_join_completion_and_collection_export_are_independent(export):
    original, selections, settlements = settled_fixture()
    plan = BranchSetPlan.seal(
        parent_work=original.parent_work,
        decision_sha256=original.decision_sha256,
        branches=original.branches,
        join=original.join,
        export=export,
        selections=selections,
    )
    join_plan, inputs = resolve_join_plan(plan, selections, tuple(settlements.values()))
    selections.update((selection.selection_sha256, selection) for selection in inputs)
    output_root = root(990, "exported-join")
    output = ArtifactSelection.seal(
        (artifact("join-output", output_root, "joined.bin", role="archive.combined/v1"),)
    )
    selections[output.selection_sha256] = output
    join = JoinSettlement.seal(
        plan=join_plan,
        producer_settlement_sha256=digest("native-target-settlement"),
        derivation_sha256=digest("join-derivation"),
        output_collection=output_root,
        output_selection=output,
    )
    pending = evaluate_branch_set(plan, selections, branch_settlements=tuple(settlements.values()))
    assert not pending.branch_set_succeeded
    result = evaluate_branch_set(
        plan, selections, branch_settlements=tuple(settlements.values()), join_settlement=join
    )
    assert result.branch_set_succeeded
    coordination = result.coordination_settlement
    assert coordination.final_join_settlement_sha256 == join.settlement_sha256
    if export is None:
        assert coordination.collection_result is None
    else:
        assert coordination.collection_result.output_selection == output.ref()
        assert coordination.collection_result.producer_work_id == join.work_id
        assert (
            coordination.collection_result.producer_settlement_sha256
            == join.producer_settlement_sha256
        )


def test_collection_export_requires_an_exact_selected_producer():
    original, selections, _ = branch_set_fixture(with_join=False)
    with pytest.raises(ValueError, match="undeclared join"):
        BranchSetPlan.seal(
            parent_work=original.parent_work,
            decision_sha256=original.decision_sha256,
            branches=original.branches,
            export="join",
            selections=selections,
        )
    with pytest.raises(ValueError, match="unselected branch"):
        BranchSetPlan.seal(
            parent_work=original.parent_work,
            decision_sha256=original.decision_sha256,
            branches=original.branches,
            export=BranchCollectionExport(branch="missing"),
            selections=selections,
        )

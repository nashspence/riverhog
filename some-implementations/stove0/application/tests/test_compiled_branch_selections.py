"""One aggregate per branch, inclusive overlap and full invocation predicates."""

from __future__ import annotations

import pytest
from stove0_core.compiled_branches import CompiledBranchScope
from stove0_core.compiled_decisions import CompiledDecisionPlanning
from stove0_core.persistence import SqlAlchemyStateStore
from test_compiled_groups import _observed_state


def _branch(*, grouped=False, when=True):
    return {
        "select": {"groups": "media"} if grouped else "all",
        "when": when,
        "call": {"operation": "effect"},
    }


def _condition(path, value, *, scope="candidate", quantifier="any"):
    return {
        "facts": {
            "view": "relations.artifacts",
            "scope": scope,
            "quantifier": quantifier,
            "where": {
                "test": {
                    "path": path,
                    "op": "eq",
                    "value": value,
                }
            },
        }
    }


def _resolve_decisions(state, work):
    for _ in range(10):
        progress = CompiledDecisionPlanning(state).step(work)
        if progress.state == "branches":
            return
        assert progress.state == "pending"
    pytest.fail("the exact false decision did not resolve")


def _select(state, url, work, branch_id):
    for _ in range(5000):
        progress = state.compiled_branches.step(work, branch_id)
        if progress is not None:
            assert isinstance(progress, CompiledBranchScope)
            return state, progress
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    pytest.fail("compiled branch selection stopped making bounded progress")


def test_many_groups_form_one_required_aggregate_and_overlapping_branches_remain_inclusive(
    tmp_path,
):
    state, url, work, subjects = _observed_state(
        tmp_path,
        ("primary", "primary", "associated", "associated", "ignored"),
        preferred=((0, 2), (1, 3)),
        fork={"grouped": _branch(grouped=True), "whole": _branch()},
    )
    with pytest.raises(ValueError, match="resolved decisions"):
        state.compiled_branches.step(work, "grouped")
    _resolve_decisions(state, work)
    state, grouped = _select(state, url, work, "grouped")
    assert grouped.choice_count == grouped.selected_count == 2
    assert grouped.selection.artifact_count == 4
    assert grouped.inventory.artifact_count == 5
    state, whole = _select(state, url, work, "whole")
    assert whole.choice_count == whole.selected_count == 1
    assert grouped.selection == whole.selection
    assert grouped.branch_selection_sha256 != whole.branch_selection_sha256
    assert {
        member.id for member in state.iter_selection_artifacts(grouped.selection.selection_sha256)
    } == {member.id for member in subjects[:4]}
    choices = state.compiled_branches.choice_page(grouped, start_ordinal=0, limit=1)
    assert len(choices) == 1 and choices[0][0] == subjects[0].id
    assert choices[0][1].artifact_count == 2
    assert choices[0][2].candidate == choices[0][1]
    assert state.compiled_branches.choice_page(grouped, start_ordinal=2) == ()
    state.engine.dispose()


def test_candidate_condition_excludes_one_group_but_input_condition_keeps_unclassified_members(
    tmp_path,
):
    from test_accepted_observations import _subject

    when = {
        "all": [
            _condition("/subject_id", _subject(0).id),
            _condition("/kind", "ignored", scope="input"),
        ]
    }
    state, url, work, subjects = _observed_state(
        tmp_path,
        ("primary", "primary", "associated", "associated", "ignored"),
        preferred=((0, 2), (1, 3)),
        fork={
            "grouped": _branch(grouped=True, when=when),
            "input": _branch(
                when=_condition("/kind", "primary", scope="input", quantifier="every")
            ),
        },
    )
    _resolve_decisions(state, work)
    state, grouped = _select(state, url, work, "grouped")
    assert grouped.choice_count == 2 and grouped.selected_count == 1
    assert grouped.selection.artifact_count == 2
    assert {
        member.id for member in state.iter_selection_artifacts(grouped.selection.selection_sha256)
    } == {
        subjects[0].id,
        subjects[2].id,
    }
    proofs = state.compiled_branches.choice_page(grouped, start_ordinal=0)
    assert [proof.truth.value for _, _, proof in proofs] == ["true", "false"]
    for _, candidate, proof in proofs:
        assert candidate.artifact_count == 2
        assert proof.inventory.artifact_count == 5
        for evaluation in proof.evaluations:
            assert evaluation.binding.scope == (
                candidate if evaluation.binding.predicate.scope == "candidate" else proof.inventory
            )
    state, all_inputs = _select(state, url, work, "input")
    assert all_inputs.selected_count == all_inputs.selection.artifact_count == 0
    assert (
        state.compiled_branches.choice_page(all_inputs, start_ordinal=0)[0][2].truth.value
        == "false"
    )
    state.engine.dispose()


def test_crash_between_paged_union_append_and_branch_cas_does_not_duplicate_or_omit_members(
    tmp_path,
):
    state, url, work, subjects = _observed_state(
        tmp_path,
        ("primary",) + ("associated",) * 205,
        preferred=tuple((0, index) for index in range(1, 206)),
        fork={"archive": _branch(grouped=True)},
    )
    _resolve_decisions(state, work)
    original = state.metadata_selections.append
    lost = False

    def fail_after_commit(builder_id, **kwargs):
        nonlocal lost
        result = original(builder_id, **kwargs)
        row = state.metadata_selections.load(builder_id)
        if not lost and '"format":"stove0-branch-members-builder/v1"' in row["binding_json"]:
            assert len(kwargs["subjects"]) == 100
            lost = True
            raise RuntimeError("lost-process-after-append")
        return result

    state.metadata_selections.append = fail_after_commit
    for _ in range(4000):
        try:
            state.compiled_branches.step(work, "archive")
        except RuntimeError as exc:
            assert str(exc) == "lost-process-after-append"
            break
    else:
        pytest.fail("the partial-commit recovery witness did not reach the bounded copy")
    assert lost
    state.engine.dispose()
    state = SqlAlchemyStateStore(url)
    state, result = _select(state, url, work, "archive")
    assert result.selected_count == result.choice_count == 1
    assert result.selection.artifact_count == 206
    assert {
        member.id for member in state.iter_selection_artifacts(result.selection.selection_sha256)
    } == {member.id for member in subjects}
    state.engine.dispose()

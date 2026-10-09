"""Whole-input conditions retain their complete domain across bounded progress."""

from __future__ import annotations

import pytest
from sqlalchemy import func, select
from stove0_core.compiled_decisions import CompiledDecisionPlanning
from stove0_core.compiled_fact_evaluation import CompiledFactEvaluation
from stove0_core.compiled_observations import CompiledObservationPlanning
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_protocol import (
    ArtifactSelection,
    CollectionRootIdentityRef,
    WorkArtifactSubject,
    WorkIdentity,
    WorkPayload,
)
from stove0_protocol.no_output_decisions import CompiledNoOutputDecision, CompiledNoOutputPayload
from stove0_protocol.predicates import FactsQuantification, Truth
from test_compiled_observation_planning import _answer, _descriptor, _program


def _condition(quantifier, *, roles=None):
    return {
        "facts": {
            "view": "first.artifacts",
            "scope": "input",
            "quantifier": quantifier,
            "where": {
                "test": {"path": "/mime_type", "op": "eq", "value": "application/octet-stream"}
            },
            **({"roles": roles} if roles is not None else {}),
        }
    }


def _prepared(tmp_path, *, decision_condition=None, count=205):
    decisions = [
        {
            "when": _condition(quantifier, roles=roles),
            "no_output": {
                "code": "example.checked/v1",
                "message": f"The exact {quantifier} decision was evaluated.",
            },
        }
        for roles in (None, ["unused"])
        for quantifier in ("every", "any", "none")
    ]
    if decision_condition is not None:
        decisions = [
            {
                "when": decision_condition,
                "no_output": {
                    "code": "example.checked/v1",
                    "message": "The independent decision was evaluated.",
                },
            }
        ]
    recipe, closure = _program(input_classification=True, decisions=decisions)
    root = CollectionRootIdentityRef(
        collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
    )
    work = WorkIdentity.seal(WorkPayload(recipe=recipe.ref, inputs=(root,), effective_intent={}))
    subjects = tuple(
        WorkArtifactSubject(
            id=f"subject-{index:04}",
            role="stove0.source/v1",
            collection=root,
            artifact_id=f"{index + 1:064x}",
            bytes="1",
            sha256="c" * 64,
        )
        for index in range(count)
    )
    selection = ArtifactSelection.seal(subjects)
    url = f"sqlite:///{tmp_path / 'state.db'}"
    state = SqlAlchemyStateStore(url)
    state.recipe_definitions.retain(recipe, closure)
    state.retain_selection(selection)
    row = state.compiled_planning.ensure(work.work_id, recipe)
    state.compiled_planning.bind_inventory(
        work.work_id, expected_revision=row["revision"], scope=selection.ref()
    )
    return url, state, work, recipe, subjects


def test_classification_waits_for_whole_input_and_a_late_negative_changes_every_member(tmp_path):
    url, state, work, recipe, subjects = _prepared(tmp_path)
    with pytest.raises(ValueError, match="whole-invocation input facts"):
        state.compiled_planning.classify_step(work.work_id, recipe=recipe, views=lambda *_: None)
    delivered = set()
    for _ in range(100):
        driver = CompiledObservationPlanning(state, object())
        progress = driver.step(work)
        if progress.state == "question":
            question = progress.question
            assert question is not None
            assert question.task_id == "first"
            if question.task_id not in delivered:
                for offset in range(0, len(subjects), 100):
                    _answer(
                        state,
                        question,
                        _descriptor(),
                        subjects[offset : offset + 100],
                        mime_types={subjects[-1].id: "text/plain"},
                    )
                delivered.add(question.task_id)
        facts = state.compiled_planning.tables["facts"]
        with state.engine.connect() as connection:
            partial = connection.scalar(
                select(facts.c.evaluation_key).where(facts.c.state == "collecting")
            )
        if partial is not None:
            assert state.compiled_planning.ensure(work.work_id, recipe)["classified_count"] == 0
        if progress.state == "complete":
            break
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    else:
        pytest.fail("whole-input classification failed to make bounded progress")
    assert delivered == {"first"}
    classifications = state.compiled_planning.tables["classification"]
    with state.engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count())
                .select_from(classifications)
                .where(classifications.c.work_id == work.work_id, classifications.c.role.is_(None))
            )
            == 205
        )
    assert state.accepted_observations.accepted(work.work_id, "second").state == "complete-empty"
    assert state.accepted_observations.accepted(work.work_id, "empty").state == "complete-empty"
    # Quantification covers the original input even when classification assigned no role.
    expected = (Truth.FALSE, Truth.TRUE, Truth.FALSE, Truth.FALSE, Truth.FALSE, Truth.TRUE)
    for decision, answer in zip(recipe.decisions, expected, strict=True):
        for _ in range(4):
            result = CompiledFactEvaluation(state).step(work, decision.when.facts)
            if result is not None:
                break
            state.engine.dispose()
            state = SqlAlchemyStateStore(url)
        assert result == answer
        assert CompiledFactEvaluation(state).step(work, decision.when.facts) == answer
    undeclared = FactsQuantification.model_validate(
        {**recipe.decisions[0].when.facts.model_dump(mode="json"), "view": "second.artifacts"}
    )
    with pytest.raises(ValueError, match="not declared"):
        CompiledFactEvaluation(state).step(work, undeclared)
    for _ in range(20):
        progress = CompiledDecisionPlanning(state).step(work)
        if progress.state != "pending":
            break
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    else:
        pytest.fail("compiled decisions failed to make bounded progress")
    assert progress.state == "no-output"
    decision = progress.decision
    assert decision is not None
    assert recipe.decisions[0].no_output.code == recipe.decisions[1].no_output.code
    assert decision.decision_index == 1
    assert decision.definition == recipe.decisions[1].no_output
    assert [condition.truth for condition in decision.conditions] == [Truth.FALSE, Truth.TRUE]
    assert decision.conditions[1].evaluations[0].binding.view.task_id == "first"
    state.engine.dispose()
    state = SqlAlchemyStateStore(url)
    assert CompiledDecisionPlanning(state).step(work).decision == decision
    assert CompiledObservationPlanning(state, object()).step(work).state == "complete"
    wrong_definition = CompiledNoOutputDecision.seal(
        CompiledNoOutputPayload.model_validate(
            {
                **decision.model_dump(mode="json", exclude={"decision_sha256"}),
                "definition": recipe.decisions[0].no_output.model_dump(mode="json"),
            }
        )
    )
    with pytest.raises(ValueError, match="indexed compiled definition"):
        wrong_definition.verify_recipe(recipe)
    state.engine.dispose()


@pytest.mark.parametrize("decision_count", [0, 1, 2, 3])
def test_classification_cursor_is_reset_before_independent_decisions_and_restart(
    tmp_path, decision_count
):
    conditions = [
        {
            "facts": {
                "view": "first.artifacts",
                "scope": "input",
                "quantifier": "every",
                "where": {
                    "test": {
                        "path": "/mime_type",
                        "op": "ne",
                        "value": f"application/unrelated-{i}",
                    }
                },
            }
        }
        for i in range(decision_count)
    ]
    condition = {"all": conditions} if conditions else True
    url, state, work, recipe, subjects = _prepared(tmp_path, decision_condition=condition, count=3)
    for _ in range(100):
        progress = CompiledObservationPlanning(state, object()).step(work)
        if progress.state == "question":
            selected = tuple(
                member
                for subject in subjects
                if (
                    member := state.accepted_observations.selections.member(
                        progress.question.scope.selection_sha256, subject.id
                    )
                )
                is not None
            )
            _answer(state, progress.question, _descriptor(), selected)
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
        if progress.state == "complete":
            break
    else:
        pytest.fail("classification did not reach its decision boundary")
    assert state.compiled_planning.ensure(work.work_id, recipe)["input_ordinal"] == 0
    for _ in range(100):
        decision = CompiledDecisionPlanning(state).step(work)
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
        if decision.state != "pending":
            break
    assert decision.state == "no-output"
    assert len(decision.decision.conditions[0].evaluations) == decision_count
    assert CompiledDecisionPlanning(state).step(work).decision == decision.decision
    state.engine.dispose()

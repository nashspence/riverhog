"""Interpret only compiled policy over exact metadata and accepted task views."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence

from stove0_protocol import (
    WorkArtifactSubject,
    canonical_json_sha256,
)
from stove0_protocol.observation_views import (
    GlobalView,
    ProjectedView,
    RelationViewResult,
)
from stove0_protocol.predicates import (
    FactsQuantification,
    Predicate,
    Truth,
    evaluate_predicate,
    evaluate_row,
    quantify,
)
from stove0_recipe_config.compiled import CompiledRecipe


class ProgramEvidenceIndeterminate(RuntimeError):
    """Complete required evidence cannot establish this policy decision."""


TaskView = Callable[[str, str], ProjectedView | None]


def condition_truth(
    condition: Predicate,
    *,
    views: TaskView,
    inputs: Sequence[WorkArtifactSubject],
    candidate: Sequence[WorkArtifactSubject] | None = None,
    subject: WorkArtifactSubject | None = None,
    classified_ids: frozenset[str] | None = None,
    input_answers: Mapping[str, Truth] | None = None,
) -> Truth:
    def facts(predicate: FactsQuantification) -> Truth:
        if predicate.scope == "input" and input_answers is not None:
            key = canonical_json_sha256(predicate.model_dump(mode="json", by_alias=True))
            if key not in input_answers:
                raise ValueError(
                    "input fact condition has no completed whole-invocation evaluation"
                )
            return input_answers[key]
        task_id, view_id = predicate.view.split(".")
        view = views(task_id, view_id)
        if view is None:
            return Truth.INDETERMINATE
        if isinstance(view, RelationViewResult):
            raise ValueError("facts condition cannot consume a relation view")
        if isinstance(view, GlobalView):
            if predicate.scope != "input" or predicate.roles:
                raise ValueError("global view cannot become per-member or candidate evidence")
            return quantify(
                predicate.quantifier, (evaluate_row(predicate.where, row) for row in view.records)
            )
        scope = (
            inputs
            if predicate.scope == "input"
            else candidate
            if predicate.scope == "candidate"
            else ((subject,) if subject is not None else None)
        )
        if scope is None:
            raise ValueError("condition refers to an unavailable exact subject scope")
        if predicate.roles is not None:
            scope = tuple(
                member
                for member in scope
                if member.role in predicate.roles
                and (classified_ids is None or member.id in classified_ids)
            )

        def answers() -> Iterator[Truth]:
            for member in scope:
                if member.id not in view.rows or member.id not in view.statuses:
                    # The accepted view can be complete for a smaller declared
                    # task domain. It proves nothing about an unasked member.
                    # Omissions inside its own domain were rejected at acceptance.
                    yield Truth.INDETERMINATE
                    continue
                if view.statuses[member.id] != "complete":
                    yield Truth.INDETERMINATE
                    continue
                rows = view.rows[member.id]
                if not rows and predicate.quantifier == "every":
                    yield Truth.FALSE
                for row in rows:
                    yield evaluate_row(predicate.where, row)

        return quantify(predicate.quantifier, answers())

    return evaluate_predicate(condition, facts)


def classify_members(
    recipe: CompiledRecipe,
    members: Sequence[WorkArtifactSubject],
    views: TaskView,
    *,
    input_answers: Mapping[str, Truth] | None = None,
) -> tuple[tuple[WorkArtifactSubject, ...], tuple[WorkArtifactSubject, ...]]:
    classified, unclassified = [], []
    for member in members:
        role = recipe.classification.otherwise
        for case in recipe.classification.cases:
            truth = condition_truth(
                case.when, views=views, inputs=members, subject=member, input_answers=input_answers
            )
            if truth == Truth.INDETERMINATE:
                raise ProgramEvidenceIndeterminate(
                    f"classification of exact member {member.id} is indeterminate"
                )
            if truth == Truth.TRUE:
                role = case.role
                break
        if role is None:
            unclassified.append(member)
        else:
            classified.append(member.model_copy(update={"role": role}))
    return tuple(classified), tuple(unclassified)

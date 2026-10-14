"""First-true compiled no-output decisions retain their exact index and proof."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stove0_core.persistence import SqlAlchemyStateStore
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from stove0_protocol import (
    WorkIdentity,
    canonical_json_bytes,
    canonical_json_sha256,
)
from stove0_protocol.compiled_evidence import CompiledFactProof
from stove0_protocol.no_output_decisions import (
    CompiledConditionProof,
    CompiledConditionProofPayload,
    CompiledNoOutputDecision,
    CompiledNoOutputPayload,
)
from stove0_protocol.predicates import (
    FactsQuantification,
    Truth,
    evaluate_predicate,
    facts_predicates,
)

from stove0_core.compiled_fact_evaluation import CompiledFactEvaluation
from stove0_core.work_state import ConcurrentWorkUpdate, WorkInapplicable


def _fact_key(predicate: FactsQuantification) -> str:
    return canonical_json_sha256(predicate.model_dump(mode="json", by_alias=True))


@dataclass(frozen=True, slots=True)
class CompiledDecisionProgress:
    state: Literal["pending", "no-output", "branches", "inapplicable"]
    decision: CompiledNoOutputDecision | None = None
    outcome: WorkInapplicable | None = None


class CompiledDecisionPlanning:
    def __init__(self, state: SqlAlchemyStateStore) -> None:
        self.state = state
        self.facts = CompiledFactEvaluation(state)

    def step(self, work: WorkIdentity) -> CompiledDecisionProgress:
        retained = self.state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise ValueError("decision planning has no retained compiled closure")
        recipe, _ = retained
        planning = self.state.compiled_planning
        row = planning.ensure(work.work_id, recipe)
        inventory = planning.inventory_ref(work.work_id)
        if inventory is None or row["phase"] not in {"decisions", "complete"}:
            raise ValueError("decision planning requires the complete compiled observation graph")
        if row["decision_json"] is not None:
            retained_decision = CompiledNoOutputDecision.model_validate_json(row["decision_json"])
            retained_decision.verify_recipe(recipe)
            if (
                retained_decision.work_id != work.work_id
                or retained_decision.inventory != inventory
            ):
                raise ValueError("retained decision changed its original invocation")
            return CompiledDecisionProgress("no-output", retained_decision)
        index = row["decision_ordinal"]
        if index == len(recipe.decisions):
            return CompiledDecisionProgress("branches")
        if index > len(recipe.decisions):
            raise ValueError("decision continuation exceeds the exact compiled program")
        definition = recipe.decisions[index]
        predicates = {
            _fact_key(predicate): predicate for predicate in facts_predicates(definition.when)
        }
        keys = sorted(predicates)
        if row["input_ordinal"] < len(keys):
            if self.facts.proof_step(work, predicates[keys[row["input_ordinal"]]]) is not None:
                planning.advance(
                    work.work_id,
                    expected_revision=row["revision"],
                    input_ordinal=row["input_ordinal"] + 1,
                )
            return CompiledDecisionProgress("pending")
        if row["input_ordinal"] > len(keys):
            raise ValueError("decision fact continuation exceeds its exact predicate set")
        proofs: list[CompiledFactProof] = []
        for key in keys:
            proof = self.facts.proof_step(work, predicates[key])
            if proof is None:
                raise ValueError("decision lost a completed exact fact evaluation")
            proofs.append(proof)
        answers = {key: proof.truth for key, proof in zip(keys, proofs, strict=True)}
        truth = evaluate_predicate(definition.when, lambda predicate: answers[_fact_key(predicate)])
        condition = CompiledConditionProof.seal(
            CompiledConditionProofPayload.model_validate(
                {
                    "work_id": work.work_id,
                    "recipe": work.recipe,
                    "inventory": inventory,
                    "decision_index": str(index),
                    "condition": definition.when,
                    "evaluations": tuple(proofs),
                    "truth": truth,
                }
            )
        )
        if truth == Truth.INDETERMINATE:
            return CompiledDecisionProgress(
                "inapplicable",
                outcome=WorkInapplicable(
                    code="decision-evidence-indeterminate",
                    message="Accepted evidence cannot resolve the required no-output decision.",
                ),
            )
        p, c = planning.tables["planning"], planning.tables["conditions"]
        insert = pg_insert if self.state.engine.dialect.name == "postgresql" else sqlite_insert
        decision: CompiledNoOutputDecision | None = None
        with self.state.engine.begin() as connection:
            current = (
                connection.execute(
                    select(p)
                    .where(p.c.work_id == self.state.planning_key(work.work_id))
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if current["revision"] != row["revision"]:
                raise ConcurrentWorkUpdate("compiled decision advanced concurrently")
            document = canonical_json_bytes(
                condition.model_dump(mode="json", by_alias=True)
            ).decode("utf-8")
            connection.execute(
                insert(c)
                .values(
                    work_id=self.state.planning_key(work.work_id),
                    decision_ordinal=index,
                    proof_sha256=condition.proof_sha256,
                    proof_json=document,
                )
                .on_conflict_do_nothing()
            )
            if (
                connection.scalar(
                    select(c.c.proof_json).where(
                        c.c.work_id == self.state.planning_key(work.work_id),
                        c.c.decision_ordinal == index,
                    )
                )
                != document
            ):
                raise ValueError("indexed decision proof was rebound")
            if truth == Truth.TRUE:
                previous = connection.scalars(
                    select(c.c.proof_json)
                    .where(
                        c.c.work_id == self.state.planning_key(work.work_id),
                        c.c.decision_ordinal <= index,
                    )
                    .order_by(c.c.decision_ordinal)
                ).all()
                decision = CompiledNoOutputDecision.seal(
                    CompiledNoOutputPayload.model_validate(
                        {
                            "work_id": work.work_id,
                            "recipe": work.recipe,
                            "inventory": inventory,
                            "decision_index": str(index),
                            "definition": definition.no_output,
                            "conditions": tuple(
                                CompiledConditionProof.model_validate_json(d) for d in previous
                            ),
                        }
                    )
                )
                decision.verify_recipe(recipe)
            changed = connection.execute(
                update(p)
                .where(
                    p.c.work_id == self.state.planning_key(work.work_id),
                    p.c.revision == row["revision"],
                )
                .values(
                    revision=row["revision"] + 1,
                    input_ordinal=0,
                    decision_ordinal=index if decision is not None else index + 1,
                    phase="complete" if decision is not None else "decisions",
                    decision_json=None
                    if decision is None
                    else canonical_json_bytes(
                        decision.model_dump(mode="json", by_alias=True)
                    ).decode("utf-8"),
                )
            )
            if changed.rowcount != 1:
                raise ConcurrentWorkUpdate("compiled decision advanced concurrently")
        return (
            CompiledDecisionProgress("pending")
            if decision is None
            else CompiledDecisionProgress("no-output", decision)
        )

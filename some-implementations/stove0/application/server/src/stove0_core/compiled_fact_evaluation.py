"""Resume whole-invocation fact quantification without substituting a page for its scope."""

from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from stove0_protocol import (
    ArtifactSelectionRef,
    WorkIdentity,
    canonical_json_bytes,
    canonical_json_sha256,
)
from stove0_protocol.compiled_evidence import (
    CompiledFactBinding,
    CompiledFactProof,
    CompiledFactProofPayload,
)
from stove0_protocol.observation_interfaces import GlobalFactsView, SubjectFactsView
from stove0_protocol.observation_views import SubjectView
from stove0_protocol.predicates import (
    FactsQuantification,
    Truth,
    evaluate_row,
    facts_predicates,
    quantify,
)

from stove0_core.accepted_view_queries import accepted_view_queries
from stove0_core.compiled_state_ports import CompiledStatePort
from stove0_core.subject_identity import subject_identity_sha256
from stove0_core.work_state import ConcurrentWorkUpdate


class CompiledFactEvaluation:
    def __init__(self, state: CompiledStatePort) -> None:
        self.state = state

    def step(
        self,
        work: WorkIdentity,
        predicate: FactsQuantification,
        *,
        candidate: ArtifactSelectionRef | None = None,
        limit: int = 100,
    ) -> Truth | None:
        proof = self.proof_step(work, predicate, candidate=candidate, limit=limit)
        return None if proof is None else proof.truth

    def proof_step(
        self,
        work: WorkIdentity,
        predicate: FactsQuantification,
        *,
        candidate: ArtifactSelectionRef | None = None,
        limit: int = 100,
    ) -> CompiledFactProof | None:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("fact evaluation page budget must be between 1 and 100")
        if predicate.scope == "self":
            raise ValueError("per-member facts are evaluated within their one exact member")
        retained = self.state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise ValueError("fact evaluation has no retained compiled recipe closure")
        recipe, closure = retained
        predicate_document = predicate.model_dump(mode="json", by_alias=True)
        declared = (
            *[case.when for case in recipe.classification.cases],
            *[decision.when for decision in recipe.decisions],
            *[branch.when for branch in recipe.branches.values()],
        )
        if not any(
            canonical_json_sha256(predicate_document)
            == canonical_json_sha256(candidate.model_dump(mode="json", by_alias=True))
            for condition in declared
            for candidate in facts_predicates(condition)
        ):
            raise ValueError("fact evaluation is not declared by the retained compiled program")
        planning = self.state.compiled_planning
        plan = planning.ensure(work.work_id, recipe)
        inventory = planning.inventory_ref(work.work_id)
        if inventory is None:
            raise ValueError("fact evaluation has no original invocation inventory")
        scope = inventory if predicate.scope == "input" else candidate
        if scope is None or self.state.load_selection_ref(scope.selection_sha256) != scope:
            raise ValueError("fact evaluation requires an exact sealed subject scope")
        if predicate.roles is not None and plan["classified_count"] != inventory.artifact_count:
            raise ValueError("fact role filters require complete invocation classification")
        task_id, view_id = predicate.view.split(".")
        task = recipe.observations.get(task_id)
        if task is None:
            raise ValueError("fact condition names an undeclared compiled task")
        resource = closure.interface(id=task.interface.id, sha256=task.interface.sha256)
        definition = resource.interface.views[view_id]
        if not isinstance(definition, (GlobalFactsView, SubjectFactsView)):
            raise ValueError("fact condition cannot quantify a relation view")
        accepted = self.state.accepted_observations.accepted(work.work_id, task_id)
        if accepted is None:
            return None
        authority = self.state.accepted_observations.retained_view(
            accepted, view_id=view_id, selected_scope=accepted.question.scope
        )
        if authority is None:
            return None
        binding_document = {
            "format": "stove0-compiled-fact-evaluation/v1",
            "work_id": work.work_id,
            "recipe": work.recipe.model_dump(mode="json"),
            "predicate": predicate_document,
            "inventory": inventory.model_dump(mode="json"),
            "scope": scope.model_dump(mode="json"),
            "view": authority.model_dump(mode="json"),
        }
        binding = CompiledFactBinding.model_validate(binding_document)
        key = binding.evaluation_key
        encoded = canonical_json_bytes(binding.model_dump(mode="json", by_alias=True)).decode(
            "utf-8"
        )
        table = planning.tables["facts"]
        insert = pg_insert if self.state.engine.dialect.name == "postgresql" else sqlite_insert
        with self.state.engine.begin() as connection:
            connection.execute(
                insert(table)
                .values(
                    evaluation_key=self.state.planning_key(key),
                    work_id=self.state.planning_key(work.work_id),
                    binding_json=encoded,
                    scope_sha256=scope.selection_sha256,
                    revision=1,
                    state="collecting",
                    record_ordinal=0,
                    positive=False,
                    negative=False,
                    indeterminate=False,
                )
                .on_conflict_do_nothing()
            )
            row = dict(
                connection.execute(
                    select(table).where(table.c.evaluation_key == self.state.planning_key(key))
                )
                .mappings()
                .one()
            )
        if row["binding_json"] != encoded:
            raise ValueError("fact evaluation changed its exact policy, scope or accepted view")
        if row["state"] == "complete":
            if row["result_json"] is None:
                raise ValueError("completed fact evaluation lost its exact result preimage")
            retained_proof = CompiledFactProof.model_validate_json(row["result_json"])
            if (
                retained_proof.binding.evaluation_key != key
                or retained_proof.truth != row["truth"]
                or (retained_proof.positive, retained_proof.negative, retained_proof.indeterminate)
                != (row["positive"], row["negative"], row["indeterminate"])
            ):
                raise ValueError("completed fact proof differs from its retained evaluation")
            return retained_proof
        answers = []
        continuation, ordinal = row["continuation"], row["record_ordinal"]
        if isinstance(definition, GlobalFactsView):
            if predicate.scope != "input" or predicate.roles is not None:
                raise ValueError("global facts cannot become per-member or role-filtered evidence")
            page = self.state.accepted_observations.view_page(
                authority, start_ordinal=ordinal, limit=limit, authorize=lambda _: None
            )
            for record in page.records:
                if record.kind != "global":
                    raise ValueError("global fact authority contains another record kind")
                answers.append(evaluate_row(predicate.where, record.value))
            ordinal += len(page.records)
            complete = page.complete
        else:
            view = accepted_view_queries(
                self.state.accepted_observations, authority, interface=resource.interface
            )
            if not isinstance(view, SubjectView):
                raise ValueError("subject fact evaluation selected another view kind")
            subjects, continuation, complete = self.state.selection_artifact_page(
                scope.selection_sha256, continuation=continuation, limit=limit
            )
            for subject in subjects:
                original = self.state.load_selection_artifact(
                    inventory.selection_sha256, subject.id
                )
                if original is None or subject_identity_sha256(subject) != subject_identity_sha256(
                    original
                ):
                    raise ValueError("fact candidate escapes or changes the original invocation")
                if predicate.roles is not None:
                    classification = planning.tables["classification"]
                    with self.state.engine.connect() as connection:
                        role = connection.scalar(
                            select(classification.c.role).where(
                                classification.c.work_id == self.state.planning_key(work.work_id),
                                classification.c.subject_id == subject.id,
                            )
                        )
                    if role not in predicate.roles:
                        continue
                if (
                    subject.id not in view.rows
                    or subject.id not in view.statuses
                    or view.statuses[subject.id] != "complete"
                ):
                    answers.append(Truth.INDETERMINATE)
                    continue
                records = view.rows[subject.id]
                if not records and predicate.quantifier == "every":
                    answers.append(Truth.FALSE)
                answers.extend(evaluate_row(predicate.where, record) for record in records)
        positive = row["positive"] or Truth.TRUE in answers
        negative = row["negative"] or Truth.FALSE in answers
        indeterminate = row["indeterminate"] or Truth.INDETERMINATE in answers
        truth = None
        proof: CompiledFactProof | None = None
        if complete:
            truth = quantify(
                predicate.quantifier,
                (
                    value
                    for present, value in (
                        (positive, Truth.TRUE),
                        (negative, Truth.FALSE),
                        (indeterminate, Truth.INDETERMINATE),
                    )
                    if present
                ),
            )
            proof = CompiledFactProof.seal(
                CompiledFactProofPayload(
                    binding=binding,
                    positive=positive,
                    negative=negative,
                    indeterminate=indeterminate,
                    truth=truth,
                )
            )
        with self.state.engine.begin() as connection:
            changed = connection.execute(
                update(table)
                .where(
                    table.c.evaluation_key == self.state.planning_key(key),
                    table.c.revision == row["revision"],
                    table.c.state == "collecting",
                )
                .values(
                    revision=row["revision"] + 1,
                    continuation=continuation,
                    record_ordinal=ordinal,
                    positive=positive,
                    negative=negative,
                    indeterminate=indeterminate,
                    truth=truth,
                    result_json=None
                    if proof is None
                    else canonical_json_bytes(proof.model_dump(mode="json", by_alias=True)).decode(
                        "utf-8"
                    ),
                    state="complete" if complete else "collecting",
                )
            )
            if changed.rowcount != 1:
                raise ConcurrentWorkUpdate("fact evaluation advanced concurrently")
        return proof

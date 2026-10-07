"""Controller endorsement of exact named, accepted per-member loss verdicts."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import JsonValue
from stove0_protocol import WorkIdentity
from stove0_protocol.no_output_decisions import CompiledNoOutputDecision

if TYPE_CHECKING:
    from stove0_core.persistence import SqlAlchemyStateStore

from riverhog_protocol.collection_workflows import (
    ArtifactDiscardApproval,
    CollectionArtifactIdentity,
)
from sqlalchemy import select
from stove0_protocol import WorkArtifactSubject, canonical_json_bytes, canonical_json_sha256
from stove0_protocol.observation_evidence import AcceptedViewRecord
from stove0_protocol.observation_interfaces import SubjectFactsView


def planning_root(state: SqlAlchemyStateStore, work: WorkIdentity) -> SqlAlchemyStateStore:
    """Child frames share their root's planning owner, not another semantic work."""
    seen: set[str] = set()
    while work.fork_join is not None:
        if work.work_id in seen:
            raise ValueError("planning ownership contains a cycle")
        seen.add(work.work_id)
        parent = state.load(work.fork_join.parent_work_id)
        if parent is None:
            raise ValueError("parent-bound settlement lost its planning owner")
        work = parent.work
    return state.planning_context("work", work.work_id)


class SourceLossEvaluation:
    """Every declared task/view must affirm the same immutable Riverhog member.

    Riverhog retains original successful request/result documents. Stove0 alone
    interprets its compiled view and verdict declarations; a contract ID cannot
    substitute for the named task or its exact accepted view.
    """

    def __init__(
        self, state: SqlAlchemyStateStore, work: WorkIdentity, decision: CompiledNoOutputDecision
    ) -> None:
        retained = state.recipe_definitions.load(work.recipe)
        if retained is None:
            raise ValueError("source loss has no retained exact compiled recipe")
        recipe, closure = retained
        decision.verify_recipe(recipe)
        if decision.work_id != work.work_id:
            raise ValueError("source loss decision belongs to another invocation")
        rule = decision.definition.source_loss
        if rule is None:
            raise ValueError("source loss requires an explicit indexed decision rule")
        self.state, self.work, self.rule = state, work, rule
        self.bindings = []
        for required in rule.evidence:
            task_id, view_id = required.view.split(".")
            task = recipe.observations[task_id]
            resource = closure.interface(id=task.interface.id, sha256=task.interface.sha256)
            if not isinstance(resource.interface.views[view_id], SubjectFactsView):
                raise ValueError("source loss requires an exact per-subject fact view")
            accepted = state.accepted_observations.accepted(work.work_id, task_id)
            if accepted is None or accepted.question.interface != resource.interface.ref:
                raise ValueError("source loss lacks its exact completed named task")
            authority = state.accepted_observations.retained_view(
                accepted,
                view_id=view_id,
                selected_scope=accepted.question.scope,
            )
            if authority is None:
                raise ValueError("source loss lacks its complete original accepted view")
            self.bindings.append((required, resource, authority))

    @property
    def declaration(self) -> dict[str, JsonValue]:
        slots = sorted(
            {
                (
                    resource.contract.id,
                    resource.contract.contract_sha256,
                    resource.contract.facts_schema.profile_sha256,
                )
                for _, resource, _ in self.bindings
            }
        )
        return {
            "rule": self.rule.model_dump(mode="json", by_alias=True),
            "rule_sha256": self.rule.sha256,
            "evidence_slots": [
                {"id": id, "contract_sha256": contract, "profile_sha256": profile}
                for id, contract, profile in slots
            ],
            "accepted_views": [
                {
                    "name": required.view,
                    "authority": authority.model_dump(mode="json", by_alias=True),
                }
                for required, _, authority in self.bindings
            ],
        }

    def approval(
        self, subject: CollectionArtifactIdentity, *, controller_id: str, reason: str
    ) -> tuple[ArtifactDiscardApproval | None, tuple[tuple[str, dict[str, JsonValue]], ...]]:
        records = self.state.accepted_observations.tables["records"]
        members = self.state.accepted_observations.members
        physical_key = canonical_json_sha256(subject.as_dict())
        documents: dict[str, dict[str, JsonValue]] = {}
        slots: dict[tuple[str, str, str], str] = {}
        for required, resource, authority in self.bindings:
            with self.state.engine.connect() as connection:
                candidates = connection.scalars(
                    select(members.c.document_json)
                    .where(
                        members.c.selection_sha256 == authority.selected_scope.selection_sha256,
                        members.c.artifact_identity_sha256 == physical_key,
                    )
                    .limit(2)
                ).all()
            if len(candidates) != 1:
                return None, ()
            member = WorkArtifactSubject.model_validate_json(candidates[0])
            if canonical_json_bytes(member.model_dump(mode="json", exclude={"id", "role"})) != (
                canonical_json_bytes(subject.as_dict())
            ):
                raise ValueError("source loss membership index changed immutable member facts")
            with self.state.engine.connect() as connection:
                rows = connection.scalars(
                    select(records.c.record_json)
                    .where(
                        records.c.question_sha256
                        == self.state.planning_key(authority.question_sha256),
                        records.c.view_id == authority.view_id,
                        records.c.subject_id == member.id,
                    )
                    .order_by(records.c.request_id, records.c.record_ordinal)
                    .limit(3)
                ).all()
            parsed = [AcceptedViewRecord.model_validate_json(document) for document in rows]
            coverage = [row for row in parsed if row.kind == "coverage"]
            facts = [row for row in parsed if row.kind == "subject"]
            if (
                len(parsed) != 2
                or len(coverage) != 1
                or coverage[0].value != "complete"
                or len(facts) != 1
            ):
                return None, ()
            fact = facts[0]
            present, verdict = _pointer(fact.value, required.verdict.path)
            if not present or canonical_json_bytes(verdict) != canonical_json_bytes(
                required.verdict.equals
            ):
                return None, ()
            slot_key = (
                resource.contract.id,
                resource.contract.contract_sha256,
                resource.contract.facts_schema.profile_sha256,
            )
            for support in fact.support:
                evidence = self.state.accepted_observations.original_evidence(
                    support.request_id,
                    authorize=lambda scope: None,
                )
                result = evidence.result
                if (
                    evidence.request.work_id != self.work.work_id
                    or evidence.request.question_sha256 != authority.question_sha256
                    or evidence.request.observer_contract_id != slot_key[0]
                    or evidence.request.observer_contract_sha256 != slot_key[1]
                    or result.result_sha256 != support.result_sha256
                    or result.facts_schema != resource.contract.facts_schema
                    or not any(
                        candidate.id == member.id
                        and canonical_json_bytes(
                            candidate.model_dump(mode="json", exclude={"id", "role"})
                        )
                        == canonical_json_bytes(subject.as_dict())
                        for candidate in evidence.request.subjects
                    )
                ):
                    raise ValueError(
                        "source loss support changed its exact task or immutable member"
                    )
                document: dict[str, JsonValue] = {
                    "request": evidence.request.model_dump(
                        mode="json", by_alias=True, exclude_none=True
                    ),
                    "result": result.model_dump(mode="json", by_alias=True, exclude_none=True),
                }
                digest = canonical_json_sha256(document)
                documents[digest] = document
                # The native slot is a contract-qualified successful-document
                # witness. All named view obligations above are checked and all
                # supporting documents are retained, including repeated tasks.
                slots[slot_key] = min(slots.get(slot_key, digest), digest)
        consideration = {
            "format": "riverhog-artifact-consideration/v1",
            "subject": subject.as_dict(),
            "slots": [
                {
                    "id": key[0],
                    "contract_sha256": key[1],
                    "profile_sha256": key[2],
                    "document_sha256": digest,
                }
                for key, digest in sorted(slots.items())
            ],
            "reason": reason,
        }
        approval = ArtifactDiscardApproval(
            controller_id=controller_id,
            rule_sha256=self.rule.sha256,
            evidence_json=canonical_json_bytes(consideration).decode("utf-8"),
            evidence_sha256=canonical_json_sha256(consideration),
        )
        return approval, tuple(sorted(documents.items()))


def _pointer(document: JsonValue, pointer: str) -> tuple[bool, JsonValue]:
    current = document
    if pointer == "":
        return True, current
    for raw in pointer.split("/")[1:]:
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict) and token in current:
            current = current[token]
        elif isinstance(current, list) and token.isdigit() and int(token) < len(current):
            current = current[int(token)]
        else:
            return False, None
    return True, current

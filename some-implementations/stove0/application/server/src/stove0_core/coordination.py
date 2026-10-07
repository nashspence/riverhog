"""Derived fork/join truth over ordinary durable Stove0 work records."""

from __future__ import annotations

from dataclasses import dataclass

from stove0_protocol import (
    ArtifactSelection,
    BranchEffectSettlement,
    BranchNoOutputSettlement,
    BranchOutcome,
    BranchOutcomeState,
    BranchPlan,
    BranchSetEvaluation,
    BranchSetPlan,
    BranchSettlement,
    CollectionRootIdentityRef,
    CoordinationBranchPlan,
    CoordinationSettlement,
    JoinOutcome,
    JoinOutcomeState,
    JoinPlan,
    JoinSettlement,
    NoOutputBranchPlan,
    WorkArtifactSubject,
    branch_work,
    evaluate_branch_set,
    resolve_join_plan,
)
from stove0_protocol.fork_join import BranchCollectionExport, CoordinationCollectionResult
from stove0_target_protocol import OutputCollectionRef

from stove0_core.work_state import WorkRecord, WorkStore


@dataclass(frozen=True, slots=True)
class CoordinationProjection:
    """Current semantic graph view plus an optional join admission."""

    evaluation: BranchSetEvaluation
    selection_documents: dict[str, ArtifactSelection]
    pending_join: JoinPlan | None = None
    pending_join_selections: tuple[ArtifactSelection, ...] = ()


def project_coordination(parent: WorkRecord, store: WorkStore) -> CoordinationProjection:
    """Project one branch set without persisting another workflow state machine."""

    plan = parent.branch_set_plan
    if plan is None or parent.work != plan.parent_work:
        raise ValueError("coordination parent has no matching branch-set plan")

    selections = _declared_selections(parent, store)
    settlements: list[BranchSettlement] = []
    effect_settlements: list[BranchEffectSettlement] = []
    no_output_settlements: list[BranchNoOutputSettlement] = []
    coordination_settlements: list[CoordinationSettlement] = []
    outcomes: list[BranchOutcome] = []
    branch_sets = _branch_set_documents(parent, store)
    for branch in plan.branches:
        child = _load_declared_work(store, branch_work(branch).work_id)
        if isinstance(branch, CoordinationBranchPlan):
            expected_plan = branch_sets[branch.branch_set_sha256]
            if child.branch_set_plan != expected_plan:
                raise RuntimeError(
                    "durable coordination child differs from its sealed branch-set plan"
                )
            settlement = child.coordination_settlement
            if settlement is not None and child.phase == "complete":
                _verify_collection_export(child, store, selections)
                coordination_settlements.append(settlement)
                continue
            if child.phase == "complete":
                raise RuntimeError("completed coordination child has no exact settlement")
            outcome = _coordination_outcome(branch, child)
            if outcome is not None:
                outcomes.append(outcome)
            continue
        if isinstance(branch, NoOutputBranchPlan):
            preview = child.no_action_preview
            if (
                preview is None
                or preview.work != branch.work
                or preview.observations != branch.observations
                or preview.outcome != branch.outcome
            ):
                raise RuntimeError("durable no-output child differs from its selected decision")
            if child.phase == "no_action":
                if child.no_output_settlement_sha256 is None:
                    raise RuntimeError("successful no-output child has no Riverhog settlement")
                no_output_settlements.append(
                    BranchNoOutputSettlement.seal(
                        branch=branch,
                        no_output_settlement_sha256=child.no_output_settlement_sha256,
                    )
                )
                continue
            outcome = _branch_outcome(branch, child)
            if outcome is not None:
                outcomes.append(outcome)
            continue
        assert isinstance(branch, BranchPlan)
        if child.workflow_plan != branch.workflow_plan:
            raise RuntimeError("durable branch child differs from its sealed workflow plan")
        branch_settlement = _branch_settlement(child)
        if branch_settlement is not None:
            if child.target_settlement is None:
                raise RuntimeError("settled branch lacks post-root target settlement")
            output_selection = _output_selection(child, store)
            _retain_selection(selections, output_selection)
            settlements.append(
                BranchSettlement.seal(
                    branch=branch,
                    derivation_sha256=branch_settlement.derivation_sha256,
                    producer_settlement_sha256=(child.target_settlement.settlement_sha256),
                    output_collection=_collection_root(branch_settlement),
                    output_selection=output_selection,
                )
            )
            continue
        effect_settlement = _branch_effect_settlement(branch, child)
        if effect_settlement is not None:
            effect_settlements.append(effect_settlement)
            continue
        outcome = _branch_outcome(branch, child)
        if outcome is not None:
            outcomes.append(outcome)

    resolution = resolve_join_plan(
        plan,
        selections,
        settlements,
        effect_settlements,
        coordination_settlements,
        branch_sets,
    )
    pending_join: JoinPlan | None = None
    pending_join_selections: tuple[ArtifactSelection, ...] = ()
    join_settlement: JoinSettlement | None = None
    join_outcome: JoinOutcome | None = None
    if parent.join_plan is not None:
        if resolution is None or parent.join_plan != resolution[0]:
            raise RuntimeError("durable join plan differs from exact branch settlements")
        join_plan, join_selections = resolution
        for selection in join_selections:
            _retain_selection(selections, selection)
        join_record = _load_declared_work(store, join_plan.work.work_id)
        if join_record.workflow_plan != join_plan.workflow_plan:
            raise RuntimeError("durable join child differs from its sealed workflow plan")
        join_settlement = _join_settlement(join_plan, join_record, selections, store)
        if join_settlement is None:
            join_outcome = _join_outcome(join_plan, join_record)
    elif resolution is not None:
        pending_join, pending_join_selections = resolution
        for selection in pending_join_selections:
            _retain_selection(selections, selection)

    evaluation = evaluate_branch_set(
        plan,
        selections,
        branch_settlements=settlements,
        branch_effect_settlements=effect_settlements,
        branch_no_output_settlements=no_output_settlements,
        branch_coordination_settlements=coordination_settlements,
        branch_outcomes=outcomes,
        branch_sets=branch_sets,
        join_settlement=join_settlement,
        join_outcome=join_outcome,
    )
    return CoordinationProjection(
        evaluation=evaluation,
        selection_documents=selections,
        pending_join=pending_join,
        pending_join_selections=pending_join_selections,
    )


def _verify_collection_export(
    child: WorkRecord, store: WorkStore, selections: dict[str, ArtifactSelection]
) -> None:
    """Follow only the explicit sealed export chain to its actual producer.

    No collection is copied or manufactured; the result still names the exact
    leaf's settled Riverhog output. Iteration imposes no child-depth ceiling.
    """
    settlement = child.coordination_settlement
    plan = child.branch_set_plan
    if (
        settlement is None
        or plan is None
        or settlement.work != child.work
        or settlement.branch_set_sha256 != plan.branch_set_sha256
    ):
        raise RuntimeError("coordination export has no exact parent settlement")
    result = settlement.collection_result
    if (plan.export is None) != (result is None):
        raise RuntimeError("coordination result differs from its explicit export")
    if result is None:
        return
    current, seen = child, set()
    while True:
        if current.work_id in seen:
            raise RuntimeError("coordination export contains a cycle")
        seen.add(current.work_id)
        plan = current.branch_set_plan
        if plan is None:
            raise RuntimeError("export chain lost its exact child plan")
        if plan.export == "join":
            producer_plan = current.join_plan
            if producer_plan is None or producer_plan.work.work_id != result.producer_work_id:
                raise RuntimeError("coordination export producer differs from its declared join")
            producer = _load_declared_work(store, producer_plan.work.work_id)
            if producer.workflow_plan != producer_plan.workflow_plan:
                raise RuntimeError("exported join work changed its exact plan")
            proof = _join_settlement(producer_plan, producer, selections, store)
            if (
                proof is None
                or current.coordination_settlement is None
                or proof.settlement_sha256
                != current.coordination_settlement.final_join_settlement_sha256
            ):
                raise RuntimeError("exported join has no exact successful settlement")
            expected = CoordinationCollectionResult(
                producer_work_id=proof.work_id,
                producer_settlement_sha256=proof.producer_settlement_sha256,
                derivation_sha256=proof.derivation_sha256,
                output_collection=proof.output_collection,
                output_selection=proof.output_selection,
            )
            break
        if not isinstance(plan.export, BranchCollectionExport):
            raise RuntimeError("export chain reached a coordinator without an export")
        branch = next(item for item in plan.branches if item.branch_id == plan.export.branch)
        producer = _load_declared_work(store, branch_work(branch).work_id)
        if isinstance(branch, CoordinationBranchPlan):
            nested = producer.coordination_settlement
            if (
                producer.phase != "complete"
                or nested is None
                or nested.work != branch.work
                or nested.branch_set_sha256 != branch.branch_set_sha256
                or nested.collection_result != result
            ):
                raise RuntimeError("nested export changed its exact settled collection")
            current = producer
            continue
        if (
            not isinstance(branch, BranchPlan)
            or producer.work_id != result.producer_work_id
            or producer.workflow_plan != branch.workflow_plan
        ):
            raise RuntimeError("export does not name its exact collection-producing leaf")
        verification = _branch_settlement(producer)
        if verification is None or producer.target_settlement is None:
            raise RuntimeError("exported leaf has no successful producer settlement")
        output = _output_selection(producer, store)
        _retain_selection(selections, output)
        leaf_proof = BranchSettlement.seal(
            branch=branch,
            derivation_sha256=verification.derivation_sha256,
            producer_settlement_sha256=producer.target_settlement.settlement_sha256,
            output_collection=_collection_root(verification),
            output_selection=output,
        )
        expected = CoordinationCollectionResult(
            producer_work_id=leaf_proof.work_id,
            producer_settlement_sha256=leaf_proof.producer_settlement_sha256,
            derivation_sha256=leaf_proof.derivation_sha256,
            output_collection=leaf_proof.output_collection,
            output_selection=leaf_proof.output_selection,
        )
        break
    if expected != result:
        raise RuntimeError("coordination export differs from its exact producer output")


def _declared_selections(
    parent: WorkRecord,
    store: WorkStore,
) -> dict[str, ArtifactSelection]:
    assert parent.branch_set_plan is not None
    documents: dict[str, ArtifactSelection] = {}
    for branch in parent.branch_set_plan.branches:
        digest = branch.artifact_selection.selection_sha256
        selection = store.load_selection(digest)
        if selection is None:
            raise RuntimeError(f"durable branch selection is unavailable: {digest}")
        _retain_selection(documents, selection)
    return documents


def _branch_set_documents(
    parent: WorkRecord,
    store: WorkStore,
) -> dict[str, BranchSetPlan]:
    if parent.branch_set_plan is None:
        raise RuntimeError("coordination has no branch-set plan")
    documents: dict[str, BranchSetPlan] = {
        parent.branch_set_plan.branch_set_sha256: parent.branch_set_plan
    }
    pending = [parent.branch_set_plan]
    while pending:
        current = pending.pop()
        for branch in current.branches:
            if not isinstance(branch, CoordinationBranchPlan):
                continue
            child = _load_declared_work(store, branch.work.work_id)
            child_plan = child.branch_set_plan
            if child_plan is None or child_plan.branch_set_sha256 != branch.branch_set_sha256:
                raise RuntimeError("durable coordination child plan is unavailable")
            existing = documents.setdefault(child_plan.branch_set_sha256, child_plan)
            if existing != child_plan:
                raise RuntimeError("branch-set identity was reused")
            pending.append(child_plan)
    return documents


def _load_declared_work(store: WorkStore, work_id: str) -> WorkRecord:
    record = store.load(work_id)
    if record is None:
        raise RuntimeError(f"atomically admitted child work is unavailable: {work_id}")
    return record


def _branch_settlement(record: WorkRecord) -> OutputCollectionRef | None:
    if record.phase not in {"settled", "source_collection_retirement_pending", "complete"}:
        return None
    output = record.output
    if output is None:
        return None
    return output


def _branch_effect_settlement(
    branch: BranchPlan,
    record: WorkRecord,
) -> BranchEffectSettlement | None:
    if record.phase not in {"settled", "source_collection_retirement_pending", "complete"}:
        return None
    status = record.target_status
    if (
        status is None
        or status.state != "succeeded"
        or status.effect_receipt is None
        or record.output is not None
        or record.effect_settlement_sha256 is None
    ):
        raise RuntimeError("successful branch work has neither collection nor effect evidence")
    return BranchEffectSettlement.seal(
        branch=branch,
        effect_receipt_sha256=status.effect_receipt.receipt_sha256,
        effect_settlement_sha256=record.effect_settlement_sha256,
    )


def _branch_outcome(
    branch: BranchPlan | NoOutputBranchPlan, record: WorkRecord
) -> BranchOutcome | None:
    state: BranchOutcomeState | None = None
    if record.phase == "failed":
        state = "failed"
    elif record.phase == "inapplicable":
        state = "inapplicable"
    elif record.phase == "canceled":
        state = "canceled"
    elif record.target_status is not None and record.target_status.state == "interrupted":
        state = "interrupted"
    if state is None:
        return None
    return BranchOutcome(
        branch_id=branch.branch_id,
        work_id=branch_work(branch).work_id,
        workflow_plan_sha256=(
            branch.workflow_plan.workflow_plan_sha256 if isinstance(branch, BranchPlan) else None
        ),
        no_output_decision_sha256=(
            branch.decision_sha256 if isinstance(branch, NoOutputBranchPlan) else None
        ),
        state=state,
    )


def _coordination_outcome(
    branch: CoordinationBranchPlan,
    record: WorkRecord,
) -> BranchOutcome | None:
    state: BranchOutcomeState | None = None
    if record.phase == "failed":
        state = "failed"
    elif record.phase == "inapplicable":
        state = "inapplicable"
    elif record.phase == "canceled":
        state = "canceled"
    if state is None:
        return None
    return BranchOutcome(
        branch_id=branch.branch_id,
        work_id=branch.work.work_id,
        branch_set_sha256=branch.branch_set_sha256,
        state=state,
    )


def _join_settlement(
    plan: JoinPlan,
    record: WorkRecord,
    selections: dict[str, ArtifactSelection],
    store: WorkStore,
) -> JoinSettlement | None:
    if record.phase not in {"settled", "source_collection_retirement_pending", "complete"}:
        return None
    output = record.output
    if output is None:
        raise RuntimeError("verified join work has no exact output collection")
    if record.target_settlement is None:
        raise RuntimeError("verified join work has no post-root target settlement")
    selection = _output_selection(record, store)
    _retain_selection(selections, selection)
    return JoinSettlement.seal(
        plan=plan,
        derivation_sha256=output.derivation_sha256,
        producer_settlement_sha256=record.target_settlement.settlement_sha256,
        output_collection=_collection_root(output),
        output_selection=selection,
    )


def _join_outcome(plan: JoinPlan, record: WorkRecord) -> JoinOutcome | None:
    state: JoinOutcomeState | None = None
    if record.phase == "failed":
        state = "failed"
    elif record.phase == "inapplicable":
        state = "inapplicable"
    elif record.phase == "canceled":
        state = "canceled"
    elif record.target_status is not None and record.target_status.state == "interrupted":
        state = "interrupted"
    if state is None:
        return None
    return JoinOutcome(
        work_id=plan.work.work_id,
        workflow_plan_sha256=plan.workflow_plan.workflow_plan_sha256,
        join_plan_sha256=plan.join_plan_sha256,
        state=state,
    )


def _output_selection(record: WorkRecord, store: WorkStore) -> ArtifactSelection:
    output = record.output
    status = record.target_status
    settlement = record.target_settlement
    if (
        output is None
        or status is None
        or status.state != "succeeded"
        or status.output_collection != output
        or status.production is None
        or settlement is None
        or settlement.output_collection != output
        or settlement.production_sha256 != status.production.production_sha256
    ):
        raise RuntimeError("settled work lacks matching post-root target settlement")
    root = _collection_root(output)
    return ArtifactSelection.seal(
        tuple(
            WorkArtifactSubject.model_validate(
                dict(
                    id=item.id,
                    role=item.role,
                    collection=root,
                    artifact_id=item.artifact_id,
                    bytes=str(item.bytes),
                    sha256=item.sha256,
                )
            )
            for item in store.iter_target_outputs(record.work_id, status.production.job_id)
        )
    )


def _collection_root(output: OutputCollectionRef) -> CollectionRootIdentityRef:
    return CollectionRootIdentityRef.model_validate(
        {
            "collection_id": str(output.collection_id),
            "archive_root_sha256": output.archive_root_sha256,
            "artifact_set_identity": output.artifact_set_identity,
        }
    )


def _retain_selection(
    documents: dict[str, ArtifactSelection],
    selection: ArtifactSelection,
) -> None:
    existing = documents.setdefault(selection.selection_sha256, selection)
    if existing != selection:
        raise RuntimeError("artifact selection identity was reused")


__all__ = ["CoordinationProjection", "project_coordination"]

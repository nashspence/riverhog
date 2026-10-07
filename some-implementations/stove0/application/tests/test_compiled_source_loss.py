"""Named loss obligations and exact no-output settlement survive persistence."""

import pytest
from riverhog_protocol.collection_workflows import CollectionArtifactIdentity
from stove0_core.observation_questions import physical_question
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.preview_state import PreviewRecord
from stove0_core.recipes import RecipePlanner
from stove0_core.source_loss import SourceLossEvaluation
from stove0_core.work_state import ClaimBinding, WorkRecord
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationResult,
    ContentObservationResultPayload,
    ObserverContract,
    ObserverContractPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    ObserverImplementation,
    SemanticFactsConformanceVectors,
)
from stove0_observer_protocol.interfaces import subject_interface
from stove0_protocol import (
    JsonSchemaValidationProfile,
    PlanningJobRequest,
    PreviewOutcome,
    WorkflowPreview,
    WorkflowPreviewPayload,
    WorkflowPreviewRequest,
    WorkflowPreviewRequestPayload,
    canonical_json_bytes,
    canonical_json_sha256,
)
from stove0_protocol.models import JSON_SCHEMA_ONLY_SEMANTIC_PROFILE
from stove0_protocol.planning_jobs import PlanningJobPayload
from stove0_recipe_config.catalog import RecipeSourceCatalog
from stove0_recipe_config.dependencies import ObserverResource
from stove0_recipe_config.source import RecipeSource
from test_accepted_observations import _subject
from test_compiled_recipe_runtime import Inventory


def _planned(tmp_path, answers, required, *, loss_enabled=True):
    contract = ObserverContract.seal(
        ObserverContractPayload(
            id="example.loss-facts/v1",
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            options_schema=JsonSchemaValidationProfile.from_schema(
                "example.options/v1", {"type": "object"}
            ),
            facts_schema=JsonSchemaValidationProfile.from_schema(
                "example.loss-schema/v1",
                {
                    "type": "object",
                    "required": ["artifacts"],
                    "additionalProperties": False,
                    "properties": {
                        "artifacts": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["subject_id", "verdict"],
                                "additionalProperties": False,
                                "properties": {"subject_id": {"type": "string"}, "verdict": {}},
                            },
                        }
                    },
                },
            ),
        )
    )
    vectors = SemanticFactsConformanceVectors(
        profile_id="example.loss-vectors/v1",
        vectors=[
            {
                "id": "complete",
                "accepted": True,
                "subjects": [_subject(0)],
                "facts": {"artifacts": [{"subject_id": _subject(0).id, "verdict": True}]},
            },
            {
                "id": "missing",
                "accepted": False,
                "subjects": [_subject(0)],
                "facts": {"artifacts": []},
            },
        ],
    )
    interface, interface_vectors = subject_interface(
        contract=contract,
        facts_vectors=vectors,
        subject_at="/subject_id",
        id="example.loss-interface/v1",
    )
    resource = ObserverResource(
        contract=contract, interface=interface, interface_vectors=interface_vectors
    )
    catalog = RecipeSourceCatalog(
        resources={"facts": resource},
        recipes={
            "loss": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "example.loss/v1",
                    "revision": 1,
                    "observe": {task: {"use": "facts"} for task in answers},
                    "decisions": [
                        {
                            "when": True,
                            "no_output": {
                                "code": "example.discard/v1",
                                "message": "Affirmative member verdicts.",
                                "source_loss": {
                                    "id": "example.loss-rule/v1",
                                    "evidence": [
                                        {
                                            "view": task + ".artifacts",
                                            "verdict": {"path": "/verdict", "equals": required},
                                        }
                                        for task in answers
                                    ],
                                }
                                if loss_enabled
                                else None,
                            },
                        }
                    ],
                }
            )
        },
    ).compile()
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    planner = RecipePlanner(
        catalog=catalog, state=state, riverhog=Inventory(), observers=object(), targets=object()
    )
    work = planner.create_work("example.loss/v1", (Inventory.root,))
    planner = planner.for_invocation("work", work.work_id)
    state = planner.state
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="example.loss-observer/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "1" * 64,
            contracts=(
                ObserverContractSupport.from_contract(contract, interfaces=(interface.ref,)),
            ),
        )
    )
    member = None
    for _ in range(200):
        progress = planner.step(work)
        if progress.state == "question":
            question = progress.question
            subjects = state.load_selection(question.scope.selection_sha256).artifacts
            member = subjects[0]
            request = physical_question(
                question=question,
                interface=interface,
                registration_id="loss",
                descriptor=descriptor,
                subjects=subjects,
                subject_ports={"subjects": tuple(s.id for s in subjects)},
                evidence_ports={},
            )
            facts = {
                "artifacts": [
                    {"subject_id": subject.id, "verdict": answers[question.task_id]}
                    for subject in subjects
                ]
            }
            result = ContentObservationResult.seal(
                ContentObservationResultPayload(
                    request_id=request.request_id,
                    state="observed",
                    observer=ObserverImplementation(
                        id=descriptor.implementation_id,
                        version="1",
                        source_revision="fixture",
                        descriptor_sha256=descriptor.descriptor_sha256,
                    ),
                    observer_contract_id=contract.id,
                    observer_contract_sha256=contract.contract_sha256,
                    subjects=request.subjects,
                    facts_schema=contract.facts_schema,
                    facts=facts,
                    facts_sha256=canonical_json_sha256(facts),
                )
            )
            store = state.accepted_observations
            store.register_request(question, request, descriptor, interface)
            store.accept(
                ContentObservationEvidence(request=request, result=result),
                contract=contract,
                interface=interface,
                subject_ports={"subjects": tuple(s.id for s in subjects)},
            )
        elif progress.state == "no-output":
            return state, work, progress.outcome.decision, member
        else:
            assert progress.state == "pending"
    pytest.fail("compiled loss decision did not complete")


@pytest.mark.parametrize(
    ("actual", "required", "approved"),
    [
        (True, True, True),
        (1, True, False),
        (0, False, False),
        (True, 1, False),
        (None, None, True),
        ({"safe": [True, 2]}, {"safe": [True, 2]}, True),
        ({"safe": True}, {"safe": 1}, False),
        ([{"safe": False}], [{"safe": 0}], False),
    ],
)
def test_source_loss_verdicts_are_typed_and_bound_to_original_member(
    tmp_path, actual, required, approved
):
    state, work, decision, member = _planned(tmp_path, {"first": actual}, required)
    loss = SourceLossEvaluation(state, work, decision)
    identity = CollectionArtifactIdentity.from_mapping(
        member.model_dump(mode="json", exclude={"id", "role"})
    )
    approval, documents = loss.approval(
        identity, controller_id="stove0", reason="Exact affirmative verdict."
    )
    assert (approval is not None) is approved
    if approved:
        assert len(documents) == 1
        assert approval.rule_sha256 == decision.definition.source_loss.sha256
        changed = CollectionArtifactIdentity(
            identity.collection, "f" * 64, identity.bytes, identity.sha256
        )
        assert loss.approval(
            changed, controller_id="stove0", reason="Exact affirmative verdict."
        ) == (None, ())
        assert loss.declaration["accepted_views"][0]["name"] == "first.artifacts"


@pytest.mark.parametrize("second", [True, False, None])
def test_same_contract_different_tasks_must_each_affirm_source_loss(tmp_path, second):
    state, work, decision, member = _planned(tmp_path, {"first": True, "second": second}, True)
    loss = SourceLossEvaluation(state, work, decision)
    identity = CollectionArtifactIdentity.from_mapping(
        member.model_dump(mode="json", exclude={"id", "role"})
    )
    approval, documents = loss.approval(
        identity, controller_id="stove0", reason="All named tasks affirm."
    )
    assert (approval is not None) is (second is True)
    assert len(loss.declaration["evidence_slots"]) == 1
    assert [view["name"] for view in loss.declaration["accepted_views"]] == [
        "first.artifacts",
        "second.artifacts",
    ]
    if approval is not None:
        assert len(documents) == 2


def test_required_null_loss_verdict_survives_work_and_preview_storage(tmp_path):
    state, work, decision, _member = _planned(tmp_path, {"first": None}, None)
    preview_request = WorkflowPreviewRequest.seal(WorkflowPreviewRequestPayload(work=work))
    preview = WorkflowPreview.seal(
        WorkflowPreviewPayload(
            preview_id=preview_request.preview_id,
            work=work,
            state="no_action",
            no_output_decision=decision,
            outcome=PreviewOutcome(
                code=decision.definition.code, message=decision.definition.message
            ),
        )
    )
    work_record = WorkRecord(
        work=work,
        phase="no_output_pending",
        claim=ClaimBinding(claim_id="c" * 64, fence=1),
        no_action_preview=preview,
        no_output_retirement_policy="retain",
    )
    state.create(work_record)
    job = PlanningJobRequest.seal(PlanningJobPayload(work=work, invocation_id="d" * 64))
    state.create_preview(PreviewRecord(job=job, phase="completed", result=preview))
    original = canonical_json_bytes(decision.model_dump(mode="json"))
    assert (
        canonical_json_bytes(
            state.load(work.work_id).no_action_preview.no_output_decision.model_dump(mode="json")
        )
        == original
    )
    assert (
        canonical_json_bytes(
            state.load_preview(job.job_id).result.no_output_decision.model_dump(mode="json")
        )
        == original
    )

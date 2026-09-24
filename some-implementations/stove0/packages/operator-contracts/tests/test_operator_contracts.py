from __future__ import annotations

import subprocess
import sys

import pytest
from lifecycle_events import lifecycle_event
from pydantic import ValidationError
from stove0_operator_contracts import (
    BRANCH_SET_ADMITTED,
    EVALUATION_CREATED,
    EVALUATION_UPDATED,
    JOIN_ADMITTED,
    STOVE0_EVENT_TYPES,
    WORK_CREATED,
    WORK_UPDATED,
    EvaluationReviewRequest,
    EvaluationView,
    OperatorWorkflowPreviewRequest,
    Stove0EventPage,
    WorkCreateRequest,
    WorkView,
    parse_stove0_event,
    stove0_event,
)
from stove0_protocol import (
    CollectionRootIdentityRef,
    EvaluationDefinition,
    EvaluationDefinitionPayload,
    EvaluationMatrix,
    EvaluationMatrixPayload,
    EvaluationVariant,
    RecipeIdentityRef,
    WorkIdentity,
    WorkPayload,
)


def _work() -> WorkIdentity:
    return WorkIdentity.seal(
        WorkPayload(
            recipe=RecipeIdentityRef(id="fixture.recipe/v1", revision="1", sha256="1" * 64),
            inputs=(
                CollectionRootIdentityRef(
                    collection_id=str(1),
                    archive_root_sha256="2" * 64,
                    content_identity="3" * 64,
                ),
            ),
        )
    )


def _evaluation() -> EvaluationDefinition:
    matrix = EvaluationMatrix.seal(
        EvaluationMatrixPayload(variants=(EvaluationVariant(id="variant-a"),))
    )
    return EvaluationDefinition.seal(
        EvaluationDefinitionPayload(
            purpose="trial",
            recipe=RecipeIdentityRef(id="fixture.recipe/v1", revision="1", sha256="1" * 64),
            inputs=_work().inputs,
            matrix=matrix,
        )
    )


def test_work_projection_adds_and_verifies_the_public_identity() -> None:
    work = _work()
    view = WorkView.from_record(
        {
            "format": "stove0-work-record/v1",
            "work": work.model_dump(mode="json"),
            "phase": "eligible",
            "revision": 1,
        }
    )

    assert view.format == "stove0-work-view/v1"
    assert view.work_id == work.work_id
    with pytest.raises(ValidationError, match="work view ID differs"):
        WorkView.model_validate({**view.model_dump(mode="json"), "work_id": "f" * 64})
    with pytest.raises(ValidationError, match="eligible work cannot already hold a claim"):
        WorkView.model_validate(
            {
                **view.model_dump(mode="json"),
                "claim": {"claim_id": "claim", "fence": 1},
            }
        )


def test_evaluation_projection_adds_and_verifies_the_public_identity() -> None:
    definition = _evaluation()
    child = definition.child_work("variant-a")
    view = EvaluationView.from_record(
        {
            "format": "stove0-evaluation-record/v1",
            "definition": definition.model_dump(mode="json"),
            "phase": "running",
            "revision": 1,
            "children": [
                {
                    "variant_id": "variant-a",
                    "work_id": child.work_id,
                    "state": "pending",
                }
            ],
        }
    )

    assert view.format == "stove0-evaluation-view/v1"
    assert view.evaluation_id == definition.evaluation_id
    with pytest.raises(ValidationError, match="only completed evaluation children"):
        EvaluationView.model_validate(
            {
                **view.model_dump(mode="json"),
                "children": [
                    {
                        "variant_id": "variant-a",
                        "work_id": child.work_id,
                        "state": "complete",
                    }
                ],
            }
        )


def test_evaluation_review_request_is_meaningful_and_canonical() -> None:
    assert EvaluationReviewRequest(rating=5).rating == 5
    assert EvaluationReviewRequest(note="use variant a").note == "use variant a"

    with pytest.raises(ValidationError, match="requires a rating or note"):
        EvaluationReviewRequest()
    with pytest.raises(ValidationError, match="pattern"):
        EvaluationReviewRequest(note=" padded ")

    schema = EvaluationReviewRequest.model_json_schema()
    assert schema["properties"]["note"]["anyOf"][0] == {
        "maxLength": 4000,
        "minLength": 1,
        "pattern": r"^\S(?:[\s\S]*\S)?$",
        "type": "string",
    }
    assert schema["anyOf"] == [
        {"properties": {"rating": {"type": "integer"}}, "required": ["rating"]},
        {
            "properties": {"note": {"type": "string"}},
            "required": ["note"],
        },
    ]


def test_operator_requests_share_one_exact_canonical_collection_contract() -> None:
    roots = (
        CollectionRootIdentityRef(
            collection_id=str(1),
            archive_root_sha256="1" * 64,
            content_identity="2" * 64,
        ),
        CollectionRootIdentityRef(
            collection_id=str(2),
            archive_root_sha256="3" * 64,
            content_identity="4" * 64,
        ),
    )
    preview = OperatorWorkflowPreviewRequest(
        recipe_id="fixture.recipe/v1",
        inputs=roots,
    )
    created = WorkCreateRequest(
        **preview.model_dump(mode="python"),
        preview_sha256="4" * 64,
    )

    assert created.inputs == roots
    with pytest.raises(ValidationError, match="unique and canonically ordered"):
        OperatorWorkflowPreviewRequest(recipe_id="fixture.recipe/v1", inputs=tuple(reversed(roots)))


def test_stove0_events_use_one_closed_typed_operator_vocabulary() -> None:
    work_id = "1" * 64
    evaluation_id = "2" * 64
    events = [
        stove0_event(
            type=WORK_CREATED,
            subject=work_id,
            payload={"work_id": work_id, "phase": "eligible"},
        ),
        stove0_event(
            type=WORK_UPDATED,
            subject=work_id,
            payload={"work_id": work_id, "phase": "claimed", "revision": 2},
        ),
        stove0_event(
            type=BRANCH_SET_ADMITTED,
            subject=work_id,
            payload={
                "work_id": work_id,
                "phase": "coordinating",
                "revision": 3,
                "branch_set_sha256": "3" * 64,
                "branch_count": 2,
                "admitted_work_count": 2,
            },
        ),
        stove0_event(
            type=JOIN_ADMITTED,
            subject=work_id,
            payload={
                "work_id": work_id,
                "phase": "coordinating",
                "revision": 4,
                "branch_set_sha256": "3" * 64,
                "join_plan_sha256": "4" * 64,
                "join_work_id": "5" * 64,
            },
        ),
        stove0_event(
            type=EVALUATION_CREATED,
            subject=evaluation_id,
            payload={"evaluation_id": evaluation_id, "phase": "planning"},
        ),
        stove0_event(
            type=EVALUATION_UPDATED,
            subject=evaluation_id,
            payload={"evaluation_id": evaluation_id, "phase": "running", "revision": 2},
        ),
    ]
    page = Stove0EventPage(events=events, next_cursor="6", has_more=False)

    assert {event.type for event in page.events} == STOVE0_EVENT_TYPES
    assert page.events[2].payload["branch_count"] == 2


def test_stove0_events_reject_unknown_types_and_mismatched_subjects() -> None:
    unknown = lifecycle_event(
        type="io.riverhog.stove0.work.mystery",
        subject="1" * 64,
        payload={"work_id": "1" * 64, "phase": "eligible"},
    )
    with pytest.raises(ValidationError):
        parse_stove0_event(unknown.model_dump(mode="python"))
    with pytest.raises(ValidationError, match="subject differs"):
        stove0_event(
            type=WORK_CREATED,
            subject="2" * 64,
            payload={"work_id": "1" * 64, "phase": "eligible"},
        )


def test_operator_contracts_do_not_load_server_or_transport_implementations() -> None:
    forbidden = {
        "httpx",
        "stove0_api_client",
        "stove0_core",
        "stove0_observer_support",
        "stove0_target_support",
    }
    code = (
        "import sys\n"
        "import stove0_operator_contracts\n"
        f"forbidden = {forbidden!r}\n"
        "loaded = forbidden & set(sys.modules)\n"
        "assert not loaded, sorted(loaded)\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)

from __future__ import annotations

import pytest
from pydantic import TypeAdapter
from stove0_core.planning_program import (
    ProgramEvidenceIndeterminate,
    classify_members,
    condition_truth,
)
from stove0_observer_protocol import SemanticFactsConformanceVectors
from stove0_observer_protocol.interfaces import subject_interface
from stove0_protocol import CollectionRootIdentityRef, WorkArtifactSubject
from stove0_protocol.models import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
)
from stove0_protocol.observation_views import SubjectView
from stove0_protocol.predicates import Predicate, Truth
from stove0_recipe_config.compiler import compile_recipe
from stove0_recipe_config.dependencies import (
    ObserverResource,
    OperationResource,
    RecipeDependencyCatalog,
)
from stove0_recipe_config.source import RecipeSource
from stove0_target_protocol import (
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
)


def _member(id, *, root="1"):
    return WorkArtifactSubject(
        id=id,
        role="stove0.source/v1",
        artifact_id=("c" if id in {"one", "a"} else "e") * 64,
        collection=CollectionRootIdentityRef(
            collection_id=root, archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
        ),
        bytes="1",
        sha256="d" * 64,
    )


def _catalog():
    owner = ObserverContract.seal(
        ObserverContractPayload(
            id="example.facts/v1",
            options_schema=JsonSchemaValidationProfile.from_schema(
                "example.options/v1", {"type": "object"}
            ),
            facts_schema=JsonSchemaValidationProfile.from_schema(
                "example.facts-schema/v1",
                {
                    "type": "object",
                    "properties": {
                        "artifacts": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "subject_id": {"type": "string"},
                                    "kind": {"type": "string"},
                                    "discard": {"type": "boolean"},
                                },
                                "required": ["subject_id", "kind", "discard"],
                            },
                        }
                    },
                    "required": ["artifacts"],
                },
            ),
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        )
    )
    vectors = SemanticFactsConformanceVectors(
        profile_id="example.facts-conformance/v1",
        vectors=[
            {
                "id": "accepted",
                "accepted": True,
                "subjects": [_member("one")],
                "facts": {"artifacts": [{"subject_id": "one", "kind": "media", "discard": False}]},
            },
            {
                "id": "rejected",
                "accepted": False,
                "subjects": [_member("one")],
                "facts": {"artifacts": []},
            },
        ],
    )
    interface, interface_vectors = subject_interface(
        contract=owner,
        facts_vectors=vectors,
        subject_at="/subject_id",
        id="example.facts-interface/v1",
    )
    operation = OperationContract.seal(
        OperationContractPayload(
            id="example.effect/v1",
            result_kind="external-effect",
            intent_schema=JsonSchemaValidationProfile.from_schema(
                "example.intent/v1", {"type": "object"}
            ),
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            inputs=[InputArtifactContract(role="*")],
            effect_receipt_schema=JsonSchemaValidationProfile.from_schema(
                "example.receipt/v1", {"type": "object"}
            ),
        )
    )
    return RecipeDependencyCatalog(
        resources={
            "facts": ObserverResource(
                contract=owner, interface=interface, interface_vectors=interface_vectors
            ),
            "effect": OperationResource(contract=operation),
        }
    )


def _test(view, path, value, *, scope="input", quantifier="any", roles=None):
    facts = {
        "view": view,
        "scope": scope,
        "quantifier": quantifier,
        "where": {"test": {"path": path, "op": "eq", "value": value}},
    }
    if roles is not None:
        facts["roles"] = roles
    return {"facts": facts}


def _source(**changes):
    return RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.recipe/v1",
            "revision": 1,
            "roles": {"media": "example.media/v1", "sidecar": "example.sidecar/v1"},
            "observe": {"probe": {"use": "facts"}},
            "classify": {
                "cases": [
                    {
                        "role": "media",
                        "when": _test("probe.artifacts", "/kind", "media", scope="self"),
                    },
                    {
                        "role": "sidecar",
                        "when": _test("probe.artifacts", "/kind", "sidecar", scope="self"),
                    },
                ],
                "otherwise": None,
            },
            "fork": {"deliver": {"call": {"operation": "effect"}}},
            **changes,
        }
    )


def _views(**rows):
    return {
        ("probe", "artifacts"): SubjectView(
            rows={id: ({"subject_id": id, **value},) for id, value in rows.items()},
            statuses={id: "complete" for id in rows},
        )
    }


def _lookup(views):
    return lambda task, view: views.get((task, view))


def test_first_true_classification_preserves_unclassified_original_inventory(tmp_path):
    from compiled_program_fixture import run_program

    planner, progress, work = run_program(
        tmp_path,
        _source(),
        _catalog(),
        (_member("one"), _member("two")),
        facts_by_task={
            "probe": {
                "one": {"kind": "media", "discard": False},
                "two": {"kind": "unknown", "discard": False},
            }
        },
    )
    assert progress.state == "ready"
    assert planner.state.compiled_planning.inventory_ref(work.work_id).artifact_count == 2
    branch = progress.decision.plan.branches[0]
    selection = planner.state.load_selection(branch.artifact_selection.selection_sha256)
    assert [(subject.id, subject.role) for subject in selection.artifacts] == [
        ("one", "example.media/v1")
    ]
    planner.state.engine.dispose()


def test_missing_earlier_classification_evidence_cannot_fall_through():
    recipe, _ = compile_recipe(
        _source(
            classify={
                "cases": _source().classify.model_dump(mode="json")["cases"],
                "otherwise": "media",
            }
        ),
        _catalog(),
    )
    with pytest.raises(ProgramEvidenceIndeterminate):
        classify_members(recipe, (_member("one"),), _lookup({}))


def test_same_contract_tasks_do_not_merge_their_distinct_accepted_questions(tmp_path):
    from compiled_program_fixture import run_program

    source = _source(
        observe={"probe": {"use": "facts"}, "second": {"use": "facts", "options": {"question": 2}}},
        fork={
            "deliver": {
                "when": _test("second.artifacts", "/discard", True),
                "call": {"operation": "effect"},
            }
        },
    )
    planner, progress, work = run_program(
        tmp_path,
        source,
        _catalog(),
        (_member("one"),),
        facts_by_task={
            "probe": {"one": {"kind": "media", "discard": True}},
            "second": {"one": {"kind": "media", "discard": False}},
        },
    )
    assert progress.state == "inapplicable"
    store = planner.state.accepted_observations
    first, second = (store.accepted(work.work_id, task) for task in ("probe", "second"))
    assert first.question.observer_contract == second.question.observer_contract
    assert first.question.question_sha256 != second.question.question_sha256
    assert {
        item.request.task_id: item.result.facts["artifacts"][0]["discard"]
        for item in planner.accepted_evidence(work)
    } == {"probe": True, "second": False}
    planner.state.engine.dispose()


def test_global_input_decision_does_not_shrink_to_only_classified_members(tmp_path):
    from compiled_program_fixture import run_program

    source = _source(
        decisions=[
            {
                "when": _test("probe.artifacts", "/discard", True, quantifier="every"),
                "no_output": {
                    "code": "example.discard/v1",
                    "message": "Discard is explicitly approved.",
                },
            }
        ]
    )
    planner, progress, work = run_program(
        tmp_path,
        source,
        _catalog(),
        (_member("one"), _member("two")),
        facts_by_task={
            "probe": {
                "one": {"kind": "media", "discard": True},
                "two": {"kind": "unknown", "discard": False},
            }
        },
    )
    assert progress.state == "ready"
    assert planner.state.compiled_planning.inventory_ref(work.work_id).artifact_count == 2
    assert progress.decision.plan.branches[0].artifact_selection.artifact_count == 1
    planner.state.engine.dispose()


def test_scope_outside_a_valid_task_domain_is_indeterminate_not_a_negative():
    condition = TypeAdapter(Predicate).validate_python(
        {"not": _test("probe.artifacts", "/discard", True)}
    )
    assert (
        condition_truth(
            condition,
            views=_lookup(_views(one={"kind": "media", "discard": False})),
            inputs=(_member("two"),),
        )
        == Truth.INDETERMINATE
    )


def test_inclusive_fork_keeps_overlap_and_aggregate_selection(tmp_path):
    from compiled_program_fixture import run_program

    planner, progress, work = run_program(
        tmp_path,
        _source(fork={name: {"call": {"operation": "effect"}} for name in ("one", "two")}),
        _catalog(),
        (_member("a"), _member("b", root="2")),
        facts_by_task={
            "probe": {
                "a": {"kind": "media", "discard": False},
                "b": {"kind": "media", "discard": False},
            }
        },
    )
    assert progress.state == "ready" and len(work.inputs) == 2
    first, second = progress.decision.plan.branches
    assert first.artifact_selection == second.artifact_selection
    assert first.artifact_selection.artifact_count == 2
    planner.state.engine.dispose()


def test_pure_decision_recipe_has_an_explicit_successful_no_output_outcome(tmp_path):
    from compiled_program_fixture import run_program

    planner, progress, work = run_program(
        tmp_path,
        _source(
            fork={},
            decisions=[
                {
                    "when": True,
                    "no_output": {
                        "code": "example.no-output/v1",
                        "message": "No target is needed.",
                    },
                }
            ],
        ),
        _catalog(),
        (_member("one"),),
        facts_by_task={"probe": {"one": {"kind": "media", "discard": False}}},
    )
    assert progress.state == "no-output" and progress.outcome.decision.decision_index == 0
    assert planner.state.recipe_definitions.load(work.recipe)[0].contract.outcomes.normal is None
    planner.state.engine.dispose()


def test_many_per_subject_every_is_nonvacuous_for_each_exact_member():
    condition = TypeAdapter(Predicate).validate_python(
        _test("probe.artifacts", "/discard", True, quantifier="every")
    )
    view = SubjectView(
        rows={"one": ({"discard": True},), "two": ()},
        statuses={"one": "complete", "two": "complete"},
    )
    assert (
        condition_truth(condition, views=lambda *_: view, inputs=(_member("one"), _member("two")))
        == Truth.FALSE
    )


def test_unmatched_role_filters_cannot_reuse_a_preclassification_role(tmp_path):
    from compiled_program_fixture import run_program

    source = _source(
        roles={"media": "stove0.source/v1", "sidecar": "example.sidecar/v1"},
        decisions=[
            {
                "when": _test(
                    "probe.artifacts", "/discard", True, roles=["media"], quantifier="every"
                ),
                "no_output": {
                    "code": "example.discard/v1",
                    "message": "Only classified media is selected.",
                },
            }
        ],
    )
    planner, progress, work = run_program(
        tmp_path,
        source,
        _catalog(),
        (_member("one"), _member("two")),
        facts_by_task={
            "probe": {
                "one": {"kind": "media", "discard": True},
                "two": {"kind": "unknown", "discard": False},
            }
        },
    )
    assert progress.state == "no-output"
    assert progress.outcome.decision.inventory.artifact_count == 2
    planner.state.engine.dispose()

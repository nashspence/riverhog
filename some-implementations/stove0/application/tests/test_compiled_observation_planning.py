"""The retained compiled graph survives partial evidence and controller restarts."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from a_stove0_magic_facts_contract_lib import (
    MAGIC_INTERFACE,
    MAGIC_INTERFACE_VECTORS,
    MAGIC_OBSERVER_CONTRACT,
)
from a_stove0_magic_facts_contract_lib.contracts import (
    MAGIC_CONFORMANCE_VECTORS,
    MAGIC_SEMANTIC_VALIDATOR,
)
from stove0_core.compiled_observations import CompiledObservationPlanning
from stove0_core.observation_questions import physical_question
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_observer_protocol import (
    ContentObservationEvidence,
    ContentObservationResult,
    ContentObservationResultPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    SemanticValidatorRegistry,
)
from stove0_protocol import (
    CollectionRootIdentityRef,
    WorkIdentity,
    WorkPayload,
    canonical_json_sha256,
)
from stove0_protocol.models import ObserverImplementation
from stove0_recipe_config.compiler import compile_recipe
from stove0_recipe_config.dependencies import ObserverResource, RecipeDependencyCatalog
from stove0_recipe_config.source import RecipeSource


def _program(*, input_classification=False, decisions=None):
    resource = ObserverResource(
        contract=MAGIC_OBSERVER_CONTRACT,
        interface=MAGIC_INTERFACE,
        interface_vectors=MAGIC_INTERFACE_VECTORS,
        facts_vectors=MAGIC_CONFORMANCE_VECTORS,
    )
    source = RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.classified/v1",
            "revision": 1,
            "roles": {"media": "example.media/v1", "unused": "example.unused/v1"},
            "observe": {
                "first": {"use": "magic"},
                "second": {"use": "magic", "inputs": {"subjects": {"roles": ["media"]}}},
                "empty": {"use": "magic", "inputs": {"subjects": {"roles": ["unused"]}}},
            },
            "classify": {
                "cases": [
                    {
                        "role": "media",
                        "when": {
                            "facts": {
                                "view": "first.artifacts",
                                "scope": "input" if input_classification else "self",
                                "quantifier": "every" if input_classification else "any",
                                "where": {
                                    "test": {
                                        "path": "/mime_type",
                                        "op": "eq",
                                        "value": "application/octet-stream",
                                    }
                                },
                            }
                        },
                    }
                ],
                "otherwise": None,
            },
            "decisions": decisions
            or [
                {
                    "when": True,
                    "no_output": {
                        "code": "example.checked/v1",
                        "message": "The exact observations were evaluated.",
                    },
                }
            ],
        }
    )
    return compile_recipe(source, RecipeDependencyCatalog(resources={"magic": resource}))


def _descriptor():
    return ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="example.magic/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "d" * 64,
            contracts=(
                ObserverContractSupport.from_contract(
                    MAGIC_OBSERVER_CONTRACT,
                    interfaces=(MAGIC_INTERFACE.ref,),
                ),
            ),
        )
    )


def _answer(state, question, descriptor, subjects, *, mime_types=None):
    request = physical_question(
        question=question,
        interface=MAGIC_INTERFACE,
        registration_id="magic",
        descriptor=descriptor,
        subjects=subjects,
        subject_ports={"subjects": tuple(sorted(subject.id for subject in subjects))},
        evidence_ports={},
    )
    sample = next(
        vector.facts["artifacts"][0]
        for vector in MAGIC_CONFORMANCE_VECTORS.vectors
        if vector.accepted
    )
    facts = {
        "artifacts": [
            {
                **sample,
                "subject_id": subject.id,
                "mime_type": (mime_types or {}).get(subject.id, sample["mime_type"]),
            }
            for subject in request.subjects
        ]
    }
    result = ContentObservationResult.seal(
        ContentObservationResultPayload(
            request_id=request.request_id,
            state="observed",
            observer=ObserverImplementation(
                id=descriptor.implementation_id,
                version=descriptor.implementation_version,
                source_revision=descriptor.source_revision,
                descriptor_sha256=descriptor.descriptor_sha256,
            ),
            observer_contract_id=MAGIC_OBSERVER_CONTRACT.id,
            observer_contract_sha256=MAGIC_OBSERVER_CONTRACT.contract_sha256,
            subjects=request.subjects,
            facts_schema=MAGIC_OBSERVER_CONTRACT.facts_schema,
            facts=facts,
            facts_sha256=canonical_json_sha256(facts),
        )
    )
    state.accepted_observations.register_request(question, request, descriptor, MAGIC_INTERFACE)
    state.accepted_observations.accept(
        ContentObservationEvidence(request=request, result=result),
        contract=MAGIC_OBSERVER_CONTRACT,
        interface=MAGIC_INTERFACE,
        subject_ports={"subjects": tuple(subject.id for subject in request.subjects)},
        semantic_validators=SemanticValidatorRegistry((MAGIC_SEMANTIC_VALIDATOR,)),
    )


def test_graph_resumes_retained_meaning_and_never_classifies_a_partial_task(tmp_path):
    recipe, closure = _program()
    root = CollectionRootIdentityRef(
        collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
    )
    work = WorkIdentity.seal(WorkPayload(recipe=recipe.ref, inputs=(root,), effective_intent={}))
    url = f"sqlite:///{tmp_path / 'state.db'}"
    state = SqlAlchemyStateStore(url)
    state.recipe_definitions.retain(recipe, closure)

    class Inventory:
        calls = 0

        def get_collection(self, collection_id):
            assert collection_id == 1
            return root.model_dump(mode="json")

        def get_portable_collection_inventory(self, collection_id, **kwargs):
            self.calls += 1
            assert kwargs == {"cursor": None, "limit": 1000, "inventory_identity": None}
            return SimpleNamespace(
                authority=SimpleNamespace(inventory_identity="c" * 64),
                artifacts=tuple(
                    SimpleNamespace(artifact_id=f"{index + 1:064x}", bytes=1, sha256="e" * 64)
                    for index in range(205)
                ),
                complete=True,
                next_cursor=None,
            )

    riverhog, descriptor = Inventory(), _descriptor()
    delivered, questions, partial = set(), {}, False
    for _step in range(100):
        driver = CompiledObservationPlanning(state, riverhog)
        progress = driver.step(work)
        if progress.state == "question":
            question = progress.question
            assert question is not None and question.scope.artifact_count == 205
            questions[question.task_id] = question
            if question.task_id not in delivered:
                subjects = tuple(state.iter_selection_artifacts(question.scope.selection_sha256))
                if question.task_id == "first" and not partial:
                    _answer(state, question, descriptor, subjects[:100])
                    partial = True
                    assert state.accepted_observations.accepted(work.work_id, "first") is None
                    assert (
                        state.compiled_planning.ensure(work.work_id, recipe)["classified_count"]
                        == 0
                    )
                else:
                    offset = 100 if question.task_id == "first" else 0
                    for start in range(offset, len(subjects), 100):
                        _answer(state, question, descriptor, subjects[start : start + 100])
                    delivered.add(question.task_id)
        if progress.state == "complete":
            break
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    else:
        pytest.fail("compiled graph stopped making bounded progress")
    assert riverhog.calls == 1
    assert delivered == {"first", "second"}
    assert questions["first"].question_sha256 != questions["second"].question_sha256
    assert state.accepted_observations.accepted(work.work_id, "empty").state == "complete-empty"
    assert state.accepted_observations.accepted(work.work_id, "empty").results.result_count == 0
    assert state.accepted_observations.accepted(work.work_id, "first").results.result_count == 3
    assert state.accepted_observations.accepted(work.work_id, "second").results.result_count == 3
    assert state.compiled_planning.ensure(work.work_id, recipe)["classified_count"] == 205
    assert driver.step(work).state == "complete"
    assert not state.compiled_planning.task_ports(work.work_id, "first") == {}
    state.engine.dispose()


def test_queued_graph_requires_its_retained_definition_and_cannot_rebind_inventory(tmp_path):
    recipe, closure = _program()
    root = CollectionRootIdentityRef(
        collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
    )
    work = WorkIdentity.seal(WorkPayload(recipe=recipe.ref, inputs=(root,), effective_intent={}))
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    with pytest.raises(ValueError, match="retained exact compiled"):
        CompiledObservationPlanning(state, object()).step(work)
    from stove0_protocol import ArtifactSelection

    empty = ArtifactSelection.seal(())
    state.retain_selection(empty)
    state.recipe_definitions.retain(recipe, closure)
    row = state.compiled_planning.ensure(work.work_id, recipe)
    state.compiled_planning.bind_inventory(
        work.work_id, expected_revision=row["revision"], scope=empty.ref()
    )
    row = state.compiled_planning.ensure(work.work_id, recipe)
    with pytest.raises(ValueError, match="cannot be rebound"):
        state.compiled_planning.bind_inventory(
            work.work_id, expected_revision=row["revision"], scope=empty.ref()
        )
    state.engine.dispose()

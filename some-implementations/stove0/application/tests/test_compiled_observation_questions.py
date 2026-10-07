"""Compiled named ports build exact questions against supplied owner contracts."""

from __future__ import annotations

import pytest
from a_stove0_magic_facts_contract_lib import (
    MAGIC_INTERFACE,
    MAGIC_INTERFACE_VECTORS,
    MAGIC_OBSERVER_CONTRACT,
)
from a_stove0_magic_facts_contract_lib.contracts import MAGIC_CONFORMANCE_VECTORS
from stove0_core.observation_questions import logical_question, physical_question
from stove0_protocol import ArtifactSelection, WorkIdentity, WorkPayload
from stove0_protocol.models import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    JsonSchemaValidationProfile,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
)
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


def _fixture():
    resource = ObserverResource(
        contract=MAGIC_OBSERVER_CONTRACT,
        interface=MAGIC_INTERFACE,
        interface_vectors=MAGIC_INTERFACE_VECTORS,
        facts_vectors=MAGIC_CONFORMANCE_VECTORS,
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
    source = RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.recipe/v1",
            "revision": 1,
            "observe": {"first": {"use": "magic"}, "second": {"use": "magic"}},
            "fork": {"effect": {"call": {"operation": "effect"}}},
        }
    )
    recipe, _ = compile_recipe(
        source,
        RecipeDependencyCatalog(
            resources={"magic": resource, "effect": OperationResource(contract=operation)}
        ),
    )
    subjects = next(
        vector.subjects for vector in MAGIC_CONFORMANCE_VECTORS.vectors if vector.accepted
    )
    selection = ArtifactSelection.seal(subjects)
    work = WorkIdentity.seal(
        WorkPayload(recipe=recipe.ref, inputs=selection.roots(), effective_intent={})
    )
    descriptor = ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="example.magic-observer/v1",
            implementation_version="1",
            source_revision="fixture",
            image_id="sha256:" + "a" * 64,
            contracts=(
                ObserverContractSupport.from_contract(
                    MAGIC_OBSERVER_CONTRACT, interfaces=(MAGIC_INTERFACE.ref,)
                ),
            ),
        )
    )
    return recipe, resource, selection, work, descriptor


def test_same_contract_named_tasks_have_distinct_question_and_request_identities():
    recipe, resource, selection, work, descriptor = _fixture()
    questions = []
    requests = []
    for name in ("first", "second"):
        question = logical_question(
            work=work,
            task_id=name,
            task=recipe.observations[name],
            resource=resource,
            scope=selection.ref(),
            subject_ports={"subjects": selection.ref()},
            predecessors={},
        )
        requests.append(
            physical_question(
                question=question,
                interface=resource.interface,
                registration_id="magic",
                descriptor=descriptor,
                subjects=selection.artifacts,
                subject_ports={
                    "subjects": tuple(sorted(member.id for member in selection.artifacts))
                },
                evidence_ports={},
            )
        )
        questions.append(question)
    assert questions[0].question_sha256 != questions[1].question_sha256
    assert requests[0].request_id != requests[1].request_id
    assert requests[0].observer_contract_sha256 == requests[1].observer_contract_sha256
    assert requests[0].task_id == "first" and requests[1].task_id == "second"
    assert requests[0].question_sha256 == questions[0].question_sha256
    assert requests[0].interface == resource.interface.ref


def test_executor_without_the_exact_interface_cannot_receive_the_task():
    recipe, resource, selection, work, descriptor = _fixture()
    question = logical_question(
        work=work,
        task_id="first",
        task=recipe.observations["first"],
        resource=resource,
        scope=selection.ref(),
        subject_ports={"subjects": selection.ref()},
        predecessors={},
    )
    mismatch = ObserverDescriptor.seal(
        ObserverDescriptorPayload.model_validate(
            {
                **descriptor.model_dump(mode="json", exclude={"descriptor_sha256"}),
                "contracts": [
                    ObserverContractSupport.from_contract(MAGIC_OBSERVER_CONTRACT).model_dump(
                        mode="json"
                    )
                ],
            }
        )
    )
    with pytest.raises(ValueError, match="exact named question"):
        physical_question(
            question=question,
            interface=resource.interface,
            registration_id="magic",
            descriptor=mismatch,
            subjects=selection.artifacts,
            subject_ports={"subjects": tuple(sorted(member.id for member in selection.artifacts))},
            evidence_ports={},
        )


def test_port_map_is_part_of_the_compiled_question_authority():
    recipe, resource, selection, work, _ = _fixture()
    with pytest.raises(ValueError, match="subject ports"):
        logical_question(
            work=work,
            task_id="first",
            task=recipe.observations["first"],
            resource=resource,
            scope=selection.ref(),
            subject_ports={"invented": selection.ref()},
            predecessors={},
        )

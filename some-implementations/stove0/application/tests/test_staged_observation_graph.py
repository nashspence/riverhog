"""Recipe stages select exact accepted facts before asking dependent observers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from stove0_core.recipes import RecipePlanner
from stove0_observer_protocol import (
    ContentObservationEvidence,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
)
from stove0_observer_support import ContentObservationResultBuilder
from stove0_protocol import JSON_SCHEMA_ONLY_SEMANTIC_PROFILE, CollectionRootIdentityRef
from stove0_recipe_config import (
    ArtifactFactBinding,
    ArtifactRule,
    FactPredicate,
    ObservationPartition,
    ObserverUse,
    RecipeDefinition,
    RecipeRoute,
)


def _contract(name: str, action: str) -> ObserverContract:
    schema = JsonSchemaValidationProfile.from_schema(name + "-schema/v1", {"type": "object"})
    return ObserverContract.seal(
        ObserverContractPayload(
            id=name,
            read_actions=(action,),  # type: ignore[arg-type]
            options_schema=schema,
            facts_schema=schema,
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        )
    )


def _descriptor(contract: ObserverContract, batch_size: int) -> ObserverDescriptor:
    return ObserverDescriptor.seal(
        ObserverDescriptorPayload(
            implementation_id="fixture." + contract.id,
            implementation_version="1",
            source_revision="test",
            image_id="sha256:" + "f" * 64,
            contracts=(
                ObserverContractSupport.from_contract(
                    contract, preferred_subject_batch_size=batch_size
                ),
            ),
        )
    )


def test_dependent_stage_receives_complete_role_partitions_and_exact_predecessors() -> None:
    classify = _contract("fixture.classify/v1", "read-inputs")
    compare = _contract("fixture.compare/v1", "read-evidence")
    descriptors = {"classify": _descriptor(classify, 1), "compare": _descriptor(compare, 2)}
    primary, sidecar = "fixture.primary/v1", "fixture.sidecar/v1"
    recipe = RecipeDefinition(
        id="fixture.staged/v1",
        revision=cast(Any, "1"),
        unmatched_artifact_disposition="retain-in-source",
        artifact_rules=(
            ArtifactRule(
                role=sidecar,
                when=(
                    FactPredicate(
                        observation_contract_id=classify.id,
                        artifact_roles=(sidecar,),
                        artifact_facts=ArtifactFactBinding(
                            records_pointer="/artifacts", artifact_id_pointer="/subject_id"
                        ),
                        pointer="/kind",
                        value="sidecar",
                    ),
                ),
            ),
            ArtifactRule(role=primary),
        ),
        observers=(
            ObserverUse(
                registration_id="classify",
                contract_id=classify.id,
                contract_sha256=classify.contract_sha256,
            ),
            ObserverUse(
                registration_id="compare",
                contract_id=compare.id,
                contract_sha256=compare.contract_sha256,
                after=("classify",),
                evidence_from=("classify",),
                evidence_slots_pointer="/provenance_slots",
                subject_roles=(primary, sidecar),
                partitions=(
                    ObservationPartition(pointer="/primary_ids", roles=(primary,)),
                    ObservationPartition(pointer="/sidecar_ids", roles=(sidecar,)),
                ),
            ),
        ),
        routes=(
            RecipeRoute(
                id="deliver",
                operation_id="fixture.deliver/v1",
                target_registration_id="target",
            ),
        ),
    )
    root = CollectionRootIdentityRef(
        collection_id=cast(Any, "1"),
        archive_root_sha256="a" * 64,
        artifact_set_identity="b" * 64,
    )
    inventory = tuple(
        {"collection": root, "artifact_id": digit * 64, "bytes": 1, "sha256": "e" * 64}
        for digit in ("1", "2", "3")
    )
    planner = RecipePlanner(
        catalog=cast(Any, SimpleNamespace(recipe=lambda *_: recipe)),
        riverhog=cast(Any, object()),
        observers=cast(
            Any, SimpleNamespace(descriptor=lambda registration: descriptors[registration])
        ),
        targets=cast(Any, object()),
    )
    planner.__dict__["_inventory"] = lambda _: inventory
    work = planner.create_work(recipe.id, (root,))
    first_stage = planner.observation_requests(work)
    assert len(first_stage) == 3
    assert {item.observer_registration_id for item in first_stage} == {"classify"}
    evidence = tuple(
        ContentObservationEvidence(
            request=request,
            result=ContentObservationResultBuilder(descriptors["classify"], request).observed(
                {
                    "artifacts": [
                        {
                            "subject_id": request.subjects[0].id,
                            "kind": (
                                "sidecar"
                                if request.subjects[0].artifact_id == "3" * 64
                                else "primary"
                            ),
                        }
                    ]
                }
            ),
        )
        for request in first_stage
    )
    second_stage = planner.observation_requests(work, evidence)
    assert len(second_stage) == 1  # complete scope despite preferred batch size of two
    request = second_stage[0]
    assert request.observer_registration_id == "compare"
    assert len(request.subjects) == 3
    assert request.options["primary_ids"] == sorted(
        item.id for item in request.subjects if item.role == primary
    )
    assert request.options["sidecar_ids"] == sorted(
        item.id for item in request.subjects if item.role == sidecar
    )
    assert len(request.evidence_slots or ()) == 3
    assert request.options["provenance_slots"] == [
        item.slot for item in request.evidence_slots or ()
    ]
    assert {item.request_id for item in request.evidence_slots or ()} == {
        item.request.request_id for item in evidence
    }

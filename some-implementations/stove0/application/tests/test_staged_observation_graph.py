"""Recipe stages select exact accepted facts before asking dependent observers."""

from __future__ import annotations

from pathlib import Path
from threading import Barrier, Lock
from types import SimpleNamespace
from typing import Any, cast

import pytest
from stove0_core.preview import WorkflowPreviewService
from stove0_core.recipes import RecipePlanner
from stove0_core.work_state import ClaimBinding, WorkNoAction
from stove0_observer_protocol import (
    ContentObservationEvidence,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
    ObserverContractSupport,
    ObserverDescriptor,
    ObserverDescriptorPayload,
    ObserverRuntimeAuthority,
)
from stove0_observer_support import ContentObservationResultBuilder
from stove0_protocol import JSON_SCHEMA_ONLY_SEMANTIC_PROFILE, CollectionRootIdentityRef
from stove0_recipe_config import (
    ArtifactFactBinding,
    ArtifactRule,
    FactPredicate,
    ObservationPartition,
    ObserverUse,
    RecipeCatalog,
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


@pytest.mark.parametrize("subject_count", (3, 9))
def test_dependent_stage_receives_complete_role_partitions_and_exact_predecessors(
    subject_count: int,
) -> None:
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
        {"collection": root, "artifact_id": f"{index:064x}", "bytes": 1, "sha256": "e" * 64}
        for index in range(1, subject_count + 1)
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
    assert len(first_stage) == subject_count
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
                                if request.subjects[0].artifact_id == f"{subject_count:064x}"
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
    assert len(request.subjects) == subject_count
    assert request.options["primary_ids"] == sorted(
        item.id for item in request.subjects if item.role == primary
    )
    assert request.options["sidecar_ids"] == sorted(
        item.id for item in request.subjects if item.role == sidecar
    )
    assert len(request.evidence_slots or ()) == subject_count
    assert request.options["provenance_slots"] == [
        item.slot for item in request.evidence_slots or ()
    ]
    assert {item.request_id for item in request.evidence_slots or ()} == {
        item.request.request_id for item in evidence
    }

    # The real preview adapter must drain the same recipe graph before routing.
    # A no-action result still requires all of its declared observation stages.
    invocations = []
    abandoned = []
    expected = {item.request.request_id: item for item in evidence}
    first_wave = Barrier(min(4, subject_count))
    lock = Lock()
    started = active = maximum_active = completed = 0

    def observe(registration, invocation, *, descriptor):
        nonlocal started, active, maximum_active, completed
        invocations.append(invocation)
        if registration == "classify":
            with lock:
                started += 1
                ordinal = started
                active += 1
                maximum_active = max(maximum_active, active)
            if ordinal <= first_wave.parties:
                first_wave.wait(timeout=5)
            with lock:
                active -= 1
                completed += 1
            return expected[invocation.request.request_id].result
        assert registration == "compare"
        assert completed == subject_count and active == 0
        assert invocation.request == request
        assert invocation.evidence == tuple(
            sorted(evidence, key=lambda item: item.request.request_id)
        )
        return ContentObservationResultBuilder(descriptor, invocation.request).observed({})

    def finish(_work, accepted, **_kwargs):
        assert len(accepted) == subject_count + 1
        assert {item.request.observer_registration_id for item in accepted} == {
            "classify",
            "compare",
        }
        return WorkNoAction(code="classified-no-action", message="All observations were evaluated.")

    planner.__dict__["workflow_plan"] = finish
    service = WorkflowPreviewService(
        planning=planner,
        riverhog=cast(
            Any,
            SimpleNamespace(
                acquire_preview_claim=lambda _: ClaimBinding(claim_id="preview", fence=1),
                observation_authority=lambda *_: ObserverRuntimeAuthority(
                    riverhog_base_url="https://riverhog.invalid",
                    capability_token="fixture",
                    declared_workspace_protection="memory-backed",
                ),
                abandon_preview_claim=lambda *_: abandoned.append(True),
            ),
        ),
        observers=cast(
            Any,
            SimpleNamespace(
                descriptor=lambda registration: descriptors[registration], observe=observe
            ),
        ),
        targets=cast(Any, object()),
    )
    preview = service.preview(work)
    assert preview.state == "no_action" and len(preview.observations) == subject_count + 1
    assert len(invocations) == subject_count + 1 and abandoned == [True]
    assert maximum_active == min(4, subject_count)


def test_supplied_media_recipe_classifies_before_probe_and_exact_provenance() -> None:
    from a_stove0_exiftool_observer import ExiftoolObserver
    from a_stove0_ffprobe_observer import FfprobeObserver
    from a_stove0_filename_prefix_sidecar_observer import FilenamePrefixSidecarObserver
    from a_stove0_riverhog_provenance_observer import RiverhogProvenanceObserver
    from stove0_observer_protocol import validate_observation_request

    catalog = RecipeCatalog.load(
        Path(__file__).resolve().parents[4] / "qualification/fixtures/stove0/recipes.yaml"
    )
    image_id = "sha256:" + "f" * 64
    exiftool = ExiftoolObserver(image_id=image_id).descriptor()
    ffprobe = FfprobeObserver(image_id=image_id).descriptor()
    provenance = RiverhogProvenanceObserver(image_id=image_id).descriptor()
    filename = FilenamePrefixSidecarObserver(image_id=image_id).descriptor()
    descriptors = {
        "exiftool": exiftool,
        "ffprobe-streams": ffprobe,
        "canonical-provenance": provenance,
        "canonical-hint": provenance,
        "filename-prefix-sidecars": filename,
    }
    root = CollectionRootIdentityRef(
        collection_id=cast(Any, "1"),
        archive_root_sha256="a" * 64,
        artifact_set_identity="b" * 64,
    )
    inventory = tuple(
        {"collection": root, "artifact_id": digit * 64, "bytes": 3, "sha256": "e" * 64}
        for digit in ("1", "2")
    )
    planner = RecipePlanner(
        catalog=catalog,
        riverhog=cast(Any, object()),
        observers=cast(
            Any, SimpleNamespace(descriptor=lambda registration: descriptors[registration])
        ),
        targets=cast(Any, object()),
    )
    planner.__dict__["_inventory"] = lambda _: inventory
    work = planner.create_work("stove0.conformance-media/v1", (root,))
    first_stage = planner.observation_requests(work)
    assert len(first_stage) == 2
    assert {item.observer_registration_id for item in first_stage} == {"exiftool"}
    evidence = tuple(
        ContentObservationEvidence(
            request=request,
            result=ContentObservationResultBuilder(exiftool, request).observed(
                {
                    "artifacts": [
                        {
                            "artifact_id": request.subjects[0].id,
                            "state": "observed",
                            "facts": [
                                {
                                    "name": "container-format",
                                    "value": (
                                        "XMP"
                                        if request.subjects[0].artifact_id == "2" * 64
                                        else "video/quicktime"
                                    ),
                                    "evidence": {
                                        "artifact_id": request.subjects[0].id,
                                        "field": "File:FileType",
                                    },
                                }
                            ],
                        }
                    ]
                }
            ),
        )
        for request in first_stage
    )
    next_stage = planner.observation_requests(work, evidence)
    assert {item.observer_registration_id for item in next_stage} == {
        "ffprobe-streams",
        "canonical-provenance",
        "canonical-hint",
    }
    assert (
        len([item for item in next_stage if item.observer_registration_id == "ffprobe-streams"])
        == 1
    )
    for request in next_stage:
        validate_observation_request(request, descriptors[request.observer_registration_id])

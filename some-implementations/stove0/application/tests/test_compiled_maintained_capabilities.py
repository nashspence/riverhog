"""Current compiled witnesses for the maintained planner's public capabilities."""

from pathlib import Path

import pytest
from a_stove0_media_archive_contract_lib import AUDIO_ARCHIVE_OPERATION, AV1_OPUS_ARCHIVE_OPERATION
from a_stove0_media_metadata_contract_lib import MEDIA_METADATA_OBSERVER_CONTRACT
from media_planning_fixture import (
    CATALOG,
    ROOT,
    AssociatedMediaCatalogApi,
    BatchMediaObservers,
    CatalogApi,
    LargeCatalogApi,
    MultiArtifactCatalogApi,
    plan_until_complete,
    planner_for,
)
from review0_contracts import (
    REVIEW_MATERIALIZE_OPERATION,
    ReviewSamplePlan,
    ReviewSamplePlanPayload,
    ReviewSampleWindow,
)
from review0_planner import ReviewVariant, review_evaluation_definition
from stove0_observer_protocol import ObserverDescriptor, ObserverDescriptorPayload
from stove0_protocol import (
    ArtifactSelection,
    BranchSettlement,
    CollectionRootIdentityRef,
    JsonSchemaValidationProfile,
    WorkArtifactSubject,
    resolve_join_plan,
)
from stove0_recipe_config import RecipeSource, RecipeSourceCatalog, load_recipe_catalog
from stove0_recipe_config.dependencies import OperationResource
from stove0_target_protocol import TargetDescriptor, TargetDescriptorPayload, TargetOperationSupport
from test_compiled_recipe_runtime import Targets, _operation


def _catalog(**recipes):
    source = RecipeSourceCatalog.load(CATALOG)
    return RecipeSourceCatalog(
        resources=source.resources,
        recipes={name: RecipeSource.model_validate(value) for name, value in recipes.items()},
    ).compile()


def _conformance(**changes):
    source = (
        RecipeSourceCatalog.load(CATALOG)
        .recipes["conformance"]
        .model_dump(mode="json", by_alias=True)
    )
    return {**source, **changes}


def _simple(operation, **changes):
    return RecipeSourceCatalog(
        resources={"copy": OperationResource(contract=operation)},
        recipes={
            "simple": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "fixture.simple/v1",
                    "revision": 1,
                    "fork": {"copy": {"call": {"operation": "copy", "executor": "target"}}},
                    **changes,
                }
            )
        },
    ).compile()


def test_explicit_observation_rule_resolves_no_output_before_target_planning(tmp_path: Path):
    source = _conformance(
        observe={"metadata": {"use": "metadata", "executor": "exiftool"}},
        classify={"otherwise": None},
        groups={},
        fork={},
        decisions=[
            {
                "when": {
                    "facts": {
                        "view": "metadata.artifacts",
                        "scope": "input",
                        "quantifier": "every",
                        "where": {"test": {"path": "/state", "op": "eq", "value": "observed"}},
                    }
                },
                "no_output": {
                    "code": "fixture.checked/v1",
                    "message": "No transformation is required.",
                },
            }
        ],
    )
    planner = planner_for(tmp_path, catalog=_catalog(checked=source), targets=object())
    work = planner.create_work(source["id"], (ROOT,))
    planner, progress, evidence = plan_until_complete(planner, work, restart=True)
    assert progress.state == "no-output"
    assert progress.outcome.code == "fixture.checked/v1"
    assert progress.outcome.decision.inventory.artifact_count == 6
    assert evidence and all(item.request.task_id == "metadata" for item in evidence)
    assert planner.step(work).outcome == progress.outcome
    planner.state.engine.dispose()


def test_installed_catalog_rejects_stale_observer_contract_before_observation(tmp_path: Path):
    observers = BatchMediaObservers(4)
    descriptor = observers.descriptors["exiftool"]
    payload = descriptor.model_dump(mode="json", exclude={"descriptor_sha256"})
    payload["contracts"][0]["contract_sha256"] = "f" * 64
    observers.descriptors["exiftool"] = ObserverDescriptor.seal(
        ObserverDescriptorPayload.model_validate(payload)
    )
    planner = planner_for(tmp_path, observers=observers)
    work = planner.create_work("stove0.conformance-media/v1", (ROOT,))
    with pytest.raises(ValueError, match="exact|contract|provider"):
        plan_until_complete(planner, work)
    planner.state.engine.dispose()


def test_planning_rejects_stale_target_operation_contract_before_preflight(tmp_path: Path):
    operation = _operation()
    targets = Targets(operation)
    payload = targets.target.model_dump(mode="json", exclude={"descriptor_sha256"})
    payload["operations"][0]["operation_contract_sha256"] = "f" * 64
    targets.target = TargetDescriptor.seal(TargetDescriptorPayload.model_validate(payload))
    planner = planner_for(tmp_path, catalog=_simple(operation), api=CatalogApi(), targets=targets)
    work = planner.create_work("fixture.simple/v1", (ROOT,))
    with pytest.raises(ValueError, match="exact|contract|provider"):
        plan_until_complete(planner, work)
    planner.state.engine.dispose()


def test_planner_seals_exact_nested_subrecipe_tree_without_target_smearing(tmp_path: Path):
    operation = _operation("collection")
    catalog = RecipeSourceCatalog(
        resources={"copy": OperationResource(contract=operation)},
        recipes={
            "child": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "fixture.child/v1",
                    "revision": 1,
                    "fork": {"copy": {"call": {"operation": "copy", "executor": "target"}}},
                    "export": {"branch": "copy"},
                }
            ),
            "parent": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "fixture.parent/v1",
                    "revision": 1,
                    "fork": {"nested": {"call": {"recipe": "child"}}},
                    "export": {"branch": "nested"},
                }
            ),
        },
    ).compile()
    planner = planner_for(tmp_path, catalog=catalog, api=CatalogApi(), targets=Targets(operation))
    work = planner.create_work("fixture.parent/v1", (ROOT,))
    planner, progress, _ = plan_until_complete(planner, work, restart=True)
    assert progress.state == "ready"
    branch = progress.decision.plan.branches[0]
    assert branch.work.recipe.id == "fixture.child/v1"
    leaves = progress.decision.leaf_branches()
    assert len(leaves) == 1 and leaves[0].workflow_plan.work.recipe.id == "fixture.child/v1"
    assert leaves[0].workflow_plan.target_registration_id == "target"
    assert branch.work.fork_join.parent_work_id == work.work_id
    planner.state.engine.dispose()


def test_recipe_explicitly_rejects_unmatched_primary_and_sidecar_artifacts(tmp_path: Path):
    source = _conformance(source={"unmatched": "reject-work"})
    planner = planner_for(
        tmp_path, catalog=_catalog(checked=source), api=AssociatedMediaCatalogApi()
    )
    work = planner.create_work(source["id"], (ROOT,))
    planner, progress, _ = plan_until_complete(planner, work, restart=True)
    assert progress.state == "inapplicable"
    assert progress.outcome.code == "unmatched-recipe-input"
    assert planner.state.compiled_planning.inventory_ref(work.work_id).artifact_count == 3
    planner.state.engine.dispose()


def test_recipe_projection_count_is_defined_by_the_recipe(tmp_path: Path):
    operation = _operation()
    bindings = [
        {
            "from": "parameters",
            "path": f"/source-{index:03}",
            "to": "intent",
            "at": f"/destination-{index:03}",
            "mode": "insert",
        }
        for index in range(65)
    ]
    catalog = _simple(
        operation,
        parameters={"type": "object"},
        fork={
            "copy": {"call": {"operation": "copy", "executor": "target", "bind": bindings}},
        },
    )
    planner = planner_for(tmp_path, catalog=catalog, api=CatalogApi(), targets=Targets(operation))
    work = planner.create_work(
        "fixture.simple/v1",
        (ROOT,),
        effective_intent={f"source-{index:03}": index for index in range(65)},
    )
    planner, progress, _ = plan_until_complete(planner, work)
    assert progress.state == "ready"
    assert progress.decision.plan.branches[0].workflow_plan.work.effective_intent == {
        f"destination-{index:03}": index for index in range(65)
    }
    planner.state.engine.dispose()


def test_observer_preference_batches_large_collection_work_without_omission(tmp_path: Path):
    source = _conformance(
        observe={"metadata": {"use": "metadata", "executor": "exiftool"}},
        classify={"otherwise": None},
        groups={},
        fork={},
        decisions=[
            {
                "when": True,
                "no_output": {"code": "fixture.checked/v1", "message": "Complete scope checked."},
            }
        ],
    )
    planner = planner_for(
        tmp_path,
        catalog=_catalog(checked=source),
        observers=BatchMediaObservers(17),
        api=LargeCatalogApi(),
        targets=object(),
    )
    work = planner.create_work(source["id"], (ROOT,))
    planner, progress, evidence = plan_until_complete(planner, work, restart=True)
    assert progress.state == "no-output"
    selected = [subject.artifact_id for item in evidence for subject in item.request.subjects]
    assert len(selected) == len(set(selected)) == 257
    assert all(len(item.request.subjects) <= 17 for item in evidence)
    assert progress.outcome.decision.inventory.artifact_count == 257
    planner.state.engine.dispose()


def test_review_recipe_projects_semantic_intent_and_options_before_preflight(tmp_path: Path):
    source = (
        RecipeSourceCatalog.load(CATALOG).recipes["review"].model_dump(mode="json", by_alias=True)
    )
    source["observe"] = {}
    source["fork"]["review"]["call"]["options"]["threads"] = 2
    catalog = _catalog(review=source)
    sample_plan = ReviewSamplePlan.seal(
        ReviewSamplePlanPayload(
            samples_per_artifact=1,
            window_duration_ms=500,
            windows=(ReviewSampleWindow(artifact_id="subject-1", start_ms=100, duration_ms=500),),
        )
    )
    definition = review_evaluation_definition(
        recipe=catalog.recipe(source["id"]).ref,
        inputs=(ROOT,),
        sample_plan=sample_plan,
        variants=(
            ReviewVariant(
                id="crf-30", portable_intent={"label": "candidate"}, target_options={"crf": 30}
            ),
        ),
    )
    target = TargetDescriptor.seal(
        TargetDescriptorPayload(
            implementation_id="fixture.review/v1",
            implementation_version="1",
            source_revision="test",
            image_id="sha256:" + "e" * 64,
            operations=(
                TargetOperationSupport(
                    operation_id=REVIEW_MATERIALIZE_OPERATION.id,
                    operation_contract_sha256=REVIEW_MATERIALIZE_OPERATION.contract_sha256,
                    options_schema=JsonSchemaValidationProfile.from_schema(
                        "fixture.review-options/v1", {"type": "object"}
                    ),
                ),
            ),
        )
    )

    class ReviewTargets:
        def registration_ids(self):
            return ("review",)

        def descriptor(self, registration_id):
            assert registration_id == "review"
            return target

    planner = planner_for(tmp_path, catalog=catalog, api=CatalogApi(), targets=ReviewTargets())
    work = definition.child_work("crf-30")
    planner, progress, _ = plan_until_complete(planner, work, restart=True)
    assert progress.state == "ready"
    plan = progress.decision.plan.branches[0].workflow_plan
    request = planner.target_preflight_request(
        plan, progress.decision.selection_documents, descriptor=target
    )
    assert plan.work.evaluation == work.evaluation
    assert request.target_options == {"sampler_registration_id": "opus", "threads": 2, "crf": 30}
    assert request.intent == {
        "sample_plan": sample_plan.model_dump(mode="json"),
        "variant": {
            "id": "crf-30",
            "portable_intent": {"label": "candidate"},
        },
    }
    assert plan.input_retrieval_policy == "available-only"
    planner.state.engine.dispose()


def test_supplied_recipes_embed_exact_maintained_contracts_and_explicit_cost_policy():
    catalog = load_recipe_catalog(CATALOG)
    assert {item.contract.contract_sha256 for item in catalog.closure.operations} >= {
        AUDIO_ARCHIVE_OPERATION.contract_sha256,
        AV1_OPUS_ARCHIVE_OPERATION.contract_sha256,
        REVIEW_MATERIALIZE_OPERATION.contract_sha256,
    }
    assert any(
        item.contract == MEDIA_METADATA_OBSERVER_CONTRACT for item in catalog.closure.observers
    )
    assert {
        branch.call.retrieve for recipe in catalog.recipes for branch in recipe.branches.values()
    } == {
        "allow",
        "available-only",
    }


def test_manual_planning_does_not_consult_derivation(tmp_path: Path):
    class DerivedCatalogApi(CatalogApi):
        def get_collection_derivation(self, collection_id):
            raise AssertionError("derivation is evidence, not planning authority")

    operation = _operation()
    planner = planner_for(
        tmp_path, catalog=_simple(operation), api=DerivedCatalogApi(), targets=Targets(operation)
    )
    work = planner.create_work("fixture.simple/v1", (ROOT,))
    planner, progress, _ = plan_until_complete(planner, work)
    assert work.inputs == (ROOT,) and progress.state == "ready"
    planner.state.engine.dispose()


def test_production_planner_resolves_overlapping_branches_into_one_exact_join(tmp_path: Path):
    operation = _operation("collection")
    join = _operation("collection", id="fixture.join/v1")
    catalog = RecipeSourceCatalog(
        resources={
            "copy": OperationResource(contract=operation),
            "join": OperationResource(contract=join),
        },
        recipes={
            "recipe": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "fixture.fork-join/v1",
                    "revision": 1,
                    "fork": {
                        name: {"call": {"operation": "copy", "executor": "target"}}
                        for name in ("audio", "video")
                    },
                    "join": {
                        "members": {name: ["example.result/v1"] for name in ("audio", "video")},
                        "call": {"operation": "join", "executor": "join-target"},
                    },
                    "export": "join",
                }
            )
        },
    ).compile()
    targets = Targets(operation)
    join_targets = Targets(join, names=("join-target",))
    targets.names = ("target", "join-target")
    original = targets.descriptor
    targets.descriptor = lambda name: (
        join_targets.descriptor(name) if name == "join-target" else original(name)
    )
    planner = planner_for(tmp_path, catalog=catalog, api=CatalogApi(), targets=targets)
    work = planner.create_work("fixture.fork-join/v1", (ROOT,))
    planner, progress, _ = plan_until_complete(planner, work, restart=True)
    decision = progress.decision
    assert (
        decision.plan.branches[0].artifact_selection == decision.plan.branches[1].artifact_selection
    )
    selections, settlements = dict(decision.selection_documents), []
    for collection_id, branch in enumerate(decision.plan.branches, start=21):
        root = CollectionRootIdentityRef(
            collection_id=str(collection_id),
            archive_root_sha256="a" * 64,
            artifact_set_identity="b" * 64,
        )
        output = ArtifactSelection.seal(
            (
                WorkArtifactSubject(
                    id=f"{branch.branch_id}-output",
                    role="example.result/v1",
                    collection=root,
                    artifact_id=f"{collection_id:064x}",
                    bytes="12",
                    sha256="e" * 64,
                ),
            )
        )
        selections[output.selection_sha256] = output
        settlements.append(
            BranchSettlement.seal(
                branch=branch,
                producer_settlement_sha256="f" * 64,
                derivation_sha256="d" * 64,
                output_collection=root,
                output_selection=output,
            )
        )
    resolved = resolve_join_plan(decision.plan, selections, settlements)
    assert resolved is not None
    join_plan, selected = resolved
    request = planner.target_preflight_request(
        join_plan.workflow_plan,
        {item.selection_sha256: item for item in selected},
        descriptor=join_targets.target,
    )
    assert request.inputs.selection.artifact_count == 2
    assert {
        item.collection.collection_id for selection in selected for item in selection.artifacts
    } == {21, 22}
    planner.state.engine.dispose()


def _retirement_source(*, complete):
    operation = _operation("collection", source_collection_retirement_permitted=True)
    resources = RecipeSourceCatalog.load(CATALOG).resources
    resources["copy"] = OperationResource(contract=operation)
    select = {
        "select": {"groups": "primary"},
        "when": {
            "facts": {
                "view": "metadata.artifacts",
                "scope": "candidate",
                "quantifier": "every",
                "where": {
                    "items": {
                        "path": "/facts",
                        "quantifier": "any",
                        "where": {
                            "all": [
                                {
                                    "test": {
                                        "path": "/name",
                                        "op": "eq",
                                        "value": "container-format",
                                    }
                                },
                                {"test": {"path": "/value", "op": "eq", "value": "MP4"}},
                            ]
                        },
                    }
                },
            }
        },
        "call": {"operation": "copy", "executor": "target"},
    }
    fork = {"video": select}
    if complete:
        fork["all"] = {"call": {"operation": "copy", "executor": "target"}}
    catalog = RecipeSourceCatalog(
        resources=resources,
        recipes={
            "retirement": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "fixture.retirement/v1",
                    "revision": 1,
                    "observe": {"metadata": {"use": "metadata", "executor": "exiftool"}},
                    "groups": {"primary": {"primary": "source"}},
                    "fork": fork,
                    "source": {"retirement": {"mode": "after-settlement"}},
                }
            )
        },
    ).compile()
    return catalog, operation


def test_retirement_plan_accepts_overlapping_selections_covering_complete_inventory(tmp_path: Path):
    catalog, operation = _retirement_source(complete=True)
    planner = planner_for(
        tmp_path, catalog=catalog, api=MultiArtifactCatalogApi(), targets=Targets(operation)
    )
    work = planner.create_work("fixture.retirement/v1", (ROOT,))
    planner, progress, _ = plan_until_complete(planner, work, restart=True)
    assert progress.state == "ready"
    assert progress.decision.plan.source_collection_retirement_policy == "retire-after-settlement"
    selected = {
        branch.branch_id: progress.decision.selection_documents[
            branch.artifact_selection.selection_sha256
        ]
        for branch in progress.decision.plan.branches
    }
    assert (
        len(selected["all"].artifacts) == 2
        and selected["video"].artifacts[0] in selected["all"].artifacts
    )
    planner.state.engine.dispose()


def test_retirement_plan_rejects_incomplete_inventory_before_target_preflight(tmp_path: Path):
    catalog, _ = _retirement_source(complete=False)
    planner = planner_for(
        tmp_path, catalog=catalog, api=MultiArtifactCatalogApi(), targets=object()
    )
    work = planner.create_work("fixture.retirement/v1", (ROOT,))
    planner, progress, _ = plan_until_complete(planner, work, restart=True)
    assert (
        progress.state == "inapplicable" and progress.outcome.code == "unsafe-retirement-coverage"
    )
    planner.state.engine.dispose()


def test_catalog_rejects_retirement_recipe_using_audio_only_operation():
    source = _conformance(source={"retirement": {"mode": "after-settlement"}})
    with pytest.raises(ValueError, match="retirement|retire"):
        _catalog(unsafe=source)

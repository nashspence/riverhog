from __future__ import annotations

from types import SimpleNamespace

import pytest
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.recipes import RecipePlanner
from stove0_protocol import (
    CollectionRootIdentityRef,
    JsonSchemaValidationProfile,
    PreviewOutcome,
    WorkflowPreview,
    WorkflowPreviewPayload,
    WorkflowPreviewRequest,
    WorkflowPreviewRequestPayload,
)
from stove0_protocol.models import JSON_SCHEMA_ONLY_SEMANTIC_PROFILE
from stove0_recipe_config.catalog import CompiledRecipeCatalog, RecipeSourceCatalog
from stove0_recipe_config.dependencies import OperationResource
from stove0_recipe_config.source import RecipeSource
from stove0_target_protocol import (
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
    OutputArtifactContract,
    TargetDescriptor,
    TargetDescriptorPayload,
    TargetOperationSupport,
)
from test_compiled_groups import _observed_state


def _operation(kind="external-effect", **changes):
    fields = {
        "id": "example.runtime-operation/v1",
        "result_kind": kind,
        "intent_schema": JsonSchemaValidationProfile.from_schema(
            "example.intent/v1", {"type": "object"}
        ),
        "intent_semantics": JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        "inputs": [InputArtifactContract(role="*")],
        **changes,
    }
    if kind == "external-effect":
        fields["effect_receipt_schema"] = JsonSchemaValidationProfile.from_schema(
            "example.receipt/v1", {"type": "object"}
        )
    else:
        fields["inputs"] = [
            InputArtifactContract(role="stove0.source/v1", allowed_dispositions=("transformed",))
        ]
        fields["outputs"] = [
            OutputArtifactContract(
                role="example.result/v1", derived_from_roles=("stove0.source/v1",)
            )
        ]
    return OperationContract.seal(OperationContractPayload(**fields))


class Targets:
    def __init__(self, operation, *, names=("target",)):
        self.operation, self.names = operation, names
        self.contacts = []
        self.target = TargetDescriptor.seal(
            TargetDescriptorPayload(
                protocol="stove0-effect-target/v1"
                if operation.result_kind == "external-effect"
                else "stove0-transform-target/v1",
                implementation_id="example.target/v1",
                implementation_version="1",
                source_revision="fixture",
                image_id="sha256:" + "d" * 64,
                operations=(
                    TargetOperationSupport(
                        operation_id=operation.id,
                        operation_contract_sha256=operation.contract_sha256,
                        result_kind=operation.result_kind,
                        options_schema=JsonSchemaValidationProfile.from_schema(
                            "example.options/v1", {"type": "object"}
                        ),
                    ),
                ),
            )
        )

    def registration_ids(self):
        return self.names

    def descriptor(self, registration_id):
        assert registration_id in self.names
        self.contacts.append(registration_id)
        return self.target


class Inventory:
    root = CollectionRootIdentityRef(
        collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
    )

    def get_collection(self, collection_id):
        assert collection_id == 1
        return self.root.model_dump(mode="json")

    def get_portable_collection_inventory(self, collection_id, **kwargs):
        assert collection_id == 1
        return SimpleNamespace(
            authority=SimpleNamespace(inventory_identity="c" * 64),
            artifacts=(SimpleNamespace(artifact_id="d" * 64, bytes=1, sha256="e" * 64),),
            complete=True,
            next_cursor=None,
        )


def _source(id, **changes):
    return RecipeSource.model_validate(
        {"format": "stove0-recipe/v1", "id": id, "revision": 1, **changes}
    )


def _catalog(operation, recipes):
    return RecipeSourceCatalog(
        resources={"operation": OperationResource(contract=operation)}, recipes=recipes
    ).compile()


def _run(planner, work, *, restart=True):
    url, state = str(planner.state.engine.url), planner.state
    for step in range(500):
        before = len(planner.targets.contacts) if isinstance(planner.targets, Targets) else 0
        progress = planner.step(work)
        if isinstance(planner.targets, Targets):
            assert len(planner.targets.contacts) - before <= 1
        if progress.state not in {"pending", "question"}:
            return planner, progress
        assert progress.state != "question", "fixture omitted required task testimony"
        if restart and step % 7 == 0:
            owner = state.planning_owner
            state.engine.dispose()
            state = SqlAlchemyStateStore(url)
            if owner is not None:
                state = state.planning_context(*owner)
            # Installed source/aliases are unnecessary to resume accepted work.
            planner = RecipePlanner(
                catalog=CompiledRecipeCatalog(),
                state=state,
                riverhog=planner.riverhog,
                observers=planner.observers,
                targets=planner.targets,
            )
    pytest.fail("compiled recipe planning stopped making progress")


def test_group_branch_calls_once_with_all_selected_groups_and_retains_overlapping_branch(tmp_path):
    state, url, work, subjects = _observed_state(
        tmp_path,
        ["primary", "primary", "associated", "associated", "ignored"],
        preferred=[(0, 2), (1, 3)],
        fork={
            "groups": {
                "select": {"groups": "media"},
                "call": {"operation": "effect", "evidence": ["relations"]},
            },
            "overlap": {"call": {"operation": "effect"}},
        },
    )
    recipe, closure = state.recipe_definitions.load(work.recipe)
    operation = closure.operations[0].contract
    planner = RecipePlanner(
        catalog=CompiledRecipeCatalog(recipes=(recipe,), closure=closure),
        state=state,
        riverhog=object(),
        observers=object(),
        targets=Targets(operation),
    )
    planner, progress = _run(planner, work)
    assert progress.state == "ready"
    decision = progress.decision
    assert len(decision.plan.branches) == 2
    groups, overlap = decision.plan.branches
    assert len(groups.workflow_plan.input_groups) == 2
    assert (
        groups.artifact_selection.artifact_count == overlap.artifact_selection.artifact_count == 4
    )
    assert groups.artifact_selection == overlap.artifact_selection
    assert groups.workflow_plan.observations[0].request.task_id == "relations"
    assert len(decision.leaf_branches()) == 2
    assert planner.state.compiled_planning.inventory_ref(work.work_id).artifact_count == 5
    planner.state.engine.dispose()


def test_planning_invocations_preserve_work_identity_and_rebind_only_fresh_provider_choices(
    tmp_path,
):
    operation = _operation()
    catalog = _catalog(
        operation,
        {
            "effect": _source(
                "example.effect/v1",
                fork={"delivery": {"call": {"operation": "operation"}}},
            )
        },
    )
    targets = Targets(operation)
    base = RecipePlanner(
        catalog=catalog,
        state=SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}"),
        riverhog=Inventory(),
        observers=object(),
        targets=targets,
    )
    work = base.create_work("example.effect/v1", (Inventory.root,))
    first, a = _run(base.for_invocation("preview", "1" * 64), work)
    second, b = _run(base.for_invocation("preview", "2" * 64), work)
    assert a.state == b.state == "ready"
    assert a.decision == b.decision  # Storage ownership cannot enter semantic evidence.
    assert (
        first.state.compiled_runtime.load(work.work_id)["work_id"]
        != second.state.compiled_runtime.load(work.work_id)["work_id"]
    )
    original = targets.target
    targets.target = TargetDescriptor.seal(
        TargetDescriptorPayload.model_validate(
            {
                **original.model_dump(mode="json", exclude={"descriptor_sha256"}),
                "source_revision": "another supplied execution",
            }
        )
    )
    retained = first.step(work)
    fresh, c = _run(base.for_invocation("preview", "3" * 64), work)
    assert retained.decision == a.decision
    assert (
        c.decision.plan.branches[0].workflow_plan.target_descriptor_sha256
        == targets.target.descriptor_sha256
    )
    assert c.decision.plan.branch_set_sha256 != a.decision.plan.branch_set_sha256
    assert c.decision.plan.parent_work == work
    fresh.state.engine.dispose()


def test_no_output_binds_index_and_proof_even_with_repeated_human_codes(tmp_path):
    catalog = _catalog(
        _operation(),
        {
            "decision": _source(
                "example.decision/v1",
                decisions=[
                    {"when": False, "no_output": {"code": "done", "message": "First"}},
                    {"when": True, "no_output": {"code": "done", "message": "Second"}},
                ],
            )
        },
    )
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    planner = RecipePlanner(
        catalog=catalog, state=state, riverhog=Inventory(), observers=object(), targets=object()
    )
    work = planner.create_work("example.decision/v1", (Inventory.root,))
    planner, progress = _run(planner, work)
    assert progress.state == "no-output"
    proof = progress.outcome.decision
    assert proof.decision_index == 1 and proof.definition.message == "Second"
    assert [condition.truth.value for condition in proof.conditions] == ["false", "true"]
    definition, policy, grace = planner.no_output_policy(work, decision=proof)
    assert (definition, policy, grace) == (proof.definition, "retain", 0)
    request = WorkflowPreviewRequest.seal(WorkflowPreviewRequestPayload(work=work))
    preview = WorkflowPreview.seal(
        WorkflowPreviewPayload(
            preview_id=request.preview_id,
            state="no_action",
            work=work,
            outcome=PreviewOutcome(code=proof.definition.code, message=proof.definition.message),
            no_output_decision=proof,
        )
    )
    assert preview.no_output_decision == proof
    with pytest.raises(ValueError, match="indexed decision"):
        WorkflowPreviewPayload.model_validate(
            {
                **preview.model_dump(mode="json", exclude={"preview_sha256"}),
                "no_output_decision": None,
            }
        )
    assert planner.state.recipe_definitions.load(work.recipe)[0].ref == work.recipe
    planner.state.engine.dispose()


def test_nested_no_output_is_successful_required_child(tmp_path):
    operation = _operation()
    catalog = _catalog(
        operation,
        {
            "child": _source(
                "example.child/v1",
                decisions=[
                    {
                        "when": True,
                        "no_output": {"code": "done", "message": "Evaluated successfully"},
                    },
                ],
            ),
            "parent": _source(
                "example.parent/v1",
                fork={
                    "child": {"call": {"recipe": "child"}},
                    "effect": {"call": {"operation": "operation"}},
                },
            ),
        },
    )
    planner = RecipePlanner(
        catalog=catalog,
        state=SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}"),
        riverhog=Inventory(),
        observers=object(),
        targets=Targets(operation),
    )
    work = planner.create_work("example.parent/v1", (Inventory.root,))
    planner, progress = _run(planner, work)
    assert progress.state == "ready"
    child, effect = progress.decision.plan.branches
    assert child.kind == "no-output" and effect.kind == "leaf"
    assert child.decision.recipe == child.work.recipe
    assert child.decision.inventory == child.artifact_selection
    assert planner.no_output_policy(child.work, decision=child.decision)[1:] == ("retain", 0)
    planner.state.engine.dispose()


def test_single_output_child_exports_existing_branch_without_dummy_join(tmp_path):
    operation = _operation("collection", source_collection_retirement_permitted=True)
    catalog = _catalog(
        operation,
        {
            "child": _source(
                "example.child/v1",
                fork={"output": {"call": {"operation": "operation"}}},
                export={"branch": "output"},
                source={"retirement": {"mode": "after-settlement", "grace_seconds": 17}},
            ),
            "parent": _source(
                "example.parent/v1",
                fork={"child": {"call": {"recipe": "child"}}},
                export={"branch": "child"},
            ),
        },
    )
    planner = RecipePlanner(
        catalog=catalog,
        state=SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}"),
        riverhog=Inventory(),
        observers=object(),
        targets=Targets(operation),
    )
    work = planner.create_work("example.parent/v1", (Inventory.root,))
    planner, progress = _run(planner, work)
    assert progress.state == "ready"
    assert progress.decision.plan.export.branch == "child"
    child = next(iter(progress.decision.branch_sets))
    assert child.export.branch == "output" and child.join is None
    assert child.source_collection_retirement_policy == "retain"
    assert child.source_collection_retirement_grace_seconds == 0
    assert len(progress.decision.leaf_branches()) == 1
    leaf = progress.decision.leaf_branches()[0]
    exact_operation = planner.operation_contract(leaf.workflow_plan.operation)
    assert exact_operation == operation
    planner.state.engine.dispose()


def test_worker_quantum_and_single_steps_seal_identical_plans_across_restart(tmp_path):
    from stove0_core.metadata_steps import _preparing, advance_planning

    operation = _operation("external-effect")
    source = _source(
        "example.worker-quantum/v1", fork={"effect": {"call": {"operation": "operation"}}}
    )
    catalog = _catalog(operation, {"recipe": source})
    outcomes = []
    for mode in ("single", "quantum"):
        url = f"sqlite:///{tmp_path / (mode + '.db')}"
        state = SqlAlchemyStateStore(url)
        planner = RecipePlanner(
            catalog=catalog,
            state=state,
            riverhog=Inventory(),
            observers=object(),
            targets=Targets(operation),
        )
        work = planner.create_work(source.id, (Inventory.root,))
        planner = planner.for_invocation("work", work.work_id)
        for ordinal in range(500):
            if mode == "single":
                progress = planner.step(work)
            else:
                with _preparing():
                    progress = advance_planning(
                        planner, work, owner_kind="work", owner_id=work.work_id, maximum_steps=7
                    )
            if progress.state != "pending":
                assert progress.state == "ready"
                outcomes.append(progress.decision)
                break
            if ordinal == 0:
                planner.state.engine.dispose()
                state = SqlAlchemyStateStore(url).planning_context("work", work.work_id)
                planner = RecipePlanner(
                    catalog=CompiledRecipeCatalog(),
                    state=state,
                    riverhog=Inventory(),
                    observers=object(),
                    targets=Targets(operation),
                )
        else:
            pytest.fail("worker quantum did not complete the exact retained plan")
        planner.state.engine.dispose()
    assert len(outcomes) == 2 and outcomes[0] == outcomes[1]

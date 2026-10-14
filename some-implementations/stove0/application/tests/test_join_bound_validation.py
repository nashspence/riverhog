"""A fully bound join must be valid before any required branch can execute."""

import pytest
from jsonschema import ValidationError
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.recipes import RecipePlanner
from stove0_protocol import JsonSchemaValidationProfile
from stove0_recipe_config.catalog import RecipeSourceCatalog
from stove0_recipe_config.dependencies import OperationResource
from stove0_target_protocol import (
    TargetDescriptor,
    TargetDescriptorPayload,
    TargetOperationSupport,
)
from test_compiled_recipe_runtime import Inventory, Targets, _operation, _run, _source


class JoinTargets(Targets):
    def __init__(self, collection, effect):
        super().__init__(collection, names=("target", "effect"))
        self.effect = Targets(effect).target

    def descriptor(self, registration_id):
        self.contacts.append(registration_id)
        return self.effect if registration_id == "effect" else self.target


def _planner(tmp_path, destination, keyword):
    schema = {
        "type": "object",
        "properties": {"x": {"type": "integer"}, "y": {"type": "integer"}},
        keyword: [
            {"required": ["x"], "properties": {"x": {"minimum": 1}}},
            {"required": ["y"]},
        ],
    }
    child = _operation("collection", id="example.child/v1")
    effect = _operation(id="example.effect/v1")
    join = _operation(
        "collection",
        id="example.join/v1",
        intent_schema=JsonSchemaValidationProfile.from_schema(
            "example.join-intent/v1", schema if destination == "intent" else {"type": "object"}
        ),
    )
    operations = {"child": child, "effect": effect, "join": join}
    source = _source(
        "example.bound-join/v1",
        parameters={"type": "object", "required": ["x"]},
        fork={
            "output-a": {"call": {"operation": "child"}},
            "output-b": {"call": {"operation": "child"}},
            "delivery": {"call": {"operation": "effect"}},
        },
        join={
            "members": {
                "output-a": ["example.result/v1"],
                "output-b": ["example.result/v1"],
            },
            "call": {
                "operation": "join",
                "bind": [
                    {
                        "from": "parameters",
                        "path": "/x",
                        "to": destination,
                        "at": "/x",
                        "mode": "insert",
                    }
                ],
            },
        },
        export="join",
    )
    catalog = RecipeSourceCatalog(
        resources={name: OperationResource(contract=op) for name, op in operations.items()},
        recipes={"recipe": source},
    ).compile()
    targets = JoinTargets(child, effect)
    targets.target = TargetDescriptor.seal(
        TargetDescriptorPayload.model_validate(
            {
                **targets.target.model_dump(mode="json", exclude={"descriptor_sha256"}),
                "operations": [
                    TargetOperationSupport(
                        operation_id=op.id,
                        operation_contract_sha256=op.contract_sha256,
                        result_kind=op.result_kind,
                        options_schema=JsonSchemaValidationProfile.from_schema(
                            "example.options/v1",
                            schema
                            if name == "join" and destination == "options"
                            else {"type": "object"},
                        ),
                    ).model_dump(mode="json")
                    for name, op in operations.items()
                    if name != "effect"
                ],
            }
        )
    )
    return RecipePlanner(
        catalog=catalog,
        state=SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}"),
        riverhog=Inventory(),
        observers=object(),
        targets=targets,
    )


@pytest.mark.parametrize("destination", ["intent", "options"])
@pytest.mark.parametrize("keyword", ["anyOf", "oneOf"])
@pytest.mark.parametrize("value", [0, "wrong"])
def test_invalid_bound_join_cannot_accept_the_parent_or_its_required_effects(
    tmp_path, destination, keyword, value
):
    planner = _planner(tmp_path, destination, keyword)
    work = planner.create_work(
        "example.bound-join/v1", (Inventory.root,), effective_intent={"x": value}
    )
    with pytest.raises(ValidationError):
        _run(planner, work, restart=False)
    # Nested declarations are preparation only. Execution admission needs the
    # root's complete accepted decision, which must not exist on this failure.
    row = planner.state.compiled_runtime.load(work.work_id)
    assert row["phase"] != "ready" and row["decision_json"] is None
    records, _ = planner.state.scan_work(phases=("eligible", "claimed"), after_work_id="", limit=10)
    assert records == []
    planner.state.engine.dispose()


@pytest.mark.parametrize("destination", ["intent", "options"])
@pytest.mark.parametrize("keyword", ["anyOf", "oneOf"])
def test_valid_deferred_join_binding_survives_restart_and_seals_exact_arguments(
    tmp_path, destination, keyword
):
    planner = _planner(tmp_path, destination, keyword)
    work = planner.create_work(
        "example.bound-join/v1", (Inventory.root,), effective_intent={"x": 7}
    )
    planner, progress = _run(planner, work)
    assert progress.state == "ready" and len(progress.decision.leaf_branches()) == 3
    join = progress.decision.plan.join
    accepted = (
        join.effective_intent
        if destination == "intent"
        else (join.workflow_intent.requested_target_options)
    )
    assert accepted == {"x": 7}
    planner.state.engine.dispose()

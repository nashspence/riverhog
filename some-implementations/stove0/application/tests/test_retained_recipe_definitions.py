from __future__ import annotations

import pytest
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_protocol import RecipeIdentityRef
from stove0_protocol.models import JSON_SCHEMA_ONLY_SEMANTIC_PROFILE, JsonSchemaValidationProfile
from stove0_recipe_config.compiler import compile_recipe
from stove0_recipe_config.dependencies import (
    OperationResource,
    RecipeDependencyCatalog,
    RecipeDependencyClosure,
)
from stove0_recipe_config.source import RecipeSource
from stove0_target_protocol import (
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
)


def _operation(id):
    return OperationContract.seal(
        OperationContractPayload(
            id=id,
            result_kind="external-effect",
            intent_schema=JsonSchemaValidationProfile.from_schema(
                id + ".intent", {"type": "object"}
            ),
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            inputs=[InputArtifactContract(role="*")],
            effect_receipt_schema=JsonSchemaValidationProfile.from_schema(
                id + ".receipt", {"type": "object"}
            ),
        )
    )


def test_queued_work_keeps_exact_compiled_policy_after_installation_changes(tmp_path):
    url = f"sqlite:///{tmp_path / 'state.db'}"
    source = RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.recipe/v1",
            "revision": 1,
            "fork": {"effect": {"call": {"operation": "effect"}}},
        }
    )
    recipe, closure = compile_recipe(
        source,
        RecipeDependencyCatalog(
            resources={"effect": OperationResource(contract=_operation("example.effect/v1"))}
        ),
    )
    state = SqlAlchemyStateStore(url)
    state.recipe_definitions.retain(recipe, closure)
    state.engine.dispose()
    # A reopened service does not need the installation catalog or source YAML.
    state = SqlAlchemyStateStore(url)
    retained = state.recipe_definitions.load(recipe.ref)
    assert retained == (recipe, closure)
    changed_source = RecipeSource.model_validate(
        {**source.model_dump(mode="json", by_alias=True), "revision": 2}
    )
    changed, _ = compile_recipe(
        changed_source,
        RecipeDependencyCatalog(
            resources={"effect": OperationResource(contract=_operation("example.other-effect/v1"))}
        ),
    )
    assert changed.sha256 != recipe.sha256
    assert state.recipe_definitions.load(recipe.ref) == retained
    wrong = RecipeIdentityRef(id=recipe.id, revision="2", sha256=recipe.sha256)
    with pytest.raises(ValueError, match="queued work"):
        state.recipe_definitions.load(wrong)
    state.engine.dispose()


def test_retained_closure_drops_unrelated_installation_resources(tmp_path):
    source = RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.recipe/v1",
            "revision": 1,
            "fork": {"effect": {"call": {"operation": "effect"}}},
        }
    )
    recipe, closure = compile_recipe(
        source,
        RecipeDependencyCatalog(
            resources={"effect": OperationResource(contract=_operation("example.effect/v1"))}
        ),
    )
    unused = OperationResource(contract=_operation("example.unused/v1"))
    wider = RecipeDependencyClosure(
        operations=tuple(
            sorted((*closure.operations, unused), key=lambda r: r.contract.contract_sha256)
        )
    )
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    state.recipe_definitions.retain(recipe, wider)
    assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    state.engine.dispose()


def test_retention_protects_exact_nested_dependencies_then_prunes_unused_definitions(tmp_path):
    from sqlalchemy import update
    from stove0_core import Stove0WorkService
    from stove0_core.persistence import _WorkRow
    from stove0_core.recipes import RecipePlanner
    from stove0_recipe_config.catalog import RecipeSourceCatalog
    from test_compiled_recipe_runtime import Inventory

    leaf = RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.child/v1",
            "revision": 1,
            "fork": {"effect": {"call": {"operation": "effect"}}},
        }
    )
    root = RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.parent/v1",
            "revision": 1,
            "fork": {"child": {"call": {"recipe": "leaf"}}},
        }
    )
    catalog = RecipeSourceCatalog(
        resources={"effect": OperationResource(contract=_operation("example.effect/v1"))},
        recipes={"parent": root, "leaf": leaf},
    ).compile()
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    planner = RecipePlanner(
        catalog=catalog,
        state=state,
        riverhog=Inventory(),
        observers=object(),
        targets=object(),
    )
    work = planner.create_work(root.id, (Inventory.root,))
    service = Stove0WorkService(state)
    record = service.create_or_resume(work)
    definitions = state.recipe_definitions.table
    with state.engine.begin() as connection:
        connection.execute(update(definitions).values(updated_at="2000-01-01T00:00:00.000000000Z"))
    assert (
        state.prune_operational_state(cutoff="2001-01-01T00:00:00.000000000Z")["recipe_definitions"]
        == 0
    )
    child = catalog.recipe(leaf.id)
    assert state.recipe_definitions.load(work.recipe) is not None
    assert state.recipe_definitions.load(child.ref) is not None
    service.cancel(work.work_id, expected_revision=record.revision)
    with state.engine.begin() as connection:
        connection.execute(update(_WorkRow).values(updated_at="2000-01-01T00:00:00.000000000Z"))
    first = state.prune_operational_state(cutoff="2001-01-01T00:00:00.000000000Z")
    assert first["work"] == 1 and first["recipe_definitions"] == 1
    assert state.recipe_definitions.load(work.recipe) is None
    # A bounded pass removes roots first; dependency edges keep the child until
    # no retained parent or ordinary invocation needs it.
    second = state.prune_operational_state(cutoff="2001-01-01T00:00:00.000000000Z")
    assert second["recipe_definitions"] == 1
    assert state.recipe_definitions.load(child.ref) is None
    # The installed source remains available; a new request retains its exact
    # complete closure before exposing an invocation identity.
    assert planner.create_work(root.id, (Inventory.root,)) == work
    assert state.recipe_definitions.load(work.recipe) is not None
    assert state.recipe_definitions.load(child.ref) is not None
    state.engine.dispose()


def _retained_fixture(tmp_path):
    source = RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.cached/v1",
            "revision": 1,
            "fork": {"effect": {"call": {"operation": "effect"}}},
        }
    )
    recipe, closure = compile_recipe(
        source,
        RecipeDependencyCatalog(
            resources={"effect": OperationResource(contract=_operation("example.effect/v1"))}
        ),
    )
    from stove0_core.recipe_definitions import _verified_documents

    _verified_documents.cache_clear()
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'cached.db'}")
    state.recipe_definitions.retain(recipe, closure)
    return state, recipe, closure


def test_repeated_exact_definition_loads_validate_once_and_return_private_mappings(
    tmp_path, monkeypatch
):
    import stove0_core.recipe_definitions as definitions

    state, recipe, closure = _retained_fixture(tmp_path)
    verify, calls = definitions.verify_compiled_recipe, []

    def counted(*args):
        calls.append(args[0].sha256)
        return verify(*args)

    monkeypatch.setattr(definitions, "verify_compiled_recipe", counted)
    first, _ = state.recipe_definitions.load(recipe.ref)
    first.branches.clear()
    for _ in range(3):
        assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    assert calls == [recipe.sha256]
    wrong = RecipeIdentityRef(id="example.other/v1", revision="1", sha256=recipe.sha256)
    with pytest.raises(ValueError, match="queued work"):
        state.recipe_definitions.load(wrong)
    state.engine.dispose()


@pytest.mark.parametrize("field", ["recipe_json", "closure_json"])
def test_warm_validation_cache_rejects_changed_stored_preimages(tmp_path, field):
    from sqlalchemy import update

    state, recipe, closure = _retained_fixture(tmp_path)
    assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    store = state.recipe_definitions
    with state.engine.begin() as connection:
        connection.execute(update(store.table).values({field: "{}"}))
    with pytest.raises(ValueError):
        store.load(recipe.ref)
    state.engine.dispose()


def test_valid_definition_larger_than_cache_budget_remains_usable(tmp_path, monkeypatch):
    import stove0_core.recipe_definitions as definitions

    state, recipe, closure = _retained_fixture(tmp_path)
    store = state.recipe_definitions
    monkeypatch.setattr(definitions, "_VERIFIED_DOCUMENT_BYTES", 1)
    verify, calls = definitions.verify_compiled_recipe, []

    def counted(*args):
        calls.append(args[0].sha256)
        return verify(*args)

    monkeypatch.setattr(definitions, "verify_compiled_recipe", counted)
    for _ in range(3):
        assert store.load(recipe.ref) == (recipe, closure)
    assert len(calls) == 3
    assert definitions._verified_documents.cache_info().currsize == 0
    state.engine.dispose()

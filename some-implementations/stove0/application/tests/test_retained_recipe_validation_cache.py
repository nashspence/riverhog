"""Reuse immutable compilation work, never cached database or caller authority."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path

import pytest
from sqlalchemy import delete, select, update
from stove0_core import recipe_definitions
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_protocol import RecipeIdentityRef
from stove0_recipe_config.compiler import compile_recipe
from stove0_recipe_config.dependencies import OperationResource, RecipeDependencyCatalog
from stove0_recipe_config.source import RecipeSource
from test_retained_recipe_definitions import _operation


@pytest.fixture
def retained(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    recipe, closure = compile_recipe(
        RecipeSource.model_validate(
            {
                "format": "stove0-recipe/v1",
                "id": "example.cached/v1",
                "revision": 1,
                "fork": {"effect": {"call": {"operation": "effect"}}},
            }
        ),
        RecipeDependencyCatalog(
            resources={"effect": OperationResource(contract=_operation("example.effect/v1"))}
        ),
    )
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'control.db'}")
    state.recipe_definitions.retain(recipe, closure)
    calls = []
    original = recipe_definitions.verify_compiled_recipe

    def verify(*args):
        calls.append(args[0].sha256)
        return original(*args)

    monkeypatch.setattr(recipe_definitions, "verify_compiled_recipe", verify)
    # Clearing a performance cache must never be necessary for correctness.
    cache = getattr(recipe_definitions, "_verified_documents", None)
    if cache is not None:
        cache.cache_clear()
    try:
        yield state, recipe, closure, calls
    finally:
        state.engine.dispose()
        if cache is not None:
            cache.cache_clear()


def test_exact_documents_are_verified_once_across_fresh_planning_contexts(retained):
    state, recipe, closure, calls = retained
    for index in range(8):
        scoped = state.planning_context("work", f"{index:064x}")
        assert scoped.recipe_definitions.load(recipe.ref) == (recipe, closure)
    assert calls == [recipe.sha256]


def test_cached_models_cannot_be_mutated_by_a_previous_caller(retained):
    state, recipe, closure, calls = retained
    first_recipe, first_closure = state.recipe_definitions.load(recipe.ref)
    first_recipe.branches.clear()
    first_closure.operations[0].contract.intent_schema.document["properties"] = {"bad": {}}
    assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    assert calls == [recipe.sha256]


def test_cached_meaning_does_not_hide_missing_or_changed_database_rows(retained):
    state, recipe, closure, calls = retained
    store = state.recipe_definitions
    assert store.load(recipe.ref) == (recipe, closure)
    with state.engine.begin() as connection:
        raw = connection.scalar(select(store.table.c.closure_json))
        connection.execute(update(store.table).values(closure_json="{}"))
    with pytest.raises(ValueError):
        store.load(recipe.ref)
    with state.engine.begin() as connection:
        connection.execute(update(store.table).values(closure_json=raw))
    assert store.load(recipe.ref) == (recipe, closure)
    with state.engine.begin() as connection:
        connection.execute(delete(store.table))
    assert store.load(recipe.ref) is None
    # The corrupt closure is revalidated (and rejected), never accepted via cache.
    assert calls == [recipe.sha256] * 2


@pytest.mark.parametrize("changed", ({"id": "example.wrong/v1"}, {"revision": "2"}))
def test_every_warm_read_still_checks_the_callers_exact_identity(retained, changed):
    state, recipe, closure, calls = retained
    assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    wrong = RecipeIdentityRef.model_validate({**recipe.ref.model_dump(mode="json"), **changed})
    with pytest.raises(ValueError, match="queued work"):
        state.recipe_definitions.load(wrong)
    assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    assert calls == [recipe.sha256]


def test_failed_validation_never_installs_a_success_cache_entry(retained, monkeypatch):
    state, recipe, closure, calls = retained
    verify = recipe_definitions.verify_compiled_recipe
    attempts = []

    def fail_once(*args):
        attempts.append(None)
        if len(attempts) == 1:
            raise ValueError("injected invalid exact closure")
        return verify(*args)

    monkeypatch.setattr(recipe_definitions, "verify_compiled_recipe", fail_once)
    with pytest.raises(ValueError, match="invalid exact closure"):
        state.recipe_definitions.load(recipe.ref)
    assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    assert len(attempts) == 2


def test_retention_still_rejects_caller_mutation_after_a_warm_load(retained):
    state, recipe, closure, calls = retained
    warm_recipe, warm_closure = state.recipe_definitions.load(recipe.ref)
    warm_recipe.branches["effect"].call.intent["changed"] = True
    with pytest.raises(ValueError):
        state.recipe_definitions.retain(warm_recipe, warm_closure)
    assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)


def test_large_valid_documents_bypass_cache_without_becoming_an_extent_limit(retained, monkeypatch):
    state, recipe, closure, calls = retained
    monkeypatch.setattr(recipe_definitions, "_VERIFIED_DOCUMENT_BYTES", 1)
    for _ in range(3):
        assert state.recipe_definitions.load(recipe.ref) == (recipe, closure)
    assert calls == [recipe.sha256] * 3


def test_parallel_callers_receive_private_nested_models(retained):
    state, recipe, closure, calls = retained
    store = state.recipe_definitions
    expected = deepcopy((recipe, closure))
    assert store.load(recipe.ref) == expected

    def read_and_mutate(_):
        result = store.load(recipe.ref)
        assert result == expected
        result[0].branches.clear()

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(read_and_mutate, range(16)))
    assert store.load(recipe.ref) == expected
    assert calls == [recipe.sha256]

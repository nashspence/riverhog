from __future__ import annotations

from copy import deepcopy

import pytest
from stove0_protocol import JSON_SCHEMA_ONLY_SEMANTIC_PROFILE, JsonSchemaValidationProfile
from stove0_recipe_config.bindings import apply_bindings
from stove0_recipe_config.compiled import (
    CompiledRecipe,
    CompiledRecipePayload,
    RecipeContract,
    RecipeContractPayload,
)
from stove0_recipe_config.compiler import (
    compile_recipe,
    compile_recipe_catalog,
    verify_compiled_recipe,
)
from stove0_recipe_config.dependencies import (
    OperationResource,
    RecipeDependencyCatalog,
    RecipeResource,
)
from stove0_recipe_config.source import RecipeSource, ValueBinding
from stove0_target_protocol import (
    InputArtifactContract,
    OperationContract,
    OperationContractPayload,
    OutputArtifactContract,
)


def _operation(*, retirement=True):
    return OperationContract.seal(
        OperationContractPayload(
            id="example.operation/v1",
            intent_schema=JsonSchemaValidationProfile.from_schema(
                "example.intent/v1",
                {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"quality": {"type": "integer"}},
                },
            ),
            intent_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
            inputs=(
                InputArtifactContract(
                    role="stove0.source/v1", allowed_dispositions=("transformed",)
                ),
            ),
            outputs=(
                OutputArtifactContract(
                    role="example.output/v1", derived_from_roles=("stove0.source/v1",)
                ),
            ),
            source_collection_retirement_permitted=retirement,
        )
    )


def _source(**changes):
    return RecipeSource.model_validate(
        {
            "format": "stove0-recipe/v1",
            "id": "example.recipe/v1",
            "revision": 1,
            "fork": {"archive": {"call": {"operation": "encode"}}},
            "export": {"branch": "archive"},
            **changes,
        }
    )


def _catalog(**extra):
    return RecipeDependencyCatalog(
        resources={"encode": OperationResource(contract=_operation()), **extra}
    )


def test_exact_offline_compilation_derives_public_collection_boundary() -> None:
    recipe, closure = compile_recipe(_source(), _catalog())
    assert recipe.contract.outcomes.normal.kind == "collection"
    assert [
        (item.role, item.minimum, item.maximum)
        for item in recipe.contract.outcomes.normal.artifacts
    ] == [
        ("example.output/v1", 1, None),
    ]
    assert recipe.contract.exposure.may_materialize_collections
    assert not recipe.contract.exposure.may_perform_external_effects
    assert recipe.contract.invocation.evaluation.usage == "unused"
    assert len(closure.operations) == 1
    verify_compiled_recipe(recipe, closure)


def test_resource_and_role_aliases_and_unused_catalog_entries_are_nonsemantic() -> None:
    original, _ = compile_recipe(_source(), _catalog())
    renamed = _source(
        roles={"input": "stove0.source/v1"},
        classify={"otherwise": "input"},
        fork={"archive": {"call": {"operation": "renamed"}}},
    )
    catalog = RecipeDependencyCatalog(
        resources={
            "renamed": OperationResource(contract=_operation()),
            "unused": OperationResource(contract=_operation(retirement=False)),
        }
    )
    actual, closure = compile_recipe(renamed, catalog)
    assert actual.sha256 == original.sha256
    assert len(closure.operations) == 1


def test_recipe_name_revision_and_internal_implementation_do_not_enter_boundary_signature() -> None:
    original, _ = compile_recipe(_source(), _catalog())
    changed, _ = compile_recipe(
        _source(
            id="another.recipe/v1",
            revision=2,
            fork={"archive": {"call": {"operation": "encode", "options": {"preset": "p4"}}}},
        ),
        _catalog(),
    )
    assert changed.sha256 != original.sha256
    assert changed.contract.contract_sha256 == original.contract.contract_sha256


def test_source_and_schema_annotations_are_not_semantic_but_literal_data_is() -> None:
    parameters = {
        "type": "object",
        "properties": {"data": {"const": {"description": "literal"}}},
        "additionalProperties": False,
    }
    original, _ = compile_recipe(_source(parameters=parameters), _catalog())
    annotated = deepcopy(parameters)
    annotated["description"] = "documentation"
    annotated["properties"]["data"]["default"] = {"description": "default annotation"}
    actual, _ = compile_recipe(
        _source(parameters=annotated, description="author prose"), _catalog()
    )
    assert actual.sha256 == original.sha256
    assert actual.parameters_schema.document["properties"]["data"]["const"] == {
        "description": "literal"
    }


def test_loader_recomputes_signature_instead_of_trusting_its_self_hash() -> None:
    recipe, closure = compile_recipe(_source(), _catalog())
    false_payload = RecipeContractPayload.model_validate(
        recipe.contract.model_dump(exclude={"contract_sha256"})
    )
    false_payload = false_payload.model_copy(
        update={
            "exposure": false_payload.exposure.model_copy(
                update={"may_materialize_collections": False},
            )
        }
    )
    false_contract = RecipeContract.seal(false_payload)
    payload = CompiledRecipePayload.model_validate(recipe.model_dump(exclude={"sha256"}))
    forged = CompiledRecipe.seal(payload.model_copy(update={"contract": false_contract}))
    with pytest.raises(ValueError, match="RecipeContract differs"):
        verify_compiled_recipe(forged, closure)


def test_same_child_contract_never_substitutes_for_full_child_program_pin() -> None:
    first, _ = compile_recipe(_source(), _catalog())
    second, _ = compile_recipe(
        _source(fork={"archive": {"call": {"operation": "encode", "intent": {"quality": 23}}}}),
        _catalog(),
    )
    assert first.contract == second.contract
    parent = _source(id="example.parent/v1", fork={"archive": {"call": {"recipe": "child"}}})
    one, _ = compile_recipe(parent, _catalog(child=RecipeResource(recipe=first)))
    two, _ = compile_recipe(parent, _catalog(child=RecipeResource(recipe=second)))
    assert one.sha256 != two.sha256
    assert one.contract == two.contract


def test_bottom_up_local_recipe_compilation_rejects_cycles_before_execution() -> None:
    sources = {
        "parent": _source(id="parent/v1", fork={"archive": {"call": {"recipe": "child"}}}),
        "child": _source(id="child/v1"),
    }
    compiled, _ = compile_recipe_catalog(sources, _catalog())
    assert compiled["parent"].branches["archive"].call.recipe == compiled["child"].ref
    sources["child"] = _source(id="child/v1", fork={"archive": {"call": {"recipe": "parent"}}})
    with pytest.raises(ValueError, match="cycle.*child.*parent"):
        compile_recipe_catalog(sources, _catalog())


def test_decision_only_success_has_no_collection_export_and_only_own_no_output_code() -> None:
    source = _source(
        fork={},
        export=None,
        decisions=[
            {
                "when": True,
                "no_output": {
                    "code": "example.nothing-required/v1",
                    "message": "No operation is required.",
                },
            }
        ],
    )
    recipe, closure = compile_recipe(source, RecipeDependencyCatalog())
    assert recipe.contract.outcomes.normal is None
    assert recipe.contract.outcomes.no_output_codes == ("example.nothing-required/v1",)
    assert recipe.contract.exposure.may_materialize_collections is False
    assert not closure.operations


def test_normal_multi_output_completion_does_not_infer_an_export() -> None:
    recipe, _ = compile_recipe(
        _source(
            export=None,
            fork={
                "one": {"call": {"operation": "encode"}},
                "two": {"call": {"operation": "encode"}},
            },
        ),
        _catalog(),
    )
    assert recipe.contract.outcomes.normal.kind == "completion"


def test_invalid_literal_and_unsupported_original_source_retirement_fail_compilation() -> None:
    with pytest.raises(ValueError, match="literal call arguments"):
        compile_recipe(
            _source(
                fork={"archive": {"call": {"operation": "encode", "intent": {"quality": True}}}}
            ),
            _catalog(),
        )
    with pytest.raises(ValueError, match="retirement"):
        compile_recipe(
            _source(source={"retirement": {"mode": "after-settlement"}}),
            RecipeDependencyCatalog(
                resources={"encode": OperationResource(contract=_operation(retirement=False))}
            ),
        )


def test_explicit_bindings_preserve_null_and_only_merge_at_the_named_object() -> None:
    bindings = tuple(
        ValueBinding.model_validate(doc)
        for doc in [
            {
                "from": "parameters",
                "path": "/value",
                "to": "intent",
                "at": "/value",
                "mode": "insert",
            },
            {
                "from": "evaluation",
                "path": "/options",
                "to": "options",
                "at": "",
                "mode": "merge-object",
            },
        ]
    )
    intent, options = apply_bindings(
        intent={},
        options={"nested": {"old": True}, "keep": True},
        bindings=bindings,
        parameters={"value": None},
        evaluation={"options": {"nested": {"new": True}}},
    )
    assert intent == {"value": None}
    assert options == {"nested": {"new": True}, "keep": True}
    with pytest.raises(ValueError, match="source is missing"):
        apply_bindings(intent={}, options={}, bindings=bindings, parameters={}, evaluation=None)

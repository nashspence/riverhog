"""Bound calls preserve composed JSON Schema constraints until their exact invocation."""

from copy import deepcopy

import pytest
from jsonschema import Draft202012Validator, ValidationError
from stove0_protocol import JsonSchemaValidationProfile
from stove0_recipe_config.bindings import apply_bindings
from stove0_recipe_config.compiler import compile_recipe
from stove0_recipe_config.dependencies import OperationResource, RecipeDependencyCatalog
from stove0_target_protocol import OperationContract, OperationContractPayload
from test_recipe_compiler import _operation, _source


def _compile(schema, *, literal=None, bindings=None):
    document = _operation().model_dump(mode="json", exclude={"contract_sha256"})
    document["intent_schema"] = JsonSchemaValidationProfile.from_schema(
        "example.bound-intent/v1", schema
    ).model_dump(mode="json")
    operation = OperationContract.seal(OperationContractPayload.model_validate(document))
    source = _source(
        parameters={"type": "object"},
        fork={
            "archive": {
                "call": {
                    "operation": "encode",
                    "intent": literal or {},
                    "bind": bindings
                    or [
                        {
                            "from": "parameters",
                            "path": "/x",
                            "to": "intent",
                            "at": "/x",
                            "mode": "insert",
                        }
                    ],
                }
            }
        },
    )
    recipe, _ = compile_recipe(
        source, RecipeDependencyCatalog(resources={"encode": OperationResource(contract=operation)})
    )
    return recipe.branches["archive"].call


@pytest.mark.parametrize("keyword", ["anyOf", "oneOf"])
def test_composed_required_alternatives_accept_valid_inserted_parameter(keyword):
    schema = {
        "type": "object",
        "properties": {"x": {"type": "integer"}, "y": {"type": "integer"}},
        keyword: [{"required": ["x"]}, {"required": ["y"]}],
    }
    call = _compile(schema)
    intent, _ = apply_bindings(
        intent=call.intent,
        options=call.options,
        bindings=call.bind,
        parameters={"x": 1},
        evaluation=None,
    )
    Draft202012Validator(schema).validate(intent)
    assert intent == {"x": 1}
    bad, _ = apply_bindings(
        intent=call.intent,
        options=call.options,
        bindings=call.bind,
        parameters={"x": "wrong"},
        evaluation=None,
    )
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(bad)


def test_bindings_can_change_conditional_applicability_without_rejecting_a_literal():
    schema = {
        "type": "object",
        "if": {"required": ["mode"], "properties": {"mode": {"const": "x"}}},
        "then": {"required": ["x"], "properties": {"x": {"type": "integer"}}},
        "else": {"properties": {"y": {"const": 2}}},
    }
    bindings = [
        {"from": "parameters", "path": "/" + key, "to": "intent", "at": "/" + key, "mode": "insert"}
        for key in ("mode", "x")
    ]
    call = _compile(schema, literal={"y": 1}, bindings=bindings)
    intent, _ = apply_bindings(
        intent=call.intent,
        options=call.options,
        bindings=call.bind,
        parameters={"mode": "x", "x": 1},
        evaluation=None,
    )
    Draft202012Validator(schema).validate(intent)
    bad = deepcopy(intent)
    bad["mode"] = "y"
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(bad)


@pytest.mark.parametrize(
    "mode,at,path,literal",
    [
        ("insert", "/config/x", "/x", {"config": {}}),
        ("merge-object", "/config", "/config", {"config": {}}),
        ("merge-object", "", "", {}),
    ],
)
def test_nested_and_object_merge_bindings_defer_the_affected_aggregate(mode, at, path, literal):
    schema = {
        "type": "object",
        "required": ["config"],
        "properties": {
            "config": {
                "type": "object",
                "anyOf": [{"required": ["x"]}, {"required": ["y"]}],
                "properties": {"x": {"type": "integer"}},
            }
        },
    }
    call = _compile(
        schema,
        literal=literal,
        bindings=[{"from": "parameters", "path": path, "to": "intent", "at": at, "mode": mode}],
    )
    intent, _ = apply_bindings(
        intent=call.intent,
        options=call.options,
        bindings=call.bind,
        parameters={"x": 1, "config": {"x": 1}},
        evaluation=None,
    )
    Draft202012Validator(schema).validate(intent)


def test_unaffected_literal_contradictions_are_rejected_at_compilation():
    schema = {
        "type": "object",
        "properties": {"x": {"type": "integer"}, "fixed": {"type": "integer"}},
        "anyOf": [{"required": ["x"]}, {"required": ["y"]}],
    }
    with pytest.raises(ValueError, match="literal call arguments contradict"):
        _compile(schema, literal={"fixed": "wrong"})


def test_join_rejects_original_input_evidence_before_it_can_be_silently_dropped():
    with pytest.raises(ValueError, match="at most 0"):
        _source(
            fork={"a": {"call": {"operation": "encode"}}, "b": {"call": {"operation": "encode"}}},
            join={
                "members": {"a": ["example.output/v1"], "b": ["example.output/v1"]},
                "call": {"operation": "encode", "evidence": ["probe"]},
            },
            export="join",
        )

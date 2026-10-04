"""#953 reference only: derive unsealed RecipeContract boundary payloads.

Inputs are a normalized BOUNDARY SLICE and mock/previously verified dependency
summaries, NOT RecipeSource or a complete CompiledRecipe. This module neither
resolves/authenticates dependencies nor executes recipes or seals JCS digests.
Production must derive the slice from its single verified semantic authority.
"""
from __future__ import annotations

import argparse
import copy
import json
import re
from typing import Any, Mapping

from jsonschema import Draft202012Validator

FORMAT = "stove0-recipe-contract/v1"
PROFILE = "stove0-recipe-contract-projection/v1"
POINTER = r"^(?:/(?:[^~/]|~[01])*)*$"
SEMANTIC_ID = r"^[a-z0-9](?:[a-z0-9._/-]{0,158}[a-z0-9])?$"
NAME = r"^[a-z][a-z0-9_-]*$"
FLAGS = ("may_materialize_collections", "may_perform_external_effects", "may_request_retrieval")


def obj(properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": properties,
            "required": list(properties), "additionalProperties": False}


def array(items: dict[str, Any], minimum: int = 0) -> dict[str, Any]:
    return {"type": "array", "items": items, "minItems": minimum}


def canonical_strings(values: list[str]) -> list[str]:
    return sorted(set(values), key=lambda value: value.encode("utf-8"))


def retirement_schema() -> dict[str, Any]:
    return {"oneOf": [obj({"mode": {"const": "retain"}}), obj({
        "mode": {"const": "after-settlement"},
        "grace_seconds": {"type": "string", "pattern": r"^(0|[1-9][0-9]*)$"}})]}


def payload_schema() -> dict[str, Any]:
    """Closed UNSEALED payload schema; semantic checks supplement JSON Schema."""
    role = obj({"role": {"type": "string", "pattern": SEMANTIC_ID},
        "minimum": {"type": "string", "pattern": r"^(0|[1-9][0-9]*)$"},
        "maximum": {"oneOf": [{"type": "null"},
            {"type": "string", "pattern": r"^[1-9][0-9]*$"}]}})
    result = obj({"format": {"const": FORMAT}, "projection_profile": {"const": PROFILE},
        "invocation": obj({"collections": obj({"minimum": {"const": "1"},
            "maximum": {"type": "null"}, "state": {"const": "finalized"},
            "root_scope": {"const": "complete-root-inventories"},
            "child_scope": {"const": "parent-bound-exact-selection"}}),
            "parameters_schema": {"type": "object"},
            "evaluation": obj({"usage": {"enum": ["unused", "path-dependent"]},
                "possible_reads": array({"type": "string", "pattern": POINTER})})}),
        "outcomes": obj({"normal": {"oneOf": [{"type": "null"},
            obj({"kind": {"const": "completion"}}),
            obj({"kind": {"const": "collection"}, "artifacts": array(role, 1)})]},
            "no_output_codes": array({"type": "string", "pattern": SEMANTIC_ID})}),
        "exposure": obj({key: {"type": "boolean"} for key in FLAGS}),
        "source": obj({"unmatched": {"enum": ["retain-in-source", "reject-work"]},
            "root_retirement": retirement_schema(),
            "parent_bound_retirement": {"const": "retain"}})})
    return {"$schema": "https://json-schema.org/draft/2020-12/schema", **result}


def boundary_slice_schema() -> dict[str, Any]:
    """Test adapter format only: not a proposed second compiled representation."""
    def named(value: dict[str, Any]) -> dict[str, Any]:
        return {"type": "object", "propertyNames": {"pattern": NAME}, "additionalProperties": value}
    retrieve = {"enum": ["available-only", "allow"]}
    call = obj({"kind": {"enum": ["operation", "recipe"]},
        "resource": {"type": "string", "pattern": NAME},
        "evaluation_reads": array({"type": "string", "pattern": POINTER}),
        "retrieve": retrieve})
    return obj({"parameters_schema": {"type": "object"},
        "observations": named(obj({"retrieve": retrieve})), "branches": named(call),
        "decisions": array({"type": "string", "pattern": SEMANTIC_ID}),
        "join": {"oneOf": [{"type": "null"}, obj({"call": call,
            "members": {**named(array({"type": "string", "pattern": SEMANTIC_ID}, 1)), "minProperties": 2}})]},
        "export": {"oneOf": [{"type": "null"}, {"const": "join"},
            obj({"branch": {"type": "string", "pattern": NAME}})]},
        "source": obj({"unmatched": {"enum": ["retain-in-source", "reject-work"]},
            "retirement": retirement_schema()})})


def exact_count(value: Any, *, positive: bool = False) -> str:
    # An adapter may receive owning operation-model Python ints or exact wire strings.
    if type(value) is int:
        text = str(value)
    elif type(value) is str:
        text = value
    else:
        raise ValueError("cardinality must be an exact integer, not a Boolean or float")
    if not re.fullmatch(r"0|[1-9][0-9]*", text) or (positive and text == "0"):
        raise ValueError("invalid exact cardinality")
    return text


def validate_payload(payload: Mapping[str, Any]) -> None:
    Draft202012Validator(payload_schema()).validate(payload)
    evaluation = payload["invocation"]["evaluation"]
    reads = evaluation["possible_reads"]
    if reads != canonical_strings(reads):
        raise ValueError("evaluation reads must be unique and canonical")
    if evaluation["usage"] != ("path-dependent" if reads else "unused"):
        raise ValueError("evaluation summary contradicts its possible reads")
    codes = payload["outcomes"]["no_output_codes"]
    if codes != canonical_strings(codes):
        raise ValueError("no-output codes must be unique and canonical")
    normal = payload["outcomes"]["normal"]
    if normal is None and not codes:
        raise ValueError("recipe has no declared successful outcome")
    if normal is not None and normal["kind"] == "collection":
        roles = [item["role"] for item in normal["artifacts"]]
        if roles != canonical_strings(roles):
            raise ValueError("export roles must be unique and canonical")
        for item in normal["artifacts"]:
            if item["maximum"] is not None and int(item["minimum"]) > int(item["maximum"]):
                raise ValueError("output minimum exceeds maximum")
        if not payload["exposure"]["may_materialize_collections"]:
            raise ValueError("exported collection contradicts collection exposure")
    # This only checks schema syntax. Local closure, semantics and exact identities
    # are deliberately responsibilities of the production compiler/resolver.
    Draft202012Validator.check_schema(payload["invocation"]["parameters_schema"])


def derive_contract(body: Mapping[str, Any], operations: Mapping[str, Any],
                    children: Mapping[str, Any]) -> dict[str, Any]:
    """Project normalized boundary inputs; dependencies MUST be verified upstream.

    Tests pass mock summaries. A self-consistent child payload is not evidence of
    authenticity or proof that its exact compiled body derives that payload.
    """
    Draft202012Validator(boundary_slice_schema()).validate(body)
    exposure = {key: False for key in FLAGS}
    reads: list[str] = []
    for task in body["observations"].values():
        exposure["may_request_retrieval"] |= task["retrieve"] == "allow"

    def call_result(call: Mapping[str, Any]) -> dict[str, Any] | None:
        reads.extend(call["evaluation_reads"])
        if call["kind"] == "recipe":
            if call["retrieve"] != "available-only":
                raise ValueError("recipe calls do not override descendant retrieval policy")
            child = children[call["resource"]]
            validate_payload(child)
            reads.extend(child["invocation"]["evaluation"]["possible_reads"])
            for key in FLAGS:
                exposure[key] |= child["exposure"][key]
            return copy.deepcopy(child["outcomes"]["normal"])
        operation = operations[call["resource"]]
        kind = operation["result_kind"]
        exposure["may_request_retrieval"] |= call["retrieve"] == "allow"
        if kind == "external-effect":
            if operation["outputs"]:
                raise ValueError("external-effect operation cannot promise collection outputs")
            exposure["may_perform_external_effects"] = True
            return None
        if kind != "collection":
            raise ValueError("unmodeled operation result kind")
        exposure["may_materialize_collections"] = True
        artifacts = []
        for item in operation["outputs"]:
            artifacts.append({"role": item["role"], "minimum": exact_count(item["minimum"]),
                "maximum": None if item["maximum"] is None else exact_count(item["maximum"], positive=True)})
        artifacts.sort(key=lambda item: item["role"].encode("utf-8"))
        if not artifacts:
            raise ValueError("collection operation requires output roles")
        roles = [item["role"] for item in artifacts]
        if roles != canonical_strings(roles) or any(not re.fullmatch(SEMANTIC_ID, role) for role in roles):
            raise ValueError("invalid or duplicate output role")
        if any(item["maximum"] is not None and int(item["minimum"]) > int(item["maximum"]) for item in artifacts):
            raise ValueError("output minimum exceeds maximum")
        # Internal derived_from_roles do not cross the public recipe boundary.
        return {"kind": "collection", "artifacts": artifacts}

    results = {name: call_result(call) for name, call in body["branches"].items()}
    join_result = None
    if body["join"] is not None:
        join = body["join"]
        if join["call"]["kind"] != "operation":
            raise ValueError("materializing join must call an operation")
        for name, required_roles in join["members"].items():
            normal = results.get(name)
            if not normal or normal["kind"] != "collection":
                raise ValueError("join member is not collection-capable")
            if len(required_roles) != len(set(required_roles)):
                raise ValueError("join roles must be unique")
            if not set(required_roles) <= {item["role"] for item in normal["artifacts"]}:
                raise ValueError("join requests an undeclared export role")
            # Zero minimum is allowed here: actual presence/count is a plan obligation.
        join_result = call_result(join["call"])
        if not join_result or join_result["kind"] != "collection":
            raise ValueError("join must produce a collection")
    normal = {"kind": "completion"} if results else None
    export = body["export"]
    if export is not None:
        normal = join_result if export == "join" else results.get(export["branch"])
        if not normal or normal["kind"] != "collection":
            raise ValueError("export must designate one collection-capable producer")
    reads = canonical_strings(reads)
    payload = {"format": FORMAT, "projection_profile": PROFILE,
        "invocation": {"collections": {"minimum": "1", "maximum": None, "state": "finalized",
            "root_scope": "complete-root-inventories", "child_scope": "parent-bound-exact-selection"},
            "parameters_schema": copy.deepcopy(body["parameters_schema"]),
            "evaluation": {"usage": "path-dependent" if reads else "unused", "possible_reads": reads}},
        "outcomes": {"normal": normal, "no_output_codes": canonical_strings(body["decisions"])},
        "exposure": exposure,
        "source": {"unmatched": body["source"]["unmatched"],
            "root_retirement": copy.deepcopy(body["source"]["retirement"]),
            "parent_bound_retirement": "retain"}}
    validate_payload(payload)
    return payload


def typed_equal(left: Any, right: Any) -> bool:
    """Structural comparison for this prototype, NOT a replacement JCS codec."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(typed_equal(left[k], right[k]) for k in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(typed_equal(a, b) for a, b in zip(left, right))
    return left == right


def verify_projection(body: Mapping[str, Any], operations: Mapping[str, Any],
                      children: Mapping[str, Any], candidate: dict[str, Any]) -> None:
    validate_payload(candidate)
    if not typed_equal(derive_contract(body, operations, children), candidate):
        raise ValueError("supplied RecipeContract differs from derived boundary")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["schema"])
    parser.parse_args()
    print(json.dumps(payload_schema(), indent=2))

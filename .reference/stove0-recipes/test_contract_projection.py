"""#953 structural projection tests; mock dependencies, no runtime/hash proof.

fixture_slice is only a test adapter. It intentionally discards predicates and
other internals because this conservative profile summarizes declared paths.
Production must derive from its one validated, resolved, normalized compiler IR.
"""
from __future__ import annotations

import copy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

from contract_projection import (
    boundary_slice_schema, derive_contract, exact_count, payload_schema,
    validate_payload, verify_projection,
)
from language import diagnostics, fixture_resources, read_documents

EXAMPLES = read_documents(Path(__file__).with_name("examples.yaml"))
EMPTY_PARAMETERS = {"type": "object", "properties": {}, "additionalProperties": False}


def fixture_slice(source):
    """Not a compiler: create a normalized projection-only slice for these tests."""
    def call(item):
        kind = "operation" if "operation" in item else "recipe"
        return {"kind": kind, "resource": item[kind],
            "evaluation_reads": [b["path"] for b in item.get("bind", []) if b["from"] == "evaluation"],
            "retrieve": item.get("retrieve", "available-only")}
    policy = copy.deepcopy(source.get("source", {}).get("retirement", {"mode": "retain"}))
    if policy["mode"] == "after-settlement":
        policy["grace_seconds"] = exact_count(policy.get("grace_seconds", 0))
    join = source.get("join")
    return {"parameters_schema": copy.deepcopy(source.get("parameters", EMPTY_PARAMETERS)),
        "observations": {name: {"retrieve": task.get("retrieve", "available-only")}
            for name, task in source.get("observe", {}).items()},
        "branches": {name: call(branch["call"]) for name, branch in source.get("fork", {}).items()},
        "decisions": [item["no_output"]["code"] for item in source.get("decisions", [])],
        "join": None if join is None else {"members": copy.deepcopy(join["members"]), "call": call(join["call"])},
        "export": copy.deepcopy(source.get("export")),
        "source": {"unmatched": source.get("source", {}).get("unmatched", "retain-in-source"), "retirement": policy}}


def mock_dependencies():
    artifact = {"role": "example.archive/v1", "minimum": 1, "maximum": None,
        "derived_from_roles": ["example.internal-input/v1"]}
    operations = {name: {"result_kind": "collection", "outputs": [copy.deepcopy(artifact)]}
        for name in ("encode", "audio", "assemble", "review")}
    operations["deliver"] = {"result_kind": "external-effect", "outputs": []}
    child_body = fixture_slice(EXAMPLES[0])
    child_body["parameters_schema"] = {"type": "object", "properties": {"quality": {"type": "integer"}},
        "additionalProperties": False}
    decision_body = fixture_slice(EXAMPLES[5])
    children = {"child": derive_contract(child_body, operations, {}),
        "no_output_child": derive_contract(decision_body, operations, {})}
    return operations, children


def project(source=None):
    operations, children = mock_dependencies()
    return derive_contract(fixture_slice(EXAMPLES[0] if source is None else source), operations, children)


def test_schemas_are_well_formed():
    Draft202012Validator.check_schema(payload_schema())
    Draft202012Validator.check_schema(boundary_slice_schema())


@pytest.mark.parametrize("source", EXAMPLES, ids=lambda item: item["id"])
def test_all_ten_existing_examples_have_a_payload(source):
    validate_payload(project(source))


def test_simple_export_boundary_exactly():
    result = project()
    assert result["invocation"] == {
        "collections": {"minimum": "1", "maximum": None, "state": "finalized",
            "root_scope": "complete-root-inventories", "child_scope": "parent-bound-exact-selection"},
        "parameters_schema": EMPTY_PARAMETERS,
        "evaluation": {"usage": "unused", "possible_reads": []}}
    assert result["outcomes"] == {"normal": {"kind": "collection", "artifacts": [
        {"role": "example.archive/v1", "minimum": "1", "maximum": None}]}, "no_output_codes": []}
    assert result["source"] == {"unmatched": "retain-in-source", "root_retirement": {"mode": "retain"},
        "parent_bound_retirement": "retain"}


def test_no_export_is_completion_not_no_output_or_pure():
    result = project(EXAMPLES[1])
    assert result["outcomes"] == {"normal": {"kind": "completion"}, "no_output_codes": []}
    assert result["exposure"]["may_materialize_collections"] is True
    assert result["exposure"]["may_request_retrieval"] is True


def test_decision_only_does_not_invent_normal_completion_or_discard_permission():
    result = project(EXAMPLES[5])
    assert result["outcomes"] == {"normal": None, "no_output_codes": ["example.approved-source-loss/v1"]}
    assert result["exposure"]["may_materialize_collections"] is False
    assert "retirement_permitted" not in result["source"]
    assert "source_disposition" not in result["outcomes"]


def test_local_no_output_alternative_does_not_make_normal_collection_untyped():
    result = project(EXAMPLES[9])
    assert result["outcomes"]["normal"]["kind"] == "collection"
    assert result["outcomes"]["no_output_codes"] == ["example.already-satisfied/v1"]


def test_child_no_output_code_does_not_bubble_to_parent():
    result = project(EXAMPLES[6])
    assert result["outcomes"]["normal"]["kind"] == "collection"
    assert result["outcomes"]["no_output_codes"] == []


def test_review_context_reads_are_possible_not_unconditional_entry_requirements():
    result = project(EXAMPLES[8])
    assert result["invocation"]["evaluation"] == {"usage": "path-dependent", "possible_reads": [
        "/parameters/review_variant/portable_intent", "/parameters/review_variant/target_options", "/variant_id"]}


def test_conditional_read_remains_path_dependent_even_with_no_output_bypass():
    source = copy.deepcopy(EXAMPLES[9])
    source["fork"]["archive"]["when"] = False
    source["fork"]["archive"]["call"]["bind"] = [
        {"from": "evaluation", "path": "/variant_id", "to": "intent", "at": "/variant", "mode": "insert"}]
    assert project(source)["invocation"]["evaluation"]["usage"] == "path-dependent"


def test_nonexported_effect_branch_is_not_hidden():
    result = project(EXAMPLES[3])
    assert result["outcomes"]["normal"]["kind"] == "collection"
    assert result["exposure"]["may_perform_external_effects"] is True


def test_effect_only_recipe_returns_completion():
    result = project(EXAMPLES[7])
    assert result["outcomes"]["normal"] == {"kind": "completion"}
    assert result["exposure"] == {"may_materialize_collections": False,
        "may_perform_external_effects": True, "may_request_retrieval": False}


def test_nested_read_and_exposure_union_excludes_child_root_retirement():
    operations, children = mock_dependencies()
    child = fixture_slice(EXAMPLES[8])
    child["source"]["retirement"] = {"mode": "after-settlement", "grace_seconds": "50"}
    child["branches"]["mirror"] = {"kind": "operation", "resource": "deliver",
        "evaluation_reads": ["/other", "/variant_id"], "retrieve": "allow"}
    children["child"] = derive_contract(child, operations, {})
    parent = fixture_slice(EXAMPLES[4])
    result = derive_contract(parent, operations, children)
    assert result["exposure"] == dict.fromkeys(result["exposure"], True)
    assert "/other" in result["invocation"]["evaluation"]["possible_reads"]
    assert result["source"]["root_retirement"] == {"mode": "retain"}
    assert result["source"]["parent_bound_retirement"] == "retain"


@pytest.mark.parametrize("field,value", [("minimum", 0), ("maximum", 3), ("role", "example.other/v1")])
def test_public_output_change_changes_boundary(field, value):
    operations, children = mock_dependencies()
    original = project()
    operations["encode"]["outputs"][0][field] = value
    assert derive_contract(fixture_slice(EXAMPLES[0]), operations, children) != original


def test_zero_minimum_unbounded_maximum_and_internal_lineage_are_preserved_correctly():
    operations, children = mock_dependencies()
    operations["encode"]["outputs"][0]["minimum"] = 0
    result = derive_contract(fixture_slice(EXAMPLES[0]), operations, children)
    assert result["outcomes"]["normal"]["artifacts"] == [
        {"role": "example.archive/v1", "minimum": "0", "maximum": None}]


def test_join_projects_its_own_output_not_union_or_group_multiplier():
    operations, children = mock_dependencies()
    operations["assemble"]["outputs"] = [{"role": "example.assembled/v1", "minimum": 1, "maximum": 1}]
    result = derive_contract(fixture_slice(EXAMPLES[3]), operations, children)
    assert result["outcomes"]["normal"]["artifacts"] == [
        {"role": "example.assembled/v1", "minimum": "1", "maximum": "1"}]


def test_optional_join_role_remains_dynamic_presence_obligation():
    operations, children = mock_dependencies()
    operations["encode"]["outputs"][0]["minimum"] = 0
    derive_contract(fixture_slice(EXAMPLES[3]), operations, children)


@pytest.mark.parametrize("kind", ["effect", "decision-only", "completion"])
def test_noncollection_children_cannot_satisfy_export(kind):
    operations, children = mock_dependencies()
    body = fixture_slice(EXAMPLES[0])
    if kind == "effect":
        body["branches"]["archive"]["resource"] = "deliver"
    else:
        body["branches"]["archive"].update(kind="recipe", resource="no_output_child" if kind == "decision-only" else "child")
        if kind == "completion":
            child_body = fixture_slice(EXAMPLES[0]); child_body["export"] = None
            children["child"] = derive_contract(child_body, operations, {})
    with pytest.raises(ValueError, match="collection-capable"):
        derive_contract(body, operations, children)


def test_collection_child_with_no_output_is_capable_but_plan_still_must_select_collection():
    operations, children = mock_dependencies()
    children["child"] = derive_contract(fixture_slice(EXAMPLES[9]), operations, {})
    result = derive_contract(fixture_slice(EXAMPLES[4]), operations, children)
    assert result["outcomes"]["normal"]["kind"] == "collection"
    assert result["outcomes"]["no_output_codes"] == []
    # This is only capability checking, not discharge of the runtime obligation.


@pytest.mark.parametrize("change", ["id", "revision", "description", "branch", "intent", "unused-resource", "map-order"])
def test_nonboundary_changes_preserve_payload(change):
    source = copy.deepcopy(EXAMPLES[0]); operations, children = mock_dependencies()
    if change == "id": source["id"] = "example.renamed/v1"
    elif change == "revision": source["revision"] = 77
    elif change == "description": source["description"] = "New documentation"
    elif change == "branch":
        source["fork"]["renamed"] = source["fork"].pop("archive"); source["export"] = {"branch": "renamed"}
    elif change == "intent": source["fork"]["archive"]["call"]["intent"]["quality"] = 42
    elif change == "unused-resource": operations["unused"] = {"result_kind": "unmodeled"}
    else: source = dict(reversed(list(source.items())))
    assert derive_contract(fixture_slice(source), operations, children) == project()


def test_same_boundary_from_distinct_operation_resources_is_not_substitution_authority():
    operations, children = mock_dependencies()
    body = fixture_slice(EXAMPLES[0]); operations["another"] = copy.deepcopy(operations["encode"])
    body["branches"]["archive"]["resource"] = "another"
    assert derive_contract(body, operations, children) == project()
    # Full compiled dependency identity MUST change; no full compiler/hash is tested here.


@pytest.mark.parametrize("change", ["parameters", "evaluation", "retirement", "unmatched", "no-output"])
def test_public_boundary_changes_are_visible(change):
    body = fixture_slice(EXAMPLES[0]); operations, children = mock_dependencies()
    if change == "parameters": body["parameters_schema"]["properties"]["quality"] = {"type": "integer"}
    elif change == "evaluation": body["branches"]["archive"]["evaluation_reads"] = ["/variant_id"]
    elif change == "retirement": body["source"]["retirement"] = {"mode": "after-settlement", "grace_seconds": "60"}
    elif change == "unmatched": body["source"]["unmatched"] = "reject-work"
    else: body["decisions"] = ["example.skip/v1"]
    assert derive_contract(body, operations, children) != project()


@pytest.mark.parametrize("value", [True, 1.0, -1, "01", "1.0", None])
def test_invalid_output_cardinality_is_rejected(value):
    operations, children = mock_dependencies()
    operations["encode"]["outputs"][0]["minimum"] = value
    with pytest.raises(ValueError): derive_contract(fixture_slice(EXAMPLES[0]), operations, children)


def test_large_exact_count_and_wire_string_normalize_equally():
    operations, children = mock_dependencies(); body = fixture_slice(EXAMPLES[0])
    operations["encode"]["outputs"][0]["minimum"] = 10 ** 30
    first = derive_contract(body, operations, children)
    operations["encode"]["outputs"][0]["minimum"] = str(10 ** 30)
    assert derive_contract(body, operations, children) == first


@pytest.mark.parametrize("case", ["empty", "duplicate", "reversed-range", "unknown-kind"])
def test_bad_operation_summary_fails_even_without_export(case):
    operations, children = mock_dependencies(); body = fixture_slice(EXAMPLES[0]); body["export"] = None
    if case == "empty": operations["encode"]["outputs"] = []
    elif case == "duplicate": operations["encode"]["outputs"] *= 2
    elif case == "reversed-range": operations["encode"]["outputs"][0].update(minimum=3, maximum=2)
    else: operations["encode"]["result_kind"] = "unknown"
    with pytest.raises(ValueError): derive_contract(body, operations, children)


def test_join_requires_declared_roles():
    body = fixture_slice(EXAMPLES[3]); operations, children = mock_dependencies()
    body["join"]["members"]["low"] = ["example.missing/v1"]
    with pytest.raises(ValueError, match="undeclared"): derive_contract(body, operations, children)


def test_reads_and_codes_are_canonical_without_dropping_root_or_overlapping_reads():
    body = fixture_slice(EXAMPLES[0]); operations, children = mock_dependencies()
    body["branches"]["archive"]["evaluation_reads"] = ["/a/b", "", "/a", "/a~1b", "/a"]
    body["decisions"] = ["example.z/v1", "example.a/v1", "example.z/v1"]
    result = derive_contract(body, operations, children)
    assert result["invocation"]["evaluation"]["possible_reads"] == ["", "/a", "/a/b", "/a~1b"]
    assert result["outcomes"]["no_output_codes"] == ["example.a/v1", "example.z/v1"]


@pytest.mark.parametrize("field", ["id", "revision", "recipe", "compiler_build", "contract_sha256", "target"])
def test_payload_rejects_backreferences_metadata_and_unsealed_hash_field(field):
    candidate = project(); candidate[field] = "unexpected"
    with pytest.raises(ValidationError): validate_payload(candidate)


@pytest.mark.parametrize("field", ["contract", "interface", "exports_collection"])
def test_derived_boundary_cannot_be_authored(field):
    source = copy.deepcopy(EXAMPLES[0]); source[field] = {}
    assert diagnostics(source, fixture_resources())


def test_supplied_well_formed_but_false_summary_is_rejected_by_rederivation():
    body = fixture_slice(EXAMPLES[7]); operations, children = mock_dependencies()
    candidate = derive_contract(body, operations, children)
    candidate["exposure"]["may_perform_external_effects"] = False
    validate_payload(candidate)  # Schema validity/self-consistency does not prove truth.
    with pytest.raises(ValueError, match="differs from derived"): verify_projection(body, operations, children, candidate)


def test_boolean_and_numeric_parameter_constraints_are_not_equal():
    body = fixture_slice(EXAMPLES[0]); operations, children = mock_dependencies()
    body["parameters_schema"]["properties"]["flag"] = {"const": True}
    candidate = derive_contract(body, operations, children)
    candidate["invocation"]["parameters_schema"]["properties"]["flag"]["const"] = 1
    with pytest.raises(ValueError, match="differs from derived"): verify_projection(body, operations, children, candidate)


def test_projection_does_not_mutate_input_or_dependency_documents():
    body = fixture_slice(EXAMPLES[4]); operations, children = mock_dependencies()
    before = copy.deepcopy((body, operations, children))
    derive_contract(body, operations, children)
    assert (body, operations, children) == before


def test_empty_recipe_is_not_implicit_success():
    body = fixture_slice(EXAMPLES[0]); operations, children = mock_dependencies()
    body["branches"] = {}; body["export"] = None
    with pytest.raises(ValueError, match="no declared successful outcome"): derive_contract(body, operations, children)

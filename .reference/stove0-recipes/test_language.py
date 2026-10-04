"""Static authoring tests only. Runtime/evidence vectors are in CONFORMANCE.md."""
from __future__ import annotations

import copy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from language import diagnostics, fixture_resources, read_documents, schema

EXAMPLES = read_documents(Path(__file__).with_name("examples.yaml"))
RESOURCES = fixture_resources()


def simple():
    return copy.deepcopy(EXAMPLES[0])


def check(document):
    return diagnostics(document, RESOURCES)


def test_schema_is_well_formed():
    Draft202012Validator.check_schema(schema())


@pytest.mark.parametrize("document", EXAMPLES, ids=lambda d: d["id"])
def test_examples(document):
    assert check(document) == []


@pytest.mark.parametrize("field", ["routes", "observers", "artifact_rules", "join_policy", "script", "timeout_seconds", "batch_size"])
def test_unknown_or_retired_fields(field):
    document = simple()
    document[field] = {}
    assert check(document)


@pytest.mark.parametrize("revision", [0, -1, True, "01", "latest", 1.5])
def test_invalid_revision(revision):
    document = simple()
    document["revision"] = revision
    assert check(document)


def test_unknown_resource():
    document = simple()
    document["fork"]["archive"]["call"]["operation"] = "missing"
    assert check(document)


def test_role_dependency_cycle():
    document = copy.deepcopy(EXAMPLES[1])
    document["observe"]["metadata"]["inputs"] = {"subjects": {"roles": ["media"]}}
    assert any("cycle" in error for error in check(document))


def test_repeated_contract_is_not_repeated_task_identity():
    assert check(EXAMPLES[2]) == []


def test_missing_evidence_predecessor():
    document = copy.deepcopy(EXAMPLES[1])
    document["observe"]["names"]["inputs"]["provenance"] = {"evidence": "missing"}
    assert check(document)


def test_wrong_port_type():
    document = copy.deepcopy(EXAMPLES[1])
    document["observe"]["names"]["inputs"]["provenance"] = "all"
    assert check(document)


def test_unknown_view():
    document = copy.deepcopy(EXAMPLES[1])
    document["groups"]["media_with_sidecars"]["prefer"] = ["metadata.no_such_view"]
    assert check(document)


def test_fact_view_is_not_relation_view():
    document = copy.deepcopy(EXAMPLES[1])
    document["groups"]["media_with_sidecars"]["prefer"] = ["metadata.records"]
    assert check(document)


def test_effect_cannot_have_collection_policy():
    document = copy.deepcopy(EXAMPLES[7])
    document["fork"]["deliver"]["call"]["output"] = {}
    assert check(document)


def test_effect_cannot_be_join_member():
    document = copy.deepcopy(EXAMPLES[3])
    document["join"]["members"]["mirror"] = ["example.archive/v1"]
    assert check(document)


def test_nonexporting_subrecipe_cannot_be_join_member():
    document = copy.deepcopy(EXAMPLES[4])
    document["fork"]["first"]["call"]["recipe"] = "no_output_child"
    assert check(document)


def test_join_cannot_be_effect():
    document = copy.deepcopy(EXAMPLES[3])
    document["join"]["call"]["operation"] = "deliver"
    assert check(document)


def test_absent_join_export():
    document = simple()
    document["export"] = "join"
    assert check(document)


def test_overlapping_binding_destinations():
    document = simple()
    document["fork"]["archive"]["call"]["bind"] = [
        {"from": "parameters", "path": "/first", "to": "intent", "at": "/a", "mode": "insert"},
        {"from": "parameters", "path": "/second", "to": "intent", "at": "/a/b", "mode": "insert"},
    ]
    assert check(document)


def test_pointer_escaped_siblings_do_not_overlap():
    document = simple()
    document["fork"]["archive"]["call"]["bind"] = [
        {"from": "parameters", "path": "/first", "to": "intent", "at": "/a~1b", "mode": "insert"},
        {"from": "parameters", "path": "/second", "to": "intent", "at": "/a/b", "mode": "insert"},
    ]
    assert check(document) == []


def test_no_output_retirement_needs_loss_rule():
    document = copy.deepcopy(EXAMPLES[5])
    del document["decisions"][0]["no_output"]["source_loss"]
    assert check(document)


def test_audio_operation_cannot_authorize_retirement():
    document = simple()
    document["fork"]["archive"]["call"]["operation"] = "audio"
    document["source"] = {"retirement": {"mode": "after-settlement"}}
    assert check(document)


def test_remote_parameter_schema_is_not_imported():
    document = simple()
    document["parameters"] = {"$ref": "https://example.invalid/schema.json"}
    assert check(document)


@pytest.mark.parametrize("text", [
    "a: 1\na: 2\n", "a: &x {b: 1}\nc: *x\n", "a: .nan\n", "1: bad\n",
])
def test_ambiguous_or_non_json_yaml_is_rejected(tmp_path, text):
    path = tmp_path / "bad.yaml"
    path.write_text(text)
    with pytest.raises(Exception):
        read_documents(path)


def test_yaml_12_keeps_yes_as_a_string(tmp_path):
    path = tmp_path / "scalar.yaml"
    path.write_text("value: yes\n")
    assert read_documents(path)[0]["value"] == "yes"


@pytest.mark.parametrize("semantic_id", ["Uppercase/v1", "white space", "", "x" * 161])
def test_invalid_semantic_ids(semantic_id):
    document = simple()
    document["id"] = semantic_id
    assert check(document)


def test_remote_dynamic_parameter_schema_is_not_imported():
    document = simple()
    document["parameters"] = {"$dynamicRef": "https://example.invalid/schema.json"}
    assert check(document)


def test_primary_store_cannot_also_be_copy_destination():
    document = simple()
    document["fork"]["archive"]["call"]["output"] = {"archive_store": "store", "copy_to": ["store"]}
    assert check(document)


def test_literal_json_is_not_recursively_interpreted_as_predicate_syntax():
    document = copy.deepcopy(EXAMPLES[9])
    document["decisions"][0]["when"]["facts"]["where"]["test"]["value"] = {"facts": {"view": "not-a-reference"}}
    assert check(document) == []



def test_candidate_role_must_belong_to_group():
    document = copy.deepcopy(EXAMPLES[1])
    document["roles"]["other"] = "example.other/v1"
    document["fork"]["audio"]["when"]["facts"]["roles"] = ["other"]
    assert check(document)


def test_duplicate_semantic_role_aliases_are_rejected():
    document = simple()
    document["roles"] = {"source": "example.role/v1", "another": "example.role/v1"}
    assert check(document)


def test_parameter_annotation_data_is_not_a_remote_reference():
    document = simple()
    document["parameters"] = {"type": "object", "examples": [{"$ref": "https://example.invalid/literal"}]}
    assert check(document) == []


def test_inline_yaml_merge_is_rejected(tmp_path):
    path = tmp_path / "merge.yaml"
    path.write_text("value: {<<: {one: 1}}\n")
    with pytest.raises(ValueError, match="merge"):
        read_documents(path)

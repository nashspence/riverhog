from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import TypeAdapter
from stove0_protocol.interface_schemas import schema_slice, validate_row_schema
from stove0_protocol.models import (
    JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
    JsonSchemaValidationProfile,
    ObserverContract,
    ObserverContractPayload,
)
from stove0_protocol.observation_interfaces import (
    OBSERVATION_INTERFACE_SEMANTICS,
    ObservationInterface,
    ObservationInterfacePayload,
)
from stove0_protocol.observation_views import RelationViewResult, SubjectView, project_interface
from stove0_protocol.predicates import RowPredicate


def _owner():
    facts = JsonSchemaValidationProfile.from_schema(
        "example.facts/v1",
        {
            "type": "object",
            "additionalProperties": False,
            "$defs": {
                "record": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "subject": {"type": "string"},
                        "value": {"type": "integer"},
                        "endpoint": {"type": "object"},
                    },
                    "required": ["subject", "value", "endpoint"],
                }
            },
            "properties": {
                "rows": {"type": "array", "items": {"$ref": "#/$defs/record"}},
                "statuses": {"type": "array", "items": {"type": "object"}},
                "relations": {"type": "array", "items": {"type": "object"}},
            },
            "required": ["rows", "statuses", "relations"],
        },
    )
    return ObserverContract.seal(
        ObserverContractPayload(
            id="example.observe/v1",
            options_schema=JsonSchemaValidationProfile.from_schema(
                "example.options/v1", {"type": "object"}
            ),
            facts_schema=facts,
            facts_semantics=JSON_SCHEMA_ONLY_SEMANTIC_PROFILE,
        )
    )


def _interface(*, many=False, relations=False):
    owner = _owner()
    views = {
        "records": {
            "kind": "subject-facts",
            "records": {"records_at": "/rows"},
            "subject_at": "/subject",
            "record_schema_at": "/properties/rows/items",
            "cardinality": "many-per-subject" if many else "one-per-subject",
        }
    }
    status = {
        "kind": "records",
        "records_at": "/statuses",
        "subject_at": "/subject",
        "value_at": "/status",
        "values": {"ok": "complete", "unknown": "unsupported"},
    }
    if many:
        views["records"]["status"] = status
    if relations:
        views["links"] = {
            "kind": "relation",
            "records": {"records_at": "/relations"},
            "where": True,
            "require": {"test": {"path": "/supported", "op": "eq", "value": True}},
            "primary": {
                "kind": "exact-endpoint",
                "at": "/primary",
                "lookup": {"source": "self", "view": "records", "keys": ["/endpoint"]},
            },
            "associated": {"kind": "subject-id", "at": "/associated"},
            "coverage": ["subjects"],
            "status": status,
        }
    return ObservationInterface.seal(
        ObservationInterfacePayload.model_validate(
            {
                "id": "example.interface/v1",
                "observer_contract": {"id": owner.id, "sha256": owner.contract_sha256},
                "facts_profile": {
                    "id": owner.facts_schema.id,
                    "sha256": owner.facts_schema.profile_sha256,
                },
                "semantic_profile": {
                    "id": owner.facts_semantics.id,
                    "sha256": owner.facts_semantics.profile_sha256,
                },
                "inputs": {"subjects": {"kind": "subjects"}},
                "views": views,
                "partitioning": "whole-scope" if relations else "independent-subjects",
                "empty_scope": "complete-empty",
                "interface_semantics": {
                    "id": OBSERVATION_INTERFACE_SEMANTICS.id,
                    "sha256": OBSERVATION_INTERFACE_SEMANTICS.profile_sha256,
                },
                "conformance_vectors_sha256": "a" * 64,
            }
        )
    )


def _facts():
    return {
        "rows": [
            {"subject": "a", "value": 1, "endpoint": {"state": "a"}},
            {"subject": "b", "value": 2, "endpoint": {"state": "b"}},
        ],
        "statuses": [{"subject": "a", "status": "ok"}, {"subject": "b", "status": "ok"}],
        "relations": [],
    }


def _project(facts, **options):
    return project_interface(
        interface=_interface(**options),
        contract=_owner(),
        subjects=("a", "b"),
        ports={"subjects": ("a", "b")},
        facts=facts,
    )


def test_record_schema_slice_preserves_owning_local_reference_environment():
    schema = schema_slice(_owner().facts_schema, "/properties/rows/items")
    schema.validate({"subject": "a", "value": 1, "endpoint": {}})
    with pytest.raises(ValueError, match="owning schema"):
        schema.validate({"subject": "a", "value": True, "endpoint": {}})
    with pytest.raises(ValueError, match="data rather than a schema"):
        schema_slice(_owner().facts_schema, "/$defs/record/required")


def test_one_per_subject_rejects_omissions_duplicates_and_unknown_instances():
    assert isinstance(_project(_facts())["records"], SubjectView)
    for rows in (
        [_facts()["rows"][0]],
        [_facts()["rows"][0]] * 2,
        [{"subject": "outside", "value": 1, "endpoint": {}}],
    ):
        with pytest.raises(ValueError, match="subject"):
            _project({**_facts(), "rows": rows})


def test_many_per_subject_empty_rows_require_independent_complete_status():
    projected = _project({**_facts(), "rows": []}, many=True)["records"]
    assert projected.rows == {"a": (), "b": ()}
    assert projected.statuses == {"a": "complete", "b": "complete"}
    with pytest.raises(ValueError, match="scope"):
        _project({**_facts(), "rows": [], "statuses": []}, many=True)
    with pytest.raises(ValueError, match="undeclared wire"):
        _project({**_facts(), "statuses": [{"subject": "a", "status": "maybe"}]}, many=True)


def test_complete_empty_is_controller_completion_without_fake_facts():
    projected = project_interface(
        interface=_interface(), contract=_owner(), subjects=(), ports={"subjects": ()}, facts=None
    )
    assert projected["records"].rows == {}
    with pytest.raises(ValueError, match="controller completion"):
        project_interface(
            interface=_interface(),
            contract=_owner(),
            subjects=(),
            ports={"subjects": ()},
            facts={"rows": [], "statuses": [], "relations": []},
        )


def test_exact_endpoints_and_relevant_unsupported_relations_never_become_negatives():
    facts = _facts()
    facts["relations"] = [{"primary": {"state": "a"}, "associated": "b", "supported": True}]
    view = _project(facts, relations=True)["links"]
    assert isinstance(view, RelationViewResult) and view.edges == (("a", "b"),)
    facts["relations"][0]["supported"] = False
    blocked = _project(facts, relations=True)["links"]
    assert not blocked.edges and blocked.statuses["b"] == "unsupported"
    facts["relations"][0]["supported"] = True
    facts["relations"][0]["primary"] = {"state": "outside"}
    assert _project(facts, relations=True)["links"].statuses["b"] == "unsupported"


def test_equal_endpoint_values_cannot_collapse_distinct_members():
    facts = _facts()
    facts["rows"][1]["endpoint"] = deepcopy(facts["rows"][0]["endpoint"])
    with pytest.raises(ValueError, match="multiple member"):
        _project(facts, relations=True)


def test_literal_operand_typing_uses_exact_interface_record_schema():
    schema = schema_slice(_owner().facts_schema, "/properties/rows/items")
    condition = TypeAdapter(RowPredicate).validate_python(
        {"test": {"path": "/value", "op": "eq", "value": True}}
    )
    with pytest.raises(ValueError, match="incompatible declared field type"):
        validate_row_schema(condition, schema)
    array = TypeAdapter(RowPredicate).validate_python(
        {"items": {"path": "/value", "quantifier": "any", "where": True}}
    )
    with pytest.raises(ValueError, match="array field"):
        validate_row_schema(array, schema)

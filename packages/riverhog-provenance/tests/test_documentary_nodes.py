from __future__ import annotations

import copy

import pytest
from riverhog_provenance import DocumentaryNode, inspect_documentary_node
from riverhog_provenance.history import _external_references


def external():
    return {
        "scope": "external",
        "journal_id": "urn:uuid:11111111-1111-4111-8111-111111111111",
        "entry": {
            "entry_id": "urn:uuid:22222222-2222-4222-8222-222222222222",
            "sequence": "17",
            "json_sha256": "a" * 64,
        },
        "assertion_id": "urn:uuid:33333333-3333-4333-8333-333333333333",
        "object_id": "urn:uuid:44444444-4444-4444-8444-444444444444",
        "object_type": "state",
    }


def test_exact_reference_is_validated_and_detached_without_selecting_its_children():
    value = external()
    selected = inspect_documentary_node(value)
    assert isinstance(selected, DocumentaryNode)
    assert selected.reference == value and not selected.descend
    selected.reference["entry"]["sequence"] = "18"
    assert value["entry"]["sequence"] == "17"
    assert inspect_documentary_node(value).reference == value


@pytest.mark.parametrize("field", ["journal_id", "entry", "assertion_id", "object_type"])
def test_exact_endpoint_shape_cannot_bypass_native_reference_validation(field):
    value = external()
    value[field] = "invalid"
    with pytest.raises(ValueError):
        inspect_documentary_node(value)


@pytest.mark.parametrize(
    "value",
    [None, True, 1, "text", {"type": "json", "value": {"scope": "external"}}],
    ids=["null", "boolean", "number", "text", "opaque-json"],
)
def test_terminal_nodes_do_not_retain_children(value):
    selected = inspect_documentary_node(value)
    assert selected.reference is None and not selected.descend


@pytest.mark.parametrize(
    "value",
    [{}, [], {"scope": "external"}, {"type": "json", "value": {}, "extra": 1}],
    ids=["object", "array", "unrelated-scope", "nonopaque-shape"],
)
def test_child_containers_preserve_original_traversal_without_claiming_a_reference(value):
    selected = inspect_documentary_node(value)
    assert selected.reference is None and selected.descend


def test_history_traversal_uses_the_same_classifier_and_keeps_opaque_payloads_opaque():
    selected = external()
    malformed = {**external(), "journal_id": "invalid"}
    value = {
        "outside": [selected],
        "opaque": {"type": "json", "value": [selected, malformed]},
        "nested": {"scope": "unrelated", "child": selected},
    }
    expected = [copy.deepcopy(selected), copy.deepcopy(selected)]
    assert list(_external_references(value)) == expected


class PointOnlyDict(dict):
    def __iter__(self):
        raise AssertionError("single-node classification iterated its fields")

    def items(self):
        raise AssertionError("single-node classification iterated its children")

    def values(self):
        raise AssertionError("single-node classification visited its children")


@pytest.mark.parametrize("width", [1, 17, 129, 4097])
def test_wide_node_classification_uses_point_probes_without_visiting_children(width):
    value = PointOnlyDict((str(index), object()) for index in range(width))
    value["scope"] = "external"
    selected = inspect_documentary_node(value)
    assert selected.reference is None and selected.descend


def test_opaque_node_never_inspects_its_unbounded_payload():
    opaque = PointOnlyDict(type="json", value=PointOnlyDict(forbidden=object()))
    selected = inspect_documentary_node(opaque)
    assert selected.reference is None and not selected.descend

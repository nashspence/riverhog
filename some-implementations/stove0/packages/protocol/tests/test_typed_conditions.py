from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError
from stove0_protocol.predicates import (
    Predicate,
    RowPredicate,
    Truth,
    evaluate_predicate,
    evaluate_row,
    quantify,
    read_pointer,
)


@pytest.mark.parametrize(
    ("kind", "values", "expected"),
    [
        ("every", [], Truth.FALSE),
        ("any", [], Truth.FALSE),
        ("none", [], Truth.TRUE),
        ("every", [Truth.TRUE, Truth.INDETERMINATE], Truth.INDETERMINATE),
        ("every", [Truth.FALSE, Truth.INDETERMINATE], Truth.FALSE),
        ("any", [Truth.TRUE, Truth.INDETERMINATE], Truth.TRUE),
        ("any", [Truth.FALSE, Truth.INDETERMINATE], Truth.INDETERMINATE),
        ("none", [Truth.FALSE, Truth.INDETERMINATE], Truth.INDETERMINATE),
    ],
)
def test_complete_empty_and_three_valued_quantification(kind, values, expected) -> None:
    assert quantify(kind, values) == expected


@pytest.mark.parametrize(
    ("record", "test", "expected"),
    [
        ({"n": 1}, {"path": "/n", "op": "eq", "value": True}, Truth.FALSE),
        ({"n": True}, {"path": "/n", "op": "in", "value": [1]}, Truth.FALSE),
        ({}, {"path": "/n", "op": "ne", "value": 1}, Truth.INDETERMINATE),
        ({}, {"path": "/n", "op": "exists", "value": False}, Truth.TRUE),
        ({"n": None}, {"path": "/n", "op": "eq", "value": None}, Truth.TRUE),
        ({"n": None}, {"path": "/n", "op": "exists", "value": False}, Truth.FALSE),
        (
            {"n": {"not": True, "facts": []}},
            {"path": "/n", "op": "eq", "value": {"not": True, "facts": []}},
            Truth.TRUE,
        ),
    ],
)
def test_literal_comparisons_preserve_json_types_and_missing_evidence(record, test, expected):
    predicate = TypeAdapter(RowPredicate).validate_python({"test": test})
    assert evaluate_row(predicate, record) == expected


def test_nested_metadata_tests_bind_the_same_array_element() -> None:
    predicate = TypeAdapter(RowPredicate).validate_python(
        {
            "items": {
                "path": "/facts",
                "quantifier": "any",
                "where": {
                    "all": [
                        {"test": {"path": "/name", "op": "eq", "value": "container-format"}},
                        {"test": {"path": "/value", "op": "eq", "value": "XMP"}},
                    ]
                },
            }
        }
    )
    assert (
        evaluate_row(
            predicate,
            {
                "facts": [
                    {"name": "container-format", "value": "MOV"},
                    {"name": "other", "value": "XMP"},
                ]
            },
        )
        == Truth.FALSE
    )
    assert (
        evaluate_row(predicate, {"facts": [{"name": "container-format", "value": "XMP"}]})
        == Truth.TRUE
    )
    assert evaluate_row(predicate, {}) == Truth.INDETERMINATE
    with pytest.raises(ValueError, match="array"):
        evaluate_row(predicate, {"facts": None})


def test_pointer_escaping_and_exact_array_indices() -> None:
    assert read_pointer({"a/b": {"~": [None]}}, "/a~1b/~0/0") is None


def test_negation_preserves_missing_task_evidence() -> None:
    predicate = TypeAdapter(Predicate).validate_python(
        {
            "not": {
                "facts": {
                    "view": "probe.records",
                    "scope": "input",
                    "quantifier": "every",
                    "where": True,
                }
            }
        }
    )
    assert evaluate_predicate(predicate, lambda _facts: Truth.INDETERMINATE) == Truth.INDETERMINATE


@pytest.mark.parametrize(
    "predicate",
    [
        1,
        "true",
        None,
        {"negated": True},
        {"test": {"path": "/n", "op": "exists", "value": 1}},
        {"test": {"path": "/n", "op": "in", "value": []}},
        {"test": {"path": "/a~2b", "op": "eq", "value": None}},
    ],
)
def test_condition_language_is_closed_and_has_exact_operand_types(predicate) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(RowPredicate).validate_python(predicate)

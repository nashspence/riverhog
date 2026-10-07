from __future__ import annotations

from pathlib import Path

import pytest
from config_validation import ConfigError
from pydantic import ValidationError
from stove0_recipe_config.reading import read_source_documents
from stove0_recipe_config.source import RecipeSource


def _source(**updates):
    return {
        "format": "stove0-recipe/v1",
        "id": "example/v1",
        "revision": 1,
        "fork": {"archive": {"call": {"operation": "encode"}}},
        **updates,
    }


@pytest.mark.parametrize("revision", [1, "1", 123456789012345678901234567890])
def test_revision_accepts_source_integers_and_exact_decimal_strings(revision) -> None:
    assert RecipeSource.model_validate(_source(revision=revision)).revision == int(revision)


@pytest.mark.parametrize("revision", [True, 1.0, "01", "1.0", 0])
def test_revision_rejects_coercion_and_noncanonical_decimal_spellings(revision) -> None:
    with pytest.raises(ValidationError):
        RecipeSource.model_validate(_source(revision=revision))


def test_literal_options_arrays_keep_order_and_types_while_output_sets_normalize() -> None:
    recipe = RecipeSource.model_validate(
        _source(
            fork={
                "archive": {
                    "call": {
                        "operation": "encode",
                        "options": {"ordered": [2, True, 1]},
                        "output": {"tags": ["z", "a"], "copy_to": ["secondary", "another"]},
                    }
                }
            }
        )
    )
    call = recipe.fork["archive"].call
    assert call.options == {"ordered": [2, True, 1]}
    assert call.output.to_policy().tags == ("a", "z")
    assert call.output.copy_to == ("another", "secondary")


def test_disjoint_explicit_bindings_and_decoded_overlap() -> None:
    bindings = [
        {"from": "parameters", "path": "/a", "to": "intent", "at": "/a~1b", "mode": "insert"},
        {
            "from": "evaluation",
            "path": "/variant_id",
            "to": "options",
            "at": "/variant",
            "mode": "insert",
        },
    ]
    RecipeSource.model_validate(
        _source(fork={"archive": {"call": {"operation": "encode", "bind": bindings}}})
    )
    overlap = {**bindings[0], "at": "/a~1b/nested"}
    with pytest.raises(ValidationError, match="overlapping"):
        RecipeSource.model_validate(
            _source(
                fork={"archive": {"call": {"operation": "encode", "bind": [bindings[0], overlap]}}}
            )
        )


def test_json_domain_yaml12_keeps_on_off_as_strings(tmp_path: Path) -> None:
    path = tmp_path / "recipe.yaml"
    path.write_text("options: {a: on, b: off, c: true, d: false}\n", encoding="utf-8")
    assert read_source_documents(path) == (
        {"options": {"a": "on", "b": "off", "c": True, "d": False}},
    )


@pytest.mark.parametrize(
    "text",
    [
        "id: a\nid: b\n",
        "a: &a {}\nb: *a\n",
        "a: {<<: {b: 1}}\n",
        "a: !!str value\n",
        "a: .nan\n",
        "a: 2020-01-01\n",
        "1: value\n",
    ],
)
def test_source_yaml_rejects_aliases_merges_tags_and_non_json_values(
    tmp_path: Path, text: str
) -> None:
    path = tmp_path / "recipe.yaml"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError):
        read_source_documents(path)

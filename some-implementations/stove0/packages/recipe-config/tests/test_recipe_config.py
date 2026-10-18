from __future__ import annotations

from pathlib import Path

import pytest
from config_validation import ConfigError
from stove0_recipe_config import (
    CompiledRecipeCatalog,
    RecipeSource,
    RecipeSourceCatalog,
    load_recipe_catalog,
)


def test_checked_example_is_a_portable_content_addressed_catalog() -> None:
    path = Path(__file__).parents[5] / "qualification/fixtures/stove0/recipes.yaml"

    catalog = load_recipe_catalog(path)
    document = catalog.validation_document()

    assert document.catalog_sha256 == catalog.sha256
    assert document.recipe_count == len(catalog.recipes)
    assert document.operation_count == len(catalog.closure.operations)
    assert "stove0.conformance-media/v1" in {recipe.id for recipe in catalog.recipes}
    assert CompiledRecipeCatalog.model_json_schema()["additionalProperties"] is False


def test_catalog_loader_rejects_ambiguous_duplicate_yaml_keys(tmp_path: Path) -> None:
    path = tmp_path / "recipes.yaml"
    path.write_text(
        "format: stove0-recipe-source-catalog/v1\nformat: stove0-recipe-source-catalog/v1\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="duplicate key"):
        load_recipe_catalog(path)


def test_exact_subrecipe_cycle_detection_is_iterative_and_ancestry_aware() -> None:
    recipes = {
        name: RecipeSource.model_validate(
            {
                "format": "stove0-recipe/v1",
                "id": "fixture." + name + "/v1",
                "revision": 1,
                "fork": {"child": {"call": {"recipe": other}}},
            }
        )
        for name, other in (("first", "second"), ("second", "first"))
    }
    with pytest.raises(ValueError, match="cycle"):
        RecipeSourceCatalog(recipes=recipes).compile()

"""Authoring installation and exact retained compiled programs are distinct."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from stove0_protocol import RecipeIdentityRef, canonical_json_sha256
from stove0_protocol.models import Sha256, Stove0ProtocolModel
from stove0_protocol.predicates import LocalName
from stove0_target_protocol import OperationContract

from stove0_recipe_config.compiled import CompiledRecipe, RecipeContract
from stove0_recipe_config.compiler import (
    compile_recipe_catalog,
    observation_order,
    verify_compiled_recipe,
)
from stove0_recipe_config.dependencies import (
    RecipeDependencyCatalog,
    RecipeDependencyClosure,
    RecipeResourceDocument,
)
from stove0_recipe_config.reading import read_source_documents
from stove0_recipe_config.source import RecipeSource


class RecipeValidation(Stove0ProtocolModel):
    recipe: RecipeIdentityRef
    contract: RecipeContract
    dependency_count: NonnegativeDecimal


class RecipeCatalogValidation(Stove0ProtocolModel):
    format: Literal["stove0-recipe-catalog-validation/v1"] = "stove0-recipe-catalog-validation/v1"
    catalog_sha256: Sha256
    operation_count: NonnegativeDecimal
    recipe_count: NonnegativeDecimal
    recipes: tuple[RecipeValidation, ...]


class RecipeExplanation(Stove0ProtocolModel):
    recipe: CompiledRecipe
    observation_order: tuple[str, ...]


class RecipeCatalogExplanation(Stove0ProtocolModel):
    format: Literal["stove0-recipe-catalog-explanation/v1"] = "stove0-recipe-catalog-explanation/v1"
    catalog_sha256: Sha256
    recipes: tuple[RecipeExplanation, ...]


class CompiledRecipeCatalog(Stove0ProtocolModel):
    format: Literal["stove0-compiled-recipe-catalog/v1"] = "stove0-compiled-recipe-catalog/v1"
    recipes: tuple[CompiledRecipe, ...] = ()
    closure: RecipeDependencyClosure = Field(default_factory=RecipeDependencyClosure)

    @model_validator(mode="after")
    def exact_programs(self) -> Self:
        keys = [(recipe.id, recipe.revision) for recipe in self.recipes]
        if keys != sorted(set(keys)):
            raise ValueError("installed recipe identities must be unique and canonical")
        for recipe in self.recipes:
            verify_compiled_recipe(recipe, self.closure)
        return self

    @property
    def sha256(self) -> str:
        return canonical_json_sha256(self.model_dump(mode="json", by_alias=True))

    def recipe(
        self, recipe_id: str, revision: int | None = None, *, sha256: str | None = None
    ) -> CompiledRecipe:
        matches = [
            recipe
            for recipe in (*self.recipes, *self.closure.recipes)
            if recipe.id == recipe_id
            and (revision is None or recipe.revision == revision)
            and (sha256 is None or recipe.sha256 == sha256)
        ]
        if not matches:
            raise KeyError(recipe_id)
        if (
            sha256 is None
            and len(
                {
                    recipe.sha256
                    for recipe in matches
                    if recipe.revision == max(item.revision for item in matches)
                }
            )
            != 1
        ):
            raise ValueError("recipe identity is ambiguous without its exact program digest")
        return max(matches, key=lambda recipe: recipe.revision)

    def operation(self, operation_id: str, sha256: str) -> OperationContract:
        return self.closure.operation(id=operation_id, sha256=sha256)

    @classmethod
    def load(cls, path: Path) -> CompiledRecipeCatalog:
        documents = read_source_documents(Path(path))
        if len(documents) != 1:
            raise ValueError("compiled catalog must be one closed document")
        return cls.model_validate(documents[0])

    def validation_document(self) -> RecipeCatalogValidation:
        return RecipeCatalogValidation.model_validate(
            {
                "catalog_sha256": self.sha256,
                "operation_count": str(len(self.closure.operations)),
                "recipe_count": str(len(self.recipes)),
                "recipes": tuple(
                    RecipeValidation.model_validate(
                        {
                            "recipe": recipe.ref,
                            "contract": recipe.contract,
                            "dependency_count": str(len(recipe.dependencies)),
                        }
                    )
                    for recipe in self.recipes
                ),
            }
        )

    def explanation_document(self) -> RecipeCatalogExplanation:
        return RecipeCatalogExplanation(
            catalog_sha256=self.sha256,
            recipes=tuple(
                RecipeExplanation(
                    recipe=recipe, observation_order=observation_order(recipe, self.closure)
                )
                for recipe in self.recipes
            ),
        )


class RecipeSourceCatalog(Stove0ProtocolModel):
    format: Literal["stove0-recipe-source-catalog/v1"] = "stove0-recipe-source-catalog/v1"
    resources: dict[LocalName, RecipeResourceDocument] = Field(default_factory=dict)
    recipes: dict[LocalName, RecipeSource] = Field(min_length=1)

    @classmethod
    def load(cls, path: Path) -> RecipeSourceCatalog:
        documents = read_source_documents(Path(path))
        if len(documents) != 1:
            raise ValueError("source catalog must be one closed document")
        return cls.model_validate(documents[0])

    def compile(self) -> CompiledRecipeCatalog:
        programs, closures = compile_recipe_catalog(
            self.recipes, RecipeDependencyCatalog(resources=self.resources)
        )
        observers, operations, children = {}, {}, {}
        for closure in closures.values():
            for resource in closure.observers:
                observers[resource.interface.interface_sha256] = resource
            for operation in closure.operations:
                operations[operation.contract.contract_sha256] = operation
            for child in closure.recipes:
                children[child.sha256] = child
        return CompiledRecipeCatalog(
            recipes=tuple(
                sorted(programs.values(), key=lambda recipe: (recipe.id, recipe.revision))
            ),
            closure=RecipeDependencyClosure(
                observers=tuple(observers[key] for key in sorted(observers)),
                operations=tuple(operations[key] for key in sorted(operations)),
                recipes=tuple(children[key] for key in sorted(children)),
            ),
        )


def load_recipe_catalog(path: Path) -> CompiledRecipeCatalog:
    """Validate current authoring or already compiled installation documents offline."""
    documents = read_source_documents(Path(path))
    if len(documents) != 1:
        raise ValueError("recipe catalog must be one closed document")
    document = documents[0]
    if document.get("format") == "stove0-recipe-source-catalog/v1":
        return RecipeSourceCatalog.model_validate(document).compile()
    if document.get("format") == "stove0-compiled-recipe-catalog/v1":
        return CompiledRecipeCatalog.model_validate(document)
    raise ValueError("recipe catalog requires a current source or compiled format")

"""Deterministic offline assembly of separately maintained exact local documents."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter, ValidationError
from stove0_protocol.predicates import LocalName

from stove0_recipe_config.catalog import (
    CompiledRecipeCatalog,
    RecipeResourceCatalog,
    RecipeSourceCatalog,
)
from stove0_recipe_config.diagnostics import RecipeCompileError, source_pointer
from stove0_recipe_config.reading import read_source_documents
from stove0_recipe_config.source import RecipeSource
from stove0_recipe_config.source_map import RecipeSourceMap, source_document_map


@dataclass(frozen=True)
class RecipeInputAssembly:
    document: RecipeSourceCatalog | CompiledRecipeCatalog
    locations: RecipeSourceMap
    paths: tuple[Path, ...]

    def compile(self) -> CompiledRecipeCatalog:
        try:
            return (
                self.document.compile()
                if isinstance(self.document, RecipeSourceCatalog)
                else self.document
            )
        except ValueError as error:
            raise self.diagnostic(error) from error

    def diagnostic(self, error: ValueError) -> ValueError:
        return _source_diagnostic(error, self.locations)


def _source_diagnostic(error: ValueError, locations: RecipeSourceMap) -> ValueError:
    if isinstance(error, RecipeCompileError):
        pointer, message = error.pointer, error.message
    elif isinstance(error, ValidationError):
        first = error.errors(include_input=False)[0]
        pointer = source_pointer(*(str(part) for part in first["loc"]))
        message = first["msg"]
    else:
        pointer, message = "", str(error)
    selected = pointer
    while selected not in locations.locations:
        selected = selected.rsplit("/", 1)[0]
    location = locations.locations[selected]
    return ValueError(
        f"{location.source}:{location.line}:{location.column} {pointer or '/'}: {message}"
    )


def _document(path: Path) -> dict[str, Any]:
    documents = read_source_documents(path)
    if len(documents) != 1:
        raise ValueError("recipe input must be one closed local document")
    return documents[0]


def assemble_recipe_inputs(
    path: Path, *, resources_path: Path | None = None, recipe_paths: dict[str, Path] | None = None
) -> RecipeInputAssembly:
    path = Path(path)
    primary = _document(path)
    primary_map = source_document_map(path)
    locations = dict(primary_map.locations)
    paths = [path]
    extra = recipe_paths or {}
    format = primary.get("format")
    if format == "stove0-compiled-recipe-catalog/v1":
        if resources_path is not None or extra:
            raise ValueError("compiled installation documents cannot assemble authoring inputs")
        try:
            compiled = CompiledRecipeCatalog.model_validate(primary)
        except ValidationError as error:
            raise _source_diagnostic(error, primary_map) from error
        return RecipeInputAssembly(compiled, primary_map, tuple(paths))
    try:
        if format == "stove0-recipe-source-catalog/v1":
            RecipeSourceCatalog.model_validate(primary)
        elif format == "stove0-recipe-resources/v1":
            RecipeResourceCatalog.model_validate(primary)
        elif format == "stove0-recipe/v1":
            RecipeSource.model_validate(primary)
    except ValidationError as error:
        raise _source_diagnostic(error, primary_map) from error
    if format == "stove0-recipe-source-catalog/v1":
        source_document = dict(primary)
    elif format == "stove0-recipe/v1":
        source_document = {
            "format": "stove0-recipe-source-catalog/v1",
            "resources": {},
            "recipes": {"main": primary},
        }
        locations.update(
            {
                "/recipes/main" + pointer: location
                for pointer, location in primary_map.locations.items()
            }
        )
    elif format == "stove0-recipe-resources/v1":
        source_document = {
            "format": "stove0-recipe-source-catalog/v1",
            "resources": primary.get("resources", {}),
            "recipes": {},
        }
    else:
        raise ValueError(f"{path}:1:1 recipe input requires a current source or compiled format")
    resources = dict(source_document.get("resources", {}))
    recipes = dict(source_document.get("recipes", {}))
    try:
        if format == "stove0-recipe-resources/v1":
            RecipeResourceCatalog.model_validate(primary)
        if resources_path is not None:
            resource_path = Path(resources_path)
            resource_document = _document(resource_path)
            resource_map = source_document_map(resource_path)
            paths.append(resource_path)
            try:
                resource_catalog = RecipeResourceCatalog.model_validate(resource_document)
            except ValidationError as error:
                raise _source_diagnostic(error, resource_map) from error
            duplicates = resources.keys() & resource_catalog.resources.keys()
            if duplicates:
                raise ValueError(
                    "resource aliases collide across exact local documents: "
                    + ", ".join(sorted(duplicates))
                )
            resources.update(resource_document["resources"])
            locations.update(
                {
                    pointer: location
                    for pointer, location in resource_map.locations.items()
                    if pointer.startswith("/resources")
                }
            )
        for alias, recipe_path in extra.items():
            TypeAdapter(LocalName).validate_python(alias)
            if alias in recipes:
                raise ValueError(f"local recipe alias is supplied more than once: {alias}")
            recipe_path = Path(recipe_path)
            document = _document(recipe_path)
            paths.append(recipe_path)
            mapped = source_document_map(recipe_path)
            prefix = source_pointer("recipes", alias)
            locations.update(
                {prefix + pointer: location for pointer, location in mapped.locations.items()}
            )
            recipes[alias] = document
        source_document.update(resources=resources, recipes=recipes)
        source_catalog = RecipeSourceCatalog.model_validate(source_document)
    except ValueError as error:
        raise _source_diagnostic(
            error, RecipeSourceMap(source=str(path), locations=locations)
        ) from error
    return RecipeInputAssembly(
        source_catalog, RecipeSourceMap(source=str(path), locations=locations), tuple(paths)
    )

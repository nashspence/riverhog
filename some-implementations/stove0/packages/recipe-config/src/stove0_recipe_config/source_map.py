"""Source locations are auxiliary diagnostics, excluded from semantic identity."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from riverhog_protocol.exact_scalar import NonnegativeDecimal
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from stove0_protocol.models import Stove0ProtocolModel
from stove0_protocol.predicates import Pointer

from stove0_recipe_config.reading import read_source_documents


class RecipeSourceLocation(Stove0ProtocolModel):
    line: NonnegativeDecimal = Field(ge=1)
    column: NonnegativeDecimal = Field(ge=1)


class RecipeSourceMap(Stove0ProtocolModel):
    format: Literal["stove0-recipe-source-map/v1"] = "stove0-recipe-source-map/v1"
    source: str
    locations: dict[Pointer, RecipeSourceLocation]


def recipe_source_map(path: Path) -> RecipeSourceMap:
    documents = read_source_documents(path)
    if len(documents) != 1:
        raise ValueError("recipe source map requires one closed catalog document")
    # The strict JSON-domain reader rejects aliases, tags and malformed source
    # first. Round-trip nodes supply locations, never another semantic parser.
    loader = YAML(typ="rt")
    loader.version = (1, 2)
    loader.allow_duplicate_keys = False
    document = loader.load(path.read_text(encoding="utf-8"))
    locations = {"": RecipeSourceLocation.model_validate({"line": "1", "column": "1"})}
    pending = [("", document)]
    while pending:
        prefix, node = pending.pop()
        if isinstance(node, CommentedMap):
            children = ((str(key), node[key], node.lc.key(key)) for key in node)
        elif isinstance(node, CommentedSeq):
            children = (
                (str(index), value, node.lc.item(index)) for index, value in enumerate(node)
            )
        else:
            continue
        for name, child, location in children:
            pointer = prefix + "/" + name.replace("~", "~0").replace("/", "~1")
            locations[pointer] = RecipeSourceLocation.model_validate(
                {"line": str(location[0] + 1), "column": str(location[1] + 1)}
            )
            pending.append((pointer, child))
    return RecipeSourceMap(source=str(path), locations=locations)

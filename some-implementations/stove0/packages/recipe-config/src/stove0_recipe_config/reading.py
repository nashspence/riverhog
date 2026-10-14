"""JSON-domain YAML 1.2 authoring, with no aliases, tags, merges or implicit code."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from config_validation import ConfigError
from riverhog_canonical_json import canonical_json_bytes
from ruamel.yaml import YAML
from ruamel.yaml.events import AliasEvent, CollectionStartEvent, ScalarEvent


def read_source_documents(path: Path) -> tuple[dict[str, Any], ...]:
    text = path.read_text(encoding="utf-8")
    loader = YAML(typ="safe")
    loader.version = (1, 2)
    loader.allow_duplicate_keys = False
    try:
        for event in loader.parse(text):
            if isinstance(event, AliasEvent):
                raise ConfigError("recipe YAML aliases are forbidden")
            if isinstance(event, (ScalarEvent, CollectionStartEvent)) and event.tag is not None:
                raise ConfigError("recipe YAML explicit tags are forbidden")
            if isinstance(event, ScalarEvent) and event.value == "<<" and event.style is None:
                raise ConfigError("recipe YAML merge syntax is forbidden")
        documents = tuple(loader.load_all(text))
    except ConfigError:
        raise
    except Exception as exc:
        raise ConfigError(f"invalid recipe YAML: {exc}") from exc
    for document in documents:
        if not isinstance(document, dict):
            raise ConfigError("recipe source must be an object")
        # The owning canonical authority rejects non-JSON scalars, non-string
        # keys, non-finite/out-of-domain numbers and preserves literal types.
        try:
            canonical_json_bytes(document)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"recipe source is outside the canonical JSON domain: {exc}") from exc
    return documents

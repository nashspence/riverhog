from __future__ import annotations

from pathlib import PurePosixPath

from arc_core.domain.errors import InvalidTarget
from arc_core.domain.models import Target
from arc_core.domain.types import CollectionId
from arc_core.fs_paths import PathNormalizationError, normalize_collection_id


def _parse_collection_id(raw: str) -> CollectionId:
    try:
        return CollectionId(normalize_collection_id(raw))
    except PathNormalizationError as exc:
        raise InvalidTarget(str(exc)) from exc


def parse_target(raw: str) -> Target:
    collection_raw, separator, remainder = raw.partition(":")
    if separator:
        collection = _parse_collection_id(collection_raw)
        raw_path = remainder
        if raw_path in {"/", ""}:
            raise InvalidTarget("empty path")
        if not raw_path.startswith("/"):
            raise InvalidTarget("invalid target syntax")
        if "//" in raw_path:
            raise InvalidTarget("repeated slash")
        is_dir = raw_path.endswith("/")
        body = raw_path[:-1] if is_dir else raw_path
        path = PurePosixPath(body)
        if str(path) != body:
            raise InvalidTarget("non-canonical path")
        if any(part in {".", ".."} for part in path.parts):
            raise InvalidTarget("dot segments not allowed")
        return Target(collection_id=collection, path=path, is_dir=is_dir)

    return Target(collection_id=_parse_collection_id(raw), path=None, is_dir=False)

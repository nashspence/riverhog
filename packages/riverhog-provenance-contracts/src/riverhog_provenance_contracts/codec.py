"""Lossless Riverhog wire admission before PostgreSQL/jsonb or schema validation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from riverhog_canonical_json import (
    CanonicalJsonError,
    canonical_json_bytes,
    parse_identity_json,
    require_canonical_json,
)

MAX_SAFE_JSON_INTEGER = (1 << 53) - 1
MAX_DEPTH = 96


def require_portable_json(value: Any, *, depth: int = 0) -> None:
    """Reject information-losing JSON inputs; wide/exact numbers use string codecs.

    This is deliberately stricter than general JCS: nulls and floats are not
    native evidence. A tagged decimal/integer preserves the intended number.
    Binary source values and non-Unicode names belong in base64 envelopes.
    """
    if depth > MAX_DEPTH:
        raise ValueError("provenance JSON nesting limit exceeded")
    if type(value) is bool:
        return
    if type(value) is int:
        if not -MAX_SAFE_JSON_INTEGER <= value <= MAX_SAFE_JSON_INTEGER:
            raise ValueError("wide integer requires a typed string codec")
        return
    if type(value) is str:
        if "\x00" in value:
            raise ValueError("U+0000 is not portable to PostgreSQL jsonb; use bytes")
        if value.isascii():
            return
        for c in value:
            cp = ord(c)
            if 0xD800 <= cp <= 0xDFFF or 0xFDD0 <= cp <= 0xFDEF or cp & 0xFFFF >= 0xFFFE:
                raise ValueError("nonportable Unicode code point; preserve source bytes")
        return
    if type(value) is list:
        for child in value:
            require_portable_json(child, depth=depth + 1)
        return
    if type(value) is dict:
        for key, child in value.items():
            if type(key) is not str:
                raise TypeError("JSON member names must be strings")
            require_portable_json(key, depth=depth + 1)
            require_portable_json(child, depth=depth + 1)
        return
    raise ValueError("provenance JSON excludes nulls, floats and non-JSON values")


def canonical_document(value: Mapping[str, Any]) -> bytes:
    document = dict(value)
    require_portable_json(document)
    return canonical_json_bytes(document)


def decode_document(raw: bytes, *, canonical: bool = True) -> dict[str, Any]:
    """Parse before duplicate keys or precision information can be discarded."""
    try:
        value = require_canonical_json(raw) if canonical else parse_identity_json(raw)
    except CanonicalJsonError as exc:
        raise ValueError(f"invalid provenance JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError("a provenance document must be a JSON object")
    require_portable_json(value)
    return value

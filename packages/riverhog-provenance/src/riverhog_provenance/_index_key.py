"""Canonical scalar, byte and tuple keys for private semantic indexes."""

from typing import Any

from riverhog_canonical_json import canonical_json_bytes


def encode_index_key(value: Any) -> bytes:
    if isinstance(value, bytes):
        return b"b" + value
    if isinstance(value, tuple):
        return b"t" + b"".join(
            str(len(part)).encode() + b":" + part for part in map(encode_index_key, value)
        )
    return b"j" + canonical_json_bytes(value)

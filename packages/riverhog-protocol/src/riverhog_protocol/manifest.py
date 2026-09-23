from __future__ import annotations

import hashlib
from collections.abc import Iterable

from riverhog_canonical_json import canonical_json_bytes, format_scalar

from riverhog_protocol.collection_upload_transport import collection_upload_path_order_key


def collection_content_identity(files: Iterable[tuple[str, int, str]]) -> str:
    return collection_content_identity_ordered(
        sorted(files, key=lambda item: collection_upload_path_order_key(item[0]))
    )


def collection_content_identity_ordered(files: Iterable[tuple[str, int, str]]) -> str:
    digest = hashlib.sha256()
    digest.update(b'{"files":[')
    separator = b""
    for path, byte_count, sha256 in files:
        digest.update(separator)
        digest.update(
            canonical_json_bytes(
                {
                    "path": path,
                    "bytes": format_scalar("nonnegative", byte_count),
                    "sha256": sha256,
                }
            )
        )
        separator = b","
    digest.update(b'],"format":"riverhog-collection-content/v1"}')
    return digest.hexdigest()


__all__ = ["collection_content_identity", "collection_content_identity_ordered"]

"""Exact selected-copy description and tag authorities for offline recovery."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_DOCUMENT_BYTES_MAX,
    COLLECTION_DESCRIPTION_RELATIVE_PATH,
    COLLECTION_TAG_HEAD_RELATIVE_PATH,
    COLLECTION_TAG_NODE_BYTES_MAX,
    CollectionDescriptionDocument,
    CollectionTagHeadDocument,
    CollectionTagSet,
    CollectionTagSetRoot,
    collection_tag_node_path,
)

ReadPlaintext = Callable[[str, int], bytes]


def _write_exact(path: Path, content: bytes) -> None:
    with path.open("xb") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())


@dataclass(frozen=True, slots=True)
class SelectedMetadata:
    description: CollectionDescriptionDocument | None
    head: CollectionTagHeadDocument
    description_sha256: str | None
    head_sha256: str
    tag_count: int
    node_count: int

    def pins(self) -> dict[str, object]:
        return {
            "description_sha256": self.description_sha256,
            "description_revision": self.description.revision if self.description else None,
            "description_identity": (
                self.description.description_identity if self.description else None
            ),
            "tag_head_sha256": self.head_sha256,
            "tag_revision": self.head.revision,
            "tag_set_identity": self.head.tag_set_identity,
            "tag_head_identity": self.head.head_identity,
        }


def stage_metadata(
    *,
    read_plaintext: ReadPlaintext,
    description_exists: bool,
    archive_root_sha256: str,
    staging: Path,
) -> SelectedMetadata:
    """Retain exact authority bytes and bounded readable exports under staging."""

    metadata = staging / "metadata"
    tags_dir = metadata / "tags"
    nodes_dir = tags_dir / "nodes"
    nodes_dir.mkdir(parents=True, exist_ok=True)
    head_raw = read_plaintext(COLLECTION_TAG_HEAD_RELATIVE_PATH, 64 * 1024)
    head = CollectionTagHeadDocument.from_json_bytes(head_raw)
    if head.archive_root_sha256 != archive_root_sha256:
        raise ValueError("tag head belongs to another archive root")
    _write_exact(tags_dir / "head.json", head_raw)

    description_raw = (
        read_plaintext(
            COLLECTION_DESCRIPTION_RELATIVE_PATH, COLLECTION_DESCRIPTION_DOCUMENT_BYTES_MAX
        )
        if description_exists
        else None
    )
    description = (
        CollectionDescriptionDocument.from_json_bytes(description_raw)
        if description_raw is not None
        else None
    )
    if description is not None and description.archive_root_sha256 != archive_root_sha256:
        raise ValueError("description belongs to another archive root")
    if description_raw is not None:
        _write_exact(metadata / "description.json", description_raw)
    value = description.description if description is not None else None
    description_sha256 = (
        hashlib.sha256(description_raw).hexdigest() if description_raw is not None else None
    )
    state = {
        "format": "riverhog-recovered-description-state/v1",
        "archive_root_sha256": archive_root_sha256,
        "status": (
            "absent-in-copy" if description is None else "unset" if value is None else "present"
        ),
        "description": value,
        "source_sha256": description_sha256,
        "revision": description.revision if description is not None else None,
        "description_identity": (
            description.description_identity if description is not None else None
        ),
    }
    _write_exact(metadata / "description-state.json", canonical_json_bytes(state))
    _write_exact(metadata / "description.txt", value.encode("utf-8") if value else b"")

    node_count = 0

    class NodeStore:
        def get(self, digest: str) -> bytes:
            nonlocal node_count
            path = nodes_dir / f"{digest}.bin"
            if not path.exists():
                raw = read_plaintext(
                    collection_tag_node_path(digest), COLLECTION_TAG_NODE_BYTES_MAX
                )
                _write_exact(path, raw)
                node_count += 1
            return path.read_bytes()

        def put(self, digest: str, encoded: bytes) -> None:
            del digest, encoded
            raise TypeError("selected tag authority is read-only")

    selected_tags: Iterator[str] = CollectionTagSet(
        NodeStore(), CollectionTagSetRoot.seal(head.root_sha256)
    ).iter_tags()
    tag_count = 0
    with (metadata / "tags.jsonseq").open("xb") as output:
        authority = {
            "format": "riverhog-recovered-collection-tags/v1",
            "record": "authority",
            "revision": head.revision,
            "tag_set_identity": head.tag_set_identity,
            "head_identity": head.head_identity,
        }
        output.write(canonical_json_bytes(authority) + b"\n")
        for tag in selected_tags:
            output.write(canonical_json_bytes({"record": "tag", "tag": tag}) + b"\n")
            tag_count += 1
        output.write(canonical_json_bytes({"record": "complete", "tag_count": tag_count}) + b"\n")
        output.flush()
        os.fsync(output.fileno())
    return SelectedMetadata(
        description=description,
        head=head,
        description_sha256=description_sha256,
        head_sha256=hashlib.sha256(head_raw).hexdigest(),
        tag_count=tag_count,
        node_count=node_count,
    )

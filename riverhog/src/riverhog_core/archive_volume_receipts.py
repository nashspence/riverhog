"""Parse the catalog's source-owned sealed volume receipt representation."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

from riverhog_core.domain.archive import SealedPackVolume, SealedRawVolume, StoredArchivePart
from riverhog_core.retrieval_cache_receipts import parse_retrieval_cache_receipt


def _parse_parts(values: Sequence[Mapping[str, object]]) -> tuple[StoredArchivePart, ...]:
    return tuple(
        StoredArchivePart(
            number=stored_nonnegative_int(value["number"], "part number"),
            plaintext_start=stored_nonnegative_int(
                value["plaintext_start"], "part plaintext start"
            ),
            plaintext_bytes=stored_nonnegative_int(
                value["plaintext_bytes"], "part plaintext bytes"
            ),
            plaintext_sha256=str(value["plaintext_sha256"]),
            stored_bytes=stored_nonnegative_int(value["stored_bytes"], "part stored bytes"),
            stored_sha256=str(value["stored_sha256"]),
        )
        for value in values
    )


def stored_nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def parse_sealed_pack(content: str) -> SealedPackVolume:
    value = json.loads(content)
    return SealedPackVolume(
        volume_id=str(value["volume_id"]),
        sequence=int(value["sequence"]),
        relative_path=str(value["relative_path"]),
        artifacts=int(value["artifacts"]),
        source_bytes=int(value["source_bytes"]),
        plaintext_bytes=int(value["plaintext_bytes"]),
        age_state_json=json.dumps(value["age_state"], sort_keys=True, separators=(",", ":")),
        index_sha256=str(value["index_sha256"]),
        plan_sha256=str(value["plan_sha256"]),
        parts=_parse_parts(value["parts"]),
        revision=str(value["revision"]) if value["revision"] is not None else None,
        completed_at=str(value["completed_at"]),
        retrieval_cache=parse_retrieval_cache_receipt(value.get("retrieval_cache")),
    )


def parse_sealed_raw(content: str) -> SealedRawVolume:
    value = json.loads(content)
    return SealedRawVolume(
        volume_id=str(value["volume_id"]),
        sequence=int(value["sequence"]),
        relative_path=str(value["relative_path"]),
        artifact_id=str(value["artifact_id"]),
        artifact_offset=int(value["artifact_offset"]),
        plaintext_bytes=int(value["plaintext_bytes"]),
        artifact_bytes=int(value["artifact_bytes"]),
        artifact_sha256=str(value["artifact_sha256"]),
        age_state_json=json.dumps(value["age_state"], sort_keys=True, separators=(",", ":")),
        parts=_parse_parts(value["parts"]),
        revision=str(value["revision"]) if value["revision"] is not None else None,
        completed_at=str(value["completed_at"]),
        retrieval_cache=parse_retrieval_cache_receipt(value.get("retrieval_cache")),
    )

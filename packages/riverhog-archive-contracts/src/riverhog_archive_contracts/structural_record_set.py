"""Page independent commitments for bounded archive structural record sets."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)
from sha256_continuation import CheckpointSHA256

RECORD_SET_FORMAT = "riverhog-archive-record-set/v1"
RECORD_PAGE_FORMAT = "riverhog-archive-record-page/v1"
RECORD_BYTES_MAX = 1024 * 1024
PAGE_BYTES_MAX = 4 * 1024 * 1024
PAGE_RECORDS_MAX = 128
_DOMAIN = b"riverhog-archive-record-set/v1\x00"


@dataclass(frozen=True, slots=True)
class RecordSetRef:
    schema_id: str
    record_count: int
    records_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.schema_id, str)
            or not self.schema_id
            or len(self.schema_id.encode("utf-8")) > 256
        ):
            raise ValueError("structural record set schema identity is invalid")
        if type(self.record_count) is not int or self.record_count < 0:
            raise ValueError("structural record count is invalid")
        if len(self.records_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.records_sha256
        ):
            raise ValueError("structural record set digest is invalid")

    def to_mapping(self) -> dict[str, object]:
        return {
            "format": RECORD_SET_FORMAT,
            "schema_id": self.schema_id,
            "record_count": format_scalar("nonnegative", self.record_count),
            "records_sha256": self.records_sha256,
        }

    @classmethod
    def from_mapping(cls, value: object) -> RecordSetRef:
        if (
            not isinstance(value, dict)
            or set(value) != {"format", "schema_id", "record_count", "records_sha256"}
            or value["format"] != RECORD_SET_FORMAT
        ):
            raise ValueError("structural record set reference is invalid")
        return cls(
            schema_id=value["schema_id"],
            record_count=parse_scalar("nonnegative", value["record_count"]),
            records_sha256=value["records_sha256"],
        )


def _prefix(schema_id: str) -> bytes:
    if not isinstance(schema_id, str) or not schema_id or len(schema_id.encode("utf-8")) > 256:
        raise ValueError("structural record set schema identity is invalid")
    encoded = schema_id.encode("utf-8")
    return _DOMAIN + len(encoded).to_bytes(2, "big") + encoded


def _record(key: str, value: Mapping[str, Any], previous: bytes | None) -> tuple[bytes, bytes]:
    if not isinstance(key, str) or not key or len(key.encode("utf-8")) > 1024:
        raise ValueError("structural record key is invalid")
    encoded = key.encode("utf-8")
    if previous is not None and encoded <= previous:
        raise ValueError("structural record keys are not strictly ordered")
    raw = canonical_json_bytes({"key": key, "value": dict(value)})
    if len(raw) > RECORD_BYTES_MAX:
        raise ValueError("structural record exceeds its byte bound")
    return encoded, len(raw).to_bytes(4, "big") + raw


@dataclass(frozen=True, slots=True)
class RecordSetCheckpoint:
    """Resume the existing ordered commitment from trusted owner work state.

    Source binding and the mandatory terminal comparison belong to the consumer.
    This checkpoint retains one key, one SHA-256 continuation and an exact count.
    """

    schema_id: str
    count: int
    previous_key: str | None
    hash_state: str

    def __post_init__(self) -> None:
        prefix = _prefix(self.schema_id)
        digest = CheckpointSHA256.from_state(self.hash_state)
        if type(self.count) is not int or not 0 <= self.count <= digest.total_bytes:
            raise ValueError("structural record checkpoint count is invalid")
        if self.count == 0:
            if (
                self.previous_key is not None
                or self.hash_state != CheckpointSHA256(prefix).export_state()
            ):
                raise ValueError("empty structural record checkpoint differs from its schema")
        elif (
            not isinstance(self.previous_key, str)
            or not self.previous_key
            or len(self.previous_key.encode("utf-8")) > 1024
            or digest.total_bytes < len(prefix) + self.count * 4
        ):
            raise ValueError("structural record checkpoint position is invalid")

    @classmethod
    def begin(cls, schema_id: str) -> RecordSetCheckpoint:
        return cls(schema_id, 0, None, CheckpointSHA256(_prefix(schema_id)).export_state())

    def append(self, key: str, value: Mapping[str, Any]) -> RecordSetCheckpoint:
        previous = None if self.previous_key is None else self.previous_key.encode("utf-8")
        _, raw = _record(key, value, previous)
        digest = CheckpointSHA256.from_state(self.hash_state)
        digest.update_bytes(raw)
        return type(self)(self.schema_id, self.count + 1, key, digest.export_state())

    def ref(self) -> RecordSetRef:
        digest = CheckpointSHA256.from_state(self.hash_state)
        digest.update_bytes(b"\x00count:" + str(self.count).encode("ascii"))
        return RecordSetRef(self.schema_id, self.count, digest.hexdigest())

    def to_json_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "format": "riverhog-record-set-checkpoint/v1",
                "schema_id": self.schema_id,
                "count": format_scalar("nonnegative", self.count),
                "previous_key": self.previous_key,
                "hash_state": self.hash_state,
            }
        )

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> RecordSetCheckpoint:
        # Schema <=256 UTF-8 octets, key <=1024, and the fixed SHA-256 state.
        if type(raw) is not bytes or len(raw) > 16 * 1024:
            raise ValueError("structural record checkpoint exceeds its codec-derived bound")
        value: Any = require_canonical_json(raw)
        if (
            not isinstance(value, dict)
            or set(value) != {"format", "schema_id", "count", "previous_key", "hash_state"}
            or value["format"] != "riverhog-record-set-checkpoint/v1"
        ):
            raise ValueError("structural record checkpoint fields are invalid")
        return cls(
            value["schema_id"],
            parse_scalar("nonnegative", value["count"]),
            value["previous_key"],
            value["hash_state"],
        )


class RecordSetCommitment:
    """Verify an ordered stream without retaining all records or page boundaries."""

    def __init__(self, schema_id: str) -> None:
        self.schema_id = schema_id
        self._digest = hashlib.sha256(_prefix(schema_id))
        self._last_key: bytes | None = None
        self.count = 0

    def update(self, key: str, value: Mapping[str, Any]) -> None:
        encoded_key, raw = _record(key, value, self._last_key)
        self._digest.update(raw)
        self._last_key = encoded_key
        self.count += 1

    def ref(self) -> RecordSetRef:
        digest = self._digest.copy()
        digest.update(b"\x00count:" + str(self.count).encode("ascii"))
        return RecordSetRef(self.schema_id, self.count, digest.hexdigest())


@dataclass(frozen=True, slots=True)
class RecordPage:
    authority: RecordSetRef
    ordinal: int
    records: tuple[dict[str, Any], ...]
    terminal: bool

    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or self.ordinal < 0:
            raise ValueError("structural record page ordinal is invalid")
        if (
            type(self.terminal) is not bool
            or self.terminal == bool(self.records)
            or len(self.records) > PAGE_RECORDS_MAX
        ):
            raise ValueError("structural record page shape is invalid")
        if len(self.to_json_bytes()) > PAGE_BYTES_MAX:
            raise ValueError("structural record page exceeds its byte bound")

    def to_mapping(self) -> dict[str, object]:
        return {
            "format": RECORD_PAGE_FORMAT,
            "authority": self.authority.to_mapping(),
            "ordinal": format_scalar("nonnegative", self.ordinal),
            "records": list(self.records),
            "terminal": self.terminal,
        }

    def to_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_mapping())

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> RecordPage:
        if len(raw) > PAGE_BYTES_MAX:
            raise ValueError("structural record page exceeds its byte bound")
        value: Any = require_canonical_json(raw)
        if (
            not isinstance(value, dict)
            or set(value) != {"format", "authority", "ordinal", "records", "terminal"}
            or value["format"] != RECORD_PAGE_FORMAT
        ):
            raise ValueError("structural record page fields are invalid")
        rows = value["records"]
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("structural record page rows are invalid")
        return cls(
            authority=RecordSetRef.from_mapping(value["authority"]),
            ordinal=parse_scalar("nonnegative", value["ordinal"]),
            records=tuple(rows),
            terminal=value["terminal"],
        )


def verify_record_pages(authority: RecordSetRef, pages: Iterable[RecordPage]) -> None:
    """Verify a bounded page stream before its staged records become visible."""

    commitment = RecordSetCommitment(authority.schema_id)
    terminal = False
    for ordinal, page in enumerate(pages):
        if terminal or page.ordinal != ordinal or page.authority != authority:
            raise ValueError("structural record page sequence or authority changed")
        for row in page.records:
            if set(row) != {"key", "value"} or not isinstance(row["value"], dict):
                raise ValueError("structural record fields are invalid")
            commitment.update(row["key"], row["value"])
        terminal = page.terminal
    if not terminal or commitment.ref() != authority:
        raise ValueError("structural record set is incomplete or changed")


__all__ = [
    "PAGE_BYTES_MAX",
    "PAGE_RECORDS_MAX",
    "RECORD_BYTES_MAX",
    "RECORD_PAGE_FORMAT",
    "RECORD_SET_FORMAT",
    "RecordPage",
    "RecordSetCheckpoint",
    "RecordSetCommitment",
    "RecordSetRef",
    "verify_record_pages",
]

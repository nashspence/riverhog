"""Exact private input bindings for retained journal replay attempts."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass

from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance_contracts import require_canonical_uuid_urn

REPLAY_BINDING_ALGORITHM = "riverhog-journal-replay-binding/v1"


@dataclass(frozen=True, slots=True)
class JournalReplayBinding:
    attempt_id: str
    directory_id: str
    journal_id: str
    source_bytes: str
    source_sha256: str
    source_version: str
    frame_count: str
    catalog_sha256: str
    require_profiles: bool

    def __post_init__(self) -> None:
        for value in (self.attempt_id, self.directory_id):
            if not isinstance(value, str) or not value:
                raise ValueError("replay binding requires exact opaque owned attempt/directory IDs")
        require_canonical_uuid_urn(self.journal_id)
        for value in (self.source_sha256, self.catalog_sha256):
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError("replay binding requires exact lowercase digests")
        for value in (self.source_bytes, self.source_version, self.frame_count):
            if not isinstance(value, str) or re.fullmatch(r"0|[1-9][0-9]*", value) is None:
                raise ValueError("replay binding requires canonical owning scalar text")
        if type(self.require_profiles) is not bool:
            raise ValueError("replay binding requires an explicit profile policy")

    def to_json(self) -> str:
        return canonical_json_bytes(
            {
                "algorithm": REPLAY_BINDING_ALGORITHM,
                "mode": "complete-finite-journal",
                **asdict(self),
            }
        ).decode()

    @classmethod
    def from_json(cls, raw: str) -> JournalReplayBinding:
        try:
            value = json.loads(raw)
            if (
                not isinstance(value, dict)
                or value.pop("algorithm") != REPLAY_BINDING_ALGORITHM
                or value.pop("mode") != "complete-finite-journal"
            ):
                raise ValueError("invalid replay binding protocol")
            binding = cls(**value)
            if binding.to_json() != raw:
                raise ValueError("noncanonical replay binding")
            return binding
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise RuntimeError("invalid retained journal replay binding") from exc

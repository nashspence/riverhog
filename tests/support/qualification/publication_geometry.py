"""Explicit input dimensions for real collection publication witnesses."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

WORKLOAD_FORMAT = "riverhog-publication-workload/v1"


@dataclass(frozen=True)
class PublicationGeometry:
    payload_sizes: tuple[int, ...]
    shared_root_members: int
    retained_journals: int = 0
    required_journals: int = 0
    shared_claim_members: int | None = None

    def __post_init__(self) -> None:
        if self.shared_claim_members is not None and (
            type(self.shared_claim_members) is not int
            or not 1 <= self.shared_claim_members <= len(self.payload_sizes)
        ):
            raise ValueError("shared documentary claims must select existing members")
        if not self.payload_sizes or any(type(n) is not int or n < 0 for n in self.payload_sizes):
            raise ValueError("payload sizes require a nonempty exact nonnegative extent")
        if type(self.shared_root_members) is not int or not 1 <= self.shared_root_members <= len(
            self.payload_sizes
        ):
            raise ValueError("shared root attachments must select existing members")
        if (
            type(self.retained_journals) is not int
            or type(self.required_journals) is not int
            or not 0 <= self.required_journals <= self.retained_journals
        ):
            raise ValueError("required journals must select from the retained journal pool")


def read_workload(path: Path) -> PublicationGeometry:
    value = json.loads(path.read_text(encoding="utf-8"))
    fields = {
        "format",
        "payload_sizes",
        "shared_root_members",
        "shared_claim_members",
        "retained_journals",
        "required_journals",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("publication workload fields are invalid")
    if value.pop("format") != WORKLOAD_FORMAT or not isinstance(value["payload_sizes"], list):
        raise ValueError("publication workload format or payload extent is invalid")
    value["payload_sizes"] = tuple(value["payload_sizes"])
    return PublicationGeometry(**value)


def write_payload(path: Path, *, member: int, size: int) -> None:
    """Generate exact source bytes with a fixed transfer buffer."""
    block = bytes([member % 251]) * (64 * 1024)
    with path.open("wb") as output:
        remaining = size
        while remaining:
            chunk = block[:remaining]
            output.write(chunk)
            remaining -= len(chunk)

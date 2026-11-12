"""Explicit input dimensions for real collection publication witnesses."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PublicationGeometry:
    payload_sizes: tuple[int, ...]
    shared_root_members: int
    retained_journals: int = 0
    required_journals: int = 0

    def __post_init__(self) -> None:
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


def write_payload(path: Path, *, member: int, size: int) -> None:
    """Generate exact source bytes with a fixed transfer buffer."""
    block = bytes([member % 251]) * (64 * 1024)
    with path.open("wb") as output:
        remaining = size
        while remaining:
            chunk = block[:remaining]
            output.write(chunk)
            remaining -= len(chunk)

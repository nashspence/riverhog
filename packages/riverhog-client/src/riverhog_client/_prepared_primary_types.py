"""Shared local state controls for exact prepared-primary byte files."""

from __future__ import annotations

import re
from dataclasses import dataclass

_FILE_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,199}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class PrimaryStateError(RuntimeError):
    """Prepared-primary state is unavailable or cannot be trusted."""


class PrimaryStateBusy(PrimaryStateError):
    """Another live process owns this construction."""


class PrimaryStateCapacity(PrimaryStateError):
    """Preparation resources cannot admit the current operation."""


def primary_file_name(value: str) -> str:
    if not isinstance(value, str) or _FILE_NAME.fullmatch(value) is None:
        raise ValueError("invalid prepared-primary file name")
    return value


@dataclass(frozen=True, slots=True)
class PrimaryByteFile:
    name: str
    bytes: int
    sha256: str

    def __post_init__(self) -> None:
        primary_file_name(self.name)
        if type(self.bytes) is not int or self.bytes < 0:
            raise ValueError("invalid prepared-primary file length")
        if not isinstance(self.sha256, str) or _SHA256.fullmatch(self.sha256) is None:
            raise ValueError("invalid prepared-primary file digest")


@dataclass(frozen=True, slots=True)
class PrimaryFileSpace:
    """Separate the logical file extent from filesystem-reported allocation."""

    end_bytes: int
    allocated_bytes: int

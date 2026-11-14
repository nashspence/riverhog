"""Explicit finite retained-call grants owned by cache population writes."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CacheWriteGrants:
    calls: int = 64
    control_bytes: int = 8 * 1024 * 1024
    result_bytes: int = 64 * 1024
    cleanup_bytes: int = 64 * 1024

    def __post_init__(self) -> None:
        for value in (self.calls, self.control_bytes, self.result_bytes, self.cleanup_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("cache write grants must be positive integers")

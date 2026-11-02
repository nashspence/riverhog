"""Explicit finite retained-call grants owned by cache population writes."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CacheWriteGrants:
    calls: int
    control_bytes: int
    result_bytes: int
    cleanup_bytes: int

    def __post_init__(self) -> None:
        for value in (self.calls, self.control_bytes, self.result_bytes, self.cleanup_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("cache write grants must be positive integers")

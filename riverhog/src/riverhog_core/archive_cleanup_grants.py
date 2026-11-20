"""Operator-owned retained cleanup capacity for one selected terminal."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArchiveCleanupGrants:
    """Outstanding reservations per selected terminal, never total archive limits."""

    calls: int = 4
    control_bytes: int = 64 * 1024
    provider_requests: int = 16
    in_flight: int = 8

    def __post_init__(self) -> None:
        for value in (self.calls, self.control_bytes, self.provider_requests, self.in_flight):
            if type(value) is not int or value < 1:
                raise ValueError("cleanup retention grants must be positive integers")

"""Operator-owned physical execution capacity for collection-upload work."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArchiveExecutionTuning:
    """Process-local slots, independently reserved for forward and terminal work.

    Retained constructions share one budget across payload and metadata actors.
    These limits do not replace durable effect, preparation or buffer accounting.
    A joined worker with an unconsumed result still occupies its execution slot.
    """

    forward_workers: int = 4
    terminal_workers: int = 2
    forward_workers_per_target: int = 2
    terminal_workers_per_target: int = 1
    retained_constructions: int = 16
    retained_constructions_per_target: int = 4
    receive_poll_interval_ms: int = 100

    def __post_init__(self) -> None:
        for name in (
            "forward_workers",
            "terminal_workers",
            "forward_workers_per_target",
            "terminal_workers_per_target",
            "retained_constructions",
            "retained_constructions_per_target",
            "receive_poll_interval_ms",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"archive execution {name} must be a positive integer")

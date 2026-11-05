"""Operator-owned physical execution capacity for collection-upload work."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArchiveExecutionTuning:
    """Process-local slots, independently reserved for forward and terminal work.

    These limits do not replace durable effect, preparation or buffer accounting.
    A joined worker with an unconsumed result still occupies its execution slot.
    """

    forward_workers: int = 4
    terminal_workers: int = 2
    forward_workers_per_target: int = 2
    terminal_workers_per_target: int = 1

    def __post_init__(self) -> None:
        for name in (
            "forward_workers",
            "terminal_workers",
            "forward_workers_per_target",
            "terminal_workers_per_target",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"archive execution {name} must be a positive integer")

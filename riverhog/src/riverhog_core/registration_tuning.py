"""Operator grants for bounded registration execution and immutable readback."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from riverhog_core.incremental_emission import VolumeEmissionLimits


@dataclass(frozen=True, slots=True)
class RegistrationTuning:
    plans_per_step: int = 32
    coverage_rows_per_step: int = 50000
    metadata_bytes_per_step: int = 32 * 1024 * 1024
    input_transitions_per_step: int = 128
    summary_page_bytes: int = 64 * 1024
    execution_quantum: timedelta = timedelta(seconds=1)
    retry_delay: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        self.emission_limits()
        if type(self.summary_page_bytes) is not int or self.summary_page_bytes < 1:
            raise ValueError("registration summary_page_bytes must be positive")
        for name in ("execution_quantum", "retry_delay"):
            value = getattr(self, name)
            if not isinstance(value, timedelta) or value.total_seconds() <= 0:
                raise ValueError(f"registration {name} must be positive")

    def emission_limits(self) -> VolumeEmissionLimits:
        return VolumeEmissionLimits(
            self.plans_per_step,
            self.coverage_rows_per_step,
            self.metadata_bytes_per_step,
            self.input_transitions_per_step,
        )

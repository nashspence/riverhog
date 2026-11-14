"""Operator-owned bounds for one inventory or disposal execution turn."""

from __future__ import annotations

from dataclasses import dataclass

from riverhog_storage_adapter_protocol import MAX_CLEANUP_INVENTORY_PAGE_ITEMS


@dataclass(frozen=True, slots=True)
class CleanupExecutionTuning:
    """Per-call work bounds, separate from outstanding terminal reservations."""

    inventory_items: int = MAX_CLEANUP_INVENTORY_PAGE_ITEMS
    inventory_bytes: int = 4096
    provider_requests: int = 4
    control_bytes: int = 16384
    in_flight: int = 1

    def __post_init__(self) -> None:
        for value in (
            self.inventory_items,
            self.inventory_bytes,
            self.provider_requests,
            self.control_bytes,
            self.in_flight,
        ):
            if type(value) is not int or value < 1:
                raise ValueError("cleanup execution grants must be positive integers")
        if self.inventory_items > MAX_CLEANUP_INVENTORY_PAGE_ITEMS:
            raise ValueError("cleanup inventory exceeds its protocol page bound")

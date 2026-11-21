"""Bounded typed control received by the original Native private epoch."""

from __future__ import annotations

from typing import Protocol

from ._native_epoch import EpochContext


class _EncodedControl(Protocol):
    def to_json(self) -> str: ...


class ChildControl[State: _EncodedControl]:
    """Keep one received child value; readers still verify their fresh fences."""

    def __init__(self, epoch: EpochContext) -> None:
        self.epoch = epoch
        self.state: State | None = None

    def get(self, encoded: str) -> State | None:
        self.epoch.require_usable()
        state = self.state
        return state if state is not None and state.to_json() == encoded else None

    def receive(self, state: State) -> None:
        self.epoch.require_receiving(self.epoch.db)
        self.state = state

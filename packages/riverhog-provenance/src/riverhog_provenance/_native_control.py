"""Bounded typed child control received by the original Native conductor."""

from __future__ import annotations

from typing import Protocol

from ._native_conductor import LiveControl


class _EncodedControl(Protocol):
    def to_json(self) -> str: ...


class ChildControl[State: _EncodedControl](LiveControl[State]):
    """Reuse received values only after their owner's fresh storage fences."""

    def get(self, encoded: str) -> State | None:
        self.epoch.require_usable()
        state = self.state
        return state if state is not None and state.to_json() == encoded else None

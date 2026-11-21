"""Original live Native control, separate from spillable semantic state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._native_epoch import EpochContext

type ControlIdentity = tuple[str | bytes | int, ...]


@dataclass(frozen=True, slots=True)
class _Received:
    identity: ControlIdentity
    state: object


class NativeConductor:
    """One current continuation per maintained Native control type.

    Replacing an attempt replaces its slot, rather than accumulating history.
    The number of slots follows the fixed control types, not archive extent.
    Variable semantic rows and source bodies retain their spillable owners.
    A live value establishes no current authorization or recovery promise.
    """

    def __init__(self) -> None:
        self._received: dict[type[object], _Received] = {}

    def _get[State](self, state_type: type[State], identity: ControlIdentity) -> State | None:
        received = self._received.get(state_type)
        if received is None or received.identity != identity:
            return None
        state = received.state
        if type(state) is not state_type:
            raise RuntimeError("Native conductor lost its typed continuation")
        return state

    def _put[State](self, state_type: type[State], identity: ControlIdentity, state: State) -> None:
        if type(state) is not state_type:
            raise TypeError("Native control requires its exact continuation type")
        self._received[state_type] = _Received(identity, state)


class LiveControl[State]:
    """Bind one typed continuation to its original executor and exact inputs."""

    def __init__(
        self, epoch: EpochContext, state_type: type[State], identity: ControlIdentity
    ) -> None:
        self.epoch, self.state_type, self.identity = epoch, state_type, identity

    @property
    def state(self) -> State | None:
        # Reading received state is also useful for diagnosing a failed epoch;
        # execution still requires the original usable/freshly fenced owner.
        return self.epoch.conductor._get(self.state_type, self.identity)

    def observe(self, state: State) -> None:
        """Retain an already validated detached control observation."""
        self.epoch.require_quiescent()
        self.epoch.conductor._put(self.state_type, self.identity, state)

    def receive(self, state: State) -> None:
        """Install pending control only after this original commit returned."""
        self.epoch.require_receiving(self.epoch.db)
        self.epoch.conductor._put(self.state_type, self.identity, state)

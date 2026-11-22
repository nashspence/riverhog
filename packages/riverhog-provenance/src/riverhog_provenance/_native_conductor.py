"""Original live Native control, separate from spillable semantic state."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._native_epoch import EpochContext

type ControlIdentity = tuple[str | bytes | int, ...]


class ControlSlot(Enum):
    """Finite source-owned roles, independent of attempts and archive extent."""

    PARENT = "parent"
    PREFIX = "prefix"
    INPUT = "input"
    PREPARATION = "preparation"
    STAGING = "staging"
    TARGETS = "targets"
    RETIREMENT = "retirement"
    SEED = "seed"
    GENERIC = "generic"
    FINISH = "finish"
    DERIVATION_EDGE = "derivation-edge"
    SPECIALIZATION_EDGE = "specialization-edge"
    DERIVATION_GRAPH = "derivation-graph"
    SPECIALIZATION_GRAPH = "specialization-graph"


@cache
def _control_types() -> dict[ControlSlot, type[object]]:
    # Resolve the finite source types after their modules have initialized.
    # No caller-defined type or slot can extend this control universe.
    from ._assertion_staging import StageState
    from ._correction_projection import CorrectionState
    from ._correction_targets import TargetState
    from ._entry_preparation import PreparationState
    from ._generic_semantics import GenericState
    from ._journal_prefix import PrefixState
    from ._online_dag import _Snapshot
    from ._semantic_edge import EdgeState
    from ._semantic_finish import FinishState
    from ._session_checkpoint import ValidationSnapshot
    from ._session_input import _InputState

    return {
        ControlSlot.PARENT: ValidationSnapshot,
        ControlSlot.PREFIX: PrefixState,
        ControlSlot.INPUT: _InputState,
        ControlSlot.PREPARATION: PreparationState,
        ControlSlot.STAGING: StageState,
        ControlSlot.TARGETS: TargetState,
        ControlSlot.RETIREMENT: CorrectionState,
        ControlSlot.SEED: CorrectionState,
        ControlSlot.GENERIC: GenericState,
        ControlSlot.FINISH: FinishState,
        ControlSlot.DERIVATION_EDGE: EdgeState,
        ControlSlot.SPECIALIZATION_EDGE: EdgeState,
        ControlSlot.DERIVATION_GRAPH: _Snapshot,
        ControlSlot.SPECIALIZATION_GRAPH: _Snapshot,
    }


def _require_slot[State](slot: ControlSlot, state_type: type[State]) -> None:
    if type(slot) is not ControlSlot or _control_types()[slot] is not state_type:
        raise TypeError("Native control requires its source-owned slot and exact continuation type")


@dataclass(frozen=True, slots=True)
class _Received:
    identity: ControlIdentity
    state: object


class NativeConductor:
    """One current continuation per finite Native control role.

    Replacing an attempt replaces its slot, rather than accumulating history.
    Separate correction operations and graph kinds retain separate original
    values even when they share a Python type. Variable semantic rows and
    source bodies retain their spillable owners. A live value establishes no
    current authorization or recovery promise.
    """

    def __init__(self) -> None:
        self._received: dict[ControlSlot, _Received] = {}

    def _get[State](
        self, slot: ControlSlot, state_type: type[State], identity: ControlIdentity
    ) -> State | None:
        _require_slot(slot, state_type)
        received = self._received.get(slot)
        if received is None or received.identity != identity:
            return None
        state = received.state
        if type(state) is not state_type:
            raise RuntimeError("Native conductor lost its typed continuation")
        return state

    def _put[State](
        self, slot: ControlSlot, state_type: type[State], identity: ControlIdentity, state: State
    ) -> None:
        _require_slot(slot, state_type)
        if type(state) is not state_type:
            raise TypeError("Native control requires its exact continuation type")
        self._received[slot] = _Received(identity, state)


class LiveControl[State]:
    """Bind one typed continuation to its original executor and exact inputs."""

    def __init__(
        self,
        epoch: EpochContext,
        state_type: type[State],
        identity: ControlIdentity,
        *,
        slot: ControlSlot,
    ) -> None:
        _require_slot(slot, state_type)
        self.epoch, self.state_type, self.identity, self.slot = epoch, state_type, identity, slot

    @property
    def state(self) -> State | None:
        # Reading received state is also useful for diagnosing a failed epoch;
        # execution still requires the original usable/freshly fenced owner.
        return self.epoch.conductor._get(self.slot, self.state_type, self.identity)

    def observe(self, state: State) -> None:
        """Retain an already validated detached control observation."""
        self.epoch.require_quiescent()
        self.epoch.conductor._put(self.slot, self.state_type, self.identity, state)

    def receive(self, state: State) -> None:
        """Install pending control only after this original commit returned."""
        self.epoch.require_receiving(self.epoch.db)
        self.epoch.conductor._put(self.slot, self.state_type, self.identity, state)

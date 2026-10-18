"""One bounded step through retained semantic planning state."""

from dataclasses import dataclass
from typing import Literal

from stove0_protocol import BranchSetDecision, WorkIdentity
from stove0_protocol.observation_evidence import ObservationQuestion

from stove0_core.work_state import WorkInapplicable, WorkNoAction


@dataclass(frozen=True, slots=True)
class PlanningProgress:
    state: Literal["pending", "question", "ready", "no-output", "inapplicable"]
    work: WorkIdentity
    question: ObservationQuestion | None = None
    decision: BranchSetDecision | None = None
    outcome: WorkInapplicable | WorkNoAction | None = None

"""Original Native generation children retained by the composing upload slot."""

from __future__ import annotations

from threading import Lock

from riverhog_growth_contracts import (
    COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_provenance import (
    JournalConstruction,
    JournalImageCapture,
    JournalImageRestore,
    JournalValidationSession,
    NativeGenerationStorage,
)

from riverhog_core.journal_recovery_frontier import JournalRecoveryContext


def _receiving_growth() -> Declaration:
    native = growth_binding(JournalValidationSession.close).declaration
    limits = {"closed_databases": 2, "closed_files": 10, "release_callbacks": 2}
    return Declaration(
        id="riverhog-server.journal-generation-resource-receiving.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.journal_generation_resources.JournalGenerationResources.close_step",
        ),
        scope="one original separately admitted Native generation and its capture/restore children",
        starts_at="receive the original generation children outside the owner pool mutex",
        ends_at="confirm closure or retain the identical pending/uncertain original children",
        dimensions=(Dimension("units", COUNT, "One original bounded receiving invocation."),),
        bounds=(
            Bound(
                "receiving_steps",
                COUNT,
                "equal",
                Expression("dimension", name="units"),
                "Observe the one actual receiving method invocation.",
            ),
            Bound(
                "receiving_steps_limit",
                COUNT,
                "upper",
                Expression("constant", value=1, unit=COUNT),
                "One call receives at most the fixed child group once.",
            ),
            *(
                Bound(
                    bound.metric,
                    bound.unit,
                    "upper" if limits.get(bound.metric, 0) else "equal",
                    Expression("constant", value=limits.get(bound.metric, 0), unit=bound.unit),
                    (
                        "At most the generation and capture-retained working connections; "
                        "five child file records plus five original workspace descriptors; "
                        "two final-storage callbacks. No source, SQL, payload or path work."
                    ),
                )
                for bound in native.bounds
            ),
            *(
                Bound(
                    name,
                    COUNT,
                    "upper",
                    Expression("constant", value=1, unit=COUNT),
                    "Visit each of the three fixed original children at most once per step.",
                )
                for name in (
                    "capture_close_calls",
                    "restore_close_calls",
                    "construction_close_calls",
                )
            ),
        ),
        conditions=(
            "The existing slot retains this passive frame before optional construction effects.",
            "Capture and restore children belong to the exact working and generation storage.",
            "The slot serializes admission and receiving outside the pool mutex.",
            "Workspace release uses the slot's existing fixed descriptor custody callback.",
        ),
        invariants=(
            "An independent generation reader keeps its original construction pending.",
            "A failed close preserves every original child and forbids replacement admission.",
            "Resource receiving grants no generation retirement, selection or cost settlement.",
        ),
        limitations=(
            "Logical resource counts exclude physical database close I/O, CPU and waiting time.",
            "Actual worker join and surviving accounting receiving remain with the application.",
        ),
    )


class JournalGenerationResources:
    """One bounded passive child group, separate from the working constructor.

    This frame owns actual handles, not a grant or a selected recovery frontier.
    The composing caller keeps its accounting original through known settlement.
    Closing a storage facade never receives another independently borrowed reader.
    """

    def __init__(
        self,
        construction: JournalConstruction,
        working: JournalValidationSession,
        context: JournalRecoveryContext,
    ) -> None:
        if not isinstance(construction, JournalConstruction) or construction.phase != "new":
            raise TypeError("generation requires its own new original Native construction")
        if not isinstance(context, JournalRecoveryContext):
            raise TypeError("generation requires its exact original Core context")
        self.context = context
        self.construction = construction
        self.working = working
        self.storage: NativeGenerationStorage | None = None
        self.capture: JournalImageCapture | None = None
        self.restore: JournalImageRestore | None = None
        self._closing = False
        self._gate = Lock()

    def retain_storage(self, storage: NativeGenerationStorage) -> None:
        if (
            self._closing
            or self.storage is not None
            or not isinstance(storage, NativeGenerationStorage)
            or self.construction.storage is not storage
        ):
            raise RuntimeError("generation must retain its same original Native storage")
        self.storage = storage

    def retain_capture(self, capture: JournalImageCapture) -> None:
        if (
            self._closing
            or self.capture is not None
            or not isinstance(capture, JournalImageCapture)
            or capture.phase != "new"
            or capture.session is not self.working
        ):
            raise RuntimeError("generation requires its original passive working capture")
        self.capture = capture

    def retain_restore(self, restore: JournalImageRestore) -> None:
        if (
            self._closing
            or self.restore is not None
            or not isinstance(restore, JournalImageRestore)
            or restore.phase != "new"
            or restore.storage is not self.storage
            or self.storage is None
        ):
            raise RuntimeError("generation requires its original passive storage restore")
        self.restore = restore

    @property
    def dependents_received(self) -> bool:
        return (
            self.construction.dependents_received
            and (self.capture is None or self.capture.received)
            and (self.restore is None or self.restore.received)
        )

    @property
    def received(self) -> bool:
        return self.construction.received and self.dependents_received

    @declare_growth(_receiving_growth())
    def close_step(self) -> bool:
        if not self._gate.acquire(blocking=False):
            raise BlockingIOError("original Native generation receiving is executing")
        try:
            self._closing = True
            if self.capture is not None:
                self.capture.close()
            if self.restore is not None:
                self.restore.close()
            return self.construction.close_step()
        finally:
            self._gate.release()

"""Caller-retained native origins across a live lease body and independent references."""

from __future__ import annotations

from threading import Lock

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from riverhog_core.retained_workspace_handles import (
    RetainedWorkspaceHandles,
    RetainedWorkspaceResource,
)


def _receiving_declaration(operation: str) -> Declaration:
    def constant(value: int, unit: tuple[tuple[str, int], ...] = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    return Declaration(
        id=f"riverhog-server.validation-lease-native-{operation}.growth/v1",
        authority="riverhog-server",
        subjects=(
            f"riverhog_core.journal_validation_workspace_lease.ValidationWorkspaceLease.{operation}",
        ),
        scope="one original live lease frame receiving call after actual caller I/O exits",
        starts_at="receive the same caller-retained native frame",
        ends_at=(
            "positive original receiving, active-dependency refusal or retained unknown outcome"
        ),
        dimensions=(
            Dimension(
                "unrelated_files", COUNT, "Independent namespace population, never enumerated."
            ),
            Dimension(
                "independent_references", COUNT, "Actual duplicate references, never enumerated."
            ),
        ),
        bounds=(
            Bound("open_calls", COUNT, "equal", constant(0), "Receiving opens no pathname."),
            Bound("dup_calls", COUNT, "equal", constant(0), "Receiving duplicates no resource."),
            Bound("fstat_calls", COUNT, "equal", constant(0), "Identity cannot certify receiving."),
            Bound("close_calls", COUNT, "upper", constant(6), "Six pre-retained native origins."),
            Bound(
                "control_octets",
                OCTETS,
                "equal",
                constant(0, OCTETS),
                "No encoded control operand is scanned during receiving.",
            ),
        ),
        conditions=(
            "The body executor calls finish only after its actual I/O has exited.",
            "The original admission remains charged until received and actual executor join.",
            "Supported native POSIX ownership and explicit borrow rules remain mandatory.",
        ),
        invariants=(
            "Body exit never closes an independently owned duplicate.",
            "Unknown native outcomes retain all original dependencies without an integer retry.",
            "Independent jobs do not wait on this frame's reference population.",
        ),
        limitations=(
            (
                "Acquisition, duplicate creation and independent reference receiving "
                "are separately owned."
            ),
            "Native syscall counts exclude filesystem latency and kernel-internal work.",
        ),
    )


class ValidationWorkspaceLease:
    """Retain six original records before any live lease namespace effect.

    The application retains this frame and its original reservation before
    acquisition. Logical body exit releases the original lease, while duplicate
    references may keep its directory and root alive. Explicit close refuses
    active execution and retains failed callbacks until actual receiving.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._handles = RetainedWorkspaceHandles()
        self.root = RetainedWorkspaceResource("directory")
        self.coordinator = RetainedWorkspaceResource("file")
        self.directory = RetainedWorkspaceResource("directory")
        self.existing_lease = RetainedWorkspaceResource("file")
        self.created_lease = RetainedWorkspaceResource("file")
        self.parent_sync = RetainedWorkspaceResource("directory")
        self.records = (
            self.root,
            self.coordinator,
            self.directory,
            self.existing_lease,
            self.created_lease,
            self.parent_sync,
        )
        for original in self.records:
            self._handles.callback(original.close)
        self.lease = self.existing_lease
        self._started = False
        self._active = False
        self._closing = False

    def begin(self) -> None:
        if not self._lock.acquire(blocking=False):
            raise BlockingIOError("workspace receiving is already executing")
        try:
            if self._started or self._closing:
                raise RuntimeError("the original workspace lease is single-attempt")
            self._started = self._active = True
        finally:
            self._lock.release()

    def select_created_lease(self) -> RetainedWorkspaceResource:
        if not self._active or self.existing_lease.owner.state != "absent":
            raise RuntimeError("initial lease creation requires definite original absence")
        self.lease = self.created_lease
        return self.lease

    @property
    def received(self) -> bool:
        return self._closing and not self._active and not self._handles._callbacks

    @declare_growth(_receiving_declaration("finish"))
    def finish(self) -> None:
        """End the actual body; preserve parent receiving for independent references."""
        if not self._lock.acquire(blocking=False):
            raise BlockingIOError("workspace receiving is already executing")
        try:
            if not self._started:
                raise RuntimeError("the original workspace lease never entered")
            self._active = False
            self._closing = True
            self.lease.close()
            if self.directory.owner.metrics["children"]:
                return
            self._handles.close()
        finally:
            self._lock.release()

    @declare_growth(_receiving_declaration("close"))
    def close(self) -> None:
        if not self._lock.acquire(blocking=False):
            raise BlockingIOError("workspace receiving is already executing")
        try:
            if self._active:
                raise BlockingIOError("the original lease body has not exited")
            self._closing = True
            self._handles.close()
        finally:
            self._lock.release()

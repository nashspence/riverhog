"""Independent references to an actual held validation kernel lease."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.journal_validation_workspace_reference_growth import (
    declare_growth_for_workspace_reference,
)
from riverhog_core.retained_workspace_handles import RetainedWorkspaceResource

if TYPE_CHECKING:
    from riverhog_core.journal_validation_workspace import ValidationWorkspaceCustody


class ValidationWorkspaceReference:
    """Keep one actual lease until its original native resource confirms close.

    Callers retain this object before acquisition can fail, and release it only
    after their independently owned Native storage has confirmed closure.
    """

    def __init__(self) -> None:
        self._resource = RetainedWorkspaceResource("file")
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        self._resource.close()
        self._closed = True


def retain_validation_workspace(
    custody: ValidationWorkspaceCustody,
    *,
    on_reference: Callable[[ValidationWorkspaceReference], None],
) -> ValidationWorkspaceReference:
    """Duplicate held kernel custody; an identity alone grants no reference.

    The caller's retention callback runs before any descriptor acquisition and
    must keep the exact reference reachable until close confirms, including an
    unconfirmed constructor-cleanup result.
    """
    if not custody.verify_current():
        raise LostJournalValidationWorkspace("validation custody is no longer held")
    reference = ValidationWorkspaceReference()
    try:
        on_reference(reference)
        if reference.closed:
            raise LostJournalValidationWorkspace("validation reference closed before acquisition")

        reference._resource.duplicate(custody._resources[2])
        if not custody.verify_current():
            raise LostJournalValidationWorkspace("validation custody changed during retention")
    except BaseException:
        reference.close()
        raise
    return reference


declare_growth_for_workspace_reference(retain_validation_workspace, ValidationWorkspaceReference)

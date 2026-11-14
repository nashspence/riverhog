"""Ordered independent references to two actual validation workspaces."""

from __future__ import annotations

from riverhog_core.journal_validation_workspace_reference import ValidationWorkspaceReference
from riverhog_core.journal_validation_workspace_references_growth import (
    declare_growth_for_reference_group,
)


class JournalWorkspaceReferences:
    """Release the child before its parent; retain each unconfirmed reference."""

    def __init__(self) -> None:
        self.parent: ValidationWorkspaceReference | None = None
        self.child: ValidationWorkspaceReference | None = None

    def retain_parent(self, reference: ValidationWorkspaceReference) -> None:
        self.parent = reference

    def retain_child(self, reference: ValidationWorkspaceReference) -> None:
        self.child = reference

    def close(self) -> None:
        if self.child is not None:
            self.child.close()
            self.child = None
        if self.parent is not None:
            self.parent.close()
            self.parent = None


declare_growth_for_reference_group(JournalWorkspaceReferences)

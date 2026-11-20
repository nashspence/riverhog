"""Budgeted cleanup execution over one independently selected object target."""

from __future__ import annotations

from typing import Protocol

from riverhog_storage_adapter_protocol import CleanupExecutionResult, CleanupInvocation


class ObjectCleanupPort(Protocol):
    """Execute an admitted call; target identity conveys no disposal authority."""

    def execute_cleanup(self, invocation: CleanupInvocation) -> CleanupExecutionResult: ...

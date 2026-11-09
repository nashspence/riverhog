"""Retain the exact original cleanup attempt and its complete resource charge.

Scope owners validate current disposal rights and durable selection before
dispatch. This primitive neither selects objects nor grants those rights.
"""

from __future__ import annotations

import threading

from riverhog_canonical_json import canonical_json_bytes, format_scalar
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import (
    CleanupExecutionResult,
    CleanupInvocation,
    unknown_cleanup_execution,
    validate_cleanup_execution,
)

from riverhog_core.ports.object_cleanup import ObjectCleanupPort


class RetainedCleanupAttempt:
    """The original live one-use right; cold catalog rows cannot recreate it."""

    def __init__(
        self,
        collection_id: int,
        operation_id: str,
        invocation: CleanupInvocation,
        nonce: str,
    ) -> None:
        self.collection_id = collection_id
        self.operation_id = operation_id
        self.invocation = invocation
        self._nonce = nonce
        self._lock = threading.Lock()
        self._started = False
        self._response: CleanupExecutionResult | None = None

    def _dispatch(self, target: ObjectCleanupPort) -> CleanupExecutionResult:
        with self._lock:
            if self._response is not None:
                return CleanupExecutionResult.model_validate_json(
                    self._response.model_dump_json(exclude_none=True)
                )
            if self._started:
                raise Conflict("original cleanup attempt is still running or unaccounted")
            self._started = True
        try:
            response = target.execute_cleanup(self.invocation)
            response = CleanupExecutionResult.model_validate_json(
                response.model_dump_json(exclude_none=True)
            )
            validate_cleanup_execution(self.invocation, response)
        except Exception:
            response = unknown_cleanup_execution(self.invocation)
        with self._lock:
            self._response = response
        return response


def cleanup_attempt_bytes(invocation: CleanupInvocation, operation_id: str) -> int:
    request = invocation.request
    # Persist invocation/response JSON with the same omission policy as
    # their protocol grant; optional nulls must not escape byte accounting.
    reservation = request.maximum_control_bytes
    while True:
        header = {
            "id": invocation.call.call_id,
            "terminal_id": operation_id,
            "page_sha256": "0" * 64,
            "dispatch_sha256": "0" * 64,
            "unresolved": False,
            "control_bytes": format_scalar("nonnegative", reservation),
            "provider_requests": format_scalar("nonnegative", request.maximum_provider_requests),
            "in_flight": format_scalar("nonnegative", request.maximum_in_flight),
        }
        total = request.maximum_control_bytes + len(canonical_json_bytes(header))
        if total == reservation:
            return total
        reservation = total

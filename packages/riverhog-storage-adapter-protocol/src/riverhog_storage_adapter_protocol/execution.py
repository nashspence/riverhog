"""Synchronous invocation support for adapter-owned execution profiles."""

from __future__ import annotations

from typing import Literal

from riverhog_storage_adapter_protocol.protocol import (
    BeginWriteOperation,
    BinaryContent,
    CompletedObjectReceipt,
    CompleteWriteOperation,
    ImmutableObjectReceipt,
    LookupWriteOperation,
    StorageAdapterPort,
    WriteExecutionResult,
    WriteInvocation,
    WritePartOperation,
    WriteSegmentReceipt,
    WriteSession,
    _ContentValidation,
    unknown_write_execution,
    validate_write_execution,
    validated_storage_adapter,
)


def execute_write_invocation(
    adapter: StorageAdapterPort,
    invocation: WriteInvocation,
    *,
    content: BinaryContent = b"",
    profile: Literal["joined-single-attempt", "unaccounted"],
) -> WriteExecutionResult:
    """Run one synchronous invocation under the effect owner's selected profile.

    Only an owner that accounts for every delegated attempt selects the joined
    profile. It is never selected from request fields. Exceptions remain unknown,
    including errors that follow a provider effect or incomplete body consumption.
    """
    try:
        invocation._require_exact_request()
        port = validated_storage_adapter(adapter)
        if port.descriptor().storage_incarnation_id != invocation.call.target_incarnation:
            return unknown_write_execution(invocation)
        operation = invocation.operation
        validation = _ContentValidation(content, expected_bytes=invocation.stored_bytes)
        body = validation.content()
        if invocation.stored_bytes == 0:
            for _chunk in body:
                pass
        result: (
            WriteSession
            | WriteSegmentReceipt
            | CompletedObjectReceipt
            | ImmutableObjectReceipt
            | None
        )
        if isinstance(operation, BeginWriteOperation):
            result = port.begin_write(operation.request)
            outcome: Literal["session", "part", "completed", "no_effect"] = "session"
        elif isinstance(operation, WritePartOperation):
            request = operation.request
            result = port.write_segment(
                session=request.session,
                number=request.number,
                stored_bytes=request.stored_bytes,
                content=body,
            )
            outcome = "part"
        elif isinstance(operation, CompleteWriteOperation):
            result = port.complete_write(operation.request)
            outcome = "completed"
        elif isinstance(operation, LookupWriteOperation):
            result = port.find_completed_write(operation.request)
            outcome = "completed" if result is not None else "no_effect"
        else:
            result = port.put_small_object(operation.request, body)
            outcome = "completed"
        validation.require_complete()
        if isinstance(operation, WritePartOperation) and (
            validation.digest.hexdigest() != operation.stored_sha256
        ):
            return unknown_write_execution(invocation)
        if outcome == "no_effect" and profile != "joined-single-attempt":
            return unknown_write_execution(invocation)
        response = WriteExecutionResult(
            call=invocation.call,
            execution="closed" if profile == "joined-single-attempt" else "unknown",
            outcome=outcome,
            result=result,
        )
        validate_write_execution(invocation, response)
        return response
    except Exception:
        return unknown_write_execution(invocation)


__all__ = ["execute_write_invocation"]

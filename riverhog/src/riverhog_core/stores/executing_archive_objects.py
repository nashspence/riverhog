"""Translate resumable calls through the workflow's admitted operation executor."""

from __future__ import annotations

import hashlib

from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import (
    BeginWriteOperation,
    CompletedWriteLookupRequest,
    CompleteWriteOperation,
    LookupWriteOperation,
    ObjectPlacementPolicy,
    WriteCompleteRequest,
    WritePartOperation,
    WriteSegmentRequest,
    WriteStartRequest,
)
from riverhog_storage_adapter_protocol import CompletedObjectReceipt as AdapterCompletedObject
from riverhog_storage_adapter_protocol import WriteCompletionPrecondition as AdapterCompletion
from riverhog_storage_adapter_protocol import WriteSegmentReceipt as AdapterSegment
from riverhog_storage_adapter_protocol import WriteSession as AdapterSession

from riverhog_core.ports.archive_objects import (
    ArchiveResumableObjectStore,
    ArchiveWriteOperationExecutor,
    CompletedObjectReceipt,
    ResumableWriteConstraints,
    WriteCompletionPrecondition,
    WriteSegmentCursor,
    WriteSegmentPage,
    WriteSegmentReceipt,
    WriteSession,
)


def _session(session: WriteSession) -> AdapterSession:
    return AdapterSession(
        object_path=session.object_path,
        write_token=session.write_token,
        expected_bytes=session.expected_bytes,
    )


def _completed(result: AdapterCompletedObject) -> CompletedObjectReceipt:
    return CompletedObjectReceipt(
        object_path=result.object_path,
        revision=result.revision,
        entity_token=result.entity_token,
        bytes=result.stored_bytes,
        completed_at=result.completed_at,
    )


class ExecutingArchiveResumableObjectStore:
    """Read through the original store and send writes only through its admitted owner."""

    def __init__(
        self,
        source: ArchiveResumableObjectStore,
        executor: ArchiveWriteOperationExecutor,
        *,
        placement_policy: ObjectPlacementPolicy,
    ) -> None:
        self._source = source
        self._executor = executor
        self._placement_policy = placement_policy

    def write_constraints(self) -> ResumableWriteConstraints:
        return self._source.write_constraints()

    def begin_write(
        self,
        *,
        object_path: str,
        expected_bytes: int,
        content_type: str,
        metadata: dict[str, str],
    ) -> WriteSession:
        result = self._executor.execute_write_operation(
            BeginWriteOperation(
                request=WriteStartRequest(
                    object_path=object_path,
                    expected_bytes=expected_bytes,
                    content_type=content_type,
                    required_identity_assertions=metadata,
                    placement_policy=self._placement_policy,
                )
            )
        ).result
        if not isinstance(result, AdapterSession):
            raise Conflict("admitted archive begin has no original session")
        return WriteSession(result.object_path, result.write_token, result.expected_bytes)

    def write_segment(
        self, *, session: WriteSession, number: int, content: bytes
    ) -> WriteSegmentReceipt:
        result = self._executor.execute_write_operation(
            WritePartOperation(
                request=WriteSegmentRequest(
                    session=_session(session), number=number, stored_bytes=len(content)
                ),
                stored_sha256=hashlib.sha256(content).hexdigest(),
            ),
            content=content,
        ).result
        if not isinstance(result, AdapterSegment):
            raise Conflict("admitted archive segment has no exact receipt")
        return WriteSegmentReceipt(
            number=result.number,
            segment_token=result.segment_token,
            bytes=result.stored_bytes,
            sha256=result.stored_sha256,
        )

    def list_segments(
        self, *, session: WriteSession, cursor: WriteSegmentCursor
    ) -> WriteSegmentPage:
        return self._source.list_segments(session=session, cursor=cursor)

    def complete_write(
        self,
        *,
        session: WriteSession,
        completion: WriteCompletionPrecondition,
        expected_bytes: int,
        expected_content_type: str,
        expected_metadata: dict[str, str],
    ) -> CompletedObjectReceipt:
        result = self._executor.execute_write_operation(
            CompleteWriteOperation(
                request=WriteCompleteRequest(
                    session=_session(session),
                    completion=AdapterCompletion(
                        segment_count=completion.segment_count,
                        stored_bytes=completion.stored_bytes,
                        state_token=completion.state_token,
                    ),
                    expected_bytes=expected_bytes,
                    expected_content_type=expected_content_type,
                    required_identity_assertions=expected_metadata,
                    expected_placement_policy=self._placement_policy,
                )
            )
        ).result
        if not isinstance(result, AdapterCompletedObject):
            raise Conflict("admitted archive completion has no exact receipt")
        return _completed(result)

    def find_completed_write(
        self,
        *,
        object_path: str,
        expected_bytes: int,
        expected_content_type: str,
        expected_metadata: dict[str, str],
    ) -> CompletedObjectReceipt | None:
        response = self._executor.execute_write_operation(
            LookupWriteOperation(
                request=CompletedWriteLookupRequest(
                    object_path=object_path,
                    expected_bytes=expected_bytes,
                    expected_content_type=expected_content_type,
                    required_identity_assertions=expected_metadata,
                    expected_placement_policy=self._placement_policy,
                )
            )
        )
        if response.outcome == "no_effect":
            return None
        if not isinstance(response.result, AdapterCompletedObject):
            raise Conflict("admitted archive lookup has no exact receipt")
        return _completed(response.result)

    def abort_write(self, *, session: WriteSession) -> None:
        raise Conflict("forward archive write actor cannot dispose provider sessions")

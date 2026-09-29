"""Pathless reference sources. Native filesystem providers implement the same port."""

from __future__ import annotations

import copy
import io
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .common import new_id
from .constants import PROFILE
from .errors import ObservationError
from .model import (
    BinaryReadable,
    ObservationSession,
    SourceCapabilities,
    SourceEvidence,
    one_pass_evidence,
)


class BytesSource:
    """An immutable bounded Python bytes value, not an operating-system file."""

    def __init__(self, content: bytes) -> None:
        if type(content) is not bytes:
            raise TypeError("BytesSource requires immutable bytes")
        self._content = content
        self._snapshot_id = new_id()

    @contextmanager
    def open(self, *, observer_agent_id: str) -> Iterator[ObservationSession]:
        def finalize() -> SourceEvidence:
            return SourceEvidence(
                consistency={
                    "level": "immutable_snapshot",
                    "method_uri": PROFILE + "/methods/immutable-python-bytes",
                    "snapshot_identifier": {
                        "scheme": PROFILE + "/identifiers/in-memory-source-instance",
                        "value": {"kind": "text", "text": self._snapshot_id},
                        "scope": "global",
                    },
                }
            )

        @contextmanager
        def repeat() -> Iterator[BinaryReadable]:
            with io.BytesIO(self._content) as reader:
                yield reader

        with io.BytesIO(self._content) as reader:
            yield ObservationSession(
                reader=reader,
                extent={"kind": "whole_object"},
                occurrence_kind="opaque",
                capabilities=SourceCapabilities(
                    repeatable=True, seekable=True, stable_version_selection=True
                ),
                expected_length=len(self._content),
                finalize=finalize,
                repeat_reader=repeat,
            )


class StreamSource:
    """Consume one finite emission from a caller-owned reader exactly once.

    The caller declares that EOF delimits the emission. Source exceptions,
    size-limit exhaustion and premature EOF are failures, never valid truncated
    observations. This adapter does not seek, close, or obtain fileno().
    """

    def __init__(
        self,
        reader: BinaryReadable,
        *,
        expected_length: int | None = None,
        boundary_policy_uri: str = PROFILE + "/boundaries/emission-eof",
    ) -> None:
        if not isinstance(reader, BinaryReadable):
            raise TypeError("reader does not implement read(size)")
        if expected_length is not None and (
            type(expected_length) is not int or expected_length < 0
        ):
            raise ValueError("expected_length must be a nonnegative integer")
        self._reader = reader
        self._expected_length = expected_length
        self._boundary_policy_uri = boundary_policy_uri
        self._lock = threading.Lock()
        self._used = False

    @contextmanager
    def open(self, *, observer_agent_id: str) -> Iterator[ObservationSession]:
        with self._lock:
            if self._used:
                raise ObservationError("one-pass source has already been consumed or attempted")
            self._used = True
        yield ObservationSession(
            reader=self._reader,
            extent={"kind": "complete_emission", "boundary_policy_uri": self._boundary_policy_uri},
            occurrence_kind="stream_emission",
            capabilities=SourceCapabilities(),
            expected_length=self._expected_length,
            finalize=one_pass_evidence,
        )


class _SegmentReader:
    def __init__(self, reader: BinaryReadable, size: int) -> None:
        self.reader, self.remaining = reader, size

    def read(self, size: int = -1, /) -> bytes:
        if self.remaining == 0:
            return b""
        count = self.remaining if size < 0 else min(size, self.remaining)
        data = self.reader.read(count)
        if type(data) is not bytes or len(data) > count:
            raise ObservationError("source violates bounded read semantics")
        self.remaining -= len(data)
        return data


class SegmentSource(StreamSource):
    """Observe exactly a declared number of bytes without consuming the next segment."""

    def __init__(
        self,
        reader: BinaryReadable,
        *,
        length: int,
        start_boundary: dict[str, Any],
        end_boundary: dict[str, Any],
        boundary_policy_uri: str,
    ) -> None:
        if type(length) is not int or length < 0:
            raise ValueError("segment length must be nonnegative")
        super().__init__(
            _SegmentReader(reader, length),
            expected_length=length,
            boundary_policy_uri=boundary_policy_uri,
        )
        self._start_boundary = copy.deepcopy(start_boundary)
        self._end_boundary = copy.deepcopy(end_boundary)

    @contextmanager
    def open(self, *, observer_agent_id: str) -> Iterator[ObservationSession]:
        with super().open(observer_agent_id=observer_agent_id) as session:
            session.extent = {
                "kind": "segment",
                "boundary_policy_uri": self._boundary_policy_uri,
                "start_boundary": self._start_boundary,
                "end_boundary": self._end_boundary,
            }
            yield session

"""Frame the directory-upload identity with one JSON source record at a time."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from typing import BinaryIO, cast

_STRUCTURAL = re.compile(rb"[\x22\x5c\x7b\x7d\x5b\x5d]")
_READ_BYTES = 64 * 1024
_SPACE = frozenset(b" \t\r\n")
_HEADER_FIELDS = frozenset({"format", "root", "idempotency_key", "source_naming_view_id"})


class UploadIdentityRecords:
    """Borrow one reader; standard JSON decoding owns every framed value.

    Work is linear in bytes framed plus JSON decoding of each value once.
    Buffers hold one read window and one header value or source record. A
    complete commitment is available only after the final delimiter and EOF.
    """

    def __init__(self, stream: BinaryIO) -> None:
        self._stream = stream
        self._buffer = b""
        self._position = 0
        self._eof = False
        self._header: dict[str, str] | None = None
        self._rows_started = False
        self._done = False
        self._bytes = 0
        self._digest = hashlib.sha256()

    def _fill(self) -> bool:
        if self._position < len(self._buffer):
            return True
        if self._eof:
            return False
        chunk = self._stream.read(_READ_BYTES)
        if not isinstance(chunk, bytes) or len(chunk) > _READ_BYTES:
            raise ValueError("upload identity reader returned an invalid byte window")
        self._buffer, self._position = chunk, 0
        self._bytes += len(chunk)
        self._digest.update(chunk)
        if not chunk:
            self._eof = True
        return bool(chunk)

    def _take(self) -> int | None:
        if not self._fill():
            return None
        value = self._buffer[self._position]
        self._position += 1
        return value

    def _next(self) -> int | None:
        while (value := self._take()) in _SPACE:
            pass
        return value

    def _string(self, first: int | None) -> str:
        if first != ord('"'):
            raise ValueError("upload identity header requires JSON strings")
        raw = bytearray(b'"')
        escaped = False
        while (value := self._take()) is not None:
            raw.append(value)
            if escaped:
                escaped = False
            elif value == ord("\\"):
                escaped = True
            elif value == ord('"'):
                result = json.loads(raw)
                if not isinstance(result, str):
                    raise ValueError("upload identity header value is not a string")
                return result
        raise ValueError("upload identity contains an unfinished header string")

    def header(self) -> dict[str, str]:
        if self._header is not None:
            return self._header
        if self._next() != ord("{"):
            raise ValueError("upload identity requires a complete JSON object")
        fields: dict[str, str] = {}
        while True:
            key = self._string(self._next())
            if self._next() != ord(":"):
                raise ValueError("upload identity header has no value separator")
            if key == "sources":
                if set(fields) != _HEADER_FIELDS or self._next() != ord("["):
                    raise ValueError("upload identity requires its complete header and sources")
                self._header = fields
                return fields
            if key not in _HEADER_FIELDS or key in fields:
                raise ValueError("upload identity header has an unknown or repeated field")
            fields[key] = self._string(self._next())
            if self._next() != ord(","):
                raise ValueError("upload identity has no sources array")

    def _record(self, first: int | None) -> dict[str, object]:
        if first != ord("{"):
            raise ValueError("upload identity source is not a JSON object")
        raw = bytearray(b"{")
        depth = 1
        quoted = escaped_first = False
        while depth:
            if not self._fill():
                raise ValueError("upload identity contains an unfinished source record")
            start = self._position
            skip_until = start + int(escaped_first)
            escaped_first = False
            # The fixed character class scans each disjoint input span once.
            # Values between structural bytes are copied in one bounded slice.
            for match in _STRUCTURAL.finditer(self._buffer, skip_until):
                position = match.start()
                if position < skip_until:
                    continue
                value = self._buffer[position]
                if quoted:
                    if value == ord("\\"):
                        skip_until = position + 2
                        escaped_first = skip_until > len(self._buffer)
                    elif value == ord('"'):
                        quoted = False
                elif value == ord('"'):
                    quoted = True
                elif value in (ord("{"), ord("[")):
                    depth += 1
                elif value in (ord("}"), ord("]")):
                    depth -= 1
                    if depth == 0:
                        raw.extend(memoryview(self._buffer)[start : position + 1])
                        self._position = position + 1
                        result = json.loads(raw)
                        if not isinstance(result, dict):
                            raise ValueError("upload identity source is not a JSON object")
                        return cast(dict[str, object], result)
            raw.extend(memoryview(self._buffer)[start:])
            self._position = len(self._buffer)
        raise AssertionError("upload identity record did not complete")

    def records(self) -> Iterator[dict[str, object]]:
        self.header()
        if self._rows_started:
            raise ValueError("upload identity records already have a consumer")
        self._rows_started = True
        first = self._next()
        if first != ord("]"):
            while True:
                yield self._record(first)
                separator = self._next()
                if separator == ord("]"):
                    break
                if separator != ord(","):
                    raise ValueError("upload identity source array has an invalid separator")
                first = self._next()
        if self._next() != ord("}") or self._next() is not None:
            raise ValueError("upload identity has an incomplete or trailing document")
        self._done = True

    @property
    def commitment(self) -> tuple[int, str]:
        if not self._done:
            raise ValueError("upload identity traversal has not completed")
        return self._bytes, self._digest.hexdigest()

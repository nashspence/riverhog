"""Exact SHA-256 continuation with bounded immutable-byte updates.

Checkpoint codecs are private operational state. Owners bind their format and
trusted source/progress; decoding a state does not authenticate that source.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar, Self

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

_MASK = 0xFFFFFFFF
_K = (
    0x428A2F98,
    0x71374491,
    0xB5C0FBCF,
    0xE9B5DBA5,
    0x3956C25B,
    0x59F111F1,
    0x923F82A4,
    0xAB1C5ED5,
    0xD807AA98,
    0x12835B01,
    0x243185BE,
    0x550C7DC3,
    0x72BE5D74,
    0x80DEB1FE,
    0x9BDC06A7,
    0xC19BF174,
    0xE49B69C1,
    0xEFBE4786,
    0x0FC19DC6,
    0x240CA1CC,
    0x2DE92C6F,
    0x4A7484AA,
    0x5CB0A9DC,
    0x76F988DA,
    0x983E5152,
    0xA831C66D,
    0xB00327C8,
    0xBF597FC7,
    0xC6E00BF3,
    0xD5A79147,
    0x06CA6351,
    0x14292967,
    0x27B70A85,
    0x2E1B2138,
    0x4D2C6DFC,
    0x53380D13,
    0x650A7354,
    0x766A0ABB,
    0x81C2C92E,
    0x92722C85,
    0xA2BFE8A1,
    0xA81A664B,
    0xC24B8B70,
    0xC76C51A3,
    0xD192E819,
    0xD6990624,
    0xF40E3585,
    0x106AA070,
    0x19A4C116,
    0x1E376C08,
    0x2748774C,
    0x34B0BCB5,
    0x391C0CB3,
    0x4ED8AA4A,
    0x5B9CCA4F,
    0x682E6FF3,
    0x748F82EE,
    0x78A5636F,
    0x84C87814,
    0x8CC70208,
    0x90BEFFFA,
    0xA4506CEB,
    0xBEF9A3F7,
    0xC67178F2,
)
_INITIAL = (
    0x6A09E667,
    0xBB67AE85,
    0x3C6EF372,
    0xA54FF53A,
    0x510E527F,
    0x9B05688C,
    0x1F83D9AB,
    0x5BE0CD19,
)


def _rotr(value: int, count: int) -> int:
    return ((value >> count) | (value << (32 - count))) & _MASK


@declare_growth(
    Declaration(
        id="sha256-continuation.sha256-compression.growth/v1",
        authority="sha256-continuation",
        subjects=("sha256_continuation.sha256_compress",),
        scope="successful-call-primitive-work",
        starts_at="accept one SHA-256 block and eight valid chaining words",
        ends_at="return the eight advanced SHA-256 chaining words",
        dimensions=(
            Dimension("block_octets", OCTETS, "Octets in the one accepted 64-octet block."),
        ),
        bounds=(
            Bound(
                "block_input_octets",
                OCTETS,
                "equal",
                Expression("constant", value=64, unit=OCTETS),
                "Block octets processed by this compression transition.",
            ),
            Bound(
                "rotate_calls",
                COUNT,
                "equal",
                Expression("constant", value=576, unit=COUNT),
                "Fixed-width rotate primitive calls in schedule expansion and compression rounds.",
            ),
            Bound(
                "returned_words",
                COUNT,
                "equal",
                Expression("constant", value=8, unit=COUNT),
                "Returned 32-bit chaining words.",
            ),
        ),
        conditions=(
            "One immutable 64-octet block and exactly eight unsigned 32-bit Python integer words.",
        ),
        invariants=("Preserve standard SHA-256 compression and return all eight words.",),
        limitations=(
            "These primitive counts do not bound elapsed time, physical RAM or caller preparation.",
        ),
    )
)
def sha256_compress(state: Sequence[int], block: bytes) -> list[int]:
    """Advance eight SHA-256 words with one exact block, without mutating the input."""
    if len(state) != 8 or any(type(word) is not int or not 0 <= word <= _MASK for word in state):
        raise ValueError("SHA-256 compression state requires eight unsigned words")
    if type(block) is not bytes or len(block) != 64:
        raise ValueError("SHA-256 compression block must be 64 bytes")
    schedule = [0] * 64
    for index in range(16):
        start = index * 4
        schedule[index] = int.from_bytes(block[start : start + 4], "big")
    for index in range(16, 64):
        x = schedule[index - 15]
        y = schedule[index - 2]
        s0 = _rotr(x, 7) ^ _rotr(x, 18) ^ (x >> 3)
        s1 = _rotr(y, 17) ^ _rotr(y, 19) ^ (y >> 10)
        schedule[index] = (schedule[index - 16] + s0 + schedule[index - 7] + s1) & _MASK
    a, b, c, d, e, f, g, h = state
    for index in range(64):
        s1 = _rotr(e, 6) ^ _rotr(e, 11) ^ _rotr(e, 25)
        choice = (e & f) ^ ((~e) & g)
        temp1 = (h + s1 + choice + _K[index] + schedule[index]) & _MASK
        s0 = _rotr(a, 2) ^ _rotr(a, 13) ^ _rotr(a, 22)
        majority = (a & b) ^ (a & c) ^ (b & c)
        temp2 = (s0 + majority) & _MASK
        h, g, f, e, d, c, b, a = g, f, e, (d + temp1) & _MASK, c, b, a, (temp1 + temp2) & _MASK
    return [
        (state[0] + a) & _MASK,
        (state[1] + b) & _MASK,
        (state[2] + c) & _MASK,
        (state[3] + d) & _MASK,
        (state[4] + e) & _MASK,
        (state[5] + f) & _MASK,
        (state[6] + g) & _MASK,
        (state[7] + h) & _MASK,
    ]


_MAX_MESSAGE_BYTES = (2**64 - 1) // 8


@dataclass(slots=True)
class CheckpointSHA256:
    """Maintain ordinary SHA-256 while retaining at most one partial block.

    The serialized format is private to its owning application binding. A restored
    state must be bound to trusted source and progress before the owner resumes it.
    """

    _CHECKPOINT_FORMAT: ClassVar[str] = "private-sha256-checkpoint/v1"
    _h: list[int]
    _buffer: bytearray
    _total_bytes: int

    def __init__(self, data: bytes = b"") -> None:
        self._h = list(_INITIAL)
        self._buffer = bytearray()
        self._total_bytes = 0
        self.update(data)

    @property
    def total_bytes(self) -> int:
        """Exact source octet position represented by this continuation."""
        return self._total_bytes

    def copy(self) -> Self:
        other = type(self)()
        other._h = list(self._h)
        other._buffer = bytearray(self._buffer)
        other._total_bytes = self._total_bytes
        return other

    def update(self, data: bytes | bytearray | memoryview) -> Self:
        """Hash the snapshot produced by bytes(data).

        Non-bytes adapters preserve their snapshot semantics and may materialize
        the complete input. Immutable bytes use the bounded update path directly.
        """
        return self.update_bytes(bytes(data))

    @declare_growth(
        Declaration(
            id="sha256-continuation.sha256-continuation-update.growth/v1",
            authority="sha256-continuation",
            subjects=("sha256_continuation.CheckpointSHA256.update_bytes",),
            scope="successful-call-primitive-work",
            starts_at="accept immutable bytes and a trusted SHA-256 continuation",
            ends_at="advance exact source position and retain the unfinished block",
            dimensions=(
                Dimension("input_octets", OCTETS, "Immutable input octets accepted by this call."),
                Dimension(
                    "initial_tail_octets",
                    OCTETS,
                    "Previously accepted octets in the unfinished block, from zero to 63.",
                ),
                Dimension(
                    "blocks",
                    COUNT,
                    "Complete compression blocks formed from this call's input and initial tail.",
                ),
            ),
            bounds=(
                Bound(
                    "compression_calls",
                    COUNT,
                    "equal",
                    Expression("dimension", name="blocks"),
                    "Actual compression delegate invocations.",
                ),
                Bound(
                    "block_input_octets",
                    OCTETS,
                    "equal",
                    Expression(
                        "delegate",
                        name="compression",
                        metric="block_input_octets",
                        terms=(Expression("dimension", name="blocks"),),
                        arguments=(
                            ("block_octets", Expression("constant", value=64, unit=OCTETS)),
                        ),
                    ),
                    "Block input octets charged through the compression owner.",
                ),
                Bound(
                    "rotate_calls",
                    COUNT,
                    "equal",
                    Expression(
                        "delegate",
                        name="compression",
                        metric="rotate_calls",
                        terms=(Expression("dimension", name="blocks"),),
                        arguments=(
                            ("block_octets", Expression("constant", value=64, unit=OCTETS)),
                        ),
                    ),
                    "Fixed-width rotates charged through actual compression calls.",
                ),
                Bound(
                    "pending_buffer_octets",
                    OCTETS,
                    "upper",
                    Expression("constant", value=64, unit=OCTETS),
                    "Maximum octets in the owned pending-input buffer during this call.",
                ),
                Bound(
                    "retained_tail_octets",
                    OCTETS,
                    "upper",
                    Expression("constant", value=63, unit=OCTETS),
                    "Octets retained in the unfinished block after the successful call.",
                ),
            ),
            conditions=(
                "A native immutable bytes argument and a trusted initialized or restored state.",
                (
                    "Initial tail is at most 63 octets; blocks is floor((initial_tail_octets + "
                    "input_octets) / 64)."
                ),
                "The total source position remains in SHA-256's 64-bit message-length domain.",
            ),
            invariants=(
                (
                    "Preserve exact ordinary SHA-256 bytes and advance source position by the "
                    "full accepted input."
                ),
                (
                    "Computing a prefix digest finalizes a copy and does not change continuation "
                    "state."
                ),
            ),
            limitations=(
                (
                    "Initialization, digest finalization, codec parsing/export, failed prefixes "
                    "and repeated attempts have separate scope."
                ),
                (
                    "The mutable/noncontiguous input snapshot adapter is separately materialized "
                    "and is not this immutable-byte scope."
                ),
                (
                    "Pending-buffer octets exclude caller storage, temporary block copies, word "
                    "schedules, allocator overhead and physical RAM."
                ),
                (
                    "State shape does not authenticate source identity or authorize progress; the "
                    "application owner supplies that binding."
                ),
            ),
        ),
        delegates=(("compression", sha256_compress),),
    )
    def update_bytes(self, data: bytes) -> Self:
        """Hash immutable input without buffering the complete supplied chunk."""
        if type(data) is not bytes:
            raise TypeError("bounded SHA-256 update requires immutable bytes")
        if self._total_bytes + len(data) > _MAX_MESSAGE_BYTES:
            raise ValueError("SHA-256 source position exceeds its message-length domain")
        view = memoryview(data)
        offset = 0
        self._total_bytes += len(view)
        if self._buffer:
            take = min(64 - len(self._buffer), len(view))
            self._buffer.extend(view[:take])
            offset = take
            if len(self._buffer) == 64:
                self._compress(bytes(self._buffer))
                self._buffer.clear()
        while len(view) - offset >= 64:
            self._compress(bytes(view[offset : offset + 64]))
            offset += 64
        self._buffer.extend(view[offset:])
        return self

    def _compress(self, block: bytes) -> None:
        self._h = sha256_compress(self._h, block)

    def digest(self) -> bytes:
        clone = self.copy()
        bit_length = clone._total_bytes * 8
        clone._buffer.append(0x80)
        while len(clone._buffer) % 64 != 56:
            clone._buffer.append(0)
        clone._buffer.extend(bit_length.to_bytes(8, "big"))
        while clone._buffer:
            block = bytes(clone._buffer[:64])
            del clone._buffer[:64]
            clone._compress(block)
        return b"".join(value.to_bytes(4, "big") for value in clone._h)

    def hexdigest(self) -> str:
        return self.digest().hex()

    def export_state(self) -> str:
        """Encode the fixed private continuation in its owner's codec domain."""
        return json.dumps(
            {
                "format": self._CHECKPOINT_FORMAT,
                "h": self._h,
                "buffer_hex": bytes(self._buffer).hex(),
                "total_bytes": self._total_bytes,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_state(cls, state: str) -> Self:
        """Validate a bounded codec shape; the owner must establish source trust."""
        try:
            if type(state) is not str or len(state) > 512:
                raise ValueError("SHA-256 checkpoint exceeds its fixed codec shape")
            payload = json.loads(state)
            if (
                not isinstance(payload, dict)
                or set(payload) != {"format", "h", "buffer_hex", "total_bytes"}
                or payload["format"] != cls._CHECKPOINT_FORMAT
            ):
                raise ValueError("unsupported SHA-256 checkpoint format")
            h, tail, total = payload["h"], payload["buffer_hex"], payload["total_bytes"]
            if (
                not isinstance(h, list)
                or len(h) != 8
                or any(type(word) is not int or not 0 <= word <= _MASK for word in h)
            ):
                raise ValueError("invalid SHA-256 checkpoint words")
            if (
                type(tail) is not str
                or len(tail) > 126
                or re.fullmatch(r"(?:[0-9a-f]{2})*", tail) is None
            ):
                raise ValueError("invalid SHA-256 checkpoint tail")
            if (
                type(total) is not int
                or not 0 <= total <= _MAX_MESSAGE_BYTES
                or total % 64 != len(tail) // 2
            ):
                raise ValueError("invalid SHA-256 checkpoint source position")
            instance = cls()
            instance._h, instance._buffer, instance._total_bytes = (
                list(h),
                bytearray.fromhex(tail),
                total,
            )
            if instance.export_state() != state:
                raise ValueError("noncanonical SHA-256 checkpoint")
            return instance
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("invalid SHA-256 checkpoint") from exc


__all__ = ["CheckpointSHA256", "sha256_compress"]

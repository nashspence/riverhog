"""Bounded verification of the documented SQLite WAL header/frame stream.

The capture owner retains the original files, sole quiescent connection and WAL
continuity interval. This parser opens nothing and grants no source, generation
or restore authority. Coalescing, shrink tombstones, complete page coverage and
original-file receiving remain mandatory work for the capture owner.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Callable
from dataclasses import dataclass

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

_HEADER = struct.Struct(">8I")
_FRAME = struct.Struct(">6I")
_MAX_PAGE = (1 << 32) - 2


class InvalidGenerationWal(ValueError):
    """The optional capture cannot establish an exact complete WAL interval."""


@dataclass(slots=True)
class GenerationWalWork:
    parsed_octets: int = 0
    checksum_octets: int = 0
    checksum_pairs: int = 0
    checksum_additions: int = 0
    checksum_masks: int = 0
    hashed_octets: int = 0
    frames: int = 0
    commit_markers: int = 0
    check_calls: int = 0


@dataclass(frozen=True, slots=True)
class GenerationWalFrame:
    ordinal: int
    page: int
    data_offset: int
    data: bytes
    sha256: bytes
    previous_pages: int
    committed_pages: int | None


@dataclass(frozen=True, slots=True)
class GenerationWalSummary:
    page_size: int
    page_count: int
    frames: int
    commit_markers: int
    wal_octets: int
    salts: tuple[int, int] | None


def _growth(mode: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    page = Expression("product", terms=(count(1), Expression("dimension", name="page_size")))
    if mode == "start":
        parsed, checksum, hashed = octets(32), octets(24), octets(0)
        pairs, frames, commits = count(3), count(0), count(0)
    elif mode == "frame":
        parsed = Expression("sum", terms=(octets(24), page))
        checksum = Expression("sum", terms=(octets(8), page))
        hashed = page
        pairs = Expression("sum", terms=(count(1), Expression("dimension", name="page_pairs")))
        frames, commits = count(1), count(1)
    else:
        assert mode == "finish"
        parsed = checksum = hashed = octets(0)
        pairs = frames = commits = count(0)
    bounds = {
        "parsed_octets": (OCTETS, parsed),
        "checksum_octets": (OCTETS, checksum),
        "checksum_pairs": (COUNT, pairs),
        "checksum_additions": (
            COUNT,
            Expression("product", terms=(Expression("constant", value=4), pairs)),
        ),
        "checksum_masks": (
            COUNT,
            Expression("product", terms=(Expression("constant", value=2), pairs)),
        ),
        "hashed_octets": (OCTETS, hashed),
        "frames": (COUNT, frames),
        "commit_markers": (COUNT, commits),
        "check_calls": (COUNT, count(2)),
    }
    return Declaration(
        id=f"riverhog-provenance.generation-wal-{mode}.growth/v1",
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance._generation_wal.GenerationWalInspector.{mode}",),
        scope="bounded-native-generation-wal-verification",
        starts_at="receive one exact admitted header/frame or final interval boundary",
        ends_at="return its verified cursor transition or preserve the original failure",
        dimensions=(
            Dimension("page_size", OCTETS_PER_COUNT, "Original SQLite page width in octets."),
            Dimension("page_pairs", COUNT, "Eight-octet checksum pairs in the original page."),
        ),
        bounds=tuple(
            Bound(
                name, unit, "upper", bound, "One fixed header or one original complete page frame."
            )
            for name, (unit, bound) in bounds.items()
        ),
        conditions=(
            "The capture owner supplies sole-connection quiescence and unchanged WAL interval.",
        ),
        invariants=(
            "No operation scans previous frames or the retained database prefix.",
            "A failed original check or invalid input never advances the continuation.",
        ),
        limitations=(
            "File reads, coalescing, tombstones, page coverage and receiving are separately owned.",
            "A valid WAL interval alone grants no authority to select or restore a generation.",
            "Checksum arithmetic counts describe format operations; observer, Python and CPU "
            "instructions are excluded.",
        ),
    )


class GenerationWalInspector:
    """One bounded transition per already admitted original WAL frame.

    The finite extent and both database sizes belong to the frozen capture, not
    to caller estimates of committed progress. A returned frame commits no
    generation state. The owner must receive the complete interval and its
    original file/connection checks before sealing any generation envelope.
    """

    def __init__(
        self,
        *,
        page_size: int,
        wal_octets: int,
        previous_pages: int,
        current_pages: int,
        check: Callable[[], None] | None = None,
    ) -> None:
        if (
            type(page_size) is not int
            or not 512 <= page_size <= 65536
            or page_size & (page_size - 1)
        ):
            raise ValueError("WAL inspection requires an exact SQLite page size")
        if type(wal_octets) is not int or wal_octets < 0:
            raise ValueError("WAL inspection requires its finite nonnegative original extent")
        if any(
            type(value) is not int or not 1 <= value <= _MAX_PAGE
            for value in (previous_pages, current_pages)
        ):
            raise ValueError("WAL inspection requires exact previous and current page extents")
        if check is not None and not callable(check):
            raise TypeError("WAL inspection requires its original callable execution check")
        self.page_size, self.wal_octets = page_size, wal_octets
        self.previous_pages, self.current_pages = previous_pages, current_pages
        self.work = GenerationWalWork()
        self._check = check
        self._phase = "new"
        self._failure: BaseException | None = None
        self._offset = self._frames = self._commits = self._last_commit = 0
        self._committed_pages = previous_pages
        self._salts: tuple[int, int] | None = None
        self._checksum = (0, 0)
        self._byte_order = "<"

    def _require(self, phase: str) -> None:
        if self._failure is not None:
            raise RuntimeError(
                "WAL capture requires reconstruction after its original failure"
            ) from self._failure
        if self._phase != phase:
            raise RuntimeError("WAL inspection is outside its original transition")

    def _check_original(self) -> None:
        if self._check is not None:
            self.work.check_calls += 1
            if self._check() is not None:
                raise TypeError("WAL execution check must return None or raise its original cause")

    def _rolling(self, data: bytes, state: tuple[int, int] = (0, 0)) -> tuple[int, int]:
        first, second = state
        self.work.checksum_octets += len(data)
        for left, right in struct.iter_unpack(self._byte_order + "II", data):
            self.work.checksum_pairs += 1
            self.work.checksum_additions += 4
            self.work.checksum_masks += 2
            first = (first + left + second) & 0xFFFFFFFF
            second = (second + right + first) & 0xFFFFFFFF
        return first, second

    @declare_growth(_growth("start"))
    def start(self, header: bytes) -> None:
        self._require("new")
        try:
            self._check_original()
            if type(header) is not bytes:
                raise TypeError("WAL header must be one original immutable byte string")
            if self.wal_octets == 0:
                if header or self.previous_pages != self.current_pages:
                    raise InvalidGenerationWal(
                        "empty WAL cannot account for the frozen page extent"
                    )
                self._check_original()
                self._phase = "scanning"
                return
            if (
                self.wal_octets < _HEADER.size
                or len(header) != _HEADER.size
                or (self.wal_octets - _HEADER.size) % (_FRAME.size + self.page_size)
            ):
                raise InvalidGenerationWal("incomplete original WAL header or frame extent")
            self.work.parsed_octets += len(header)
            magic, version, size, _sequence, salt1, salt2, check1, check2 = _HEADER.unpack(header)
            if (
                magic not in {0x377F0682, 0x377F0683}
                or version != 3007000
                or size != self.page_size
            ):
                raise InvalidGenerationWal("WAL format or exact page width differs")
            self._byte_order = "<" if magic == 0x377F0682 else ">"
            checksum = self._rolling(header[:24])
            if checksum != (check1, check2):
                raise InvalidGenerationWal("original WAL header checksum differs")
            self._check_original()
            self._salts, self._checksum = (salt1, salt2), checksum
            self._offset, self._phase = _HEADER.size, "scanning"
        except BaseException as error:
            self._failure, self._phase = error, "failed"
            raise

    @declare_growth(_growth("frame"))
    def frame(self, packet: bytes) -> GenerationWalFrame:
        self._require("scanning")
        try:
            self._check_original()
            width = _FRAME.size + self.page_size
            if type(packet) is not bytes:
                raise TypeError("WAL frame must be one original immutable byte string")
            if len(packet) != width or self._offset + width > self.wal_octets:
                raise InvalidGenerationWal("original WAL frame is incomplete or exceeds its extent")
            self.work.parsed_octets += len(packet)
            page, committed, salt1, salt2, check1, check2 = _FRAME.unpack_from(packet)
            if not 1 <= page <= _MAX_PAGE or committed > _MAX_PAGE:
                raise InvalidGenerationWal("WAL frame page or commit extent is invalid")
            if self._salts != (salt1, salt2):
                raise InvalidGenerationWal("original WAL frame salts differ")
            data = packet[_FRAME.size :]
            checksum = self._rolling(data, self._rolling(packet[:8], self._checksum))
            if checksum != (check1, check2):
                raise InvalidGenerationWal("original WAL frame checksum differs")
            self.work.hashed_octets += len(data)
            digest = hashlib.sha256(data).digest()
            result = GenerationWalFrame(
                self._frames + 1,
                page,
                self._offset + _FRAME.size,
                data,
                digest,
                self._committed_pages,
                committed or None,
            )
            self._check_original()
            self._offset += width
            self._frames += 1
            self.work.frames += 1
            self._checksum = checksum
            if committed:
                self._committed_pages = committed
                self._last_commit = self._frames
                self._commits += 1
                self.work.commit_markers += 1
            return result
        except BaseException as error:
            self._failure, self._phase = error, "failed"
            raise

    @declare_growth(_growth("finish"))
    def finish(self) -> GenerationWalSummary:
        self._require("scanning")
        try:
            self._check_original()
            if self._offset != self.wal_octets:
                raise InvalidGenerationWal("original WAL interval was not exhausted")
            if self.wal_octets and (self._last_commit != self._frames or self._last_commit == 0):
                raise InvalidGenerationWal(
                    "original WAL has no final commit or has an uncommitted tail"
                )
            if self._committed_pages != self.current_pages:
                raise InvalidGenerationWal("last WAL commit differs from the frozen page extent")
            result = GenerationWalSummary(
                self.page_size,
                self.current_pages,
                self._frames,
                self._commits,
                self.wal_octets,
                self._salts,
            )
            self._check_original()
            self._phase = "finished"
            return result
        except BaseException as error:
            self._failure, self._phase = error, "failed"
            raise

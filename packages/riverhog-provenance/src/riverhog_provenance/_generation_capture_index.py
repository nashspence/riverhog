"""Bounded WAL coalescing in the original admitted disposable capture epoch.

Only verified frame offsets and digests enter the index; page bytes stay with
the original frozen WAL. Intermediate shrink removals are explicit bounded
units. The capture owner still owns files, complete page coverage, the original
connection interval and all receiving before any generation may be sealed.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass, replace
from typing import Any

from riverhog_canonical_json import format_scalar, parse_scalar
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

from ._generation_wal import GenerationWalFrame, GenerationWalInspector, GenerationWalSummary
from ._native_epoch import EpochContext, observe_preparation
from ._native_work import WorkAmount
from .errors import JournalValidationCapacity


class InvalidCaptureIndex(ValueError):
    """The original optional interval cannot be completely represented."""


@dataclass(frozen=True, slots=True)
class CapturePage:
    page: int
    offset: int | None
    sha256: bytes | None


@dataclass(frozen=True, slots=True)
class CapturePageRead:
    pages: tuple[CapturePage, ...]
    after: int
    more: bool


@dataclass(frozen=True, slots=True)
class _State:
    entries: int = 0
    frames: int = 0
    remove_after: int = 0
    remove_through: int = 0
    ready: bool = False
    read_after: int = 0
    read_count: int = 0
    exhausted: bool = False


@dataclass(slots=True)
class CaptureIndexWork:
    sql_calls: int = 0
    loaded_octets: int = 0
    stored_octets: int = 0
    row_visits: int = 0
    frame_visits: int = 0
    tombstones: int = 0
    page_payload_octets: int = 0


def _growth(mode: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    units = Expression("dimension", name="units")
    row = Expression("dimension", name="metadata_octets")

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    width = Expression("sum", terms=(row, Expression("constant", value=64, unit=OCTETS_PER_COUNT)))
    octets = Expression("product", terms=(units, width))
    sql = {"initialize": 1, "record_frame": 2, "remove": 2, "finish": 0, "read": 2}[mode]
    writes = mode in {"record_frame", "remove"}
    metrics = {
        "sql_calls": (COUNT, scale(sql, units)),
        "loaded_octets": (
            OCTETS,
            octets if mode == "read" else Expression("constant", value=0, unit=OCTETS),
        ),
        "stored_octets": (
            OCTETS,
            octets if writes else Expression("constant", value=0, unit=OCTETS),
        ),
        "row_visits": (COUNT, scale(2 if mode == "read" else 1 if writes else 0, units)),
        "frame_visits": (COUNT, units if mode == "record_frame" else count(0)),
        "tombstones": (COUNT, units if mode == "remove" else count(0)),
        "page_payload_octets": (OCTETS, Expression("constant", value=0, unit=OCTETS)),
    }
    return Declaration(
        id=f"riverhog-provenance.generation-capture-index-{mode}.growth/v1",
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance._generation_capture_index.GenerationCaptureIndex.{mode}",),
        scope="original-disposable-native-wal-coalescing",
        starts_at="admit one original frame, removal/read limit or fixed boundary",
        ends_at="receive bounded index work or preserve its original failure",
        dimensions=(
            Dimension("units", COUNT, "One original operation or the admitted removal/read limit."),
            Dimension("metadata_octets", OCTETS_PER_COUNT, "Original per-row metadata admission."),
            Dimension(
                "previous_pages", COUNT, "Original database extent before this WAL interval."
            ),
        ),
        bounds=tuple(
            Bound(name, unit, "upper", value, "Point updates or one bounded ordered range.")
            for name, (unit, value) in metrics.items()
        ),
        conditions=(
            "The original disposable connection and finite index capacity are admitted before use.",
        ),
        invariants=(
            "A shrink does not enumerate its removed page range in the frame transition.",
            "Every intermediate shrink is received before the next WAL frame is indexed.",
            "The index stores no page payload and never scans earlier output pages.",
        ),
        limitations=(
            "SQL-call counts exclude physical SQLite B-tree/VM/I/O work.",
            "The owner still verifies full page coverage and original file/connection receiving.",
            "This disposable index grants no source or generation-selection authority.",
            "WAL verification retains its original inspector's separate work declaration.",
        ),
    )


class GenerationCaptureIndex:
    """One original WAL inspector and separately admitted capture transaction owner.

    Losing this live continuation abandons the disposable index. A complete
    iterator is not a recovery descriptor. No database, file, grant or reader
    pin is created here, and no owner outcome is inferred from an index row.
    """

    def __init__(
        self,
        epoch: EpochContext,
        inspector: GenerationWalInspector,
        *,
        entry_limit: int,
        metadata_octets: int,
    ) -> None:
        if not isinstance(epoch, EpochContext) or not isinstance(inspector, GenerationWalInspector):
            raise TypeError("capture requires its original epoch and WAL inspector")
        if any(type(value) is not int or value < 1 for value in (entry_limit, metadata_octets)):
            raise ValueError("capture index requires positive original resource admission")
        self.epoch, self.inspector = epoch, inspector
        self.entry_limit, self.metadata_octets = entry_limit, metadata_octets
        self.work = CaptureIndexWork()
        self._state: _State | None = None
        self._failure: BaseException | None = None

    def _require(self, *, ready: bool = False) -> _State:
        if self._failure is not None:
            raise RuntimeError("original capture index requires reconstruction") from self._failure
        self.epoch.require_quiescent()
        if self._state is None or self._state.ready is not ready:
            raise RuntimeError("capture index is outside its original transition")
        self.epoch.check()
        return self._state

    @property
    def exhausted(self) -> bool:
        return self._failure is None and self._state is not None and self._state.exhausted

    def _execute(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        self.epoch.check()
        self.epoch.account(WorkAmount(rows=1))
        self.work.sql_calls += 1
        return self.epoch.db.execute(query, parameters)

    def _receive(self, state: _State) -> None:
        self.epoch.require_receiving(self.epoch.db)
        self._state = state

    def _record(self, page: int, offset: int | None, digest: bytes | None, entries: int) -> int:
        self.epoch.require(self.epoch.db)
        with closing(
            self._execute("SELECT 1 FROM capture_pages WHERE page_no=?", (page,))
        ) as cursor:
            exists = cursor.fetchone() is not None
        self.work.row_visits += int(exists)
        if not exists and entries >= self.entry_limit:
            raise JournalValidationCapacity("optional capture index entry admission exhausted")
        encoded = None if offset is None else format_scalar("nonnegative", offset)
        octets = (
            len(format_scalar("nonnegative", page))
            + (0 if encoded is None else len(encoded))
            + (0 if digest is None else len(digest))
        )
        if octets > self.metadata_octets:
            raise JournalValidationCapacity("optional capture index metadata admission exhausted")
        self.epoch.account(WorkAmount(octets=octets))
        self.work.stored_octets += octets
        self._execute(
            "INSERT INTO capture_pages VALUES(?,?,?) ON CONFLICT(page_no) DO UPDATE SET "
            "frame_offset=excluded.frame_offset,digest=excluded.digest",
            (page, encoded, digest),
        ).close()
        return entries + int(not exists)

    @declare_growth(_growth("initialize"))
    def initialize(self) -> None:
        self.epoch.require_quiescent()
        if self._state is not None or self._failure is not None:
            raise RuntimeError("capture index initialization is single-attempt")
        try:

            def apply(epoch: EpochContext) -> _State:
                epoch.require(self.epoch.db)
                self._execute(
                    "CREATE TABLE capture_pages(page_no INTEGER PRIMARY KEY,"
                    "frame_offset TEXT,digest BLOB)"
                ).close()
                return _State()

            self.epoch.apply(apply, receive=self._receive, admission=WorkAmount(rows=1))
        except BaseException as error:
            self._failure = error
            raise

    @declare_growth(_growth("record_frame"))
    def record_frame(self, frame: GenerationWalFrame) -> None:
        try:
            before = self._require()
            if before.remove_after != before.remove_through:
                raise RuntimeError("receive original shrink removals before another frame")
            if self.inspector.verified_frame is not frame or frame.ordinal != before.frames + 1:
                raise InvalidCaptureIndex("capture lost the original verified WAL frame")

            def apply(epoch: EpochContext) -> _State:
                epoch.require(self.epoch.db)
                entries = self._record(frame.page, frame.data_offset, frame.sha256, before.entries)
                self.work.frame_visits += 1
                after = replace(before, entries=entries, frames=frame.ordinal)
                if (
                    frame.committed_pages is not None
                    and frame.committed_pages < frame.previous_pages
                ):
                    after = replace(
                        after,
                        remove_after=frame.committed_pages,
                        remove_through=frame.previous_pages,
                    )
                return after

            self.epoch.apply(
                apply,
                receive=self._receive,
                admission=WorkAmount(rows=2, octets=self.metadata_octets),
            )
        except BaseException as error:
            self._failure = error
            raise

    @declare_growth(_growth("remove"))
    def remove(self, *, page_limit: int) -> bool:
        try:
            before = self._require()
            if type(page_limit) is not int or page_limit < 1:
                raise ValueError("capture removal requires its positive original page limit")
            through = min(before.remove_through, before.remove_after + page_limit)
            if through == before.remove_after:
                self.epoch.check()
                return True

            def apply(epoch: EpochContext) -> _State:
                epoch.require(self.epoch.db)
                entries = before.entries
                for page in range(before.remove_after + 1, through + 1):
                    self.epoch.check()
                    entries = self._record(page, None, None, entries)
                    self.work.tombstones += 1
                return replace(before, entries=entries, remove_after=through)

            self.epoch.apply(
                apply,
                receive=self._receive,
                admission=WorkAmount(
                    rows=2 * (through - before.remove_after),
                    octets=self.metadata_octets * (through - before.remove_after),
                ),
            )
            return through == before.remove_through
        except BaseException as error:
            self._failure = error
            raise

    @declare_growth(_growth("finish"))
    def finish(self) -> GenerationWalSummary:
        try:
            before = self._require()
            if before.remove_after != before.remove_through:
                raise InvalidCaptureIndex("capture has unreceived original shrink removals")
            result = self.inspector.finish()
            if before.frames != result.frames:
                raise InvalidCaptureIndex("capture omitted original WAL frames")
            self.epoch.check()
            self._state = replace(before, ready=True)
            return result
        except BaseException as error:
            self._failure = error
            raise

    @declare_growth(_growth("read"))
    @observe_preparation
    def read(self, *, page_limit: int) -> CapturePageRead:
        try:
            before = self._require(ready=True)
            if before.exhausted:
                raise RuntimeError("original capture output was already exhausted")
            after = before.read_after
            if type(page_limit) is not int or page_limit < 1:
                raise ValueError("capture requires its positive original page limit")
            # The carrier cannot contain more distinct SQLite page keys than
            # either this original index admission or the SQLite page domain.
            # Keep the caller's natural limit out of SQLite's signed LIMIT slot.
            limit = min(page_limit, self.entry_limit, (1 << 32) - 2)
            pages = []
            with closing(
                self._execute(
                    "SELECT page_no,typeof(frame_offset),length(CAST(frame_offset AS BLOB)),"
                    "substr(CAST(frame_offset AS BLOB),1,?),typeof(digest),"
                    "length(CAST(digest AS BLOB)),"
                    "substr(CAST(digest AS BLOB),1,33) "
                    "FROM capture_pages WHERE page_no>? ORDER BY page_no LIMIT ?",
                    (self.metadata_octets + 1, after, limit),
                )
            ) as cursor:
                for row in cursor:
                    self.epoch.check()
                    page, offset_kind, offset_size, encoded, digest_kind, digest_size, digest = row
                    self.work.row_visits += 1
                    size = (len(encoded) if type(encoded) is bytes else 0) + (
                        len(digest) if type(digest) is bytes else 0
                    )
                    self.epoch.account(WorkAmount(octets=size))
                    self.work.loaded_octets += size
                    if type(page) is not int or not after < page <= (1 << 32) - 2:
                        raise InvalidCaptureIndex("capture page cursor differs")
                    offset: int | None
                    if encoded is None and digest is None and offset_kind == digest_kind == "null":
                        offset = None
                    elif (
                        offset_kind == "text"
                        and type(encoded) is bytes
                        and offset_size == len(encoded) <= self.metadata_octets
                        and digest_kind == "blob"
                        and type(digest) is bytes
                        and digest_size == len(digest) == 32
                    ):
                        offset = parse_scalar("nonnegative", encoded.decode("ascii"))
                        if (
                            not 32 + 24
                            <= offset
                            <= self.inspector.wal_octets - self.inspector.page_size
                        ):
                            raise InvalidCaptureIndex(
                                "capture page offset differs from its original WAL"
                            )
                    else:
                        raise InvalidCaptureIndex("capture offset/digest metadata differs")
                    pages.append(CapturePage(page, offset, digest))
                    after = page
            more = False
            if len(pages) == limit:
                with closing(
                    self._execute("SELECT 1 FROM capture_pages WHERE page_no>? LIMIT 1", (after,))
                ) as cursor:
                    more = cursor.fetchone() is not None
                self.work.row_visits += int(more)
            self.epoch.check()
            count = before.read_count + len(pages)
            if count > before.entries or not more and count != before.entries:
                raise InvalidCaptureIndex("capture lost its exact original indexed row count")
            self._state = replace(before, read_after=after, read_count=count, exhausted=not more)
            return CapturePageRead(tuple(pages), after, more)
        except BaseException as error:
            self._failure = error
            raise

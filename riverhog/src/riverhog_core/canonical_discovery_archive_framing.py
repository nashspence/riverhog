"""Retain original archive frame positions without granting semantic acceptance."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_provenance import (
    JOURNAL_FRAMING_ALGORITHM,
    JournalFramePosition,
    JournalFramingAdvance,
    JournalFramingCursor,
    advance_journal_framing,
)
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX

from riverhog_core.canonical_discovery_archive_framing_growth import (
    declare_growth_for_archive_framing,
)
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    archive_journal_header,
)
from riverhog_core.canonical_discovery_history_inputs import _atomic, _one

ARCHIVE_CAPTURE_PAGE_OCTETS = 64 * 1024


if TYPE_CHECKING:
    from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource


@dataclass(frozen=True, slots=True)
class ArchiveFramingProgress:
    version: int
    cursor: JournalFramingCursor
    phase: str
    failure: str | None


@dataclass(frozen=True, slots=True)
class CapturedArchivePage:
    ordinal: int
    content: bytes
    sha256: str


@dataclass(frozen=True, slots=True)
class PreparedArchiveFraming:
    key: ArchiveTaskKey
    expected: ArchiveFramingProgress
    advance: JournalFramingAdvance
    captured: CapturedArchivePage
    previous_page: tuple[int, str] | None


class ArchiveJournalFramingWork:
    """One original source and at most one detached, bounded framing result."""

    def __init__(self, source: ArchiveJournalSource) -> None:
        self.source = source
        self._prepared: PreparedArchiveFraming | None = None


def _natural(value: Any) -> int:
    if type(value) is not str:
        raise StaleArchiveTask("archive frame counter representation changed")
    try:
        return parse_scalar("nonnegative", value)
    except (TypeError, ValueError):
        raise StaleArchiveTask("archive frame counter is not canonical") from None


def _key(key: ArchiveTaskKey, owner: ArchiveJournalFramingWork) -> tuple[str, str, str]:
    return key.build_id, key.admission_sha256, owner.source.identity.journal_id


def _fence(db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveJournalFramingWork) -> None:
    if key != owner.source.metadata.key or not owner.source.verify_current():
        raise StaleArchiveTask("archive framing has another current source owner")
    if archive_journal_header(db, key, owner.source.identity.journal_id) != owner.source.identity:
        raise StaleArchiveTask("archive framing original source header changed")


def _position(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveJournalFramingWork, ordinal: int
) -> JournalFramePosition:
    row = _one(
        db,
        "SELECT byte_offset,json_bytes FROM discovery_archive_frames "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND ordinal=?",
        (*_key(key, owner), format_scalar("nonnegative", ordinal)),
    )
    if row is None:
        raise StaleArchiveTask("archive framing lost an original frame position")
    return JournalFramePosition(ordinal, _natural(row[0]), _natural(row[1]))


def _page_header(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveJournalFramingWork, ordinal: int
) -> tuple[int, str] | None:
    row = _one(
        db,
        "SELECT length(content),sha256 FROM discovery_archive_pages "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND ordinal=?",
        (*_key(key, owner), format_scalar("nonnegative", ordinal)),
    )

    return None if row is None else (row[0], row[1])


def _captured_page(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: ArchiveJournalFramingWork,
    ordinal: int,
    expected_octets: int,
) -> CapturedArchivePage:
    row = _one(
        db,
        "SELECT content,sha256 FROM discovery_archive_pages "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=? AND ordinal=?",
        (*_key(key, owner), format_scalar("nonnegative", ordinal)),
    )
    if (
        row is None
        or type(row[0]) is not bytes
        or len(row[0]) != expected_octets
        or hashlib.sha256(row[0]).hexdigest() != row[1]
    ):
        raise StaleArchiveTask("archive capture lost its exact retained page")
    return CapturedArchivePage(ordinal, row[0], row[1])


def archive_framing_progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveJournalFramingWork
) -> ArchiveFramingProgress:
    _fence(db, key, owner)
    row = _one(
        db,
        "SELECT worker_id,algorithm,source_bytes,source_sha256,version,byte_offset,"
        "frame_start,frame_count,phase,failure FROM discovery_archive_framing "
        "WHERE build_id=? AND admission_sha256=? AND journal_id=?",
        _key(key, owner),
    )
    header = owner.source.identity
    if row is None or row[:4] != (
        key.worker_id,
        JOURNAL_FRAMING_ALGORITHM,
        format_scalar("nonnegative", header.source_bytes),
        header.source_sha256,
    ):
        raise StaleArchiveTask("archive framing has another original source binding")
    try:
        cursor = JournalFramingCursor(
            _natural(row[5]),
            None if row[6] is None else _natural(row[6]),
            _natural(row[7]),
        )
    except ValueError:
        raise StaleArchiveTask("archive framing cursor is invalid") from None
    progress = ArchiveFramingProgress(_natural(row[4]), cursor, row[8], row[9])
    if (
        cursor.byte_offset > header.source_bytes
        or (progress.version == 0) != (cursor.byte_offset == 0)
        or progress.version > cursor.byte_offset
        or progress.phase not in {"scanning", "complete", "deferred"}
        or (progress.phase == "deferred")
        != (type(progress.failure) is str and bool(progress.failure))
        or progress.phase != "deferred"
        and progress.failure is not None
        or progress.phase == "complete"
        and (cursor.byte_offset != header.source_bytes or cursor.frame_start is not None)
        or progress.phase == "complete"
        and not cursor.frame_count
        or progress.phase == "scanning"
        and cursor.byte_offset == header.source_bytes
    ):
        raise StaleArchiveTask("archive framing phase differs from its original source")
    if cursor.byte_offset:
        ordinal, tail = divmod(cursor.byte_offset - 1, ARCHIVE_CAPTURE_PAGE_OCTETS)
        page_header = _page_header(db, key, owner, ordinal)
        if (
            page_header is None
            or page_header[0] != tail + 1
            or type(page_header[1]) is not str
            or len(page_header[1]) != 64
            or any(c not in "0123456789abcdef" for c in page_header[1])
        ):
            raise StaleArchiveTask("archive capture checkpoint lost its last exact page")
    last_end = 0
    if cursor.frame_count:
        last = _position(db, key, owner, cursor.frame_count - 1)
        last_end = last.byte_offset + last.json_bytes + 2
    if (
        last_end > cursor.byte_offset
        or progress.phase != "deferred"
        and last_end != (cursor.byte_offset if cursor.frame_start is None else cursor.frame_start)
    ):
        raise StaleArchiveTask("archive framing checkpoint lost its last original position")
    return progress


def begin_archive_framing(
    db: sqlite3.Connection, key: ArchiveTaskKey, owner: ArchiveJournalFramingWork
) -> ArchiveFramingProgress:
    with _atomic(db):
        _fence(db, key, owner)
        header = owner.source.identity
        db.execute(
            "INSERT OR IGNORE INTO discovery_archive_framing VALUES("
            "?,?,?,?,?,?,?,'0','0',NULL,'0','scanning',NULL)",
            (
                *_key(key, owner),
                key.worker_id,
                JOURNAL_FRAMING_ALGORITHM,
                format_scalar("nonnegative", header.source_bytes),
                header.source_sha256,
            ),
        ).close()
        return archive_framing_progress(db, key, owner)


def archive_journal_frame(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: ArchiveJournalFramingWork,
    ordinal: int,
) -> JournalFramePosition | None:
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("archive frame ordinal must be nonnegative")
    progress = archive_framing_progress(db, key, owner)
    return None if ordinal >= progress.cursor.frame_count else _position(db, key, owner, ordinal)


def archive_framing_page(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: ArchiveJournalFramingWork,
    ordinal: int,
) -> CapturedArchivePage | None:
    """Authenticate one normalized page beneath the current original source fence."""
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("archive capture ordinal must be nonnegative")
    progress = archive_framing_progress(db, key, owner)
    start = ordinal * ARCHIVE_CAPTURE_PAGE_OCTETS
    if start >= progress.cursor.byte_offset:
        return None
    return _captured_page(
        db,
        key,
        owner,
        ordinal,
        min(ARCHIVE_CAPTURE_PAGE_OCTETS, progress.cursor.byte_offset - start),
    )


def _capture_bytes(tail: bytes, content: bytes, consumed: int) -> bytes:
    return tail + content[:consumed]


def prepare_archive_framing(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: ArchiveJournalFramingWork,
    *,
    byte_budget: int,
    frame_budget: int,
) -> PreparedArchiveFraming | None:
    """Read one authenticated short window, then run the public Native framer."""
    if db.in_transaction:
        raise ValueError("archive framing preparation requires no active write transaction")
    if type(byte_budget) is not int or byte_budget < 1:
        raise ValueError("archive framing byte budget must be positive")
    if type(frame_budget) is not int or frame_budget < 1:
        raise ValueError("archive framing frame budget must be positive")
    progress = archive_framing_progress(db, key, owner)
    if progress.phase != "scanning":
        owner._prepared = None
        return None
    if (
        owner._prepared is not None
        and owner._prepared.expected == progress
        and owner._prepared.advance.cursor.byte_offset - progress.cursor.byte_offset <= byte_budget
        and len(owner._prepared.advance.positions) <= frame_budget
    ):
        return owner._prepared
    offset = progress.cursor.byte_offset
    ordinal, tail_octets = divmod(offset, ARCHIVE_CAPTURE_PAGE_OCTETS)
    tail = None if not tail_octets else _captured_page(db, key, owner, ordinal, tail_octets)
    content = owner.source.read_window(
        offset,
        min(
            byte_budget,
            owner.source.identity.source_bytes - offset,
            ARCHIVE_CAPTURE_PAGE_OCTETS - tail_octets,
        ),
    )
    advance = advance_journal_framing(
        progress.cursor,
        content,
        byte_budget=byte_budget,
        frame_budget=frame_budget,
        at_end=offset + len(content) == owner.source.identity.source_bytes,
    )
    cursor = advance.cursor
    if (
        not offset < cursor.byte_offset <= offset + len(content)
        or len(advance.positions) > frame_budget
        or cursor.frame_count != progress.cursor.frame_count + len(advance.positions)
        or advance.complete
        and (
            cursor.byte_offset != owner.source.identity.source_bytes
            or cursor.frame_start is not None
            or advance.failure is not None
        )
    ):
        raise StaleArchiveTask("archive framing acknowledgement exceeds its original window")
    end = offset if progress.cursor.frame_start is None else progress.cursor.frame_start
    for index, position in enumerate(advance.positions):
        if (
            position.ordinal != progress.cursor.frame_count + index
            or position.byte_offset != end
            or not 0 <= position.json_bytes <= PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
        ):
            raise StaleArchiveTask("archive framing position differs from its original coordinate")
        end = position.byte_offset + position.json_bytes + 2
    if (
        end > cursor.byte_offset
        or advance.failure is None
        and end != (cursor.byte_offset if cursor.frame_start is None else cursor.frame_start)
    ):
        raise StaleArchiveTask("archive framing result lost its original consumed extent")
    if not owner.source.verify_current():
        raise StaleArchiveTask("archive framing source changed during preparation")
    captured = _capture_bytes(
        b"" if tail is None else tail.content, content, cursor.byte_offset - offset
    )
    page = PreparedArchiveFraming(
        key,
        progress,
        advance,
        CapturedArchivePage(ordinal, captured, hashlib.sha256(captured).hexdigest()),
        None if tail is None else (len(tail.content), tail.sha256),
    )
    owner._prepared = page
    return page


def apply_archive_framing(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: ArchiveJournalFramingWork,
    page: PreparedArchiveFraming,
) -> bool:
    """Commit original positions and the exact cursor together; stale work writes nothing."""
    if page is not owner._prepared or page.key != key:
        raise StaleArchiveTask("archive framing has another detached prepared unit")
    with _atomic(db):
        if archive_framing_progress(db, key, owner) != page.expected:
            return False
        captured = page.captured
        if _page_header(db, key, owner, captured.ordinal) != page.previous_page:
            raise StaleArchiveTask("archive capture page changed before cursor publication")
        db.execute(
            "INSERT INTO discovery_archive_pages VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(build_id,admission_sha256,journal_id,ordinal) "
            "DO UPDATE SET content=excluded.content,sha256=excluded.sha256",
            (
                *_key(key, owner),
                format_scalar("nonnegative", captured.ordinal),
                captured.content,
                captured.sha256,
            ),
        ).close()
        advance = page.advance
        cursor = advance.cursor
        phase = (
            "deferred"
            if advance.failure is not None
            else ("complete" if advance.complete else "scanning")
        )
        for position in advance.positions:
            db.execute(
                "INSERT INTO discovery_archive_frames VALUES(?,?,?,?,?,?)",
                (
                    *_key(key, owner),
                    format_scalar("nonnegative", position.ordinal),
                    format_scalar("nonnegative", position.byte_offset),
                    format_scalar("nonnegative", position.json_bytes),
                ),
            ).close()
        changed = db.execute(
            "UPDATE discovery_archive_framing SET version=?,byte_offset=?,frame_start=?,"
            "frame_count=?,phase=?,failure=? WHERE build_id=? AND admission_sha256=? "
            "AND journal_id=? AND worker_id=? AND version=?",
            (
                format_scalar("nonnegative", page.expected.version + 1),
                format_scalar("nonnegative", cursor.byte_offset),
                None
                if cursor.frame_start is None
                else format_scalar("nonnegative", cursor.frame_start),
                format_scalar("nonnegative", cursor.frame_count),
                phase,
                advance.failure,
                *_key(key, owner),
                key.worker_id,
                format_scalar("nonnegative", page.expected.version),
            ),
        )
        try:
            if changed.rowcount != 1:
                raise StaleArchiveTask("archive framing cursor changed before publication")
        finally:
            changed.close()
        archive_framing_progress(db, key, owner)
        return True


declare_growth_for_archive_framing(
    {
        "begin": begin_archive_framing,
        "progress": archive_framing_progress,
        "prepare": prepare_archive_framing,
        "apply": apply_archive_framing,
        "position": archive_journal_frame,
        "page": archive_framing_page,
    }
)

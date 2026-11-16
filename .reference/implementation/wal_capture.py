"""Streaming SQLite page capture for the isolated generation reference.

This is not an admitted Riverhog connection or workspace implementation. The
caller owns one private disposable WAL connection, freezes all use of it for
the entire context, and guarantees that no checkpoint/reset occurred since the
selected base. A lost continuity guarantee disables incremental capture. These
conditions cannot be established by examining a WAL file alone.

The first capture checkpoints and reads the complete main database once. Later
captures validate the entire finite WAL interval and keep only final frame
offsets in a disk SQLite index. They do not checkpoint, reset or select anything.
The caller stages pages inside this context but completes the immutable manifest
only AFTER successful context exit; partial stream consumption is a refusal.
The caller resets the private WAL only after positive selection receiving when
continuing optional tracking. It may abandon tracking, retain an unresolved
candidate, and continue disposable semantic execution without new promotions.
Production callers must supply finite admitted WAL/index envelopes and retain
the original construction's file/connection receiving; omitted limits and raw
path operations below are reference-only conveniences.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import struct
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

MAX_PAGE = (1 << 32) - 2
WAL_HEADER = struct.Struct(">8I")
FRAME_HEADER = struct.Struct(">6I")
INDEX_QUANTUM = 128


class CaptureRefused(ValueError):
    """Optional capture cannot establish a complete consistent page image."""


@dataclass
class Capture:
    page_size: int
    page_count: int
    initial: bool
    wal_bytes_read: int = 0
    database_bytes_read: int = 0
    copied_page_bytes: int = 0
    zero_page_bytes: int = 0
    wal_frames: int = 0
    committed_transactions: int = 0
    index_updates: int = 0
    tombstones: int = 0
    exhausted: bool = False
    _pages: Iterator[tuple[int, bytes | None]] = field(default_factory=lambda: iter(()), repr=False)

    @property
    def pages(self) -> Iterator[tuple[int, bytes | None]]:
        """One-pass pages; consume within the capture context."""
        return self._pages


def _positive_limit(value: int | None, label: str) -> None:
    if value is not None and (type(value) is not int or value < 1):
        raise ValueError(f"{label} must be a positive integer or None")


def _checksum(data: bytes, byte_order: str, state: tuple[int, int] = (0, 0)) -> tuple[int, int]:
    if len(data) % 8:
        raise CaptureRefused("misaligned WAL checksum input")
    first, second = state
    for left, right in struct.iter_unpack(byte_order + "II", data):
        first = (first + left + second) & 0xFFFFFFFF
        second = (second + right + first) & 0xFFFFFFFF
    return first, second


def _identity(handle: BinaryIO) -> tuple[int, int, int, int]:
    state = os.fstat(handle.fileno())
    return state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns


def _owner_shape(connection: sqlite3.Connection, database: Path) -> tuple[int, int, int, int]:
    if connection.in_transaction:
        raise CaptureRefused("capture requires a quiescent committed owner")
    databases = connection.execute("PRAGMA database_list").fetchall()
    main = [row for row in databases if row[1] == "main"]
    if len(main) != 1 or Path(main[0][2]).resolve() != database.resolve():
        raise CaptureRefused("connection does not own the requested main database")
    if any(row[1] not in {"main", "temp"} for row in databases):
        raise CaptureRefused("attached database state is outside this capture closure")
    if connection.execute("SELECT 1 FROM sqlite_temp_schema LIMIT 1").fetchone():
        raise CaptureRefused("temporary semantic state is outside this capture closure")
    if connection.execute("PRAGMA journal_mode").fetchone() != ("wal",):
        raise CaptureRefused("private scratch must already use WAL mode")
    if connection.execute("PRAGMA synchronous").fetchone() != (0,):
        raise CaptureRefused("this extractor accepts only disposable OFF scratch")
    if connection.execute("PRAGMA wal_autocheckpoint").fetchone() != (0,):
        raise CaptureRefused("automatic WAL reset must already be disabled")
    size = connection.execute("PRAGMA page_size").fetchone()[0]
    count = connection.execute("PRAGMA page_count").fetchone()[0]
    if not 512 <= size <= 65536 or size & (size - 1):
        raise CaptureRefused("unsupported SQLite page size")
    if not 1 <= count <= MAX_PAGE:
        raise CaptureRefused("invalid initialized SQLite page extent")
    schema = connection.execute("PRAGMA schema_version").fetchone()[0]
    return size, count, connection.total_changes, schema


class _Index:
    def __init__(self, path: Path, maximum: int | None, capture: Capture) -> None:
        self.path = path
        self.maximum = maximum
        self.count = 0
        self.capture = capture
        self.db: sqlite3.Connection | None = None
        # Refuse overwrite, including an existing symlink. The production owner
        # additionally enforces admitted namespaces and original file identity.
        for suffix in ("", "-journal", "-wal", "-shm"):
            existing = Path(str(path) + suffix)
            if existing.exists() or existing.is_symlink():
                raise FileExistsError(existing)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        try:
            self.db = sqlite3.connect(path, isolation_level=None)
            self.db.execute("PRAGMA journal_mode=DELETE")
            self.db.execute("PRAGMA synchronous=OFF")
            self.db.execute("PRAGMA mmap_size=0")
            self.db.execute("PRAGMA cache_size=-512")
            self.db.execute("PRAGMA temp_store=FILE")
            self.db.execute(
                "CREATE TABLE pages(page_no INTEGER PRIMARY KEY, frame_offset INTEGER, digest BLOB)"
            )
            self.db.execute("BEGIN")
        except BaseException:
            self.close()
            raise

    def record(self, page: int, offset: int | None, digest: bytes | None) -> None:
        assert self.db is not None
        found = self.db.execute("SELECT 1 FROM pages WHERE page_no=?", (page,)).fetchone()
        if found is None:
            if self.maximum is not None and self.count >= self.maximum:
                raise CaptureRefused("admitted dirty-page index envelope exhausted")
            self.db.execute("INSERT INTO pages VALUES(?,?,?)", (page, offset, digest))
            self.count += 1
        else:
            self.db.execute(
                "UPDATE pages SET frame_offset=?,digest=? WHERE page_no=?", (offset, digest, page)
            )
        self.capture.index_updates += 1
        if self.capture.index_updates % INDEX_QUANTUM == 0:
            self.db.commit()
            self.db.execute("BEGIN")

    def close(self) -> None:
        if self.db is not None:
            self.db.close()
            self.db = None
        for suffix in ("", "-journal", "-wal", "-shm"):
            Path(str(self.path) + suffix).unlink(missing_ok=True)


def _scan_wal(
    handle: BinaryIO,
    index: _Index,
    capture: Capture,
    previous_page_count: int,
    max_wal_bytes: int | None,
) -> None:
    length = os.fstat(handle.fileno()).st_size
    if max_wal_bytes is not None and length > max_wal_bytes:
        raise CaptureRefused("admitted WAL envelope exhausted")
    if length == 0:
        if capture.page_count != previous_page_count:
            raise CaptureRefused("empty WAL cannot account for changed page extent")
        return
    if length < WAL_HEADER.size or (length - WAL_HEADER.size) % (
        FRAME_HEADER.size + capture.page_size
    ):
        raise CaptureRefused("incomplete WAL header or trailing frame")
    header = handle.read(WAL_HEADER.size)
    capture.wal_bytes_read += len(header)
    magic, version, page_size, _, salt1, salt2, check1, check2 = WAL_HEADER.unpack(header)
    if magic not in (0x377F0682, 0x377F0683) or version != 3007000:
        raise CaptureRefused("unknown WAL format")
    if page_size != capture.page_size:
        raise CaptureRefused("WAL and live database page sizes differ")
    byte_order = "<" if magic == 0x377F0682 else ">"
    state = _checksum(header[:24], byte_order)
    if state != (check1, check2):
        raise CaptureRefused("WAL header checksum differs")
    committed_count = previous_page_count
    last_commit = 0
    while handle.tell() < length:
        frame = handle.read(FRAME_HEADER.size)
        data_offset = handle.tell()
        data = handle.read(capture.page_size)
        capture.wal_bytes_read += len(frame) + len(data)
        if len(frame) != FRAME_HEADER.size or len(data) != capture.page_size:
            raise CaptureRefused("WAL changed or truncated during capture")
        page, commit_count, frame_salt1, frame_salt2, check1, check2 = FRAME_HEADER.unpack(frame)
        if not 1 <= page <= MAX_PAGE or commit_count > MAX_PAGE:
            raise CaptureRefused("WAL frame has an invalid page extent")
        if (frame_salt1, frame_salt2) != (salt1, salt2):
            raise CaptureRefused("stale or mismatched WAL frame salts")
        state = _checksum(frame[:8], byte_order, state)
        state = _checksum(data, byte_order, state)
        if state != (check1, check2):
            raise CaptureRefused("WAL frame checksum differs")
        capture.wal_frames += 1
        index.record(page, data_offset, hashlib.sha256(data).digest())
        if commit_count:
            # A shrink invalidates pages even when this WAL later regrows. Keep
            # those removals until a later actual frame supplies new contents.
            for removed in range(commit_count + 1, committed_count + 1):
                index.record(removed, None, None)
            committed_count = commit_count
            last_commit = capture.wal_frames
            capture.committed_transactions += 1
    if last_commit != capture.wal_frames or last_commit == 0:
        raise CaptureRefused("WAL has no final commit or has an uncommitted tail")
    if committed_count != capture.page_count:
        raise CaptureRefused("last WAL commit does not match the live page extent")
    assert index.db is not None
    # SQLite's built-in Unix/Windows VFSes never write the reserved lock-byte
    # page. Crossing that page therefore need not produce a WAL frame. Give
    # this semantically unused hole an explicit all-zero image, bound by the
    # generation map. Other VFSes need their own qualified rule; ordinary
    # missing pages are always refused. No complete database scan is needed.
    lock_page = (0x40000000 // capture.page_size) + 1
    if lock_page <= capture.page_count:
        lock_row = index.db.execute(
            "SELECT frame_offset FROM pages WHERE page_no=?", (lock_page,)
        ).fetchone()
        if (previous_page_count < lock_page and lock_row is None) or lock_row == (None,):
            index.record(lock_page, -1, None)
    # Every other newly allocated logical page needs an actual final image.
    additions = max(0, capture.page_count - previous_page_count)
    observed = index.db.execute(
        "SELECT count(*) FROM pages WHERE page_no>? AND page_no<=? AND frame_offset IS NOT NULL",
        (previous_page_count, capture.page_count),
    ).fetchone()[0]
    if observed != additions:
        raise CaptureRefused("WAL omits a newly allocated database page")
    if index.db.execute(
        "SELECT 1 FROM pages WHERE page_no<=? AND frame_offset IS NULL LIMIT 1",
        (capture.page_count,),
    ).fetchone():
        raise CaptureRefused("regrown page has no new image after an intervening shrink")
    index.db.commit()


@contextmanager
def capture_pages(
    connection: sqlite3.Connection,
    database: Path,
    spool: Path,
    *,
    initial: bool,
    previous_page_count: int,
    max_wal_bytes: int | None = None,
    max_spool_pages: int | None = None,
) -> Iterator[Capture]:
    """Yield a one-pass complete initial image or exact incremental changes.

    ``max_spool_pages`` limits distinct page keys, not SQLite allocation bytes;
    actual file/journal byte admission remains with the construction owner.
    A refusal affects optional recovery only. Never replay an invalid optional
    cursor, clear a root selector, or treat this as archive/source validation.
    """
    _positive_limit(max_wal_bytes, "max_wal_bytes")
    _positive_limit(max_spool_pages, "max_spool_pages")
    if type(initial) is not bool or type(previous_page_count) is not int:
        raise ValueError("initial and previous_page_count have strict scalar types")
    if previous_page_count < 0 or previous_page_count > MAX_PAGE:
        raise ValueError("invalid previous page extent")
    if initial != (previous_page_count == 0):
        raise ValueError("the one initial capture has no prior page extent")
    shape = _owner_shape(connection, database)
    capture = Capture(shape[0], shape[1], initial)
    handle: BinaryIO | None = None
    index: _Index | None = None
    try:
        if initial:
            wal_path = Path(str(database) + "-wal")
            if (
                max_wal_bytes is not None
                and wal_path.is_file()
                and wal_path.stat().st_size > max_wal_bytes
            ):
                raise CaptureRefused("admitted initial WAL envelope exhausted")
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint != (0, 0, 0):
                raise CaptureRefused("private initial checkpoint did not completely truncate")
            handle = database.open("rb")
            identity = _identity(handle)
            if identity[2] != capture.page_size * capture.page_count:
                raise CaptureRefused("main database length differs from the committed page extent")

            def initial_pages() -> Iterator[tuple[int, bytes | None]]:
                assert handle is not None
                for page in range(1, capture.page_count + 1):
                    data = handle.read(capture.page_size)
                    capture.database_bytes_read += len(data)
                    if len(data) != capture.page_size:
                        raise CaptureRefused("main database truncated during initial capture")
                    capture.copied_page_bytes += len(data)
                    yield page, data

            capture._pages = initial_pages()
        else:
            wal = Path(str(database) + "-wal")
            if not wal.is_file():
                raise CaptureRefused("incremental WAL is missing; continuity is unprovable")
            handle = wal.open("rb")
            identity = _identity(handle)
            index = _Index(spool, max_spool_pages, capture)
            _scan_wal(handle, index, capture, previous_page_count, max_wal_bytes)

            def changed_pages() -> Iterator[tuple[int, bytes | None]]:
                assert handle is not None and index is not None and index.db is not None
                rows = index.db.execute(
                    "SELECT page_no,frame_offset,digest FROM pages "
                    "WHERE page_no<=? ORDER BY page_no",
                    (max(previous_page_count, capture.page_count),),
                )
                try:
                    for page, offset, digest in rows:
                        if page > capture.page_count:
                            capture.tombstones += 1
                            yield page, None
                            continue
                        if offset is None:
                            raise CaptureRefused("missing current page image")
                        if offset == -1:
                            data = bytes(capture.page_size)
                            capture.zero_page_bytes += len(data)
                            capture.copied_page_bytes += len(data)
                            yield page, data
                            continue
                        handle.seek(offset)
                        data = handle.read(capture.page_size)
                        capture.wal_bytes_read += len(data)
                        if (
                            len(data) != capture.page_size
                            or hashlib.sha256(data).digest() != digest
                        ):
                            raise CaptureRefused("WAL page changed after frame validation")
                        capture.copied_page_bytes += len(data)
                        yield page, data
                finally:
                    rows.close()

            capture._pages = changed_pages()

        def verify() -> None:
            assert handle is not None
            if _identity(handle) != identity or _owner_shape(connection, database) != shape:
                raise CaptureRefused("owner or captured file changed across the capture boundary")

        source_pages = capture._pages

        def checked_pages() -> Iterator[tuple[int, bytes | None]]:
            try:
                yield from source_pages
                # A consumer must receive this check before treating the page
                # stream as complete, not only after it has selected a root.
                verify()
                capture.exhausted = True
            finally:
                close_source = getattr(source_pages, "close", None)
                if close_source is not None:
                    close_source()

        capture._pages = checked_pages()
        yield capture
        if not capture.exhausted:
            raise CaptureRefused("capture page stream was not completely received")
        verify()
    finally:
        close_pages = getattr(capture._pages, "close", None)
        if close_pages is not None:
            close_pages()
        if handle is not None:
            handle.close()
        if index is not None:
            index.close()

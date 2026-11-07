"""Current normalized archive bytes for retained Native journal validation."""

from __future__ import annotations

import sqlite3

from riverhog_canonical_json import format_scalar
from riverhog_provenance import JournalValidationCapacity
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from riverhog_provenance.replay_binding import JournalReplayBinding

from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_core.canonical_discovery_archive_work import ArchiveTaskKey, StaleArchiveTask
from riverhog_core.canonical_discovery_journal_source_growth import (
    declare_growth_for_journal_source,
)


def _slice(raw: bytes, start: int, octets: int) -> bytes:
    return raw[start : start + octets]


def _join(pieces: list[bytes]) -> bytes:
    return b"".join(pieces)


class DiscoveryJournalSource:
    """One frozen framing directory and at most one normalized read page.

    The caller supplies its independently owned Native directory identity and
    serializes all work beneath the current discovery metadata/kernel custody.
    Framing remains structural preparation; Native owns ordered acceptance.
    """

    def __init__(self, work: framing.ArchiveJournalFramingWork, *, directory_id: str) -> None:
        self._work, self._source = work, work.source
        self._metadata = self._source.metadata
        self._identity = self._source.identity
        self._page: framing.CapturedArchivePage | None = None
        with self._metadata.read() as (db, key):
            if db.in_transaction:
                raise ValueError("journal source construction requires no write transaction")
            progress = framing.archive_framing_progress(db, key, work)
            if progress.phase not in {"complete", "deferred"}:
                raise StaleArchiveTask("journal source has no terminal original framing directory")
            self._progress = progress
            self._binding = JournalReplayBinding(
                attempt_id=self._metadata.job.attempt_id,
                directory_id=directory_id,
                journal_id=self._identity.journal_id,
                source_bytes=format_scalar("nonnegative", self._identity.source_bytes),
                source_sha256=self._identity.source_sha256,
                source_version=format_scalar("nonnegative", progress.version),
                frame_count=format_scalar("nonnegative", progress.cursor.frame_count),
                catalog_sha256=self._metadata.job.catalog_sha256,
                require_profiles=False,
            )

    @property
    def binding(self) -> JournalReplayBinding:
        return self._binding

    def _current(self, db: sqlite3.Connection, key: ArchiveTaskKey) -> None:
        if db.in_transaction:
            raise ValueError("journal source reads require no write transaction")
        if (
            self._work.source is not self._source
            or self._source.metadata is not self._metadata
            or self._source.identity != self._identity
            or self._metadata.job.attempt_id != self._binding.attempt_id
            or self._metadata.job.catalog_sha256 != self._binding.catalog_sha256
            or framing.archive_framing_progress(db, key, self._work) != self._progress
        ):
            raise StaleArchiveTask("journal source lost its exact original frozen directory")

    def verify_current(self) -> bool:
        try:
            with self._metadata.read() as (db, key):
                self._current(db, key)
            return True
        except StaleArchiveTask:
            return False

    def _range(self, offset: int, octets: int) -> None:
        if (
            type(offset) is not int
            or type(octets) is not int
            or offset < 0
            or octets < 0
            or offset + octets > self._identity.source_bytes
        ):
            raise ValueError("invalid normalized journal source range")

    def _captured(self, db: sqlite3.Connection, key: ArchiveTaskKey, ordinal: int) -> bytes:
        page = self._page
        if page is None or page.ordinal != ordinal:
            page = framing.archive_framing_page(db, key, self._work, ordinal)
            if page is None:
                raise StaleArchiveTask("journal source lost a captured page")
            self._page = page
        return page.content

    def read_window(self, offset: int, maximum_octets: int) -> bytes:
        """Return a short fixed page, or one original window after deferred framing."""
        self._range(offset, maximum_octets)
        with self._metadata.read() as (db, key):
            self._current(db, key)
            if not maximum_octets:
                return b""
            if offset < self._progress.cursor.byte_offset:
                extent = framing.ARCHIVE_CAPTURE_PAGE_OCTETS
                raw = self._captured(db, key, offset // extent)
                return _slice(raw, offset % extent, maximum_octets)
            if self._progress.phase != "deferred":
                raise StaleArchiveTask("journal source has no captured prefix at this range")
            return self._source.read_window(
                offset, min(maximum_octets, framing.ARCHIVE_CAPTURE_PAGE_OCTETS)
            )

    def read_exact(self, offset: int, octets: int) -> bytes:
        """Assemble one existing admitted frame from normalized pages only."""
        self._range(offset, octets)
        if octets > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise JournalValidationCapacity("journal source cannot admit the existing frame")
        with self._metadata.read() as (db, key):
            self._current(db, key)
            end = offset + octets
            if end > self._progress.cursor.byte_offset:
                raise ValueError("journal frame exceeds the captured source prefix")
            extent = framing.ARCHIVE_CAPTURE_PAGE_OCTETS
            pieces = []
            while offset < end:
                raw = self._captured(db, key, offset // extent)
                piece = _slice(raw, offset % extent, end - offset)
                if not piece:
                    raise StaleArchiveTask("journal source page cannot advance the exact range")
                pieces.append(piece)
                offset += len(piece)
            return _join(pieces)


declare_growth_for_journal_source(DiscoveryJournalSource)

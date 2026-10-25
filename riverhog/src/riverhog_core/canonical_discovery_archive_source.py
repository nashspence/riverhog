"""Current archive-qualified journal ranges without metadata rescans."""

from __future__ import annotations

import sqlite3

from riverhog_archive_contracts import ProvenanceVolumeDocument
from riverhog_provenance import JournalValidationCapacity
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_core.canonical_discovery_archive_source_growth import declare_archive_source_growth
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveJournalHeader,
    ArchiveTaskKey,
    StaleArchiveTask,
    archive_journal_header,
    archive_journal_segment_at,
)
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata
from riverhog_core.provenance_archive_read import CanonicalProvenanceArchiveReader


class ArchiveJournalSource:
    """Hold at most one authenticated payload beneath the caller's current custody.

    The caller serializes work with its discovery owner. Original directory
    membership precedes payload reads; complete segment length/digest checks
    precede caching. A cached segment never replaces live archive/worker fences.
    Native framing and semantic acceptance remain independent obligations.
    """

    def __init__(
        self,
        metadata: DiscoveryMetadata,
        reader: CanonicalProvenanceArchiveReader,
        journal_id: str,
    ) -> None:
        require_canonical_uuid_urn(journal_id)
        self.metadata, self.reader = metadata, reader
        self._admission = metadata.admission
        with metadata.read() as (db, key):
            self._header = archive_journal_header(db, key, journal_id)
        self._page: tuple[ProvenanceVolumeDocument, bytes] | None = None

    @property
    def identity(self) -> ArchiveJournalHeader:
        return self._header

    def _original(self, db: sqlite3.Connection, key: ArchiveTaskKey) -> None:
        if self.metadata.admission is not self._admission:
            raise StaleArchiveTask("archive journal source has another original admission")
        if archive_journal_header(db, key, self._header.journal_id) != self._header:
            raise StaleArchiveTask("archive journal source header changed")

    def verify_current(self) -> bool:
        try:
            with self.metadata.read() as (db, key):
                self._original(db, key)
            return True
        except StaleArchiveTask:
            return False

    def _range(self, offset: int, octets: int) -> None:
        if (
            type(offset) is not int
            or type(octets) is not int
            or offset < 0
            or octets < 0
            or offset + octets > self._header.source_bytes
        ):
            raise ValueError("invalid exact archive journal range")

    def _window(
        self, db: sqlite3.Connection, key: ArchiveTaskKey, offset: int, maximum: int
    ) -> bytes:
        document = archive_journal_segment_at(db, key, self._header.journal_id, offset)
        if document is None or document.journal_offset is None:
            raise StaleArchiveTask("archive source lost its covering segment")
        page = self._page
        raw = (
            self.reader.payload_bytes(document) if page is None or page[0] != document else page[1]
        )
        begin = offset - document.journal_offset
        result = raw[begin : begin + maximum]
        if not result:
            raise StaleArchiveTask("archive segment cannot advance the exact source range")
        if archive_journal_segment_at(db, key, self._header.journal_id, offset) != document:
            raise StaleArchiveTask("archive source changed during payload acquisition")
        self._page = document, raw
        return result

    def read_window(self, offset: int, maximum_octets: int) -> bytes:
        """Return a short range from one original segment, with live pre/post fences."""
        self._range(offset, maximum_octets)
        with self.metadata.read() as (db, key):
            self._original(db, key)
            result = b"" if not maximum_octets else self._window(db, key, offset, maximum_octets)
            self._original(db, key)
        return result

    def read_exact(self, offset: int, octets: int) -> bytes:
        """Assemble one admitted Native frame within one current read boundary."""
        self._range(offset, octets)
        if octets > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise JournalValidationCapacity("archive source cannot admit the existing frame extent")
        pieces = []
        with self.metadata.read() as (db, key):
            self._original(db, key)
            position = offset
            end = offset + octets
            while position < end:
                raw = self._window(db, key, position, end - position)
                pieces.append(raw)
                position += len(raw)
            self._original(db, key)
        return b"".join(pieces)


declare_archive_source_growth(ArchiveJournalSource)

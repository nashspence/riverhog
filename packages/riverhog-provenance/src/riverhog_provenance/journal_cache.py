"""Bounded reuse of pure validation for freshly read exact journal bytes."""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from tempfile import TemporaryFile
from threading import Lock
from typing import TYPE_CHECKING, Any, cast

from riverhog_provenance_contracts import ContractCatalog

from .errors import ProvenanceValidationError
from .journal_store import JournalStore, _Frames

if TYPE_CHECKING:
    from .journal import JournalSummary


class JournalValidationCache:
    """Cache immutable SQLite validation projections, never read authority or scope.

    Every call consumes the current provider and checks exact bytes. The fixed
    catalog and validation mode qualify reuse. Eviction drops only the cache's
    reference; an active caller keeps its immutable snapshot alive. Larger valid
    journals bypass caching and retain the ordinary disk-backed validation path.
    """

    def __init__(
        self,
        catalog: ContractCatalog,
        *,
        byte_budget: int = 256 * 1024 * 1024,
        entry_budget: int = 1024,
    ) -> None:
        if byte_budget < 0 or entry_budget < 0:
            raise ValueError("validation cache capacities cannot be negative")
        self.catalog = catalog
        self.byte_budget = byte_budget
        self.entry_budget = entry_budget
        self._entries: OrderedDict[tuple[str, int, bool], tuple[JournalSummary, int]] = (
            OrderedDict()
        )
        self._bytes = 0
        self._lock = Lock()

    def validate(
        self,
        chunks: Iterable[bytes],
        *,
        catalog: ContractCatalog | None,
        require_profiles: bool,
        expected_anchor: Mapping[str, Any] | None,
        require_exact_tail: bool,
    ) -> JournalSummary:
        from .journal import validate_journal_chunks

        if catalog is not self.catalog:
            return validate_journal_chunks(
                chunks,
                catalog=catalog,
                require_profiles=require_profiles,
                expected_anchor=expected_anchor,
                require_exact_tail=require_exact_tail,
            )
        with TemporaryFile() as spool:
            digest = hashlib.sha256()
            size = 0
            for chunk in chunks:
                digest.update(chunk)
                size += len(chunk)
                spool.write(chunk)
            key = (digest.hexdigest(), size, require_profiles)
            with self._lock:
                cached = self._entries.get(key)
                if cached is not None:
                    self._entries.move_to_end(key)
            if cached is not None:
                summary = self._reader(cached[0])
            else:
                spool.seek(0)
                summary = validate_journal_chunks(
                    iter(lambda: spool.read(64 * 1024), b""),
                    catalog=catalog,
                    require_profiles=require_profiles,
                )
                if (summary.journal_sha256, summary.journal_bytes) != key[:2]:
                    raise ProvenanceValidationError("validated journal differs from current bytes")
            self._check_anchor(summary, expected_anchor, require_exact_tail)
            if cached is None:
                stored = self._put(key, summary)
                if stored is not None:
                    summary = self._reader(stored)
            return summary

    @staticmethod
    def _check_anchor(
        summary: JournalSummary,
        expected: Mapping[str, Any] | None,
        require_exact_tail: bool,
    ) -> None:
        if expected:
            if summary.anchor != dict(expected):
                digest = hashlib.sha256()
                size = 0
                matched = False
                for frame in summary.frames:
                    digest.update(frame.encoded)
                    size += len(frame.encoded)
                    if frame.document["id"] == expected["through"]["entry_id"]:
                        candidate = {
                            "journal_id": summary.journal_id,
                            "through": frame.reference,
                            "prefix_sha256": digest.hexdigest(),
                            "prefix_bytes": str(size),
                        }
                        if candidate != dict(expected):
                            raise ProvenanceValidationError(
                                "externally supplied prefix anchor mismatch"
                            )
                        matched = True
                        break
                if not matched:
                    raise ProvenanceValidationError(
                        "journal does not contain the expected anchored prefix"
                    )
        if require_exact_tail and (
            expected is None or summary.tail.reference != expected["through"]
        ):
            raise ProvenanceValidationError("an exact tail anchor was required")

    @staticmethod
    def _reader(summary: JournalSummary) -> JournalSummary:
        from .journal import JournalSummary

        store = JournalStore.reader_from(cast(_Frames, summary.frames).store)
        return JournalSummary(
            summary.journal_id,
            store.frames,
            store.graph_validation,
            store.retired,
            summary.journal_sha256,
            summary.journal_bytes,
            store.origins,
        )

    def _put(self, key: tuple[str, int, bool], summary: JournalSummary) -> JournalSummary | None:
        store = cast(_Frames, summary.frames).store
        # Bound the pager working set as well as the retained SQLite file extent.
        store.db.execute("PRAGMA cache_size = -32")
        pages = store.db.execute("PRAGMA page_count").fetchone()[0]
        page_bytes = store.db.execute("PRAGMA page_size").fetchone()[0]
        cost = pages * page_bytes + 32 * 1024
        if cost > self.byte_budget or self.entry_budget == 0:
            return None
        with self._lock:
            if key in self._entries:
                self._entries.move_to_end(key)
                return self._entries[key][0]
            while self._entries and (
                len(self._entries) >= self.entry_budget or self._bytes + cost > self.byte_budget
            ):
                _, (_, removed) = self._entries.popitem(last=False)
                self._bytes -= removed
            # The cache retains sealed files, not a connection per cached journal.
            # Each caller opens its own reader; eviction preserves active leases.
            store.db.close()
            self._entries[key] = summary, cost
            self._bytes += cost
            return summary

"""Exact member-history closure with a disk-backed worklist and no graph union."""

from __future__ import annotations

import hashlib
import sqlite3
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    HistoryJournalAnchor,
    MemberHistoryBinding,
    MemberHistoryImport,
    MemberHistoryStore,
    history_record_page_object_path,
    member_history_object_path,
    provenance_structure_identity,
    read_bounded_history_object,
    source_binding_proof_object_path,
)
from riverhog_canonical_json import canonical_json_bytes, require_canonical_json
from riverhog_provenance_contracts import ContractCatalog, ExternalReference

from .delivery import selected_delivery_occurrence
from .errors import ProvenanceValidationError
from .graph import iter_assertions
from .journal import (
    JournalSummary,
    external_reference,
    iter_journal_frames,
    validate_journal_chunks,
)


def _external_references(value: object) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        if value.get("scope") == "external":
            # Only canonical endpoint records are dependencies. An opaque profile
            # may contain unrelated data which happens to have a scope field.
            fields = {"scope", "journal_id", "entry", "assertion_id", "object_id", "object_type"}
            if set(value) == fields:
                yield ExternalReference.model_validate(value).model_dump(mode="json")
                return
        if value.get("type") == "json" and set(value) == {"type", "value"}:
            return  # Opaque profile data is not a canonical endpoint language.
        for child in value.values():
            yield from _external_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from _external_references(child)


def _prefix(chunks: Iterable[bytes], count: int) -> Iterator[bytes]:
    source = iter(chunks)
    remaining = count
    try:
        while remaining:
            chunk = next(source, None)
            if chunk is None:
                raise ProvenanceValidationError("required canonical prefix is incomplete")
            selected = chunk[:remaining]
            remaining -= len(selected)
            yield selected
    finally:
        close = getattr(source, "close", None)
        if close is not None:
            close()


class MemberHistoryMembership:
    """Transient local query projection of a previously verified closure.

    Images are process-local cache data, never archive authority or wire evidence.
    The caller retains the exact root, selection and extent and current read fences.
    """

    def __init__(self, image: bytes) -> None:
        self._scratch = TemporaryDirectory(prefix="riverhog-history-membership-")
        path = Path(self._scratch.name) / "membership.sqlite3"
        path.write_bytes(image)
        self._db = sqlite3.connect(path)
        self._cursors: set[sqlite3.Cursor] = set()
        try:
            self._db.execute("PRAGMA query_only = ON")
            self._db.execute("PRAGMA cache_size = -512")
        except BaseException:
            self._db.close()
            self._scratch.cleanup()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        for cursor in self._cursors:
            cursor.close()
        self._cursors.clear()
        self._db.close()
        self._scratch.cleanup()

    def journal_anchors(self) -> Iterator[HistoryJournalAnchor]:
        cursor = self._db.execute("SELECT anchor FROM journals ORDER BY identity")
        self._cursors.add(cursor)
        try:
            for (encoded,) in cursor:
                yield HistoryJournalAnchor.from_mapping(require_canonical_json(encoded))
        finally:
            if cursor in self._cursors:
                self._cursors.remove(cursor)
                cursor.close()

    def journal_anchor(self, journal_id: str) -> HistoryJournalAnchor | None:
        row = self._db.execute(
            "SELECT anchor FROM journals WHERE identity = ?", (journal_id,)
        ).fetchone()
        return HistoryJournalAnchor.from_mapping(require_canonical_json(row[0])) if row else None

    def contains_structure_object(self, path: str) -> bool:
        return (
            self._db.execute("SELECT 1 FROM objects WHERE path = ?", (path,)).fetchone() is not None
        )


class MemberHistoryClosure:
    """Resolve exactly requested roots, documentary dependencies and accepted imports.

    Callers supply authenticated local archive reads. The resolver never follows
    URLs or imports a dependency's effective graph into a selected root. Selection
    sets, dependency work and deduplication are stored on disk; each journal is
    evaluated independently by the unchanged canonical engine.
    """

    def __init__(
        self,
        store: MemberHistoryStore,
        read_journal: Callable[[str, int | None], Iterable[bytes]],
        *,
        member_role: str,
        catalog: ContractCatalog | None = None,
    ) -> None:
        self.store = store
        self.read_journal = read_journal
        self.member_role = member_role
        self.catalog = catalog
        self._summaries: OrderedDict[bytes, JournalSummary] = OrderedDict()
        self._summary_bytes = 0
        self._scratch = TemporaryDirectory(prefix="riverhog-history-closure-")
        self._db = sqlite3.connect(Path(self._scratch.name) / "closure.sqlite3")
        self._db.execute("PRAGMA cache_size = -512")
        self._db.executescript(
            "CREATE TABLE histories (identity TEXT, extent TEXT, binding BLOB, done INTEGER, "
            "PRIMARY KEY(identity, extent));"
            "CREATE TABLE imports (source TEXT, target TEXT, PRIMARY KEY(source, target));"
            "CREATE TABLE snapshots (identity TEXT PRIMARY KEY, anchor BLOB, done INTEGER);"
            "CREATE TABLE journals (identity TEXT PRIMARY KEY, size TEXT, anchor BLOB);"
            "CREATE TABLE objects (path TEXT PRIMARY KEY);"
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._summaries.clear()
        self._summary_bytes = 0
        self._db.close()
        self._scratch.cleanup()

    def _object(self, path: str) -> None:
        self._db.execute("INSERT OR IGNORE INTO objects VALUES (?)", (path,))

    def _history(self, binding: MemberHistoryBinding, extent: str) -> None:
        if extent not in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            raise ValueError("unsupported history request extent")
        self._db.execute(
            "INSERT OR IGNORE INTO histories VALUES (?, ?, ?, 0)",
            (binding.history_sha256, extent, canonical_json_bytes(binding.to_mapping())),
        )

    def _snapshot(self, anchor: HistoryJournalAnchor) -> None:
        encoded = canonical_json_bytes(anchor.to_mapping())
        self._db.execute(
            "INSERT OR IGNORE INTO snapshots VALUES (?, ?, 0)",
            (hashlib.sha256(encoded).hexdigest(), encoded),
        )
        old = self._db.execute(
            "SELECT size FROM journals WHERE identity = ?", (anchor.journal_id,)
        ).fetchone()
        if old is None or int(old[0]) < anchor.prefix_bytes:
            self._db.execute(
                "INSERT OR REPLACE INTO journals VALUES (?, ?, ?)",
                (anchor.journal_id, str(anchor.prefix_bytes), encoded),
            )

    def summary_at(self, anchor: HistoryJournalAnchor) -> JournalSummary:
        key = canonical_json_bytes(anchor.to_mapping())
        chunks = _prefix(
            self.read_journal(anchor.journal_id, anchor.prefix_bytes), anchor.prefix_bytes
        )
        cached = self._summaries.get(key)
        if cached is not None:
            # Re-read the authenticated provider even on a cache hit. Exact byte
            # identity permits reusing validation; current read fences still run.
            digest = hashlib.sha256()
            for chunk in chunks:
                digest.update(chunk)
            if digest.hexdigest() != anchor.prefix_sha256:
                raise ProvenanceValidationError("canonical journal prefix identity changed")
            self._summaries.move_to_end(key)
            return cached
        summary = validate_journal_chunks(
            chunks,
            catalog=self.catalog,
            expected_anchor=anchor.to_mapping(),
            require_exact_tail=True,
            require_profiles=False,
        )
        # Cache capacity bounds working state, never accepted journal extents.
        budget = 4 * 1024 * 1024
        if summary.journal_bytes <= budget:
            while self._summaries and (
                len(self._summaries) >= 32 or self._summary_bytes + summary.journal_bytes > budget
            ):
                _, removed = self._summaries.popitem(last=False)
                self._summary_bytes -= removed.journal_bytes
            self._summaries[key] = summary
            self._summary_bytes += summary.journal_bytes
        return summary

    def _reference_anchor(self, reference: Mapping[str, Any]) -> HistoryJournalAnchor:
        selected = ExternalReference.model_validate(reference).model_dump(mode="json")
        digest = hashlib.sha256()
        count = 0
        found: HistoryJournalAnchor | None = None
        # Consume the authenticated stream even after finding the entry. Its
        # enclosing provider can verify ciphertext/fixity and the final read fence.
        for frame in iter_journal_frames(self.read_journal(selected["journal_id"], None)):
            digest.update(frame.encoded)
            count += len(frame.encoded)
            if frame.reference != selected["entry"]:
                continue
            rows = (
                row
                for _, row in iter_assertions(frame.document["body"].get("assertions", {}))
                if row["assertion_id"] == selected["assertion_id"]
            )
            row = next(rows, None)
            if (
                row is None
                or row["id"] != selected["object_id"]
                or row["type"] != selected["object_type"]
            ):
                raise ProvenanceValidationError(
                    "foreign reference differs from its exact assertion"
                )
            found = HistoryJournalAnchor.from_mapping(
                {
                    "journal_id": selected["journal_id"],
                    "through": frame.reference,
                    "prefix_sha256": digest.hexdigest(),
                    "prefix_bytes": str(count),
                }
            )
        if found is None:
            raise ProvenanceValidationError("required foreign entry is absent")
        return found

    def _visit_history(self, binding: MemberHistoryBinding, extent: str) -> None:
        history = self.store.descriptor(binding)
        self._object(member_history_object_path(binding.history_sha256))
        for authority in (history.roots, history.imports):
            for page in self.store.pages(authority):
                self._object(
                    history_record_page_object_path(authority.records_sha256, page.ordinal)
                )
        for selected in self.store.roots(binding, extent=extent):
            self._snapshot(selected.journal)
        for imported in self.store.imports(binding):
            source_binding = self._accept_import(imported)
            self._db.execute(
                "INSERT OR IGNORE INTO imports VALUES (?, ?)",
                (binding.history_sha256, source_binding.history_sha256),
            )
            cycle = self._db.execute(
                "WITH RECURSIVE descendants(id) AS (SELECT target FROM imports WHERE source = ? "
                "UNION SELECT target FROM imports JOIN descendants ON source = id) "
                "SELECT 1 FROM descendants WHERE id = ? LIMIT 1",
                (binding.history_sha256, binding.history_sha256),
            ).fetchone()
            if cycle:
                raise ProvenanceValidationError("cyclic member-history imports")

    def _accept_import(self, imported: MemberHistoryImport) -> MemberHistoryBinding:
        proof = self.store.source_proof(imported)
        source = self.store.descriptor(proof.binding)
        primary = self.summary_at(source.primary.journal)
        state, _ = selected_delivery_occurrence(
            primary,
            binding={"artifact_id": source.artifact_id, **source.primary.to_mapping()},
            artifact_id=source.artifact_id,
            byte_count=source.bytes,
            sha256=source.sha256,
            member_role=self.member_role,
        )
        if external_reference(primary, state["id"]) != imported.input_state:
            raise ProvenanceValidationError("imported State differs from the source primary")
        self._object(source_binding_proof_object_path(imported.source_binding_proof_sha256))
        self._history(proof.binding, imported.extent)
        return proof.binding

    def resolve_import(self, imported: MemberHistoryImport) -> None:
        """Validate an accepted early input selection, before output H can be sealed."""
        self._accept_import(imported)
        self._drain()

    def resolve(self, binding: MemberHistoryBinding, *, extent: str) -> None:
        self._history(binding, extent)
        self._drain()

    def resolve_snapshot(self, anchor: HistoryJournalAnchor) -> None:
        """Validate early construction custody without claiming a finalized H."""
        self._snapshot(anchor)
        self._drain()

    def _drain(self) -> None:
        while True:
            pending = self._db.execute(
                "SELECT identity, extent, binding FROM histories WHERE done = 0 LIMIT 1"
            ).fetchone()
            if pending is not None:
                identity, requested, encoded = pending
                self._visit_history(
                    MemberHistoryBinding.from_mapping(require_canonical_json(encoded)), requested
                )
                self._db.execute(
                    "UPDATE histories SET done = 1 WHERE identity = ? AND extent = ?",
                    (identity, requested),
                )
                continue
            snapshot = self._db.execute(
                "SELECT identity, anchor FROM snapshots WHERE done = 0 LIMIT 1"
            ).fetchone()
            if snapshot is None:
                return
            identity, encoded = snapshot
            summary = self.summary_at(
                HistoryJournalAnchor.from_mapping(require_canonical_json(encoded))
            )
            # Inspect documentary assertions, including retracted rows. Required
            # preimages do not disappear when a later local correction retires a claim.
            for frame in summary.frames:
                for reference in _external_references(frame.document["body"].get("assertions", {})):
                    self._snapshot(self._reference_anchor(reference))
            parent = summary.frames[0].document["body"]["journal"].get("forked_from")
            if parent is not None:
                self._snapshot(HistoryJournalAnchor.from_mapping(parent))
            self._db.execute("UPDATE snapshots SET done = 1 WHERE identity = ?", (identity,))

    def journal_anchors(self) -> Iterator[HistoryJournalAnchor]:
        """Largest required exact prefix per journal, never a substituted current head."""
        for (encoded,) in self._db.execute("SELECT anchor FROM journals ORDER BY identity"):
            yield HistoryJournalAnchor.from_mapping(require_canonical_json(encoded))

    def journal_anchor(self, journal_id: str) -> HistoryJournalAnchor | None:
        row = self._db.execute(
            "SELECT anchor FROM journals WHERE identity = ?", (journal_id,)
        ).fetchone()
        return HistoryJournalAnchor.from_mapping(require_canonical_json(row[0])) if row else None

    def membership_image(self, *, max_bytes: int) -> bytes | None:
        """Freeze verified membership for a bounded process-local cache.

        A larger closure remains usable without caching; capacity never limits
        accepted history. No journal graph or causal authority is unioned.
        """
        pages = self._db.execute("PRAGMA page_count").fetchone()[0]
        page_bytes = self._db.execute("PRAGMA page_size").fetchone()[0]
        if pages * page_bytes > max_bytes:
            return None
        self._db.commit()
        path = Path(self._scratch.name) / "membership.sqlite3"
        copy = sqlite3.connect(path)
        try:
            self._db.backup(copy)
        finally:
            copy.close()
        return path.read_bytes()

    def snapshots(self) -> Iterator[HistoryJournalAnchor]:
        """Every exact selected or required snapshot, without a latest-head union."""
        for (encoded,) in self._db.execute("SELECT anchor FROM snapshots ORDER BY identity"):
            yield HistoryJournalAnchor.from_mapping(require_canonical_json(encoded))

    def contains_journal(self, journal_id: str) -> bool:
        return (
            self._db.execute("SELECT 1 FROM journals WHERE identity = ?", (journal_id,)).fetchone()
            is not None
        )

    def structure_objects(self) -> Iterator[bytes]:
        for (path,) in self._db.execute("SELECT path FROM objects ORDER BY path"):
            content = read_bounded_history_object(self.store.read_object(path), 4 * 1024 * 1024)
            if provenance_structure_identity(content).relative_path != path:
                raise ProvenanceValidationError("history structure path differs from its bytes")
            yield content

    def contains_structure_object(self, path: str) -> bool:
        """Whether this exact structural object supports the resolved selections."""
        return (
            self._db.execute("SELECT 1 FROM objects WHERE path = ?", (path,)).fetchone() is not None
        )


__all__ = ["MemberHistoryClosure", "MemberHistoryMembership"]

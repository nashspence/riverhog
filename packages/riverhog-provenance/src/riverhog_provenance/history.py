"""Exact member-history closure with a disk-backed worklist and no graph union."""

from __future__ import annotations

import hashlib
import sqlite3
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self, cast

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
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
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


def _pending_declaration(kind: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id=f"riverhog-provenance.history-pending-{kind}.growth/v1",
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance.history.MemberHistoryClosure._pending_{kind}",),
        scope="native-history-pending-work-point-selection",
        starts_at="select the first unfinished registered obligation from the owned closure",
        ends_at="close the SQLite cursor and return at most one detached exact work record",
        dimensions=(
            Dimension(
                "registered_jobs", COUNT, "All retained completed and pending jobs of this kind."
            ),
            Dimension(
                "candidate_octets",
                OCTETS,
                "Maximum complete registered record width, measured before selection.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                count(1),
                "One indexed pending selection, including an empty queue.",
            ),
            Bound(
                "returned_rows", COUNT, "upper", count(1), "The exact first pending row or no row."
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="candidate_octets"),
                "All detached fields are accounted for without payload abbreviation.",
            ),
            Bound(
                "closed_cursors",
                COUNT,
                "upper",
                count(1),
                "Any created selection cursor is closed before returning or propagating a failure.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(512),
                        Expression(
                            "product",
                            terms=(
                                count(128),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="registered_jobs"),
                                                count(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "The pending index skips completed work; VM constants depend on the SQLite engine.",
            ),
            Bound(
                "json_calls",
                COUNT,
                "equal",
                count(0),
                "Selection returns the stored exact binding or anchor without decoding it.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Selection neither rehashes a source nor establishes source authority.",
            ),
        ),
        conditions=(
            "The closure owns its current SQLite schema with its pending partial indexes.",
            "Row-byte input describes complete registered records, including every field.",
            "SQLite VM counts are engine dependent; lock waits and elapsed time are observations.",
        ),
        invariants=(
            "Previously completed obligations remain in the full deduplication relations.",
            "Pending rows preserve insertion order, including new dependencies.",
            "Selection changes no completion, source, import or semantic state.",
        ),
        limitations=(
            "History visits, source validation, imports, writes and cleanup have separate costs.",
            "This primitive supplies neither a complete closure bound nor fresh authorization.",
        ),
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
            "CREATE INDEX pending_histories ON histories(done) WHERE done = 0;"
            "CREATE TABLE imports (source TEXT, target TEXT, PRIMARY KEY(source, target));"
            "CREATE TABLE snapshots (identity TEXT PRIMARY KEY, anchor BLOB, done INTEGER);"
            "CREATE INDEX pending_snapshots ON snapshots(done) WHERE done = 0;"
            "CREATE TABLE journals (identity TEXT PRIMARY KEY, size TEXT, anchor BLOB);"
            "CREATE TABLE objects (path TEXT PRIMARY KEY);"
            "CREATE TABLE reference_journals (identity TEXT PRIMARY KEY, sha256 TEXT, size TEXT);"
            "CREATE TABLE reference_assertions (journal TEXT, entry TEXT, sequence TEXT, "
            "json_sha256 TEXT, assertion TEXT, object TEXT, type TEXT, prefix_sha256 TEXT, "
            "prefix_bytes TEXT, PRIMARY KEY(journal, entry, assertion));"
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        for summary in self._summaries.values():
            summary.close()
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
        """Return an owned exact snapshot; cache eviction does not close its lease."""
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
            return cached.borrow()
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
                removed.close()
            self._summaries[key] = summary
            self._summary_bytes += summary.journal_bytes
            return summary.borrow()
        return summary

    def _reference_anchor(self, reference: Mapping[str, Any]) -> HistoryJournalAnchor:
        selected = ExternalReference.model_validate(reference).model_dump(mode="json")
        journal_id = selected["journal_id"]
        cached = self._db.execute(
            "SELECT sha256, size FROM reference_journals WHERE identity = ?", (journal_id,)
        ).fetchone()
        digest = hashlib.sha256()
        count = 0
        # Each lookup consumes the current authenticated provider completely,
        # including its final read fence. Reuse only the documentary address
        # projection of the same exact bytes; full journal validation remains
        # owned by the snapshot worklist. The index lives only in this closure's
        # disk-backed scratch and has no semantic extent limit.
        chunks = iter(self.read_journal(journal_id, None))
        try:
            if cached is not None:
                for chunk in chunks:
                    digest.update(chunk)
                    count += len(chunk)
                if (digest.hexdigest(), str(count)) != cached:
                    raise ProvenanceValidationError("canonical reference journal identity changed")
            else:
                try:
                    for frame in iter_journal_frames(chunks):
                        document = frame.document
                        if document["journal_id"] != journal_id:
                            raise ProvenanceValidationError("foreign journal identity changed")
                        digest.update(frame.encoded)
                        count += len(frame.encoded)
                        entry = frame.reference
                        for _, row in iter_assertions(document["body"].get("assertions", {})):
                            if "id" not in row:
                                continue
                            self._db.execute(
                                "INSERT INTO reference_assertions "
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (
                                    journal_id,
                                    entry["entry_id"],
                                    entry["sequence"],
                                    entry["json_sha256"],
                                    row["assertion_id"],
                                    row["id"],
                                    row["type"],
                                    digest.hexdigest(),
                                    str(count),
                                ),
                            )
                    self._db.execute(
                        "INSERT INTO reference_journals VALUES (?, ?, ?)",
                        (journal_id, digest.hexdigest(), str(count)),
                    )
                except BaseException:
                    self._db.execute(
                        "DELETE FROM reference_assertions WHERE journal = ?", (journal_id,)
                    )
                    raise
        finally:
            close = getattr(chunks, "close", None)
            if close is not None:
                close()
        row = self._db.execute(
            "SELECT sequence, json_sha256, object, type, prefix_sha256, prefix_bytes "
            "FROM reference_assertions WHERE journal = ? AND entry = ? AND assertion = ?",
            (journal_id, selected["entry"]["entry_id"], selected["assertion_id"]),
        ).fetchone()
        if row is None:
            raise ProvenanceValidationError("required foreign entry or assertion is absent")
        if row[:4] != (
            selected["entry"]["sequence"],
            selected["entry"]["json_sha256"],
            selected["object_id"],
            selected["object_type"],
        ):
            raise ProvenanceValidationError("foreign reference differs from its exact assertion")
        return HistoryJournalAnchor.from_mapping(
            {
                "journal_id": journal_id,
                "through": selected["entry"],
                "prefix_sha256": row[4],
                "prefix_bytes": row[5],
            }
        )

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
        with self.summary_at(source.primary.journal) as primary:
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

    @declare_growth(_pending_declaration("history"))
    def _pending_history(self) -> tuple[str, str, bytes] | None:
        with closing(
            self._db.execute(
                "SELECT identity, extent, binding FROM histories "
                "WHERE done = 0 ORDER BY rowid LIMIT 1"
            )
        ) as rows:
            return cast(tuple[str, str, bytes] | None, rows.fetchone())

    @declare_growth(_pending_declaration("snapshot"))
    def _pending_snapshot(self) -> tuple[str, bytes] | None:
        with closing(
            self._db.execute(
                "SELECT identity, anchor FROM snapshots WHERE done = 0 ORDER BY rowid LIMIT 1"
            )
        ) as rows:
            return cast(tuple[str, bytes] | None, rows.fetchone())

    def _drain(self) -> None:
        while True:
            pending = self._pending_history()
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
            snapshot = self._pending_snapshot()
            if snapshot is None:
                return
            identity, encoded = snapshot
            with self.summary_at(
                HistoryJournalAnchor.from_mapping(require_canonical_json(encoded))
            ) as summary:
                # Inspect documentary assertions, including retracted rows. Required
                # preimages do not disappear when a later local correction retires a claim.
                for frame in summary.frames:
                    for reference in _external_references(
                        frame.document["body"].get("assertions", {})
                    ):
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

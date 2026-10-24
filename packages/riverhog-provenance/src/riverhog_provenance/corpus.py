"""Disk-backed validation across independently evaluated canonical journals."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self, cast

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    Unit,
    declare_growth,
)
from riverhog_provenance_contracts import canonical_document

from .common import fingerprint
from .errors import ProvenanceValidationError
from .graph import iter_assertions
from .history import _external_references
from .journal import JournalSummary, _body_assertions, _identity_signature


def _corpus_order_declaration() -> Declaration:
    def number(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def multiply(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    records = add(dimension("edges"), dimension("vertices"), number(1))
    widths = add(
        multiply(number(8, SCALAR), dimension("identity_octets")),
        multiply(number(4, SCALAR), dimension("degree_octets")),
        multiply(number(2, SCALAR), dimension("kind_octets")),
    )
    return Declaration(
        id="riverhog-provenance.corpus-topological-order.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus.CanonicalCorpusValidator._acyclic",),
        scope="native-complete-corpus-graph-kind-cycle-check",
        starts_at="open an owned disk-backed indegree and ready-work index",
        ends_at="accept or reject the graph kind and release all ordering scratch state",
        dimensions=(
            Dimension("edges", COUNT, "Distinct edges of this kind in the accepted corpus index."),
            Dimension("vertices", COUNT, "Distinct source and target identities of those edges."),
            Dimension(
                "identity_octets",
                OCTETS_PER_COUNT,
                "Maximum complete UTF-8 endpoint identity width.",
            ),
            Dimension("degree_octets", OCTETS_PER_COUNT, "Decimal width of max(1, edges)."),
            Dimension(
                "kind_octets", OCTETS_PER_COUNT, "Complete UTF-8 width of the selected graph kind."
            ),
        ),
        bounds=(
            Bound(
                "source_edge_rows",
                COUNT,
                "upper",
                dimension("edges"),
                "One ordered input pass; no recomputation over a shrinking remaining graph.",
            ),
            Bound(
                "outgoing_edge_rows",
                COUNT,
                "upper",
                dimension("edges"),
                "Each popped vertex has one indexed outgoing range, visiting each edge once.",
            ),
            Bound(
                "seed_vertex_rows",
                COUNT,
                "upper",
                dimension("vertices"),
                "One pass over stored vertices seeds the initial ready queue.",
            ),
            Bound(
                "ready_rows",
                COUNT,
                "upper",
                dimension("vertices"),
                "A vertex becomes ready at most once and is removed by its exact key.",
            ),
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                multiply(number(8, SCALAR), records),
                "Includes both connections, schema, empty reads, point updates and queue "
                "operations.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                multiply(number(6, SCALAR), records),
                "All input, degree, seed, outgoing and queue rows are observed.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                multiply(records, widths),
                "Full endpoint and decimal-degree bytes are counted, without abbreviated records.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                multiply(records, widths),
                "All SQL parameter bytes, including repeatedly written keys and exact degrees.",
            ),
            Bound(
                "written_rows",
                COUNT,
                "upper",
                multiply(number(5, SCALAR), records),
                "Includes successful node insertion, degree replacement and ready "
                "insertion/deletion.",
            ),
            Bound(
                "written_octets",
                OCTETS,
                "upper",
                multiply(records, widths),
                "Full logical node/ready fields for every successful row mutation, including "
                "deletion.",
            ),
            Bound(
                "degree_decoded_octets",
                OCTETS,
                "upper",
                multiply(number(2, SCALAR), dimension("edges"), dimension("degree_octets")),
                "Python exact-integer degree updates decode at most two scalars per edge.",
            ),
            Bound(
                "degree_encoded_octets",
                OCTETS,
                "upper",
                multiply(number(2, SCALAR), dimension("edges"), dimension("degree_octets")),
                "Degree replacements encode decimal scalars without SQLite integer arithmetic.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                multiply(
                    records,
                    add(
                        number(4096, SCALAR),
                        multiply(
                            number(1024, SCALAR),
                            Expression("log2_ceil", terms=(add(records, number(1)),)),
                        ),
                    ),
                ),
                "Indexed B-tree work includes construction and mutation; VM constants are "
                "engine dependent.",
            ),
            Bound(
                "json_calls",
                COUNT,
                "equal",
                number(0),
                "Ordering uses existing endpoint identities, not assertion decoding.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "equal",
                number(0, OCTETS),
                "Cycle checking introduces no journal hashing or source authority.",
            ),
        ),
        conditions=(
            "The owning corpus edge primary key is (kind, source, target), retaining "
            "exact deduplication.",
            "The private ordering database uses bounded SQLite cache and disk-backed "
            "node/ready indexes.",
            "Inputs describe complete endpoint widths; ordering has an explicit B-tree "
            "logarithmic allowance.",
            "VM instructions describe the selected SQLite engine, not portable internal "
            "byte-comparison counts.",
        ),
        invariants=(
            "Both derivation and specialization are checked independently with the same "
            "exact cycle rule.",
            "Corpus facts and error ordering before cycle checking are unchanged.",
            "Failure releases ordering state; the ordering phase performs no source corpus writes.",
        ),
        limitations=(
            "This bounds complete-corpus cycle checking, not incremental per-snapshot "
            "graph updates.",
            "Source validation, corpus population, foreign references and "
            "selected-history closure have separate costs.",
            "File release and elapsed time are observations; scratch has a constant "
            "number of database files.",
        ),
    )


_CORPUS_ORDER_GROWTH = _corpus_order_declaration()


def _source_ingest_declaration() -> Declaration:
    def count(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    work = Expression(
        "sum",
        terms=(
            count(4),
            Expression("product", terms=(count(8, SCALAR), dimension("new_frames"))),
            Expression("product", terms=(count(5, SCALAR), dimension("new_assertions"))),
            dimension("new_references"),
        ),
    )
    return Declaration(
        id="riverhog-provenance.corpus-source-extension.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus.CanonicalCorpusValidator._add_source",),
        scope="native-corpus-raw-source-extension-after-exact-shared-prefix-proof",
        starts_at="receive one independently accepted immutable Native snapshot",
        ends_at="record only its missing complete source frames and the last atomic raw terminal",
        dimensions=(
            Dimension("new_frames", COUNT, "Source frames beyond the completed shared prefix."),
            Dimension("new_assertions", COUNT, "Raw assertions in those source frames."),
            Dimension("new_references", COUNT, "Raw external references in those source frames."),
            Dimension("frame_octets", OCTETS, "Complete newly loaded RFC 7464 frame octets."),
            Dimension("entry_octets", OCTETS, "Original JSON octets in the newly loaded frames."),
        ),
        bounds=(
            Bound(
                "source_frame_reads",
                COUNT,
                "equal",
                dimension("new_frames"),
                "Completed source frames are not fetched again, including shorter snapshots.",
            ),
            Bound(
                "source_frame_octets",
                OCTETS,
                "equal",
                dimension("frame_octets"),
                "Only original missing frame payloads are loaded.",
            ),
            Bound(
                "hashed_entry_octets",
                OCTETS,
                "equal",
                dimension("entry_octets"),
                "Each new original entry preimage matches its accepted Native header hash.",
            ),
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                work,
                "Includes corpus source/identity seeks, entry savepoints and all raw obligations.",
            ),
            Bound(
                "native_anchor_calls",
                COUNT,
                "upper",
                Expression("sum", terms=(dimension("new_frames"), count(1))),
                "At most one shared cut plus one exact header for each new frame.",
            ),
        ),
        conditions=(
            "Every input owns an open independently accepted frozen Native projection.",
            "Original journal identity, exact reference, prefix bytes and SHA agree at overlap.",
            "The caller retains admission and current authority for the selected source union.",
        ),
        invariants=(
            "An entry's raw assertions/references/fork and source continuation advance atomically.",
            "Retry unfinished entries while retaining each completed exact source terminal.",
            "Each selected snapshot still contributes its own effective semantic obligations.",
            "A shared prefix never makes a documentary dependency an effective selected root.",
            "Source identity/domain conflicts and divergent writers fail closed.",
        ),
        limitations=(
            "Native indexed headers and their SHA padding have separate delegate evidence.",
            "This profile counts payload reads, entry hashing and corpus SQL statement visits.",
            "Canonical encoding, index/metadata bytes and storage-engine work require composition.",
            "Effective snapshot traversal, corpus references, comparisons and cycles are separate.",
            "The corpus workspace lifetime is unchanged; this is not durable build restart proof.",
        ),
    )


class CanonicalCorpusValidator:
    """Keep corpus identity/dependency/edge indexes on disk, without a graph union.

    Every supplied journal must first pass canonical snapshot validation. This
    validates documentary identities, exact foreign references, forks, resolved
    comparisons and cross-journal acyclicity. It gives no precedence to a tail
    and does not make a dependency an effective selected root. Raw prefixes are
    reused only after an exact Native overlap proof. Each selected snapshot still
    contributes its own effective semantic obligations.
    """

    def __init__(self) -> None:
        self._scratch = TemporaryDirectory(prefix="riverhog-canonical-corpus-")
        self._db = sqlite3.connect(Path(self._scratch.name) / "corpus.sqlite3")
        self._db.execute("PRAGMA cache_size = -512")
        self._db.executescript(
            "CREATE TABLE identities (id TEXT PRIMARY KEY, domain TEXT, value BLOB);"
            "CREATE TABLE entries (journal TEXT, sequence TEXT, reference BLOB, prefix TEXT, "
            "bytes TEXT, PRIMARY KEY(journal, sequence));"
            "CREATE TABLE assertions (journal TEXT, assertion TEXT, entry BLOB, value BLOB, "
            "PRIMARY KEY(journal, assertion));"
            "CREATE TABLE refs (value BLOB PRIMARY KEY);"
            "CREATE TABLE forks (anchor BLOB PRIMARY KEY);"
            "CREATE TABLE comparisons (value BLOB PRIMARY KEY);"
            "CREATE TABLE edges (kind TEXT, source TEXT, target TEXT, "
            "PRIMARY KEY(kind, source, target));"
            "CREATE INDEX incoming ON edges(kind, target);"
            "CREATE TABLE source_extents (journal TEXT PRIMARY KEY, sequence TEXT NOT NULL);"
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._db.close()
        self._scratch.cleanup()

    def _source_read(self, query: str, parameters: tuple[object, ...]) -> Any:
        with closing(self._db.execute(query, parameters)) as rows:
            return rows.fetchone()

    def _source_write(self, query: str, parameters: tuple[object, ...] = ()) -> None:
        with closing(self._db.execute(query, parameters)):
            pass

    def _identity(self, identity: str, domain: str, value: bytes) -> None:
        old = self._source_read("SELECT domain, value FROM identities WHERE id = ?", (identity,))
        if old is not None and old != (domain, value):
            raise ProvenanceValidationError("immutable documentary identity or domain redefined")
        self._source_write(
            "INSERT OR IGNORE INTO identities VALUES (?, ?, ?)", (identity, domain, value)
        )

    @declare_growth(_source_ingest_declaration(), delegates=(("anchor", JournalSummary.anchor_at),))
    def _add_source(self, summary: JournalSummary) -> None:
        frame_count = len(summary.frames)
        self._identity(summary.journal_id, "journal", b"")
        extent = self._source_read(
            "SELECT sequence FROM source_extents WHERE journal = ?", (summary.journal_id,)
        )
        first = 0
        if extent is not None:
            through = min(int(extent[0]), frame_count - 1)
            anchor = summary.anchor_at(through)
            found = self._source_read(
                "SELECT reference, prefix, bytes FROM entries WHERE journal = ? AND sequence = ?",
                (summary.journal_id, str(through)),
            )
            if found != (
                canonical_document(anchor["through"]),
                anchor["prefix_sha256"],
                anchor["prefix_bytes"],
            ):
                raise ProvenanceValidationError(
                    "divergent writers reused the same journal identity"
                )
            first = through + 1
        for sequence in range(first, frame_count):
            frame = summary.frames[sequence]
            document = frame.document
            anchor = summary.anchor_at(sequence)
            if (
                anchor["through"]["entry_id"] != document["id"]
                or anchor["through"]["sequence"] != document["sequence"]
                or anchor["through"]["json_sha256"] != frame.sha256
            ):
                raise ProvenanceValidationError("accepted corpus source entry preimage changed")
            row = (
                canonical_document(anchor["through"]),
                anchor["prefix_sha256"],
                anchor["prefix_bytes"],
            )
            self._source_write("SAVEPOINT corpus_source_entry")
            try:
                self._identity(document["id"], "entry", frame.json_bytes)
                old = self._source_read(
                    "SELECT reference, prefix, bytes FROM entries "
                    "WHERE journal = ? AND sequence = ?",
                    (summary.journal_id, document["sequence"]),
                )
                if old is not None and old != row:
                    raise ProvenanceValidationError(
                        "divergent writers reused the same journal identity"
                    )
                self._source_write(
                    "INSERT OR IGNORE INTO entries VALUES (?, ?, ?, ?, ?)",
                    (summary.journal_id, document["sequence"], *row),
                )
                for _, assertion in iter_assertions(_body_assertions(document)):
                    encoded = canonical_document(assertion)
                    self._identity(assertion["assertion_id"], "assertion", encoded)
                    self._identity(assertion["id"], "referent", _identity_signature(assertion))
                    self._source_write(
                        "INSERT OR IGNORE INTO assertions VALUES (?, ?, ?, ?)",
                        (summary.journal_id, assertion["assertion_id"], row[0], encoded),
                    )
                for reference in _external_references(_body_assertions(document)):
                    self._source_write(
                        "INSERT OR IGNORE INTO refs VALUES (?)", (canonical_document(reference),)
                    )
                if sequence == 0:
                    parent = document["body"]["journal"].get("forked_from")
                    if parent is not None:
                        self._source_write(
                            "INSERT OR IGNORE INTO forks VALUES (?)", (canonical_document(parent),)
                        )
                self._source_write(
                    "INSERT INTO source_extents VALUES (?, ?) "
                    "ON CONFLICT(journal) DO UPDATE SET sequence=excluded.sequence",
                    (summary.journal_id, str(sequence)),
                )
            except BaseException:
                self._source_write("ROLLBACK TO corpus_source_entry")
                self._source_write("RELEASE corpus_source_entry")
                raise
            else:
                self._source_write("RELEASE corpus_source_entry")

    def add(self, summary: JournalSummary) -> None:
        self._add_source(summary)
        for reference in summary.graph_validation.external_references:
            self._db.execute(
                "INSERT OR IGNORE INTO refs VALUES (?)", (canonical_document(reference),)
            )
        for assertion in summary.graph_validation.objects.values():
            kind = assertion["type"]
            if kind == "content_comparison":
                # Keep the independently resolved local objects with this row.
                local = {
                    key: summary.graph_validation.objects.get(assertion[key]["object_id"])
                    for key in ("left_description", "right_description")
                    if assertion[key]["scope"] == "local"
                }
                self._db.execute(
                    "INSERT OR IGNORE INTO comparisons VALUES (?)",
                    (canonical_document({"row": assertion, "local": local}),),
                )
            if kind in ("derivation", "specialization"):
                fields = (
                    ("used_state", "generated_state")
                    if kind == "derivation"
                    else ("specific", "general")
                )
                self._db.execute(
                    "INSERT OR IGNORE INTO edges VALUES (?, ?, ?)",
                    (kind, assertion[fields[0]]["object_id"], assertion[fields[1]]["object_id"]),
                )

    def _resolve(self, reference: dict[str, Any]) -> dict[str, Any]:
        found = self._db.execute(
            "SELECT entry, value FROM assertions WHERE journal = ? AND assertion = ?",
            (reference["journal_id"], reference["assertion_id"]),
        ).fetchone()
        if found is None:
            raise ProvenanceValidationError("unresolved required external journal assertion")
        entry, encoded = found
        row = cast(dict[str, Any], json.loads(encoded))
        if (
            entry != canonical_document(reference["entry"])
            or row["id"] != reference["object_id"]
            or row["type"] != reference["object_type"]
        ):
            raise ProvenanceValidationError("foreign reference does not match its exact assertion")
        return row

    def validate(self) -> None:
        for (encoded,) in self._db.execute("SELECT value FROM refs"):
            self._resolve(json.loads(encoded))
        for (encoded,) in self._db.execute("SELECT anchor FROM forks"):
            anchor = json.loads(encoded)
            found = self._db.execute(
                "SELECT reference, prefix, bytes FROM entries WHERE journal = ? AND sequence = ?",
                (anchor["journal_id"], anchor["through"]["sequence"]),
            ).fetchone()
            if found != (
                canonical_document(anchor["through"]),
                anchor["prefix_sha256"],
                anchor["prefix_bytes"],
            ):
                raise ProvenanceValidationError("fork prefix anchor could not be resolved exactly")
        for (encoded,) in self._db.execute("SELECT value FROM comparisons"):
            comparison = json.loads(encoded)
            row = comparison["row"]
            descriptions = [
                comparison["local"][key]
                if row[key]["scope"] == "local"
                else self._resolve(row[key])
                for key in ("left_description", "right_description")
            ]
            if any(item is None or "content" not in item for item in descriptions):
                if row["result"] != "indeterminate":
                    raise ProvenanceValidationError("resolved comparison lacks complete fixity")
            else:
                same = fingerprint(descriptions[0]["content"]) == fingerprint(
                    descriptions[1]["content"]
                )
                if (row["result"] == "matching_fixity" and not same) or (
                    row["result"] == "different" and same
                ):
                    raise ProvenanceValidationError(
                        "comparison contradicts foreign content evidence"
                    )
        for kind in ("derivation", "specialization"):
            self._acyclic(kind)

    @declare_growth(_CORPUS_ORDER_GROWTH)
    def _acyclic(self, kind: str) -> None:
        # Only indegrees and pending vertex identities are duplicated. The owned
        # corpus edge index supplies both the input pass and exact outgoing ranges.
        # Separate scratch keeps cancellation from leaving partially built work in
        # the corpus connection, whose facts remain available for a fresh retry.
        with (
            TemporaryDirectory(prefix="order-", dir=self._scratch.name) as directory,
            closing(sqlite3.connect(Path(directory) / "order.sqlite3")) as work,
        ):
            with closing(work.execute("PRAGMA cache_size = -512")):
                pass
            with closing(work.execute("BEGIN")):
                pass
            with closing(
                work.execute("CREATE TABLE nodes (id TEXT PRIMARY KEY, degree TEXT NOT NULL)")
            ):
                pass
            with closing(work.execute("CREATE TABLE ready (id TEXT PRIMARY KEY)")):
                pass
            vertices = 0
            with closing(
                self._db.execute(
                    "SELECT source, target FROM edges WHERE kind = ? ORDER BY source, target",
                    (kind,),
                )
            ) as edges:
                for source, target in edges:
                    with closing(
                        work.execute("INSERT OR IGNORE INTO nodes VALUES (?, '0')", (source,))
                    ) as inserted:
                        vertices += inserted.rowcount
                    with closing(
                        work.execute("SELECT degree FROM nodes WHERE id = ?", (target,))
                    ) as cursor:
                        found = cursor.fetchone()
                    if found is None:
                        with closing(work.execute("INSERT INTO nodes VALUES (?, '1')", (target,))):
                            pass
                        vertices += 1
                    else:
                        degree = str(int(found[0]) + 1)
                        with closing(
                            work.execute(
                                "UPDATE nodes SET degree = ? WHERE id = ?", (degree, target)
                            )
                        ):
                            pass
            with closing(work.execute("SELECT id, degree FROM nodes ORDER BY id")) as nodes:
                for identity, degree in nodes:
                    if degree == "0":
                        with closing(work.execute("INSERT INTO ready VALUES (?)", (identity,))):
                            pass
            completed = 0
            while True:
                with closing(work.execute("SELECT id FROM ready ORDER BY id LIMIT 1")) as cursor:
                    pending = cursor.fetchone()
                if pending is None:
                    break
                source = pending[0]
                with closing(work.execute("DELETE FROM ready WHERE id = ?", (source,))):
                    pass
                with closing(
                    self._db.execute(
                        "SELECT target FROM edges WHERE kind = ? AND source = ? ORDER BY target",
                        (kind, source),
                    )
                ) as edges:
                    for (target,) in edges:
                        with closing(
                            work.execute("SELECT degree FROM nodes WHERE id = ?", (target,))
                        ) as cursor:
                            found = cursor.fetchone()
                        if found is None:
                            raise RuntimeError("corpus ordering indegree is inconsistent")
                        remaining = int(found[0]) - 1
                        if remaining < 0:
                            raise RuntimeError("corpus ordering indegree is inconsistent")
                        degree = str(remaining)
                        with closing(
                            work.execute(
                                "UPDATE nodes SET degree = ? WHERE id = ?", (degree, target)
                            )
                        ):
                            pass
                        if degree == "0":
                            with closing(work.execute("INSERT INTO ready VALUES (?)", (target,))):
                                pass
                completed += 1
            if completed != vertices:
                raise ProvenanceValidationError(f"cross-journal {kind} graph contains a cycle")


def validate_canonical_corpus(summaries: Iterable[JournalSummary]) -> None:
    with CanonicalCorpusValidator() as validator:
        for summary in summaries:
            validator.add(summary)
        validator.validate()


__all__ = ["CanonicalCorpusValidator", "validate_canonical_corpus"]

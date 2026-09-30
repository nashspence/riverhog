"""Disk-backed validation across independently evaluated canonical journals."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self, cast

from riverhog_provenance_contracts import canonical_document

from .common import fingerprint
from .errors import ProvenanceValidationError
from .graph import iter_assertions
from .history import _external_references
from .journal import JournalSummary, _body_assertions, _identity_signature


class CanonicalCorpusValidator:
    """Keep corpus identity/dependency/edge indexes on disk, without a graph union.

    Every supplied journal must first pass canonical snapshot validation. This
    validates documentary identities, exact foreign references, forks, resolved
    comparisons and cross-journal acyclicity. It gives no precedence to a tail
    and does not make a dependency an effective selected root.
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
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self._db.close()
        self._scratch.cleanup()

    def _identity(self, identity: str, domain: str, value: bytes) -> None:
        old = self._db.execute(
            "SELECT domain, value FROM identities WHERE id = ?", (identity,)
        ).fetchone()
        if old is not None and old != (domain, value):
            raise ProvenanceValidationError("immutable documentary identity or domain redefined")
        self._db.execute(
            "INSERT OR IGNORE INTO identities VALUES (?, ?, ?)", (identity, domain, value)
        )

    def add(self, summary: JournalSummary) -> None:
        import hashlib

        self._identity(summary.journal_id, "journal", b"")
        prefix = hashlib.sha256()
        byte_count = 0
        for frame in summary.frames:
            prefix.update(frame.encoded)
            byte_count += len(frame.encoded)
            self._identity(frame.document["id"], "entry", frame.json_bytes)
            row = (canonical_document(frame.reference), prefix.hexdigest(), str(byte_count))
            old = self._db.execute(
                "SELECT reference, prefix, bytes FROM entries WHERE journal = ? AND sequence = ?",
                (summary.journal_id, frame.document["sequence"]),
            ).fetchone()
            if old is not None and old != row:
                raise ProvenanceValidationError(
                    "divergent writers reused the same journal identity"
                )
            self._db.execute(
                "INSERT OR IGNORE INTO entries VALUES (?, ?, ?, ?, ?)",
                (summary.journal_id, frame.document["sequence"], *row),
            )
            for _, assertion in iter_assertions(_body_assertions(frame.document)):
                encoded = canonical_document(assertion)
                self._identity(assertion["assertion_id"], "assertion", encoded)
                self._identity(assertion["id"], "referent", _identity_signature(assertion))
                self._db.execute(
                    "INSERT OR IGNORE INTO assertions VALUES (?, ?, ?, ?)",
                    (summary.journal_id, assertion["assertion_id"], row[0], encoded),
                )
            for reference in _external_references(_body_assertions(frame.document)):
                self._db.execute(
                    "INSERT OR IGNORE INTO refs VALUES (?)", (canonical_document(reference),)
                )
        for reference in summary.graph_validation.external_references:
            self._db.execute(
                "INSERT OR IGNORE INTO refs VALUES (?)", (canonical_document(reference),)
            )
        parent = summary.frames[0].document["body"]["journal"].get("forked_from")
        if parent is not None:
            self._db.execute(
                "INSERT OR IGNORE INTO forks VALUES (?)", (canonical_document(parent),)
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
            # Kahn elimination on a bounded persistent index. No Python node set
            # or recursive call stack grows with the collection's edge surface.
            self._db.execute(
                "CREATE TEMP TABLE remaining AS SELECT source, target FROM edges WHERE kind = ?",
                (kind,),
            )
            self._db.execute("CREATE INDEX remaining_targets ON remaining(target)")
            while self._db.execute("SELECT 1 FROM remaining LIMIT 1").fetchone() is not None:
                sources = self._db.execute(
                    "SELECT DISTINCT source FROM remaining WHERE source NOT IN "
                    "(SELECT target FROM remaining) LIMIT 128"
                ).fetchall()
                if not sources:
                    raise ProvenanceValidationError(f"cross-journal {kind} graph contains a cycle")
                self._db.executemany("DELETE FROM remaining WHERE source = ?", sources)
            self._db.execute("DROP TABLE remaining")


def validate_canonical_corpus(summaries: Iterable[JournalSummary]) -> None:
    with CanonicalCorpusValidator() as validator:
        for summary in summaries:
            validator.add(summary)
        validator.validate()


__all__ = ["CanonicalCorpusValidator", "validate_canonical_corpus"]

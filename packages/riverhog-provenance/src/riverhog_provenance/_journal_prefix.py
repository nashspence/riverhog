"""Exact accepted-prefix checkpoints for an owned native replay.

This is the prefix phase, not a resumable semantic validator. Its caller completes
native frame checks before committing. A durable owner must retain and authenticate
the source and semantic workspace as well as this checkpoint. The synchronous
validator's projection remains disposable.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import (
    ENTRY_SCHEMA,
    ContractCatalog,
    require_canonical_uuid_urn,
)
from sha256_continuation import CheckpointSHA256, sha256_compress

from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ProvenanceValidationError
from .graph import time_ns

if TYPE_CHECKING:
    from .journal import JournalFrame

ALGORITHM = "riverhog-journal-prefix/v1"
# UUID references, SHA-256's owning source/count domain, and its <=512-character
# continuation codec fit this fixed private header envelope. It is not a journal
# or entry-size limit.
_CHECKPOINT_CHARACTERS = 2048


def _preparation_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog-provenance.sqlite-journal-prefix-preparation.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._journal_prefix.JournalPrefix.prepare",),
        scope="native-sqlite-accepted-prefix-entry-preparation",
        starts_at="load an exact owned prefix and one admitted immutable entry atom",
        ends_at="close metadata cursors and return the detached exact next-prefix proposal",
        dimensions=(
            Dimension("retained_frames", COUNT, "Previously accepted frame records."),
            Dimension("frame_octets", OCTETS, "Exact framed octets in this entry atom."),
            Dimension("initial_tail_octets", OCTETS, "Retained SHA-256 source tail, zero to 63."),
            Dimension("source_blocks", COUNT, "floor((initial_tail_octets + frame_octets) / 64)."),
        ),
        bounds=(
            Bound(
                "sql_statements", COUNT, "upper", count(2), "Header and predecessor point seeks."
            ),
            Bound("returned_rows", COUNT, "upper", count(2), "One fixed header and one reference."),
            Bound("changed_rows", COUNT, "equal", count(0), "Preparation publishes no prefix."),
            Bound(
                "metadata_octets",
                OCTETS,
                "upper",
                Expression("constant", value=_CHECKPOINT_CHARACTERS + 512, unit=OCTETS),
                "The valid ASCII checkpoint shape and bounded canonical predecessor reference.",
            ),
            Bound(
                "source_hashed_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="frame_octets"),
                "Only this exact framed entry advances the source SHA continuation.",
            ),
            Bound(
                "native_hashed_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="frame_octets"),
                "The native predecessor reference hashes this entry's JSON octets once.",
            ),
            Bound(
                "sha256_compression_calls",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(2),
                        Expression(
                            "delegate",
                            name="source_update",
                            metric="compression_calls",
                            terms=(count(1),),
                            arguments=(
                                ("input_octets", Expression("dimension", name="frame_octets")),
                                (
                                    "initial_tail_octets",
                                    Expression("dimension", name="initial_tail_octets"),
                                ),
                                ("blocks", Expression("dimension", name="source_blocks")),
                            ),
                        ),
                    ),
                ),
                "The neutral update delegate and at most two checkpoint-prefix padding blocks.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(1024),
                        Expression(
                            "product",
                            terms=(
                                count(256),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="retained_frames"),
                                                count(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Fixed header control and indexed predecessor lookup growth.",
            ),
        ),
        conditions=(
            "The owner retains exact immutable source and an integrity-checked private workspace.",
            "The existing entry-byte contract bounds this atom, without a total journal cap.",
            "Source tail and source_blocks describe the actual restored SHA-256 continuation.",
        ),
        invariants=(
            "No accepted prefix, graph fact or frame is published during preparation.",
            "No preceding source frame is loaded or rehashed to continue the original SHA-256.",
            "The shared backend preserves ordinary SHA-256 and its reviewed continuation codec.",
        ),
        limitations=(
            "Canonical decoding, the pinned entry schema, codec handling and physical CPU/RAM "
            "have entry-bounded costs outside these primitive counters.",
            "Source acquisition, native semantic checks, commit, terminalization and cleanup "
            "are separate phases; this does not establish whole-journal resumability "
            "or a full validator bound.",
            "Lock waits, corrupt-state handling and failed/retried work are separate observations.",
        ),
    )


def _publication_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog-provenance.sqlite-journal-prefix-publication.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._journal_prefix.JournalPrefix.commit",),
        scope="native-sqlite-accepted-prefix-frame-publication",
        starts_at="receive one exact prepared frame after native semantic checks",
        ends_at="commit its original bytes and fenced prefix checkpoint together",
        dimensions=(
            Dimension("retained_frames", COUNT, "Previously accepted frame records."),
            Dimension("frame_octets", OCTETS, "Exact framed octets of the prepared entry."),
        ),
        bounds=(
            Bound(
                "sql_statements", COUNT, "upper", count(4), "Begin, header CAS, frame and commit."
            ),
            Bound("returned_rows", COUNT, "equal", count(0), "No historical source is returned."),
            Bound("changed_rows", COUNT, "equal", count(2), "One header and one exact frame."),
            Bound(
                "published_metadata_octets",
                OCTETS,
                "upper",
                Expression("constant", value=_CHECKPOINT_CHARACTERS + 512 + 20 + 512, unit=OCTETS),
                "The header, predecessor, ordinal and accepted frame SHA continuation.",
            ),
            Bound(
                "published_payload_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="frame_octets"),
                "One original JSON entry, without a cumulative-prefix copy.",
            ),
            Bound(
                "source_hashed_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Source hashing completed during detached preparation.",
            ),
            Bound(
                "sha256_compression_calls",
                COUNT,
                "upper",
                count(2),
                "Only a matched anchor may finalize a copy with at most two padding blocks.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(1024),
                        Expression(
                            "product",
                            terms=(
                                count(256),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="retained_frames"),
                                                count(2),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Fixed checkpoint writes and indexed frame insertion growth.",
            ),
        ),
        conditions=(
            "The owning private schema has no application-supplied triggers.",
            "The owner completed the native checks for this exact prepared entry and attempt.",
            "The existing entry-byte contract bounds this atom, without a total journal cap.",
        ),
        invariants=(
            "Frame bytes, predecessor, source SHA state and accepted watermark commit together.",
            "A superseded proposal cannot write or replace the current accepted prefix.",
            "No source prefix is reread, rehashed or copied during this publication phase.",
        ),
        limitations=(
            "Prior native semantic writes retain their own work and durability costs.",
            "Codec handling, anchor finalization, physical CPU, durable I/O and lock waits "
            "are not physical resource guarantees from these counters.",
            "Rejected proposals and rollback have separate correctness and attempt evidence.",
            "This phase does not establish resumability of the whole semantic validator.",
        ),
    )


def _publication_phase_declaration(operation: str) -> Declaration:
    base = _publication_declaration()
    planning = operation == "prepare_publication"

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    bounds = tuple(
        replace(
            bound,
            relation="equal" if planning else bound.relation,
            expression=(
                Expression("dimension", name="padding_blocks")
                if planning and bound.metric == "sha256_compression_calls"
                else Expression("constant", value=0, unit=bound.unit)
                if planning or bound.metric == "sha256_compression_calls"
                else count(2)
                if bound.metric == "sql_statements"
                else bound.expression
            ),
            meaning=(
                "Detached planning publishes nothing; only a matched anchor finalizes SHA padding."
                if planning
                else "The caller transaction contains exactly the fenced header and frame writes."
            ),
        )
        for bound in base.bounds
    )
    padding = (
        tuple(
            Bound(
                "padding_" + metric,
                unit,
                "equal",
                Expression(
                    "delegate",
                    name="padding",
                    metric=metric,
                    terms=(Expression("dimension", name="padding_blocks"),),
                    arguments=(("block_octets", Expression("constant", value=64, unit=OCTETS)),),
                ),
                "Actual native SHA padding calls, already included in the compression count.",
            )
            for metric, unit in (
                ("block_input_octets", OCTETS),
                ("rotate_calls", COUNT),
                ("returned_words", COUNT),
            )
        )
        if planning
        else ()
    )
    return replace(
        base,
        id="riverhog-provenance.sqlite-journal-prefix-publication-"
        + ("planning" if planning else "application")
        + ".growth/v1",
        subjects=("riverhog_provenance._journal_prefix.JournalPrefix." + operation,),
        scope="native-sqlite-accepted-prefix-"
        + ("detached-publication-plan" if planning else "caller-transaction-publication"),
        starts_at="receive an exact prepared frame under its owner binding",
        ends_at=(
            "return detached prefix metadata without database work"
            if planning
            else "return after two writes while preserving the caller transaction"
        ),
        dimensions=(
            *base.dimensions,
            *(
                (
                    Dimension(
                        "padding_blocks",
                        COUNT,
                        "Zero without an anchor entry match; otherwise one for a prepared SHA tail "
                        "below 56 octets, two for 56 through 63.",
                    ),
                )
                if planning
                else ()
            ),
        ),
        bounds=(*bounds, *padding),
        conditions=(
            *base.conditions,
            "Padding count is derived from the input anchor entry identity and prepared SHA "
            "continuation, not from measured calls.",
            "Application requires the caller transaction; planning does not change its "
            "transaction state.",
        ),
        invariants=(
            *base.invariants,
            "Neither phase commits nor rolls back its caller's transaction.",
        ),
    )


def _composed_publication_declaration() -> Declaration:
    base = _publication_declaration()
    planning = _publication_phase_declaration("prepare_publication")
    application = _publication_phase_declaration("_apply")
    subtotals = tuple(
        Bound(
            "prefix_" + bound.metric,
            bound.unit,
            "upper",
            Expression(
                "sum",
                terms=tuple(
                    Expression(
                        "delegate",
                        name=name,
                        metric=bound.metric,
                        terms=(Expression("constant", value=1, unit=COUNT),),
                        arguments=tuple(
                            (dimension.name, Expression("dimension", name=dimension.name))
                            for dimension in child.dimensions
                        ),
                    )
                    for name, child in (("plan", planning), ("application", application))
                ),
            ),
            "Actual phase subtotal, already included in the inclusive primitive observation.",
        )
        for bound in base.bounds
    )
    return replace(
        base,
        dimensions=planning.dimensions,
        bounds=(*base.bounds, *subtotals),
        invariants=(
            *base.invariants,
            "Actual planning and application compose without charging their inclusive work twice.",
        ),
    )


class StaleJournalPrefix(RuntimeError):
    pass


def _natural(value: Any) -> int:
    if type(value) is not str or re.fullmatch(r"0|[1-9][0-9]*", value) is None:
        raise ValueError("noncanonical prefix scalar")
    return int(value)


def _reference(value: Any) -> dict[str, str]:
    if type(value) is not dict or set(value) != {"entry_id", "sequence", "json_sha256"}:
        raise ValueError("invalid prefix predecessor")
    require_canonical_uuid_urn(value["entry_id"])
    _natural(value["sequence"])
    if (
        type(value["json_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", value["json_sha256"]) is None
    ):
        raise ValueError("invalid prefix predecessor digest")
    return value


@dataclass(frozen=True, slots=True)
class PrefixState:
    frame_count: int
    byte_count: int
    journal_id: str | None
    previous_json: str | None
    sha256_state: str
    matched_anchor: bool

    @classmethod
    def empty(cls) -> PrefixState:
        return cls(0, 0, None, None, CheckpointSHA256().export_state(), False)

    @property
    def previous(self) -> dict[str, str] | None:
        return None if self.previous_json is None else _reference(json.loads(self.previous_json))

    @property
    def hexdigest(self) -> str:
        return CheckpointSHA256.from_state(self.sha256_state).hexdigest()

    def to_json(self) -> str:
        return canonical_json_bytes(
            {
                "algorithm": ALGORITHM,
                "frame_count": str(self.frame_count),
                "byte_count": str(self.byte_count),
                "journal_id": self.journal_id,
                "previous": self.previous,
                "sha256_state": self.sha256_state,
                "matched_anchor": self.matched_anchor,
            }
        ).decode()

    @classmethod
    def from_json(cls, raw: str) -> PrefixState:
        try:
            if type(raw) is not str or len(raw) > _CHECKPOINT_CHARACTERS:
                raise ValueError("prefix checkpoint exceeds its fixed shape")
            value = json.loads(raw)
            if (
                type(value) is not dict
                or set(value)
                != {
                    "algorithm",
                    "frame_count",
                    "byte_count",
                    "journal_id",
                    "previous",
                    "sha256_state",
                    "matched_anchor",
                }
                or value["algorithm"] != ALGORITHM
            ):
                raise ValueError("invalid prefix algorithm or shape")
            count, size = _natural(value["frame_count"]), _natural(value["byte_count"])
            digest = CheckpointSHA256.from_state(value["sha256_state"])
            if digest.total_bytes != size or type(value["matched_anchor"]) is not bool:
                raise ValueError("prefix source position differs from its SHA checkpoint")
            if count:
                require_canonical_uuid_urn(value["journal_id"])
                previous = _reference(value["previous"])
                if _natural(previous["sequence"]) != count - 1 or size < 2 * count:
                    raise ValueError("prefix predecessor differs from its watermark")
                previous_json = canonical_json_bytes(previous).decode()
            else:
                if (
                    size
                    or value["journal_id"] is not None
                    or value["previous"] is not None
                    or value["matched_anchor"]
                    or digest.export_state() != cls.empty().sha256_state
                ):
                    raise ValueError("nonempty initial prefix")
                previous_json = None
            state = cls(
                count,
                size,
                value["journal_id"],
                previous_json,
                digest.export_state(),
                value["matched_anchor"],
            )
            if state.to_json() != raw:
                raise ValueError("noncanonical prefix checkpoint")
            return state
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise RuntimeError("invalid retained journal prefix checkpoint") from exc


@dataclass(frozen=True, slots=True)
class PreparedEntry:
    before: PrefixState
    before_json: str
    frame: JournalFrame
    attempt_id: str
    binding: bytes
    journal_id: str
    reference_json: str
    sha256_state: str

    @property
    def document(self) -> dict[str, Any]:
        return self.frame.document


@dataclass(frozen=True)
class PrefixPublication:
    prepared: PreparedEntry
    after: PrefixState


def install(db: sqlite3.Connection) -> None:
    """Initialize a fresh owning workspace; never upgrade or replace one."""
    db.executescript(
        "CREATE TABLE frames(sequence TEXT PRIMARY KEY, value BLOB, reference BLOB, "
        "prefix_state TEXT NOT NULL);"
        "CREATE TABLE journal_prefix(singleton INTEGER PRIMARY KEY CHECK(singleton = 1), "
        "attempt_id TEXT NOT NULL, binding BLOB NOT NULL, checkpoint TEXT NOT NULL);"
    )


class JournalPrefix:
    def __init__(
        self,
        db: sqlite3.Connection,
        *,
        attempt_id: str,
        source_binding: str,
        catalog: ContractCatalog,
        require_profiles: bool,
        expected_anchor: Mapping[str, Any] | None = None,
        require_exact_tail: bool = False,
    ) -> None:
        if any(type(value) is not str or not value for value in (attempt_id, source_binding)):
            raise ValueError("prefix requires exact owned attempt and source identities")
        if type(require_profiles) is not bool or type(require_exact_tail) is not bool:
            raise ValueError("prefix requires exact validation modes")
        self.db, self.attempt_id = db, attempt_id
        self.catalog_sha256 = catalog.checkpoint_sha256()
        # Detach the caller's mapping: this owner binds the complete original mode.
        self.anchor_json = canonical_json_bytes(dict(expected_anchor)) if expected_anchor else None
        self.require_exact_tail = require_exact_tail
        self.binding = canonical_json_bytes(
            {
                "algorithm": ALGORITHM,
                "source_binding": source_binding,
                "catalog_sha256": self.catalog_sha256,
                "require_profiles": require_profiles,
                "expected_anchor": self.expected_anchor,
                "require_exact_tail": require_exact_tail,
            }
        )

    @property
    def expected_anchor(self) -> dict[str, Any] | None:
        return None if self.anchor_json is None else json.loads(self.anchor_json)

    def _initialize(self) -> PrefixState:
        if not self.db.in_transaction:
            raise RuntimeError("prefix initialization requires an owner transaction")
        with closing(self.db.execute("SELECT 1 FROM frames LIMIT 1")) as rows:
            if rows.fetchone() is not None:
                raise RuntimeError("prefix initialization requires a fresh frame directory")
        state = PrefixState.empty()
        self.db.execute(
            "INSERT INTO journal_prefix VALUES (1, ?, ?, ?)",
            (self.attempt_id, self.binding, state.to_json()),
        ).close()
        return state

    def initialize(self) -> PrefixState:
        if self.db.in_transaction:
            raise RuntimeError("prefix binding requires a closed owner transaction")
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            self._initialize()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return self.load()

    def load(self) -> PrefixState:
        with closing(
            self.db.execute(
                "SELECT substr(checkpoint, 1, ?) FROM journal_prefix WHERE singleton = 1 "
                "AND attempt_id = ? AND binding = ?",
                (_CHECKPOINT_CHARACTERS + 1, self.attempt_id, self.binding),
            )
        ) as rows:
            row = rows.fetchone()
        if row is None:
            raise StaleJournalPrefix("journal prefix differs from its owned replay inputs")
        state = PrefixState.from_json(row[0])
        if state.frame_count:
            with closing(
                self.db.execute(
                    "SELECT substr(reference, 1, 512) FROM frames WHERE sequence = ?",
                    (str(state.frame_count - 1),),
                )
            ) as rows:
                previous = rows.fetchone()
            if (
                previous is None
                or state.previous_json is None
                or previous[0] != state.previous_json.encode()
            ):
                raise RuntimeError("journal prefix has a missing or changed predecessor link")
        return state

    @declare_growth(
        _preparation_declaration(),
        delegates=(("source_update", CheckpointSHA256.update_bytes),),
    )
    def prepare(self, frame: JournalFrame, catalog: ContractCatalog) -> PreparedEntry:
        """Run native entry/chain checks before the caller's graph/profile checks."""
        if self.db.in_transaction:
            raise RuntimeError("prefix preparation requires a closed owner transaction")
        if len(frame.json_bytes) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise ProvenanceValidationError("journal entry exceeds the admission byte limit")
        if catalog.checkpoint_sha256() != self.catalog_sha256:
            raise StaleJournalPrefix("journal prefix catalog changed")
        state = self.load()
        sequence, document = state.frame_count, frame.document
        try:
            catalog.validate(ENTRY_SCHEMA, document)
            time_ns(document["recorded_at"])
        except (ValueError, TypeError) as exc:
            raise ProvenanceValidationError(f"entry {sequence}: {exc}") from exc
        if int(document["sequence"]) != sequence:
            raise ProvenanceValidationError("noncontiguous journal sequence")
        if state.journal_id is None:
            if (
                document["entry_kind"] != "journal_init"
                or document["body"]["journal"]["id"] != document["journal_id"]
            ):
                raise ProvenanceValidationError("first entry must initialize this journal")
            parent = document["body"]["journal"].get("forked_from")
            if parent and parent["journal_id"] == document["journal_id"]:
                raise ProvenanceValidationError(
                    "independent journal fork requires a distinct journal identity"
                )
        else:
            if (
                document["journal_id"] != state.journal_id
                or document["entry_kind"] == "journal_init"
            ):
                raise ProvenanceValidationError("journal identity or initialization changed")
            if document["previous_entry"] != state.previous:
                raise ProvenanceValidationError(
                    "predecessor does not match exact previous JSON text"
                )
        if document["entry_kind"] == "checkpoint":
            body = document["body"]
            if state.previous is None or body["covered_through"] != state.previous:
                raise ProvenanceValidationError(
                    "checkpoint must cover the immediately preceding entry"
                )
            if (
                body["prefix_sha256"] != state.hexdigest
                or int(body["prefix_bytes"]) != state.byte_count
            ):
                raise ProvenanceValidationError("checkpoint prefix commitment mismatch")
        digest = CheckpointSHA256.from_state(state.sha256_state)
        digest.update_bytes(frame.encoded)
        return PreparedEntry(
            state,
            state.to_json(),
            frame,
            self.attempt_id,
            self.binding,
            document["journal_id"],
            canonical_json_bytes(frame.reference).decode(),
            digest.export_state(),
        )

    @declare_growth(
        _publication_phase_declaration("prepare_publication"),
        delegates=(("padding", sha256_compress),),
    )
    def prepare_publication(self, prepared: PreparedEntry) -> PrefixPublication:
        """Prepare native anchor and hash metadata without publishing its prefix."""
        if (prepared.attempt_id, prepared.binding) != (self.attempt_id, self.binding):
            raise StaleJournalPrefix("journal prefix proposal belongs to another replay")
        before = prepared.before
        digest = CheckpointSHA256.from_state(prepared.sha256_state)
        reference = _reference(json.loads(prepared.reference_json))
        matched = before.matched_anchor
        expected = self.expected_anchor
        if expected and reference["entry_id"] == expected["through"]["entry_id"]:
            candidate = {
                "journal_id": prepared.journal_id,
                "through": reference,
                "prefix_sha256": digest.hexdigest(),
                "prefix_bytes": str(digest.total_bytes),
            }
            if candidate != expected:
                raise ProvenanceValidationError("externally supplied prefix anchor mismatch")
            matched = True
        after = PrefixState(
            before.frame_count + 1,
            digest.total_bytes,
            prepared.journal_id,
            prepared.reference_json,
            digest.export_state(),
            matched,
        )
        return PrefixPublication(prepared, after)

    @declare_growth(_publication_phase_declaration("_apply"))
    def _apply(self, batch: PrefixPublication) -> PrefixState:
        if not self.db.in_transaction:
            raise RuntimeError("prefix application requires an owner transaction")
        prepared, after = (batch.prepared, batch.after)
        before, frame = (prepared.before, prepared.frame)
        if (prepared.attempt_id, prepared.binding) != (self.attempt_id, self.binding):
            raise StaleJournalPrefix("journal prefix proposal belongs to another replay")
        with closing(
            self.db.execute(
                "UPDATE journal_prefix SET checkpoint = ? WHERE singleton = 1 "
                "AND attempt_id = ? AND binding = ? AND checkpoint = ?",
                (after.to_json(), self.attempt_id, self.binding, prepared.before_json),
            )
        ) as rows:
            if rows.rowcount != 1:
                raise StaleJournalPrefix("journal prefix proposal was superseded")
        self.db.execute(
            "INSERT INTO frames VALUES (?, ?, ?, ?)",
            (
                str(before.frame_count),
                frame.json_bytes,
                prepared.reference_json.encode(),
                after.sha256_state,
            ),
        ).close()
        return after

    @declare_growth(
        _composed_publication_declaration(),
        delegates=(("plan", prepare_publication), ("application", _apply)),
    )
    def commit(self, prepared: PreparedEntry) -> PrefixState:
        """Publish the exact accepted frame and hash watermark after native checks."""
        if (prepared.attempt_id, prepared.binding) != (self.attempt_id, self.binding):
            raise StaleJournalPrefix("journal prefix proposal belongs to another replay")
        try:
            batch = self.prepare_publication(prepared)
            if not self.db.in_transaction:
                self.db.execute("BEGIN IMMEDIATE").close()
            after = self._apply(batch)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return after

    def finish(self) -> PrefixState:
        state = self.load()
        if not state.frame_count:
            raise ProvenanceValidationError("journal is empty")
        expected = self.expected_anchor
        if expected and not state.matched_anchor:
            raise ProvenanceValidationError("journal does not contain the expected anchored prefix")
        if self.require_exact_tail and (expected is None or state.previous != expected["through"]):
            raise ProvenanceValidationError("an exact tail anchor was required")
        return state

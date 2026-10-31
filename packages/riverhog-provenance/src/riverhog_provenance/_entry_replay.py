"""Owned native entry subphases with one atomic parent checkpoint per unit.

The source owner admits exact frames and retains the workspace. This conductor
does not supply source authentication, provider fencing or archive authority.
Only the current frame and current assertion are cached; reopening reconstructs
those admitted atoms rather than revalidating an accepted journal prefix.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_provenance_contracts import ContractCatalog

from ._assertion_staging import AssertionStaging
from ._changed_assertions import read_changed_assertion
from ._correction_projection import CorrectionWork
from ._correction_targets import CorrectionTargets
from ._entry_preparation import EntryPreparation
from ._generic_semantics import GenericAtom, GenericSemantics
from ._journal_prefix import JournalPrefix, PreparedEntry
from ._online_dag import OnlineDag, _Snapshot
from ._semantic_finish import SemanticFinish
from ._semantic_program import ROW_TYPES, Nodes
from ._semantic_source import SemanticSource
from ._session_checkpoint import (
    StaleValidationCheckpoint,
    ValidationCheckpoint,
    ValidationLedger,
    ValidationSnapshot,
)
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import JournalValidationCapacity
from .graph import time_ns
from .journal import JournalFrame
from .journal_store import JournalStore

if TYPE_CHECKING:
    from .graph_certificates import JournalGraphCertificates

type _Child = (
    EntryPreparation
    | CorrectionTargets
    | AssertionStaging
    | CorrectionWork
    | GenericSemantics
    | SemanticFinish
)


class EntryReplayCapacity(JournalValidationCapacity):
    """The owning unit must admit more of its actual existing atom/control bytes."""


@dataclass(frozen=True)
class ReplayBudget:
    rows: int = 64
    octets: int = 4 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 65536

    def __post_init__(self) -> None:
        if type(self.rows) is not int or not 1 <= self.rows <= 64:
            raise ValueError("entry replay row budget must be one to 64")
        if type(self.octets) is not int or self.octets < 1:
            raise ValueError("entry replay byte budget must be positive")


_DEFAULT_BUDGET = ReplayBudget()


@dataclass
class _CurrentEntry:
    frame: JournalFrame
    prepared: PreparedEntry
    document: dict[str, Any]
    source: SemanticSource
    child_kind: str | None = None
    child_id: str | None = None
    child: _Child | None = None
    row_key: str | None = None
    row_sha256: str | None = None
    row: dict[str, Any] | None = None
    atom: GenericAtom | None = None


def install(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE journal_validation_frame(singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
        "ordinal TEXT NOT NULL,sha256 TEXT NOT NULL,value BLOB NOT NULL)"
    ).close()


def _digest(state: Any) -> str:
    return hashlib.sha256(state.to_json().encode()).hexdigest()


def _admission_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def metadata(rows: int) -> Expression:
        return Expression("product", terms=(count(rows), dimension("metadata_row_octets")))

    def frames(factor: int) -> Expression:
        return Expression(
            "product", terms=(Expression("constant", value=factor), dimension("frame_octets"))
        )

    children: tuple[tuple[str, Callable[..., Any]], ...] = (
        ("prefix", JournalPrefix.prepare),
        ("load", ValidationLedger.load),
        ("fence", ValidationLedger._fence),
        ("publication", ValidationLedger._publish),
    )

    def child_metric(name: str, operation: Callable[..., Any], metric: str) -> Expression:
        return Expression(
            "delegate",
            name=name,
            metric=metric,
            terms=(count(1),),
            arguments=tuple(
                (
                    item.name,
                    dimension(
                        "checkpoint_retained_rows"
                        if name != "prefix" and item.name == "retained_rows"
                        else item.name
                    ),
                )
                for item in growth_binding(operation).declaration.dimensions
            ),
        )

    def add(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    bounds = (
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            count(14),
            (
                "Nine parent-control statements, two preparation probes, one atom "
                "insert and BEGIN/COMMIT."
            ),
        ),
        Bound(
            "returned_rows",
            COUNT,
            "upper",
            count(10),
            "Only exact parent, prefix and predecessor control rows are returned.",
        ),
        Bound(
            "changed_rows",
            COUNT,
            "equal",
            count(2),
            "One quarantined current-frame row and one parent checkpoint change atomically.",
        ),
        Bound(
            "returned_metadata_octets",
            OCTETS,
            "upper",
            metadata(10),
            "Returned rows retain their complete actual control widths.",
        ),
        Bound(
            "bound_parameter_octets",
            OCTETS,
            "upper",
            add(frames(1), metadata(14)),
            "One exact JSON atom plus the fixed statement control envelopes.",
        ),
        Bound(
            "written_value_octets",
            OCTETS,
            "upper",
            add(frames(1), metadata(2)),
            "The atom insert and parent CAS include their exact bytes and control keys.",
        ),
        Bound(
            "adjacency_rows",
            COUNT,
            "equal",
            count(0),
            "Quarantine does not walk native semantic adjacency.",
        ),
        Bound(
            "source_payload_octets",
            OCTETS,
            "equal",
            Expression("constant", value=0, unit=OCTETS),
            "Admission never reads accepted or quarantined frame payloads from SQLite.",
        ),
        Bound(
            "source_hashed_octets",
            OCTETS,
            "equal",
            dimension("frame_octets"),
            "Only the exact newly supplied framed atom advances the source SHA continuation.",
        ),
        Bound(
            "native_hashed_octets",
            OCTETS,
            "upper",
            frames(2),
            "The native reference and SemanticSource identity each hash current JSON once.",
        ),
        Bound(
            "decoded_json_octets",
            OCTETS,
            "upper",
            add(frames(4), metadata(40)),
            "Four current-frame decodes plus fixed parent/prefix/continuation codec checks.",
        ),
        Bound(
            "encoded_json_octets",
            OCTETS,
            "upper",
            add(frames(4), metadata(40)),
            "Four native frame canonicality re-encodes plus fixed metadata codec outputs.",
        ),
        Bound(
            "native_schema_calls",
            COUNT,
            "equal",
            count(1),
            "The owning native entry schema validates this exact admitted document once.",
        ),
        Bound(
            "sha256_compression_calls",
            COUNT,
            "upper",
            child_metric("prefix", JournalPrefix.prepare, "sha256_compression_calls"),
            (
                "All source continuation and checkpoint-padding compression is charged "
                "through actual prefix preparation."
            ),
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            add(
                count(1024),
                Expression(
                    "product",
                    terms=(
                        count(256),
                        Expression("log2_ceil", terms=(add(count(2), dimension("retained_rows")),)),
                    ),
                ),
                *(
                    child_metric(child, operation, "sqlite_vm_instructions")
                    for child, operation in children
                ),
            ),
            (
                "Actual child budgets plus fixed transaction and indexed current-atom"
                " insertion work."
            ),
        ),
    )
    subsets = tuple(
        Bound(
            "prefix_" + bound.metric,
            bound.unit,
            "upper",
            child_metric("prefix", JournalPrefix.prepare, bound.metric),
            "Actual preparation subtotal already included in the inclusive observation.",
        )
        for bound in growth_binding(JournalPrefix.prepare).declaration.bounds
    ) + tuple(
        Bound(
            "checkpoint_" + bound.metric,
            bound.unit,
            "upper",
            add(
                *(child_metric(child, operation, bound.metric) for child, operation in children[1:])
            ),
            "Actual parent-control subtotal already included in the inclusive observation.",
        )
        for bound in growth_binding(ValidationLedger.load).declaration.bounds
    )
    return Declaration(
        id="riverhog-provenance.sqlite-entry-replay-admission.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_replay.EntryReplay.admit",),
        scope="whole-native-entry-quarantine-with-parent-checkpoint",
        starts_at="receive one exact frame and an integrity-checked owned native workspace",
        ends_at=(
            "commit current-frame quarantine and its fenced parent while retaining "
            "the prior accepted prefix"
        ),
        dimensions=(
            Dimension(
                "retained_rows",
                COUNT,
                "All actual retained native workspace rows before admission.",
            ),
            Dimension(
                "checkpoint_retained_rows",
                COUNT,
                "Actual parent, accepted-prefix and accepted-frame rows before admission.",
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Actual old/new parent, prefix, binding, epoch and continuation control envelope.",
            ),
            Dimension("retained_frames", COUNT, "Previously accepted exact frames."),
            Dimension(
                "frame_octets", OCTETS, "Exact original JSON plus its two native framing octets."
            ),
            Dimension(
                "initial_tail_octets", OCTETS, "Actual previous SHA source tail, zero through 63."
            ),
            Dimension("source_blocks", COUNT, "floor((initial_tail_octets+frame_octets)/64)."),
        ),
        bounds=(*bounds, *subsets),
        conditions=(
            (
                "The caller owns the exact source/catalog/profile/attempt and a "
                "detached parent at phase frame."
            ),
            (
                "The existing entry-byte contract bounds this atom, without limiting "
                "total journal size."
            ),
            (
                "The sealed catalog checkpoint is cached; no catalog is reconstructed "
                "inside admission."
            ),
            (
                "The metadata envelope includes old and next parent, actual prefix, "
                "binding and worker widths."
            ),
            (
                "The owned native schema has no application-supplied triggers; "
                "sufficient unit capacity is admitted."
            ),
        ),
        invariants=(
            (
                "The whole method charges its own frame decoding, source identity, "
                "cache, transaction and insert work as well as actual named children."
            ),
            (
                "Admission quarantines one current frame without accepting its semantic"
                " prefix or changing any native graph fact."
            ),
            (
                "Quarantine and parent checkpoint commit together; stale workers cannot"
                " publish and failed writes roll back together."
            ),
            (
                "No preceding source frame is read or rehashed; child subtotals are "
                "inclusive subsets, not extra charges."
            ),
        ),
        limitations=(
            (
                "Source custody, framing, semantic advance, terminalization, owner "
                "construction and cleanup retain their own scopes."
            ),
            (
                "Codec octets and schema calls describe processed inputs; they do not "
                "bound physical CPU/RAM, native schema engine internals, B-tree byte "
                "comparisons, durable I/O or lock waits."
            ),
            (
                "Rejected, capacity-limited and retried calls retain separate "
                "correctness and attempt evidence."
            ),
        ),
    )


def _publication_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    def metadata(rows: int) -> Expression:
        return Expression("product", terms=(count(rows), dimension("metadata_row_octets")))

    children: tuple[tuple[str, Callable[..., Any]], ...] = (
        ("plan", JournalPrefix.prepare_publication),
        ("application", JournalPrefix._apply),
        ("fence", ValidationLedger._fence),
        ("publication", ValidationLedger._publish),
    )

    def child_metric(name: str, operation: Callable[..., Any], metric: str) -> Expression:
        arguments = []
        for item in growth_binding(operation).declaration.dimensions:
            value = dimension(
                "checkpoint_retained_rows" if item.name == "retained_rows" else item.name
            )
            if name == "publication" and item.name == "retained_rows":
                value = add(value, count(1))
            arguments.append((item.name, value))
        return Expression(
            "delegate", name=name, metric=metric, terms=(count(1),), arguments=tuple(arguments)
        )

    bounds = (
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            count(11),
            (
                "Three fence probes, two prefix writes, quarantine deletion, three "
                "parent publication statements and BEGIN/COMMIT."
            ),
        ),
        Bound(
            "returned_rows",
            COUNT,
            "upper",
            count(5),
            "Only fenced parent/prefix metadata and two exact predecessor references.",
        ),
        Bound(
            "changed_rows",
            COUNT,
            "equal",
            count(4),
            (
                "Prefix header, accepted frame, current quarantine deletion and "
                "parent advance commit together."
            ),
        ),
        Bound(
            "returned_metadata_octets",
            OCTETS,
            "upper",
            metadata(5),
            "Full actual metadata widths are returned, without source payload reads.",
        ),
        Bound(
            "bound_parameter_octets",
            OCTETS,
            "upper",
            add(dimension("frame_octets"), metadata(8)),
            "One original JSON frame plus fixed old/new source, prefix and worker fences.",
        ),
        Bound(
            "written_value_octets",
            OCTETS,
            "upper",
            add(dimension("frame_octets"), metadata(4)),
            ("The exact accepted JSON and old/new header controls, not a cumulative prefix copy."),
        ),
        Bound(
            "adjacency_rows",
            COUNT,
            "equal",
            count(0),
            "All native semantic and correction checks completed before publication.",
        ),
        Bound(
            "source_payload_octets",
            OCTETS,
            "equal",
            Expression("constant", value=0, unit=OCTETS),
            (
                "The prepared atom is already owned in memory; no accepted or "
                "quarantined payload is reread."
            ),
        ),
        Bound(
            "source_hashed_octets",
            OCTETS,
            "equal",
            Expression("constant", value=0, unit=OCTETS),
            "Source continuation is already prepared for this exact frame.",
        ),
        Bound(
            "native_hashed_octets",
            OCTETS,
            "equal",
            Expression("constant", value=0, unit=OCTETS),
            "Publication neither reconstructs nor rehashes the admitted native frame.",
        ),
        Bound(
            "native_schema_calls",
            COUNT,
            "equal",
            count(0),
            "Native schema and semantics precede this atomic publication phase.",
        ),
        Bound(
            "decoded_json_octets",
            OCTETS,
            "upper",
            metadata(40),
            "Fixed parent, prefix, reference and continuation codec checks.",
        ),
        Bound(
            "encoded_json_octets",
            OCTETS,
            "upper",
            metadata(40),
            "Fixed old/new control encodings without full-frame canonicality rechecks.",
        ),
        Bound(
            "sha256_compression_calls",
            COUNT,
            "equal",
            dimension("padding_blocks"),
            (
                "Only an exact matching external anchor finalizes the prepared source"
                " continuation copy."
            ),
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            add(
                count(1024),
                Expression(
                    "product",
                    terms=(
                        count(256),
                        Expression("log2_ceil", terms=(add(count(2), dimension("retained_rows")),)),
                    ),
                ),
                *(
                    child_metric(name, operation, "sqlite_vm_instructions")
                    for name, operation in children
                ),
            ),
            "Actual child budgets plus transaction control and indexed quarantine deletion.",
        ),
    )
    subsets = tuple(
        Bound(
            "prefix_" + bound.metric,
            bound.unit,
            "upper",
            add(*(child_metric(name, operation, bound.metric) for name, operation in children[:2])),
            "Actual prefix phase subtotal already included in the inclusive observation.",
        )
        for bound in growth_binding(JournalPrefix._apply).declaration.bounds
    ) + tuple(
        Bound(
            "checkpoint_" + bound.metric,
            bound.unit,
            "upper",
            add(*(child_metric(name, operation, bound.metric) for name, operation in children[2:])),
            "Actual parent subtotal already included in the inclusive observation.",
        )
        for bound in growth_binding(ValidationLedger._fence).declaration.bounds
    )
    return Declaration(
        id="riverhog-provenance.sqlite-entry-replay-publication.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_replay.EntryReplay._publish",),
        scope="one-atomic-native-frame-publication-transition",
        starts_at=(
            "receive the exact loaded publish checkpoint and its checked prepared native frame"
        ),
        ends_at=(
            "commit accepted frame/prefix and parent together, retire quarantine,"
            " then release the warm current-frame cache"
        ),
        dimensions=(
            Dimension(
                "retained_rows",
                COUNT,
                "All actual retained native workspace rows before publication.",
            ),
            Dimension(
                "checkpoint_retained_rows",
                COUNT,
                (
                    "Actual parent, prefix and accepted-frame index rows before "
                    "publication; one frame is added before parent publication."
                ),
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                (
                    "Actual old/new parent, prefix, binding, worker and continuation "
                    "control envelope."
                ),
            ),
            Dimension("retained_frames", COUNT, "Previously accepted exact frames."),
            Dimension(
                "frame_octets", OCTETS, "Original framed current atom already prepared in memory."
            ),
            Dimension(
                "padding_blocks",
                COUNT,
                (
                    "Zero without a current anchor entry match; otherwise one for "
                    "prepared SHA tail below56octets, two for56through63."
                ),
            ),
        ),
        bounds=(*bounds, *subsets),
        conditions=(
            (
                "The native conductor completed semantic, recording and correction "
                "checks and loaded parent phase publish."
            ),
            (
                "Prepared frame, prior prefix, source/catalog/profile modes and "
                "worker remain exactly owned and fenced."
            ),
            (
                "The private schema has no application-supplied triggers; the "
                "existing native atom contract is unchanged."
            ),
            (
                "Padding count and complete metadata envelope are input-derived, "
                "independent of observed work."
            ),
        ),
        invariants=(
            (
                "Frame bytes, accepted prefix and next parent checkpoint commit "
                "together with current quarantine retirement."
            ),
            (
                "A failed or superseded publication rolls all writes back and retains"
                " the cached current frame for its owner."
            ),
            (
                "The warm current-frame cache and store watermark change only after "
                "successful commit."
            ),
            (
                "No prior source is reread or rehashed; named child work is an "
                "inclusive subset, not an additional charge."
            ),
        ),
        limitations=(
            (
                "Whole advance includes prior load, source/atom restoration and "
                "capacity admission; those remain separate from this publication "
                "boundary."
            ),
            (
                "Native semantic checks, source custody, terminalization, result "
                "borrowing and cleanup retain their own scopes."
            ),
            (
                "Primitive bytes, SQL/VM and native padding counts do not guarantee "
                "physical CPU/RAM, B-tree byte comparisons, durable I/O, lock wait or"
                " power-loss outcomes."
            ),
        ),
    )


def _restoration_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def product(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    def total(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def restored(value: Expression) -> Expression:
        return product(dimension("cold_cache"), value)

    def metadata(rows: int) -> Expression:
        return product(count(rows), dimension("metadata_row_octets"))

    def frames(copies: int) -> Expression:
        return product(Expression("constant", value=copies), dimension("frame_octets"))

    prefix = growth_binding(JournalPrefix.prepare).declaration

    def delegated(metric: str) -> Expression:
        return Expression(
            "delegate",
            name="prefix",
            metric=metric,
            terms=(product(count(1), dimension("cold_cache")),),
            arguments=tuple((item.name, dimension(item.name)) for item in prefix.dimensions),
        )

    bounds = (
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            total(restored(count(2)), delegated("sql_statements")),
            (
                "Cold restoration reads current singleton length/control and then its "
                "admitted payload; prefix preparation retains its actual probes."
            ),
        ),
        Bound(
            "returned_rows",
            COUNT,
            "upper",
            total(restored(count(2)), delegated("returned_rows")),
            "Only current singleton and exact prefix/predecessor rows are returned.",
        ),
        Bound(
            "returned_metadata_octets",
            OCTETS,
            "upper",
            restored(metadata(4)),
            (
                "Current ordinal/hash/length and exact prefix controls exclude the "
                "separately counted current payload."
            ),
        ),
        Bound(
            "source_payload_octets",
            OCTETS,
            "equal",
            restored(dimension("quarantine_octets")),
            (
                "Only the admitted current JSON atom is restored; all accepted frame "
                "and semantic payload reads remain forbidden."
            ),
        ),
        Bound(
            "bound_parameter_octets",
            OCTETS,
            "upper",
            restored(metadata(2)),
            "Only fixed prefix source/owner controls and predecessor ordinal are bound.",
        ),
        Bound(
            "changed_rows",
            COUNT,
            "equal",
            count(0),
            "Restoration does not publish or rewrite native durable state.",
        ),
        Bound(
            "written_value_octets",
            OCTETS,
            "equal",
            Expression("constant", value=0, unit=OCTETS),
            "The sole warm-cache replacement is process-local.",
        ),
        Bound(
            "adjacency_rows",
            COUNT,
            "equal",
            count(0),
            "Restoring a quarantined frame does not replay semantic adjacency.",
        ),
        Bound(
            "source_hashed_octets",
            OCTETS,
            "equal",
            restored(dimension("frame_octets")),
            "Only the current exact framed atom advances the restored source continuation.",
        ),
        Bound(
            "native_hashed_octets",
            OCTETS,
            "upper",
            restored(frames(3)),
            (
                "Current payload integrity, native predecessor identity and semantic "
                "source identity each hash current JSON once."
            ),
        ),
        Bound(
            "decoded_json_octets",
            OCTETS,
            "upper",
            restored(total(frames(4), metadata(40))),
            "Four current-frame decodes plus fixed prefix/continuation metadata codecs.",
        ),
        Bound(
            "encoded_json_octets",
            OCTETS,
            "upper",
            restored(total(frames(4), metadata(40))),
            "Four frame canonicality re-encodes plus fixed prefix/continuation metadata outputs.",
        ),
        Bound(
            "native_schema_calls",
            COUNT,
            "equal",
            restored(count(1)),
            (
                "A cold current frame passes the pinned native entry schema once; the "
                "warm cache is reused."
            ),
        ),
        Bound(
            "sha256_compression_calls",
            COUNT,
            "upper",
            delegated("sha256_compression_calls"),
            (
                "All current source and checkpoint padding work belongs to the actual "
                "prefix preparation child."
            ),
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            total(restored(count(1024)), delegated("sqlite_vm_instructions")),
            "Two singleton seeks plus the actual prefix child indexed-work budget.",
        ),
    )
    subsets = tuple(
        Bound(
            "prefix_" + bound.metric,
            bound.unit,
            "upper",
            delegated(bound.metric),
            "Actual prefix preparation is an inclusive subset of current-frame restoration.",
        )
        for bound in prefix.bounds
    )
    return Declaration(
        id="riverhog-provenance.sqlite-entry-replay-restoration.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_replay.EntryReplay._entry",),
        scope="owned-current-native-frame-cache-or-restoration",
        starts_at=(
            "receive the loaded unfinished parent checkpoint after its replay budget admission"
        ),
        ends_at=(
            "reuse the exact warm current frame or restore and cache that single quarantined frame"
        ),
        dimensions=(
            *prefix.dimensions,
            Dimension(
                "cold_cache",
                SCALAR,
                (
                    "Zero for an exact owned warm current-frame cache; one when its current"
                    " frame must be restored."
                ),
            ),
            Dimension(
                "quarantine_octets",
                OCTETS,
                "Exact current JSON payload length, two octets shorter than its framed input.",
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                (
                    "Complete actual current ordinal/hash, source binding and "
                    "prefix/checkpoint metadata envelope."
                ),
            ),
        ),
        bounds=(*bounds, *subsets),
        conditions=(
            (
                "The loaded parent identifies the exact unfinished current frame; the "
                "caller already admitted its fixed metadata budget."
            ),
            (
                "The private schema, retained immutable source, source/catalog/profile "
                "binding and worker remain owned."
            ),
            (
                "Cache presence, actual current payload length and retained prefix SHA "
                "tail are input facts, not derived from observed work."
            ),
            (
                "Current payload length is checked and charged before its point payload"
                " read; the existing native entry-byte contract is unchanged."
            ),
        ),
        invariants=(
            (
                "No accepted historical frame or assertion payload is read or "
                "revalidated to reconstruct the current source continuation."
            ),
            "A warm exact frame performs no SQLite, JSON, native schema or source hashing work.",
            (
                "Restoration leaves every durable row, parent checkpoint and "
                "transaction boundary unchanged."
            ),
            (
                "Named prefix work is included in the primitive root counters, not an "
                "additional charge."
            ),
        ),
        limitations=(
            (
                "Whole advance also owns prior parent loading, budget setup and "
                "subsequent semantic or publication phases."
            ),
            (
                "This private boundary restores the current admitted atom; it does not "
                "authenticate source custody or finish semantic validation."
            ),
            (
                "Rejected, corrupt, capacity-limited and stale attempts retain separate"
                " correctness evidence."
            ),
            (
                "Primitive bytes and SQL/VM counts do not promise physical CPU/RAM, "
                "lock latency, durable I/O or power-loss behavior."
            ),
        ),
    )


def _row_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    calls = Expression("product", terms=(count(1), dimension("cold_row")))
    admission = growth_binding(read_changed_assertion).declaration
    index = growth_binding(Nodes.__init__).declaration

    def delegate(name: str, metric: str) -> Expression:
        child = admission if name == "admission" else index
        return Expression(
            "delegate",
            name=name,
            metric=metric,
            terms=(calls,),
            arguments=tuple(
                (
                    item.name,
                    dimension(
                        "atom_input_items"
                        if name == "index" and item.name == "input_items"
                        else item.name
                    ),
                )
                for item in child.dimensions
            ),
        )

    def atom_bytes(copies: int) -> Expression:
        return Expression(
            "product",
            terms=(
                Expression("constant", value=copies),
                dimension("cold_row"),
                dimension("atom_octets"),
            ),
        )

    observed_admission = tuple(
        Bound(
            bound.metric,
            bound.unit,
            bound.relation,
            delegate("admission", bound.metric),
            "Actual selected-assertion admission is included in this row restoration.",
        )
        for bound in admission.bounds
    )
    return Declaration(
        id="riverhog-provenance.sqlite-entry-replay-row-restoration.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_replay.EntryReplay._row",),
        scope="owned-current-selected-assertion-cache-or-restoration",
        starts_at="receive the exact loaded selected-row parent and owned current frame",
        ends_at=(
            "reuse its warm exact row or restore and cache its immutable native semantic atom/index"
        ),
        dimensions=(
            *admission.dimensions,
            Dimension(
                "cold_row",
                SCALAR,
                "Zero for matching cached row identity/hash and a retained row; otherwise one.",
            ),
            Dimension(
                "atom_input_items",
                COUNT,
                "Root plus every actual JSON value slot of this selected current assertion.",
            ),
            *index.dimensions[1:],
        ),
        bounds=(
            *observed_admission,
            Bound(
                "native_hashed_octets",
                OCTETS,
                "upper",
                atom_bytes(2),
                (
                    "Current selected payload integrity and its canonical immutable "
                    "semantic atom identity each hash once."
                ),
            ),
            Bound(
                "decoded_json_octets",
                OCTETS,
                "upper",
                atom_bytes(2),
                "Decode the selected row and the immutable private copy used by its node index.",
            ),
            Bound(
                "encoded_json_octets",
                OCTETS,
                "upper",
                atom_bytes(1),
                (
                    "The exact schema-admitted selected row is canonically encoded once for"
                    " its private atom."
                ),
            ),
            Bound(
                "native_schema_calls",
                COUNT,
                "equal",
                count(0),
                "The entry owner already validated this selected native assertion's schema.",
            ),
            Bound(
                "indexed_containers",
                COUNT,
                "upper",
                delegate("index", "indexed_containers"),
                "Actual nested semantic node-index construction is an inclusive subset.",
            ),
            Bound(
                "index_python_opcode_events",
                COUNT,
                "upper",
                delegate("index", "python_opcode_events"),
                (
                    "Actual opcode work of the maintained node-index kernel is observed "
                    "through its delegate."
                ),
            ),
        ),
        conditions=(
            (
                "The loaded parent pins the selected row key/hash, current source and "
                "previous completed key in one owned private workspace."
            ),
            (
                "Stored selected bytes are the exact canonical assertion staged from a "
                "schema-admitted native entry; JSON trees have no cycles or foreign "
                "container implementations."
            ),
            (
                "Actual selected atom octets, JSON value-slot count and cache state are"
                " causal input facts, not derived from observed work."
            ),
            (
                "The existing changed-assertion owner checks selected payload length "
                "against the available byte budget before acquisition; the caller "
                "retains its fixed metadata admission."
            ),
        ),
        invariants=(
            (
                "No accepted frame, retired or unselected assertion body is read to "
                "restore this selected atom."
            ),
            (
                "An exact warm row is returned without SQL, JSON, native hashes or "
                "index reconstruction."
            ),
            "The immutable semantic atom does not borrow mutable containers from the returned row.",
            (
                "Restoration leaves every durable row, checkpoint and transaction "
                "unchanged; named child observations are inclusive subsets."
            ),
        ),
        limitations=(
            (
                "Current-frame restoration, child construction, semantic progress and "
                "parent publication retain their own conductor scopes."
            ),
            (
                "This private cache boundary does not grant source custody, archive "
                "authority or successful semantic validation."
            ),
            "Stale, corrupt and capacity-limited attempts retain separate correctness evidence.",
            (
                "Primitive bytes, SQL/VM and index opcode counts do not guarantee "
                "physical CPU/RAM, native allocation, B-tree byte comparisons or lock "
                "latency."
            ),
        ),
    )


def _child_step_delegates(
    row_operation: Callable[..., Any],
) -> tuple[tuple[str, Callable[..., Any]], ...]:
    families = (
        ("entry", EntryPreparation),
        ("targets", CorrectionTargets),
        ("staging", AssertionStaging),
        ("correction", CorrectionWork),
        ("generic", GenericSemantics),
        ("finish", SemanticFinish),
    )
    operations = tuple(
        (family + "_" + stage, getattr(owner, operation))
        for family, owner in families
        for stage, operation in (
            ("admission", "prepare_admission"),
            ("admit", "_admit"),
            ("planning", "prepare"),
            ("application", "_apply"),
        )
    )
    return (
        *operations,
        ("entry_result", EntryPreparation.result),
        ("generic_result", GenericSemantics.result),
        ("row_constructor", row_operation),
        ("row_completion", row_operation),
        ("fence", ValidationLedger._fence),
        ("publication", ValidationLedger._publish),
    )


def _child_step_declaration(row_operation: Callable[..., Any]) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def multiple(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def add(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    delegates = _child_step_delegates(row_operation)
    declarations = {name: growth_binding(operation).declaration for name, operation in delegates}

    def child(name: str, metric: str) -> Expression:
        declaration = declarations[name]
        return Expression(
            "delegate",
            name=name,
            metric=metric,
            terms=(dimension(name + "_calls"),),
            arguments=tuple(
                (item.name, dimension(name + "_" + item.name)) for item in declaration.dimensions
            ),
        )

    def subtotal(metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        terms = tuple(
            child(name, metric)
            for name, declaration in declarations.items()
            if any(bound.metric == metric for bound in declaration.bounds)
        )
        return add(*terms) if terms else Expression("constant", value=0, unit=unit)

    width = dimension("metadata_row_octets")
    controls = Expression("product", terms=(add(count(1), dimension("row_budget")), width))
    loaded = dimension("bare_load_calls")
    metadata = add(
        Expression("product", terms=(multiple(16, loaded), width)),
        subtotal("returned_metadata_octets", OCTETS),
        *(
            child(name, "metadata_octets")
            for name, declaration in declarations.items()
            if not any(bound.metric == "returned_metadata_octets" for bound in declaration.bounds)
            and any(bound.metric == "metadata_octets" for bound in declaration.bounds)
        ),
    )
    # Constructor document canonicality checks touch only this admitted current frame.
    # Native plans meter their encoded keys, values and bounded private frames; the
    # multiplier covers decode/re-encode, identity checks and application repeats.
    codec = add(
        multiple(4, dimension("constructor_frame_octets")),
        multiple(4, subtotal("returned_atom_octets", OCTETS)),
        multiple(16, dimension("planning_byte_budget")),
        multiple(128, controls),
    )
    return Declaration(
        id="riverhog-provenance.sqlite-entry-replay-child-step.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_replay.EntryReplay._child_step",),
        scope="actual-owned-native-child-admission-resume-or-completion",
        starts_at="receive the exact loaded parent and its owned current native frame/cache",
        ends_at="atomically publish the child checkpoint or its checked completion transition",
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "All indexed private workspace rows before this unit."
            ),
            Dimension("row_budget", COUNT, "Actual parent row quantum, one to 64."),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Fixed-shape parent, child, prefix, identity and checkpoint parameter envelope.",
            ),
            Dimension("bare_load_calls", COUNT, "Zero for admission; one for a retained child."),
            Dimension(
                "constructor_frame_octets",
                OCTETS,
                (
                    "Current frame JSON octets if an uncached "
                    "entry/targets/staging/finish constructor runs; otherwise zero."
                ),
            ),
            Dimension(
                "planning_byte_budget",
                OCTETS,
                "Actual remaining bytes passed to a nonterminal child plan; zero without a plan.",
            ),
            Dimension(
                "generic_schema_steps",
                COUNT,
                "Actual step budget for an active generic/row plan; otherwise zero.",
            ),
            *(
                item
                for name, declaration in declarations.items()
                for item in (
                    Dimension(
                        name + "_calls",
                        COUNT,
                        "Causal actual call count for " + name + "; zero or one.",
                    ),
                    *(
                        Dimension(name + "_" + value.name, value.unit, value.meaning)
                        for value in declaration.dimensions
                    ),
                )
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                add(count(2), multiple(16, loaded), subtotal("sql_statements", COUNT)),
                (
                    "One owner transaction plus actual delegates and the fixed "
                    "indexed bare child load."
                ),
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                add(multiple(16, loaded), subtotal("returned_rows", COUNT)),
                "Bare load point/boundary probes and inclusive actual child rows.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                subtotal("changed_rows", COUNT),
                "All durable writes belong to actual child application or parent publication.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                metadata,
                "Actual child metadata and the bounded bare-load control envelope.",
            ),
            Bound(
                "returned_atom_octets",
                OCTETS,
                "upper",
                add(
                    subtotal("returned_atom_octets", OCTETS),
                    subtotal("payload_fetched_octets", OCTETS),
                ),
                (
                    "Only admitted current selected rows or the finish owner's "
                    "length-admitted semantic atoms."
                ),
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                add(multiple(16, dimension("planning_byte_budget")), multiple(128, controls)),
                "Metered atom/key parameters and repeated fixed-shape child/parent fences.",
            ),
            Bound(
                "written_value_octets",
                OCTETS,
                "upper",
                add(multiple(16, dimension("planning_byte_budget")), multiple(128, controls)),
                "Actual complete mutation parameters, including private checkpoint publication.",
            ),
            Bound(
                "historical_frame_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No accepted journal frame or historical identity payload is fetched.",
            ),
            Bound(
                "adjacency_rows",
                COUNT,
                "upper",
                subtotal("adjacency_rows", COUNT),
                "Only the actual nested semantic graph owner may walk adjacency rows.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    count(8192),
                    subtotal("sqlite_vm_instructions", COUNT),
                    Expression(
                        "product",
                        terms=(
                            multiple(8192, loaded),
                            Expression(
                                "log2_ceil", terms=(add(count(4), dimension("retained_rows")),)
                            ),
                        ),
                    ),
                ),
                "Actual child VM bounds plus indexed bare-load and fixed transaction overhead.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    codec,
                    (
                        "Inclusive native codecs/hashes, including constructors, "
                        "undeclared child load and control digests."
                    ),
                )
                for metric in ("native_hashed_octets", "decoded_json_octets", "encoded_json_octets")
            ),
            Bound(
                "native_schema_calls",
                COUNT,
                "upper",
                multiple(4, dimension("generic_schema_steps")),
                "Only step-bounded native generic metadata/profile checks validate schemas here.",
            ),
            Bound(
                "indexed_containers",
                COUNT,
                "upper",
                subtotal("indexed_containers", COUNT),
                "Actual cold selected-row node index construction remains nested and inclusive.",
            ),
            Bound(
                "index_python_opcode_events",
                COUNT,
                "upper",
                subtotal("index_python_opcode_events", COUNT),
                (
                    "Actual node/coverage kernel opcodes remain visible through "
                    "selected-row restoration."
                ),
            ),
        ),
        conditions=(
            "The exact current frame is already schema-admitted and belongs to this loaded parent.",
            (
                "Call counts, cache state and every child input are causal pre-call "
                "facts from genuine native checkpoints."
            ),
            (
                "Each delegate retained-row count uses its owning tables; the root "
                "count cannot replace a child domain."
            ),
            (
                "The metadata envelope excludes native documentary payloads and "
                "includes actual old/new control widths."
            ),
            (
                "An admission invokes exactly its selected plan/application pair; a "
                "retained unfinished child invokes its progress pair; a completed "
                "child invokes only its required result."
            ),
            (
                "Cold row restoration consumes its actual current atom before the "
                "remaining child byte budget is supplied."
            ),
        ),
        invariants=(
            "Parent and child writes share one fenced SQLite transaction; failure rolls both back.",
            (
                "Accepted prefix, source identity, child digest and worker ownership "
                "retain their native checks."
            ),
            (
                "Constructors, undeclared load, parent digest work and transaction "
                "overhead remain part of inclusive counters."
            ),
            (
                "A cache contains only this current frame, selected assertion and "
                "exact child identity."
            ),
        ),
        limitations=(
            (
                "This successful child boundary does not qualify graph "
                "replacement/retirement, whole advance or whole-session composition."
            ),
            (
                "Stale, capacity, corruption, interruption and rollback retain "
                "independent correctness coverage."
            ),
            (
                "Logical bytes and SQLite/kernel counters exclude allocation, B-tree "
                "byte comparisons, fsync, lock latency and physical power-loss "
                "guarantees."
            ),
        ),
    )


def _graph_step_delegates() -> tuple[tuple[str, Callable[..., Any]], ...]:
    return (
        ("replacement_plan", OnlineDag._prepare_replacement),
        ("replacement_application", OnlineDag._apply_replacement),
        ("retirement_plan", OnlineDag._prepare_retirement),
        ("retirement_application", OnlineDag._apply_retirement),
        ("fence", ValidationLedger._fence),
        ("publication", ValidationLedger._publish),
    )


def _graph_step_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def multiple(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    delegates = _graph_step_delegates()
    declarations = {name: growth_binding(operation).declaration for name, operation in delegates}

    def child(name: str, metric: str) -> Expression:
        return Expression(
            "delegate",
            name=name,
            metric=metric,
            terms=(dimension(name + "_calls"),),
            arguments=tuple(
                (item.name, dimension(name + "_" + item.name))
                for item in declarations[name].dimensions
            ),
        )

    def subtotal(metric: str) -> Expression:
        return add(*(child(name, metric) for name, _operation in delegates))

    width = dimension("metadata_row_octets")
    metadata = Expression("product", terms=(count(4), width))
    zero = Expression("constant", value=0, unit=OCTETS)
    return Declaration(
        id="riverhog-provenance.sqlite-entry-replay-graph-step.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_replay.EntryReplay._graph_step",),
        scope="actual-owned-native-correction-graph-replacement-or-retirement",
        starts_at="receive the exact loaded correction graph phase and owner byte/row admission",
        ends_at=(
            "atomically publish the new run, bounded retired-key window or exhausted transition"
        ),
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "All indexed private workspace rows before this unit."
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                (
                    "Actual fixed-shape owner/prefix/checkpoint and graph-work "
                    "envelope, excluding retired values."
                ),
            ),
            Dimension(
                "retirement_byte_budget",
                OCTETS,
                "Actual remaining byte admission passed to retirement; zero for replacement.",
            ),
            Dimension(
                "digest_calls",
                COUNT,
                (
                    "One graph digest for replacement or exhausted retirement, two "
                    "for a retirement batch."
                ),
            ),
            *(
                item
                for name, declaration in declarations.items()
                for item in (
                    Dimension(
                        name + "_calls",
                        COUNT,
                        "Causal actual call count for " + name + "; zero or one.",
                    ),
                    *(
                        Dimension(name + "_" + value.name, value.unit, value.meaning)
                        for value in declaration.dimensions
                    ),
                )
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                add(count(5), subtotal("sql_statements")),
                (
                    "Active-run lookup, constructor algorithm check, optional bare "
                    "digest load and owner transaction, plus actual delegates."
                ),
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                add(count(3), subtotal("returned_rows")),
                "At most three bare graph metadata rows plus actual named child rows.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                subtotal("changed_rows"),
                "Only the actual graph application and parent publication mutate durable rows.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                add(metadata, subtotal("returned_metadata_octets")),
                "Bare active-run/algorithm/work metadata and actual graph/parent descendants.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                add(metadata, subtotal("bound_parameter_octets")),
                "Actual bare kind/run keys plus complete child statement parameters.",
            ),
            Bound(
                "written_value_octets",
                OCTETS,
                "upper",
                subtotal("written_value_octets"),
                (
                    "Complete graph and parent mutation parameters remain included "
                    "in their actual delegates."
                ),
            ),
            Bound(
                "admitted_graph_octets",
                OCTETS,
                "upper",
                dimension("retirement_byte_budget"),
                (
                    "The native owner claims the actual selected keys and "
                    "control-write bytes before committing retirement."
                ),
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                zero,
                (
                    "Graph replacement and retirement read no source, assertion, "
                    "identity or retired value payload."
                ),
            ),
            Bound(
                "adjacency_rows",
                COUNT,
                "upper",
                subtotal("adjacency_rows"),
                "Retirement reads length-admitted key windows, never graph adjacency payloads.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    count(8192),
                    subtotal("sqlite_vm_instructions"),
                    Expression(
                        "product",
                        terms=(
                            count(512),
                            Expression(
                                "log2_ceil", terms=(add(count(4), dimension("retained_rows")),)
                            ),
                        ),
                    ),
                ),
                "Actual child instruction envelopes and the fixed indexed bare control reads.",
            ),
            Bound(
                "native_hashed_octets",
                OCTETS,
                "upper",
                Expression("product", terms=(dimension("digest_calls"), width)),
                "Each graph digest hashes one input-derived fixed-shape run/work document.",
            ),
            Bound(
                "decoded_json_octets",
                OCTETS,
                "upper",
                multiple(64, Expression("product", terms=(count(1), width))),
                (
                    "Graph work and exact parent/prefix codecs decode a fixed "
                    "number of bounded controls."
                ),
            ),
            Bound(
                "encoded_json_octets",
                OCTETS,
                "upper",
                multiple(96, Expression("product", terms=(count(1), width))),
                (
                    "Canonicality checks, graph digest documents and checkpoint "
                    "writes encode only fixed-shape controls."
                ),
            ),
            Bound(
                "native_schema_calls",
                COUNT,
                "equal",
                count(0),
                "The graph unit does not repeat current-frame or assertion schema validation.",
            ),
        ),
        conditions=(
            (
                "The exact parent belongs to an admitted native correction and pins "
                "the active graph kind and run digest."
            ),
            (
                "Replacement uses a fresh native run identity and the owner updates "
                "its in-memory run only after durable commit."
            ),
            (
                "Each graph child uses its own graph row domain and input-derived "
                "scalar/run/key widths; replacement row_budget is one."
            ),
            (
                "Retirement receives the exact parent row quantum and remaining "
                "byte admission and never acquires retired level/undo values."
            ),
            (
                "Call counts and digest multiplicity come from the input phase and "
                "actual retired-run directory, not measured cost."
            ),
        ),
        invariants=(
            (
                "The graph run, retirement keys and next parent checkpoint share "
                "one exact fenced SQLite transaction."
            ),
            (
                "Failure preserves prior graph and parent state; foreign graph "
                "kinds and runs remain unchanged."
            ),
            "No accepted journal frame or selected assertion is reloaded by graph work.",
            "Exhausted retirement advances only after the exact old run directory is empty.",
        ),
        limitations=(
            (
                "This successful boundary does not establish whole advance, "
                "whole-session completion or archive/source authority."
            ),
            (
                "Stale run/worker, capacity, statement failure and reopen retain "
                "independent correctness evidence."
            ),
            (
                "Logical byte and SQLite counts exclude allocation, B-tree byte "
                "comparisons, lock waits, fsync and physical power loss."
            ),
        ),
    )


def _row_transition_delegates(
    mode: str, row_operation: Callable[..., Any]
) -> tuple[tuple[str, Callable[..., Any]], ...]:
    selected = (
        (("admission", read_changed_assertion), ("index", Nodes.__init__))
        if mode == "admission"
        else (("restoration", row_operation),)
    )
    return (
        *selected,
        ("fence", ValidationLedger._fence),
        ("publication", ValidationLedger._publish),
    )


def _row_transition_declaration(mode: str, row_operation: Callable[..., Any]) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def multiple(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def add(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    declarations = {
        name: growth_binding(operation).declaration
        for name, operation in _row_transition_delegates(mode, row_operation)
    }

    def child(name: str, metric: str) -> Expression:
        return Expression(
            "delegate",
            name=name,
            metric=metric,
            terms=(dimension(name + "_calls"),),
            arguments=tuple(
                (item.name, dimension(name + "_" + item.name))
                for item in declarations[name].dimensions
            ),
        )

    def subtotal(metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        terms = tuple(
            child(name, metric)
            for name, declaration in declarations.items()
            if any(bound.metric == metric for bound in declaration.bounds)
        )
        return add(*terms) if terms else Expression("constant", value=0, unit=unit)

    admission = mode == "admission"
    width = dimension("metadata_row_octets")
    update = (
        count(0)
        if admission
        else Expression("product", terms=(count(1), dimension("lifecycle_update")))
    )
    atom = (
        dimension("admission_atom_octets")
        if admission
        else Expression("constant", value=0, unit=OCTETS)
    )
    helper = "_admit_row" if admission else "_lifecycle"
    return Declaration(
        id="riverhog-provenance.sqlite-entry-replay-row-" + mode + ".growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_replay.EntryReplay." + helper,),
        scope="owned-selected-assertion-" + mode + "-and-atomic-parent-transition",
        starts_at="receive the exact parent and its admitted current native frame",
        ends_at="commit the selected identity or lifecycle result with its fenced parent",
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "All private indexed workspace rows before this unit."
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Actual fixed-shape old/new parent, prefix, worker, key and time envelope.",
            ),
            *(
                (
                    Dimension(
                        "lifecycle_update",
                        SCALAR,
                        (
                            "One only for usage/invalidation with an admitted timestamp; otherwise "
                            "zero."
                        ),
                    ),
                )
                if not admission
                else ()
            ),
            *(
                item
                for name, declaration in declarations.items()
                for item in (
                    Dimension(name + "_calls", COUNT, "Actual causal call count for " + name + "."),
                    *(
                        Dimension(name + "_" + value.name, value.unit, value.meaning)
                        for value in declaration.dimensions
                    ),
                )
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                add(count(2), multiple(2, update), subtotal("sql_statements", COUNT)),
                "Owner transaction and optional indexed extrema read/upsert plus actual delegates.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                add(update, subtotal("returned_rows", COUNT)),
                "Only the selected row, exact parent controls and optional extrema metadata.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                add(update, count(1)),
                "A successful phase publishes exactly one parent row and optionally upserts "
                "lifecycle extrema; the parent equality is independent of rejected helper calls.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                add(
                    Expression("product", terms=(update, width)),
                    subtotal("returned_metadata_octets", OCTETS),
                ),
                "Selected identity and parent metadata plus at most two exact decimal times.",
            ),
            Bound(
                "returned_atom_octets",
                OCTETS,
                "upper",
                subtotal("returned_atom_octets", OCTETS),
                "Only the selected length-admitted current assertion is acquired.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    add(
                        multiple(2, Expression("product", terms=(update, width))),
                        subtotal(metric, OCTETS),
                    ),
                    "Exact child/parent parameters and optional state/time upsert values.",
                )
                for metric in ("bound_parameter_octets", "written_value_octets")
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No accepted frame, retired or unselected assertion body is acquired.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    count(1024),
                    subtotal("sqlite_vm_instructions", COUNT),
                    Expression(
                        "product",
                        terms=(
                            multiple(512, update),
                            Expression(
                                "log2_ceil", terms=(add(count(2), dimension("retained_rows")),)
                            ),
                        ),
                    ),
                ),
                "Actual delegates, fixed transaction work and optional indexed extrema mutation.",
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    add(
                        multiple(copies, atom),
                        multiple(32, Expression("product", terms=(count(1), width))),
                        subtotal(metric, OCTETS),
                    ),
                    (
                        "Current atom construction and exact fixed-shape parent codecs remain "
                        "inclusive."
                    ),
                )
                for metric, copies in (
                    ("native_hashed_octets", 2),
                    ("decoded_json_octets", 2),
                    ("encoded_json_octets", 1),
                )
            ),
            Bound(
                "native_schema_calls",
                COUNT,
                "equal",
                count(0),
                "This row already passed its owning entry schema.",
            ),
            Bound(
                "indexed_containers",
                COUNT,
                "upper",
                subtotal("indexed_containers", COUNT),
                "Only the actual selected-row native node index is constructed.",
            ),
            Bound(
                "index_python_opcode_events",
                COUNT,
                "upper",
                subtotal("python_opcode_events", COUNT)
                if admission
                else subtotal("index_python_opcode_events", COUNT),
                "Native node and coverage kernels are observed through their actual calls.",
            ),
        ),
        conditions=(
            (
                "Input dimensions and optional call selectors are causal native "
                "checkpoint/cache facts."
            ),
            "Every child retains its own indexed table domain and actual selected-atom dimensions.",
            "The selected active payload length is admitted before acquisition and decoding.",
            "Lifecycle instants are exact nanoseconds from schema-admitted four-digit UTC years.",
            "The fixed metadata envelope excludes documentary frame and assertion payloads.",
        ),
        invariants=(
            "The parent fence and mutation share one transaction; failure preserves durable state.",
            "Inactive or missing changed assertions advance only their exact keyset cursor.",
            "A completed lifecycle clears only the current selected-row/atom cache after commit.",
            "Accepted prefix, worker, child identity and source custody checks remain native.",
        ),
        limitations=(
            "Current-frame restoration and whole advance retain separate conductor scopes.",
            (
                "Stale, capacity, corruption, interruption and rollback require "
                "separate correctness evidence."
            ),
            (
                "Logical bytes and kernel/SQLite counts exclude allocation, fsync, "
                "lock waits and physical power loss."
            ),
        ),
    )


def _advance_delegates(
    entry: Callable[..., Any],
    row_admission: Callable[..., Any],
    lifecycle: Callable[..., Any],
    child: Callable[..., Any],
    graph: Callable[..., Any],
    publication: Callable[..., Any],
) -> tuple[tuple[str, Callable[..., Any]], ...]:
    return (
        ("load", ValidationLedger.load),
        ("entry", entry),
        ("row_admission", row_admission),
        ("lifecycle", lifecycle),
        ("child", child),
        ("graph", graph),
        ("entry_publication", publication),
        ("recording", JournalStore._check_entry_recording),
        ("fence", ValidationLedger._fence),
        ("publication", ValidationLedger._publish),
    )


def _advance_declaration(
    entry: Callable[..., Any],
    row_admission: Callable[..., Any],
    lifecycle: Callable[..., Any],
    child: Callable[..., Any],
    graph: Callable[..., Any],
    publication: Callable[..., Any],
) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    declarations = {
        name: growth_binding(operation).declaration
        for name, operation in _advance_delegates(
            entry, row_admission, lifecycle, child, graph, publication
        )
    }

    def subtotal(metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        terms = tuple(
            Expression(
                "delegate",
                name=name,
                metric=metric,
                terms=(dimension(name + "_calls"),),
                arguments=tuple(
                    (item.name, dimension(name + "_" + item.name))
                    for item in declaration.dimensions
                ),
            )
            for name, declaration in declarations.items()
            if any(bound.metric == metric for bound in declaration.bounds)
        )
        return add(*terms) if terms else Expression("constant", value=0, unit=unit)

    controls = Expression("product", terms=(count(64), dimension("metadata_row_octets")))
    return Declaration(
        id="riverhog-provenance.sqlite-entry-replay-advance.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_replay.EntryReplay.advance",),
        scope="whole-owned-native-entry-replay-advance",
        starts_at="load the exact native parent and admit this call's actual row/byte budget",
        ends_at=(
            "return idle progress, commit one native phase, or reject its "
            "effective recorder/context before parent publication"
        ),
        dimensions=(
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Actual old/new parent, source, prefix, worker and budget control envelope.",
            ),
            *(
                item
                for name, declaration in declarations.items()
                for item in (
                    Dimension(
                        name + "_calls", COUNT, "Actual causal invocation count for " + name + "."
                    ),
                    *(
                        Dimension(name + "_" + value.name, value.unit, value.meaning)
                        for value in declaration.dimensions
                    ),
                )
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    unit,
                    "upper",
                    subtotal(metric, unit),
                    (
                        "Inclusive actual phase scopes include all SQL, parameters, "
                        "payload and native kernels."
                    ),
                )
                for metric, unit in (
                    ("sql_statements", COUNT),
                    ("returned_rows", COUNT),
                    ("changed_rows", COUNT),
                    ("returned_metadata_octets", OCTETS),
                    ("returned_atom_octets", OCTETS),
                    ("source_payload_octets", OCTETS),
                    ("bound_parameter_octets", OCTETS),
                    ("written_value_octets", OCTETS),
                    ("adjacency_rows", COUNT),
                    ("sqlite_vm_instructions", COUNT),
                    ("source_hashed_octets", OCTETS),
                    ("indexed_containers", COUNT),
                    ("index_python_opcode_events", COUNT),
                    ("native_schema_calls", COUNT),
                )
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    add(controls, subtotal(metric, OCTETS)),
                    (
                        "Native child scopes plus initial parent/control decoding and"
                        " budget checkpoint encoding."
                    ),
                )
                for metric in ("native_hashed_octets", "decoded_json_octets", "encoded_json_octets")
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                (
                    "Only the current length-admitted frame or selected effective"
                    " assertion may be restored."
                ),
            ),
        ),
        conditions=(
            (
                "All call selectors and child input dimensions come from the "
                "exact pre-call parent and caches."
            ),
            (
                "Ledger load is always invoked once; the frame cache is "
                "consulted only for an unfinished entry."
            ),
            (
                "At most one row, lifecycle, semantic child, graph, recording"
                " or publication branch is selected."
            ),
            (
                "Remaining child bytes subtract actual cold frame and row "
                "admissions in native call order."
            ),
            "Each child retains its own indexed table domain and unchanged primitive bounds.",
            "The control envelope excludes documentary frame and assertion payloads.",
        ),
        invariants=(
            "The current entry and exact parent source/worker ownership retain every native check.",
            "A successful phase commits child changes and parent publication atomically.",
            (
                "Cold state restores only its unfinished current "
                "frame/assertion; warm caches retain identity checks."
            ),
            "Idle frame/terminal/done parents return without a semantic or publication phase.",
            "Recording resolves only the exact effective recorder and optional context references.",
        ),
        limitations=(
            (
                "This entry conductor does not supply source framing, whole-"
                "session completion or archive authority."
            ),
            (
                "Failure, corruption, capacity, stale ownership and "
                "interruption retain separate correctness coverage."
            ),
            (
                "Logical counters exclude allocation, B-tree byte "
                "comparisons, lock latency, fsync and physical power loss."
            ),
        ),
    )


class EntryReplay:
    def __init__(
        self,
        store: JournalStore,
        *,
        prefix: JournalPrefix,
        ledger: ValidationLedger,
        catalog: ContractCatalog,
        certificates: JournalGraphCertificates | None = None,
    ) -> None:
        if store.db is not prefix.db or store.db is not ledger.db:
            raise ValueError("entry replay requires one owned checkpoint connection")
        if (
            prefix.attempt_id != ledger.attempt_id
            or catalog.checkpoint_sha256() != prefix.catalog_sha256
        ):
            raise StaleValidationCheckpoint("entry replay inputs differ from its parent")
        self.store, self.db, self.prefix, self.ledger, self.catalog = (
            store,
            store.db,
            prefix,
            ledger,
            catalog,
        )
        profile_policy = json.loads(prefix.binding)["require_profiles"]
        if type(profile_policy) is not bool:
            raise RuntimeError("entry replay lost its bound profile policy")
        self.require_profiles = profile_policy
        self.certificates = certificates
        self.certificate_binding = None if certificates is None else certificates.binding
        self._current: _CurrentEntry | None = None
        self._spent = 0
        self._budget = ReplayBudget()

    def _one(self, sql: str, values: tuple[Any, ...] = ()) -> Any:
        with closing(self.db.execute(sql, values)) as cursor:
            return cursor.fetchone()

    def _start(self, before: ValidationSnapshot, budget: ReplayBudget) -> None:
        if self.certificates is not None and self.certificates.binding != self.certificate_binding:
            raise StaleValidationCheckpoint("entry replay certificate manifest changed")
        self._budget = budget
        # Reserve actual repeated fixed-shape owner/source/checkpoint metadata.
        prefix_octets = sum(
            len(v.encode()) if isinstance(v, str) else len(v) for v in before.prefix
        )
        self._spent = 16384 + 16 * (
            len(self.ledger.binding) + prefix_octets + len(before.checkpoint.to_json().encode())
        )
        self._remaining()

    def _claim(self, octets: int) -> None:
        if octets > self._budget.octets - self._spent:
            raise EntryReplayCapacity("entry replay cannot admit the next existing atom")
        self._spent += octets

    def _remaining(self) -> int:
        left = self._budget.octets - self._spent
        if left < 16384:
            raise EntryReplayCapacity("entry replay cannot admit fixed owner/child control")
        return left

    def _cache(self, frame: JournalFrame) -> _CurrentEntry:
        prepared = self.prefix.prepare(frame, self.catalog)
        current = _CurrentEntry(
            frame,
            prepared,
            prepared.document,
            SemanticSource.from_frame(
                frame,
                catalog_sha256=self.prefix.catalog_sha256,
                require_profiles=self.require_profiles,
            ),
        )
        self._current = current
        return current

    @declare_growth(_restoration_declaration(), delegates=(("prefix", JournalPrefix.prepare),))
    def _entry(self, state: ValidationCheckpoint) -> _CurrentEntry:
        if self._current is not None:
            if self._current.source.sha256 != state.frame_sha256:
                raise StaleValidationCheckpoint("cached entry differs from its parent atom")
            return self._current
        metadata = self._one(
            "SELECT ordinal,sha256,length(value) FROM journal_validation_frame WHERE singleton=1"
        )
        if (
            metadata is None
            or metadata[:2] != (str(state.frame_ordinal), state.frame_sha256)
            or metadata[2] + 2 != state.frame_octets
        ):
            raise RuntimeError("retained entry atom differs from its parent checkpoint")
        self._claim(metadata[2])
        value = self._one("SELECT value FROM journal_validation_frame WHERE singleton=1")
        if (
            value is None
            or type(value[0]) is not bytes
            or hashlib.sha256(value[0]).hexdigest() != state.frame_sha256
        ):
            raise RuntimeError("retained entry atom is missing or changed")
        return self._cache(JournalFrame(value[0]))

    def _commit(
        self, before: ValidationSnapshot, mutate: Callable[[], ValidationCheckpoint]
    ) -> ValidationSnapshot:
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            self.ledger._fence(before)
            after = mutate()
            result = self.ledger._publish(before, after)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return result

    def _move(self, before: ValidationSnapshot, phase: str) -> ValidationSnapshot:
        after = replace(
            before.checkpoint,
            version=before.checkpoint.version + 1,
            phase=phase,
            child_id=None,
            child_sha256=None,
        )
        return self._commit(before, lambda: after)

    @declare_growth(
        _admission_declaration(),
        delegates=(
            ("prefix", JournalPrefix.prepare),
            ("load", ValidationLedger.load),
            ("fence", ValidationLedger._fence),
            ("publication", ValidationLedger._publish),
        ),
    )
    def admit(
        self, frame: JournalFrame, budget: ReplayBudget = _DEFAULT_BUDGET
    ) -> ValidationSnapshot:
        before = self.ledger.load()
        if before.checkpoint.phase != "frame":
            raise RuntimeError("the current entry remains quarantined")
        self._start(before, budget)
        self._claim(2 * len(frame.json_bytes))
        current = self._cache(frame)
        after = replace(
            before.checkpoint,
            version=before.checkpoint.version + 1,
            phase="entry-admission",
            frame_sha256=current.source.sha256,
            frame_octets=len(frame.json_bytes) + 2,
        )

        def apply() -> ValidationCheckpoint:
            self.db.execute(
                "INSERT INTO journal_validation_frame VALUES(1,?,?,?)",
                (
                    str(before.checkpoint.frame_ordinal),
                    current.source.sha256,
                    current.frame.json_bytes,
                ),
            ).close()
            return after

        return self._commit(before, apply)

    @declare_growth(
        _row_declaration(),
        delegates=(("admission", read_changed_assertion), ("index", Nodes.__init__)),
    )
    def _row(self, state: ValidationCheckpoint, current: _CurrentEntry) -> dict[str, Any]:
        if (
            current.row_key == state.row_key
            and current.row_sha256 == state.row_sha256
            and current.row is not None
        ):
            return current.row
        selected = read_changed_assertion(
            self.db, after=state.row_after, byte_budget=self._remaining()
        )
        if (
            selected is None
            or selected.assertion_id != state.row_key
            or selected.raw is None
            or hashlib.sha256(selected.raw).hexdigest() != state.row_sha256
        ):
            raise RuntimeError("retained assertion atom differs from its parent checkpoint")
        self._claim(len(selected.raw))
        row: dict[str, Any] = json.loads(selected.raw)
        current.row_key, current.row_sha256, current.row = state.row_key, state.row_sha256, row
        current.atom = GenericAtom.admit(row)
        return row

    def _child(
        self, state: ValidationCheckpoint, current: _CurrentEntry, kind: str, identity: str
    ) -> _Child:
        if (
            current.child_kind == kind
            and current.child_id == identity
            and current.child is not None
        ):
            return current.child
        if kind == "targets":
            child: _Child = CorrectionTargets(self.db, attempt_id=identity, frame=current.frame)
        elif kind in {"retire", "seed"}:
            child = CorrectionWork(
                self.db, operation=kind, attempt_id=identity, source_binding=current.source.sha256
            )
        elif kind in {"generic", "row"}:
            self._row(state, current)
            if current.atom is None:
                raise RuntimeError("semantic admission lost its current row")
            child = GenericSemantics(
                self.db,
                attempt_id=identity,
                source_binding=current.source,
                atom=current.atom,
                catalog=self.catalog,
                operation=kind,
                certificates=self.certificates,
            )
        elif kind == "staging":
            child = AssertionStaging(
                self.db,
                attempt_id=identity,
                frame=current.frame,
                catalog_sha256=self.prefix.catalog_sha256,
                require_profiles=self.require_profiles,
            )
        elif kind == "entry":
            child = EntryPreparation(
                self.store,
                attempt_id=identity,
                frame=current.frame,
                catalog_sha256=self.prefix.catalog_sha256,
                require_profiles=self.require_profiles,
            )
        elif kind == "finish":
            child = SemanticFinish(
                self.store,
                attempt_id=identity,
                frame=current.frame,
                catalog_sha256=self.prefix.catalog_sha256,
                require_profiles=self.require_profiles,
            )
        else:
            raise RuntimeError("unknown native entry child")
        current.child_kind, current.child_id, current.child = kind, identity, child
        return child

    def _next(self, kind: str, current: _CurrentEntry, state: ValidationCheckpoint) -> str:
        correction = current.document["entry_kind"] == "correction"
        if kind == "entry":
            return "targets-admission" if correction else "staging-admission"
        if kind == "targets":
            return "staging-admission"
        if kind == "staging":
            return "retire-admission" if correction else "row-admit"
        if kind == "retire":
            return "derivation-replace"
        if kind == "seed":
            return "row-admit"
        if kind == "generic":
            return (
                "row-admission" if self._row(state, current)["type"] in ROW_TYPES else "lifecycle"
            )
        if kind == "row":
            return "lifecycle"
        if kind == "finish":
            return "recording"
        raise RuntimeError("unknown native entry completion")

    @declare_growth(_child_step_declaration(_row), delegates=_child_step_delegates(_row))
    def _child_step(self, before: ValidationSnapshot, current: _CurrentEntry) -> ValidationSnapshot:
        state = before.checkpoint
        admission = state.phase.endswith("-admission")
        kind = state.phase.removesuffix("-admission")
        identity = uuid4().hex if admission else state.child_id
        if identity is None:
            raise RuntimeError("native entry child lost its identity")
        child = self._child(state, current, kind, identity)
        if admission:
            batch: Any = child.prepare_admission()
        else:
            retained = child.load()
            if _digest(retained) != state.child_sha256:
                raise StaleValidationCheckpoint("native child differs from its parent checkpoint")
            if retained.done:
                if isinstance(child, (GenericSemantics, EntryPreparation)):
                    child.result()
                return self._move(before, self._next(kind, current, state))
            if isinstance(child, GenericSemantics):
                batch = child.prepare(step_budget=self._budget.rows, byte_budget=self._remaining())
            else:
                batch = child.prepare(row_budget=self._budget.rows, byte_budget=self._remaining())

        def apply() -> ValidationCheckpoint:
            changed = child._admit(batch) if admission else child._apply(batch)
            return replace(
                state,
                version=state.version + 1,
                phase=kind,
                child_id=identity,
                child_sha256=_digest(changed),
            )

        return self._commit(before, apply)

    @staticmethod
    def _graph_digest(graph: OnlineDag, after: _Snapshot | None = None) -> str:
        state = graph._load() if after is None else after
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "run_id": graph.run_id,
                    "version": str(state.version),
                    "work": state.work.encoded(),
                }
            )
        ).hexdigest()

    @declare_growth(_graph_step_declaration(), delegates=_graph_step_delegates())
    def _graph_step(self, before: ValidationSnapshot) -> ValidationSnapshot:
        state = before.checkpoint
        kind, phase = state.phase.split("-", 1)
        row = self._one("SELECT run_id FROM dag_runs WHERE kind=?", (kind,))
        if row is None:
            raise RuntimeError("entry replay lost its active graph")
        graph = OnlineDag(self.db, kind, row[0])
        if phase == "replace":
            replacement = graph._prepare_replacement(uuid4().hex)

            def apply() -> ValidationCheckpoint:
                new = graph._apply_replacement(replacement)
                return replace(
                    state,
                    version=state.version + 1,
                    phase=kind + "-retire",
                    child_id=new.run_id,
                    child_sha256=self._graph_digest(new, replacement.after),
                )

            result = self._commit(before, apply)
            self.store._dag_runs[kind] = replacement.run_id
            return result
        if state.child_id != graph.run_id or state.child_sha256 != self._graph_digest(graph):
            raise StaleValidationCheckpoint("retired graph differs from its parent checkpoint")
        batch = graph._prepare_retirement(self._budget.rows, byte_budget=self._remaining())
        if batch is None:
            return self._move(
                before, "specialization-replace" if kind == "derivation" else "seed-admission"
            )
        self._claim(batch.octets)

        def retire() -> ValidationCheckpoint:
            graph._apply_retirement(batch)
            return replace(
                state,
                version=state.version + 1,
                child_sha256=self._graph_digest(graph, batch.after),
            )

        return self._commit(before, retire)

    @declare_growth(
        _publication_declaration(),
        delegates=(
            ("plan", JournalPrefix.prepare_publication),
            ("application", JournalPrefix._apply),
            ("fence", ValidationLedger._fence),
            ("publication", ValidationLedger._publish),
        ),
    )
    def _publish(self, before: ValidationSnapshot, current: _CurrentEntry) -> ValidationSnapshot:
        "Atomically accept one checked native frame and replace its quarantine checkpoint."
        state = before.checkpoint
        publication = self.prefix.prepare_publication(current.prepared)
        after = ValidationCheckpoint(
            version=state.version + 1, frame_ordinal=state.frame_ordinal + 1
        )
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            self.ledger._fence(before)
            accepted = self.prefix._apply(publication)
            self.db.execute("DELETE FROM journal_validation_frame WHERE singleton=1").close()
            result = self.ledger._publish(
                before,
                after,
                prefix=(self.prefix.attempt_id, self.prefix.binding, accepted.to_json()),
            )
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        self.store.frame_count = after.frame_ordinal
        self._current = None
        return result

    @declare_growth(
        _row_transition_declaration("admission", _row),
        delegates=_row_transition_delegates("admission", _row),
    )
    def _admit_row(self, before: ValidationSnapshot, current: _CurrentEntry) -> ValidationSnapshot:
        state = before.checkpoint
        selected = read_changed_assertion(
            self.db, after=state.row_after, byte_budget=self._remaining()
        )
        if selected is None:
            return self._move(before, "finish-admission")
        if selected.raw is None:
            after = replace(state, version=state.version + 1, row_after=selected.assertion_id)
        else:
            self._claim(len(selected.raw))
            current.row_key, current.row_sha256, current.row = (
                selected.assertion_id,
                hashlib.sha256(selected.raw).hexdigest(),
                json.loads(selected.raw),
            )
            current.atom = GenericAtom.admit(current.row)
            after = replace(
                state,
                version=state.version + 1,
                phase="generic-admission",
                row_key=current.row_key,
                row_sha256=current.row_sha256,
            )
        return self._commit(before, lambda: after)

    @declare_growth(
        _row_transition_declaration("lifecycle", _row),
        delegates=_row_transition_delegates("lifecycle", _row),
    )
    def _lifecycle(self, before: ValidationSnapshot, current: _CurrentEntry) -> ValidationSnapshot:
        state = before.checkpoint
        row = self._row(state, current)

        def lifecycle() -> ValidationCheckpoint:
            if row["type"] in {"usage", "invalidation"} and "at" in row:
                self.store._record_lifecycle_time(
                    row["state"]["object_id"], row["type"], time_ns(row["at"])
                )
            return replace(
                state,
                version=state.version + 1,
                phase="row-admit",
                row_after=state.row_key,
                row_key=None,
                row_sha256=None,
                child_id=None,
                child_sha256=None,
            )

        result = self._commit(before, lifecycle)
        current.row_key, current.row_sha256, current.row, current.atom = None, None, None, None
        return result

    @declare_growth(
        _advance_declaration(_entry, _admit_row, _lifecycle, _child_step, _graph_step, _publish),
        delegates=_advance_delegates(
            _entry, _admit_row, _lifecycle, _child_step, _graph_step, _publish
        ),
    )
    def advance(self, budget: ReplayBudget = _DEFAULT_BUDGET) -> ValidationSnapshot:
        before = self.ledger.load()
        self._start(before, budget)
        state = before.checkpoint
        if state.phase in {"frame", "terminal", "done"}:
            return before
        if state.phase == "failed":
            raise RuntimeError("entry replay is quarantined after failure")
        current = self._entry(state)
        if state.phase.startswith(("derivation-", "specialization-")):
            return self._graph_step(before)
        if state.phase == "row-admit":
            return self._admit_row(before, current)
        if state.phase == "lifecycle":
            return self._lifecycle(before, current)
        if state.phase == "recording":
            self.store._check_entry_recording(current.document)
            return self._move(before, "publish")
        if state.phase == "publish":
            return self._publish(before, current)
        return self._child_step(before, current)

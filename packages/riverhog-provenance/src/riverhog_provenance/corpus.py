"""Disk-backed validation across independently evaluated canonical journals."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self, cast

from riverhog_archive_contracts import HistoryJournalAnchor
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
from riverhog_provenance_contracts import canonical_document, require_canonical_uuid_urn

from ._corpus_order import CorpusOrderProgress, _CorpusOrder
from .common import fingerprint
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .delivery import _delivery_projection, _selected_delivery_occurrence
from .errors import JournalValidationCapacity, ProvenanceValidationError
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
                multiply(number(32, SCALAR), records),
                "Includes both connections, schema, empty reads, point updates and queue "
                "operations and atomic checkpoint transactions.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                multiply(number(32, SCALAR), records),
                "All input, degree, seed, outgoing and queue rows are observed.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                multiply(number(8, SCALAR), records, widths),
                "Full endpoint and decimal-degree bytes are counted, without abbreviated records.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                multiply(number(8, SCALAR), records, widths),
                "All SQL parameter bytes, including repeatedly written keys and exact degrees.",
            ),
            Bound(
                "written_rows",
                COUNT,
                "upper",
                multiply(number(8, SCALAR), records),
                "Includes successful node insertion, degree replacement and ready "
                "insertion/deletion.",
            ),
            Bound(
                "written_octets",
                OCTETS,
                "upper",
                multiply(number(8, SCALAR), records, widths),
                "Full logical node/ready fields for every successful row mutation, including "
                "deletion.",
            ),
            Bound(
                "degree_decoded_octets",
                OCTETS,
                "upper",
                multiply(number(24, SCALAR), records, dimension("degree_octets")),
                "Exact degree and checkpoint counters decode bounded decimal scalars.",
            ),
            Bound(
                "degree_encoded_octets",
                OCTETS,
                "upper",
                multiply(number(24, SCALAR), records, dimension("degree_octets")),
                "Exact degree and checkpoint counters use decimal scalars, not SQLite arithmetic.",
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
            "The whole-run wrapper uses 1024 atoms per quantum; Native metadata widths "
            "fit its admitted byte budget.",
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


def _source_ingest_declaration(*, bounded: bool = False) -> Declaration:
    def count(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    work = Expression(
        "sum",
        terms=(
            count(4),
            Expression("product", terms=(count(8, SCALAR), dimension("new_frames"))),
            Expression("product", terms=(count(8, SCALAR), dimension("new_assertions"))),
            dimension("new_references"),
            Expression("product", terms=(count(2, SCALAR), dimension("new_retractions"))),
        ),
    )
    return Declaration(
        id=(
            "riverhog-provenance.corpus-source-advance.growth/v1"
            if bounded
            else "riverhog-provenance.corpus-source-extension.growth/v1"
        ),
        authority="riverhog-provenance",
        subjects=(
            "riverhog_provenance.corpus.CanonicalCorpusValidator."
            + ("advance_source" if bounded else "_add_source"),
        ),
        scope="native-corpus-raw-source-extension-after-exact-shared-prefix-proof",
        starts_at="receive one independently accepted immutable Native snapshot",
        ends_at="record only its missing complete source frames and the last atomic raw terminal",
        dimensions=(
            Dimension("new_frames", COUNT, "Missing source frames admitted by this call."),
            Dimension(
                "frame_budget", COUNT, "Maximum bounded Native entry atoms admitted by the call."
            ),
            Dimension("new_assertions", COUNT, "Raw assertions in those source frames."),
            Dimension("new_references", COUNT, "Raw external references in those source frames."),
            Dimension("new_retractions", COUNT, "Original assertion retirements in new frames."),
            Dimension("frame_octets", OCTETS, "Complete newly loaded RFC 7464 frame octets."),
            Dimension("entry_octets", OCTETS, "Original JSON octets in the newly loaded frames."),
        ),
        bounds=(
            Bound(
                "admitted_frames",
                COUNT,
                "upper",
                dimension("frame_budget"),
                "Every actual Native source frame load is charged to the caller's quantum.",
            ),
            Bound(
                "admitted_frame_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        dimension("frame_budget"),
                        count(PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 2, OCTETS_PER_COUNT),
                    ),
                ),
                "Each admitted Native JSON atom retains the existing wire envelope plus RS/LF.",
            ),
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
                Expression("sum", terms=(dimension("new_frames"), count(2))),
                "One optional exact source endpoint, one shared cut and one header per new frame.",
            ),
        ),
        conditions=(
            "Every input owns an open independently accepted frozen Native projection.",
            "An explicit through selects only an accepted prefix of that same projection.",
            "Original journal identity, exact reference, prefix bytes and SHA agree at overlap.",
            "The caller retains admission and current authority for the selected source union.",
        ),
        invariants=(
            "An entry's assertions, retirements, references, fork and continuation are atomic.",
            "Retry unfinished entries while retaining each completed exact source terminal.",
            "Raw reuse never discharges the selected effective-snapshot semantic obligations.",
            "A shared prefix never makes a documentary dependency an effective selected root.",
            "Source identity/domain conflicts and divergent writers fail closed.",
        ),
        limitations=(
            "Native indexed headers and their SHA padding have separate delegate evidence.",
            "This profile counts payload reads, entry hashing and corpus SQL statement visits.",
            "Canonical encoding, index/metadata bytes and storage-engine work require composition.",
            "Effective snapshot traversal, corpus references, comparisons and cycles are separate.",
            "Source indexing does not admit retained ownership or durable parent progress.",
        ),
    )


def _historical_assertion_declaration() -> Declaration:
    def constant(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def product(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    records = add(dimension("retained_assertions"), dimension("retained_frames"), constant(1))
    return Declaration(
        id="riverhog-provenance.corpus-historical-assertion.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus.CanonicalCorpusValidator.historical_assertion",),
        scope="exact-native-source-prefix-effective-object-point-selection",
        starts_at="receive one exact source anchor, object identity and explicit byte admission",
        ends_at="return absence or one detached effective assertion with original support",
        dimensions=(
            Dimension("retained_assertions", COUNT, "All original corpus assertion index rows."),
            Dimension("retained_frames", COUNT, "All exact corpus source entry index rows."),
            Dimension("anchor_octets", OCTETS, "Complete canonical selected anchor octets."),
            Dimension(
                "selected_octets",
                OCTETS,
                "Admitted assertion and original entry reference octets, zero for absence.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                constant(3),
                "One exact header, one latest-birth seek, at most one admitted payload.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                constant(3),
                "The latest-birth range returns at most one row, including retired gaps.",
            ),
            Bound(
                "closed_cursors",
                COUNT,
                "upper",
                constant(3),
                "Every point cursor closes before subsequent work or return.",
            ),
            Bound(
                "metadata_octets",
                OCTETS,
                "upper",
                add(
                    dimension("anchor_octets"),
                    constant(512, OCTETS),
                    product(
                        Expression("log2_ceil", terms=(records,)),
                        constant(1, OCTETS),
                    ),
                ),
                "Exact header, identity/type/size and decimal positions; future retirement "
                "width follows the retained frame extent, not the earlier selected anchor.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                add(
                    product(constant(4, SCALAR), dimension("anchor_octets")), constant(512, OCTETS)
                ),
                "Full repeated source, object, assertion, sequence and admitted size keys.",
            ),
            Bound(
                "payload_octets",
                OCTETS,
                "equal",
                dimension("selected_octets"),
                "Only one admitted assertion and its original entry reference are fetched.",
            ),
            Bound(
                "decoded_octets",
                OCTETS,
                "equal",
                dimension("selected_octets"),
                "Only the selected original reference and assertion are decoded.",
            ),
            Bound(
                "json_decodes",
                COUNT,
                "upper",
                constant(2),
                "No assertion decoding across absent, retired or unrelated rows.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    constant(768), product(constant(128), Expression("log2_ceil", terms=(records,)))
                ),
                "The stored-width tuple index seeks exact journal, object and numeric birth order.",
            ),
            Bound(
                "source_frame_reads",
                COUNT,
                "equal",
                constant(0),
                "No journal replay or original frame load.",
            ),
        ),
        conditions=(
            "Native source admission and exact shared-prefix proof precede corpus indexing.",
            "The owning index retains canonical decimal birth and retirement positions.",
            "A positive caller byte admission precedes any selected assertion payload read.",
        ),
        invariants=(
            "Effective intervals are half open; replacement begins at the correction entry.",
            "Retired gaps return absence without scanning for an earlier surviving assertion.",
            "The original documentary assertion address is preserved in every selected result.",
            "Independent journal sources do not substitute for the exact selected anchor.",
        ),
        limitations=(
            "This bounds point source attribution, not whole historical semantic validation.",
            "Source population, global corpus validity, custody and current access are separate.",
            "Retained workspace admission and durable parent progress require composition.",
            "VM constants depend on SQLite; physical I/O and elapsed time remain observations.",
        ),
    )


def _delivery_support_declaration() -> Declaration:
    original = _historical_assertion_declaration()
    meanings = {
        "sql_statements": "One source header, four latest-birth seeks and four metadata reads.",
        "returned_rows": "Every exact point or range seek returns at most one row.",
        "closed_cursors": "Each cursor closes before another query or return.",
        "json_decodes": "Decode only four admitted projections and original entry references.",
        "payload_octets": "Only the four selected projections and original entry references.",
        "decoded_octets": "Exactly the admitted projection and entry-reference octets.",
    }
    limits = {
        "sql_statements": 9,
        "returned_rows": 9,
        "closed_cursors": 9,
        "json_decodes": 8,
    }
    bounds = []
    for bound in original.bounds:
        if bound.metric in limits:
            bound = replace(
                bound,
                expression=Expression("constant", value=limits[bound.metric], unit=COUNT),
            )
        elif bound.metric in ("metadata_octets", "parameter_octets", "sqlite_vm_instructions"):
            bound = replace(
                bound,
                expression=Expression(
                    "product",
                    terms=(Expression("constant", value=4, unit=SCALAR), bound.expression),
                ),
            )
        if bound.metric in meanings:
            bound = replace(bound, meaning=meanings[bound.metric])
        bounds.append(bound)
    bounds.append(
        Bound(
            "full_assertion_reads",
            COUNT,
            "equal",
            Expression("constant", value=0, unit=COUNT),
            "Only Native-owned delivery semantic projections and original entry references.",
        )
    )
    return replace(
        original,
        id="riverhog-provenance.corpus-selected-delivery-support.growth/v1",
        subjects=("riverhog_provenance.corpus.CanonicalCorpusValidator.selected_delivery_support",),
        scope="exact-historical-native-delivery-support-without-full-assertion-payloads",
        starts_at="receive one exact member binding and explicit per-projection byte admission",
        ends_at="return original State and Occurrence support after Native delivery checks",
        dimensions=tuple(
            replace(
                dimension,
                meaning="Admitted semantic projections and their original entry references.",
            )
            if dimension.name == "selected_octets"
            else dimension
            for dimension in original.dimensions
        ),
        bounds=tuple(bounds),
        invariants=(
            *original.invariants,
            "The same Native delivery predicate checks association, State, Occurrence and fixity.",
            "No original assertion payload is loaded or substituted by a partial assertion result.",
        ),
        limitations=(
            *original.limitations,
            "Source ingestion derives each semantic projection once from accepted Native bytes.",
            "Per-selection metadata includes matching role/slot/size widths, not opaque payloads.",
        ),
    )


def _selection_declaration(phase: str) -> Declaration:
    def number(value: int, unit: Unit = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    bounded = phase == "advance_semantics"
    visits = Expression("sum", terms=(dimension("row_budget"), number(1)))
    return Declaration(
        id="riverhog-provenance.corpus-" + phase.replace("_", "-") + ".growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus.CanonicalCorpusValidator." + phase,),
        scope="one fixed Native selected-source union semantic registration"
        if bounded
        else "one exact Native source-prefix selection registration",
        starts_at="receive explicit work admission"
        if bounded
        else "receive an exact indexed anchor",
        ends_at="advance original semantic assertions once or report exhausted input"
        if bounded
        else "record the exact selected Native prefix",
        dimensions=(
            Dimension("row_budget", COUNT, "Maximum semantic input rows processed by this call."),
            Dimension(
                "byte_budget", OCTETS, "Maximum projected payload bytes decoded by this call."
            ),
        )
        if bounded
        else (Dimension("selected_prefixes", COUNT, "Already registered exact source prefixes."),),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("product", terms=(number(8, SCALAR), visits)) if bounded else number(2),
                "Indexed input, interval and payload seeks plus atomic obligation writes.",
            ),
            Bound(
                "processed_rows",
                COUNT,
                "upper",
                dimension("row_budget") if bounded else number(0),
                "One cursor advance per original semantic assertion, never per snapshot.",
            ),
            Bound(
                "projected_payload_octets",
                OCTETS,
                "upper",
                dimension("byte_budget") if bounded else number(0, OCTETS),
                "Length admission precedes every selected semantic projection fetch.",
            ),
            Bound(
                "json_decodes",
                COUNT,
                "upper",
                dimension("row_budget") if bounded else number(0),
                "Only admitted semantic projection atoms are decoded.",
            ),
            Bound(
                "full_assertion_reads",
                COUNT,
                "equal",
                number(0),
                "Original assertion payloads remain in a separate table and are not selected.",
            ),
            Bound(
                "source_frame_reads",
                COUNT,
                "equal",
                number(0),
                "Accepted Native source frames are never replayed by selection work.",
            ),
        ),
        conditions=(
            "Raw source indexing came from owned independently accepted Native summaries.",
            "Native entry semantics precede publication of every retained prefix header.",
            "The source and selected-prefix set remain fixed during this semantic attempt.",
        ),
        invariants=(
            "An assertion contributes only if a selected prefix intersects [birth,retirement).",
            "Complete each semantic assertion once; retry at most one unfinished metadata atom.",
            "A selected historical edge survives withdrawal at a later selected prefix.",
            "Interrupted registration retries one atom; prior cursor advances remain intact.",
        ),
        limitations=(
            "Retained workspace admission and durable parent checkpoints require composition.",
            "SQL visits and payload bytes exclude storage I/O and metadata-width composition.",
            "Foreign references, comparisons and acyclicity have separate completion work.",
            "Added inputs in a later whole-corpus validation start a new selection attempt.",
            "Completeness, member relevance, current access and publication remain separate.",
        ),
    )


def _dependency_declaration() -> Declaration:
    original = _selection_declaration("advance_semantics")
    return replace(
        original,
        id="riverhog-provenance.corpus-advance-dependencies.growth/v1",
        subjects=("riverhog_provenance.corpus.CanonicalCorpusValidator.advance_dependencies",),
        scope="fixed selected-corpus documentary dependency validation",
        starts_at="receive row and projected-byte admission after semantic registration",
        ends_at="check admitted references, forks or comparisons and retain the next atom",
        dimensions=(
            Dimension("row_budget", COUNT, "Maximum dependency atoms checked by this call."),
            Dimension(
                "byte_budget", OCTETS, "Maximum dependency payload bytes fetched by this call."
            ),
        ),
        bounds=tuple(
            replace(
                bound,
                meaning={
                    "sql_statements": (
                        "One metadata pass, admitted payload reads and exact target checks."
                    ),
                    "processed_rows": "Each checked obligation advances its cursor once.",
                    "projected_payload_octets": (
                        "Length admission precedes dependency payload reads."
                    ),
                    "json_decodes": "At most one admitted reference or anchor per atom.",
                }.get(bound.metric, bound.meaning),
            )
            for bound in original.bounds
        ),
        conditions=(
            "Raw Native source indexing and selected semantic registration are complete.",
            "The source and selected-prefix set remain frozen during this attempt.",
            "Indexed resume keys retain at most one unfinished metadata atom.",
        ),
        invariants=(
            "Foreign references retain exact journal, entry, assertion, object and type checks.",
            "References precede forks; forks precede comparisons; all precede cycle checks.",
            "Native size and SHA256 comparison policy is unchanged.",
            "Successful cursor advances are retained across a later interrupted atom.",
            "Validation performs no corpus writes and reads no original assertion payloads.",
        ),
        limitations=(
            "Completion covers dependencies, not cycle checking or whole-corpus acceptance.",
            "Retained workspace admission and durable parent checkpoints require composition.",
            "SQL visits and fetched payload bytes exclude storage I/O and metadata widths.",
            "A later explicit whole-corpus validation is a new fixed-selection attempt.",
            "Completeness, member relevance, current access and publication remain separate.",
        ),
    )


def _order_step_declaration() -> Declaration:
    original = _selection_declaration("advance_semantics")
    return replace(
        original,
        id="riverhog-provenance.corpus-advance-order.growth/v1",
        subjects=("riverhog_provenance.corpus.CanonicalCorpusValidator.advance_order",),
        scope="one bounded atomic cycle-ordering quantum for a fixed graph kind",
        dimensions=(
            Dimension("row_budget", COUNT, "Maximum ordering phase atoms per call."),
            Dimension(
                "byte_budget", OCTETS, "Maximum admitted checkpoint and scalar payload bytes."
            ),
        ),
        starts_at="receive row and payload-byte admission after complete dependency checks",
        ends_at="commit ordering mutations with their checkpoint or roll back the quantum",
        bounds=tuple(
            replace(
                bound,
                expression=Expression(
                    "product",
                    terms=(
                        Expression("constant", value=32, unit=SCALAR),
                        Expression(
                            "sum",
                            terms=(
                                Expression("dimension", name="row_budget"),
                                Expression("constant", value=1, unit=COUNT),
                            ),
                        ),
                    ),
                ),
                meaning="Checkpoint admission, private initialization and indexed atom work.",
            )
            if bound.metric == "sql_statements"
            else replace(bound, meaning="At most the admitted number of phase atoms.")
            if bound.metric == "processed_rows"
            else replace(bound, meaning="Checkpoint, endpoint and degree payload admission.")
            if bound.metric == "projected_payload_octets"
            else replace(
                bound,
                expression=Expression("constant", value=0, unit=COUNT),
                meaning="Ordering uses exact indexed scalars, without JSON.",
            )
            if bound.metric == "json_decodes"
            else bound
            for bound in original.bounds
        ),
        conditions=(
            "The Native source and selected semantic set are fixed.",
            "Documentary dependencies have completed before graph ordering begins.",
            "A graph kind retains disk-backed indegrees, ready keys and an atomic checkpoint.",
        ),
        invariants=(
            "Ordering retains the exact independent derivation and specialization cycle rules.",
            "Each committed input, seed or outgoing key advances once.",
            "Rollback repeats only the unfinished bounded quantum.",
            "A lost return after commit reloads the published checkpoint.",
            "No original assertion or source frame is read.",
        ),
        limitations=(
            "A completed kind does not complete the other kind or grant current access.",
            "Retained admission and durable worker binding have separate lifecycle costs.",
            "This bounds SQL visits and fetched payload; complete ordering owns physical growth.",
        ),
    )


def _semantic_projection(value: Mapping[str, Any]) -> dict[str, Any] | None:
    kind = value["type"]
    if kind in ("derivation", "specialization"):
        left, right = (
            ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
        )
        return {
            "type": kind,
            "source": value[left]["object_id"],
            "target": value[right]["object_id"],
        }
    if kind == "content_comparison":
        return {
            "type": kind,
            "left": value["left_description"]["object_id"],
            "right": value["right_description"]["object_id"],
            "result": value["result"],
        }
    return None


@dataclass(frozen=True, slots=True)
class CorpusDependencyProgress:
    """Documentary dependency progress, independently of cycle acceptance."""

    phase: str
    checked_obligations: int
    admitted_octets: int
    complete: bool


@dataclass(frozen=True, slots=True)
class CorpusSemanticProgress:
    """Selected semantic registration in this workspace, not global acceptance."""

    processed_assertions: int
    admitted_octets: int
    complete: bool


@dataclass(frozen=True, slots=True)
class CorpusSourceProgress:
    """Raw-source completion for one exact immutable Native input."""

    journal_id: str
    source_sha256: str
    source_octets: int
    accepted_frames: int
    complete: bool


@dataclass(frozen=True, slots=True)
class SelectedDeliverySupport:
    """Original documentary support after the selected Native delivery checks."""

    state_reference: dict[str, Any]
    occurrence_reference: dict[str, Any]


@dataclass(frozen=True, slots=True)
class HistoricalAssertion:
    """One effective assertion with its original documentary address.

    Values are detached from the corpus. The address remains original across
    later corrections; it does not grant source admission or current access.
    """

    value: dict[str, Any]
    reference: dict[str, Any]


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
        self._initialize_state()
        self._scratch: TemporaryDirectory[str] | None = TemporaryDirectory(
            prefix="riverhog-canonical-corpus-"
        )
        self._directory = Path(self._scratch.name)
        self._db = sqlite3.connect(self._directory / "corpus.sqlite3")
        self._db.execute("PRAGMA cache_size = -512")
        self._initialize_schema()

    @classmethod
    def _from_workspace(cls, db: sqlite3.Connection, directory: Path) -> Self:
        result = cls.__new__(cls)
        result._initialize_state()
        result._scratch = None
        result._directory, result._db = directory, db
        return result

    def _initialize_state(self) -> None:
        self._closed = False
        self._semantic_phase = "open"
        self._semantic_after: tuple[str, str] | None = None
        self._semantic_processed = 0
        self._dependency_phase = "references"
        self._dependency_after: tuple[Any, ...] | None = None
        self._dependency_pending: tuple[Any, ...] | None = None
        self._dependency_checked = 0
        self._orders: dict[str, _CorpusOrder] = {}

    def _initialize_schema(self) -> None:
        schema = (
            "CREATE TABLE identities (id TEXT PRIMARY KEY, domain TEXT, value BLOB);"
            "CREATE TABLE entries (journal TEXT, sequence TEXT, reference BLOB, prefix TEXT, "
            "bytes TEXT, PRIMARY KEY(journal, sequence));"
            "CREATE TABLE assertions (journal TEXT, assertion TEXT, object TEXT NOT NULL, "
            "type TEXT NOT NULL, sequence TEXT NOT NULL, sequence_width INTEGER NOT NULL, "
            "retired TEXT, entry BLOB, PRIMARY KEY(journal, assertion)) WITHOUT ROWID;"
            "CREATE TABLE assertion_payloads (journal TEXT, assertion TEXT, value BLOB NOT NULL, "
            "PRIMARY KEY(journal, assertion)) WITHOUT ROWID;"
            "CREATE TABLE delivery_projections (journal TEXT, assertion TEXT, value BLOB NOT NULL, "
            "PRIMARY KEY(journal, assertion)) WITHOUT ROWID;"
            "CREATE INDEX historical_objects ON assertions "
            "(journal, object, sequence_width, sequence);"
            "CREATE TABLE refs (value BLOB PRIMARY KEY);"
            "CREATE TABLE forks (anchor BLOB PRIMARY KEY);"
            "CREATE TABLE selected_prefixes (journal TEXT, width INTEGER, sequence TEXT, "
            "PRIMARY KEY(journal,width,sequence)) WITHOUT ROWID;"
            "CREATE TABLE semantic_inputs (journal TEXT, assertion TEXT, value BLOB NOT NULL, "
            "PRIMARY KEY(journal,assertion)) WITHOUT ROWID;"
            "CREATE TABLE fixities (object TEXT PRIMARY KEY, size TEXT, sha256 TEXT) WITHOUT ROWID;"
            "CREATE TABLE comparisons (journal TEXT, assertion TEXT, left_object TEXT, "
            "right_object TEXT, result TEXT, PRIMARY KEY(journal,assertion)) WITHOUT ROWID;"
            "CREATE TABLE edges (kind TEXT, source TEXT, target TEXT, "
            "PRIMARY KEY(kind, source, target));"
            "CREATE INDEX incoming ON edges(kind, target);"
            "CREATE TABLE source_extents (journal TEXT PRIMARY KEY, sequence TEXT NOT NULL);"
        )
        for statement in schema.split(";"):
            if statement:
                self._source_write(statement)

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("canonical corpus is closed")

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._closed = True
        self._reset_dependencies()
        self._close_orders()
        self._db.close()
        if self._scratch is not None:
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
    def _add_source(
        self,
        summary: JournalSummary,
        *,
        frame_budget: int | None = None,
        through: int | None = None,
    ) -> CorpusSourceProgress:
        self._require_open()
        self._require_source_selection_open()
        if summary._store is None:
            raise RuntimeError("corpus sources require an owned native journal summary")
        available_frames = len(summary.frames)
        if through is not None and (
            type(through) is not int or not 0 <= through < available_frames
        ):
            raise ValueError("corpus source prefix must select an accepted Native frame")
        frame_count = available_frames if through is None else through + 1
        if through is None:
            source_sha256, source_octets = summary.journal_sha256, summary.journal_bytes
        else:
            endpoint = summary.anchor_at(through)
            source_sha256, source_octets = endpoint["prefix_sha256"], int(endpoint["prefix_bytes"])
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
        stop = frame_count if frame_budget is None else min(frame_count, first + frame_budget)
        for sequence in range(first, stop):
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
                for target in document["body"].get("retracts", ()):
                    old = self._source_read(
                        "SELECT entry, retired FROM assertions WHERE journal=? AND assertion=?",
                        (summary.journal_id, target["assertion_id"]),
                    )
                    if old != (canonical_document(target["entry"]), None):
                        raise ProvenanceValidationError(
                            "corpus correction differs from its original active assertion"
                        )
                    self._source_write(
                        "UPDATE assertions SET retired=? WHERE journal=? AND assertion=?",
                        (document["sequence"], summary.journal_id, target["assertion_id"]),
                    )
                for _, assertion in iter_assertions(_body_assertions(document)):
                    encoded = canonical_document(assertion)
                    self._identity(assertion["assertion_id"], "assertion", encoded)
                    self._identity(assertion["id"], "referent", _identity_signature(assertion))
                    self._source_write(
                        "INSERT OR IGNORE INTO assertions VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                        (
                            summary.journal_id,
                            assertion["assertion_id"],
                            assertion["id"],
                            assertion["type"],
                            document["sequence"],
                            len(document["sequence"]),
                            row[0],
                        ),
                    )
                    self._source_write(
                        "INSERT OR IGNORE INTO assertion_payloads VALUES (?, ?, ?)",
                        (summary.journal_id, assertion["assertion_id"], encoded),
                    )
                    projection = _delivery_projection(assertion)
                    if projection is not None:
                        self._source_write(
                            "INSERT OR IGNORE INTO delivery_projections VALUES (?, ?, ?)",
                            (
                                summary.journal_id,
                                assertion["assertion_id"],
                                canonical_document(projection),
                            ),
                        )
                    semantic = _semantic_projection(assertion)
                    if semantic is not None:
                        self._source_write(
                            "INSERT OR IGNORE INTO semantic_inputs VALUES (?, ?, ?)",
                            (
                                summary.journal_id,
                                assertion["assertion_id"],
                                canonical_document(semantic),
                            ),
                        )
                    # Native description identity fixes content across evidence corrections.
                    if "content" in assertion:
                        size, digest = fingerprint(assertion["content"])
                        self._source_write(
                            "INSERT OR IGNORE INTO fixities VALUES (?, ?, ?)",
                            (assertion["id"], str(size), digest),
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
        return CorpusSourceProgress(
            summary.journal_id,
            source_sha256,
            source_octets,
            stop,
            stop == frame_count,
        )

    @declare_growth(
        _source_ingest_declaration(bounded=True), delegates=(("anchor", JournalSummary.anchor_at),)
    )
    def advance_source(
        self, summary: JournalSummary, *, frame_budget: int = 1, through: int | None = None
    ) -> CorpusSourceProgress:
        """Advance raw indexing by at most the caller's bounded Native entry atoms.

        Each atom has Native's existing entry wire bound. Completed exact
        prefixes survive interruption within this corpus workspace; a fresh call
        proves overlap before loading any missing frame. An explicit through
        selects an accepted source prefix, including its exact digest and byte
        count; later frames contribute no raw obligations. This does not register
        selected effective snapshots or accept global corpus/history semantics.
        """
        self._require_open()
        if type(frame_budget) is not int or frame_budget < 1:
            raise ValueError("corpus source frame budget must be positive")
        return self._add_source(summary, frame_budget=frame_budget, through=through)

    @declare_growth(_historical_assertion_declaration())
    def historical_assertion(
        self,
        anchor: HistoryJournalAnchor,
        object_id: str,
        *,
        byte_budget: int,
    ) -> HistoricalAssertion | None:
        """Read one effective object at an exact admitted source prefix.

        The budget admits the selected assertion and original entry reference
        before either payload is fetched. A retired gap is absent, without
        falling back to an earlier assertion. This source view does not accept
        a whole historical graph or the corpus's separate global obligations.
        """
        self._require_open()
        self._check_selection_budget(byte_budget)
        self._check_source_anchor(anchor)
        return self._historical_value(anchor, object_id, byte_budget=byte_budget, projection=False)

    @staticmethod
    def _check_selection_budget(byte_budget: int) -> None:
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("historical assertion byte budget must be positive")

    def _check_source_anchor(self, anchor: HistoryJournalAnchor) -> None:
        if not isinstance(anchor, HistoryJournalAnchor):
            raise TypeError("historical selection requires an exact history journal anchor")
        selected = cast(dict[str, Any], anchor.to_mapping())
        found = self._source_read(
            "SELECT reference, prefix, bytes FROM entries WHERE journal=? AND sequence=?",
            (anchor.journal_id, selected["through"]["sequence"]),
        )
        if found != (
            canonical_document(selected["through"]),
            anchor.prefix_sha256,
            str(anchor.prefix_bytes),
        ):
            raise ProvenanceValidationError("historical prefix is not an exact corpus source")

    def _historical_value(
        self, anchor: HistoryJournalAnchor, object_id: str, *, byte_budget: int, projection: bool
    ) -> HistoricalAssertion | None:
        require_canonical_uuid_urn(object_id)
        sequence = str(anchor.through_sequence)
        table = "delivery_projections" if projection else "assertion_payloads"
        metadata = self._source_read(
            "SELECT a.assertion,a.type,a.sequence,a.retired,length(a.entry),length(p.value) "
            "FROM assertions a LEFT JOIN " + table + " p "
            "ON p.journal=a.journal AND p.assertion=a.assertion "
            "WHERE a.journal=? AND a.object=? "
            "AND (a.sequence_width, a.sequence) <= (?, ?) "
            "ORDER BY a.sequence_width DESC, a.sequence DESC LIMIT 1",
            (anchor.journal_id, object_id, len(sequence), sequence),
        )
        if metadata is None:
            return None
        assertion_id, kind, born, retired, entry_size, value_size = metadata
        if retired is not None and (len(retired), retired) <= (len(sequence), sequence):
            return None
        if value_size is None:
            if projection and kind not in (
                "delivery_association",
                "state",
                "occurrence",
                "observation",
            ):
                return None
            raise RuntimeError("historical assertion payload is missing from its source")
        size = entry_size + value_size
        if size > byte_budget:
            raise JournalValidationCapacity("historical assertion exceeds the admitted byte budget")
        encoded = self._source_read(
            "SELECT substr(a.entry, 1, ?),substr(p.value, 1, ?) "
            "FROM assertions a JOIN " + table + " p "
            "ON p.journal=a.journal AND p.assertion=a.assertion "
            "WHERE a.journal=? AND a.assertion=? AND length(a.entry)=? AND length(p.value)=?",
            (entry_size, value_size, anchor.journal_id, assertion_id, entry_size, value_size),
        )
        if encoded is None:
            raise RuntimeError("historical assertion changed outside its source fence")
        entry, value = json.loads(encoded[0]), json.loads(encoded[1])
        if (
            entry["sequence"] != born
            or value["id"] != object_id
            or value["type"] != kind
            or value["assertion_id"] != assertion_id
        ):
            raise RuntimeError("historical assertion differs from its source metadata")
        return HistoricalAssertion(
            value,
            {
                "scope": "external",
                "journal_id": anchor.journal_id,
                "entry": entry,
                "assertion_id": assertion_id,
                "object_id": object_id,
                "object_type": kind,
            },
        )

    @declare_growth(_delivery_support_declaration())
    def selected_delivery_support(
        self,
        *,
        binding: Mapping[str, Any],
        artifact_id: str,
        byte_count: int,
        sha256: str,
        member_role: str,
        metadata_byte_budget: int,
    ) -> SelectedDeliverySupport:
        """Check delivery policy using Native's narrow historical metadata.

        The explicit admission applies to each projected assertion and original
        entry reference. No full assertion is read. The result is documentary
        support, not whole-corpus, historical-graph or current-access acceptance.
        """
        self._require_open()
        self._check_selection_budget(metadata_byte_budget)
        anchor = HistoryJournalAnchor.from_mapping(binding.get("journal"))
        self._check_source_anchor(anchor)
        references: dict[str, dict[str, Any]] = {}

        def lookup(identity: str) -> dict[str, Any] | None:
            result = self._historical_value(
                anchor, identity, byte_budget=metadata_byte_budget, projection=True
            )
            if result is None:
                return None
            references[identity] = result.reference
            return result.value

        state, occurrence = _selected_delivery_occurrence(
            anchor.to_mapping(),
            lookup,
            binding=binding,
            artifact_id=artifact_id,
            byte_count=byte_count,
            sha256=sha256,
            member_role=member_role,
        )
        return SelectedDeliverySupport(references[state["id"]], references[occurrence["id"]])

    def _require_source_selection_open(self) -> None:
        if self._semantic_phase != "open":
            raise RuntimeError("canonical corpus source selection is frozen during semantic work")

    @declare_growth(_selection_declaration("select_source"))
    def select_source(self, anchor: HistoryJournalAnchor) -> None:
        """Register one exact intrinsically checked Native prefix.

        Native publishes each original entry header only after its semantic
        phases. This registers the prefix's effective intervals; it does not
        accept foreign corpus obligations or grant member relevance/access.
        """
        self._require_open()
        self._require_source_selection_open()
        self._check_source_anchor(anchor)
        sequence = str(anchor.through_sequence)
        self._source_write(
            "INSERT OR IGNORE INTO selected_prefixes VALUES (?, ?, ?)",
            (anchor.journal_id, len(sequence), sequence),
        )

    @declare_growth(_selection_declaration("advance_semantics"))
    def advance_semantics(
        self, *, row_budget: int = 64, byte_budget: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
    ) -> CorpusSemanticProgress:
        """Advance semantic registration over one fixed selected-source union.

        Exact source-prefix positions seek each assertion's half-open lifetime.
        Full effective snapshots and original assertions are never loaded.
        Budgets bound this call; global validation and durable progress retain
        their own completion requirements.
        """
        self._require_open()
        if type(row_budget) is not int or row_budget < 1:
            raise ValueError("corpus semantic row budget must be positive")
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("corpus semantic byte budget must be positive")
        if self._semantic_phase == "complete":
            return CorpusSemanticProgress(self._semantic_processed, 0, True)
        if self._semantic_phase == "open":
            self._semantic_phase, self._semantic_after, self._semantic_processed = (
                "walking",
                None,
                0,
            )
        admitted = 0
        for _ in range(row_budget):
            found = self._source_read(
                "SELECT a.journal,a.assertion,a.type,a.sequence,a.retired,length(s.value) "
                "FROM semantic_inputs s JOIN assertions a "
                "ON a.journal=s.journal AND a.assertion=s.assertion "
                "WHERE (s.journal,s.assertion)>(?,?) "
                "ORDER BY s.journal,s.assertion LIMIT 1",
                self._semantic_after or ("", ""),
            )
            if found is None:
                self._semantic_phase = "complete"
                break
            journal, assertion_id, kind, born, retired, size = found
            selected = self._source_read(
                "SELECT sequence FROM selected_prefixes WHERE journal=? "
                "AND (width,sequence)>=(?,?) ORDER BY width,sequence LIMIT 1",
                (journal, len(born), born),
            )
            effective = selected is not None and (
                retired is None or (len(selected[0]), selected[0]) < (len(retired), retired)
            )
            if effective:
                if size > byte_budget:
                    raise JournalValidationCapacity(
                        "semantic projection exceeds admitted byte budget"
                    )
                if size > byte_budget - admitted:
                    break
                encoded = self._source_read(
                    "SELECT substr(value,1,?) FROM semantic_inputs "
                    "WHERE journal=? AND assertion=? AND length(value)=?",
                    (size, journal, assertion_id, size),
                )
                if encoded is None:
                    raise RuntimeError("semantic projection changed outside its source fence")
                value = json.loads(encoded[0])
                if value["type"] != kind:
                    raise RuntimeError("semantic projection differs from its source metadata")
                self._source_write("SAVEPOINT corpus_semantic_assertion")
                try:
                    if kind == "content_comparison":
                        self._source_write(
                            "INSERT OR IGNORE INTO comparisons VALUES (?, ?, ?, ?, ?)",
                            (journal, assertion_id, value["left"], value["right"], value["result"]),
                        )
                    else:
                        self._source_write(
                            "INSERT OR IGNORE INTO edges VALUES (?, ?, ?)",
                            (kind, value["source"], value["target"]),
                        )
                except BaseException:
                    self._source_write("ROLLBACK TO corpus_semantic_assertion")
                    self._source_write("RELEASE corpus_semantic_assertion")
                    raise
                else:
                    self._source_write("RELEASE corpus_semantic_assertion")
                admitted += size
            self._semantic_after = (journal, assertion_id)
            self._semantic_processed += 1
        return CorpusSemanticProgress(
            self._semantic_processed, admitted, self._semantic_phase == "complete"
        )

    def add(self, summary: JournalSummary) -> None:
        self._add_source(summary)
        self.select_source(HistoryJournalAnchor.from_mapping(summary.anchor))

    def _resolve(self, reference: dict[str, Any]) -> None:
        found = self._source_read(
            "SELECT entry=? AND object=? AND type=? FROM assertions "
            "WHERE journal=? AND assertion=?",
            (
                canonical_document(reference["entry"]),
                reference["object_id"],
                reference["object_type"],
                reference["journal_id"],
                reference["assertion_id"],
            ),
        )
        if found is None:
            raise ProvenanceValidationError("unresolved required external journal assertion")
        if found != (1,):
            raise ProvenanceValidationError("foreign reference does not match its exact assertion")

    def _reset_dependencies(self) -> None:
        self._dependency_after, self._dependency_pending = None, None
        self._dependency_phase, self._dependency_checked = "references", 0

    def _check_dependency(self, pending: tuple[Any, ...], size: int) -> None:
        if self._dependency_phase in ("references", "forks"):
            table, column = (
                ("refs", "value") if self._dependency_phase == "references" else ("forks", "anchor")
            )
            encoded = self._source_read(
                f"SELECT substr({column},1,?) FROM {table} WHERE rowid=? AND length({column})=?",
                (size, pending[0], size),
            )
            if encoded is None:
                raise RuntimeError("documentary dependency changed outside its source fence")
            value = json.loads(encoded[0])
            if self._dependency_phase == "references":
                self._resolve(value)
            else:
                found = self._source_read(
                    "SELECT reference=? AND prefix=? AND bytes=? FROM entries "
                    "WHERE journal=? AND sequence=?",
                    (
                        canonical_document(value["through"]),
                        value["prefix_sha256"],
                        value["prefix_bytes"],
                        value["journal_id"],
                        value["through"]["sequence"],
                    ),
                )
                if found != (1,):
                    raise ProvenanceValidationError(
                        "fork prefix anchor could not be resolved exactly"
                    )
            return
        value = self._source_read(
            "SELECT left_object,right_object,result FROM comparisons "
            "WHERE journal=? AND assertion=? AND "
            "length(CAST(left_object AS BLOB))+length(CAST(right_object AS BLOB))"
            "+length(CAST(result AS BLOB))=?",
            (*pending[:2], size),
        )
        if value is None:
            raise RuntimeError("comparison dependency changed outside its source fence")
        left, right, result = value
        # Prefix admission checked local presence; Native fixes description content.
        found = self._source_read(
            "SELECT l.size=r.size AND l.sha256=r.sha256 FROM fixities l,fixities r "
            "WHERE l.object=? AND r.object=?",
            (left, right),
        )
        if found is None:
            if result != "indeterminate":
                raise ProvenanceValidationError("resolved comparison lacks complete fixity")
        elif (result == "matching_fixity" and found != (1,)) or (
            result == "different" and found == (1,)
        ):
            raise ProvenanceValidationError("comparison contradicts foreign content evidence")

    @declare_growth(_dependency_declaration())
    def advance_dependencies(
        self, *, row_budget: int = 64, byte_budget: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
    ) -> CorpusDependencyProgress:
        """Check a bounded portion of the fixed corpus's documentary obligations.

        Each phase retains an indexed resume key and one unfinished atom.
        Length admission precedes payload fetches. Completion does not include
        cycle checking, historical member relevance or current authorization.
        """
        self._require_open()
        if type(row_budget) is not int or row_budget < 1:
            raise ValueError("corpus dependency row budget must be positive")
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("corpus dependency byte budget must be positive")
        if self._semantic_phase != "complete":
            raise RuntimeError("corpus dependencies require complete semantic registration")
        if self._dependency_phase == "complete":
            return CorpusDependencyProgress("complete", self._dependency_checked, 0, True)
        admitted = 0
        for _ in range(row_budget):
            if self._dependency_pending is None:
                if self._dependency_phase in ("references", "forks"):
                    table, column = (
                        ("refs", "value")
                        if self._dependency_phase == "references"
                        else ("forks", "anchor")
                    )
                    self._dependency_pending = self._source_read(
                        f"SELECT rowid,length({column}) FROM {table} "
                        "WHERE rowid>? ORDER BY rowid LIMIT 1",
                        self._dependency_after or (0,),
                    )
                else:
                    self._dependency_pending = self._source_read(
                        "SELECT journal,assertion,length(CAST(left_object AS BLOB))"
                        "+length(CAST(right_object AS BLOB))+length(CAST(result AS BLOB)) "
                        "FROM comparisons WHERE (journal,assertion)>(?,?) "
                        "ORDER BY journal,assertion LIMIT 1",
                        self._dependency_after or ("", ""),
                    )
            if self._dependency_pending is None:
                self._dependency_after = None
                self._dependency_phase = {
                    "references": "forks",
                    "forks": "comparisons",
                    "comparisons": "complete",
                }[self._dependency_phase]
                break
            size = self._dependency_pending[-1]
            if size > byte_budget:
                raise JournalValidationCapacity("dependency atom exceeds admitted byte budget")
            if size > byte_budget - admitted:
                break
            self._check_dependency(self._dependency_pending, size)
            self._dependency_after = self._dependency_pending[:-1]
            self._dependency_pending = None
            self._dependency_checked += 1
            admitted += size
        return CorpusDependencyProgress(
            self._dependency_phase,
            self._dependency_checked,
            admitted,
            self._dependency_phase == "complete",
        )

    def _close_orders(self) -> None:
        for order in self._orders.values():
            order.close()
        self._orders.clear()

    @declare_growth(_order_step_declaration())
    def advance_order(
        self,
        kind: str,
        *,
        row_budget: int = 64,
        byte_budget: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX,
    ) -> CorpusOrderProgress:
        """Advance one kind's exact cycle check with atomic bounded progress."""
        self._require_open()
        if kind not in ("derivation", "specialization"):
            raise ValueError("unknown canonical corpus graph kind")
        if type(row_budget) is not int or row_budget < 1:
            raise ValueError("corpus ordering row budget must be positive")
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("corpus ordering byte budget must be positive")
        if self._semantic_phase != "complete" or self._dependency_phase != "complete":
            raise RuntimeError("corpus ordering requires complete semantics and dependencies")
        if kind not in self._orders:
            self._orders[kind] = _CorpusOrder(self._db, str(self._directory), kind)
        return self._orders[kind].advance(row_budget=row_budget, byte_budget=byte_budget)

    def validate(self) -> None:
        self._require_open()
        try:
            while not self.advance_semantics().complete:
                pass
            self._validate_registered()
        finally:
            # A later explicit batch validation has a new fixed input set.
            self._semantic_phase = "open"
            self._reset_dependencies()
            self._close_orders()

    def _validate_registered(self) -> None:
        while not self.advance_dependencies().complete:
            pass
        for kind in ("derivation", "specialization"):
            if kind in self._orders:
                while not self.advance_order(kind).complete:
                    pass
            else:
                self._acyclic(kind)

    @declare_growth(_CORPUS_ORDER_GROWTH)
    def _acyclic(self, kind: str) -> None:
        with _CorpusOrder(self._db, str(self._directory), kind) as order:
            while not order.advance(
                row_budget=1024, byte_budget=PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
            ).complete:
                pass


def validate_canonical_corpus(summaries: Iterable[JournalSummary]) -> None:
    with CanonicalCorpusValidator() as validator:
        for summary in summaries:
            validator.add(summary)
        validator.validate()


__all__ = [
    "CanonicalCorpusValidator",
    "CorpusSourceProgress",
    "CorpusDependencyProgress",
    "CorpusOrderProgress",
    "CorpusSemanticProgress",
    "HistoricalAssertion",
    "SelectedDeliverySupport",
    "validate_canonical_corpus",
]

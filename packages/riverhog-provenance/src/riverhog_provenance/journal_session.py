"""Bounded native journal validation with explicit attempt and reader ownership."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol, Self, cast

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_provenance_contracts import ContractCatalog
from sha256_continuation import sha256_compress

from ._entry_replay import EntryReplay, EntryReplayCapacity, ReplayBudget
from ._entry_replay import install as install_replay
from ._journal_prefix import JournalPrefix, PrefixState
from ._session_checkpoint import ValidationLedger, ValidationSnapshot
from ._session_checkpoint import install as install_parent
from ._session_input import RetainedInput
from ._session_input import install as install_input
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import IncompleteSourceError, JournalValidationCapacity, ProvenanceValidationError
from .framing import JournalFramingCursor, advance_journal_framing
from .graph_certificates import InvalidJournalGraphCertificate, JournalGraphCertificates
from .journal import JournalFrame, JournalSummary
from .journal_store import JournalStore
from .replay_binding import JournalReplayBinding


class JournalValidationSource(Protocol):
    """Owner-authenticated retained bytes, independent of catalog or provider type.

    Every returned range belongs to the exact immutable binding. The owner keeps
    source custody and workspace retention across workers. Verification checks
    the current source/job fence and must not scan source contents. This port
    conveys no authority to accept a client-supplied checkpoint or database.
    A scan window may be short but must make progress before the exact source
    ends. Exact reads supply one already framed atom from the retained source.
    """

    @property
    def binding(self) -> JournalReplayBinding: ...

    def read_window(self, offset: int, maximum_octets: int) -> bytes: ...

    def read_exact(self, offset: int, octets: int) -> bytes: ...

    def verify_current(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class JournalValidationBudget:
    rows: int = 64
    octets: int = 4 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 65536
    source_octets: int = 65536

    def __post_init__(self) -> None:
        ReplayBudget(self.rows, self.octets)
        if type(self.source_octets) is not int or not 1 <= self.source_octets <= self.octets:
            raise ValueError("journal source budget must be positive and fit the work byte budget")


@dataclass(frozen=True, slots=True)
class JournalValidationProgress:
    attempt_id: str
    worker_epoch: int
    phase: str
    version: int
    checkpoint_sha256: str
    accepted_frames: int
    accepted_octets: int
    accepted_sha256: str
    source_octets: int
    complete: bool


def _progress_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    compression = growth_binding(sha256_compress).declaration

    def padding(metric: str) -> Expression:
        return Expression(
            "delegate",
            name="padding",
            metric=metric,
            terms=(dimension("padding_blocks"),),
            arguments=(("block_octets", Expression("constant", value=64, unit=OCTETS)),),
        )

    return Declaration(
        id="riverhog-provenance.native-session-checkpoint-progress.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal_session._checkpoint_progress",),
        scope="one-exact-native-session-checkpoint-progress-value",
        starts_at="receive one loaded parent/prefix and the exact source cursor offset",
        ends_at="return its length-delimited checkpoint digest and accepted-prefix progress",
        dimensions=(
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Actual binding, epoch, parent, prefix, attempt and cursor scalar envelope.",
            ),
            Dimension(
                "padding_blocks",
                COUNT,
                "One or two padding blocks from the accepted continuation's input tail.",
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal",
                    count(0),
                    "Progress encodes the supplied checkpoint without native storage access.",
                )
                for metric in ("sql_statements", "returned_rows", "changed_rows")
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    Expression("product", terms=(count(16), dimension("metadata_row_octets"))),
                    "Only a fixed number of exact checkpoint/control encodings and hash segments.",
                )
                for metric in ("native_hashed_octets", "decoded_json_octets", "encoded_json_octets")
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "The accepted continuation and exact cursor replace source rereads.",
            ),
            *(
                Bound(
                    "padding_" + bound.metric,
                    bound.unit,
                    "upper",
                    padding(bound.metric),
                    (
                        "Actual continuation finalization composes its maintained "
                        "native compression kernel."
                    ),
                )
                for bound in compression.bounds
            ),
        ),
        conditions=(
            "The owner already loaded and fenced this exact private checkpoint.",
            "Metadata widths and padding multiplicity are causal input facts, not measured work.",
        ),
        invariants=(
            "Reporting progress changes no accepted prefix, source cursor or durable state.",
            "The checkpoint digest retains exact length-delimited ownership and prefix inputs.",
        ),
        limitations=(
            (
                "This pure encoding supplies neither source custody nor "
                "successful terminal validation."
            ),
            "Physical allocation and machine timing remain separate observations.",
        ),
    )


@declare_growth(_progress_declaration(), delegates=(("padding", sha256_compress),))
def _checkpoint_progress(
    attempt_id: str,
    ledger: ValidationLedger,
    snapshot: ValidationSnapshot,
    source_octets: int,
) -> JournalValidationProgress:
    state = snapshot.checkpoint
    accepted = PrefixState.from_json(snapshot.prefix[2])
    checkpoint_digest = hashlib.sha256()
    for value in (
        ledger.binding,
        str(ledger.worker_epoch).encode(),
        state.to_json().encode(),
        *(part.encode() if isinstance(part, str) else part for part in snapshot.prefix),
    ):
        checkpoint_digest.update(str(len(value)).encode() + b":" + value)
    return JournalValidationProgress(
        attempt_id,
        ledger.worker_epoch,
        "source" if state.phase == "frame" else state.phase,
        state.version,
        checkpoint_digest.hexdigest(),
        state.frame_ordinal,
        accepted.byte_count,
        accepted.hexdigest,
        source_octets,
        state.phase == "done",
    )


_DEFAULT_BUDGET = JournalValidationBudget()


class _ChunkInput:
    """One caller chunk and one entry atom; no durable source custody."""

    def __init__(self, chunks: Iterable[bytes]) -> None:
        self.chunks = iter(chunks)
        self.chunk = b""
        self.offset = 0
        self.cursor = JournalFramingCursor()
        self.buffer = bytearray()
        self.pending = False
        self.eof = False
        self.complete = False

    def scan(self, octets: int) -> None:
        if self.pending or self.complete:
            raise RuntimeError("journal source already has a pending frame or terminal state")
        if self.offset == len(self.chunk) and not self.eof:
            try:
                chunk = next(self.chunks)
            except StopIteration:
                self.eof = True
            else:
                if type(chunk) is not bytes:
                    raise TypeError("journal chunks must be bytes")
                self.chunk, self.offset = chunk, 0
                # One source pull is a unit even when it yields no octets.
                if not chunk:
                    return
        piece = self.chunk[self.offset : self.offset + min(octets, 65536)]
        before = self.cursor
        result = advance_journal_framing(
            before,
            piece,
            byte_budget=octets,
            frame_budget=1,
            at_end=self.eof,
        )
        consumed = result.cursor.byte_offset - before.byte_offset
        if result.failure is not None:
            raise ProvenanceValidationError(result.failure)
        first = int(before.frame_start is None and consumed > 0)
        last = consumed - int(bool(result.positions))
        self.buffer.extend(piece[first:last])
        self.offset += consumed
        self.cursor = result.cursor
        self.pending = bool(result.positions)
        self.complete = result.complete

    def frame(self, octets: int) -> JournalFrame:
        if not self.pending:
            raise RuntimeError("journal source has no complete frame")
        if 2 * len(self.buffer) + 65536 > octets:
            raise EntryReplayCapacity("journal validation cannot admit its existing frame atom")
        frame = JournalFrame(bytes(self.buffer))
        try:
            _ = frame.document
        except (ValueError, TypeError) as exc:
            raise ProvenanceValidationError(str(exc)) from exc
        return frame

    def admitted(self) -> None:
        self.buffer.clear()
        self.pending = False

    def release(self) -> None:
        self.chunk = b""
        self.chunks = iter(())
        self.buffer.clear()


def _session_delegates(mode: str) -> tuple[tuple[str, Callable[..., Any]], ...]:
    if mode == "result":
        return (("borrow", JournalSummary.borrow),)
    common = (("load", ValidationLedger.load), ("progress", _checkpoint_progress))
    if mode == "progress":
        return common
    return (
        *common,
        ("entry_advance", EntryReplay.advance),
        ("entry_admission", EntryReplay.admit),
        ("fence", ValidationLedger._fence),
        ("publication", ValidationLedger._publish),
        ("framing", advance_journal_framing),
        ("padding", sha256_compress),
    )


def _session_declaration(mode: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def multiple(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def add(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    declarations = {
        name: growth_binding(operation).declaration for name, operation in _session_delegates(mode)
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

    delegate_dimensions = tuple(
        item
        for name, declaration in declarations.items()
        for item in (
            Dimension(name + "_calls", COUNT, "Actual causal call count for " + name + "."),
            *(
                Dimension(name + "_" + value.name, value.unit, value.meaning)
                for value in declaration.dimensions
            ),
        )
    )
    if mode == "result":
        bounds = tuple(
            Bound(
                bound.metric,
                bound.unit,
                bound.relation,
                child("borrow", bound.metric),
                "The exact successful summary supplies one actual independent reader borrow.",
            )
            for bound in declarations["borrow"].bounds
        )
        dimensions = delegate_dimensions
    else:
        advance = mode == "advance"
        width = dimension("metadata_row_octets")
        control = Expression("product", terms=(count(128 if advance else 16), width))
        publication = dimension("input_publications") if advance else count(0)
        atom = (
            dimension("input_atom_octets")
            if advance
            else Expression("constant", value=0, unit=OCTETS)
        )
        own_sql = 8 if advance else 0
        dimensions = (
            Dimension("retained_rows", COUNT, "All native workspace rows before this call."),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Actual parent, prefix, input cursor, source manifest, worker and result envelope.",
            ),
            Dimension(
                "source_verifications",
                COUNT,
                "Actual trusted source-currentness port calls, zero for disposable input.",
            ),
            *(
                (
                    Dimension(
                        "input_publications", COUNT, "Zero or one native source-cursor mutation."
                    ),
                    Dimension(
                        "input_atom_octets",
                        OCTETS,
                        "Pending frame JSON octets parsed by the input owner; otherwise zero.",
                    ),
                    Dimension(
                        "source_windows",
                        COUNT,
                        "Actual retained source-window port calls, zero or one.",
                    ),
                    Dimension(
                        "source_atoms",
                        COUNT,
                        "Actual exact current-frame source port calls, zero or one.",
                    ),
                    Dimension(
                        "caller_pulls",
                        COUNT,
                        "Actual caller iterator pulls including empty/eof, zero or one.",
                    ),
                    Dimension(
                        "source_window_octets",
                        OCTETS,
                        "Exact source window returned by its trusted owner.",
                    ),
                    Dimension(
                        "source_atom_octets",
                        OCTETS,
                        "Exact current native frame returned by its trusted owner.",
                    ),
                    Dimension(
                        "caller_pull_octets",
                        OCTETS,
                        "Actual caller bytes chunk supplied, independent of scan quantum.",
                    ),
                    Dimension(
                        "caller_slice_octets",
                        OCTETS,
                        "Selected local caller slice before framing; zero for retained input.",
                    ),
                )
                if advance
                else ()
            ),
            *delegate_dimensions,
        )
        bounds = (
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                add(count(own_sql), subtotal("sql_statements", COUNT)),
                "Actual delegates and fixed cursor/header/quarantine/transaction/freeze work.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                add(count(3 if advance else 0), subtotal("returned_rows", COUNT)),
                (
                    "Only exact fixed-shape prefix/predecessor/quarantine "
                    "metadata lies outside delegates."
                ),
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                add(publication, subtotal("changed_rows", COUNT)),
                (
                    "The native input cursor is the only extra durable row beyond"
                    " actual child publications."
                ),
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    add(
                        Expression("product", terms=(count(8 if advance else 0), width)),
                        subtotal(metric, OCTETS),
                    ),
                    (
                        "Only fixed-shape cursor, source and checkpoint metadata is "
                        "outside actual delegates."
                    ),
                )
                for metric in (
                    "returned_metadata_octets",
                    "bound_parameter_octets",
                    "written_value_octets",
                )
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    add(control, multiple(4, atom), subtotal(metric, OCTETS)),
                    (
                        "Inclusive fixed metadata codecs/hashes and actual pending-"
                        "frame canonical parsing."
                    ),
                )
                for metric in ("native_hashed_octets", "decoded_json_octets", "encoded_json_octets")
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    count(2048 if advance else 0),
                    subtotal("sqlite_vm_instructions", COUNT),
                    Expression(
                        "product",
                        terms=(
                            count(1024 if advance else 0),
                            Expression(
                                "log2_ceil", terms=(add(count(2), dimension("retained_rows")),)
                            ),
                        ),
                    ),
                ),
                "Actual delegate VM envelopes plus only fixed indexed bare control reads/writes.",
            ),
            *(
                Bound(
                    metric,
                    unit,
                    "upper",
                    subtotal(metric, unit),
                    "Native entry work remains visible through its unchanged actual conductor.",
                )
                for metric, unit in (
                    ("returned_atom_octets", OCTETS),
                    ("source_payload_octets", OCTETS),
                    ("source_hashed_octets", OCTETS),
                    ("native_schema_calls", COUNT),
                    ("indexed_containers", COUNT),
                    ("index_python_opcode_events", COUNT),
                    ("adjacency_rows", COUNT),
                )
            ),
            Bound(
                "source_verifications",
                COUNT,
                "equal",
                dimension("source_verifications"),
                "Every actual trusted source-currentness check is retained.",
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No accepted native frame or unrelated assertion body is reloaded.",
            ),
            *(
                (
                    Bound(
                        tag,
                        COUNT,
                        "equal",
                        dimension(tag),
                        "Actual trusted source/caller port calls.",
                    )
                    for tag in ("source_windows", "source_atoms", "caller_pulls")
                )
                if advance
                else ()
            ),
            *(
                (
                    Bound(
                        "caller_copied_octets",
                        OCTETS,
                        "upper",
                        add(
                            dimension("caller_slice_octets"),
                            multiple(2, child("framing", "consumed_octets")),
                            dimension("input_atom_octets"),
                        ),
                        "Actual local slice, payload append and pending buffer copy ranges.",
                    ),
                    Bound(
                        "source_port_octets",
                        OCTETS,
                        "equal",
                        add(
                            dimension("source_window_octets"),
                            dimension("source_atom_octets"),
                            dimension("caller_pull_octets"),
                        ),
                        (
                            "Actual returned source ranges/chunk sizes, not a claimed "
                            "caller chunk cap."
                        ),
                    ),
                    *(
                        Bound(
                            "framing_" + bound.metric,
                            bound.unit,
                            "upper",
                            child("framing", bound.metric),
                            "Actual consumed source framing remains a nested maintained kernel.",
                        )
                        for bound in declarations["framing"].bounds
                    ),
                    *(
                        Bound(
                            "padding_" + bound.metric,
                            bound.unit,
                            "upper",
                            child("padding", bound.metric),
                            (
                                "Only terminal accepted-source finalizations outside the "
                                "progress child."
                            ),
                        )
                        for bound in declarations["padding"].bounds
                    ),
                )
                if advance
                else ()
            ),
        )
    return Declaration(
        id="riverhog-provenance.native-journal-session-" + mode + ".growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal_session.JournalValidationSession." + mode,),
        scope="whole-native-journal-session-" + mode,
        starts_at="receive the exact open native session and its current owned inputs",
        ends_at=(
            (
                "return checked native progress or reject retained input capacity "
                "before atom acquisition, or preserve effective recorder/context rejection"
            )
            if mode == "advance"
            else "return one checked native progress value or independent successful result reader"
        ),
        dimensions=dimensions,
        bounds=bounds,
        conditions=(
            (
                "Actual pre-call phase, input cursor, summary/cache state and"
                " budgets determine every child input/call count."
            ),
            "Every child retains its own native row domain and primitive bounds.",
            (
                "Workspace counters cover this native SQLite owner; "
                "provider/caller internals remain with that owner."
            ),
            (
                "Source port bodies execute normally; their calls and "
                "returned byte ranges are observed separately."
            ),
            (
                "The metadata envelope excludes documentary payloads; "
                "pending-frame parsing uses its own admitted atom bytes."
            ),
        ),
        invariants=(
            (
                "All native source/catalog/worker/anchor/currentness checks "
                "execute on the exact owning inputs."
            ),
            "Input cursor and semantic work publish only with their exact parent checkpoint.",
            (
                "Terminal success requires a complete finite source, matching"
                " accepted prefix and no quarantined frame."
            ),
            (
                "A successful result borrows an independently closeable "
                "reader without replaying or materializing the source."
            ),
        ),
        limitations=(
            (
                "Trusted source/caller implementation cost, acquisition and "
                "retention remain with that provider."
            ),
            (
                (
                    "Retained fixed-control and pending-frame capacity rejection before "
                    "source acquisition or checkpoint transition is included. "
                    "Effective recorder/context rejection is included before parent "
                    "publication; other semantic, checkpoint, stale-source and "
                    "interruption failures retain separate evidence."
                )
                if mode == "advance"
                else (
                    "Open/replacement/close lifecycle, failure, stale state, "
                    "capacity and interruption retain separate evidence."
                )
            ),
            (
                "Logical bytes/SQL/kernel counts do not guarantee physical "
                "allocation, timing, lock/fsync latency or power-loss "
                "behavior."
            ),
        ),
    )


def _lifecycle_delegates(mode: str) -> tuple[tuple[str, Callable[..., Any]], ...]:
    if mode == "close":
        return ()
    if mode == "__init__":
        return (("initialize", ValidationLedger._initialize),)
    return (
        ("initialize", ValidationLedger._initialize),
        ("replacement", ValidationLedger.replace_worker),
        ("load", ValidationLedger.load),
    )


def _lifecycle_declaration(mode: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value, unit=()), term))

    delegates = _lifecycle_delegates(mode)
    child_dimensions = growth_binding(ValidationLedger.load).declaration.dimensions

    def checkpoint(metric: str, unit: tuple[tuple[str, int], ...]) -> Expression:
        terms = tuple(
            Expression(
                "delegate",
                name=name,
                metric=metric,
                terms=(dimension(name + "_calls"),),
                arguments=tuple(
                    (item.name, dimension(name + "_" + item.name)) for item in child_dimensions
                ),
            )
            for name, _operation in delegates
        )
        return add(*terms) if terms else Expression("constant", value=0, unit=unit)

    closing = mode == "close"
    retained_open = mode == "open_retained"
    metadata = dimension("metadata_row_octets")
    catalog = dimension("constructed_catalog_octets")
    dimensions = (
        Dimension("retained_rows", COUNT, "Native indexed rows before opening, zero for creation."),
        Dimension(
            "metadata_row_octets",
            OCTETS,
            "Envelope of exact input, old/new checkpoint, graph and source controls.",
        ),
        Dimension(
            "constructed_catalog_octets",
            OCTETS,
            "Packaged core schema bytes when the default catalog is constructed, otherwise zero.",
        ),
        Dimension(
            "default_catalog", COUNT, "One only when constructing an unsupplied default catalog."
        ),
        Dimension(
            "constructed_catalog_entries",
            COUNT,
            "Packaged schema-directory entries for default construction; otherwise zero.",
        ),
        Dimension(
            "source_checks",
            COUNT,
            "One source-currentness port call for retained admission; otherwise zero.",
        ),
        Dimension(
            "release_callbacks",
            COUNT,
            "Actual transferred owner-release callbacks invoked by this close.",
        ),
        *(
            (
                Dimension(
                    "connection_admissions",
                    COUNT,
                    (
                        "Zero or one actual native "
                        "connection admission, "
                        "distinguished from a failed open"
                        " attempt."
                    ),
                ),
                Dimension(
                    "rejected_connections",
                    COUNT,
                    (
                        "Admitted connections rejected "
                        "during this call; successful "
                        "ownership transfer has zero."
                    ),
                ),
            )
            if retained_open
            else ()
        ),
        *(
            Dimension(name + "_calls", COUNT, "Actual " + name + " checkpoint calls.")
            for name, _operation in delegates
        ),
        *(
            Dimension(name + "_" + item.name, item.unit, item.meaning)
            for name, _operation in delegates
            for item in child_dimensions
        ),
    )
    zero_octets = Expression("constant", value=0, unit=OCTETS)
    sql = count(0) if closing else add(count(128), checkpoint("sql_statements", COUNT))
    controls = zero_octets if closing else add(scale(64, metadata), scale(24, catalog))
    bounds = [
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            sql,
            (
                "Fixed native format/schema and indexed control work plus every actual "
                "checkpoint delegate."
            ),
        ),
        Bound(
            "returned_rows",
            COUNT,
            "upper",
            count(0) if closing else add(count(32), checkpoint("returned_rows", COUNT)),
            "Fixed control probes; no source, assertion or historical frame payload is loaded.",
        ),
        Bound(
            "changed_rows",
            COUNT,
            "upper",
            count(0) if closing else add(count(8), checkpoint("changed_rows", COUNT)),
            "Creation writes fixed schema controls; replacement changes one fenced parent.",
        ),
        Bound(
            "returned_metadata_octets",
            OCTETS,
            "upper",
            controls,
            "Every actual control row retains its byte width.",
        ),
        Bound(
            "bound_parameter_octets",
            OCTETS,
            "upper",
            controls,
            "Exact native keys, mode/source bindings and old/new worker controls.",
        ),
        Bound(
            "written_value_octets",
            OCTETS,
            "upper",
            controls,
            "Fixed initialization or replacement metadata, never retained source payload.",
        ),
        Bound(
            "decoded_json_octets",
            OCTETS,
            "upper",
            controls,
            "Control codecs and an optional packaged default catalog execute normally.",
        ),
        Bound(
            "encoded_json_octets",
            OCTETS,
            "upper",
            controls,
            "Actual canonical control and optional default-catalog construction bytes.",
        ),
        Bound(
            "native_hashed_octets",
            OCTETS,
            "upper",
            controls,
            "Actual native hashes retain metadata and optional packaged catalog bytes.",
        ),
        Bound(
            "source_payload_octets",
            OCTETS,
            "equal",
            zero_octets,
            "Opening, replacement and release do not load a native frame or replay source content.",
        ),
        Bound(
            "adjacency_rows",
            COUNT,
            "equal",
            count(0),
            "Native graph reopening reads fixed run controls, never adjacency.",
        ),
        Bound(
            "opened_databases",
            COUNT,
            "equal",
            dimension("connection_admissions") if retained_open else count(0 if closing else 1),
            (
                (
                    "Actual connection admission remains distinct from a rejected "
                    "attempt; successful construction admits exactly one and release "
                    "opens none."
                )
                if retained_open
                else (
                    "One actual native connection for successful construction; release opens none."
                )
            ),
        ),
        Bound(
            "closed_databases",
            COUNT,
            "equal" if retained_open else "upper",
            dimension("rejected_connections") if retained_open else count(int(closing)),
            (
                (
                    "Rejected admitted connections close exactly once; successful "
                    "retained transfer closes none, and final reader release preserves"
                    " independent readers."
                )
                if retained_open
                else (
                    "The last reader closes the one native connection; another reader preserves it."
                )
            ),
        ),
        Bound(
            "source_verifications",
            COUNT,
            "equal",
            dimension("source_checks"),
            "All actual source-currentness port invocations are observed.",
        ),
        Bound(
            "release_callbacks",
            COUNT,
            "equal",
            dimension("release_callbacks"),
            "The transferred owner callback runs only with final storage release.",
        ),
        Bound(
            "retention_callbacks",
            COUNT,
            "upper",
            count(int(retained_open)),
            "One optional early-retention callback precedes retained initialization.",
        ),
        Bound(
            "opened_files",
            COUNT,
            "upper",
            count(1 if closing else 3),
            (
                "Exclusive retained-file creation and fixed directory synchronization, or"
                " private disposal."
            ),
        ),
        Bound(
            "closed_files",
            COUNT,
            "upper",
            count(1 if closing else 3),
            "Each explicit descriptor is closed by its owning native operation.",
        ),
        Bound(
            "file_syncs",
            COUNT,
            "upper",
            count(0 if closing else 2),
            "The retained create path synchronizes its directory before and after initialization.",
        ),
        Bound(
            "directory_scans",
            COUNT,
            "upper",
            count(1) if closing else dimension("default_catalog"),
            (
                "Default construction may enumerate packaged schemas; disposable final "
                "release visits its one owned directory."
            ),
        ),
        Bound(
            "directory_entries",
            COUNT,
            "upper",
            count(4) if closing else dimension("constructed_catalog_entries"),
            "Only packaged schema entries or the disposable owner’s fixed SQLite files "
            "are visited.",
        ),
        Bound(
            "unlinks",
            COUNT,
            "upper",
            count(4 if closing else 1),
            "Disposal unlinks fixed owner files; cold temporary-root selection may "
            "remove its probe file.",
        ),
        Bound(
            "removed_directories",
            COUNT,
            "upper",
            count(int(closing)),
            "Only final disposable release removes its one private directory.",
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            count(0)
            if closing
            else add(
                count(16000),
                Expression(
                    "product",
                    terms=(
                        count(512),
                        Expression("log2_ceil", terms=(add(count(2), dimension("retained_rows")),)),
                    ),
                ),
                checkpoint("sqlite_vm_instructions", COUNT),
            ),
            (
                "Fixed schema/format work and indexed metadata probes, including actual "
                "checkpoint delegates."
            ),
        ),
    ]
    for metric, unit in (
        ("sql_statements", COUNT),
        ("returned_rows", COUNT),
        ("changed_rows", COUNT),
        ("returned_metadata_octets", OCTETS),
        ("bound_parameter_octets", OCTETS),
        ("written_value_octets", OCTETS),
        ("sqlite_vm_instructions", COUNT),
    ):
        if delegates:
            bounds.append(
                Bound(
                    "checkpoint_" + metric,
                    unit,
                    "upper",
                    checkpoint(metric, unit),
                    "Compose every actual checkpoint call with its own declared dimensions.",
                )
            )
    if retained_open:
        bounds.extend(
            (
                Bound(
                    "database_open_attempts",
                    COUNT,
                    "upper",
                    count(1),
                    (
                        "The exact storage "
                        "admission attempts "
                        "at most one "
                        "connection, "
                        "including a raising "
                        "attempt."
                    ),
                ),
                Bound(
                    "file_open_attempts",
                    COUNT,
                    "upper",
                    count(3),
                    (
                        "Exclusive database "
                        "creation and fixed "
                        "directory "
                        "synchronization include "
                        "raising attempts."
                    ),
                ),
                Bound(
                    "sql_attempts",
                    COUNT,
                    "upper",
                    sql,
                    (
                        "Native execute/executescript calls "
                        "include a raising storage-admission"
                        " statement as well as successful "
                        "checkpoint work."
                    ),
                ),
                Bound(
                    "opened_database_limit",
                    COUNT,
                    "upper",
                    count(1),
                    ("Independently bound the observed connection-admission primitive."),
                ),
                Bound(
                    "closed_database_limit",
                    COUNT,
                    "upper",
                    dimension("connection_admissions"),
                    ("Only an actually admitted connection can be closed on rejection."),
                ),
                Bound(
                    "source_check_limit",
                    COUNT,
                    "upper",
                    count(1),
                    ("Opening invokes its exact source fence at most once."),
                ),
                Bound(
                    "transition_call_limit",
                    COUNT,
                    "upper",
                    count(1),
                    ("Initialization and replacement are mutually exclusive branches."),
                ),
                Bound(
                    "lifecycle_call_limit",
                    COUNT,
                    "upper",
                    count(2),
                    ("At most one initialization/replacement and one entry-owner load occur."),
                ),
            )
        )
    return Declaration(
        id="riverhog-provenance.native-session-lifecycle-" + mode.strip("_") + ".growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal_session.JournalValidationSession." + mode,),
        scope="native journal session lifecycle",
        starts_at="construct, replace or close one exact owner-authenticated native session",
        ends_at=(
            (
                "return the native owner or reject input/storage admission before "
                "checkpoint transition work"
            )
            if retained_open
            else ("return the native owner, or release its one session handle")
        ),
        dimensions=dimensions,
        bounds=tuple(bounds),
        conditions=(
            "The host-selected temporary root is available for cold disposable construction.",
            (
                "The trusted owner supplies the complete private workspace and exact "
                "immutable source/catalog/certificate pins."
            ),
            (
                "The current native SQLite schema and primary keys are used; creation "
                "owns a fresh empty directory and disposal owns its fixed files."
            ),
            (
                "A supplied sealed catalog exposes its cached checkpoint identity; "
                "optional default construction reads only the packaged core schemas."
            ),
            (
                "Source-currentness, early-retention and release callback bodies execute "
                "normally, with their provider-owned internal work composed by that "
                "owner."
            ),
        ),
        invariants=(
            (
                "A replacement requires a newer fenced worker epoch and preserves "
                "accepted semantic and input progress."
            ),
            (
                "Opening never loads source payload or replays accepted history; missing "
                "retained state is never silently recreated."
            ),
            (
                "Closing is idempotent, preserves retained files and does not invalidate "
                "an independently borrowed result reader."
            ),
        ),
        limitations=(
            (
                (
                    "Failed checkpoint transitions and process interruption retain "
                    "separate required evidence; retained input/storage admission "
                    "rejection is included."
                )
                if retained_open
                else (
                    "Failure, missing/corrupt state, capacity, ownership and process "
                    "interruption retain their separate required evidence."
                )
            ),
            (
                "Logical SQL, metadata and explicit syscall counts do not bound physical "
                "page I/O, SQLite comparison bytes, latency or power-loss behavior."
            ),
            (
                "Trusted source/callback implementations and caller iterable finalizers "
                "remain with their owning providers."
            ),
        ),
    )


class JournalValidationSession:
    """Validate a caller stream through bounded, ordered native work units.

    The caller retains ownership of its iterable. This disposable stream mode
    does not promise recovery after process death. A successful result owns an
    independent reader handle and remains usable after this session is closed.
    """

    def __init__(
        self,
        chunks: Iterable[bytes],
        *,
        attempt_id: str,
        catalog: ContractCatalog | None = None,
        require_profiles: bool = True,
        expected_anchor: Mapping[str, Any] | None = None,
        require_exact_tail: bool = False,
    ) -> None:
        self.attempt_id = attempt_id
        self._closed = False
        self._closing = False
        self._summary: JournalSummary | None = None
        self._failure: BaseException | None = None
        self._catalog = catalog or ContractCatalog()
        self._store = JournalStore()
        self._source: JournalValidationSource | None = None
        self._source_binding: JournalReplayBinding | None = None
        self._input: _ChunkInput | RetainedInput
        try:
            self._prefix = JournalPrefix(
                self._store.db,
                attempt_id=attempt_id,
                source_binding="disposable-stream:" + attempt_id,
                catalog=self._catalog,
                require_profiles=require_profiles,
                expected_anchor=expected_anchor,
                require_exact_tail=require_exact_tail,
            )
            self._ledger = ValidationLedger(
                self._store.db,
                attempt_id=attempt_id,
                binding=self._prefix.binding,
                worker_epoch=0,
            )
            self._initialize(retained=False)
            self._replay = EntryReplay(
                self._store, prefix=self._prefix, ledger=self._ledger, catalog=self._catalog
            )
            self._input = _ChunkInput(chunks)
        except BaseException:
            self._store.close()
            raise

    def _initialize(self, *, retained: bool) -> None:
        db = self._store.db
        try:
            db.execute("BEGIN IMMEDIATE").close()
            install_parent(db)
            install_replay(db)
            if retained:
                install_input(self._ledger)
            self._prefix._initialize()
            self._ledger._initialize()
            db.commit()
        except BaseException:
            db.rollback()
            raise

    @classmethod
    @declare_growth(
        _lifecycle_declaration("open_retained"), delegates=_lifecycle_delegates("open_retained")
    )
    def open_retained(
        cls,
        directory: Path,
        source: JournalValidationSource,
        *,
        attempt_id: str,
        worker_epoch: int,
        create: bool = False,
        catalog: ContractCatalog | None = None,
        expected_anchor: Mapping[str, Any] | None = None,
        require_exact_tail: bool = False,
        certificates: JournalGraphCertificates | None = None,
        on_storage_close: Callable[[], None] | None = None,
        on_storage_open: Callable[[Self], None] | None = None,
    ) -> Self:
        """Open this owner's workspace; a replacement requires a newer worker epoch.

        The owner supplies an existing private directory and current exact source
        authority. Creation never replaces a file; reopening never recreates a
        missing database. Closing preserves retained files for their owner.

        On success, optional on_storage_close ownership passes to the shared
        database. The callback runs after its final result reader closes,
        so an owner may retain its kernel lease beyond the Session's lifetime.
        Failed callbacks remain retryable and must retain unconfirmed release work.
        A failed constructor leaves callback ownership with the caller.
        The optional open callback receives the closeable Session before native
        schema or prefix initialization. It must retain the handle first; a
        cleanup failure then remains reachable for a later explicit close.
        """
        selected = catalog or ContractCatalog()
        binding = source.binding
        certificate_binding = None if certificates is None else certificates.binding
        if certificate_binding is not None and certificate_binding.source != binding:
            raise InvalidJournalGraphCertificate(
                "graph certificate belongs to another finite source"
            )
        prefix_source_binding = binding.to_json()
        if certificate_binding is not None:
            prefix_source_binding = canonical_json_bytes(
                {
                    "source": prefix_source_binding,
                    "graph_certificate": certificate_binding.to_json(),
                }
            ).decode()
        if binding.catalog_sha256 != selected.checkpoint_sha256():
            raise ValueError("retained validation source has a different contract catalog")
        if type(worker_epoch) is not int or worker_epoch < 0:
            raise ValueError("retained validation requires an explicit nonnegative worker epoch")
        if source.verify_current() is not True:
            raise IncompleteSourceError("retained validation source is no longer current")
        result = object.__new__(cls)
        result.attempt_id = attempt_id
        result._closed = False
        result._closing = False
        result._summary = None
        result._failure = None
        result._catalog = selected
        result._source, result._source_binding = source, binding

        def retain_storage(store: JournalStore) -> None:
            result._store = store
            if on_storage_open is not None:
                on_storage_open(result)

        store_opened = False
        try:
            result._store = JournalStore.open_retained(
                directory, create=create, on_storage_open=retain_storage
            )
            store_opened = True
            result._prefix = JournalPrefix(
                result._store.db,
                attempt_id=attempt_id,
                source_binding=prefix_source_binding,
                catalog=selected,
                require_profiles=binding.require_profiles,
                expected_anchor=expected_anchor,
                require_exact_tail=require_exact_tail,
            )
            with closing(
                result._store.db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='journal_validation'"
                )
            ) as rows:
                initialized = rows.fetchone() is not None
            if initialized:
                with closing(
                    result._store.db.execute(
                        "SELECT substr(worker_epoch,1,8193) FROM journal_validation "
                        "WHERE singleton=1"
                    )
                ) as rows:
                    old = rows.fetchone()
                if (
                    old is None
                    or type(old[0]) is not str
                    or not old[0].isascii()
                    or not old[0].isdecimal()
                ):
                    raise RuntimeError("retained validation worker epoch is missing or corrupt")
                old_epoch = int(old[0])
                if str(old_epoch) != old[0] or worker_epoch <= old_epoch:
                    raise ValueError(
                        "retained validation replacement requires a newer worker epoch"
                    )
                previous = ValidationLedger(
                    result._store.db,
                    attempt_id=attempt_id,
                    binding=result._prefix.binding,
                    worker_epoch=old_epoch,
                )
                result._ledger = previous.replace_worker(worker_epoch)
            else:
                # An interrupted atomic initialization has no accepted prefix.
                result._ledger = ValidationLedger(
                    result._store.db,
                    attempt_id=attempt_id,
                    binding=result._prefix.binding,
                    worker_epoch=worker_epoch,
                )
                result._initialize(retained=True)
            result._replay = EntryReplay(
                result._store,
                prefix=result._prefix,
                ledger=result._ledger,
                catalog=selected,
                certificates=certificates,
            )
            result._input = RetainedInput(result._ledger, source)
        except BaseException:
            result._closing = True
            # A failing Store constructor already attempted its own cleanup.
            # Its early retained handle preserves a failed close for retry.
            if store_opened:
                result.close()
            raise
        result._store._storage.on_close = on_storage_close
        return result

    def _require_open(self) -> None:
        if self._closing or self._closed:
            raise RuntimeError("journal validation session is closed")

    def _progress(self, snapshot: ValidationSnapshot) -> JournalValidationProgress:
        return _checkpoint_progress(
            self.attempt_id, self._ledger, snapshot, self._input.cursor.byte_offset
        )

    def _move(self, before: ValidationSnapshot, phase: str) -> ValidationSnapshot:
        after = replace(before.checkpoint, version=before.checkpoint.version + 1, phase=phase)
        db = self._store.db
        try:
            db.execute("BEGIN IMMEDIATE").close()
            self._ledger._fence(before)
            result = self._ledger._publish(before, after)
            db.commit()
        except BaseException:
            db.rollback()
            raise
        return result

    def _finish(self, before: ValidationSnapshot) -> ValidationSnapshot:
        state = self._prefix.finish()
        if (
            not self._input.complete
            or self._input.pending
            or state.byte_count != self._input.cursor.byte_offset
        ):
            raise RuntimeError("journal terminal source and accepted prefix differ")
        with closing(
            self._store.db.execute("SELECT 1 FROM journal_validation_frame WHERE singleton=1")
        ) as rows:
            if rows.fetchone() is not None:
                raise RuntimeError("journal validation has an unfinished frame atom")
        if self._source is not None:
            binding = self._source_binding
            if binding is None or self._source.binding != binding:
                raise RuntimeError("journal validation lost its exact source binding")
            if self._source.verify_current() is not True:
                raise IncompleteSourceError("retained validation source is no longer current")
            if (
                state.journal_id != binding.journal_id
                or state.byte_count != int(binding.source_bytes)
                or state.hexdigest != binding.source_sha256
                or state.frame_count != int(binding.frame_count)
            ):
                raise IncompleteSourceError(
                    "validated journal differs from its exact retained binding"
                )
        after = self._move(before, "done") if before.checkpoint.phase != "done" else before
        self._store.freeze()
        summary = JournalSummary(
            cast(str, state.journal_id),
            self._store.frames,
            self._store.graph_validation,
            self._store.retired,
            state.hexdigest,
            state.byte_count,
            self._store.origins,
        )
        object.__setattr__(summary, "_store", self._store)
        self._summary = summary
        return after

    @declare_growth(_session_declaration("advance"), delegates=_session_delegates("advance"))
    def advance(
        self, budget: JournalValidationBudget = _DEFAULT_BUDGET
    ) -> JournalValidationProgress:
        self._require_open()
        if self._failure is not None:
            raise RuntimeError("journal validation session failed") from self._failure
        if not isinstance(budget, JournalValidationBudget):
            raise TypeError("journal validation requires its explicit work budget")
        try:
            if self._source is not None and (
                self._source.binding != self._source_binding
                or self._source.verify_current() is not True
            ):
                raise IncompleteSourceError("retained validation source is no longer current")
            before = self._ledger.load()
            # The entry conductor admits the same fixed owner/prefix controls.
            self._replay._start(before, ReplayBudget(budget.rows, budget.octets))
            phase = before.checkpoint.phase
            if phase == "terminal":
                return self._progress(self._finish(before))
            if phase == "done":
                if self._summary is None:
                    return self._progress(self._finish(before))
                return self._progress(before)
            if phase != "frame":
                return self._progress(
                    self._replay.advance(ReplayBudget(budget.rows, budget.octets))
                )
            if isinstance(self._input, RetainedInput):
                changed = self._input.synchronize(before, budget.octets)
                if changed is not None:
                    return self._progress(changed)
            if self._input.pending:
                frame = self._input.frame(budget.octets)
                after = self._replay.admit(frame, ReplayBudget(budget.rows, budget.octets))
                self._input.admitted()
                return self._progress(after)
            if self._input.complete:
                return self._progress(self._move(before, "terminal"))
            if isinstance(self._input, RetainedInput):
                after = self._input.scan(before, budget.octets, budget.source_octets)
            else:
                self._input.scan(budget.source_octets)
                after = before
            return self._progress(after)
        except JournalValidationCapacity:
            raise
        except BaseException as exc:
            self._failure = exc
            raise

    @declare_growth(_session_declaration("progress"), delegates=_session_delegates("progress"))
    def progress(self) -> JournalValidationProgress:
        """Read the current fixed checkpoint without advancing or loading source bytes."""
        self._require_open()
        if self._source is not None and (
            self._source.binding != self._source_binding
            or self._source.verify_current() is not True
        ):
            raise IncompleteSourceError("retained validation source is no longer current")
        return self._progress(self._ledger.load())

    @declare_growth(_session_declaration("result"), delegates=_session_delegates("result"))
    def result(self) -> JournalSummary:
        self._require_open()
        if self._summary is None:
            raise RuntimeError("journal validation has no successful terminal result")
        return self._summary.borrow()

    @declare_growth(_lifecycle_declaration("close"))
    def close(self) -> None:
        if not self._closed:
            self._closing = True
            self._store.close()
            source = getattr(self, "_input", None)
            if isinstance(source, _ChunkInput):
                source.release()
            replay = getattr(self, "_replay", None)
            if replay is not None:
                replay._current = None
            self._failure = None
            self._closed = True

    def __enter__(self) -> Self:
        self._require_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


declare_growth(_lifecycle_declaration("__init__"), delegates=_lifecycle_delegates("__init__"))(
    JournalValidationSession.__init__
)

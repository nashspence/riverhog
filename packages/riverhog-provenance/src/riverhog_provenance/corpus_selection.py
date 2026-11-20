"""Select effective keyed assertions without scanning a historical graph.

The source owner indexes each accepted assertion's half-open lifetime. A
historical point visits 64 dyadic nodes, independent of retired or unrelated
assertions. These are documentary facts, never current access or acceptance of
the corpus's separate semantic obligations.

The interval domain is a superset of every Native-accepted source, not a new
archive ceiling or a restriction on the public anchor's integer grammar.
CheckpointSHA256.update_bytes and from_state enforce SHA-256's existing maximum
message length, (2**64 - 1)//8 bytes. JournalPrefix.prepare hashes every encoded
frame and requires contiguous positions; JournalFrame.encoded includes RS and
LF. Accepted frame count is therefore less than 2**60. The 2**63 interval domain
strictly contains every possible accepted source position. Selection checks the
exact independently accepted header before using this derived representation.
"""

from __future__ import annotations

import heapq
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
    growth_binding,
)
from riverhog_provenance_contracts import canonical_document, require_canonical_uuid_urn

from .errors import JournalValidationCapacity

if TYPE_CHECKING:
    from .corpus import CanonicalCorpusValidator

_END = 1 << 63


@declare_growth(
    Declaration(
        id="riverhog-provenance.primary-source-extent.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_selection.source_extent",),
        scope="exact-indexed-source-progress-observation",
        starts_at="receive one admitted journal identity in the original source index",
        ends_at="return its committed frame and byte extent or absence",
        dimensions=(Dimension("indexed_sources", COUNT, "Independent source index extent."),),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=1, unit=COUNT),
                "One source/header primary-key join with a closed cursor.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=1, unit=COUNT),
                "At most one exact committed source extent.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No original frame or assertion payload is fetched.",
            ),
        ),
        conditions=("Caller retains the original corpus and independent source admission.",),
        invariants=("Progress observation grants no new source, access or closure authority.",),
        limitations=("Physical B-tree work and independent admission retain their owners.",),
    )
)
def source_extent(corpus: CanonicalCorpusValidator, journal_id: str) -> tuple[int, int] | None:
    """Read accepted preparation extent after an uncertain parent checkpoint."""
    corpus._require_open()
    require_canonical_uuid_urn(journal_id)
    row = corpus._source_read(
        "SELECT s.sequence,e.bytes FROM source_extents s JOIN entries e "
        "ON e.journal=s.journal AND e.sequence=s.sequence WHERE s.journal=?",
        (journal_id,),
    )
    return None if row is None else (int(row[0]) + 1, int(row[1]))


def _cover(first: int, stop: int) -> Iterator[str]:
    """Canonical disjoint dyadic cover of [first, stop), at most 126 nodes."""
    if not 0 <= first <= stop <= _END:
        raise ValueError("selection interval exceeds the accepted SHA-256 source domain")
    while first < stop:
        power = min(
            63 if first == 0 else (first & -first).bit_length() - 1,
            (stop - first).bit_length() - 1,
        )
        yield f"{power}:{first >> power:x}"
        first += 1 << power


def _path(sequence: int) -> tuple[str, ...]:
    if type(sequence) is not int or not 0 <= sequence < _END:
        raise ValueError("selection point exceeds the accepted SHA-256 source domain")
    return tuple(f"{power}:{sequence >> power:x}" for power in range(64))


def enable_selection(corpus: CanonicalCorpusValidator) -> None:
    """Select the optional derived view before the first accepted source atom."""
    corpus._require_open()
    if corpus._primary_selection_enabled:
        return
    if corpus._source_read("SELECT 1 FROM source_extents LIMIT 1", ()) is not None:
        raise ValueError("historical selectors must be enabled before source indexing")
    corpus._source_write(
        "CREATE TABLE primary_selection_payloads ("
        "journal TEXT, assertion TEXT, value BLOB NOT NULL, "
        "PRIMARY KEY(journal,assertion)) WITHOUT ROWID"
    )
    corpus._source_write(
        "CREATE TABLE primary_selection_keys ("
        "journal TEXT, assertion TEXT, kind TEXT NOT NULL, subject BLOB NOT NULL, "
        "predicate TEXT NOT NULL, born INTEGER NOT NULL, "
        "PRIMARY KEY(journal,assertion)) WITHOUT ROWID"
    )
    corpus._source_write(
        "CREATE TABLE primary_selection_intervals ("
        "journal TEXT, kind TEXT, subject BLOB, predicate TEXT, node TEXT, assertion TEXT, "
        "PRIMARY KEY(journal,kind,subject,predicate,node,assertion)) WITHOUT ROWID"
    )
    corpus._source_write(
        "CREATE INDEX primary_selection_retire ON primary_selection_intervals(journal,assertion)"
    )
    corpus._source_write(
        "CREATE TABLE primary_selection_events ("
        "journal TEXT, kind TEXT, subject BLOB, predicate TEXT, width INTEGER, sequence TEXT, "
        "PRIMARY KEY(journal,kind,subject,predicate,width,sequence)) WITHOUT ROWID"
    )
    corpus._primary_selection_enabled = True


def _index_declaration(retirement: bool) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog-provenance.primary-selection-"
        + ("retire" if retirement else "index")
        + ".growth/v1",
        authority="riverhog-provenance",
        subjects=(
            "riverhog_provenance.corpus_selection."
            + ("retire_assertion" if retirement else "index_assertion"),
        ),
        scope="native-keyed-historical-lifetime-index",
        starts_at="receive an accepted source assertion inside its atomic frame savepoint",
        ends_at="retain its exact generic selector and disjoint lifetime nodes",
        dimensions=(
            Dimension("indexed_assertions", COUNT, "All indexed selector assertion keys."),
            Dimension("key_octets", OCTETS, "Complete logical selector key and scalar octets."),
            Dimension(
                "projection_octets",
                OCTETS,
                "Exact newly encoded narrow fact projection bytes; zero for retirement.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(129 if retirement else 67),
                "One key lookup and indexed deletion plus at most 126 interval insertions; "
                "initial intervals need at most 64 nodes, one key, projection and change event.",
            ),
            Bound(
                "interval_insertions",
                COUNT,
                "upper",
                count(126 if retirement else 64),
                "The interval domain contains every possible accepted SHA-256 framed source.",
            ),
            Bound(
                "interval_deletions",
                COUNT,
                "upper",
                count(64 if retirement else 0),
                "An assertion is retired once; delete only its original open interval nodes.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "The containing source frame already supplies the assertion; no replay occurs.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=256),
                                Expression("dimension", name="key_octets"),
                            ),
                        ),
                        Expression("dimension", name="projection_octets"),
                    ),
                ),
                "All repeated complete keys and bounded node labels remain accounted.",
            ),
            Bound(
                "projection_written_octets",
                OCTETS,
                "equal",
                Expression("dimension", name="projection_octets"),
                "Each selected projection is encoded and written once with its original assertion.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        count(262144),
                        Expression(
                            "log2_ceil",
                            terms=(
                                Expression(
                                    "sum",
                                    terms=(
                                        count(2),
                                        Expression(
                                            "product",
                                            terms=(
                                                Expression("constant", value=126),
                                                Expression("dimension", name="indexed_assertions"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "SQLite instruction envelope includes the bounded mutation count and B-tree depth.",
            ),
        ),
        conditions=(
            "The containing Native frame savepoint commits assertions and retirements atomically.",
            "Native contiguous framing and SHA-256 continuation bound every indexed coordinate.",
        ),
        invariants=(
            "Corrections close the original lifetime before adding any replacement.",
            "Selector keys are extension subject/property and journal-subject artifact/role.",
            "No effective graph, application policy, source permit or closure result is copied.",
        ),
        limitations=(
            "Source payload loading, decoding and Native validation retain their existing owners.",
            "Index-key comparisons, filesystem I/O and elapsed time are separate observations.",
        ),
    )


@declare_growth(_index_declaration(False))
def index_assertion(
    corpus: CanonicalCorpusValidator, journal: str, value: Mapping[str, Any], sequence: int
) -> None:
    corpus._source_write(
        "INSERT INTO primary_selection_payloads VALUES (?,?,?)",
        (journal, value["assertion_id"], canonical_document(_projection(value))),
    )
    kind = value["type"]
    if kind == "extension":
        subject, predicate = value["subject"], value["property"]
    elif kind == "journal_subject":
        subject, predicate = value["artifact"], value["role"]
    else:
        return
    encoded = canonical_document(subject)
    corpus._source_write(
        "INSERT INTO primary_selection_keys VALUES (?,?,?,?,?,?)",
        (journal, value["assertion_id"], kind, encoded, predicate, sequence),
    )
    _selection_event(corpus, journal, kind, encoded, predicate, sequence)
    for node in _cover(sequence, _END):
        corpus._source_write(
            "INSERT INTO primary_selection_intervals VALUES (?,?,?,?,?,?)",
            (journal, kind, encoded, predicate, node, value["assertion_id"]),
        )


def _projection(value: Mapping[str, Any]) -> dict[str, Any]:
    """Exact consumed Native predicates, excluding opaque documentary fields."""
    projected = {name: value[name] for name in ("id", "type", "assertion_id")}
    kind = value["type"]
    fields = {
        "delivery_association": (
            "delivery_context_id",
            "slot",
            "role",
            "state",
            "verification_observation_id",
        ),
        "state": ("occurrence_id",),
        "occurrence": ("artifact_id",),
        "extension": ("subject", "property", "value"),
        "journal_subject": ("journal_id", "artifact", "role"),
    }
    projected.update((name, value[name]) for name in fields.get(kind, ()))
    if kind == "occurrence" and "materialization_hint" in value:
        projected["materialization_hint"] = value["materialization_hint"]
    if kind == "observation":
        projected["state"] = value["state"]
        projected["content"] = {
            "size_bytes": value["content"]["size_bytes"],
            "digests": [
                item for item in value["content"]["digests"] if item["algorithm"] == "sha-256"
            ],
        }
        projected["evidence"] = (
            [{"basis": "direct_measurement"}]
            if any(item["basis"] == "direct_measurement" for item in value["evidence"])
            else []
        )
    return projected


def _object_declaration() -> Declaration:
    def constant(value: int, unit: tuple[tuple[str, int], ...] = COUNT) -> Expression:
        return Expression("constant", value=value, unit=unit)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    return Declaration(
        id="riverhog-provenance.primary-selected-object.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_selection.selected_object",),
        scope="narrow-effective-historical-native-object-facts",
        starts_at="receive one exact admitted source anchor, object and byte budget",
        ends_at="return absence or one bounded projection after closed indexed reads",
        dimensions=(
            Dimension("retained_assertions", COUNT, "Complete original assertion index extent."),
            Dimension("retained_frames", COUNT, "Complete source header index extent."),
            Dimension("anchor_octets", OCTETS, "Complete selected anchor metadata bytes."),
            Dimension("selected_octets", OCTETS, "Exact admitted projection bytes or zero."),
        ),
        bounds=(
            Bound(
                "sql_statements", COUNT, "upper", constant(3), "Three indexed point/range reads."
            ),
            Bound("returned_rows", COUNT, "upper", constant(3), "At most one row per read."),
            Bound(
                "json_decodes", COUNT, "upper", constant(1), "Decode only the admitted projection."
            ),
            Bound(
                "payload_octets",
                OCTETS,
                "equal",
                dimension("selected_octets"),
                "Only the selected policy fields cross the database boundary.",
            ),
            Bound(
                "metadata_octets",
                OCTETS,
                "upper",
                Expression("sum", terms=(dimension("anchor_octets"), constant(512, OCTETS))),
                "Exact source header and bounded identity/type/position/length metadata.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        constant(768),
                        Expression(
                            "product",
                            terms=(
                                constant(128),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                constant(2),
                                                dimension("retained_assertions"),
                                                dimension("retained_frames"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "SQLite instruction envelope for source header, latest birth and projection keys.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                constant(0, OCTETS),
                "No source frame, opaque assertion field, replay or hash is requested.",
            ),
        ),
        conditions=(
            "Optional narrow projections were populated from independently accepted Native frames.",
            "The caller supplies current source admission and workspace custody.",
            "Positive byte admission precedes the one projection BLOB read.",
        ),
        invariants=(
            "Latest birth and half-open retirement preserve correction and absent-gap semantics.",
            "Projection identity, type and assertion identity must match the exact selected row.",
            "Direct measurement is an explicit existence predicate over original evidence.",
            "Exact requested materialization hints and profile values are retained unchanged.",
        ),
        limitations=(
            "This is a fixed fact subset and does not pretend to be a complete assertion or graph.",
            "Required hint and profile-value bytes are selected data, not opaque source extent.",
            "Authority, complete semantics, preparation and retirement retain separate owners.",
        ),
    )


@declare_growth(_object_declaration())
def selected_object(
    corpus: CanonicalCorpusValidator,
    anchor: HistoryJournalAnchor,
    identity: str,
    *,
    byte_budget: int,
) -> dict[str, Any] | None:
    """Read only exact policy fields of one effective historical assertion."""
    corpus._require_open()
    corpus._check_selection_budget(byte_budget)
    corpus._check_source_anchor(anchor)
    require_canonical_uuid_urn(identity)
    sequence = str(anchor.through_sequence)
    metadata = corpus._source_read(
        "SELECT a.assertion,a.type,a.retired,length(p.value) FROM assertions a "
        "JOIN primary_selection_payloads p ON p.journal=a.journal AND p.assertion=a.assertion "
        "WHERE a.journal=? AND a.object=? "
        "AND (a.sequence_width,a.sequence)<=(?,?) "
        "ORDER BY a.sequence_width DESC,a.sequence DESC LIMIT 1",
        (anchor.journal_id, identity, len(sequence), sequence),
    )
    if metadata is None:
        return None
    assertion_id, kind, retired, size = metadata
    if retired is not None and (len(retired), retired) <= (len(sequence), sequence):
        return None
    if size > byte_budget:
        raise JournalValidationCapacity("selected fact exceeds the admitted byte budget")
    encoded = corpus._source_read(
        "SELECT substr(value,1,?) FROM primary_selection_payloads "
        "WHERE journal=? AND assertion=? AND length(value)=?",
        (size, anchor.journal_id, assertion_id, size),
    )
    if encoded is None:
        raise RuntimeError("selected fact changed outside its source fence")
    value: dict[str, Any] = json.loads(encoded[0])
    if value["id"] != identity or value["type"] != kind or value["assertion_id"] != assertion_id:
        raise RuntimeError("selected fact differs from its historical source identity")
    return value


@declare_growth(_index_declaration(True))
def retire_assertion(
    corpus: CanonicalCorpusValidator, journal: str, assertion_id: str, sequence: int
) -> None:
    selected = corpus._source_read(
        "SELECT kind,subject,predicate,born FROM primary_selection_keys "
        "WHERE journal=? AND assertion=?",
        (journal, assertion_id),
    )
    if selected is None:
        return
    kind, subject, predicate, born = selected
    _selection_event(corpus, journal, kind, subject, predicate, sequence)
    corpus._source_write(
        "DELETE FROM primary_selection_intervals WHERE journal=? AND assertion=?",
        (journal, assertion_id),
    )
    for node in _cover(born, sequence):
        corpus._source_write(
            "INSERT INTO primary_selection_intervals VALUES (?,?,?,?,?,?)",
            (journal, kind, subject, predicate, node, assertion_id),
        )


def _selection_event(
    corpus: CanonicalCorpusValidator,
    journal: str,
    kind: str,
    subject: bytes,
    predicate: str,
    sequence: int,
) -> None:
    text = str(sequence)
    corpus._source_write(
        "INSERT OR IGNORE INTO primary_selection_events VALUES(?,?,?,?,?,?)",
        (journal, kind, subject, predicate, len(text), text),
    )


def _revision_declaration() -> Declaration:
    probes = Expression(
        "sum",
        terms=(
            Expression("constant", value=1, unit=COUNT),
            Expression("dimension", name="predicates"),
        ),
    )
    return Declaration(
        id="riverhog-provenance.primary-selection-revision.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_selection.selection_revision",),
        scope="source-pinned-effective-selector-change-vector",
        starts_at="receive an exact source anchor and complete generic selector keys",
        ends_at="return the last indexed change at or before that anchor for each exact key",
        dimensions=(
            Dimension("predicates", COUNT, "Distinct exact selector predicates supplied."),
            Dimension("indexed_events", COUNT, "Independent full selector change index extent."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                probes,
                "One exact source header and one indexed predecessor seek per predicate.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                probes,
                "Every query returns at most one header or change position.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No frame, assertion or projected payload is loaded or hashed.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=4096),
                        probes,
                        Expression(
                            "log2_ceil",
                            terms=(
                                Expression(
                                    "sum",
                                    terms=(
                                        Expression("constant", value=2, unit=COUNT),
                                        Expression("dimension", name="indexed_events"),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Explicit SQLite indexed predecessor allowance; no event scan or sort.",
            ),
        ),
        conditions=(
            "Exact source identity and selector keys are part of the caller's memo key.",
            "Every selector assertion addition and retirement records an atomic source event.",
            "Current admission remains independent of this detached change vector.",
        ),
        invariants=(
            "Equal revisions within one exact source prove an unchanged selected effective set.",
            "Corrections advance the affected key even if values are subsequently restored.",
            "Unrelated documentary or other-context changes do not invalidate the selected set.",
        ),
        limitations=(
            "The vector is a source-local memo discriminator, not a permit or content identity.",
            "Application validation of the selected set retains its own owner and evidence.",
        ),
    )


@declare_growth(_revision_declaration())
def selection_revision(
    corpus: CanonicalCorpusValidator,
    anchor: HistoryJournalAnchor,
    *,
    kind: str,
    subject: Mapping[str, Any],
    predicates: tuple[str, ...],
) -> tuple[str | None, ...]:
    """Prove unchanged selected facts across different prefixes of one exact source."""
    corpus._require_open()
    if kind not in {"extension", "journal_subject"}:
        raise ValueError("unsupported keyed assertion category")
    if not predicates or any(not isinstance(value, str) or not value for value in predicates):
        raise ValueError("selection requires nonempty exact predicates")
    if len(set(predicates)) != len(predicates):
        raise ValueError("selection predicates must be distinct")
    corpus._check_source_anchor(anchor)
    encoded, sequence = canonical_document(subject), str(anchor.through_sequence)
    result = []
    for predicate in predicates:
        changed = corpus._source_read(
            "SELECT sequence FROM primary_selection_events "
            "WHERE journal=? AND kind=? AND subject=? AND predicate=? "
            "AND (width,sequence)<=(?,?) ORDER BY width DESC,sequence DESC LIMIT 1",
            (anchor.journal_id, kind, encoded, predicate, len(sequence), sequence),
        )
        result.append(None if changed is None else changed[0])
    return tuple(result)


@dataclass(frozen=True, slots=True)
class SelectionPage:
    """A bounded detached page; its continuation conveys no admission authority."""

    rows: tuple[dict[str, Any], ...]
    after: str | None
    complete: bool


def _selection_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    seeks = Expression(
        "sum",
        terms=(
            count(1),
            Expression("product", terms=(Expression("constant", value=64), dim("predicates"))),
            dim("selected_rows"),
        ),
    )
    child = growth_binding(selected_object).declaration
    return Declaration(
        id="riverhog-provenance.primary-selection-page.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_selection.selected_rows",),
        scope="bounded-exact-historical-selector-page",
        starts_at="receive a current admitted exact source anchor and generic selector keys",
        ends_at="return at most the admitted effective assertions and a detached keyset cursor",
        dimensions=(
            Dimension("predicates", COUNT, "Distinct exact property or role keys requested."),
            Dimension("row_budget", COUNT, "Maximum detached assertion count admitted by caller."),
            Dimension("selected_rows", COUNT, "Actual effective returned assertions."),
            Dimension("indexed_assertions", COUNT, "Independent complete selector index extent."),
            *(
                Dimension("object_" + item.name, item.unit, item.meaning)
                for item in child.dimensions
            ),
        ),
        bounds=(
            Bound("selected_rows", COUNT, "upper", dim("row_budget"), "Explicit page admission."),
            Bound(
                "selector_sql_statements",
                COUNT,
                "upper",
                seeks,
                "One exact anchor check, 64 initial node seeks per predicate, "
                "and at most one continuation seek per returned assertion.",
            ),
            Bound(
                "selector_metadata_rows",
                COUNT,
                "upper",
                seeks,
                "Each seek returns at most one fixed assertion/object identity pair.",
            ),
            Bound(
                "selected_object_calls",
                COUNT,
                "equal",
                dim("selected_rows"),
                "Historical narrow projection admission owns every returned fact payload.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "product",
                    terms=(
                        Expression("constant", value=4096),
                        seeks,
                        Expression(
                            "log2_ceil",
                            terms=(
                                Expression(
                                    "sum",
                                    terms=(
                                        count(2),
                                        Expression(
                                            "product",
                                            terms=(
                                                Expression("constant", value=126),
                                                dim("indexed_assertions"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "SQLite instruction envelope includes indexed node seeks and narrow object reads.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No original frame payload, source hashing or Native replay occurs.",
            ),
            *(
                Bound(
                    "object_" + bound.metric,
                    bound.unit,
                    "upper",
                    Expression(
                        "delegate",
                        name="object",
                        metric=bound.metric,
                        terms=(dim("selected_rows"),),
                        arguments=tuple(
                            (item.name, dim("object_" + item.name)) for item in child.dimensions
                        ),
                    ),
                    "Compose every actual narrow historical point read and its byte admission.",
                )
                for bound in child.bounds
            ),
        ),
        conditions=(
            "The caller retains current exact source admission and independent workspace custody.",
            "Native source indexing and correction intervals committed before selection.",
            "Each payload has the separately explicit positive byte admission.",
        ),
        invariants=(
            "Historical lifetimes are half-open and retired gaps never select earlier facts.",
            "Absent predicates visit no assertion payloads or unrelated historical rows.",
            "Every cursor closes before a value is returned; pages retain no database cursor.",
            "Metadata frontier is at most 64 rows per supplied predicate, never graph-sized.",
        ),
        limitations=(
            "Narrow payload bytes and their exact indexed work belong to selected_object.",
            "Application producer/completion validation and current authority require composition.",
            "No whole historical graph or corpus acceptance is implied.",
        ),
    )


@declare_growth(_selection_declaration(), delegates=(("object", selected_object),))
def selected_rows(
    corpus: CanonicalCorpusValidator,
    anchor: HistoryJournalAnchor,
    *,
    kind: str,
    subject: Mapping[str, Any],
    predicates: tuple[str, ...],
    byte_budget: int,
    row_budget: int = 1,
    after: str | None = None,
) -> SelectionPage:
    """Select one bounded page in assertion-ID order without scanning other rows."""
    corpus._require_open()
    corpus._check_selection_budget(byte_budget)
    if type(row_budget) is not int or row_budget < 1:
        raise ValueError("selection row budget must be positive")
    if kind not in {"extension", "journal_subject"}:
        raise ValueError("unsupported keyed assertion category")
    if not predicates or any(not isinstance(value, str) or not value for value in predicates):
        raise ValueError("selection requires nonempty exact predicates")
    if len(set(predicates)) != len(predicates):
        raise ValueError("selection predicates must be distinct")
    if after is not None:
        require_canonical_uuid_urn(after)
    corpus._check_source_anchor(anchor)
    encoded = canonical_document(subject)
    frontier: list[tuple[str, str, str, str]] = []

    def next_at(predicate: str, node: str, cursor: str) -> None:
        result = corpus._source_read(
            "SELECT i.assertion,a.object FROM primary_selection_intervals i "
            "JOIN assertions a ON a.journal=i.journal AND a.assertion=i.assertion "
            "WHERE i.journal=? AND i.kind=? AND i.subject=? AND i.predicate=? "
            "AND i.node=? AND i.assertion>? ORDER BY i.assertion LIMIT 1",
            (anchor.journal_id, kind, encoded, predicate, node, cursor),
        )
        if result is not None:
            heapq.heappush(frontier, (result[0], predicate, node, result[1]))

    for predicate in predicates:
        for node in _path(anchor.through_sequence):
            next_at(predicate, node, after or "")
    result: list[dict[str, Any]] = []
    cursor = after
    while frontier and len(result) < row_budget:
        assertion_id, predicate, node, object_id = heapq.heappop(frontier)
        selected = selected_object(corpus, anchor, object_id, byte_budget=byte_budget)
        if selected is None or selected["assertion_id"] != assertion_id:
            raise RuntimeError("historical selector differs from the exact effective assertion")
        result.append(selected)
        cursor = assertion_id
        next_at(predicate, node, assertion_id)
    return SelectionPage(tuple(result), cursor, not frontier)


class _SelectedObjects(Mapping[str, dict[str, Any]]):
    def __init__(self, selection: HistoricalSelection) -> None:
        self._selection = selection

    def __getitem__(self, identity: str) -> dict[str, Any]:
        value = self._selection.object(identity)
        if value is None:
            raise KeyError(identity)
        return value

    def __iter__(self) -> Iterator[str]:
        raise TypeError("historical object selection supports exact point access only")

    def __len__(self) -> int:
        raise TypeError("historical object selection has no whole-graph census")


class HistoricalSelection:
    """Current admitted source facts with explicit point and keyed-set access."""

    def __init__(
        self, corpus: CanonicalCorpusValidator, anchor: HistoryJournalAnchor, *, byte_budget: int
    ) -> None:
        corpus._require_open()
        corpus._check_selection_budget(byte_budget)
        corpus._check_source_anchor(anchor)
        self.corpus, self.anchor, self.byte_budget = corpus, anchor, byte_budget
        self.objects: Mapping[str, dict[str, Any]] = _SelectedObjects(self)

    def object(self, identity: str) -> dict[str, Any] | None:
        return selected_object(self.corpus, self.anchor, identity, byte_budget=self.byte_budget)

    def extensions(
        self,
        *,
        subject: Mapping[str, Any],
        properties: tuple[str, ...],
        after: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        yield from self._rows("extension", subject, properties, after)

    def journal_subjects(
        self,
        *,
        artifact: Mapping[str, Any],
        role: str,
        after: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        yield from self._rows("journal_subject", artifact, (role,), after)

    def _rows(
        self, kind: str, subject: Mapping[str, Any], predicates: tuple[str, ...], after: str | None
    ) -> Iterator[dict[str, Any]]:
        while True:
            page = selected_rows(
                self.corpus,
                self.anchor,
                kind=kind,
                subject=subject,
                predicates=predicates,
                byte_budget=self.byte_budget,
                after=after,
            )
            yield from page.rows
            if page.complete:
                return
            after = page.after

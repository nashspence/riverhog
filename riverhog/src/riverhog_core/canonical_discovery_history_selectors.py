"""Lower authenticated histories through their shared authored set incidences.

The fixed documentary and import-reachability joins create no member-by-snapshot
relation. Native corpus acceptance and current access retain their owners.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, cast

from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    MemberHistoryDocument,
    MemberHistoryImport,
    MemberHistoryRoot,
    RecordSetRef,
)
from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)
from riverhog_provenance_contracts import ExternalReference

from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    _atomic,
    _digest,
    _one,
)
from riverhog_core.canonical_discovery_history_selector_growth import (
    declare_growth_for_history_selectors,
)
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    HistorySetWork,
    _require_archive,
    history_descriptor_ready,
    history_import_progress,
    history_set_progress,
)
from riverhog_core.canonical_discovery_primary_work import verify_history_import_primary
from riverhog_core.canonical_discovery_selector_work import (
    PreparedSelectorPage,
    SelectorCapacity,
    SelectorProgress,
    SelectorWork,
    _encoded,
    _output_mapping,
    apply_selector_page,
    prepare_selector_page,
)
from riverhog_core.canonical_discovery_selectors import ImportedLateGuard, Selector
from riverhog_core.canonical_discovery_snapshot_work import history_snapshot_progress

_KINDS = {
    "history-documentary",
    "import-reachable",
    "root-set-documentary",
    "import-set-documentary",
    "import-set-reachable",
}


@dataclass(frozen=True, slots=True)
class _SourceReceipt:
    sql: str
    parameters: tuple[object, ...]
    exact_json: str


@dataclass(frozen=True, slots=True)
class PreparedHistorySelector:
    page: PreparedSelectorPage
    sources: tuple[_SourceReceipt, ...]


class HistorySelectorWork(SelectorWork):
    def __init__(
        self, key: ArchiveTaskKey, history: ArchiveHistoryWork, *, octet_budget: int
    ) -> None:
        super().__init__(key, octet_budget=octet_budget)
        self.history = history
        self._archive_binding = self._binding()
        self._prepared_history: PreparedHistorySelector | None = None

    def _binding(self) -> tuple[str, str, str]:
        return (
            self.history.admission.identity,
            self.history.admission.binding_json,
            self.history.admission.root_json,
        )

    def check(self, key: ArchiveTaskKey) -> None:
        if (
            self.key != key
            or key.admission_sha256 != self._archive_binding[0]
            or self._binding() != self._archive_binding
        ):
            raise StaleArchiveTask("history selector lost its exact archive/build/attempt binding")


def _receipt_json(sql: str, parameters: tuple[object, ...], row: Any, budget: int) -> str:
    return _encoded(
        {"sql": sql, "parameters": list(parameters), "row": None if row is None else list(row)},
        budget,
    )


class _CaptureCursor:
    def __init__(
        self, actual: sqlite3.Cursor, owner: _SourceReads, sql: str, parameters: tuple[object, ...]
    ) -> None:
        self.actual, self.owner, self.sql, self.parameters = actual, owner, sql, parameters
        self._read = False

    def fetchone(self) -> Any:
        if self._read:
            raise RuntimeError("history source control reads must detach at most one row")
        self._read = True
        row = self.actual.fetchone()
        self.owner.receipts.append(
            _SourceReceipt(
                self.sql,
                self.parameters,
                _receipt_json(self.sql, self.parameters, row, self.owner.work.octet_budget),
            )
        )
        return row

    def close(self) -> None:
        self.actual.close()


class _SourceReads:
    """Retain the fixed point/range controls actually used by one preparation."""

    def __init__(self, db: sqlite3.Connection, work: HistorySelectorWork) -> None:
        self.db, self.work = db, work
        self.receipts: list[_SourceReceipt] = []

    @property
    def in_transaction(self) -> bool:
        return self.db.in_transaction

    def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> _CaptureCursor:
        if not sql.lstrip().startswith("SELECT "):
            raise RuntimeError("history source preparation must only read immutable controls")
        return _CaptureCursor(self.db.execute(sql, parameters), self, sql, parameters)


def _same_source(left: str, right: str) -> bool:
    return left == right


def _text(raw: str, work: SelectorWork) -> bytes:
    value = raw.encode()
    if len(value) > work.octet_budget:
        raise SelectorCapacity("history selector control exceeds execution capacity")
    return value


def _previous(
    expected: SelectorProgress, output: Selector | ImportedLateGuard | None, work: SelectorWork
) -> None:
    if expected.last_output_json != _encoded(_output_mapping(output), work.octet_budget):
        raise StaleArchiveTask("history selector cursor differs from its original incidence")


def _descriptor(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    history: ArchiveHistoryWork,
    work: SelectorWork,
    identity: str,
) -> MemberHistoryDocument:
    row = _one(
        db,
        "SELECT descriptor_json FROM discovery_history_descriptors "
        "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
        (key.build_id, key.admission_sha256, identity),
    )
    if row is None:
        raise StaleArchiveTask("history selector has no original descriptor")
    descriptor = MemberHistoryDocument.from_json_bytes(_text(row[0], work))
    if descriptor.identity != identity or not history_descriptor_ready(db, key, history, identity):
        raise StaleArchiveTask("history selector descriptor or original set terminals changed")
    return descriptor


def _primary(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: SelectorWork,
    identity: str,
    descriptor: MemberHistoryDocument,
) -> ImportedLateGuard:
    row = _one(
        db,
        "SELECT worker_id,anchor_json,state_reference_json "
        "FROM discovery_history_primary_bindings "
        "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
        (key.build_id, key.admission_sha256, identity),
    )
    if row is None or row[:2] != (
        key.worker_id,
        canonical_json_bytes(descriptor.primary.journal.to_mapping()).decode(),
    ):
        raise StaleArchiveTask("import selector has no exact Native-verified history primary")
    reference = ExternalReference.model_validate(require_canonical_json(_text(row[2], work)))
    return ImportedLateGuard(identity, reference)


def _history_page(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    history: ArchiveHistoryWork,
    work: SelectorWork,
    expected: SelectorProgress,
) -> PreparedSelectorPage:
    identity = expected.selector.history_sha256
    assert identity is not None
    descriptor = _descriptor(db, key, history, work, identity)
    documentary = expected.selector.kind == "history-documentary"
    first: Selector | ImportedLateGuard = (
        Selector(
            "root-set-documentary",
            records_sha256=descriptor.roots.records_sha256,
            extent=expected.selector.extent,
        )
        if documentary
        else _primary(db, key, work, identity, descriptor)
    )
    second = Selector(
        "import-set-documentary" if documentary else "import-set-reachable",
        records_sha256=descriptor.imports.records_sha256,
    )
    first_phase = "roots" if documentary else "guard"
    if not expected.position and expected.version == 0:
        return prepare_selector_page(work, expected, position=(first_phase,), output=first)
    if expected.position == (first_phase,) and expected.version == 1:
        _previous(expected, first, work)
        return prepare_selector_page(work, expected, position=("imports",), output=second)
    if expected.position == ("imports",) and expected.version == 2:
        _previous(expected, second, work)
        return prepare_selector_page(work, expected, position=("end",), complete=True)
    raise StaleArchiveTask("history selector has an invalid fixed phase cursor")


def _set(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    history: ArchiveHistoryWork,
    work: SelectorWork,
    selector: Selector,
) -> tuple[HistorySetWork, int]:
    identity = selector.records_sha256
    assert identity is not None
    row = _one(
        db,
        "SELECT authority_json FROM discovery_history_sets "
        "WHERE build_id=? AND admission_sha256=? AND records_sha256=?",
        (key.build_id, key.admission_sha256, identity),
    )
    if row is None:
        raise StaleArchiveTask("selector has no original authored set authority")
    authority = RecordSetRef.from_mapping(require_canonical_json(_text(row[0], work)))
    schema = (
        MEMBER_HISTORY_ROOTS_SCHEMA
        if selector.kind == "root-set-documentary"
        else MEMBER_HISTORY_IMPORTS_SCHEMA
    )
    if authority.records_sha256 != identity or authority.schema_id != schema:
        raise StaleArchiveTask("selector authored set authority or kind changed")
    selected = HistorySetWork(history.admission, history.reader, authority)
    progress = history_set_progress(db, key, selected)
    if not progress.complete or progress.records != authority.record_count:
        raise StaleArchiveTask("selector authored set is not completely verified")
    if selector.kind == "root-set-documentary":
        assert selector.extent is not None
        coverage = history_snapshot_progress(db, key, selected, extent=selector.extent).complete
    else:
        coverage = history_import_progress(db, key, selected).complete
    if not coverage:
        raise StaleArchiveTask("selector authored set has incomplete original incidence coverage")
    return selected, progress.bound_roots


def _root_child(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: SelectorWork,
    selected: HistorySetWork,
    selector: Selector,
    record_key: str,
    raw: str,
) -> tuple[Selector | None, bool]:
    root = MemberHistoryRoot.from_mapping(require_canonical_json(_text(raw, work)))
    if root.key != record_key:
        raise StaleArchiveTask("root selector record differs from its original key")
    bound = root.inclusion == "bound"
    if selector.extent == BOUND_HISTORY_EXTENT and not bound:
        return None, False
    row = _one(
        db,
        "SELECT l.source_key,l.prefix_sha256,l.inclusion,h.anchor_json "
        "FROM discovery_history_snapshot_links l JOIN discovery_snapshot_headers h ON "
        "h.build_id=l.build_id AND h.admission_sha256=l.admission_sha256 "
        "AND h.source_key=l.source_key AND h.prefix_sha256=l.prefix_sha256 "
        "JOIN discovery_snapshot_sources s ON s.build_id=h.build_id "
        "AND s.admission_sha256=h.admission_sha256 AND s.source_key=h.source_key "
        "WHERE l.build_id=? AND l.admission_sha256=? AND l.root_set_sha256=? AND l.record_key=?",
        (key.build_id, key.admission_sha256, selected.authority.records_sha256, record_key),
    )
    if row is None or row[1:] != (
        root.journal.prefix_sha256,
        root.inclusion,
        canonical_json_bytes(root.journal.to_mapping()).decode(),
    ):
        raise StaleArchiveTask("root selector lost its exact admitted snapshot incidence")
    return Selector("snapshot-documentary", source_key=row[0], anchor=root.journal), bound


def _import_child(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: SelectorWork,
    selected: HistorySetWork,
    selector: Selector,
    record_key: str,
    raw: str,
) -> tuple[Selector, bool]:
    imported = MemberHistoryImport.from_mapping(require_canonical_json(_text(raw, work)))
    if imported.key != record_key:
        raise StaleArchiveTask("import selector record differs from its original key")
    verify_history_import_primary(db, key, selected, record_key)
    if selector.kind == "import-set-documentary":
        return Selector(
            "history-documentary",
            history_sha256=imported.source_history_sha256,
            extent=imported.extent,
        ), False
    return Selector("import-reachable", history_sha256=imported.source_history_sha256), False


def _child(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: SelectorWork,
    selected: HistorySetWork,
    selector: Selector,
    record_key: str,
    raw: str,
) -> tuple[Selector | None, bool]:
    if selector.kind == "root-set-documentary":
        return _root_child(db, key, work, selected, selector, record_key, raw)
    return _import_child(db, key, work, selected, selector, record_key, raw)


def _set_page(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    history: ArchiveHistoryWork,
    work: SelectorWork,
    expected: SelectorProgress,
) -> PreparedSelectorPage:
    selected, bound_total = _set(db, key, history, work, expected.selector)
    base = (key.build_id, key.admission_sha256, selected.authority.records_sha256)
    after, count, bound = "", 0, 0
    query = "SELECT record_key,value_json FROM discovery_history_set_records "
    condition = "WHERE build_id=? AND admission_sha256=? AND records_sha256=? "
    if expected.position:
        if len(expected.position) != 4 or expected.position[0] != "records":
            raise StaleArchiveTask("authored set selector has an invalid original record cursor")
        _, after, count_text, bound_text = expected.position
        _digest(after)
        count, bound = (
            parse_scalar("nonnegative", count_text),
            parse_scalar("nonnegative", bound_text),
        )
        if not 0 < count <= selected.authority.record_count or not 0 <= bound <= count:
            raise StaleArchiveTask("authored set selector cursor exceeds independent source counts")
        previous = _one(db, query + condition + "AND record_key=?", (*base, after))
        if previous is None:
            raise StaleArchiveTask("authored set selector lost its last original record")
        child, _ = _child(db, key, work, selected, expected.selector, *previous)
        _previous(expected, child, work)
    if expected.version != count:
        raise StaleArchiveTask(
            "authored set selector version differs from its original record count"
        )
    row = _one(
        db, query + condition + "AND record_key>? ORDER BY record_key LIMIT 1", (*base, after)
    )
    if row is None:
        if count != selected.authority.record_count or bound != bound_total:
            raise StaleArchiveTask("authored set selector end omits independently counted records")
        return prepare_selector_page(work, expected, position=("end",), complete=True)
    if count == selected.authority.record_count:
        raise StaleArchiveTask("authored set selector has excess original records")
    child, is_bound = _child(db, key, work, selected, expected.selector, *row)
    return prepare_selector_page(
        work,
        expected,
        position=(
            "records",
            row[0],
            format_scalar("nonnegative", count + 1),
            format_scalar("nonnegative", bound + int(is_bound)),
        ),
        output=child,
    )


def prepare_history_selector(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: HistorySelectorWork,
    expected: SelectorProgress,
) -> PreparedHistorySelector:
    """Prepare one exact history/set incidence without expanding member relevance."""
    if db.in_transaction:
        raise ValueError("history selector preparation requires no active transaction")
    work.check(key)
    if expected.selector.kind not in _KINDS or expected.complete:
        raise StaleArchiveTask("history selector preparation has another owner or fixed kind")
    sources = _SourceReads(db, work)
    # The owning helpers use only this closed one-row SELECT interface here.
    source_db = cast(sqlite3.Connection, sources)
    _require_archive(source_db, key, work.history.admission)
    if expected.selector.kind in {"history-documentary", "import-reachable"}:
        page = _history_page(source_db, key, work.history, work, expected)
    else:
        page = _set_page(source_db, key, work.history, work, expected)
    result = PreparedHistorySelector(page, tuple(sources.receipts))
    work._prepared_history = result
    return result


def apply_history_selector(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    work: HistorySelectorWork,
    prepared: PreparedHistorySelector,
) -> SelectorProgress:
    """Recheck original source controls before committing incidence plus cursor."""
    work.check(key)
    if work._prepared_history is not prepared:
        raise StaleArchiveTask("history selector is not its current exact detached preparation")
    with _atomic(db):
        for receipt in prepared.sources:
            row = _one(db, receipt.sql, receipt.parameters)
            actual = _receipt_json(receipt.sql, receipt.parameters, row, work.octet_budget)
            if not _same_source(actual, receipt.exact_json):
                raise StaleArchiveTask("history selector original source changed after preparation")
        return apply_selector_page(db, key, work, prepared.page)


declare_growth_for_history_selectors(globals())

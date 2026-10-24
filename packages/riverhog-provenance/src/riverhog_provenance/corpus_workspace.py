"""Owner-current retained work for the exact Native corpus algorithms."""

from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol, Self, cast

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import declare_growth, growth_binding
from riverhog_provenance_contracts import ContractCatalog

from ._corpus_order import _CorpusOrder
from ._corpus_workspace_growth import (
    dependency_declaration,
    lifecycle_declaration,
    order_declaration,
    semantic_declaration,
)
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .corpus import (
    CanonicalCorpusValidator,
    CorpusDependencyProgress,
    CorpusOrderProgress,
    CorpusSemanticProgress,
    CorpusSourceProgress,
    HistoricalAssertion,
    SelectedDeliverySupport,
)
from .errors import IncompleteSourceError, JournalValidationCapacity
from .journal import JournalSummary

_FORMAT = "riverhog-native-corpus-workspace/v1"


class StaleCorpusWorkspace(RuntimeError):
    """The retained corpus belongs to different inputs, state or worker."""


@dataclass(frozen=True, slots=True)
class CorpusReplayBinding:
    """Independent fixed input expectations supplied by the current owner.

    Manifest identities bind the attempt. Exact membership and unique completed
    counts establish coverage; these labels alone do not establish that proof.
    """

    attempt_id: str
    directory_id: str
    source_manifest_sha256: str
    source_count: int
    selection_manifest_sha256: str
    selection_count: int
    policy_sha256: str
    catalog_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.attempt_id) is not str
            or re.fullmatch(r"[0-9a-f]{32}", self.attempt_id) is None
        ):
            raise ValueError("retained corpus requires an exact owned attempt identity")
        if type(self.directory_id) is not str or not self.directory_id:
            raise ValueError("retained corpus requires an owned directory identity")
        for count in (self.source_count, self.selection_count):
            if type(count) is not int or count < 0:
                raise ValueError("corpus manifest counts must be nonnegative integers")
        for value in (
            self.source_manifest_sha256,
            self.selection_manifest_sha256,
            self.policy_sha256,
            self.catalog_sha256,
        ):
            if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError("corpus manifest and policy identities require lowercase SHA-256")

    def to_bytes(self) -> bytes:
        value = asdict(self)
        value.update(source_count=str(self.source_count), selection_count=str(self.selection_count))
        return canonical_json_bytes({"format": _FORMAT, **value})


class CorpusValidationSource(Protocol):
    """Current owner-authenticated fixed sets, with bounded exact membership.

    The owner holds source custody and the exclusive workspace kernel lease.
    Each manifest is a set of exact anchors: its count is independent of the
    work already performed. Admission checks one indexed record without scanning
    source bytes or a manifest. Currentness checks the live owner/job fence and
    immutable binding. This port does not admit client-supplied databases.
    """

    @property
    def binding(self) -> CorpusReplayBinding: ...

    def verify_current(self) -> bool: ...

    def admit_source(self, anchor: HistoryJournalAnchor) -> bool: ...

    def admit_selection(self, anchor: HistoryJournalAnchor) -> bool: ...


def _one(db: sqlite3.Connection, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
    with closing(db.execute(sql, parameters)) as rows:
        return rows.fetchone()


def _write(db: sqlite3.Connection, sql: str, parameters: tuple[Any, ...] = ()) -> int:
    with closing(db.execute(sql, parameters)) as rows:
        return rows.rowcount


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _checkpoint(corpus: CanonicalCorpusValidator) -> bytes:
    def cursor(value: tuple[Any, ...] | None) -> list[str] | None:
        return None if value is None else [str(item) for item in value]

    return canonical_json_bytes(
        {
            "semantic_phase": corpus._semantic_phase,
            "semantic_after": cursor(corpus._semantic_after),
            "semantic_processed": str(corpus._semantic_processed),
            "dependency_phase": corpus._dependency_phase,
            "dependency_after": cursor(corpus._dependency_after),
            "dependency_pending": cursor(corpus._dependency_pending),
            "dependency_checked": str(corpus._dependency_checked),
        }
    )


def _restore(corpus: CanonicalCorpusValidator, raw: bytes) -> None:
    import json

    try:
        value = json.loads(raw)
        phase = value["semantic_phase"]
        dependency = value["dependency_phase"]
        if phase not in ("open", "walking", "complete") or dependency not in (
            "references",
            "forks",
            "comparisons",
            "complete",
        ):
            raise ValueError("invalid corpus phase")
        after = value["semantic_after"]
        if after is not None and (len(after) != 2 or any(type(item) is not str for item in after)):
            raise ValueError("invalid corpus semantic key")
        corpus._semantic_phase = phase
        corpus._semantic_after = None if after is None else cast(tuple[str, str], tuple(after))
        corpus._semantic_processed = int(value["semantic_processed"])
        corpus._dependency_phase = dependency
        for field in ("after", "pending"):
            key = value["dependency_" + field]
            if key is not None:
                if dependency in ("references", "forks"):
                    expected = 1 if field == "after" else 2
                    if len(key) != expected:
                        raise ValueError("invalid corpus dependency key")
                    key = tuple(int(item) for item in key)
                elif dependency == "comparisons":
                    expected = 2 if field == "after" else 3
                    if len(key) != expected or any(type(item) is not str for item in key):
                        raise ValueError("invalid corpus comparison key")
                    key = (*key[:2], int(key[2])) if field == "pending" else tuple(key)
                else:
                    raise ValueError("completed dependency retains an unfinished key")
            setattr(corpus, "_dependency_" + field, key)
        corpus._dependency_checked = int(value["dependency_checked"])
        if (
            min(corpus._semantic_processed, corpus._dependency_checked) < 0
            or _checkpoint(corpus) != raw
        ):
            raise ValueError("noncanonical corpus checkpoint")
    except (ValueError, TypeError, KeyError) as error:
        raise StaleCorpusWorkspace("retained corpus checkpoint is corrupt") from error


class _OrderConnection:
    """Map private ordering tables and commits into the owner transaction."""

    def __init__(self, db: sqlite3.Connection, kind: str) -> None:
        self.db, self.kind = db, kind

    def execute(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        if query == "BEGIN IMMEDIATE":
            query = "SAVEPOINT corpus_order_quantum"
        elif query == "COMMIT":
            query = "RELEASE corpus_order_quantum"
        query = re.sub(
            r"\b(nodes|ready|checkpoint)\b",
            lambda match: "corpus_order_" + self.kind + "_" + match[0],
            query,
        )
        return self.db.execute(query, parameters)

    def rollback(self) -> None:
        self.db.rollback()

    def close(self) -> None:
        pass


class _RetainedOrder(_CorpusOrder):
    """Use the existing ordering algorithm inside the owner's single database."""

    def __init__(self, db: sqlite3.Connection, kind: str) -> None:
        self.source, self.db = db, _OrderConnection(db, kind)
        self.kind, self._finished = kind, None

    def close(self) -> None:
        pass


class RetainedCorpusValidator:
    """Retain Native progress under the application's current source and lease.

    Every successful bounded call commits its data and checkpoint together.
    This owner exposes no whole-corpus reset or discovery-publication grant.
    It owns no independent result readers; close releases the callback only
    after its sole database handle closes. The application retains the files.
    """

    _source: CorpusValidationSource
    _binding: CorpusReplayBinding
    _encoded: bytes
    _epoch: int
    _metadata_budget: int
    _closed: bool
    _on_close: Callable[[], None] | None
    _db: sqlite3.Connection
    _corpus: CanonicalCorpusValidator

    @classmethod
    @declare_growth(lifecycle_declaration("open_retained"))
    def open_retained(
        cls,
        directory: Path,
        source: CorpusValidationSource,
        *,
        worker_epoch: int,
        create: bool = False,
        catalog: ContractCatalog | None = None,
        metadata_byte_budget: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX,
        on_storage_close: Callable[[], None] | None = None,
    ) -> Self:
        if type(worker_epoch) is not int or worker_epoch < 0:
            raise ValueError("retained corpus requires a nonnegative worker epoch")
        if type(metadata_byte_budget) is not int or metadata_byte_budget < 1:
            raise ValueError("retained corpus metadata budget must be positive")
        binding = source.binding
        if not isinstance(binding, CorpusReplayBinding):
            raise TypeError("retained corpus requires an exact replay binding")
        if binding.catalog_sha256 != (catalog or ContractCatalog()).checkpoint_sha256():
            raise ValueError("retained corpus source has a different contract catalog")
        encoded = binding.to_bytes()
        if len(encoded) > metadata_byte_budget:
            raise JournalValidationCapacity("corpus binding exceeds admitted metadata budget")
        if source.verify_current() is not True:
            raise IncompleteSourceError("retained corpus source is no longer current")
        directory = Path(directory)
        path = directory / "corpus.sqlite3"
        if directory.is_symlink() or not directory.is_dir() or path.is_symlink():
            raise StaleCorpusWorkspace("retained corpus directory is unavailable")
        if create:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(descriptor)
            _sync_directory(directory)
        elif not path.is_file():
            raise StaleCorpusWorkspace("retained corpus database is missing")
        result = cls.__new__(cls)
        result._source, result._binding, result._encoded = source, binding, encoded
        result._epoch, result._metadata_budget = worker_epoch, metadata_byte_budget
        result._closed, result._on_close = False, None
        result._db = sqlite3.connect(path.resolve().as_uri() + "?mode=rw", uri=True)
        try:
            db = result._db
            if create:
                _write(db, "PRAGMA auto_vacuum=INCREMENTAL")
                if _one(db, "PRAGMA journal_mode=DELETE") != ("delete",):
                    raise StaleCorpusWorkspace("retained corpus journal mode is unavailable")
            elif _one(db, "PRAGMA journal_mode") != ("delete",) or _one(
                db, "PRAGMA auto_vacuum"
            ) != (2,):
                raise StaleCorpusWorkspace("retained corpus storage format changed")
            for statement in (
                "PRAGMA synchronous=FULL",
                "PRAGMA fullfsync=ON",
                "PRAGMA cache_size=-512",
                "PRAGMA temp_store=FILE",
            ):
                _write(db, statement)
            result._corpus = CanonicalCorpusValidator._from_workspace(db, directory)
            _write(db, "BEGIN IMMEDIATE")
            if create:
                result._corpus._initialize_schema()
                result._initialize()
            else:
                old_epoch, version, checkpoint, sources, selections = result._load(replacing=True)
                if worker_epoch <= old_epoch:
                    raise ValueError("retained corpus replacement requires a newer worker epoch")
                _restore(result._corpus, checkpoint)
                changed = _write(
                    db,
                    "UPDATE corpus_workspace SET worker_epoch=?,version=? WHERE singleton=1 "
                    "AND worker_epoch=? AND version=? AND binding=? AND checkpoint=?",
                    (
                        str(worker_epoch),
                        str(version + 1),
                        str(old_epoch),
                        str(version),
                        encoded,
                        checkpoint,
                    ),
                )
                if changed != 1:
                    raise StaleCorpusWorkspace("retained corpus worker changed")
            result._admit_checkpoint(
                _checkpoint(result._corpus), version=0 if create else version + 1
            )
            result._verify_current()
            db.commit()
            if create:
                _sync_directory(directory)
        except sqlite3.DatabaseError as error:
            result._db.close()
            raise StaleCorpusWorkspace(
                "retained corpus storage is corrupt or incomplete"
            ) from error
        except BaseException:
            result._db.close()
            raise
        result._on_close = on_storage_close
        return result

    def _initialize(self) -> None:
        db = self._db
        _write(
            db,
            "CREATE TABLE corpus_workspace(singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "format TEXT NOT NULL,binding BLOB NOT NULL,worker_epoch TEXT NOT NULL,"
            "version TEXT NOT NULL,checkpoint BLOB NOT NULL,"
            "sources TEXT NOT NULL,selections TEXT NOT NULL,schema_cookie TEXT NOT NULL)",
        )
        _write(db, "CREATE TABLE corpus_source_inputs(anchor BLOB PRIMARY KEY) WITHOUT ROWID")
        _write(db, "CREATE TABLE corpus_selected_inputs(anchor BLOB PRIMARY KEY) WITHOUT ROWID")
        for kind in ("derivation", "specialization"):
            order = _RetainedOrder(db, kind)
            order._write("CREATE TABLE nodes(id TEXT PRIMARY KEY,degree TEXT NOT NULL)")
            order._write("CREATE TABLE ready(id TEXT PRIMARY KEY)")
            order._write(
                "CREATE TABLE checkpoint(singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                "phase TEXT,source TEXT,target TEXT,seed TEXT,current TEXT,outgoing TEXT,"
                "vertices TEXT,completed TEXT,source_edges TEXT)"
            )
            order._write("INSERT INTO checkpoint VALUES(1,'input','','','','','','0','0','0')")

        schema_cookie = _one(db, "PRAGMA schema_version")[0]
        _write(
            db,
            "INSERT INTO corpus_workspace VALUES(1,?,?,?,?,?,'0','0',?)",
            (
                _FORMAT,
                self._encoded,
                str(self._epoch),
                "0",
                _checkpoint(self._corpus),
                str(schema_cookie),
            ),
        )

    def _verify_current(self) -> None:
        if self._source.binding != self._binding or self._source.verify_current() is not True:
            raise IncompleteSourceError("retained corpus source is no longer current")

    def _load(self, *, replacing: bool = False) -> tuple[int, int, bytes, int, int]:
        metadata = _one(
            self._db,
            "SELECT format=?,binding=?,schema_cookie=?,length(CAST(worker_epoch AS BLOB))"
            "+length(CAST(version AS BLOB))"
            "+length(checkpoint)+length(CAST(sources AS BLOB))+length(CAST(selections AS BLOB)) "
            "FROM corpus_workspace WHERE singleton=1",
            (_FORMAT, self._encoded, str(_one(self._db, "PRAGMA schema_version")[0])),
        )
        if metadata is None or metadata[:3] != (1, 1, 1):
            raise StaleCorpusWorkspace("retained corpus format or input binding changed")
        if metadata[3] > self._metadata_budget:
            raise JournalValidationCapacity("corpus checkpoint exceeds admitted metadata budget")
        row = _one(
            self._db,
            "SELECT worker_epoch,version,checkpoint,sources,selections "
            "FROM corpus_workspace WHERE singleton=1",
        )
        try:
            assert row is not None
            epoch, version, checkpoint, sources, selections = row
            values = tuple(int(item) for item in (epoch, version, sources, selections))
            if min(values) < 0 or tuple(str(item) for item in values) != (
                epoch,
                version,
                sources,
                selections,
            ):
                raise ValueError("noncanonical workspace counters")
            if not replacing and values[0] != self._epoch:
                raise StaleCorpusWorkspace("retained corpus worker changed")
            return values[0], values[1], checkpoint, values[2], values[3]
        except (ValueError, TypeError) as error:
            raise StaleCorpusWorkspace("retained corpus counters are corrupt") from error

    def _admit_checkpoint(self, checkpoint: bytes, *, version: int) -> None:
        output_size = (
            len(checkpoint)
            + len(str(version))
            + len(str(self._epoch))
            + len(str(self._binding.source_count))
            + len(str(self._binding.selection_count))
        )
        if output_size > self._metadata_budget:
            raise JournalValidationCapacity("next corpus checkpoint exceeds metadata budget")

    @declare_growth(lifecycle_declaration("_operation"))
    @contextmanager
    def _operation(self) -> Iterator[None]:
        if self._closed:
            raise RuntimeError("retained corpus is closed")
        self._verify_current()
        db = self._db
        _write(db, "BEGIN IMMEDIATE")
        try:
            epoch, version, checkpoint, _, _ = self._load()
            _restore(self._corpus, checkpoint)
            self._corpus._orders = {
                kind: _RetainedOrder(db, kind) for kind in ("derivation", "specialization")
            }
            yield
            self._verify_current()
            after_checkpoint = _checkpoint(self._corpus)
            self._admit_checkpoint(after_checkpoint, version=version + 1)
            changed = _write(
                db,
                "UPDATE corpus_workspace SET checkpoint=?,version=? WHERE singleton=1 "
                "AND worker_epoch=? AND version=? AND checkpoint=? AND binding=?",
                (
                    after_checkpoint,
                    str(version + 1),
                    str(epoch),
                    str(version),
                    checkpoint,
                    self._encoded,
                ),
            )
            if changed != 1:
                raise StaleCorpusWorkspace("retained corpus checkpoint changed")
            db.commit()
        except BaseException:
            db.rollback()
            raise

    def _admit(self, anchor: HistoryJournalAnchor, *, selection: bool) -> None:
        admission = self._source.admit_selection if selection else self._source.admit_source
        if admission(anchor) is not True:
            raise IncompleteSourceError("corpus input is absent from its exact owned manifest")

    def _register(self, anchor: HistoryJournalAnchor, *, selection: bool) -> None:
        table = "corpus_selected_inputs" if selection else "corpus_source_inputs"
        column = "selections" if selection else "sources"
        expected = self._binding.selection_count if selection else self._binding.source_count
        key = canonical_json_bytes(anchor.to_mapping())
        changed = _write(self._db, "INSERT OR IGNORE INTO " + table + " VALUES (?)", (key,))
        if changed:
            row = _one(self._db, "SELECT " + column + " FROM corpus_workspace WHERE singleton=1")
            count = int(row[0]) + 1
            if count > expected:
                raise IncompleteSourceError("corpus inputs exceed the independent manifest count")
            _write(
                self._db,
                "UPDATE corpus_workspace SET " + column + "=? WHERE singleton=1",
                (str(count),),
            )

    def advance_source(
        self, summary: JournalSummary, *, frame_budget: int = 1
    ) -> CorpusSourceProgress:
        anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
        with self._operation():
            self._admit(anchor, selection=False)
            progress = self._corpus.advance_source(summary, frame_budget=frame_budget)
            if progress.complete:
                self._register(anchor, selection=False)
            return progress

    def select_source(self, anchor: HistoryJournalAnchor) -> None:
        with self._operation():
            self._admit(anchor, selection=True)
            self._corpus.select_source(anchor)
            self._register(anchor, selection=True)

    @declare_growth(
        semantic_declaration(
            growth_binding(CanonicalCorpusValidator.advance_semantics).declaration,
            growth_binding(_operation).declaration,
        ),
        delegates=(
            ("native", CanonicalCorpusValidator.advance_semantics),
            ("transaction", _operation),
        ),
    )
    def advance_semantics(
        self, *, row_budget: int = 64, byte_budget: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
    ) -> CorpusSemanticProgress:
        with self._operation():
            row = _one(
                self._db, "SELECT sources,selections FROM corpus_workspace WHERE singleton=1"
            )
            if row != (str(self._binding.source_count), str(self._binding.selection_count)):
                raise IncompleteSourceError("corpus fixed input manifests are incomplete")
            return self._corpus.advance_semantics(row_budget=row_budget, byte_budget=byte_budget)

    @declare_growth(
        dependency_declaration(
            growth_binding(CanonicalCorpusValidator.advance_dependencies).declaration,
            growth_binding(_operation).declaration,
        ),
        delegates=(
            ("native", CanonicalCorpusValidator.advance_dependencies),
            ("transaction", _operation),
        ),
    )
    def advance_dependencies(
        self, *, row_budget: int = 64, byte_budget: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
    ) -> CorpusDependencyProgress:
        with self._operation():
            return self._corpus.advance_dependencies(row_budget=row_budget, byte_budget=byte_budget)

    @declare_growth(
        order_declaration(
            growth_binding(CanonicalCorpusValidator.advance_order).declaration,
            growth_binding(_operation).declaration,
        ),
        delegates=(("native", CanonicalCorpusValidator.advance_order), ("transaction", _operation)),
    )
    def advance_order(
        self,
        kind: str,
        *,
        row_budget: int = 64,
        byte_budget: int = PROVENANCE_JOURNAL_ENTRY_BYTES_MAX,
    ) -> CorpusOrderProgress:
        with self._operation():
            return self._corpus.advance_order(kind, row_budget=row_budget, byte_budget=byte_budget)

    def historical_assertion(
        self, anchor: HistoryJournalAnchor, object_id: str, *, byte_budget: int
    ) -> HistoricalAssertion | None:
        with self._operation():
            return self._corpus.historical_assertion(anchor, object_id, byte_budget=byte_budget)

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
        with self._operation():
            return self._corpus.selected_delivery_support(
                binding=binding,
                artifact_id=artifact_id,
                byte_count=byte_count,
                sha256=sha256,
                member_role=member_role,
                metadata_byte_budget=metadata_byte_budget,
            )

    @declare_growth(lifecycle_declaration("close"))
    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._corpus.__exit__()
            callback, self._on_close = self._on_close, None
            if callback is not None:
                callback()

    def __enter__(self) -> Self:
        if self._closed:
            raise RuntimeError("retained corpus is closed")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

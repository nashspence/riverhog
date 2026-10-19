"""Bounded persistent indexes for one independently evaluated journal snapshot."""

from __future__ import annotations

import json
import shutil
import sqlite3
import weakref
from collections.abc import (
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Sequence,
)
from contextlib import closing
from pathlib import Path
from tempfile import mkdtemp
from threading import Lock
from typing import Any, cast, overload
from uuid import uuid4

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
)
from riverhog_provenance_contracts import ContractCatalog

from ._activity_projection import read_activity
from ._assertion_staging import AssertionStaging
from ._assertion_staging import install as install_assertion_staging
from ._correction_projection import CorrectionWork
from ._correction_projection import install as install_correction_projection
from ._correction_targets import CorrectionTargets
from ._correction_targets import install as install_correction_targets
from ._entry_preparation import EntryPreparation
from ._entry_preparation import install as install_entry_preparation
from ._generic_semantics import GenericAtom, GenericSemantics
from ._generic_semantics import install as install_generic_semantics
from ._journal_prefix import install as install_journal_prefix
from ._online_dag import OnlineDag
from ._online_dag import install as install_online_dag
from ._semantic_edge import SemanticEdge
from ._semantic_edge import install as install_semantic_edges
from ._semantic_finish import SemanticFinish
from ._semantic_finish import install as install_semantic_finish
from ._semantic_source import SemanticSource
from ._semantic_source import install as install_semantic_source
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ProvenanceValidationError
from .graph import GraphValidation, _Validator, time_ns


def _key(value: Any) -> bytes:
    if isinstance(value, bytes):
        return b"b" + value
    if isinstance(value, tuple):
        return b"t" + b"".join(str(len(part)).encode() + b":" + part for part in map(_key, value))
    return b"j" + canonical_json_bytes(value)


class _Map[T](MutableMapping[Any, T]):
    def __init__(self, store: JournalStore, name: str) -> None:
        self.store, self.name = store, name

    def __getitem__(self, key: Any) -> T:
        row = self.store.db.execute(
            "SELECT value FROM maps WHERE name = ? AND key = ?", (self.name, _key(key))
        ).fetchone()
        if row is None:
            raise KeyError(key)
        return cast(T, json.loads(row[0]))

    def __setitem__(self, key: Any, value: T) -> None:
        self.store.db.execute(
            "INSERT OR REPLACE INTO maps VALUES (?, ?, ?)",
            (self.name, _key(key), canonical_json_bytes(value)),
        )

    def __delitem__(self, key: Any) -> None:
        if not self.store.db.execute(
            "DELETE FROM maps WHERE name = ? AND key = ?", (self.name, _key(key))
        ).rowcount:
            raise KeyError(key)

    def __iter__(self) -> Iterator[Any]:
        with closing(
            self.store.db.execute("SELECT key FROM maps WHERE name = ?", (self.name,))
        ) as rows:
            for (key,) in rows:
                self.store.require_open()
                yield key[1:] if key.startswith(b"b") else json.loads(key[1:])

    def __len__(self) -> int:
        return int(
            self.store.db.execute(
                "SELECT count(*) FROM maps WHERE name = ?", (self.name,)
            ).fetchone()[0]
        )

    def values(self) -> Any:
        with closing(
            self.store.db.execute(
                "SELECT value FROM maps WHERE name = ? ORDER BY key", (self.name,)
            )
        ) as rows:
            for row in rows:
                self.store.require_open()
                yield json.loads(row[0])


class _Set(MutableSet[Any]):
    def __init__(self, store: JournalStore, name: str) -> None:
        self.store, self.name = store, name

    def __contains__(self, value: object) -> bool:
        return (
            self.store.db.execute(
                "SELECT 1 FROM maps WHERE name = ? AND key = ?", (self.name, _key(value))
            ).fetchone()
            is not None
        )

    def __iter__(self) -> Iterator[Any]:
        return iter(_Map[Any](self.store, self.name))

    def __len__(self) -> int:
        return len(_Map[Any](self.store, self.name))

    def add(self, value: Any) -> None:
        self.store.db.execute(
            "INSERT OR IGNORE INTO maps VALUES (?, ?, ?)", (self.name, _key(value), b"null")
        )

    def discard(self, value: Any) -> None:
        self.store.db.execute(
            "DELETE FROM maps WHERE name = ? AND key = ?", (self.name, _key(value))
        )


class _Values[T](Sequence[T]):
    def __init__(self, values: _Map[T]) -> None:
        self.values = values

    def __len__(self) -> int:
        return len(self.values)

    def __iter__(self) -> Iterator[T]:
        yield from self.values.values()

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[T]: ...

    def __getitem__(self, index: int | slice) -> T | Sequence[T]:
        if isinstance(index, slice):
            return _Slice(self, range(*index.indices(len(self))), store=self.values.store)
        position = index if index >= 0 else len(self) + index
        if position < 0:
            raise IndexError(index)
        row = self.values.store.db.execute(
            "SELECT value FROM maps WHERE name = ? ORDER BY key LIMIT 1 OFFSET ?",
            (self.values.name, position),
        ).fetchone()
        if row is None:
            raise IndexError(index)
        return cast(T, json.loads(row[0]))


class _Slice[T](Sequence[T]):
    def __init__(self, source: Sequence[T], indices: range, *, store: JournalStore) -> None:
        self.source, self.indices, self.store = source, indices, store

    def __len__(self) -> int:
        self.store.require_open()
        return len(self.indices)

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[T]: ...

    def __getitem__(self, index: int | slice) -> T | Sequence[T]:
        if isinstance(index, slice):
            self.store.require_open()
            return _Slice(self.source, self.indices[index], store=self.store)
        return self.source[self.indices[index]]


class _Objects(Mapping[str, dict[str, Any]]):
    def __init__(self, store: JournalStore) -> None:
        self.store = store

    def __getitem__(self, key: str) -> dict[str, Any]:
        row = self.store.db.execute(
            "SELECT value FROM assertions WHERE object_id = ? AND active = 1", (key,)
        ).fetchone()
        if row is None:
            raise KeyError(key)
        return cast(dict[str, Any], json.loads(row[0]))

    def __iter__(self) -> Iterator[str]:
        with closing(
            self.store.db.execute(
                "SELECT object_id FROM assertions WHERE active = 1 ORDER BY assertion_id"
            )
        ) as rows:
            for (identity,) in rows:
                self.store.require_open()
                yield identity

    def __len__(self) -> int:
        return int(
            self.store.db.execute("SELECT count(*) FROM assertions WHERE active = 1").fetchone()[0]
        )

    def values(self) -> Any:
        with closing(
            self.store.db.execute(
                "SELECT value FROM assertions WHERE active = 1 ORDER BY assertion_id"
            )
        ) as rows:
            for row in rows:
                self.store.require_open()
                yield json.loads(row[0])


class _Rows(Sequence[dict[str, Any]]):
    def __init__(self, store: JournalStore, category: str) -> None:
        self.store, self.category = store, category

    def __len__(self) -> int:
        return int(
            self.store.db.execute(
                "SELECT count(*) FROM assertions WHERE active = 1 AND category = ?",
                (self.category,),
            ).fetchone()[0]
        )

    @overload
    def __getitem__(self, index: int) -> dict[str, Any]: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[dict[str, Any]]: ...

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return _Slice(self, range(*index.indices(len(self))), store=self.store)
        index = index if index >= 0 else len(self) + index
        if index < 0:
            raise IndexError(index)
        row = self.store.db.execute(
            "SELECT value FROM assertions WHERE active = 1 AND category = ? "
            "ORDER BY assertion_id LIMIT 1 OFFSET ?",
            (self.category, index),
        ).fetchone()
        if row is None:
            raise IndexError(index)
        return json.loads(row[0])

    def __iter__(self) -> Iterator[dict[str, Any]]:
        with closing(
            self.store.db.execute(
                "SELECT value FROM assertions WHERE active = 1 AND category = ? "
                "ORDER BY assertion_id",
                (self.category,),
            )
        ) as rows:
            for (value,) in rows:
                self.store.require_open()
                yield json.loads(value)


class _Graph(Mapping[str, Sequence[dict[str, Any]]]):
    def __init__(self, store: JournalStore) -> None:
        self.store = store

    def __getitem__(self, category: str) -> Sequence[dict[str, Any]]:
        if (
            self.store.db.execute(
                "SELECT 1 FROM assertions WHERE active = 1 AND category = ? LIMIT 1", (category,)
            ).fetchone()
            is None
        ):
            raise KeyError(category)
        return _Rows(self.store, category)

    def __iter__(self) -> Iterator[str]:
        with closing(
            self.store.db.execute(
                "SELECT DISTINCT category FROM assertions WHERE active = 1 ORDER BY category"
            )
        ) as rows:
            for (category,) in rows:
                self.store.require_open()
                yield category

    def __len__(self) -> int:
        return sum(1 for _ in self)


class _Frames(Sequence[Any]):
    def __init__(self, store: JournalStore) -> None:
        self.store = store

    def __len__(self) -> int:
        self.store.require_open()
        return self.store.frame_count

    @overload
    def __getitem__(self, index: int) -> Any: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[Any]: ...

    def __getitem__(self, index: int | slice) -> Any:
        from .journal import JournalFrame

        if isinstance(index, slice):
            return _Slice(self, range(*index.indices(len(self))), store=self.store)
        index = index if index >= 0 else len(self) + index
        row = self.store.db.execute(
            "SELECT value FROM frames WHERE sequence = ?", (str(index),)
        ).fetchone()
        if row is None:
            raise IndexError(index)
        return JournalFrame(row[0])

    def __iter__(self) -> Iterator[Any]:
        from .journal import JournalFrame

        with closing(
            self.store.db.execute("SELECT value FROM frames ORDER BY length(sequence), sequence")
        ) as rows:
            for (value,) in rows:
                self.store.require_open()
                yield JournalFrame(value)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Sequence)
            and len(self) == len(other)
            and all(a == b for a, b in zip(self, other, strict=True))
        )


class _Origins(Mapping[str, dict[str, str]]):
    def __init__(self, store: JournalStore) -> None:
        self.store = store

    def __getitem__(self, identity: str) -> dict[str, str]:
        row = self.store.db.execute(
            "SELECT frames.reference FROM assertions JOIN frames USING(sequence) "
            "WHERE assertion_id = ?",
            (identity,),
        ).fetchone()
        if row is None:
            raise KeyError(identity)
        return cast(dict[str, str], json.loads(row[0]))

    def __iter__(self) -> Iterator[str]:
        with closing(
            self.store.db.execute("SELECT assertion_id FROM assertions ORDER BY assertion_id")
        ) as rows:
            for (identity,) in rows:
                self.store.require_open()
                yield identity

    def __len__(self) -> int:
        return int(self.store.db.execute("SELECT count(*) FROM assertions").fetchone()[0])


class _Retired(MutableSet[str]):
    def __init__(self, store: JournalStore) -> None:
        self.store = store

    def __contains__(self, value: object) -> bool:
        return (
            isinstance(value, str)
            and self.store.db.execute(
                "SELECT 1 FROM assertions WHERE assertion_id = ? AND active = 0", (value,)
            ).fetchone()
            is not None
        )

    def __iter__(self) -> Iterator[str]:
        with closing(
            self.store.db.execute(
                "SELECT assertion_id FROM assertions WHERE active = 0 ORDER BY assertion_id"
            )
        ) as rows:
            for (identity,) in rows:
                self.store.require_open()
                yield identity

    def __len__(self) -> int:
        return int(
            self.store.db.execute("SELECT count(*) FROM assertions WHERE active = 0").fetchone()[0]
        )

    def add(self, value: str) -> None:
        raise TypeError("a validated snapshot is immutable")

    def discard(self, value: str) -> None:
        raise TypeError("a validated snapshot is immutable")


class _Findings(MutableSequence[str]):
    def __init__(self, store: JournalStore) -> None:
        self.store = store

    def __len__(self) -> int:
        return int(self.store.db.execute("SELECT count(*) FROM findings").fetchone()[0])

    @overload
    def __getitem__(self, index: int) -> str: ...

    @overload
    def __getitem__(self, index: slice) -> MutableSequence[str]: ...

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        index = index if index >= 0 else len(self) + index
        row = self.store.db.execute(
            "SELECT value FROM findings ORDER BY key LIMIT 1 OFFSET ?", (index,)
        ).fetchone()
        if row is None:
            raise IndexError(index)
        return row[0]

    def __iter__(self) -> Iterator[str]:
        with closing(self.store.db.execute("SELECT value FROM findings ORDER BY key")) as rows:
            for (value,) in rows:
                self.store.require_open()
                yield value

    def __setitem__(self, index: Any, value: Any) -> None:
        raise TypeError("findings are append-only")

    def __delitem__(self, index: Any) -> None:
        raise TypeError("findings are append-only")

    def insert(self, index: int, value: str) -> None:
        self.append(value)

    def append(self, value: str) -> None:
        self.store.db.execute("INSERT OR IGNORE INTO findings VALUES (?, ?)", (value, value))


class _Edges:
    def __init__(self, validator: _StoredValidator, kind: str) -> None:
        self.validator, self.kind = validator, kind

    def append(self, edge: tuple[str, str]) -> None:
        source, target = edge
        if source == target:
            raise ProvenanceValidationError(f"{self.kind} cannot relate an entity to itself")
        row = self.validator.current_row
        if row is None:
            raise RuntimeError("semantic graph insertion requires its current assertion")
        store = self.validator.store
        # Native checks precede insertion; the private child owns each graph unit.
        store.db.commit()
        work = SemanticEdge(
            store.db,
            kind=self.kind,
            run_id=store._dag_runs[self.kind],
            attempt_id=uuid4().hex,
            source_binding=self.validator.semantic_source,
            assertion_id=row["assertion_id"],
            object_id=row["id"],
            source=source,
            target=target,
        )
        work.initialize()
        while not work.advance().done:
            pass
        work.result()


class _StoredValidator(_Validator):
    def __init__(
        self,
        store: JournalStore,
        rows: Iterable[dict[str, Any]],
        *,
        catalog: ContractCatalog,
        journal_id: str,
        require_profiles: bool,
        frame: Any,
    ) -> None:
        self.store, self.rows, self.frame = store, rows, frame
        self.catalog, self.journal_id, self.require_profiles = catalog, journal_id, require_profiles
        self.current_row: dict[str, Any] | None = None
        self.semantic_source = SemanticSource.from_frame(
            frame, catalog_sha256=catalog.checkpoint_sha256(), require_profiles=require_profiles
        )
        self.objects = store.objects
        self.externals = _Map(store, "externals")
        self.profiles = _Map(store, "profiles")
        self.findings = _Findings(store)

    def _lifecycle_activity(self, identity: str) -> dict[str, Any]:
        return read_activity(self.store.db, identity)

    def _semantics(self, value: Mapping[str, Any], operation: str) -> None:
        self.store.db.commit()
        work = GenericSemantics(
            self.store.db,
            attempt_id=uuid4().hex,
            source_binding=self.semantic_source,
            atom=GenericAtom.admit(value),
            catalog=self.catalog,
            operation=operation,
        )
        work.initialize()
        while not work.advance().done:
            pass
        work.result()

    def generic(self, value: Mapping[str, Any]) -> None:
        self._semantics(value, "generic")

    def row_semantics(self, value: Mapping[str, Any]) -> None:
        self._semantics(value, "row")

    def aggregates(self) -> tuple[Any, ...]:
        return (
            _Map(self.store, "generation"),
            _Map(self.store, "invalidation"),
            _Edges(self, "derivation"),
            _Edges(self, "specialization"),
            _Map(self.store, "captures"),
            _Set(self.store, "ends"),
            _Set(self.store, "slots"),
        )

    def validation_rows(self) -> Iterable[dict[str, Any]]:
        for row in self.rows:
            self.current_row = row
            yield row
            # The native row checks run before this iterator resumes.
            if row["type"] in ("usage", "invalidation") and "at" in row:
                self.store._record_lifecycle_time(
                    row["state"]["object_id"], row["type"], time_ns(row["at"])
                )
            self.current_row = None

    def finish(
        self,
        generation: Mapping[str, Any],
        invalidation: Mapping[str, Any],
        derivations: Iterable[tuple[str, str]],
        specializations: Iterable[tuple[str, str]],
    ) -> None:
        # Native row checks precede these reverse checks. Each post-row unit
        # owns its transaction and remains private until prefix publication.
        self.store.db.commit()
        work = SemanticFinish(
            self.store,
            attempt_id=uuid4().hex,
            frame=self.frame,
            catalog_sha256=self.catalog.checkpoint_sha256(),
            require_profiles=self.require_profiles,
        )
        work.initialize()
        atom_admission = 2 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 4096
        while not work.advance(byte_budget=atom_admission).done:
            pass


def _cleanup_snapshot(connection: sqlite3.Connection, directory: Path) -> None:
    connection.close()
    shutil.rmtree(directory)


class _SnapshotStorage:
    """One frozen projection shared by independently owned reader leases."""

    def __init__(self, connection: sqlite3.Connection, directory: Path) -> None:
        self.connection = connection
        self.directory = directory
        self._lock = Lock()
        self._readers = 0
        self._closed = False

    def retain(self, owner: JournalStore | None = None) -> None:
        with self._lock:
            if self._closed or (owner is not None and not owner._cleanup.alive):
                raise RuntimeError("journal summary is closed")
            self._readers += 1

    def release(self) -> None:
        with self._lock:
            self._readers -= 1
            if self._readers:
                return
            self._closed = True
        _cleanup_snapshot(self.connection, self.directory)


class JournalStore:
    def __init__(self) -> None:
        directory = Path(mkdtemp(prefix="riverhog-journal-"))
        try:
            connection = sqlite3.connect(directory / "snapshot.sqlite3", check_same_thread=False)
        except BaseException:
            shutil.rmtree(directory)
            raise
        self._storage = _SnapshotStorage(connection, directory)
        self._storage.retain()
        self._cleanup = weakref.finalize(self, self._storage.release)
        self._frozen = False
        try:
            # This is a disposable validation projection, never journal custody.
            # Failed validation destroys it; preserve disk-backed rollback without
            # synchronizing each temporary schema write to durable storage.
            self.db.execute("PRAGMA synchronous = OFF")
            self.db.execute("PRAGMA cache_size = -512")
            self.db.execute("PRAGMA temp_store = FILE")
            self.db.executescript(
                "CREATE TABLE identities(id TEXT PRIMARY KEY, domain TEXT, value BLOB);"
                "CREATE TABLE assertions(assertion_id TEXT PRIMARY KEY, object_id TEXT, "
                "category TEXT, type TEXT, state_id TEXT, observation_id TEXT, sequence TEXT, "
                "value BLOB, active INTEGER);"
                "CREATE UNIQUE INDEX objects ON assertions(object_id) WHERE active = 1;"
                "CREATE INDEX states ON assertions(state_id, type) WHERE active = 1;"
                "CREATE INDEX observations ON assertions(observation_id, type) WHERE active = 1;"
                "CREATE TABLE changes(assertion_id TEXT PRIMARY KEY);"
                "CREATE TABLE lifecycle_extrema(state_id TEXT PRIMARY KEY, "
                "earliest TEXT NOT NULL, latest_usage TEXT);"
                "CREATE INDEX categories ON assertions(category, assertion_id) WHERE active = 1;"
                "CREATE TABLE maps(name TEXT, key BLOB, value BLOB, PRIMARY KEY(name, key));"
                "CREATE INDEX external_types ON maps(name, json_extract(value, '$.object_id'));"
                "CREATE TABLE findings(key TEXT PRIMARY KEY, value TEXT);"
                "CREATE TABLE fixities(state TEXT, size TEXT, sha256 TEXT, "
                "PRIMARY KEY(state, size, sha256));"
            )
            install_journal_prefix(self.db)
            install_correction_projection(self.db)
            install_correction_targets(self.db)
            install_semantic_finish(self.db)
            install_entry_preparation(self.db)
            install_assertion_staging(self.db)
            install_semantic_source(self.db)
            install_semantic_edges(self.db)
            install_generic_semantics(self.db)
            install_online_dag(self.db)
            self._dag_runs = {
                "derivation": uuid4().hex,
                "specialization": uuid4().hex,
            }
            for kind, run in self._dag_runs.items():
                OnlineDag(self.db, kind, run).bind()
            self.frame_count = 0
        except BaseException:
            self.close()
            raise

    def _record_lifecycle_time(self, state: str, kind: str, instant: int) -> None:
        with closing(
            self.db.execute(
                "SELECT earliest, latest_usage FROM lifecycle_extrema WHERE state_id = ?",
                (state,),
            )
        ) as rows:
            prior = rows.fetchone()
        earliest = min(instant, int(prior[0])) if prior else instant
        latest = int(prior[1]) if prior and prior[1] is not None else None
        if kind == "usage":
            latest = max(instant, latest) if latest is not None else instant
        self.db.execute(
            "INSERT INTO lifecycle_extrema VALUES (?, ?, ?) "
            "ON CONFLICT(state_id) DO UPDATE SET "
            "earliest = excluded.earliest, latest_usage = excluded.latest_usage",
            (state, str(earliest), str(latest) if latest is not None else None),
        ).close()

    @declare_growth(
        Declaration(
            id="riverhog-provenance.sqlite-lifecycle-extrema-check.growth/v1",
            authority="riverhog-provenance",
            subjects=("riverhog_provenance.journal_store.JournalStore._lifecycle_bounds_hold",),
            scope="native-sqlite-effective-lifecycle-metadata-check",
            starts_at="check one state against its effective dated extrema",
            ends_at="return the inequality result after closing the native cursor",
            dimensions=(Dimension("indexed_states", COUNT, "States with dated extrema."),),
            bounds=(
                Bound(
                    "sql_statements",
                    COUNT,
                    "equal",
                    Expression("constant", value=1, unit=COUNT),
                    "One primary-key metadata lookup.",
                ),
                Bound(
                    "returned_rows",
                    COUNT,
                    "upper",
                    Expression("constant", value=1, unit=COUNT),
                    "At most the selected state's extrema.",
                ),
                Bound(
                    "returned_time_octets",
                    OCTETS,
                    "upper",
                    Expression("constant", value=42, unit=OCTETS),
                    "Two exact decimal nanosecond values from native four-digit UTC years.",
                ),
                Bound(
                    "sqlite_vm_instructions",
                    COUNT,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=128, unit=COUNT),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=64, unit=COUNT),
                                    Expression(
                                        "log2_ceil",
                                        terms=(
                                            Expression(
                                                "sum",
                                                terms=(
                                                    Expression("constant", value=2, unit=COUNT),
                                                    Expression("dimension", name="indexed_states"),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                    "Maintained native SQLite instruction envelope for indexed metadata.",
                ),
            ),
            conditions=(
                "The component-owned extrema table has its state-identity primary key.",
                "Extrema contain only effective natively checked dated usage/invalidation rows.",
                "Known bounds use the native canonical UTC instant interpretation.",
            ),
            invariants=(
                "Untimed events supply no synthetic timestamp or bound.",
                "The earliest dated usage/invalidation and latest dated usage preserve the "
                "native generation/invalidation inequalities, including equality.",
                "No assertion payload is read by this primitive; missing dated extrema succeed.",
                "Real corrections clear and rebuild extrema from their effective view.",
            ),
            limitations=(
                "Population, native row/uniqueness/activity checks, violation diagnostic scans, "
                "graph work, retries and cleanup are separately charged.",
                "This disposable projection is not source custody or a durable "
                "semantic checkpoint.",
                "SQLite VM instructions exclude internal B-tree operations, page I/O and CPU.",
            ),
        )
    )
    def _lifecycle_bounds_hold(
        self, state: str, *, generated_at: str | None, invalidated_at: str | None
    ) -> bool:
        with closing(
            self.db.execute(
                "SELECT earliest, latest_usage FROM lifecycle_extrema WHERE state_id = ?",
                (state,),
            )
        ) as rows:
            extrema = rows.fetchone()
        if extrema is None:
            return True
        if generated_at is not None and int(extrema[0]) < time_ns(generated_at):
            return False
        return not (
            extrema[1] is not None
            and invalidated_at is not None
            and int(extrema[1]) > time_ns(invalidated_at)
        )

    @declare_growth(
        Declaration(
            id="riverhog-provenance.sqlite-changed-assertion-reset.growth/v1",
            authority="riverhog-provenance",
            subjects=("riverhog_provenance.journal_store.JournalStore._reset_changed_assertions",),
            scope="native-sqlite-successful-change-metadata-retirement",
            starts_at="retire one admitted batch from the owned changed-identity queue",
            ends_at="return the retired count after closing the native result",
            dimensions=(
                Dimension("queued_identities", COUNT, "Identities queued before this unit."),
                Dimension("row_budget", COUNT, "Maximum identities admitted for this unit."),
            ),
            bounds=(
                Bound(
                    "sql_statements",
                    COUNT,
                    "equal",
                    Expression("constant", value=1, unit=COUNT),
                    "One indexed metadata DELETE with an admitted LIMIT and RETURNING.",
                ),
                Bound(
                    "retired_rows",
                    COUNT,
                    "upper",
                    Expression("dimension", name="row_budget"),
                    "Actual identities retired, including inactive assertions.",
                ),
                Bound(
                    "retired_identity_octets",
                    OCTETS,
                    "upper",
                    Expression(
                        "product",
                        terms=(
                            Expression("constant", value=45, unit=OCTETS_PER_COUNT),
                            Expression("dimension", name="row_budget"),
                        ),
                    ),
                    "Returned identity text; native assertion identities are UUID URNs.",
                ),
                Bound(
                    "sqlite_vm_instructions",
                    COUNT,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=128, unit=COUNT),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=128),
                                    Expression("dimension", name="row_budget"),
                                ),
                            ),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=64, unit=COUNT),
                                    Expression(
                                        "log2_ceil",
                                        terms=(
                                            Expression(
                                                "sum",
                                                terms=(
                                                    Expression("constant", value=2, unit=COUNT),
                                                    Expression(
                                                        "dimension", name="queued_identities"
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                    "Maintained native SQLite instruction envelope for admitted deletion.",
                ),
            ),
            conditions=(
                "The component-owned changes queue has its assertion-identity primary key.",
                "Queue identities come from the native entry schema's canonical UUID URNs.",
                "row_budget is a positive native SQLite LIMIT admission.",
            ),
            invariants=(
                "Both active and inactive changed identities are retired without changing "
                "assertion content or liveness.",
                "Assertion payload rows are not read or rewritten by this primitive.",
                "Empty and exhausted queues succeed; the result closes on every exit.",
            ),
            limitations=(
                "This private projection is disposable and is not source custody or a "
                "durable semantic checkpoint.",
                "Population, correction rebuilds, native graph/schema work, retries and "
                "cleanup are separately charged.",
                "SQLite VM instructions exclude internal B-tree operations, page I/O and CPU.",
            ),
        )
    )
    def _reset_changed_assertions(self, *, row_budget: int) -> int:
        """Retire metadata without traversing or rewriting assertion payloads."""
        if type(row_budget) is not int or row_budget < 1:
            raise ValueError("changed-identity row budget must be positive")
        with closing(
            self.db.execute(
                "DELETE FROM changes WHERE assertion_id IN "
                "(SELECT assertion_id FROM changes ORDER BY assertion_id LIMIT ?) "
                "RETURNING assertion_id",
                (row_budget,),
            )
        ) as rows:
            return sum(1 for _ in rows)

    def require_open(self) -> None:
        if not self._cleanup.alive:
            raise RuntimeError("journal summary is closed")

    @property
    def db(self) -> sqlite3.Connection:
        self.require_open()
        return self._storage.connection

    def close(self) -> None:
        self._cleanup()

    def borrow(self) -> JournalStore:
        self.require_open()
        if not self._frozen:
            raise RuntimeError("only a frozen journal summary can be borrowed")
        reader = object.__new__(JournalStore)
        reader._storage = self._storage
        reader._storage.retain(self)
        reader._cleanup = weakref.finalize(reader, reader._storage.release)
        reader._frozen = True
        reader.frame_count = self.frame_count
        return reader

    def freeze(self) -> None:
        self.db.commit()
        self.db.execute("PRAGMA query_only = ON")
        self._frozen = True

    @declare_growth(
        Declaration(
            id="riverhog-provenance.sqlite-entry-recording.growth/v1",
            authority="riverhog-provenance",
            subjects=("riverhog_provenance.journal_store.JournalStore._check_entry_recording",),
            scope="native-sqlite-effective-entry-recorder-and-context-check",
            starts_at="receive one admitted entry and its owner-pinned effective view",
            ends_at="return native recording checks after closing at most two indexed cursors",
            dimensions=(
                Dimension(
                    "indexed_assertions", COUNT, "Retained active and retired assertion rows."
                ),
            ),
            bounds=(
                Bound(
                    "sql_statements",
                    COUNT,
                    "upper",
                    Expression("constant", value=2, unit=COUNT),
                    "One recorder predicate and at most one recording-context predicate.",
                ),
                Bound(
                    "returned_rows",
                    COUNT,
                    "upper",
                    Expression("constant", value=2, unit=COUNT),
                    "The unique effective-object index admits at most one row per identity.",
                ),
                Bound(
                    "returned_metadata_octets",
                    OCTETS,
                    "upper",
                    Expression("constant", value=2, unit=OCTETS),
                    "Each returned predicate is a single SQLite Boolean scalar.",
                ),
                Bound(
                    "bound_parameter_octets",
                    OCTETS,
                    "upper",
                    Expression("constant", value=102, unit=OCTETS),
                    "Two 45-octet canonical UUID URNs plus the agent and context type names.",
                ),
                Bound(
                    "changed_rows",
                    COUNT,
                    "equal",
                    Expression("constant", value=0, unit=COUNT),
                    "The fixed unit is read-only and leaves caller transactions unchanged.",
                ),
                Bound(
                    "historical_payload_octets",
                    OCTETS,
                    "equal",
                    Expression("constant", value=0, unit=OCTETS),
                    "Only indexed type predicates are read; no assertion or frame body is fetched.",
                ),
                Bound(
                    "sqlite_vm_instructions",
                    COUNT,
                    "upper",
                    Expression(
                        "product",
                        terms=(
                            Expression("constant", value=128, unit=COUNT),
                            Expression(
                                "log2_ceil",
                                terms=(
                                    Expression(
                                        "sum",
                                        terms=(
                                            Expression("dimension", name="indexed_assertions"),
                                            Expression("constant", value=2, unit=COUNT),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                    "Maintained native instruction envelope for two covering-index point queries.",
                ),
            ),
            conditions=(
                "The entry has passed its owning schema and preceding native assertion checks.",
                "Canonical entry identity fields are 45-octet UUID URNs.",
                "Owner-authenticated staging keeps type metadata equal "
                "to the admitted payload type.",
                "The effective-object uniqueness and semantic-reference "
                "covering indexes are retained.",
                "The owning session pins one effective view throughout this read-only unit.",
            ),
            invariants=(
                "Recorder resolution precedes optional recording-context resolution.",
                "Retired assertions do not resolve either effective identity.",
                "All cursors close before return or semantic rejection; stored state is unchanged.",
            ),
            limitations=(
                "SQLite B-tree work, page I/O and lock waits are separate indexed native costs.",
                "This fixed unit owns no durable progress; "
                "session completion checkpoints its result.",
                "The unit grants no accepted-prefix advance or archive authority.",
            ),
        )
    )
    def _check_entry_recording(self, document: Mapping[str, Any]) -> None:
        """Check one entry's effective recorder/context without reading retained bodies."""

        def matches(identity: str, kind: str) -> bool:
            with closing(
                self.db.execute(
                    "SELECT type = ? FROM assertions INDEXED BY semantic_reference_type "
                    "WHERE object_id = ? AND active = 1",
                    (kind, identity),
                )
            ) as rows:
                row = rows.fetchone()
            return row is not None and row[0] == 1

        if not matches(document["recorded_by_agent_id"], "agent"):
            raise ProvenanceValidationError("entry recorder must resolve to an effective agent")
        if "recording_context_id" in document and not matches(
            document["recording_context_id"], "context"
        ):
            raise ProvenanceValidationError("recording context does not resolve")

    def accept(
        self,
        document: Mapping[str, Any],
        frame: Any,
        *,
        catalog: ContractCatalog,
        require_profiles: bool,
    ) -> None:
        journal_id = document["journal_id"]
        self.db.commit()
        preparation = EntryPreparation(
            self,
            attempt_id=uuid4().hex,
            frame=frame,
            catalog_sha256=catalog.checkpoint_sha256(),
            require_profiles=require_profiles,
        )
        preparation.initialize()
        while not preparation.advance().done:
            pass
        preparation.result()
        correction = document["entry_kind"] == "correction"
        if correction:
            # Entry identity staging is private; target units own fresh transactions.
            self.db.commit()
            correction_attempt = uuid4().hex
            targets = CorrectionTargets(self.db, attempt_id=correction_attempt, frame=frame)
            targets.initialize()
            # Existing rows originate in admitted entries; charge their actual
            # logical bytes without fetching or rehashing historical payloads.
            atom_admission = 2 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 4096
            while not targets.advance(byte_budget=atom_admission).done:
                pass
        # Each source-ordered staging page owns its identities, facts and cursor.
        # All work remains private until subsequent native semantics accept it.
        self.db.commit()
        staging = AssertionStaging(
            self.db,
            attempt_id=uuid4().hex,
            frame=frame,
            catalog_sha256=catalog.checkpoint_sha256(),
            require_profiles=require_profiles,
        )
        staging.initialize()
        staging_admission = 4 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 8192
        while not staging.advance(byte_budget=staging_admission).done:
            pass
        if correction:
            # Earlier native checks/staging remain entry-local, private work.
            # Each projection phase starts without inheriting that transaction.
            self.db.commit()
            correction_source = frame.sha256
            retirement = CorrectionWork(
                self.db,
                operation="retire",
                attempt_id=correction_attempt,
                source_binding=correction_source,
            )
            retirement.initialize()
            while not retirement.retire(byte_budget=atom_admission).done:
                pass
            for kind, run in self._dag_runs.items():
                checker = OnlineDag(self.db, kind, run).replace_run(uuid4().hex)
                self._dag_runs[kind] = checker.run_id
                while checker.retire():
                    pass
            seeding = CorrectionWork(
                self.db,
                operation="seed",
                attempt_id=correction_attempt,
                source_binding=correction_source,
            )
            seeding.initialize()
            while not seeding.seed().done:
                pass
        rows = (
            json.loads(value)
            for (value,) in self.db.execute(
                "SELECT assertions.value FROM changes CROSS JOIN assertions "
                "WHERE assertions.assertion_id = changes.assertion_id AND active = 1 "
                "ORDER BY changes.assertion_id"
            )
        )
        validator = _StoredValidator(
            self,
            rows,
            catalog=catalog,
            journal_id=journal_id,
            require_profiles=require_profiles,
            frame=frame,
        )
        validator.run()
        self._check_entry_recording(document)

    @property
    def frames(self) -> Sequence[Any]:
        return _Frames(self)

    @property
    def origins(self) -> Mapping[str, dict[str, str]]:
        return _Origins(self)

    @property
    def retired(self) -> MutableSet[str]:
        return _Retired(self)

    @property
    def objects(self) -> Mapping[str, dict[str, Any]]:
        return _Objects(self)

    @property
    def view(self) -> Mapping[str, Sequence[dict[str, Any]]]:
        return _Graph(self)

    @property
    def external_references(self) -> Sequence[dict[str, Any]]:
        return _Values(_Map(self, "externals"))

    @property
    def unresolved_profiles(self) -> Sequence[dict[str, str]]:
        return _Values(_Map(self, "profiles"))

    @property
    def findings(self) -> Sequence[str]:
        return _Findings(self)

    def iter_assertions(self) -> Iterable[tuple[str, dict[str, Any]]]:
        with closing(
            self.db.execute(
                "SELECT category, value FROM assertions WHERE active = 1 "
                "ORDER BY category, assertion_id"
            )
        ) as rows:
            for category, value in rows:
                self.require_open()
                yield category, json.loads(value)

    @property
    def graph_validation(self) -> GraphValidation:
        return GraphValidation(b"", b"", b"", (), self)

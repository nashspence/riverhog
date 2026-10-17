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
from riverhog_provenance_contracts import ContractCatalog, canonical_document

from ._journal_prefix import install as install_journal_prefix
from ._online_dag import OnlineDag
from ._online_dag import install as install_online_dag
from .common import fingerprint
from .errors import ProvenanceValidationError
from .graph import GraphValidation, _Validator, iter_assertions, time_ns


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
    def __init__(self, store: JournalStore, kind: str) -> None:
        self.store, self.kind = store, kind
        self.checker = OnlineDag(store.db, kind, store._dag_runs[kind])

    def append(self, edge: tuple[str, str]) -> None:
        source, target = edge
        if source == target:
            raise ProvenanceValidationError(f"{self.kind} cannot relate an entity to itself")
        # The projection stays private until the whole native result is frozen.
        # Each graph transition owns a separate bounded checkpoint transaction.
        self.store.db.commit()
        self.checker.add(source, target)


class _StoredValidator(_Validator):
    def __init__(
        self,
        store: JournalStore,
        rows: Iterable[dict[str, Any]],
        *,
        catalog: ContractCatalog,
        journal_id: str,
        require_profiles: bool,
    ) -> None:
        self.store, self.rows = store, rows
        self.catalog, self.journal_id, self.require_profiles = catalog, journal_id, require_profiles
        self.objects = store.objects
        self.externals = _Map(store, "externals")
        self.profiles = _Map(store, "profiles")
        self.findings = _Findings(store)

    def aggregates(self) -> tuple[Any, ...]:
        return (
            _Map(self.store, "generation"),
            _Map(self.store, "invalidation"),
            _Edges(self.store, "derivation"),
            _Edges(self.store, "specialization"),
            _Map(self.store, "captures"),
            _Set(self.store, "ends"),
            _Set(self.store, "slots"),
        )

    def validation_rows(self) -> Iterable[dict[str, Any]]:
        for row in self.rows:
            yield row
            # The native row checks run before this iterator resumes.
            if row["type"] in ("usage", "invalidation") and "at" in row:
                self.store._record_lifecycle_time(
                    row["state"]["object_id"], row["type"], time_ns(row["at"])
                )

    def finish(
        self,
        generation: Mapping[str, Any],
        invalidation: Mapping[str, Any],
        derivations: Iterable[tuple[str, str]],
        specializations: Iterable[tuple[str, str]],
    ) -> None:
        # New immutable rows cannot invalidate existing local references. Only
        # lifecycle bounds, external reference types and completeness need
        # reverse checks. Corrections rebuild the same indexes from effective rows.
        with closing(
            self.store.db.execute(
                "SELECT assertions.value FROM changes CROSS JOIN assertions "
                "WHERE assertions.assertion_id = changes.assertion_id AND active = 1 "
                "ORDER BY changes.assertion_id"
            )
        ) as rows:
            for (value,) in rows:
                self.store.require_open()
                row = json.loads(value)
                kind = row["type"]
                if kind in ("usage", "generation", "invalidation"):
                    state = row["state"]["object_id"]
                    generated, ended = generation.get(state), invalidation.get(state)
                    if not self.store._lifecycle_bounds_hold(
                        state,
                        generated_at=generated.get("at") if generated else None,
                        invalidated_at=ended.get("at") if ended else None,
                    ):
                        # Preserve native diagnostic selection on a violation.
                        with closing(
                            self.store.db.execute(
                                "SELECT value FROM assertions WHERE active = 1 AND state_id = ? "
                                "AND type IN ('usage', 'invalidation')",
                                (state,),
                            )
                        ) as rows:
                            for (related,) in rows:
                                self.store.require_open()
                                item = json.loads(related)
                                if "at" not in item:
                                    continue
                                if (
                                    generated
                                    and "at" in generated
                                    and time_ns(item["at"]) < time_ns(generated["at"])
                                ):
                                    raise ProvenanceValidationError(
                                        "state used or invalidated before known generation"
                                    )
                                if (
                                    item["type"] == "usage"
                                    and ended
                                    and "at" in ended
                                    and time_ns(item["at"]) > time_ns(ended["at"])
                                ):
                                    raise ProvenanceValidationError(
                                        "state used after known invalidation"
                                    )
                if kind == "observation" and row["address_status"] == "known":
                    if (
                        self.store.db.execute(
                            "SELECT 1 FROM assertions WHERE active = 1 AND observation_id = ? "
                            "AND type = 'locator_binding' LIMIT 1",
                            (row["id"],),
                        ).fetchone()
                        is None
                    ):
                        raise ProvenanceValidationError(
                            "known address requires an observed locator binding"
                        )
                if kind in ("observation", "reported_description") and "content" in row:
                    state = row["state"]["object_id"]
                    fixity = fingerprint(row["content"])
                    self.store.db.execute(
                        "INSERT OR IGNORE INTO fixities VALUES (?, ?, ?)",
                        (state, str(fixity[0]), fixity[1]),
                    )
                    if (
                        self.store.db.execute(
                            "SELECT count(*) FROM (SELECT 1 FROM fixities WHERE state = ? LIMIT 2)",
                            (state,),
                        ).fetchone()[0]
                        > 1
                    ):
                        self.findings.append(
                            f"{state}: conflicting primary-content descriptions; no value selected"
                        )
                with closing(
                    self.store.db.execute(
                        "SELECT value FROM maps WHERE name = 'externals' "
                        "AND json_extract(value, '$.object_id') = ?",
                        (row["id"],),
                    )
                ) as rows:
                    for (external,) in rows:
                        self.store.require_open()
                        reference = json.loads(external)
                        self.node(reference["object_id"], reference["object_type"])


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

    def _identity(self, identity: str, domain: str, value: bytes) -> None:
        old = self.db.execute(
            "SELECT domain, value FROM identities WHERE id = ?", (identity,)
        ).fetchone()
        if old is not None and old != (domain, value):
            raise ProvenanceValidationError(
                "an immutable referent or identity domain was redefined"
            )
        self.db.execute(
            "INSERT OR IGNORE INTO identities VALUES (?, ?, ?)", (identity, domain, value)
        )

    def accept(
        self,
        document: Mapping[str, Any],
        frame: Any,
        *,
        catalog: ContractCatalog,
        require_profiles: bool,
    ) -> None:
        from .journal import _body_assertions, _identity_signature

        journal_id = document["journal_id"]
        self._identity(journal_id, "journal", b"")
        if (
            self.db.execute("SELECT 1 FROM identities WHERE id = ?", (document["id"],)).fetchone()
            is not None
        ):
            raise ProvenanceValidationError("entry identity reused or identity domains overlap")
        self._identity(document["id"], "entry", frame.json_bytes)
        while self._reset_changed_assertions(row_budget=256):
            pass
        correction = document["entry_kind"] == "correction"
        if correction:
            target_ids: set[str] = set()
            for target in document["body"]["retracts"]:
                identity = target["assertion_id"]
                if identity in target_ids:
                    raise ProvenanceValidationError("duplicate correction target")
                target_ids.add(identity)
                prior = self.db.execute(
                    "SELECT sequence, active FROM assertions WHERE assertion_id = ?", (identity,)
                ).fetchone()
                if prior is None or self.frames[int(prior[0])].reference != target["entry"]:
                    raise ProvenanceValidationError(
                        "correction target has no exact earlier assertion"
                    )
                if not prior[1]:
                    raise ProvenanceValidationError(
                        "correction cannot reactivate or retract an already retired assertion"
                    )
                self.db.execute(
                    "UPDATE assertions SET active = 0 WHERE assertion_id = ?", (identity,)
                )
        for category, row in iter_assertions(_body_assertions(document)):
            if (
                self.db.execute(
                    "SELECT 1 FROM identities WHERE id = ?", (row["assertion_id"],)
                ).fetchone()
                is not None
            ):
                raise ProvenanceValidationError(
                    "assertion identities cannot be reused or overlap another domain"
                )
            self._identity(row["assertion_id"], "assertion", b"")
            self._identity(row["id"], "referent", _identity_signature(row))
            try:
                self.db.execute(
                    "INSERT INTO assertions VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (
                        row["assertion_id"],
                        row["id"],
                        category,
                        row["type"],
                        row.get("state", {}).get("object_id"),
                        row.get("observation_id"),
                        document["sequence"],
                        canonical_document(row),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ProvenanceValidationError(
                    "duplicate effective entity/record identity"
                ) from exc
            self.db.execute("INSERT INTO changes VALUES (?)", (row["assertion_id"],))
        if correction:
            for table in ("maps", "findings", "fixities", "lifecycle_extrema"):
                self.db.execute("DELETE FROM " + table)
            self.db.commit()
            for kind, run in self._dag_runs.items():
                checker = OnlineDag(self.db, kind, run).replace_run(uuid4().hex)
                self._dag_runs[kind] = checker.run_id
                while checker.retire():
                    pass
            self.db.execute(
                "INSERT OR IGNORE INTO changes SELECT assertion_id FROM assertions WHERE active = 1"
            )
        rows = (
            json.loads(value)
            for (value,) in self.db.execute(
                "SELECT assertions.value FROM changes CROSS JOIN assertions "
                "WHERE assertions.assertion_id = changes.assertion_id AND active = 1 "
                "ORDER BY changes.assertion_id"
            )
        )
        validator = _StoredValidator(
            self, rows, catalog=catalog, journal_id=journal_id, require_profiles=require_profiles
        )
        validator.run()
        recorder = self.objects.get(document["recorded_by_agent_id"])
        if recorder is None or recorder["type"] != "agent":
            raise ProvenanceValidationError("entry recorder must resolve to an effective agent")
        if "recording_context_id" in document:
            context = self.objects.get(document["recording_context_id"])
            if context is None or context["type"] != "context":
                raise ProvenanceValidationError("recording context does not resolve")

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

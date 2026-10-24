"""Atomic bounded ordering work for a fixed Native corpus edge index."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Protocol, Self, cast

from .errors import JournalValidationCapacity, ProvenanceValidationError


@dataclass(frozen=True, slots=True)
class CorpusOrderProgress:
    """One graph kind's cycle work, independently of other corpus obligations."""

    kind: str
    phase: str
    source_edges: int
    vertices: int
    completed_vertices: int
    processed_atoms: int
    admitted_octets: int
    complete: bool


@dataclass(frozen=True, slots=True)
class _State:
    phase: str = "input"
    source: str = ""
    target: str = ""
    seed: str = ""
    current: str = ""
    outgoing: str = ""
    vertices: int = 0
    completed: int = 0
    source_edges: int = 0


def _decode_count(value: str) -> int:
    return int(value)


def _encode_count(value: int) -> str:
    return str(value)


class _ByteBoundary(Exception):
    pass


@dataclass(slots=True)
class _Admission:
    limit: int
    used: int = 0

    def reserve(self, size: int) -> None:
        if size > self.limit - self.used:
            raise _ByteBoundary
        self.used += size


_FIELDS = "phase,source,target,seed,current,outgoing,vertices,completed,source_edges"
_STATE_LENGTH = "+".join("length(CAST(" + name + " AS BLOB))" for name in _FIELDS.split(","))


class _OrderDatabase(Protocol):
    def execute(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class _CorpusOrder:
    def __init__(self, source: sqlite3.Connection, directory: str, kind: str) -> None:
        self.source: _OrderDatabase = source
        self.kind = kind
        self._finished: CorpusOrderProgress | None = None
        self.scratch = TemporaryDirectory(prefix="order-", dir=directory)
        try:
            self.db: _OrderDatabase = sqlite3.connect(
                Path(self.scratch.name) / "order.sqlite3", isolation_level=None
            )
            self._write("PRAGMA cache_size=-512")
            self._write("BEGIN IMMEDIATE")
            self._write("CREATE TABLE nodes (id TEXT PRIMARY KEY, degree TEXT NOT NULL)")
            self._write("CREATE TABLE ready (id TEXT PRIMARY KEY)")
            self._write(
                "CREATE TABLE checkpoint (singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                "phase TEXT,source TEXT,target TEXT,seed TEXT,current TEXT,outgoing TEXT,"
                "vertices TEXT,completed TEXT,source_edges TEXT)"
            )
            self._write("INSERT INTO checkpoint VALUES (1,'input','','','','','','0','0','0')")
            self._write("COMMIT")
        except BaseException:
            if hasattr(self, "db"):
                self.db.close()
            self.scratch.cleanup()
            raise

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        try:
            self.db.close()
        finally:
            self.scratch.cleanup()

    @staticmethod
    def _read(
        db: _OrderDatabase, query: str, parameters: tuple[Any, ...] = ()
    ) -> tuple[Any, ...] | None:
        with closing(db.execute(query, parameters)) as rows:
            return cast(tuple[Any, ...] | None, rows.fetchone())

    def _write(self, query: str, parameters: tuple[Any, ...] = ()) -> int:
        with closing(self.db.execute(query, parameters)) as cursor:
            return cursor.rowcount

    def _payload(
        self,
        db: _OrderDatabase,
        metadata: str,
        query: str,
        parameters: tuple[Any, ...],
        admission: _Admission,
    ) -> tuple[Any, ...] | None:
        lengths = self._read(db, metadata, parameters)
        if lengths is None:
            return None
        size = sum(lengths)
        admission.reserve(size)
        value = self._read(db, query, parameters)
        if value is None or sum(len(item.encode()) for item in value) != size:
            raise RuntimeError("corpus ordering payload changed outside its fixed input")
        return value

    def _state(self, admission: _Admission) -> _State:
        value = self._payload(
            self.db,
            "SELECT " + _STATE_LENGTH + " FROM checkpoint WHERE singleton=1",
            "SELECT " + _FIELDS + " FROM checkpoint WHERE singleton=1",
            (),
            admission,
        )
        if value is None:
            raise RuntimeError("corpus ordering checkpoint is missing")
        return _State(*value[:6], *(_decode_count(item) for item in value[6:]))

    def _save(self, state: _State) -> None:
        self._write(
            "UPDATE checkpoint SET phase=?,source=?,target=?,seed=?,current=?,outgoing=?,"
            "vertices=?,completed=?,source_edges=? WHERE singleton=1",
            (
                state.phase,
                state.source,
                state.target,
                state.seed,
                state.current,
                state.outgoing,
                _encode_count(state.vertices),
                _encode_count(state.completed),
                _encode_count(state.source_edges),
            ),
        )

    def _degree(self, identity: str, admission: _Admission) -> int | None:
        found = self._payload(
            self.db,
            "SELECT length(CAST(degree AS BLOB)) FROM nodes WHERE id=?",
            "SELECT degree FROM nodes WHERE id=?",
            (identity,),
            admission,
        )
        return None if found is None else _decode_count(found[0])

    def _next(self, state: _State, admission: _Admission) -> _State:
        if state.phase == "input":
            predicate = (
                " FROM edges WHERE kind=? AND (source,target)>(?,?) ORDER BY source,target LIMIT 1"
            )
            found = self._payload(
                self.source,
                "SELECT length(CAST(source AS BLOB)),length(CAST(target AS BLOB))" + predicate,
                "SELECT source, target" + predicate,
                (self.kind, state.source, state.target),
                admission,
            )
            if found is None:
                return replace(state, phase="seed")
            source, target = found
            degree = self._degree(target, admission)
            added = self._write("INSERT OR IGNORE INTO nodes VALUES (?, '0')", (source,))
            if degree is None and target != source:
                self._write("INSERT INTO nodes VALUES (?, '1')", (target,))
                added += 1
            else:
                self._write(
                    "UPDATE nodes SET degree=? WHERE id=?",
                    (_encode_count((degree or 0) + 1), target),
                )
            return replace(
                state,
                source=source,
                target=target,
                vertices=state.vertices + added,
                source_edges=state.source_edges + 1,
            )
        if state.phase == "seed":
            predicate = " FROM nodes WHERE id>? ORDER BY id LIMIT 1"
            found = self._payload(
                self.db,
                "SELECT length(CAST(id AS BLOB)),length(CAST(degree AS BLOB))" + predicate,
                "SELECT id, degree" + predicate,
                (state.seed,),
                admission,
            )
            if found is None:
                return replace(state, phase="ready")
            identity, degree = found
            if degree == "0":
                self._write("INSERT INTO ready VALUES (?)", (identity,))
            return replace(state, seed=identity)
        if state.phase == "ready":
            found = self._payload(
                self.db,
                "SELECT length(CAST(id AS BLOB)) FROM ready ORDER BY id LIMIT 1",
                "SELECT id FROM ready ORDER BY id LIMIT 1",
                (),
                admission,
            )
            if found is None:
                if state.completed != state.vertices:
                    raise ProvenanceValidationError(
                        f"cross-journal {self.kind} graph contains a cycle"
                    )
                return replace(state, phase="complete")
            self._write("DELETE FROM ready WHERE id=?", (found[0],))
            return replace(state, phase="outgoing", current=found[0], outgoing="")
        if state.phase != "outgoing":
            raise RuntimeError("invalid corpus ordering phase")
        predicate = " FROM edges WHERE kind=? AND source=? AND target>? ORDER BY target LIMIT 1"
        found = self._payload(
            self.source,
            "SELECT length(CAST(target AS BLOB))" + predicate,
            "SELECT target" + predicate,
            (self.kind, state.current, state.outgoing),
            admission,
        )
        if found is None:
            return replace(
                state, phase="ready", current="", outgoing="", completed=state.completed + 1
            )
        target = found[0]
        degree = self._degree(target, admission)
        if degree is None or degree < 1:
            raise RuntimeError("corpus ordering indegree is inconsistent")
        remaining = _encode_count(degree - 1)
        self._write("UPDATE nodes SET degree=? WHERE id=?", (remaining, target))
        if remaining == "0":
            self._write("INSERT INTO ready VALUES (?)", (target,))
        return replace(state, outgoing=target)

    def _progress(self, state: _State, processed: int, admitted: int) -> CorpusOrderProgress:
        return CorpusOrderProgress(
            self.kind,
            state.phase,
            state.source_edges,
            state.vertices,
            state.completed,
            processed,
            admitted,
            state.phase == "complete",
        )

    def advance(self, *, row_budget: int, byte_budget: int) -> CorpusOrderProgress:
        if type(row_budget) is not int or row_budget < 1:
            raise ValueError("corpus ordering row budget must be positive")
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("corpus ordering byte budget must be positive")
        if self._finished is not None:
            return replace(self._finished, processed_atoms=0, admitted_octets=0)
        admission = _Admission(byte_budget)
        try:
            before = self._state(admission)
        except _ByteBoundary as error:
            raise JournalValidationCapacity(
                "ordering checkpoint exceeds admitted byte budget"
            ) from error
        state, processed = before, 0
        if state.phase != "complete":
            self._write("BEGIN IMMEDIATE")
            try:
                for _ in range(row_budget):
                    try:
                        after = self._next(state, admission)
                    except _ByteBoundary as error:
                        if processed == 0:
                            raise JournalValidationCapacity(
                                "ordering checkpoint and atom exceed admitted byte budget"
                            ) from error
                        break
                    state, processed = after, processed + 1
                    if state.phase == "complete":
                        break
                self._save(state)
                self._write("COMMIT")
            except BaseException:
                self.db.rollback()
                raise
        progress = self._progress(state, processed, admission.used)
        if progress.complete:
            self._finished = progress
        return progress

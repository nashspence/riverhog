"""Bounded immutable generation objects in their owner's original private epoch.

This port opens no file, owns no connection and selects no recovery frontier.
The generation owner admits storage and the work envelope before application,
retains candidate/reader roots before collection, and receives its actual epoch.
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Literal

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

from ._generation_page_map import (
    _DEPTH,
    _DOMAINS,
    _FANOUT,
    _HEADER,
    _MAX_PAGE,
    _NODE_OCTETS,
    _ZERO,
    GenerationPageMap,
    InvalidGenerationMap,
)
from ._native_epoch import EpochContext
from ._native_work import WorkAmount

_MAX_REFERENCES = (1 << 63) - 1
type _Kind = Literal["node", "page"]


@dataclass(slots=True)
class GenerationObjectWork:
    sql_calls: int = 0
    loaded_octets: int = 0
    offered_octets: int = 0
    hashed_octets: int = 0
    child_slots: int = 0
    reference_updates: int = 0
    objects_created: int = 0
    objects_deleted: int = 0
    garbage_visits: int = 0


def _growth(mode: str) -> Declaration:
    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def width(extra: int = 0) -> Expression:
        return Expression(
            "product",
            terms=(
                dim("units"),
                Expression(
                    "sum",
                    terms=(
                        dim("object_octets"),
                        Expression("constant", value=extra, unit=OCTETS_PER_COUNT),
                    ),
                ),
            ),
        )

    sql = {"initialize": 2, "load": 1, "store": 18, "retain": 1, "release": 2, "collect": 36}
    reads = int(mode in {"load", "store", "collect"})
    metrics = {
        "sql_calls": (COUNT, scale(sql[mode], dim("units"))),
        "loaded_octets": (OCTETS, scale(reads, width(1))),
        "offered_octets": (OCTETS, scale(int(mode == "store"), width())),
        "hashed_octets": (
            OCTETS,
            scale(2 if mode == "store" else reads, width(max(map(len, _DOMAINS.values())))),
        ),
        "child_slots": (COUNT, scale(64 if mode == "store" else 32 if reads else 0, dim("units"))),
        "reference_updates": (
            COUNT,
            scale(
                16 if mode in {"store", "collect"} else int(mode in {"retain", "release"}),
                dim("units"),
            ),
        ),
        "objects_created": (COUNT, scale(int(mode == "store"), dim("units"))),
        "objects_deleted": (COUNT, scale(int(mode == "collect"), dim("units"))),
        "garbage_visits": (COUNT, scale(int(mode == "collect"), dim("units"))),
    }
    return Declaration(
        id=f"riverhog-provenance.generation-object-{mode}.growth/v1",
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance._generation_objects.GenerationObjectStore.{mode}",),
        scope="original-epoch-native-generation-object-storage",
        starts_at="receive original transaction, bounded object width and work admission",
        ends_at="return bounded object work or propagate the original failure",
        dimensions=(
            Dimension(
                "units", COUNT, "One original operation or the admitted collector visit limit."
            ),
            Dimension(
                "object_octets",
                OCTETS_PER_COUNT,
                "Maximum of the SQLite page width and fixed node width.",
            ),
        ),
        bounds=tuple(
            Bound(
                name,
                unit,
                "upper",
                bound,
                "Fixed point SQL and at most sixteen original child edges per object.",
            )
            for name, (unit, bound) in metrics.items()
        ),
        conditions=(
            "The caller retains the original epoch and root/reader references "
            "through actual receiving.",
        ),
        invariants=(
            "No object enumeration or retained-prefix scan is required by an admitted operation.",
        ),
        limitations=(
            "SQL-call counts do not bound SQLite B-tree instructions, physical I/O "
            "or synchronization.",
            "The port neither creates optional storage nor selects or verifies "
            "a complete recovery generation.",
        ),
    )


class GenerationObjectStore:
    """Immutable object references borrowed from one admitted generation epoch.

    Newly interned, unreferenced objects enter the garbage queue immediately.
    Retaining a root and changing its references belong to the same transaction
    as the generation metadata. Collection visits only its admitted queue count;
    a positive reference protects that object and its original child references.
    A failed operation must escape the owning epoch so its partial writes roll
    back and the disposable executor cannot infer success from an empty writer.
    """

    def __init__(self, epoch: EpochContext, *, page_size: int) -> None:
        if not isinstance(epoch, EpochContext):
            raise TypeError("generation objects require their original private epoch")
        if (
            type(page_size) is not int
            or not 512 <= page_size <= 65536
            or page_size & (page_size - 1)
        ):
            raise ValueError("generation objects require an exact SQLite page size")
        self.epoch, self.db, self.page_size = epoch, epoch.db, page_size
        self.work = GenerationObjectWork()

    def _require(self) -> None:
        self.epoch.require_usable()
        self.epoch.require(self.db)
        self.epoch.check()

    def _execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        self.epoch.check()
        self.epoch.account(WorkAmount(rows=1))
        self.work.sql_calls += 1
        return self.db.execute(sql, parameters)

    def _octets(self, count: int) -> None:
        self.epoch.account(WorkAmount(octets=count))

    @declare_growth(_growth("initialize"))
    def initialize(self) -> None:
        """Create only in the original new-store construction transaction."""
        self._require()
        self._execute(
            "CREATE TABLE generation_objects(address TEXT PRIMARY KEY,kind TEXT NOT NULL,"
            "body BLOB NOT NULL,refs INTEGER NOT NULL CHECK(refs>=0))"
        ).close()
        self._execute("CREATE TABLE generation_garbage(address TEXT PRIMARY KEY)").close()

    def _children(self, kind: _Kind, body: bytes) -> tuple[str, ...]:
        if kind == "page":
            if len(body) != self.page_size:
                raise InvalidGenerationMap("generation object page width differs")
            return ()
        if len(body) != _NODE_OCTETS:
            raise InvalidGenerationMap("generation object node shape differs")
        level, count = _HEADER.unpack_from(body)
        if not 0 <= level < _DEPTH or not 1 <= count <= min(_MAX_PAGE, _FANOUT ** (_DEPTH - level)):
            raise InvalidGenerationMap("generation object node level or coverage differs")
        slots = tuple(body[offset : offset + 32] for offset in range(_HEADER.size, len(body), 32))
        self.work.child_slots += 2 * _FANOUT
        children = tuple(slot.hex() for slot in slots if slot != _ZERO)
        if (
            not children
            or count < len(children)
            or (level == _DEPTH - 1 and count != len(children))
        ):
            raise InvalidGenerationMap("generation object child coverage differs")
        return children

    def _verify(self, address: str, kind: str, body: bytes) -> tuple[_Kind, tuple[str, ...]]:
        GenerationPageMap._address(address)
        if kind not in _DOMAINS:
            raise InvalidGenerationMap("generation object kind differs")
        selected: _Kind = "node" if kind == "node" else "page"
        prefix = _DOMAINS[selected]
        self._octets(len(prefix) + len(body))
        self.work.hashed_octets += len(prefix) + len(body)
        if hashlib.sha256(prefix + body).hexdigest() != address:
            raise InvalidGenerationMap("generation object digest differs")
        return selected, self._children(selected, body)

    def _object(self, address: str, maximum: int) -> tuple[_Kind, bytes, int, tuple[str, ...]]:
        GenerationPageMap._address(address)
        with closing(
            self._execute(
                "SELECT substr(kind,1,5),typeof(body),length(body),substr(body,1,?),refs "
                "FROM generation_objects WHERE address=?",
                (maximum + 1, address),
            )
        ) as cursor:
            row = cursor.fetchone()
        if row is None:
            raise InvalidGenerationMap("original generation object is missing")
        kind, storage_type, size, body, references = row
        if type(body) is bytes:
            self._octets(len(body))
            self.work.loaded_octets += len(body)
        if (
            storage_type != "blob"
            or type(body) is not bytes
            or type(size) is not int
            or size != len(body)
            or size > maximum
            or type(references) is not int
            or not 0 <= references <= _MAX_REFERENCES
        ):
            raise InvalidGenerationMap("generation object exceeds its admitted shape or extent")
        selected, children = self._verify(address, kind, body)
        return selected, body, references, children

    @declare_growth(_growth("load"))
    def load(self, address: str, *, max_octets: int) -> tuple[str, bytes]:
        """Materialize at most the exact original bound plus one refusal byte."""
        if type(max_octets) is not int or not 1 <= max_octets <= max(_NODE_OCTETS, self.page_size):
            raise ValueError("generation load requires bounded original object admission")
        self.epoch.require_usable()
        if self.db.in_transaction:
            self._require()
            kind, body, _references, _children = self._object(address, max_octets)
        else:
            self.epoch.require_quiescent()
            with self.epoch.preparing():
                kind, body, _references, _children = self._object(address, max_octets)
        return kind, body

    def _retain(self, address: str, kind: _Kind | None = None) -> None:
        GenerationPageMap._address(address)
        with closing(
            self._execute(
                "UPDATE generation_objects SET refs=refs+1 WHERE address=? "
                "AND typeof(refs)='integer' AND refs>=0 AND refs<? "
                "AND (? IS NULL OR kind=?)",
                (address, _MAX_REFERENCES, kind, kind),
            )
        ) as cursor:
            if cursor.rowcount != 1:
                raise InvalidGenerationMap("cannot retain original generation dependency")
        self.work.reference_updates += 1

    def _release(self, address: str) -> None:
        GenerationPageMap._address(address)
        with closing(
            self._execute(
                "UPDATE generation_objects SET refs=refs-1 WHERE address=? "
                "AND typeof(refs)='integer' AND refs>0",
                (address,),
            )
        ) as cursor:
            if cursor.rowcount != 1:
                raise InvalidGenerationMap("original generation reference is unavailable")
        self.work.reference_updates += 1
        self._execute(
            "INSERT INTO generation_garbage SELECT address FROM generation_objects "
            "WHERE address=? AND refs=0 ON CONFLICT(address) DO NOTHING",
            (address,),
        ).close()

    @declare_growth(_growth("store"))
    def store(self, address: str, kind: _Kind, body: bytes, children: tuple[str, ...]) -> None:
        """Intern one bounded image and its exact encoded child edges atomically."""
        self._require()
        if type(body) is not bytes or len(body) > max(_NODE_OCTETS, self.page_size):
            raise InvalidGenerationMap("generation object offer exceeds its admitted extent")
        selected, actual = self._verify(address, kind, body)
        if type(children) is not tuple or children != actual:
            raise InvalidGenerationMap("generation object edges differ from its original image")
        self._octets(len(body))
        self.work.offered_octets += len(body)
        with closing(
            self._execute(
                "INSERT INTO generation_objects VALUES(?,?,?,0) ON CONFLICT(address) DO NOTHING",
                (address, selected, body),
            )
        ) as cursor:
            inserted = cursor.rowcount == 1
        if not inserted:
            previous_kind, previous_body, _references, _children = self._object(address, len(body))
            if previous_kind != selected or previous_body != body:
                raise InvalidGenerationMap("immutable generation object differs")
            return
        self.work.objects_created += 1
        self._execute("INSERT INTO generation_garbage VALUES(?)", (address,)).close()
        child_kind: _Kind = "page" if selected == "node" and body[0] == _DEPTH - 1 else "node"
        for child in actual:
            self._retain(child, child_kind)

    @declare_growth(_growth("retain"))
    def retain(self, address: str) -> None:
        """Retain an exact candidate or original reader root in its owner epoch."""
        self._require()
        self._retain(address, "node")

    @declare_growth(_growth("release"))
    def release(self, address: str) -> None:
        """Receive only a reference the generation owner has actually relinquished."""
        self._require()
        self._release(address)

    @declare_growth(_growth("collect"))
    def collect(self, *, limit: int) -> int:
        """Visit at most limit queued objects, including stale positive references."""
        self._require()
        if type(limit) is not int or limit < 1:
            raise ValueError("generation collection requires a positive original visit admission")
        deleted = 0
        for _ in range(limit):
            with closing(
                self._execute(
                    "SELECT substr(g.address,1,65),o.refs FROM generation_garbage AS g "
                    "LEFT JOIN generation_objects AS o ON o.address=g.address LIMIT 1"
                )
            ) as cursor:
                queued = cursor.fetchone()
            if queued is None:
                break
            self.work.garbage_visits += 1
            address, references = queued
            GenerationPageMap._address(address)
            if type(references) is not int or not 0 <= references <= _MAX_REFERENCES:
                raise InvalidGenerationMap(
                    "generation garbage lost its original reference metadata"
                )
            self._execute("DELETE FROM generation_garbage WHERE address=?", (address,)).close()
            if references:
                continue
            _kind, _body, _references, children = self._object(
                address, max(_NODE_OCTETS, self.page_size)
            )
            for child in children:
                self._release(child)
            with closing(
                self._execute(
                    "DELETE FROM generation_objects WHERE address=? AND refs=0",
                    (address,),
                )
            ) as cursor:
                if cursor.rowcount != 1:
                    raise InvalidGenerationMap(
                        "generation collection lost its original zero reference"
                    )
            self.work.objects_deleted += 1
            deleted += 1
        return deleted

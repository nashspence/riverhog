"""Durable root references and original bounded reader lifetime.

Rows retain immutable page-map roots. Original live reader handles block row
retirement; they are not durable pin records or evidence of Core selection.
The composing generation owner supplies exact grant identities, seals the full
continuation separately, and receives the workspace before another process opens it.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from threading import get_ident
from typing import TYPE_CHECKING, Any

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

from ._generation_objects import GenerationObjectStore, GenerationObjectWork
from ._generation_page_map import (
    GenerationMapWork,
    GenerationPageMap,
    GenerationPageRead,
    InvalidGenerationMap,
)
from ._native_epoch import EpochContext
from ._native_storage import NativeStorageLease
from ._native_work import WorkAmount, WorkQuantum
from .errors import JournalValidationCapacity

if TYPE_CHECKING:
    from ._generation_storage import NativeGenerationStorage


@dataclass(slots=True)
class GenerationRootWork:
    sql_calls: int = 0
    loaded_octets: int = 0
    stored_octets: int = 0
    reader_visits: int = 0


def _growth(mode: str) -> Declaration:
    units = Expression("dimension", name="units")
    width = Expression("dimension", name="metadata_octets")

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    reads = int(mode not in {"initialize", "begin"})
    # A base-root lookup is optional at begin. Every row projects only its
    # root/status (64 + 9 UTF-8 bytes), plus one refusal byte for each.
    reads = 1 if mode == "begin" else reads
    stored = int(mode in {"begin", "replace", "freeze"})
    metrics = {
        "sql_calls": (COUNT, scale(1 if mode in {"initialize", "borrow"} else 2, units)),
        "loaded_octets": (
            OCTETS,
            scale(
                75 * reads,
                Expression(
                    "product", terms=(units, Expression("constant", value=1, unit=OCTETS_PER_COUNT))
                ),
            ),
        ),
        "stored_octets": (OCTETS, scale(stored, Expression("product", terms=(units, width)))),
        "reader_visits": (
            COUNT,
            scale(int(mode == "retire"), Expression("dimension", name="reader_limit")),
        ),
    }
    return Declaration(
        id=f"riverhog-provenance.generation-root-{mode}.growth/v1",
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance._generation_roots.GenerationRoots.{mode}",),
        scope="original-generation-root-and-reader-lifetime",
        starts_at="receive original gated epoch and explicit root/reader metadata admission",
        ends_at="return root work or preserve the original failure and resource handles",
        dimensions=(
            Dimension("units", COUNT, "One original root operation attempted."),
            Dimension(
                "metadata_octets", OCTETS_PER_COUNT, "Admitted identity/root/status bytes per row."
            ),
            Dimension("reader_limit", COUNT, "Original simultaneously retained reader admission."),
        ),
        bounds=tuple(
            Bound(
                name,
                unit,
                "upper",
                value,
                "Point metadata SQL and bounded original-reader inspection.",
            )
            for name, (unit, value) in metrics.items()
        ),
        conditions=("Map/object work retains its own complete delegated bounds.",),
        invariants=("No historical generation enumeration or durable reader-pin transaction.",),
        limitations=(
            "SQL-call counts exclude SQLite B-tree instructions, physical I/O and synchronization.",
            "An immutable root is not a complete continuation or a selected recovery frontier.",
        ),
    )


class GenerationRoots:
    """Original root rows; serialization and resources remain with their storage.

    Mutations run inside the storage's gated original epoch. A failed epoch
    cannot be reused. Borrowing and reader operations use that same gate, so
    retirement cannot race a page read or an uncertain last-reader close.
    Strong references retain failed original readers within explicit capacity.
    """

    def __init__(
        self, storage: NativeGenerationStorage, *, reader_limit: int, metadata_octets: int
    ) -> None:
        if any(type(value) is not int or value < 1 for value in (reader_limit, metadata_octets)):
            raise ValueError("generation roots require positive original resource admission")
        self.storage = storage
        self.epoch = storage.epoch
        self.db = self.epoch.db
        self.reader_limit, self.metadata_octets = reader_limit, metadata_octets
        self.map = GenerationPageMap(storage.objects, page_size=storage.objects.page_size)
        self.work = GenerationRootWork()
        self._readers: dict[GenerationReader, str] = {}

    def _require(self) -> None:
        self.storage._require_operation()
        self.epoch.require(self.db)
        self.epoch.check()

    def _execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        self.storage._require_operation()
        self.epoch.check()
        self.epoch.account(WorkAmount(rows=1))
        self.work.sql_calls += 1
        return self.db.execute(sql, parameters)

    def _identity(self, identity: str) -> int:
        if type(identity) is not str or not identity:
            raise ValueError("generation requires its exact original grant identity")
        # Check the character bound before allocating an encoded copy. Every
        # UTF-8 scalar has at least one byte; no identity is narrowed to INTEGER.
        if len(identity) > self.metadata_octets:
            raise JournalValidationCapacity("optional generation metadata admission exhausted")
        size = len(identity.encode("utf-8"))
        if size > self.metadata_octets:
            raise JournalValidationCapacity("optional generation metadata admission exhausted")
        return size

    def _stored(self, identity: str, root: str | None, status: str) -> None:
        size = self._identity(identity) + (0 if root is None else 64) + len(status)
        if size > self.metadata_octets:
            raise JournalValidationCapacity("optional generation metadata admission exhausted")
        self.epoch.account(WorkAmount(octets=size))
        self.work.stored_octets += size

    def _get(self, identity: str) -> tuple[str | None, str]:
        self._identity(identity)
        with closing(
            self._execute(
                "SELECT typeof(root),substr(CAST(root AS BLOB),1,65),"
                "typeof(status),substr(CAST(status AS BLOB),1,10) "
                "FROM generation_roots WHERE identity=?",
                (identity,),
            )
        ) as cursor:
            row = cursor.fetchone()
        if row is None:
            raise InvalidGenerationMap("original generation root is missing")
        root_type, encoded_root, status_type, encoded_status = row
        size = sum(len(item) for item in (encoded_root, encoded_status) if type(item) is bytes)
        self.epoch.account(WorkAmount(octets=size))
        self.work.loaded_octets += size
        if root_type == "null" and encoded_root is None:
            root = None
        elif root_type == "text" and type(encoded_root) is bytes:
            try:
                root = encoded_root.decode("ascii")
            except UnicodeDecodeError as error:
                raise InvalidGenerationMap("generation root address differs") from error
            GenerationPageMap._address(root)
        else:
            raise InvalidGenerationMap("generation root storage type differs")
        if status_type != "text" or encoded_status not in {b"building", b"immutable"}:
            raise InvalidGenerationMap("generation root status differs")
        return root, encoded_status.decode("ascii")

    @declare_growth(_growth("initialize"))
    def initialize(self) -> None:
        self._require()
        self._execute(
            "CREATE TABLE generation_roots(identity TEXT PRIMARY KEY,root TEXT,"
            "status TEXT NOT NULL CHECK(status IN ('building','immutable')),"
            "envelope BLOB,envelope_sha256 TEXT)"
        ).close()

    @declare_growth(_growth("begin"))
    def begin(self, identity: str, *, base: str | None = None) -> None:
        """Retain an exact existing root in the same transaction as its new row."""
        self._require()
        self._identity(identity)
        root = None
        if base is not None:
            root, status = self._get(base)
            if status != "immutable":
                raise InvalidGenerationMap("generation base root is not immutable")
        self._stored(identity, root, "building")
        if root is not None:
            self.storage.objects.retain(root)
        self._execute(
            "INSERT INTO generation_roots(identity,root,status) VALUES(?,?,'building')",
            (identity, root),
        ).close()

    @declare_growth(_growth("replace"))
    def replace(self, identity: str, page: int, data: bytes | None) -> str | None:
        """Stage one admitted change; the row always retains its actual root."""
        self._require()
        previous, status = self._get(identity)
        if status != "building":
            raise InvalidGenerationMap("immutable generation root cannot change")
        root = self.map.replace(previous, page, data)
        self._stored(identity, root, status)
        if root is not None:
            self.storage.objects.retain(root)
        if previous is not None:
            self.storage.objects.release(previous)
        self._execute(
            "UPDATE generation_roots SET root=? WHERE identity=?", (root, identity)
        ).close()
        return root

    @declare_growth(_growth("freeze"))
    def freeze(self, identity: str) -> str:
        """Make the root immutable; full envelope/coverage validation stays with its owner."""
        self._require()
        root, status = self._get(identity)
        if root is None or status != "building":
            raise InvalidGenerationMap("generation root is empty or already immutable")
        self._stored(identity, root, "immutable")
        self._execute(
            "UPDATE generation_roots SET status='immutable' WHERE identity=?", (identity,)
        ).close()
        return root

    @declare_growth(_growth("retire"))
    def retire(
        self,
        identity: str,
        *,
        check: Callable[[], None] | None = None,
        work: WorkQuantum | None = None,
    ) -> None:
        """Receive a row only after every original reader of that row has received."""
        self._identity(identity)
        with self.storage.execution(check, work=work):
            for reader_identity in self._readers.values():
                self.epoch.check()
                self.epoch.account(WorkAmount(rows=1))
                self.work.reader_visits += 1
                if reader_identity == identity:
                    raise RuntimeError("original generation reader is not received")

            def retire(epoch: EpochContext) -> None:
                self._require()
                root, _status = self._get(identity)
                self._execute("DELETE FROM generation_roots WHERE identity=?", (identity,)).close()
                if root is not None:
                    self.storage.objects.release(root)

            self.epoch.apply(
                retire, receive=lambda result: None, admission=WorkAmount(rows=4, octets=75)
            )

    @declare_growth(_growth("borrow"))
    def borrow(
        self,
        identity: str,
        *,
        expected_root: str,
        check: Callable[[], None] | None = None,
        work: WorkQuantum | None = None,
    ) -> GenerationReader:
        """Borrow an exact immutable root; no cached positive authorization is imported."""
        GenerationPageMap._address(expected_root)
        if expected_root is None:
            raise ValueError("generation reader requires an exact immutable root")
        with self.storage.execution(check, work=work):
            if len(self._readers) >= self.reader_limit:
                raise JournalValidationCapacity("optional generation reader admission exhausted")
            root, status = self._get(identity)
            if root is None or root != expected_root or status != "immutable":
                raise InvalidGenerationMap("generation reader root or status differs")
            reader = GenerationReader(self, root)
            # Allocate and register the original handle before retaining the
            # shared resource. Retain refuses before effects; after it returns
            # only scalar assignments remain before handing back the handle.
            self._readers[reader] = identity
            try:
                self.epoch.check()
                self.storage._storage.retain(self.storage)
            except BaseException:
                del self._readers[reader]
                raise
            reader._retained = True
            return reader

    @property
    def readers(self) -> tuple[GenerationReader, ...]:
        """Bounded original handles, including an uncertain borrow/close outcome."""
        with self.storage._operation():
            return tuple(self._readers)

    @property
    def dependents_received(self) -> bool:
        """Safe inside the final callback, before the final lease's closed flag."""

        def received() -> bool:
            return all(reader.dependents_received for reader in self._readers)

        if self.storage._operation_thread == get_ident():
            return received()
        try:
            with self.storage._operation():
                return received()
        except RuntimeError:
            return False


class GenerationReader:
    """An independently owned, explicitly received reader of one immutable root."""

    def __init__(self, roots: GenerationRoots, root: str) -> None:
        self.roots, self._root = roots, root
        # Read-only work has its own original epoch, sharing the admitted
        # connection only under the storage gate. A later failed candidate
        # writer cannot poison already retained immutable reader control.
        self._epoch = EpochContext(roots.db)
        self._objects = GenerationObjectStore(
            self._epoch, page_size=roots.storage.objects.page_size
        )
        self._map = GenerationPageMap(self._objects, page_size=self._objects.page_size)
        self._lease = NativeStorageLease(roots.storage._storage)
        self._retained = False
        self._closing = False

    @property
    def root(self) -> str:
        return self._root

    @property
    def object_work(self) -> GenerationObjectWork:
        return self._objects.work

    @property
    def map_work(self) -> GenerationMapWork:
        return self._map.work

    @property
    def closed(self) -> bool:
        return self._retained and self._lease.closed

    @property
    def dependents_received(self) -> bool:
        return self.closed or (self._closing and self.roots.storage._storage._database_closed)

    def read(
        self,
        *,
        after: int,
        page_limit: int,
        check: Callable[[], None] | None = None,
        work: WorkQuantum | None = None,
    ) -> GenerationPageRead:
        with self.roots.storage._operation():
            if not self._retained or self._closing or self.closed:
                raise RuntimeError("generation reader is closed")
            with self._epoch.execution(check, work=work):
                result = self._map.read(self.root, after=after, page_limit=page_limit)
                self._epoch.check()
                return result

    def close(self) -> None:
        if self.closed:
            return
        self._closing = True
        with self.roots.storage._operation():
            if self.closed:
                return
            if not self._retained:
                raise RuntimeError("generation reader has no original storage lease")
            self._lease.close()
            # Failed last-close/callback leaves this exact strong handle in the
            # bounded original registry. No retirement is inferred from intent.
            del self.roots._readers[self]

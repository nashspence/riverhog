"""Executable storage reference, not a Riverhog runtime or authority implementation.

Immutable SQLite page images are addressed by a fixed-depth Merkle radix map.
Only changed paths are copied. SQLite persists candidate dependencies separately
from the application's selection transaction. Callers supply already admitted,
quiescent page changes and retain original resource ownership outside this model.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import sqlite3
import struct
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEPTH = 8
FANOUT = 16
MAX_PAGE = (1 << 32) - 2
ZERO = bytes(32)
NODE_HEADER = struct.Struct(">BQ")
FORMAT = "native-page-generation-reference/v1"


class InvalidGeneration(ValueError):
    """Derived recovery state is incomplete, incompatible or corrupt."""


class LineageRefused(ValueError):
    """The proposed candidate would branch or outrun an unresolved candidate."""


@dataclass(frozen=True)
class Descriptor:
    store_id: str
    generation: int
    manifest_sha256: str


@dataclass
class Counters:
    page_bytes_written: int = 0
    page_objects_created: int = 0
    node_objects_created: int = 0
    node_reads: int = 0
    objects_restored: int = 0
    bytes_restored: int = 0
    objects_collected: int = 0
    child_edges_received: int = 0


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(kind: str, body: bytes) -> str:
    return hashlib.sha256(kind.encode() + b"\x00" + body).hexdigest()


class GenerationStore:
    """One owner-private SQLite object store, with one linear candidate at a time.

    Direct sqlite3/path use is intentionally reference-only. Production must use
    the original admitted connection, namespace and receiving frame. No method
    here authorizes source access or selects a Core recovery frontier.
    """

    def __init__(self, path: Path, *, create: bool = False) -> None:
        if create and path.exists():
            raise FileExistsError(path)
        if not create and not path.is_file():
            raise FileNotFoundError(path)
        self.db = sqlite3.connect(path, isolation_level=None)
        self.counters = Counters()
        try:
            mode = self.db.execute("PRAGMA journal_mode=DELETE").fetchone()
            if mode != ("delete",):
                raise InvalidGeneration("required retained journal mode unavailable")
            self.db.execute("PRAGMA synchronous=EXTRA")
            if self.db.execute("PRAGMA synchronous").fetchone() != (3,):
                raise InvalidGeneration("required retained synchronization unavailable")
            if create:
                self.db.executescript(
                    """
                    CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                    CREATE TABLE objects(
                        address TEXT PRIMARY KEY, kind TEXT NOT NULL,
                        body BLOB NOT NULL, refs INTEGER NOT NULL CHECK(refs >= 0)
                    );
                    CREATE TABLE garbage(address TEXT PRIMARY KEY);
                    CREATE TABLE generations(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        root TEXT, status TEXT NOT NULL,
                        page_size INTEGER NOT NULL, binding BLOB NOT NULL,
                        manifest BLOB, manifest_sha256 TEXT,
                        pins INTEGER NOT NULL DEFAULT 0 CHECK(pins >= 0)
                    );
                    CREATE TABLE pins(token TEXT PRIMARY KEY, generation INTEGER NOT NULL);
                    """
                )
                self.db.executemany(
                    "INSERT INTO metadata VALUES(?,?)",
                    [("format", FORMAT), ("store_id", uuid.uuid4().hex), ("tip", "0")],
                )
            if self._meta("format") != FORMAT:
                raise InvalidGeneration("incompatible optional store")
            self.store_id = self._meta("store_id")
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _meta(self, key: str) -> str:
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        if row is None or not isinstance(row[0], str):
            raise InvalidGeneration("missing store identity")
        return row[0]

    @property
    def tip(self) -> int:
        return int(self._meta("tip"))

    def _object(self, address: str, expected_kind: str) -> bytes:
        row = self.db.execute(
            "SELECT kind,body FROM objects WHERE address=?", (address,)
        ).fetchone()
        if row is None or row[0] != expected_kind or not isinstance(row[1], bytes):
            raise InvalidGeneration("missing or wrong-kind generation dependency")
        body = row[1]
        if digest(expected_kind, body) != address:
            raise InvalidGeneration("generation dependency digest mismatch")
        return body

    def _node(self, address: str, level: int) -> tuple[int, list[str | None]]:
        body = self._object(address, "node")
        self.counters.node_reads += 1
        if len(body) != NODE_HEADER.size + FANOUT * 32:
            raise InvalidGeneration("malformed radix node")
        encoded_level, count = NODE_HEADER.unpack_from(body)
        if encoded_level != level or not 0 < count <= MAX_PAGE:
            raise InvalidGeneration("wrong radix level or extent")
        children: list[str | None] = []
        for offset in range(NODE_HEADER.size, len(body), 32):
            child = body[offset : offset + 32]
            children.append(None if child == ZERO else child.hex())
        return count, children

    def _retain(self, address: str | None) -> None:
        if address is not None:
            cursor = self.db.execute("UPDATE objects SET refs=refs+1 WHERE address=?", (address,))
            if cursor.rowcount != 1:
                raise InvalidGeneration("cannot retain missing dependency")

    def _release(self, address: str | None) -> None:
        if address is not None:
            cursor = self.db.execute(
                "UPDATE objects SET refs=refs-1 WHERE address=? AND refs>0", (address,)
            )
            if cursor.rowcount != 1:
                raise InvalidGeneration("missing original dependency reference")
            self.db.execute(
                "INSERT OR IGNORE INTO garbage "
                "SELECT address FROM objects WHERE address=? AND refs=0",
                (address,),
            )

    def _intern(self, kind: str, body: bytes, children: Iterable[str | None] = ()) -> str:
        address = digest(kind, body)
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO objects VALUES(?,?,?,0)", (address, kind, body)
        )
        if cursor.rowcount:
            if kind == "page":
                self.counters.page_bytes_written += len(body)
                self.counters.page_objects_created += 1
            else:
                self.counters.node_objects_created += 1
            for child in children:
                self._retain(child)
        else:
            self._object(address, kind)
        return address

    def _replace(self, root: str | None, level: int, page: int, image: str | None) -> str | None:
        children: list[str | None] = [None] * FANOUT if root is None else self._node(root, level)[1]
        position = (page >> (4 * (DEPTH - 1 - level))) & 15
        if level == DEPTH - 1:
            children[position] = image
        else:
            children[position] = self._replace(children[position], level + 1, page, image)
        count = 0
        for child in children:
            if child is not None:
                count += 1 if level == DEPTH - 1 else self._node(child, level + 1)[0]
        if count == 0:
            return None
        body = NODE_HEADER.pack(level, count) + b"".join(
            ZERO if child is None else bytes.fromhex(child) for child in children
        )
        return self._intern("node", body, children)

    def begin(self, *, base: int, page_size: int, binding: Mapping[str, Any]) -> int:
        if page_size < 512 or page_size > 65536 or page_size & (page_size - 1):
            raise ValueError("invalid SQLite page size")
        with self._transaction():
            if base != self.tip:
                raise LineageRefused("fallback replay may not inherit a newer page lineage")
            if self.db.execute(
                "SELECT 1 FROM generations WHERE status IN ('building','ready') LIMIT 1"
            ).fetchone():
                raise LineageRefused("receive the outstanding candidate outcome first")
            encoded_binding = canonical(dict(binding))
            root = None
            if base:
                row = self.db.execute(
                    "SELECT root,page_size,binding,status FROM generations WHERE id=?", (base,)
                ).fetchone()
                if row is None or row[1:] != (page_size, encoded_binding, "selected"):
                    raise InvalidGeneration("base binding or selected status differs")
                root = row[0]
            self._retain(root)
            cursor = self.db.execute(
                "INSERT INTO generations(root,status,page_size,binding) VALUES(?,'building',?,?)",
                (root, page_size, encoded_binding),
            )
            generation = cursor.lastrowid
            assert generation is not None
            return generation

    def stage(
        self,
        generation: int,
        changes: Iterable[tuple[int, bytes | None]],
        *,
        batch_pages: int = 64,
    ) -> None:
        if batch_pages < 1:
            raise ValueError("a private write quantum must be positive")
        iterator = iter(changes)
        while batch := list(itertools.islice(iterator, batch_pages)):
            with self._transaction():
                row = self.db.execute(
                    "SELECT root,page_size,status FROM generations WHERE id=?", (generation,)
                ).fetchone()
                if row is None or row[2] != "building":
                    raise InvalidGeneration("candidate is not open for staging")
                root, page_size, _ = row
                for page, data in batch:
                    if isinstance(page, bool) or not 1 <= page <= MAX_PAGE:
                        raise ValueError("invalid SQLite page number")
                    if data is not None and len(data) != page_size:
                        raise ValueError("incomplete page image")
                    image = None if data is None else self._intern("page", data)
                    next_root = self._replace(root, 0, page, image)
                    self._retain(next_root)
                    self._release(root)
                    root = next_root
                self.db.execute("UPDATE generations SET root=? WHERE id=?", (root, generation))

    def _extreme(self, root: str, *, largest: bool) -> int:
        page = 0
        address = root
        for level in range(DEPTH):
            _, children = self._node(address, level)
            positions = range(15, -1, -1) if largest else range(16)
            position = next((i for i in positions if children[i] is not None), None)
            if position is None:
                raise InvalidGeneration("empty radix node")
            page = (page << 4) | position
            child = children[position]
            assert child is not None
            address = child
        return page

    def complete(
        self,
        generation: int,
        *,
        page_count: int,
        control: Mapping[str, Any],
        control_limit: int = 65536,
    ) -> Descriptor:
        encoded_control = canonical(dict(control))
        if len(encoded_control) > control_limit:
            raise InvalidGeneration("control state exceeds its admitted metadata envelope")
        with self._transaction():
            row = self.db.execute(
                "SELECT root,page_size,binding,status FROM generations WHERE id=?", (generation,)
            ).fetchone()
            if row is None or row[3] != "building":
                raise InvalidGeneration("candidate is not building")
            root, page_size, binding, _ = row
            if (
                root is None
                or self._node(root, 0)[0] != page_count
                or self._extreme(root, largest=False) != 1
                or self._extreme(root, largest=True) != page_count
            ):
                raise InvalidGeneration("candidate does not cover its complete page extent")
            manifest = canonical(
                {
                    "format": FORMAT,
                    "store_id": self.store_id,
                    "generation": generation,
                    "binding": json.loads(binding),
                    "control": dict(control),
                    "page_count": page_count,
                    "page_size": page_size,
                    "root": root,
                }
            )
            manifest_sha256 = digest("manifest", manifest)
            self.db.execute(
                "UPDATE generations SET status='ready',manifest=?,manifest_sha256=? WHERE id=?",
                (manifest, manifest_sha256, generation),
            )
            return Descriptor(self.store_id, generation, manifest_sha256)

    def manifest(self, descriptor: Descriptor, binding: Mapping[str, Any]) -> dict[str, Any]:
        if descriptor.store_id != self.store_id:
            raise InvalidGeneration("foreign generation store")
        row = self.db.execute(
            "SELECT manifest,status FROM generations WHERE id=?", (descriptor.generation,)
        ).fetchone()
        if row is None or row[1] not in {"ready", "selected"} or not isinstance(row[0], bytes):
            raise InvalidGeneration("generation is unavailable or incomplete")
        encoded = row[0]
        if digest("manifest", encoded) != descriptor.manifest_sha256:
            raise InvalidGeneration("selected manifest digest differs")
        value: dict[str, Any] = json.loads(encoded)
        if (
            value.get("format") != FORMAT
            or value.get("store_id") != self.store_id
            or value.get("generation") != descriptor.generation
            or canonical(value.get("binding")) != canonical(dict(binding))
        ):
            raise InvalidGeneration("source/history/configuration/algorithm binding differs")
        return value

    def selected(self, descriptor: Descriptor, binding: Mapping[str, Any]) -> None:
        """Receive a known successful external selection; this is not that selection."""
        with self._transaction():
            self.manifest(descriptor, binding)
            row = self.db.execute(
                "SELECT status FROM generations WHERE id=?", (descriptor.generation,)
            ).fetchone()
            if row == ("selected",):
                if self.tip != descriptor.generation:
                    raise LineageRefused("cannot rewind the accepted tip")
                return
            self.db.execute(
                "UPDATE generations SET status='selected' WHERE id=?", (descriptor.generation,)
            )
            self.db.execute(
                "UPDATE metadata SET value=? WHERE key='tip'", (str(descriptor.generation),)
            )

    def _walk(self, root: str, level: int = 0, prefix: int = 0) -> Iterator[tuple[int, bytes]]:
        count, children = self._node(root, level)
        self.counters.objects_restored += 1
        received = 0
        for position, address in enumerate(children):
            if address is None:
                continue
            page = (prefix << 4) | position
            if level == DEPTH - 1:
                data = self._object(address, "page")
                self.counters.objects_restored += 1
                self.counters.bytes_restored += len(data)
                received += 1
                yield page, data
            else:
                for item in self._walk(address, level + 1, page):
                    received += 1
                    yield item
        if received != count:
            raise InvalidGeneration("radix extent is inconsistent")

    def restore(
        self, descriptor: Descriptor, *, binding: Mapping[str, Any], destination: Path
    ) -> dict[str, Any]:
        """Stream into a NEW disposable file; never expose a retained page as writable."""
        value = self.manifest(descriptor, binding)
        try:
            with destination.open("xb") as target:
                received = 0
                for page, data in self._walk(value["root"]):
                    received += 1
                    if page != received or len(data) != value["page_size"]:
                        raise InvalidGeneration("page coverage or width differs")
                    target.write(data)
                if received != value["page_count"]:
                    raise InvalidGeneration("incomplete generation")
        except FileExistsError:
            raise
        except BaseException:
            destination.unlink(missing_ok=True)
            raise
        control: dict[str, Any] = value["control"]
        return control

    def pin(self, descriptor: Descriptor, binding: Mapping[str, Any]) -> str:
        with self._transaction():
            self.manifest(descriptor, binding)
            token = uuid.uuid4().hex
            self.db.execute("INSERT INTO pins VALUES(?,?)", (token, descriptor.generation))
            self.db.execute(
                "UPDATE generations SET pins=pins+1 WHERE id=?", (descriptor.generation,)
            )
            return token

    def receive_pin(self, token: str) -> None:
        with self._transaction():
            row = self.db.execute("SELECT generation FROM pins WHERE token=?", (token,)).fetchone()
            if row is None:
                raise InvalidGeneration("no original pin receipt")
            self.db.execute("DELETE FROM pins WHERE token=?", (token,))
            self.db.execute("UPDATE generations SET pins=pins-1 WHERE id=?", (row[0],))

    def discard(self, generation: int, *, rejected_candidate: bool = False) -> None:
        """Require known selection outcome and actual external reader/worker receiving."""
        with self._transaction():
            row = self.db.execute(
                "SELECT root,status,pins FROM generations WHERE id=?", (generation,)
            ).fetchone()
            if row is None:
                raise InvalidGeneration("unknown generation")
            root, status, pins = row
            if pins or generation == self.tip:
                raise InvalidGeneration("generation is selected tip or has unreceived pins")
            if (status in {"building", "ready"}) != rejected_candidate:
                raise InvalidGeneration("candidate outcome must be positively reconciled")
            self.db.execute("DELETE FROM generations WHERE id=?", (generation,))
            self._release(root)

    def collect(self, *, limit: int) -> int:
        if limit < 1:
            raise ValueError("collection quantum must be positive")
        deleted = 0
        with self._transaction():
            for _ in range(limit):
                queued = self.db.execute("SELECT address FROM garbage LIMIT 1").fetchone()
                if queued is None:
                    break
                address = queued[0]
                self.db.execute("DELETE FROM garbage WHERE address=?", (address,))
                row = self.db.execute(
                    "SELECT kind,body,refs FROM objects WHERE address=?", (address,)
                ).fetchone()
                if row is None or row[2] != 0:
                    continue
                kind, body, _ = row
                if digest(kind, body) != address:
                    raise InvalidGeneration("corrupt dependency during receiving")
                if kind == "node":
                    level = NODE_HEADER.unpack_from(body)[0]
                    for child in self._node(address, level)[1]:
                        if child is not None:
                            self._release(child)
                            self.counters.child_edges_received += 1
                self.db.execute("DELETE FROM objects WHERE address=?", (address,))
                deleted += 1
                self.counters.objects_collected += 1
        return deleted

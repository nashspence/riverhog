"""Bounded immutable SQLite page addressing for optional Native generations.

The owning generation store supplies an admitted immutable-object port and its
original transaction/receiving frame. This algebra neither opens storage nor
selects a recovery frontier. A root covers eight four-bit levels; updating one
page copies only its path. Complete restore still verifies contiguous pages,
their exact widths and the selected manifest's full extent.
"""

from __future__ import annotations

import hashlib
import re
import struct
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Literal, Protocol

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

_DEPTH = 8
_FANOUT = 16
_MAX_PAGE = (1 << 32) - 2
_HEADER = struct.Struct(">BQ")
_NODE_OCTETS = _HEADER.size + _FANOUT * 32
_ZERO = bytes(32)
_ADDRESS = re.compile(r"[0-9a-f]{64}")
_DOMAINS = {
    "node": b"riverhog-native-generation/node/v1\x00",
    "page": b"riverhog-native-generation/page/v1\x00",
}
_HASH_PREFIX_OCTETS = max(map(len, _DOMAINS.values()))
_REPLACE_READS = _DEPTH

type _Kind = Literal["node", "page"]


class InvalidGenerationMap(ValueError):
    """An optional generation object or its fixed-depth addressing is invalid."""


class GenerationObjects(Protocol):
    """Original bounded storage, without source or selection authority.

    A load must enforce max_octets before materializing its result. A store
    must intern immutable content and its child references atomically inside
    the caller's original transaction; it must not replace different content.
    The caller retains that transaction and all resource/cleanup obligations.
    """

    def load(self, address: str, *, max_octets: int) -> tuple[str, bytes]: ...

    def store(self, address: str, kind: _Kind, body: bytes, children: tuple[str, ...]) -> None: ...


@dataclass(slots=True)
class GenerationMapWork:
    object_load_calls: int = 0
    object_store_calls: int = 0
    node_loads: int = 0
    page_loads: int = 0
    node_offers: int = 0
    page_offers: int = 0
    child_slots: int = 0
    hashed_octets: int = 0
    loaded_octets: int = 0
    stored_octets: int = 0


@dataclass(frozen=True, slots=True)
class GenerationMapExtent:
    page_count: int
    first_page: int | None
    last_page: int | None


@dataclass(frozen=True, slots=True)
class GenerationPageRead:
    pages: tuple[tuple[int, bytes], ...]
    after: int
    more: bool


def _growth(mode: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def octets(value: int) -> Expression:
        return Expression("constant", value=value, unit=OCTETS)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    def product(left: Expression, right: Expression) -> Expression:
        return Expression("product", terms=(left, right))

    dimensions: tuple[Dimension, ...]
    if mode == "replace":
        dimensions = (
            Dimension("page_octets", OCTETS, "Actual supplied complete page or zero for deletion."),
        )
        loads, stores = count(_REPLACE_READS), count(_DEPTH + 1)
        node_loads, page_loads = loads, count(0)
        node_offers, page_offers = count(_DEPTH), count(1)
        loaded = octets(_REPLACE_READS * _NODE_OCTETS)
        stored = add(octets(_DEPTH * _NODE_OCTETS), dim("page_octets"))
        hashed = add(loaded, stored, octets((_REPLACE_READS + _DEPTH + 1) * _HASH_PREFIX_OCTETS))
        slots = count(_FANOUT * (2 * _REPLACE_READS + 4 * _DEPTH))
    elif mode == "extent":
        dimensions = (Dimension("roots", COUNT, "Zero or one original supplied immutable root."),)
        loads, stores = scale(1 + 2 * _DEPTH, dim("roots")), count(0)
        node_loads, page_loads, node_offers, page_offers = loads, count(0), count(0), count(0)
        loaded = product(
            dim("roots"),
            Expression("constant", value=(1 + 2 * _DEPTH) * _NODE_OCTETS, unit=OCTETS_PER_COUNT),
        )
        stored = octets(0)
        hashed = product(
            dim("roots"),
            Expression(
                "constant",
                value=(1 + 2 * _DEPTH) * (_NODE_OCTETS + _HASH_PREFIX_OCTETS),
                unit=OCTETS_PER_COUNT,
            ),
        )
        slots = scale(_FANOUT * 3 * (1 + 2 * _DEPTH), dim("roots"))
    else:
        assert mode == "read"
        dimensions = (
            Dimension("page_limit", COUNT, "Original positive maximum returned page count."),
            Dimension(
                "page_size", OCTETS_PER_COUNT, "Original SQLite page width, at most 65536 octets."
            ),
        )
        # Seek and one address lookahead add at most two paths. Completed
        # subtrees charge a path to each emitted address, never old prefixes.
        nodes = add(count(1 + 2 * _DEPTH), scale(_DEPTH, dim("page_limit")))
        loads, stores = add(nodes, dim("page_limit")), count(0)
        node_loads, page_loads = nodes, dim("page_limit")
        node_offers, page_offers = count(0), count(0)
        loaded = add(
            product(nodes, Expression("constant", value=_NODE_OCTETS, unit=OCTETS_PER_COUNT)),
            product(dim("page_limit"), dim("page_size")),
        )
        stored = octets(0)
        hashed = add(
            loaded,
            product(
                loads, Expression("constant", value=_HASH_PREFIX_OCTETS, unit=OCTETS_PER_COUNT)
            ),
        )
        slots = scale(3 * _FANOUT, nodes)
    metrics = {
        "object_load_calls": (COUNT, loads),
        "object_store_calls": (COUNT, stores),
        "node_loads": (COUNT, node_loads),
        "page_loads": (COUNT, page_loads),
        "node_offers": (COUNT, node_offers),
        "page_offers": (COUNT, page_offers),
        "child_slots": (COUNT, slots),
        "hashed_octets": (OCTETS, hashed),
        "loaded_octets": (OCTETS, loaded),
        "stored_octets": (OCTETS, stored),
    }
    return Declaration(
        id=f"riverhog-provenance.generation-page-map-{mode}.growth/v1",
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance._generation_page_map.GenerationPageMap.{mode}",),
        scope="immutable-native-generation-page-addressing",
        starts_at="receive an exact root and original page or metadata admission",
        ends_at="return bounded immutable addressing work or the original failure",
        dimensions=dimensions,
        bounds=tuple(
            Bound(name, unit, "upper", bound, "Original fixed-depth object-port work.")
            for name, (unit, bound) in metrics.items()
        ),
        conditions=(
            "The owner admits original object storage and transactions before these operations.",
        ),
        invariants=(
            "Replacing one page never traverses the retained page prefix.",
            "Old roots and objects remain immutable; their lifetime belongs to the store owner.",
        ),
        limitations=(
            "These port-call bounds exclude the port's SQL, physical storage and receiving work.",
            "Source authority, selection and complete restore verification stay with their owners.",
        ),
    )


class GenerationPageMap:
    """One fixed-depth map under its original object port and execution check."""

    def __init__(
        self, objects: GenerationObjects, *, page_size: int, check: Callable[[], None] | None = None
    ) -> None:
        if (
            type(page_size) is not int
            or not 512 <= page_size <= 65536
            or page_size & (page_size - 1)
        ):
            raise ValueError("generation requires an exact SQLite page size")
        if check is not None and not callable(check):
            raise TypeError("generation execution check must be callable")
        self.objects, self.page_size, self._check = objects, page_size, check
        self.work = GenerationMapWork()

    @staticmethod
    def _address(address: str | None) -> None:
        if address is not None and (
            type(address) is not str
            or _ADDRESS.fullmatch(address) is None
            or address == _ZERO.hex()
        ):
            raise InvalidGenerationMap("invalid generation object address")

    def _check_original(self) -> None:
        if self._check is not None and self._check() is not None:
            raise TypeError("generation execution check must return None")

    def _digest(self, kind: _Kind, body: bytes) -> str:
        prefix = _DOMAINS[kind]
        self.work.hashed_octets += len(prefix) + len(body)
        address = hashlib.sha256(prefix + body).hexdigest()
        self._address(address)
        return address

    def _load(self, address: str, kind: _Kind, limit: int) -> bytes:
        self._address(address)
        self._check_original()
        self.work.object_load_calls += 1
        stored_kind, body = self.objects.load(address, max_octets=limit)
        if stored_kind != kind or type(body) is not bytes or len(body) > limit:
            raise InvalidGenerationMap("generation dependency kind or extent differs")
        self.work.loaded_octets += len(body)
        if self._digest(kind, body) != address:
            raise InvalidGenerationMap("generation dependency digest differs")
        return body

    def _store(self, kind: _Kind, body: bytes, children: tuple[str, ...] = ()) -> str:
        self._check_original()
        address = self._digest(kind, body)
        self.work.object_store_calls += 1
        self.work.stored_octets += len(body)
        if kind == "node":
            self.work.node_offers += 1
        else:
            self.work.page_offers += 1
        self.objects.store(address, kind, body, children)
        return address

    def _node(self, address: str, level: int) -> tuple[int, tuple[str | None, ...]]:
        body = self._load(address, "node", _NODE_OCTETS)
        self.work.node_loads += 1
        if len(body) != _NODE_OCTETS:
            raise InvalidGenerationMap("generation node has an incomplete shape")
        actual_level, count = _HEADER.unpack_from(body)
        if actual_level != level or not 1 <= count <= min(_MAX_PAGE, 16 ** (_DEPTH - level)):
            raise InvalidGenerationMap("generation node level or extent differs")
        children = tuple(
            None if (child := body[offset : offset + 32]) == _ZERO else child.hex()
            for offset in range(_HEADER.size, len(body), 32)
        )
        self.work.child_slots += 2 * _FANOUT
        occupied = sum(child is not None for child in children)
        if not occupied or count < occupied or (level == _DEPTH - 1 and count != occupied):
            raise InvalidGenerationMap("generation node coverage is inconsistent")
        return count, children

    def _replace(
        self, root: str | None, level: int, page: int, image: str | None
    ) -> tuple[str | None, int, int]:
        original: tuple[str | None, ...]
        if root is None:
            previous_count, original = 0, (None,) * _FANOUT
        else:
            previous_count, original = self._node(root, level)
        children: list[str | None] = list(original)
        self.work.child_slots += _FANOUT
        position = (page >> (4 * (_DEPTH - 1 - level))) & 15
        if level == _DEPTH - 1:
            replaced_count = int(children[position] is not None)
            children[position], changed_count = image, int(image is not None)
        else:
            children[position], changed_count, replaced_count = self._replace(
                children[position], level + 1, page, image
            )
        # The old parent and replaced child's counts arrive along this same
        # verified path. Untouched sibling headers need no separate reload.
        if replaced_count > previous_count:
            raise InvalidGenerationMap("generation child exceeds its original parent extent")
        count = previous_count - replaced_count + changed_count
        self.work.child_slots += _FANOUT
        occupied = sum(child is not None for child in children)
        if count < occupied or count > min(_MAX_PAGE, _FANOUT ** (_DEPTH - level)):
            raise InvalidGenerationMap("generation replacement has inconsistent coverage")
        if count == 0:
            return None, 0, previous_count
        self.work.child_slots += _FANOUT
        body = _HEADER.pack(level, count) + b"".join(
            _ZERO if child is None else bytes.fromhex(child) for child in children
        )
        self.work.child_slots += _FANOUT
        return (
            self._store("node", body, tuple(child for child in children if child is not None)),
            count,
            previous_count,
        )

    @declare_growth(_growth("replace"))
    def replace(self, root: str | None, page: int, data: bytes | None) -> str | None:
        """Offer one page and at most eight immutable replacement path nodes."""
        self._address(root)
        if type(page) is not int or not 1 <= page <= _MAX_PAGE:
            raise ValueError("generation page lies outside SQLite's page-number domain")
        if data is not None and (type(data) is not bytes or len(data) != self.page_size):
            raise ValueError("generation requires one complete immutable SQLite page")
        image = None if data is None else self._store("page", data)
        return self._replace(root, 0, page, image)[0]

    def _extreme(self, root: str, *, largest: bool) -> int:
        address, page = root, 0
        for level in range(_DEPTH):
            children = self._node(address, level)[1]
            positions = range(_FANOUT - 1, -1, -1) if largest else range(_FANOUT)
            self.work.child_slots += _FANOUT
            position = next(index for index in positions if children[index] is not None)
            page = (page << 4) | position
            child = children[position]
            assert child is not None
            address = child
        if not 1 <= page <= _MAX_PAGE:
            raise InvalidGenerationMap("generation contains an invalid SQLite page number")
        return page

    @declare_growth(_growth("extent"))
    def extent(self, root: str | None) -> GenerationMapExtent:
        """Read fixed metadata; the restoring owner verifies complete page coverage."""
        self._address(root)
        if root is None:
            return GenerationMapExtent(0, None, None)
        count = self._node(root, 0)[0]
        return GenerationMapExtent(
            count, self._extreme(root, largest=False), self._extreme(root, largest=True)
        )

    def _addresses(
        self, root: str, level: int, prefix: int, after: int
    ) -> Generator[tuple[int, str], None, None]:
        children = self._node(root, level)[1]
        self.work.child_slots += _FANOUT
        for position, child in enumerate(children):
            if child is None:
                continue
            page = (prefix << 4) | position
            remaining = 4 * (_DEPTH - level - 1)
            if ((page + 1) << remaining) - 1 <= after:
                continue
            if level == _DEPTH - 1:
                if not 1 <= page <= _MAX_PAGE:
                    raise InvalidGenerationMap("generation contains an invalid SQLite page number")
                yield page, child
            else:
                yield from self._addresses(child, level + 1, page, after)

    @declare_growth(_growth("read"))
    def read(self, root: str | None, *, after: int, page_limit: int) -> GenerationPageRead:
        """Seek after an exact page, load admitted pages and one address lookahead."""
        self._address(root)
        if type(after) is not int or not 0 <= after <= _MAX_PAGE:
            raise ValueError("generation page cursor is invalid")
        if type(page_limit) is not int or page_limit < 1:
            raise ValueError("generation page admission must be a positive integer")
        if root is None or after == _MAX_PAGE:
            return GenerationPageRead((), after, False)
        iterator = self._addresses(root, 0, 0, after)
        selected = []
        try:
            for _index, item in zip(range(page_limit + 1), iterator, strict=False):
                selected.append(item)
        finally:
            iterator.close()
        pages = []
        for page, address in selected[:page_limit]:
            data = self._load(address, "page", self.page_size)
            self.work.page_loads += 1
            if len(data) != self.page_size:
                raise InvalidGenerationMap("generation page image is incomplete")
            pages.append((page, data))
        return GenerationPageRead(
            tuple(pages), pages[-1][0] if pages else after, len(selected) > page_limit
        )

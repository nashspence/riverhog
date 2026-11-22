"""Immutable generation-map addressing, admission and original-failure witnesses."""

from __future__ import annotations

import hashlib
import struct
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import _generation_page_map as map_owner
from riverhog_provenance._generation_page_map import (
    GenerationMapExtent,
    GenerationPageMap,
    InvalidGenerationMap,
)

from tests.support.growth_batch import SampleBatch


class Objects:
    def __init__(self):
        self.rows = {}
        self.loads = []
        self.stores = []
        self.fail_load = None
        self.fail_store = None

    def load(self, address, *, max_octets):
        self.loads.append((address, max_octets))
        if self.fail_load is not None:
            raise self.fail_load
        try:
            kind, body, _children = self.rows[address]
        except KeyError:
            raise InvalidGenerationMap("original object is missing") from None
        if len(body) > max_octets:
            raise InvalidGenerationMap("original object exceeds admitted read")
        return kind, body

    def store(self, address, kind, body, children):
        self.stores.append((address, kind, body, children))
        if self.fail_store is not None:
            raise self.fail_store
        assert (
            address
            == hashlib.sha256(f"riverhog-native-generation/{kind}/v1\0".encode() + body).hexdigest()
        )
        if address in self.rows:
            assert self.rows[address] == (kind, body, children)
        else:
            self.rows[address] = kind, body, children


def page(value, width=512):
    return value.to_bytes(8, "big") * (width // 8)


def build(count=32, *, width=512):
    objects = Objects()
    owner = GenerationPageMap(objects, page_size=width)
    root = None
    for number in range(1, count + 1):
        root = owner.replace(root, number, page(number, width))
    return objects, owner, root


def read_all(owner, root, *, limit=7):
    pages, after = [], 0
    while True:
        current = owner.read(root, after=after, page_limit=limit)
        assert len(current.pages) <= limit
        pages.extend(current.pages)
        if not current.more:
            return pages
        assert current.after > after
        after = current.after


@pytest.mark.parametrize("width", [512, 4096, 65536])
def test_passive_map_and_complete_page_extent(width):
    objects = Objects()
    owner = GenerationPageMap(objects, page_size=width)
    assert not objects.loads and not objects.stores
    assert owner.extent(None) == GenerationMapExtent(0, None, None)
    root = owner.replace(None, 1, page(1, width))
    assert owner.extent(root) == GenerationMapExtent(1, 1, 1)
    assert read_all(owner, root) == [(1, page(1, width))]
    assert all(len(body) == 521 for _, kind, body, _ in objects.stores if kind == "node")


def test_one_changed_page_copies_only_its_eight_nodes_and_keeps_old_roots():
    objects, owner, first = build(257)
    previous = dict(objects.rows)
    original_calls = len(objects.stores)
    second = owner.replace(first, 256, page(1000))
    new = objects.stores[original_calls:]
    assert sum(kind == "page" for _, kind, _, _ in new) == 1
    assert sum(kind == "node" for _, kind, _, _ in new) == 8
    assert all(objects.rows[address] == value for address, value in previous.items())
    assert owner.read(first, after=255, page_limit=1).pages == ((256, page(256)),)
    assert owner.read(second, after=255, page_limit=1).pages == ((256, page(1000)),)
    assert owner.extent(first) == owner.extent(second) == GenerationMapExtent(257, 1, 257)


def test_deletion_and_regrowth_never_resurrect_previous_page_content():
    objects, owner, first = build(32)
    second = first
    for number in range(17, 33):
        second = owner.replace(second, number, None)
    third = second
    for number in range(17, 25):
        third = owner.replace(third, number, page(number + 100))
    assert owner.extent(first) == GenerationMapExtent(32, 1, 32)
    assert owner.extent(second) == GenerationMapExtent(16, 1, 16)
    assert owner.extent(third) == GenerationMapExtent(24, 1, 24)
    assert read_all(owner, third)[16:] == [(n, page(n + 100)) for n in range(17, 25)]
    for number in range(1, 17):
        second = owner.replace(second, number, None)
    assert second is None and owner.extent(second).page_count == 0
    assert len(read_all(owner, first)) == 32


def test_sparse_full_domain_cursor_seeks_without_reading_previous_pages():
    objects = Objects()
    owner = GenerationPageMap(objects, page_size=512)
    root = None
    numbers = [1, 17, 65537, 2**28, 2**31, 2**32 - 2]
    for number in numbers:
        root = owner.replace(root, number, page(number))
    assert owner.extent(root) == GenerationMapExtent(len(numbers), 1, numbers[-1])
    assert read_all(owner, root, limit=1) == [(n, page(n)) for n in numbers]
    before = len(objects.loads)
    last = owner.read(root, after=2**31, page_limit=1)
    assert last.pages == ((numbers[-1], page(numbers[-1])),) and not last.more
    assert len(objects.loads) - before <= 17
    assert owner.read(root, after=numbers[-1], page_limit=1).pages == ()


def test_read_lookahead_does_not_load_an_unadmitted_page_image():
    objects, owner, root = build(32, width=65536)
    previous = asdict(owner.work)
    result = owner.read(root, after=0, page_limit=1)
    assert result.more and result.after == 1
    assert owner.work.page_loads - previous["page_loads"] == 1
    assert len(result.pages[0][1]) == 65536


@pytest.mark.parametrize("value", [True, 0, -1, 511, 513, 131072, 512.0])
def test_invalid_page_width_refuses_before_object_effects(value):
    objects = Objects()
    with pytest.raises(ValueError):
        GenerationPageMap(objects, page_size=value)
    assert not objects.loads and not objects.stores


@pytest.mark.parametrize("number", [True, 0, -1, 2**32 - 1, 1.0])
def test_invalid_page_number_refuses_before_object_effects(number):
    objects = Objects()
    owner = GenerationPageMap(objects, page_size=512)
    with pytest.raises(ValueError):
        owner.replace(None, number, page(1))
    assert not objects.loads and not objects.stores


@pytest.mark.parametrize("body", [b"", b"x" * 511, b"x" * 513, bytearray(512), "x" * 512])
def test_incomplete_or_mutable_page_refuses_before_object_effects(body):
    objects = Objects()
    owner = GenerationPageMap(objects, page_size=512)
    with pytest.raises(ValueError):
        owner.replace(None, 1, body)
    assert not objects.loads and not objects.stores


@pytest.mark.parametrize("kind", ["load", "store", "check"])
def test_original_failure_propagates_and_previous_root_is_not_replaced(kind):
    objects, owner, root = build()
    failure = RuntimeError("original admitted generation operation failed")
    if kind == "check":

        def check():
            raise failure

        owner._check = check
    else:
        setattr(objects, "fail_" + kind, failure)
    original = dict(objects.rows)
    with pytest.raises(RuntimeError) as caught:
        owner.replace(root, 2, page(999))
    assert caught.value is failure
    assert all(objects.rows[address] == value for address, value in original.items())
    objects.fail_load = objects.fail_store = owner._check = None
    assert owner.read(root, after=1, page_limit=1).pages == ((2, page(2)),)


@pytest.mark.parametrize("corruption", ["digest", "kind", "missing", "oversize"])
def test_original_generation_corruption_never_substitutes_a_different_page(corruption):
    objects, owner, root = build()
    changed = owner.replace(root, 2, page(999))
    address = next(
        a for a, (k, body, _) in objects.rows.items() if k == "page" and body == page(999)
    )
    kind, body, children = objects.rows[address]
    if corruption == "missing":
        del objects.rows[address]
    else:
        objects.rows[address] = (
            "node" if corruption == "kind" else kind,
            page(1000)
            if corruption == "digest"
            else body + b"x"
            if corruption == "oversize"
            else body,
            children,
        )
    with pytest.raises(InvalidGenerationMap):
        owner.read(changed, after=1, page_limit=1)
    assert owner.read(root, after=1, page_limit=1).pages == ((2, page(2)),)


@pytest.mark.parametrize("level,count", [(1, 1), (0, 0), (0, 2**32 - 1), (7, 2)])
def test_hashed_but_malformed_node_is_refused(level, count):
    objects = Objects()
    body = struct.pack(">BQ", level, count) + bytes.fromhex("a" * 64) + bytes(15 * 32)
    address = hashlib.sha256(b"riverhog-native-generation/node/v1\0" + body).hexdigest()
    objects.store(address, "node", body, ("a" * 64,))
    with pytest.raises(InvalidGenerationMap):
        GenerationPageMap(objects, page_size=512).extent(address)


@pytest.mark.growth_witness(
    "riverhog-native-generation-map-replacement-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_page_map.GenerationPageMap.replace",
)
def test_actual_page_replacement_has_fixed_depth_at_independent_retained_extents(
    numeric_policy, record_property
):
    samples = []
    operation = GenerationPageMap.replace
    projection = project_bindings((growth_binding(operation),), numeric_policy=numeric_policy)
    for count in (4, 64, 513):
        for width in (512, 4096):
            objects, owner, root = build(count, width=width)
            for data in (page(9999, width), None):
                before = asdict(owner.work)
                result = owner.replace(root, min(2, count), data)
                metrics = {key: value - before[key] for key, value in asdict(owner.work).items()}
                with SampleBatch((operation,), limit=1, numeric_policy=numeric_policy) as batch:
                    sample = batch.sample(
                        operation, {"page_octets": 0 if data is None else len(data)}, metrics
                    )
                validate_execution_samples(
                    projection,
                    growth_binding(operation).declaration.id,
                    [sample],
                    numeric_policy=numeric_policy,
                )
                assert metrics["node_offers"] <= 8 and metrics["page_offers"] <= 1
                assert owner.extent(root).page_count == count
                assert owner.extent(result).page_count == count - int(data is None)
                samples.append(sample)
    record_property("growth_samples", samples)
    assert len(samples) == 12


def read_samples(mode, numeric_policy):
    operation = getattr(GenerationPageMap, mode)
    samples = []
    cases = (
        [1],
        list(range(1, 18)),
        list(range(1, 258)),
        [1, 17, 65537, 2**28, 2**31, 2**32 - 2],
    )
    for numbers in cases:
        for width in (512, 4096):
            objects = Objects()
            owner = GenerationPageMap(objects, page_size=width)
            root = None
            for number in numbers:
                root = owner.replace(root, number, page(number, width))
            coordinates = (
                [(0, 1)]
                if mode == "extent"
                else [
                    (after, limit)
                    for after in (0, numbers[-1] // 2, numbers[-1])
                    for limit in (1, 4, 16)
                ]
            )
            for after, limit in coordinates:
                before = asdict(owner.work)
                if mode == "extent":
                    owner.extent(root)
                    dimensions = {"roots": int(root is not None)}
                else:
                    result = owner.read(root, after=after, page_limit=limit)
                    dimensions = {"page_limit": limit, "page_size": owner.page_size}
                    expected = [(n, page(n, width)) for n in numbers if n > after][:limit]
                    assert list(result.pages) == expected
                    assert result.more == (sum(n > after for n in numbers) > limit)
                metrics = {key: value - before[key] for key, value in asdict(owner.work).items()}
                with SampleBatch((operation,), limit=1, numeric_policy=numeric_policy) as batch:
                    samples.append(batch.sample(operation, dimensions, metrics))
    if mode == "extent":
        owner = GenerationPageMap(Objects(), page_size=512)
        before = asdict(owner.work)
        owner.extent(None)
        metrics = {key: value - before[key] for key, value in asdict(owner.work).items()}
        with SampleBatch((operation,), limit=1, numeric_policy=numeric_policy) as batch:
            samples.append(batch.sample(operation, {"roots": 0}, metrics))
    return samples


@pytest.mark.growth_witness(
    "riverhog-native-generation-map-page-read-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_page_map.GenerationPageMap.read",
)
def test_actual_sparse_and_dense_read_work_respects_original_page_admission(
    numeric_policy, record_property
):
    samples = read_samples("read", numeric_policy)
    record_property("growth_samples", samples)
    assert len(samples) == 72


@pytest.mark.growth_witness(
    "riverhog-native-generation-map-extent-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._generation_page_map.GenerationPageMap.extent",
)
def test_actual_extent_work_is_independent_of_retained_page_count(numeric_policy, record_property):
    samples = read_samples("extent", numeric_policy)
    record_property("growth_samples", samples)
    assert len(samples) == 9


def test_replacement_reads_only_its_original_path_with_every_sibling_populated():
    objects = Objects()
    owner = GenerationPageMap(objects, page_size=512)
    root = None
    numbers = {
        1,
        *(1 | (nibble << (4 * level)) for level in range(1, 8) for nibble in range(1, 16)),
    }
    for number in sorted(numbers):
        root = owner.replace(root, number, page(number))
    before = asdict(owner.work)
    result = owner.replace(root, 1, page(99999))
    assert owner.work.node_loads - before["node_loads"] == 8
    assert owner.extent(result) == owner.extent(root)


def test_reported_port_and_digest_work_matches_independent_original_calls(monkeypatch):
    objects, owner, first = build(65)
    original = hashlib.sha256
    hashes = []

    def digest(data):
        hashes.append(len(data))
        return original(data)

    monkeypatch.setattr(map_owner, "hashlib", SimpleNamespace(sha256=digest))
    for operation in (
        lambda: owner.replace(first, 2, page(999)),
        lambda: owner.extent(first),
        lambda: owner.read(first, after=31, page_limit=4),
    ):
        before = asdict(owner.work)
        loads, stores, hashed = len(objects.loads), len(objects.stores), len(hashes)
        operation()
        assert owner.work.hashed_octets - before["hashed_octets"] == sum(hashes[hashed:])
        assert (
            owner.work.object_load_calls - before["object_load_calls"] == len(objects.loads) - loads
        )
        assert (
            owner.work.object_store_calls - before["object_store_calls"]
            == len(objects.stores) - stores
        )
        assert owner.work.stored_octets - before["stored_octets"] == sum(
            len(item[2]) for item in objects.stores[stores:]
        )


def test_path_delta_refuses_a_child_larger_than_its_authenticated_parent():
    objects, owner, original = build(32)
    kind, body, children = objects.rows[original]
    damaged = body[:1] + struct.pack(">Q", 1) + body[9:]
    address = hashlib.sha256(b"riverhog-native-generation/node/v1\0" + damaged).hexdigest()
    objects.store(address, kind, damaged, children)
    with pytest.raises(InvalidGenerationMap, match="child exceeds"):
        owner.replace(address, 2, page(999))
    assert owner.extent(original).page_count == 32
    assert owner.read(original, after=1, page_limit=1).pages == ((2, page(2)),)


@pytest.mark.parametrize("metric", ["object_load_calls", "node_loads", "child_slots"])
def test_independent_path_work_ceiling_rejects_an_extra_original_visit(metric, numeric_policy):
    objects, owner, root = build(32)
    operation = GenerationPageMap.replace
    before = asdict(owner.work)
    owner.replace(root, 2, page(999))
    metrics = {key: value - before[key] for key, value in asdict(owner.work).items()}
    with SampleBatch((operation,), limit=1, numeric_policy=numeric_policy) as batch:
        sample = batch.sample(operation, {"page_octets": 512}, metrics)
    invalid = {**sample, "metrics": {**sample["metrics"], metric: metrics[metric] + 1}}
    projection = project_bindings((growth_binding(operation),), numeric_policy=numeric_policy)
    with pytest.raises(ValueError, match="actual primitive observations violate"):
        validate_execution_samples(
            projection,
            growth_binding(operation).declaration.id,
            [invalid],
            numeric_policy=numeric_policy,
        )

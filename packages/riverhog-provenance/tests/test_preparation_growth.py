from __future__ import annotations

import builtins
import hashlib
import io
import json
import os
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
import riverhog_canonical_json as canonical
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import epoch_boundaries as epochs
from riverhog_provenance import topology_facts as topology
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX


class UnavailableSource(OSError):
    pass


class Recorder:
    def __init__(self):
        self.metrics = defaultdict(int)

    @contextmanager
    def observe(self):
        metrics = self.metrics
        loads, encode, sha256 = canonical.json.loads, canonical.canonical_json_bytes, hashlib.sha256
        sorted_values, scan = sorted, topology._scan_item

        def decoded(value, *args, **kwargs):
            metrics["json_decodes"] += 1
            metrics["json_decoded_octets"] += (
                len(value.encode()) if isinstance(value, str) else len(value)
            )
            return loads(value, *args, **kwargs)

        def encoded(*args, **kwargs):
            metrics["json_encodes"] += 1
            result = encode(*args, **kwargs)
            metrics["json_encoded_octets"] += len(result)
            return result

        def hashed(value=b"", *args, **kwargs):
            metrics["hashed_octets"] += len(value)
            return sha256(value, *args, **kwargs)

        def sorted_keys(values, *, key):
            def observed_key(value):
                result = key(value)
                metrics["range_keys"] += 1
                metrics["range_key_octets"] += len(result)
                return result

            return sorted_values(values, key=observed_key)

        class ObservedBytes(bytes):
            def __iter__(self):
                for value in super().__iter__():
                    metrics["visited_octets"] += 1
                    yield value

        def scanned(read, cursor):
            metrics["item_calls"] += 1

            def observed_read(offset, length):
                metrics["read_calls"] += 1
                metrics["requested_range_octets"] += length
                result = read(offset, length)
                metrics["source_range_octets"] += len(result)
                return ObservedBytes(result)

            result = scan(observed_read, cursor)
            metrics["returned_octets"] += len(result[0])
            return result

        def forbidden(*_args, **_kwargs):
            raise AssertionError("native preparation performed unowned filesystem or database work")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                canonical,
                "json",
                SimpleNamespace(
                    loads=decoded,
                    dumps=canonical.json.dumps,
                    JSONDecodeError=canonical.json.JSONDecodeError,
                ),
            )
            patch.setattr(canonical, "canonical_json_bytes", encoded)
            patch.setattr(topology, "canonical_json_bytes", encoded)
            patch.setattr(epochs, "hashlib", SimpleNamespace(sha256=hashed))
            patch.setattr(topology, "hashlib", SimpleNamespace(sha256=hashed))
            patch.setattr(topology, "sorted", sorted_keys, raising=False)
            patch.setattr(topology, "_scan_item", scanned)
            for module, name in (
                (os, "open"),
                (os, "read"),
                (os, "write"),
                (os, "listdir"),
                (os, "scandir"),
                (io, "open"),
                (builtins, "open"),
                (sqlite3, "connect"),
            ):
                patch.setattr(module, name, forbidden)
            yield


def sample(operation, dimensions, recorder):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical.canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {
            bound.metric: recorder.metrics[bound.metric] for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [result])
    return result


def edge(number, *, padding="", ignored=False):
    return {
        "assertion_id": f"edge:{number}",
        "type": "other" if ignored else "derivation",
        "used_state": {"object_id": "root"},
        "generated_state": {"object_id": f"leaf:{number}"},
        "padding": padding,
    }


def document(relations=(), *, retracts=(), kind="assertion", padding="", fields=0):
    additions = {f"extra-{number:08x}": "é" for number in range(fields)}
    return {
        **additions,
        "entry_kind": kind,
        "notes": padding,
        "body": {
            **additions,
            "retracts": list(retracts),
            "assertions": {**additions, "relations": list(relations)},
        },
    }


def range_fields(raw):
    try:
        value = json.loads(raw)
    except (ValueError, TypeError, UnicodeError):
        return 0
    body = value.get("body") if isinstance(value, dict) else None
    assertions = body.get("assertions") if isinstance(body, dict) else None
    return sum(len(item) for item in (value, body, assertions) if isinstance(item, dict))


def entry_samples(samples, *, large=True):
    specifications = [(0, 0, 0), (1, 0, 0), (17, 0, 0)]
    if large:
        specifications.extend(((1025, 0, 0), (1, 65537, 0), (1, 0, 17), (1, 0, 1025)))
    raws = [
        canonical.canonical_json_bytes(
            document(
                (edge(n) for n in range(count)),
                kind=kind,
                retracts=[{"assertion_id": "old"}],
                padding="é" * padding,
                fields=fields,
            )
        )
        for count, padding, fields in specifications
        for kind in ("journal_init", "assertion", "correction", "checkpoint")
    ]
    raws.extend(
        (
            b"{}",
            b"[]",
            b"1e20",
            b"1e9999",
            b'{"entry_kind":"unknown","body":{}}',
            b'{"entry_kind":"assertion","body":[]}',
            b'{"body":{"assertions":[]},"entry_kind":"assertion"}',
            b'{"body":{"assertions":{"relations":{}}},"entry_kind":"assertion"}',
            b'{"body":{},"entry_kind":"assertion","x":1e20}',
            b'{"x":1,"x":2}',
            b"\xff",
            b"{",
            "not bytes",
            b"x" * (PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 1),
        )
    )
    for raw in raws:
        for operation in (epochs.classify_epoch_boundary, topology.plan_topology_frame):
            dimensions = {"input_octets": len(raw) if type(raw) is bytes else 0}
            if operation is topology.plan_topology_frame:
                dimensions["range_fields"] = range_fields(raw)
            arguments = (
                {"ordinal": 17, "previous_epoch": 5}
                if operation is epochs.classify_epoch_boundary
                else {}
            )
            try:
                expected = operation(raw, **arguments)
                expected_error = None
            except (ValueError, TypeError) as error:
                expected, expected_error = None, type(error)
            recorder = Recorder()
            with recorder.observe():
                if expected_error is None:
                    actual = operation(raw, **arguments)
                    assert actual == expected
                    recorder.metrics[
                        "returned_facts"
                        if operation is epochs.classify_epoch_boundary
                        else "returned_cursors"
                    ] += 1
                else:
                    with pytest.raises(expected_error):
                        operation(raw, **arguments)
            if expected_error is None:
                parsed = json.loads(raw)
                assert actual.json_sha256 == hashlib.sha256(raw).hexdigest()
                if operation is epochs.classify_epoch_boundary:
                    assert (actual.ordinal, actual.epoch, actual.entry_kind) == (
                        17,
                        5 + int(parsed["entry_kind"] == "correction"),
                        parsed["entry_kind"],
                    )
                else:
                    wanted = {
                        "relations": parsed["body"].get("assertions", {}).get("relations", []),
                        "retracts": parsed["body"].get("retracts", [])
                        if parsed.get("entry_kind") == "correction"
                        else [],
                    }
                    for name, items in wanted.items():
                        array = getattr(actual, name)
                        assert array.index == 0 and array.count == len(items)
                        if array.end:
                            assert raw[
                                array.offset - 1 : array.end
                            ] == canonical.canonical_json_bytes(items)
            samples.append(sample(operation, dimensions, recorder))
    raw = canonical.canonical_json_bytes(document(kind="correction"))
    for ordinal, previous in (
        (0, 0),
        (1, 0),
        (17, 5),
        (2**200, 2**199),
        (-1, 0),
        (False, 0),
        (1, 2),
        (1, -1),
        (1, True),
    ):
        recorder = Recorder()
        with recorder.observe():
            if (
                type(ordinal) is int
                and ordinal >= 0
                and type(previous) is int
                and 0 <= previous <= ordinal
            ):
                value = epochs.classify_epoch_boundary(
                    raw, ordinal=ordinal, previous_epoch=previous
                )
                assert value.epoch == previous + int(ordinal > 0)
                recorder.metrics["returned_facts"] += 1
            else:
                with pytest.raises(ValueError):
                    epochs.classify_epoch_boundary(raw, ordinal=ordinal, previous_epoch=previous)
        samples.append(sample(epochs.classify_epoch_boundary, {"input_octets": len(raw)}, recorder))


def advance_measure(
    raw, cursor, remaining, *, item_budget, octet_budget, block, fail=False, error=None
):
    recorder, reads = Recorder(), []
    start = min(
        (
            array.offset
            for array in (cursor.retracts, cursor.relations)
            if array.index < array.count
        ),
        default=len(raw),
    )

    def read(offset, length):
        assert offset >= start
        reads.append(offset)
        if fail and len(reads) == 2:
            raise UnavailableSource("authenticated source temporarily unavailable")
        return raw[offset : offset + min(length, block)]

    dimensions = {
        "item_budget": item_budget,
        "octet_budget": octet_budget,
        "maximum_item_octets": max(map(len, remaining), default=0),
    }
    result = None
    with recorder.observe():
        if error is None:
            result = topology.advance_topology_frame(
                cursor, read, item_budget=item_budget, octet_budget=octet_budget
            )
            recorder.metrics["returned_facts"] += len(result.facts)
        else:
            with pytest.raises(error):
                topology.advance_topology_frame(
                    cursor, read, item_budget=item_budget, octet_budget=octet_budget
                )
    if result is not None:
        for fact in result.facts:
            encoded = raw[fact.byte_offset : fact.byte_offset + fact.json_bytes]
            assert fact.json_sha256 == hashlib.sha256(encoded).hexdigest()
            assert json.loads(encoded)["assertion_id"] == fact.assertion_id
    return result, sample(topology.advance_topology_frame, dimensions, recorder)


def advance_samples(samples, *, large=True):
    counts = (0, 1, 17, 129, 1025) if large else (0, 1, 17)
    for count in counts:
        values = [edge(number, ignored=number % 7 == 1) for number in range(count)]
        raw = canonical.canonical_json_bytes(
            document(values, retracts=[{"assertion_id": "old"}], kind="correction")
        )
        encoded = [
            canonical.canonical_json_bytes({"assertion_id": "old"}),
            *(canonical.canonical_json_bytes(value) for value in values),
        ]
        for admission in (1, 7, 64):
            cursor = topology.plan_topology_frame(raw)
            seen, consumed = [], 0
            history_start = len(samples)
            while True:
                result, observed = advance_measure(
                    raw,
                    cursor,
                    encoded[consumed:],
                    item_budget=admission,
                    octet_budget=65536,
                    block=65536,
                )
                samples.append(observed)
                consumed += result.inspected_items
                seen.extend(result.facts)
                if result.complete:
                    break
                assert result.inspected_items > 0
                cursor = result.cursor
            assert consumed == count + 1
            assert len(seen) == 1 + sum(number % 7 != 1 for number in range(count))
            history = samples[history_start:]
            assert sum(row["metrics"]["hashed_octets"] for row in history) == len(encoded[0]) + sum(
                len(value) for number, value in enumerate(encoded[1:]) if number % 7 != 1
            )
            assert sum(row["metrics"]["visited_octets"] for row in history) == sum(
                len(value) + 1 for value in encoded
            )
            assert sum(row["metrics"]["json_decodes"] for row in history) == count + 1
    raw = canonical.canonical_json_bytes(document())
    cursor = topology.plan_topology_frame(raw)
    result, observed = advance_measure(raw, cursor, (), item_budget=1, octet_budget=1, block=1)
    assert result.complete and not result.facts and result.inspected_items == 0
    samples.append(observed)
    for prefix, padding in (
        ((0, 0), (17, 17), (1025, 65537), (17, 131089)) if large else ((0, 0), (17, 17))
    ):
        values = [edge(number) for number in range(prefix)] + [edge(prefix, padding="é" * padding)]
        encoded = [canonical.canonical_json_bytes(value) for value in values]
        raw = canonical.canonical_json_bytes(document(values))
        initial = topology.plan_topology_frame(raw)
        offset = initial.relations.offset + sum(len(value) + 1 for value in encoded[:-1])
        cursor = replace(initial, relations=replace(initial.relations, offset=offset, index=prefix))
        for block in (1, 7, 65536):
            result, observed = advance_measure(
                raw, cursor, encoded[-1:], item_budget=1, octet_budget=1, block=block
            )
            assert result.complete and len(result.facts) == 1
            samples.append(observed)
        if padding > 65536:
            result, observed = advance_measure(
                raw,
                cursor,
                encoded[-1:],
                item_budget=1,
                octet_budget=1,
                block=65536,
                fail=True,
                error=UnavailableSource,
            )
            assert result is None
            samples.append(observed)
            result, observed = advance_measure(
                raw, cursor, encoded[-1:], item_budget=1, octet_budget=1, block=65536
            )
            assert result.complete
            samples.append(observed)
    for bad in (
        [1],
        {"type": "derivation", "assertion_id": "bad"},
        {"type": "derivation", "assertion_id": 1, "used_state": {}, "generated_state": {}},
    ):
        raw = canonical.canonical_json_bytes(document([bad]))
        cursor = topology.plan_topology_frame(raw)
        result, observed = advance_measure(
            raw,
            cursor,
            [canonical.canonical_json_bytes(bad)],
            item_budget=1,
            octet_budget=65536,
            block=7,
            error=ValueError,
        )
        assert result is None
        samples.append(observed)


@pytest.mark.growth_witness
def test_actual_entry_preparation_preserves_complete_codec_and_hash_work(record_property):
    samples = []
    entry_samples(samples)
    record_property("growth_samples", samples)


@pytest.mark.growth_witness
def test_actual_topology_advance_never_revisits_committed_items(record_property):
    samples = []
    advance_samples(samples)
    record_property("growth_samples", samples)

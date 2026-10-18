from __future__ import annotations

import hashlib
import subprocess
import sys
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_topology as topology
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import parse_journal, validate_journal
from riverhog_provenance import topology_facts as neutral
from sqlalchemy import delete, event, select, update

from tests.unit.test_journal_capture import retained
from tests.unit.test_journal_epoch_scan import finish as finish_epochs
from tests.unit.test_journal_summary_consumers import source
from tests.unit.test_journal_validation_handoff import complete_capture, service_for, terminal
from tests.unit.test_staged_journal_range import JOURNAL, Journal

Fact, Scan = topology.Fact, topology.Scan


def edge(n, *, kind="derivation", target=None, padding=""):
    left, right = (
        ("used_state", "generated_state") if kind == "derivation" else ("specific", "general")
    )
    return {
        "assertion_id": f"assertion:{n}",
        "type": kind,
        left: {"object_id": "root"},
        right: {"object_id": f"leaf:{n}" if target is None else target},
        "padding": padding,
    }


def entry(relations=(), *, retracts=(), kind="assertion"):
    body = {"assertions": {"relations": list(relations)}}
    if kind == "correction":
        body["retracts"] = list(retracts)
    return canonical_json_bytes({"entry_kind": kind, "body": body})


def raw_journal(*entries):
    return b"".join(b"\x1e" + value + b"\n" for value in entries)


def captured(factory):
    with factory() as session:
        return list(
            session.scalars(
                select(Fact).order_by(Fact.frame_ordinal, Fact.array_kind, Fact.item_ordinal)
            )
        )


def progress(factory):
    with factory() as session:
        row = session.get(Scan, (1, JOURNAL))
        return row.version, row.topology_next_frame, row.topology_state, row.topology_cursor_json


def ready(factory):
    work = complete_capture(factory)
    finish_epochs(factory, work)
    return work


def finish(factory, work):
    calls = 0
    while True:
        calls += 1
        done = topology.advance_upload_journal_topology_capture(factory, work)
        assert terminal(factory)["anchor"] is None
        assert terminal(factory)["state"] == "validating"
        if done:
            return calls
        assert calls < 100


@pytest.mark.parametrize("chunk", [1, 7, 127, 65536])
def test_exact_item_offsets_survive_strings_nested_containers_and_unicode(chunk):
    values = [edge(0, padding='é ],[ \\ " \n'), edge(1, padding="x" * 70000), edge(2)]
    raw = entry(
        values, retracts=[{"assertion_id": "old", "entry": {"id": "past"}}], kind="correction"
    )
    cursor = neutral.plan_topology_frame(raw)
    seen, reads = [], []

    def read(offset, length):
        reads.append((offset, min(length, chunk)))
        return raw[offset : offset + min(length, chunk)]

    while True:
        advance = neutral.advance_topology_frame(cursor, read, item_budget=1)
        assert advance.inspected_items == 1
        seen.extend(advance.facts)
        cursor = advance.cursor
        if advance.complete:
            break
    assert len(seen) == 4 and [fact.kind for fact in seen] == ["retraction"] + ["derivation"] * 3
    expected = [{"assertion_id": "old", "entry": {"id": "past"}}, *values]
    for fact, item in zip(seen, expected, strict=True):
        value = raw[fact.byte_offset : fact.byte_offset + fact.json_bytes]
        assert value == canonical_json_bytes(item)
        assert hashlib.sha256(value).hexdigest() == fact.json_sha256


def test_large_array_resume_does_not_decode_or_visit_its_committed_prefix(monkeypatch):
    raw = entry(edge(n) for n in range(1025))
    cursor = neutral.plan_topology_frame(raw)
    decode = neutral.decode_document
    decoded = []

    def observed(raw):
        decoded.append(len(raw))
        return decode(raw)

    monkeypatch.setattr(neutral, "decode_document", observed)
    seen, lower_bound, visits = [], 0, 0
    while True:
        starts = []

        def read(offset, length, lower_bound=lower_bound, starts=starts):
            assert offset >= lower_bound
            starts.append(offset)
            return raw[offset : offset + length]

        result = neutral.advance_topology_frame(cursor, read)
        assert 0 < result.inspected_items <= 64
        assert len(result.facts) <= 64
        visits += result.inspected_octets
        seen.extend(result.facts)
        cursor = result.cursor
        lower_bound = cursor.relations.offset
        if result.complete:
            break
    assert len(seen) == len(decoded) == 1025
    assert visits == cursor.relations.end - neutral.plan_topology_frame(raw).relations.offset


@pytest.mark.parametrize("fault", ["delimiter", "extent", "missing"])
def test_changed_item_source_fails_closed(fault):
    raw = entry([edge(0), edge(1)])
    cursor = neutral.plan_topology_frame(raw)
    if fault == "delimiter":
        raw = raw[: cursor.relations.end - 1] + b"," + raw[cursor.relations.end :]
    elif fault == "extent":
        cursor = replace(cursor, relations=replace(cursor.relations, count=1))
    with pytest.raises(RuntimeError):
        neutral.advance_topology_frame(
            cursor, lambda start, size: b"" if fault == "missing" else raw[start : start + size]
        )


def test_capture_preserves_parallel_documentary_edges_kinds_and_real_epochs(tmp_path):
    first = entry(
        [edge(0, target="leaf"), edge(1, target="leaf"), edge(2, kind="specialization")],
        kind="journal_init",
    )
    correction = entry(
        [edge(3)],
        retracts=[{"assertion_id": "assertion:0", "entry": {"id": "first"}}],
        kind="correction",
    )
    raw = raw_journal(first, correction)
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        assert finish(factory, work) == 2
        rows = captured(factory)
        assert len(rows) == 5
        assert [r.assertion_id for r in rows if r.frame_ordinal == 0] == [
            "assertion:0",
            "assertion:1",
            "assertion:2",
        ]
        assert {r.epoch for r in rows if r.frame_ordinal == 0} == {0}
        assert {r.epoch for r in rows if r.frame_ordinal == 1} == {1}
        assert [r.kind for r in rows if r.array_kind == "retracts"] == ["retraction"]
        for row in rows:
            exact = raw[row.byte_offset : row.byte_offset + row.json_bytes]
            assert hashlib.sha256(exact).hexdigest() == row.json_sha256
        assert topology._decode(progress(factory)[-1]).facts == 5


def test_high_degree_capture_commits_bounded_rows_and_plans_each_entry_once(tmp_path, monkeypatch):
    raw = raw_journal(entry(edge(n) for n in range(1025)))
    with retained(tmp_path, raw) as (engine, factory):
        work = ready(factory)
        planned = []
        plan = topology.plan_topology_frame

        def observed(raw):
            assert engine.pool.checkedout() == 0
            planned.append(len(raw))
            return plan(raw)

        monkeypatch.setattr(topology, "plan_topology_frame", observed)
        previous = 0
        while True:
            done = topology.advance_upload_journal_topology_capture(factory, work)
            rows = captured(factory)
            assert 0 < len(rows) - previous <= 64
            previous = len(rows)
            assert engine.pool.checkedout() == 0
            if done:
                break
        assert len(planned) == 1 and len(rows) == 1025
        assert topology._decode(progress(factory)[-1]).active is None


def test_item_decode_and_page_digest_computation_release_catalog_readers(tmp_path, monkeypatch):
    raw = raw_journal(entry([edge(n, padding="x" * 5000) for n in range(65)]))
    with retained(tmp_path, raw) as (engine, factory):
        work = ready(factory)
        decode = neutral.decode_document
        seen = []

        def detached(raw):
            assert engine.pool.checkedout() == 0
            seen.append(True)
            return decode(raw)

        monkeypatch.setattr(neutral, "decode_document", detached)
        finish(factory, work)
        assert len(seen) == 65 and len(captured(factory)) == 65


@pytest.mark.parametrize(
    "changed", ["capture_attempt_id", "capture_directory_id", "capture_version", "sha256"]
)
def test_stale_topology_computation_cannot_publish(tmp_path, changed):
    with retained(tmp_path, raw_journal(entry([edge(0)]))) as (_, factory):
        work = ready(factory)
        pending = topology._begin(factory, work)
        advance = topology._compute(factory, pending)
        before = progress(factory)
        with factory() as session, session.begin():
            session.execute(
                update(Journal).values({changed: 99 if changed == "capture_version" else "f" * 64})
            )
        assert not topology._commit(factory, pending, advance)
        assert progress(factory) == before and not captured(factory)


def test_duplicate_topology_computations_cannot_duplicate_output(tmp_path):
    with retained(tmp_path, raw_journal(entry(edge(n) for n in range(129)))) as (_, factory):
        work = ready(factory)
        pending = topology._begin(factory, work)
        advance = topology._compute(factory, pending)
        assert topology._commit(factory, pending, advance)
        before = progress(factory)
        assert not topology._commit(factory, pending, advance)
        assert progress(factory) == before and len(captured(factory)) == 64
        assert finish(factory, work) == 2 and len(captured(factory)) == 129


def test_failed_fact_publication_rolls_back_the_cursor_and_all_output(tmp_path):
    with retained(tmp_path, raw_journal(entry([edge(0), edge(1)]))) as (engine, factory):
        work = ready(factory)
        pending = topology._begin(factory, work)
        advance = topology._compute(factory, pending)
        before = progress(factory)

        def fail(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.startswith("INSERT INTO collection_upload_provenance_topology_facts"):
                raise RuntimeError("injected fact publication failure")

        event.listen(engine, "before_cursor_execute", fail)
        try:
            with pytest.raises(RuntimeError, match="injected"):
                topology._commit(factory, pending, advance)
        finally:
            event.remove(engine, "before_cursor_execute", fail)
        assert progress(factory) == before and not captured(factory)


@pytest.mark.parametrize(
    "fault", ["missing-last", "algorithm", "cursor", "frame-digest", "missing-page"]
)
def test_reopen_fails_closed_on_changed_progress_or_missing_source(tmp_path, fault):
    with retained(
        tmp_path, raw_journal(entry(edge(n, padding="x" * 4000) for n in range(129)))
    ) as (_, factory):
        work = ready(factory)
        assert not topology.advance_upload_journal_topology_capture(factory, work)
        with factory() as session, session.begin():
            if fault == "missing-last":
                last = topology._decode(progress(factory)[-1]).last
                session.execute(delete(Fact).where(Fact.item_ordinal == last[2]))
            elif fault == "algorithm":
                session.execute(update(Scan).values(topology_algorithm="different"))
            elif fault == "cursor":
                session.execute(update(Scan).values(topology_cursor_json='{"active":null}'))
            elif fault == "frame-digest":
                session.execute(update(topology.Frame).values(epoch_json_sha256="f" * 64))
            else:
                from riverhog_core.catalog_models import (
                    CollectionUploadProvenanceCaptureSegmentRecord as Segment,
                )

                session.execute(delete(Segment))
        with pytest.raises(RuntimeError):
            topology.advance_upload_journal_topology_capture(factory, work)
        assert terminal(factory)["anchor"] is None


@pytest.mark.parametrize("commit", [False, True])
def test_process_death_resumes_only_committed_facts_without_replanning_the_prefix(
    tmp_path, commit, monkeypatch
):
    with retained(tmp_path, raw_journal(entry(edge(n) for n in range(129)))) as (engine, factory):
        work = ready(factory)
        before = progress(factory)
        code = """import os,sys
from sqlalchemy import create_engine,event
from sqlalchemy.orm import sessionmaker
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_journal_topology as topology
engine=create_engine(sys.argv[1])
factory=sessionmaker(engine)
source=owner._read_upload_journal_validation(factory,1,sys.argv[2])
work=topology._begin(factory,source)
advance=topology._compute(factory,work)
if sys.argv[3]=='before':
    event.listen(engine,'commit',lambda _connection: os._exit(73))
assert topology._commit(factory,work,advance)
os._exit(73)
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(engine.url), JOURNAL, "after" if commit else "before"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 73, result.stderr
        engine.dispose()
        assert len(captured(factory)) == (64 if commit else 0)
        if not commit:
            assert progress(factory) == before
        else:

            def no_prefix_replan(_raw):
                pytest.fail("committed entry planning was repeated after process restart")

            monkeypatch.setattr(topology, "plan_topology_frame", no_prefix_replan)
        finish(factory, work)
        assert len(captured(factory)) == 129


def test_native_replay_remains_authority_after_topology_capture(tmp_path, monkeypatch):
    raw = source(journal_id=JOURNAL)
    with retained(tmp_path, raw) as (engine, factory):
        complete_capture(factory)
        native = owner._compute_upload_journal_validation
        seen = []

        def observed(*args):
            assert progress(factory)[2] == "complete"
            assert engine.pool.checkedout() == 0
            assert terminal(factory)["anchor"] is None
            seen.append(True)
            return native(*args)

        monkeypatch.setattr(owner, "_compute_upload_journal_validation", observed)
        service_for(factory)._advance_provenance_journal_validation(1, JOURNAL)
        assert seen == [True]
        with validate_journal(raw) as expected:
            assert terminal(factory)["anchor"] == expected.anchor


@pytest.mark.parametrize(
    ("late", "speculative_state"),
    [
        ({"type": "derivation"}, "deferred"),
        ({"type": "retraction", "assertion_id": "fake"}, "complete"),
        ([1], "deferred"),
    ],
)
def test_late_topology_failure_never_outranks_an_earlier_native_error(
    tmp_path, late, speculative_state
):
    document = parse_journal(source(journal_id=JOURNAL))[0].document
    document["sequence"] = "7"
    raw = raw_journal(canonical_json_bytes(document), entry([late]))
    with pytest.raises(ValueError) as expected:
        validate_journal(raw)
    with retained(tmp_path, raw) as (_, factory):
        work = ready(factory)
        finish(factory, work)
        assert progress(factory)[2] == speculative_state
        assert owner._compute_upload_journal_validation(factory, work).failure == str(
            expected.value
        )


@pytest.mark.growth_witness(
    "riverhog-topology-item-scan-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.topology_facts._scan_item",
)
def test_actual_topology_item_scanning_never_revisits_committed_prefix(record_property):
    binding = growth_binding(neutral._scan_item)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    samples = []
    for prefix, padding in [
        (0, 0),
        (17, 0),
        (1025, 0),
        (0, 63),
        (0, 65535),
        (17, 65536),
        (129, 131089),
        (1025, 262145),
    ]:
        target = edge(prefix, padding="x" * padding)
        values = [edge(n) for n in range(prefix)] + [target]
        raw = entry(values)
        array = neutral.plan_topology_frame(raw).relations
        start = array.offset + sum(len(canonical_json_bytes(value)) + 1 for value in values[:-1])
        array = replace(array, offset=start, index=prefix)
        metrics = {"visited_octets": 0, "source_range_octets": 0}

        class ObservedBytes(bytes):
            observed = metrics

            def __iter__(self):
                for value in super().__iter__():
                    self.observed["visited_octets"] += 1
                    yield value

        def read(offset, length, start=start, raw=raw, metrics=metrics, block_type=ObservedBytes):
            assert offset >= start
            block = raw[offset : offset + length]
            metrics["source_range_octets"] += len(block)
            return block_type(block)

        actual, next_offset = neutral._scan_item(read, array)
        assert actual == canonical_json_bytes(target) and next_offset == array.end
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": digest,
            "dimensions": {"item_octets": len(canonical_json_bytes(target))},
            "metrics": {**metrics, "returned_octets": len(actual)},
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        samples.append(sample)
    record_property("growth_samples", samples)

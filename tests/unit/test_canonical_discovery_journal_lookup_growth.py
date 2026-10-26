from __future__ import annotations

import copy
import json
import time
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_journal_lookup as lookup
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import JournalTaskKey, QualifiedJournalSource
from riverhog_provenance import (
    CanonicalCorpusValidator,
    JournalFrame,
    append_correction,
    validate_journal,
)

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import ADMISSION, observe_metadata, open_journal_work
from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_journal_extraction import run
from tests.unit.test_journal_validation_workspace_reference_growth import sample

DOCUMENTARY = lookup.documentary_journal_assertion
EFFECTIVE = lookup.effective_journal_assertion
ABSENT = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"


def repeated_source(replacements):
    fixture = native_scalar_fixture(3)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as initial:
        first = initial.frames[0]
        recorder = first.document["recorded_by_agent_id"]
        row = copy.deepcopy(first.document["body"]["assertions"]["extensions"][0])
        referent = row["id"]
        target = {"entry": first.reference, "assertion_id": row["assertion_id"]}
        row["assertion_id"] = "urn:uuid:" + str(uuid4())
        two = append_correction(
            fixture.raw,
            [target],
            reason="exact same-object replacement",
            recorded_by_agent_id=recorder,
            assertions={"extensions": [row]},
            catalog=fixture.catalog,
        )
    with validate_journal(two, catalog=fixture.catalog) as checked:
        second = checked.frames[1]
        template = second.document
        previous = second.reference
        assertion_id = template["body"]["assertions"]["extensions"][0]["assertion_id"]
        pieces = [two]
    for sequence in range(2, replacements + 1):
        document = copy.deepcopy(template)
        document["id"] = "urn:uuid:" + str(uuid4())
        document["sequence"] = str(sequence)
        document["previous_entry"] = previous
        document["body"]["retracts"] = [{"entry": previous, "assertion_id": assertion_id}]
        assertion_id = "urn:uuid:" + str(uuid4())
        document["body"]["assertions"]["extensions"][0]["assertion_id"] = assertion_id
        frame = JournalFrame(canonical_json_bytes(document))
        pieces.append(frame.encoded)
        previous = frame.reference
    return fixture, b"".join(pieces), referent


@pytest.fixture(scope="module")
def histories(tmp_path_factory):
    root = tmp_path_factory.mktemp("historical-source-lookup")
    with ExitStack() as resources:
        cases = []
        for count in (1, 17, 1025):
            started = time.perf_counter()
            fixture, raw, referent = repeated_source(count)
            print(
                json.dumps(
                    {
                        "phase": "fixture-bytes",
                        "replacements": count,
                        "seconds": time.perf_counter() - started,
                    }
                ),
                flush=True,
            )
            started = time.perf_counter()
            native = resources.enter_context(validate_journal(raw, catalog=fixture.catalog))
            print(
                json.dumps(
                    {
                        "phase": "Native-validation",
                        "replacements": count,
                        "seconds": time.perf_counter() - started,
                    }
                ),
                flush=True,
            )
            started = time.perf_counter()
            source = resources.enter_context(QualifiedJournalSource(native, ADMISSION))
            db = open_journal_work(root / (str(count) + ".sqlite3"))
            resources.callback(db.close)
            key = JournalTaskKey("build", source.source_key, "worker")
            run(JournalFactExtraction(db, key, source), 128)
            print(
                json.dumps(
                    {
                        "phase": "source-facts",
                        "replacements": count,
                        "seconds": time.perf_counter() - started,
                    }
                ),
                flush=True,
            )
            started = time.perf_counter()
            corpus = resources.enter_context(CanonicalCorpusValidator())
            while not corpus.advance_source(native, frame_budget=64).complete:
                pass
            print(
                json.dumps(
                    {
                        "phase": "corpus-source",
                        "replacements": count,
                        "seconds": time.perf_counter() - started,
                    }
                ),
                flush=True,
            )
            other = db.execute(
                "SELECT referent_id FROM discovery_journal_assertions WHERE kind='activity' LIMIT 1"
            ).fetchone()[0]
            anchors = [
                HistoryJournalAnchor.from_mapping(native.anchor_at(i))
                for i in sorted({0, count // 2, count})
            ]
            first = lookup.effective_journal_assertion(db, key, source, anchors[0], referent)
            last = lookup.effective_journal_assertion(db, key, source, anchors[-1], referent)
            cases.append(
                SimpleNamespace(
                    db=db,
                    key=key,
                    source=source,
                    native=native,
                    corpus=corpus,
                    referent=referent,
                    anchors=anchors,
                    first=first,
                    last=last,
                    count=count,
                    other=other,
                )
            )
        yield cases


@contextmanager
def observe(case, monkeypatch):
    with observe_metadata(case.db, monkeypatch) as recorder:
        recorder.metrics.update(
            decoded_octets=0,
            scalar_encoded_octets=0,
            scalar_decoded_octets=0,
            source_checks=0,
            native_reference_calls=0,
            source_payload_octets=0,
        )
        depth = 0

        def child(function, metric):
            def measured(*args, **kwargs):
                nonlocal depth
                if depth:
                    return function(*args, **kwargs)
                before = dict(recorder.metrics)
                depth += 1
                try:
                    return function(*args, **kwargs)
                finally:
                    depth -= 1
                    recorder.metrics.update(before)
                    recorder.metrics[metric] += 1

            return measured

        encode, parse = lookup.format_scalar, lookup.parse_scalar

        def encoded(kind, value):
            result = encode(kind, value)
            recorder.metrics["scalar_encoded_octets"] += len(result.encode())
            return result

        def parsed(kind, value):
            recorder.metrics["scalar_decoded_octets"] += (
                len(value.encode()) if isinstance(value, str) else 0
            )
            return parse(kind, value)

        def forbidden(*args, **kwargs):
            pytest.fail("historical lookup loaded source entry or assertion payload")

        with monkeypatch.context() as patch:
            for name, metric in (
                ("check_open", "source_checks"),
                ("anchor_at", "native_anchor_calls"),
                ("reference_for_assertion", "native_reference_calls"),
            ):
                patch.setattr(case.source, name, child(getattr(case.source, name), metric))
            patch.setattr(lookup, "format_scalar", encoded)
            patch.setattr(lookup, "parse_scalar", parsed)
            patch.setattr(case.source, "entry", forbidden)
            patch.setattr(case.source, "assertion", forbidden)
            patch.setattr(JournalFrame, "document", property(forbidden))
            yield recorder
    for sql, parameters in recorder.statements:
        plan = " ".join(
            row[3].upper() for row in case.db.execute("EXPLAIN QUERY PLAN " + sql, parameters)
        )
        assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan
        if "referent_id=?" in sql:
            assert "DISCOVERY_JOURNAL_HISTORICAL_OBJECTS" in plan, plan


def dimensions(case):
    rows, width = (
        0,
        octets((case.key, case.source.binding_json, case.source.terminal_json)),
    )
    for table in (
        "discovery_journal_tasks",
        "discovery_journal_assertions",
        "discovery_fact_tasks",
    ):
        for row in case.db.execute("SELECT * FROM " + table):
            rows += 1
            width = max(width, octets(row))
    return {"retained_rows": rows, "metadata_octets": width}


def exercise(histories, monkeypatch, record_property, operation):
    samples = []
    for case in histories:
        values = dimensions(case)
        for anchor in case.anchors:
            candidates = (
                (case.first.assertion_id, case.last.assertion_id, ABSENT)
                if operation is DOCUMENTARY
                else (case.referent, case.other, ABSENT)
            )
            for selected in candidates:
                values = dict(
                    values,
                    metadata_octets=max(
                        values["metadata_octets"], octets((case.key, anchor, selected))
                    ),
                )
                for cold in (False, True):
                    for _retry in range(3):
                        if cold:
                            case.db.execute("PRAGMA shrink_memory").close()
                        with observe(case, monkeypatch) as recorder:
                            result = operation(recorder, case.key, case.source, anchor, selected)
                        samples.append(sample(operation, values, recorder.metrics))
                        if operation is EFFECTIVE:
                            expected = case.corpus.historical_assertion(
                                anchor, selected, byte_budget=65536
                            )
                            assert (result is None) == (expected is None)
                            if expected is not None:
                                assert json.loads(result.reference_json) == expected.reference
                        elif selected == ABSENT or (
                            selected == case.last.assertion_id
                            and anchor.through_sequence < case.count
                        ):
                            assert result is None
                        else:
                            assert result is not None
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-original-documentary-header/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_lookup.documentary_journal_assertion",
)
def test_actual_documentary_births_and_retired_headers(histories, monkeypatch, record_property):
    exercise(histories, monkeypatch, record_property, DOCUMENTARY)


@pytest.mark.growth_witness(
    "riverhog-discovery-original-effective-header/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_lookup.effective_journal_assertion",
)
def test_actual_early_prefix_seeks_past_later_object_versions(
    histories, monkeypatch, record_property
):
    exercise(histories, monkeypatch, record_property, EFFECTIVE)


@pytest.mark.growth_witness(
    "riverhog-original-owner-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_owners.next_owner_assertion",
)
def test_original_owner_ranges_seek_dense_native_corrections_without_snapshot_expansion(
    histories, tmp_path, monkeypatch, record_property
):
    import sqlite3

    from riverhog_core import canonical_discovery_journal_owners as owners

    samples = []
    for original in histories:
        path = tmp_path / (str(original.count) + ".sqlite3")
        db = sqlite3.connect(path, isolation_level=None)
        original.db.backup(db)
        case = SimpleNamespace(**vars(original))
        case.db = db
        full = original.anchors[-1]
        middle_id = db.execute(
            "SELECT assertion_id FROM discovery_journal_assertions "
            "WHERE build_id=? AND source_key=? AND referent_id=? AND born_sequence=?",
            (case.key.build_id, case.key.source_key, case.referent, str(case.count // 2)),
        ).fetchone()[0]
        middle = lookup.documentary_journal_assertion(db, case.key, case.source, full, middle_id)
        inputs = dimensions(case)
        inputs["metadata_octets"] = max(
            inputs["metadata_octets"], octets((case.first, middle, case.last))
        )
        scalar = owners.format_scalar
        try:
            for anchor in case.anchors:
                for after in (None, case.first, middle, case.last):
                    for cold in (False, True):
                        if cold:
                            case.db.close()
                            case.db = sqlite3.connect(path, isolation_level=None)
                        for _retry in range(3):
                            with observe(case, monkeypatch) as recorder:

                                def encoded(kind, value, _scalar=scalar, _recorder=recorder):
                                    result = _scalar(kind, value)
                                    _recorder.metrics["scalar_encoded_octets"] += len(
                                        result.encode()
                                    )
                                    return result

                                with monkeypatch.context() as patch:
                                    patch.setattr(owners, "format_scalar", encoded)
                                    if (
                                        after is not None
                                        and after.born_sequence > anchor.through_sequence
                                    ):
                                        from riverhog_core.canonical_discovery_journal_work import (
                                            StaleJournalTask,
                                        )

                                        with pytest.raises(StaleJournalTask):
                                            owners.next_owner_assertion(
                                                recorder,
                                                case.key,
                                                case.source,
                                                anchor,
                                                case.referent,
                                                after=after,
                                            )
                                        selected = None
                                    else:
                                        selected = owners.next_owner_assertion(
                                            recorder,
                                            case.key,
                                            case.source,
                                            anchor,
                                            case.referent,
                                            after=after,
                                        )
                            samples.append(
                                sample(owners.next_owner_assertion, inputs, recorder.metrics)
                            )
                            if selected is not None:
                                assert selected.referent_id == case.referent
                                assert selected.born_sequence <= anchor.through_sequence
                                assert after is None or selected.born_sequence > after.born_sequence
            with observe(case, monkeypatch) as recorder:
                assert (
                    owners.next_owner_assertion(recorder, case.key, case.source, full, ABSENT)
                    is None
                )
            samples.append(sample(owners.next_owner_assertion, inputs, recorder.metrics))
        finally:
            case.db.close()
    record_property("growth_samples", samples)

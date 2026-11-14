from __future__ import annotations

import base64
import copy
import hashlib
import sqlite3
from collections import defaultdict
from uuid import uuid4

import pytest
from riverhog_core.collection_production_validation import validate_collection_production_records
from riverhog_core.services import primary_production as owner
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_PRODUCTION_CONTRACT_ID,
    COLLECTION_RECORD_FRAGMENT_BYTES_MAX,
    PRODUCER_SCHEMA_ID,
    RECORD_FRAGMENT_SCHEMA_ID,
    RECORD_MANIFEST_SCHEMA_ID,
    collection_production_profile,
)
from riverhog_provenance import JournalValidationSession, ProvenanceValidationError

from tests.unit.test_imported_history_growth import _sample

CONTEXT = "urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31"


def records(size=COLLECTION_RECORD_FRAGMENT_BYTES_MAX + 11):
    raw = b"x" * size
    digest = hashlib.sha256(raw).hexdigest()

    def extension(name, schema, data):
        return {
            "id": "urn:uuid:" + str(uuid4()),
            "assertion_id": "urn:uuid:" + str(uuid4()),
            "property": COLLECTION_PRODUCTION_CONTRACT_ID + "/" + name,
            "subject": {"scope": "local", "object_id": CONTEXT, "object_type": "context"},
            "value": {"type": "json", "value": collection_production_profile(schema, data)},
        }

    rows = [
        extension(
            "producer",
            PRODUCER_SCHEMA_ID,
            {"source_context_sha256": digest, "source_context_bytes": str(size)},
        ),
        extension(
            "record-manifest",
            RECORD_MANIFEST_SCHEMA_ID,
            {
                "record_kind": "source-context",
                "record_sha256": digest,
                "total_bytes": str(size),
                "part_count": str(
                    (size + COLLECTION_RECORD_FRAGMENT_BYTES_MAX - 1)
                    // COLLECTION_RECORD_FRAGMENT_BYTES_MAX
                ),
            },
        ),
    ]
    for offset in range(0, size, COLLECTION_RECORD_FRAGMENT_BYTES_MAX):
        rows.append(
            extension(
                "record-fragment",
                RECORD_FRAGMENT_SCHEMA_ID,
                {
                    "record_kind": "source-context",
                    "record_sha256": digest,
                    "total_bytes": str(size),
                    "offset": str(offset),
                    "data_base64": base64.b64encode(
                        raw[offset : offset + COLLECTION_RECORD_FRAGMENT_BYTES_MAX]
                    ).decode(),
                },
            )
        )
    return rows


def finish(work, rows):
    # Arrival order is unrelated to fragment offset order.
    for row in reversed(rows):
        assert work.advance(row) is False
    for _ in range(4 * len(rows) + 10):
        if work.advance():
            assert work.complete
            return
    pytest.fail("producer record preparation failed to finish its finite input")


@pytest.mark.growth_witness(
    "riverhog-primary-production-atoms/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.primary_production.PrimaryProductionWork.advance",
)
def test_producer_atoms_match_original_validator_and_bound_each_continuation(
    monkeypatch, record_property
):
    rows = records()
    validate_collection_production_records({"extensions": rows}, delivery_context_id=CONTEXT)
    samples, measured = [], defaultdict(int)
    actual_ingest = owner.PrimaryProductionWork._ingest
    actual_fragment = owner.PrimaryProductionWork._fragment
    actual_hash = owner.CheckpointSHA256.update
    actual_advance = owner.PrimaryProductionWork.advance
    active = False

    def ingest(self, row):
        measured["extension_visits"] += 1
        return actual_ingest(self, row)

    def fragment(self, state):
        before = measured["record_hash_octets"]
        result = actual_fragment(self, state)
        measured["fragment_visits"] += int(measured["record_hash_octets"] > before)
        return result

    def update(self, data):
        if active:
            measured["record_hash_octets"] += len(data)
        return actual_hash(self, data)

    def forbidden(*args, **kwargs):
        pytest.fail("producer preparation attempted Native semantic replay")

    monkeypatch.setattr(owner.PrimaryProductionWork, "_ingest", ingest)
    monkeypatch.setattr(owner.PrimaryProductionWork, "_fragment", fragment)
    monkeypatch.setattr(owner.CheckpointSHA256, "update", update)
    monkeypatch.setattr(JournalValidationSession, "advance", forbidden)
    with sqlite3.connect(":memory:", isolation_level=None) as db:
        owner.install(db)
        work = owner.PrimaryProductionWork(db, "exact-source-anchor-context")

        def step(self, row=None):
            nonlocal active
            measured.clear()
            db.set_trace_callback(
                lambda _: measured.__setitem__("sql_statements", measured["sql_statements"] + 1)
            )
            active = True
            try:
                result = actual_advance(self, row)
            finally:
                active = False
                db.set_trace_callback(None)
            samples.append(
                _sample(
                    actual_advance,
                    {
                        "atom_octets": max(len(str(item).encode()) for item in rows),
                        "retained_rows": 4 * len(rows),
                    },
                    measured,
                )
            )
            return result

        monkeypatch.setattr(owner.PrimaryProductionWork, "advance", step)
        finish(work, rows)
    assert sum(sample["metrics"]["record_hash_octets"] for sample in samples) == (
        COLLECTION_RECORD_FRAGMENT_BYTES_MAX + 11
    )
    assert sum(sample["metrics"]["extension_visits"] for sample in samples) == len(rows)
    record_property("growth_samples", samples)


@pytest.mark.parametrize(
    "fault",
    [
        "missing-fragment",
        "missing-manifest",
        "part-count",
        "gap",
        "content",
        "producer-digest",
        "duplicate-manifest",
        "orphan-fragment",
        "bad-profile",
        "base64",
        "extra-source-manifest",
        "fragment-total",
    ],
)
def test_streaming_producer_preserves_original_rejections(fault):
    rows = records()
    producer = rows[0]["value"]["value"]["data"]
    manifest = rows[1]["value"]["value"]["data"]
    fragment = rows[2]["value"]["value"]["data"]
    if fault == "missing-fragment":
        rows.pop()
    elif fault == "missing-manifest":
        rows.pop(1)
    elif fault == "part-count":
        manifest["part_count"] = "3"
    elif fault == "gap":
        fragment["offset"] = "1"
    elif fault == "content":
        fragment["data_base64"] = "eA=="
    elif fault == "producer-digest":
        producer["source_context_sha256"] = "0" * 64
    elif fault == "duplicate-manifest":
        extra = copy.deepcopy(rows[1])
        extra["assertion_id"] = "urn:uuid:" + str(uuid4())
        rows.append(extra)
    elif fault == "orphan-fragment":
        fragment["record_kind"] = "unselected-record"
    elif fault == "bad-profile":
        rows[0]["value"]["value"]["profile"]["contract_sha256"] = "0" * 64
    elif fault == "base64":
        fragment["data_base64"] = "%%%"
    elif fault == "fragment-total":
        fragment["total_bytes"] = "0"
    else:
        extra = copy.deepcopy(rows[1])
        extra["assertion_id"] = "urn:uuid:" + str(uuid4())
        extra["value"]["value"]["data"]["record_sha256"] = "0" * 64
        rows.append(extra)
    with pytest.raises(ProvenanceValidationError):
        validate_collection_production_records({"extensions": rows}, delivery_context_id=CONTEXT)
    with sqlite3.connect(":memory:", isolation_level=None) as db:
        owner.install(db)
        work = owner.PrimaryProductionWork(db, "exact-source-anchor-context")
        with pytest.raises(ProvenanceValidationError):
            finish(work, rows)
        assert not work.complete


def test_original_intake_can_be_received_again_without_duplicating_a_record():
    rows = records(19)
    with sqlite3.connect(":memory:", isolation_level=None) as db:
        owner.install(db)
        work = owner.PrimaryProductionWork(db, "original-selection")
        assert work.advance(rows[1]) is False
        assert work.advance(rows[1]) is False
        finish(work, rows)
        assert work.advance() is True

"""Focused semantic witnesses for the disposable parent-publication experiment."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from copy import deepcopy
from pathlib import Path

import pytest
from native_execution_experiment import (
    Measurements,
    _VolatileSession,
    capture_examples,
    observed_session,
    result_evidence,
    run_journal,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance import ProvenanceValidationError, iter_journal_frames, new_id
from riverhog_provenance.journal_session import JournalValidationBudget
from riverhog_provenance_contracts import PROFILE, ContractCatalog


@pytest.fixture(scope="module")
def catalog():
    return ContractCatalog()


@pytest.fixture(scope="module")
def captured(catalog):
    return capture_examples(catalog)


def finish(session, *, rows=64):
    while not session.advance(JournalValidationBudget(rows=rows)).complete:
        pass


@pytest.mark.parametrize("example", ["small", "correction", "journal_checkpoint"])
@pytest.mark.parametrize("rows", [1, 64])
def test_same_captured_bytes_preserve_full_native_result(captured, catalog, example, rows):
    raw = captured[example]
    baseline = run_journal(raw, volatile=False, catalog=catalog, rows=rows)
    experiment = run_journal(raw, volatile=True, catalog=catalog, rows=rows)
    assert "success" in baseline["outcome"]
    assert experiment["outcome"] == baseline["outcome"]
    evidence = experiment["outcome"]["success"]
    assert evidence["sha256"] == hashlib.sha256(raw).hexdigest()
    assert evidence["octets"] == len(raw)
    old, new = baseline["measurements"], experiment["measurements"]
    assert old["parent_checkpoint_sql"] > 0
    assert old["parent_checkpoint_commits"] > 0
    assert new["parent_checkpoint_sql"] == new["parent_checkpoint_writes"] == 0
    assert new["parent_checkpoint_commits"] == 0
    assert new["parent_encoding_octets"] < old["parent_encoding_octets"]
    assert new["parent_encoding_octets"] > 0  # Real EntryReplay._start still encodes.
    assert new["commit_attempts"] == old["commit_attempts"] > 0
    assert new["transaction_commits"] == old["transaction_commits"] > 0
    assert new["native_advance_calls"] == old["native_advance_calls"] > 20
    assert new["final_semantic_assertions"] == old["final_semantic_assertions"] > 0
    assert new["physical_sync_calls"] is old["physical_sync_calls"] is None


@pytest.mark.parametrize(
    "name",
    [
        "never-named-emission",
        "mixed-storage-history",
        "reported-prehistory-and-correction",
        "independent-journal-fork",
        "simultaneous-deliveries",
    ],
)
def test_fixed_formal_examples_match_baseline_and_expected_materialization(catalog, name):
    examples = Path(__file__).resolve().parents[2] / "packages/riverhog-provenance/examples"
    raw = (examples / (name + ".jsonseq")).read_bytes()
    expected = json.loads((examples / (name + ".materialized.json")).read_text())
    baseline = run_journal(raw, volatile=False, catalog=catalog, rows=64)
    experiment = run_journal(raw, volatile=True, catalog=catalog, rows=64)
    assert "success" in baseline["outcome"]
    assert experiment["outcome"] == baseline["outcome"]
    actual = experiment["outcome"]["success"]
    assert actual["materialized"] == expected
    assert actual["sha256"] == hashlib.sha256(raw).hexdigest()
    assert actual["octets"] == len(raw)
    assert experiment["measurements"]["parent_checkpoint_sql"] == 0
    catalog.validate(PROFILE + "/materialized.schema.json", expected)


@pytest.mark.parametrize("fault", ["recorder", "sequence", "journal_checkpoint"])
def test_real_native_rejection_is_identical(captured, catalog, fault):
    raw = captured["journal_checkpoint" if fault == "journal_checkpoint" else "small"]
    frames = list(iter_journal_frames((raw,)))
    position = -1 if fault == "journal_checkpoint" else 0
    document = deepcopy(frames[position].document)
    if fault == "recorder":
        document["recorded_by_agent_id"] = new_id()
    elif fault == "sequence":
        document["sequence"] = "1"
    else:
        document["body"]["prefix_sha256"] = "0" * 64
    encoded = [frame.encoded for frame in frames]
    encoded[position] = b"\x1e" + canonical_json_bytes(document) + b"\n"
    invalid = b"".join(encoded)
    baseline = run_journal(invalid, volatile=False, catalog=catalog)
    experiment = run_journal(invalid, volatile=True, catalog=catalog)
    assert baseline["outcome"]["error_type"] == "ProvenanceValidationError"
    assert experiment["outcome"] == baseline["outcome"]


@pytest.mark.parametrize("volatile", [False, True])
@pytest.mark.parametrize("same_chunk", [False, True])
def test_semantic_failure_precedes_later_malformed_input(captured, catalog, volatile, same_chunk):
    frame = next(iter_journal_frames((captured["small"],)))
    document = deepcopy(frame.document)
    document["recorded_by_agent_id"] = new_id()
    invalid = b"\x1e" + canonical_json_bytes(document) + b"\n"
    pulls = []

    def chunks():
        pulls.append(1)
        yield invalid + (b"later malformed bytes" if same_chunk else b"")
        pulls.append(2)
        yield b"later malformed bytes"

    with observed_session(
        chunks(), volatile=volatile, measurements=Measurements(), catalog=catalog
    ) as session:
        with pytest.raises(ProvenanceValidationError, match="recorder"):
            finish(session, rows=1)
        assert pulls == [1]
        assert session._ledger.load().checkpoint.frame_ordinal == 0
        with pytest.raises(RuntimeError, match="session failed"):
            session.advance()
        assert pulls == [1]


@pytest.mark.parametrize("volatile", [False, True])
def test_original_independent_result_lifetime_is_preserved(captured, catalog, volatile):
    with observed_session(
        (captured["correction"],),
        volatile=volatile,
        measurements=Measurements(),
        catalog=catalog,
    ) as session:
        finish(session)
        one, two = session.result(), session.result()
        expected = result_evidence(one)
        directory = session._store._storage.directory
        one.close()
    assert directory.exists()
    assert result_evidence(two) == expected
    two.close()
    assert not directory.exists()


def test_live_parent_does_not_advance_before_private_commit(captured, catalog):
    with observed_session(
        (captured["small"],), volatile=True, measurements=Measurements(), catalog=catalog
    ) as session:
        while session._ledger.load().checkpoint.phase != "entry-admission":
            session.advance()
        before = session._ledger.load()
        session._store.db.fail_next_commit = True
        with pytest.raises(sqlite3.OperationalError, match="injected pre-commit failure"):
            session.advance()
        assert session._ledger.load() == before
        assert session._ledger._pending is None
        with closing(session._store.db.execute("SELECT count(*) FROM identities")) as cursor:
            assert cursor.fetchone() == (0,)
        with closing(
            session._store.db.execute(
                "SELECT count(*) FROM sqlite_master WHERE name='journal_validation'"
            )
        ) as cursor:
            assert cursor.fetchone() == (0,)


def test_experiment_never_claims_retained_resume():
    with pytest.raises(NotImplementedError, match="does not admit retained recovery"):
        _VolatileSession.open_retained(None, None)

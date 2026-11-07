from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing, contextmanager
from copy import deepcopy
from dataclasses import replace

import pytest
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import (
    ProvenanceValidationError,
    append_checkpoint,
    encode_entry,
    parse_journal,
    validate_journal,
)
from riverhog_provenance._entry_replay import EntryReplay, ReplayBudget
from riverhog_provenance._entry_replay import install as install_replay
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint, ValidationCheckpoint
from riverhog_provenance._session_checkpoint import install as install_parent
from riverhog_provenance.journal_store import JournalStore
from test_child_admission_growth import octets
from test_entry_replay import owned_summary
from test_entry_replay_growth import (
    REGIMES,
    ReplayConnection,
    finish_entry,
    observe_replay,
    owner,
)
from test_entry_replay_growth import admission_histories as admission_histories
from test_semantic_edge import observed_work
from test_semantic_owner import assert_uncommitted_hidden, snapshot

# History/current widths, exact native checkpoint padding and anchor-position regimes.
PUBLICATION_CASES = (
    (REGIMES[0], "none", None),
    (REGIMES[1], "none", None),
    (REGIMES[2], "none", None),
    (REGIMES[3], "none", None),
    (REGIMES[4], "none", None),
    (REGIMES[5], "none", None),
    (REGIMES[6], "none", None),
    (REGIMES[7], "none", None),
    (REGIMES[8], "none", None),
    (REGIMES[10], "matched", 55),
    (REGIMES[10], "matched", 56),
    (REGIMES[3], "prior", None),
    (REGIMES[3], "future", None),
    (REGIMES[11], "none", None),
    (REGIMES[12], "none", None),
)


def restore(store, case, catalog, *, anchor=None):
    if anchor is None:
        with closing(sqlite3.connect(case["path"])) as seed:
            seed.backup(store.db)
        current = owner(store, catalog, case["source_binding"])
    else:
        install_parent(store.db)
        install_replay(store.db)
        current = owner(store, catalog, case["source_binding"], expected_anchor=anchor)
        current.prefix.initialize()
        current.ledger.initialize()
        for frame in parse_journal(case["raw"][: -len(case["candidate"].encoded)]):
            current.admit(frame)
            finish_entry(current)
    if case["epoch"] != 2:
        ledger = current.ledger.replace_worker(case["epoch"])
        current = EntryReplay(store, prefix=current.prefix, ledger=ledger, catalog=catalog)
    return current


@contextmanager
def publication_fixture(build, specification, catalog):
    regime, mode, tail = specification
    case = dict(build(regime))
    candidate = case["candidate"]
    prefix_raw = case["raw"][: -len(candidate.encoded)]
    if tail is not None:
        document = deepcopy(candidate.document)
        for padding in range(64):
            document["body"]["purpose"] = "urn:test:publication:" + "x" * padding
            candidate = parse_journal(encode_entry(document, catalog=catalog))[0]
            if (len(prefix_raw) + len(candidate.encoded)) % 64 == tail:
                break
        else:
            pytest.fail("native SHA padding-boundary fixture was not found")
    case["candidate"] = candidate
    case["raw"] = prefix_raw + candidate.encoded
    anchor = None
    if mode != "none":
        anchored_raw = case["raw"]
        through = candidate
        if mode == "prior":
            anchored_raw = prefix_raw
            through = parse_journal(prefix_raw)[-1]
        elif mode == "future":
            anchored_raw = append_checkpoint(
                case["raw"],
                recorded_by_agent_id=case["who"],
                purpose="urn:test:future-publication",
                catalog=catalog,
            )
            through = parse_journal(anchored_raw)[-1]
        anchor = {
            "journal_id": through.document["journal_id"],
            "through": through.reference,
            "prefix_sha256": hashlib.sha256(anchored_raw).hexdigest(),
            "prefix_bytes": str(len(anchored_raw)),
        }
    with validate_journal(case["raw"], catalog=catalog) as expected:
        case["expected_document"] = expected.materialize()
    with closing(JournalStore()) as store:
        current = restore(store, case, catalog, anchor=anchor)
        current.admit(candidate)
        for _ in range(100000):
            if current.ledger.load().checkpoint.phase == "publish":
                break
            current.advance(ReplayBudget(rows=64))
        else:
            pytest.fail("the finite native publication fixture did not reach publish")
        before = current.ledger.load()
        # Data-only prepared cache is detached from the setup pager. Native child
        # checks really ran, and the persisted parent retains their exact outcome.
        cached = replace(
            current._current,
            child_kind=None,
            child_id=None,
            child=None,
            row_key=None,
            row_sha256=None,
            row=None,
            atom=None,
        )
        publication = current.prefix.prepare_publication(cached.prepared)
        after = ValidationCheckpoint(
            version=before.checkpoint.version + 1,
            frame_ordinal=before.checkpoint.frame_ordinal + 1,
        )
        prefix_after = (
            current.prefix.attempt_id,
            current.prefix.binding,
            publication.after.to_json(),
        )
        original = snapshot(store.db)
        padding_blocks = 0
        if mode == "matched":
            padding_blocks = 1 if publication.after.byte_count % 64 < 56 else 2
        width = 1024 + 2 * octets(
            (before, after, prefix_after, current.prefix.binding, str(case["epoch"]))
        )
        dimensions = {
            "retained_rows": sum(len(rows) for rows in original.values()),
            "checkpoint_retained_rows": sum(
                len(original[name]) for name in ("journal_validation", "journal_prefix", "frames")
            ),
            "metadata_row_octets": width,
            "retained_frames": before.checkpoint.frame_ordinal,
            "frame_octets": len(candidate.encoded),
            "padding_blocks": padding_blocks,
        }
        case.update(
            before=before,
            after=after,
            cached=cached,
            anchor=anchor,
            original=original,
            dimensions=dimensions,
            publication=publication,
        )
        yield store, case


@contextmanager
def measured_store(store, case, catalog):
    path = store._storage.directory / "snapshot.sqlite3"
    store.db.close()
    store._storage.connection = sqlite3.connect(path, factory=ReplayConnection)
    store.db.execute("PRAGMA synchronous=FULL").close()
    store.db.execute("PRAGMA cache_size=-64").close()
    current = owner(
        store, catalog, case["source_binding"], case["epoch"], expected_anchor=case["anchor"]
    )
    current._current = case["cached"]
    yield current, path


def assert_published(current, case):
    assert current._current is None and not current.db.in_transaction
    assert current.store.frame_count == case["after"].frame_ordinal
    assert current.ledger.load().checkpoint == case["after"]
    assert current.prefix.load() == case["publication"].after
    changed = snapshot(current.db)
    assert not changed["journal_validation_frame"]
    assert {
        name: rows
        for name, rows in changed.items()
        if name
        not in {"frames", "journal_prefix", "journal_validation", "journal_validation_frame"}
    } == {
        name: rows
        for name, rows in case["original"].items()
        if name
        not in {"frames", "journal_prefix", "journal_validation", "journal_validation_frame"}
    }
    accepted = current.prefix.load()
    if case["anchor"] and not accepted.matched_anchor:
        with pytest.raises(ProvenanceValidationError, match="does not contain the expected"):
            current.prefix.finish()
    else:
        assert current.prefix.finish() == accepted
    with owned_summary(current.store, accepted) as actual:
        assert actual.materialize() == case["expected_document"]
        assert actual.journal_sha256 == hashlib.sha256(case["raw"]).hexdigest()
        assert actual.journal_bytes == len(case["raw"])


def publication_samples(catalog, build, *, samples, cases=PUBLICATION_CASES):
    binding = growth_binding(EntryReplay._publish)
    projection = project_bindings((binding,))
    for specification in cases:
        with publication_fixture(build, specification, catalog) as (store, case):
            with measured_store(store, case, catalog) as (current, path):
                with observe_replay(
                    store.db, catalog, case["dimensions"], root_operation=EntryReplay._publish
                ) as (observed, complete):
                    with store.db.observe():
                        result = current._publish(case["before"], current._current)
                primitive = observed_work(store.db, {key: 0 for key in store.db.metrics}, 0, 0)
                primitive["changed_rows"] = store.db.metrics["changed_rows"]
                sample = complete(observed, primitive)
                validate_execution_samples(projection, binding.declaration.id, [sample])
                samples.append(sample)
                assert result.checkpoint == case["after"]
                with closing(sqlite3.connect(path)) as reader:
                    assert snapshot(reader) == snapshot(store.db)
                assert_published(current, case)
    assert len(samples) == len(cases)


@pytest.mark.growth_witness(
    "riverhog-entry-replay-publication-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._entry_replay.EntryReplay._publish",
)
def test_actual_cold_atomic_entry_publication(catalog, admission_histories, record_property):
    samples = []
    record_property("growth_samples", samples)
    publication_samples(catalog, admission_histories, samples=samples)


@pytest.mark.parametrize("cold", [False, True])
def test_advance_uses_the_atomic_publication_boundary(
    catalog, admission_histories, monkeypatch, cold
):
    with publication_fixture(admission_histories, PUBLICATION_CASES[0], catalog) as (store, case):
        with measured_store(store, case, catalog) as (current, _path):
            if cold:
                current._current = None
            real_publish = current._publish
            calls = []

            def publish(before, cached):
                calls.append((before, cached.prepared.frame))
                return real_publish(before, cached)

            monkeypatch.setattr(current, "_publish", publish)
            assert current.advance().checkpoint == case["after"]
            assert calls == [(case["before"], case["candidate"])]
            assert_published(current, case)


def test_failed_publication_rolls_back_all_writes_and_retains_cache(
    catalog, admission_histories, monkeypatch
):
    with publication_fixture(admission_histories, PUBLICATION_CASES[1], catalog) as (store, case):
        with measured_store(store, case, catalog) as (current, path):
            real_publish = current.ledger._publish

            def fail_after_write(*args, **kwargs):
                result = real_publish(*args, **kwargs)
                assert_uncommitted_hidden(path, case["original"])
                assert result.checkpoint == case["after"]
                raise StaleValidationCheckpoint("superseded before publication completed")

            with monkeypatch.context() as patch:
                patch.setattr(current.ledger, "_publish", fail_after_write)
                with pytest.raises(StaleValidationCheckpoint, match="superseded"):
                    current._publish(case["before"], current._current)
            assert not store.db.in_transaction and snapshot(store.db) == case["original"]
            assert current._current is case["cached"]
            current._publish(case["before"], current._current)
            assert_published(current, case)

"""The public producer writes members and their canonical history by opaque ID."""

from __future__ import annotations

import hashlib
import json
import os
from collections import OrderedDict
from types import SimpleNamespace
from typing import Any

import pytest
import riverhog_client.producer as producer_module
from riverhog_client.producer import IncrementalCollectionProducer, ProducerStream
from riverhog_protocol import CollectionArtifactProvenanceBindingDocument
from riverhog_protocol.artifact_identity import ArtifactId

from tests.support.prepared_primary import custody_receipt
from tests.support.prepared_primary_producer import PreparedProducerApi


@pytest.mark.skipif(os.name != "posix", reason="POSIX producer allocation fixture")
def test_producer_registers_pathless_member_and_exact_canonical_history(
    primary_store, monkeypatch: Any
) -> None:
    monkeypatch.setattr(producer_module, "upload_collection_units", lambda *a, **k: None)
    content = b"exact original bytes"
    artifact_id = ArtifactId("a" * 64)
    api = PreparedProducerApi()
    producer = IncrementalCollectionProducer(
        api,  # type: ignore[arg-type]
        primary_store=primary_store,
        member_inventory=lambda: (),
        producer_app="a-test-producer",
        adapter_id="test-adapter/v1",
        adapter_version="1",
        ingest_source="test",
        source_event_id="event-1",
        source_context={"source": "fixture"},
    )
    try:
        receipts = producer.append_inputs(
            (
                ProducerStream(
                    artifact_id=artifact_id,
                    bytes=len(content),
                    sha256=hashlib.sha256(content).hexdigest(),
                    read_range=lambda start, size: content[start : start + size],
                    materialization_hint=("source.txt",),
                ),
            )
        )
        assert receipts == ()
        assert api.registered[0]["artifact_id"] == artifact_id
        assert "path" not in api.registered[0]
        assert len(api.journals) == 1
        assert api.bindings[0]["artifact_id"] == artifact_id
        assert api.decisions == [
            {
                "artifact_id": artifact_id,
                "materialization_hint": {"components": ["source.txt"]},
                "allow_missing_materialization_hint": False,
            }
        ]
    finally:
        producer.stop()


@pytest.mark.skipif(os.name != "posix", reason="SQL producer composition uses the Unix VFS")
def test_public_producer_prepares_sql_before_remote_creation_and_cold_replays_native(
    tmp_path, record_testsuite_property
):
    from functools import partial

    from riverhog_client._prepared_primary_factory import sqlite_primary_preparation_step
    from riverhog_client._prepared_primary_sqlite import (
        SqlitePrimaryLimits,
        TransactionalPrimaryFiles,
    )
    from riverhog_client._prepared_primary_sqlite_preparation import SqliteReadinessPlan

    from tests.support.prepared_primary import SqlitePrimaryTestGuard
    from tests.support.prepared_primary_producer import witness
    from tests.support.prepared_primary_replay import DESCRIPTOR

    def factory(path, *, allocation_descriptor, initialize):
        assert allocation_descriptor == DESCRIPTOR
        if initialize:
            path.touch(exist_ok=False)
        return TransactionalPrimaryFiles(
            SqlitePrimaryTestGuard(path),
            allocation_descriptor=allocation_descriptor,
            limits=SqlitePrimaryLimits(4 * 1024**2, 2 * 1024**2, 128 * 1024),
            initialize=initialize,
        )

    plan = SqliteReadinessPlan((8201, 16), 65536, 8, 192, 512 * 1024)
    proof = witness(
        tmp_path / "producer.sqlite3",
        factory,
        partial(sqlite_primary_preparation_step, plan=plan),
    )
    assert proof["root_deferred"] >= 1 and proof["member_calls"] >= 1
    assert proof["observations"] == proof["remote_create_calls"] == 1
    assert proof["provenance_connections"] == proof["provenance_close_calls"] == 4
    assert proof["provenance_connection_peak"] == proof["provenance_connections_released"] == 1
    assert proof["provenance_handles"] == 8 and proof["provenance_replay_opens"] == 0
    record_testsuite_property(
        "producer_provenance_admission",
        json.dumps({key: value for key, value in proof.items() if key.startswith("provenance_")}),
    )


class _MemoryPendingSourceIndex:
    """Scheduling stand-in; real byte-owner durability is exercised separately."""

    def __init__(self):
        self.pending = OrderedDict()

    @property
    def count(self):
        return len(self.pending)

    def get(self, key):
        return self.pending.get(key)

    def record(self, source, *, payload_sealed):
        key = source.member.artifact_id
        old = self.pending.get(key)
        if old is not None:
            assert old.source == source
            payload_sealed = payload_sealed or old.payload_sealed
        self.pending[key] = SimpleNamespace(source=source, payload_sealed=payload_sealed)
        return True

    def rotate_front(self, key):
        assert next(iter(self.pending)) == key
        self.pending.move_to_end(key)

    def retire(self, key):
        self.pending.pop(key, None)

    def rows(self):
        yield from self.pending.values()


class _CountedPendingSources(producer_module._PendingSources):
    scanned = 0

    def __init__(self):
        super().__init__(_MemoryPendingSourceIndex())

    def pending_values(self):
        for source in super().pending_values():
            self.scanned += 1
            yield source


def _pending_custody_producer(members, *, sealed=True):
    """Exercise scheduling and real receipt decoding with synthetic primary authority."""
    sources = _CountedPendingSources()
    originals = {}
    for index in range(members):
        content = b"x" * (128 // members + (index < 128 % members))
        member = ArtifactId(f"{index + 1:064x}")
        source = producer_module._Source(
            member, len(content), hashlib.sha256(content).hexdigest(), None, True, content=content
        )
        sources[member] = source
        sources.admit(source)
        originals[member] = (source.bytes, source.sha256)

    class Api:
        calls = None
        seen = None
        mode = None
        grant_custody = False

        def __init__(self):
            self.calls = []
            self.seen = set()

        def register_collection_upload_session_artifacts(self, collection_id, artifacts, **options):
            assert (
                collection_id == 42 and options["registration_constraints"] is producer.constraints
            )
            self.calls.append(tuple(row["artifact_id"] for row in artifacts))
            self.seen.update(self.calls[-1])
            if self.mode == "error":
                raise OSError("fixture registration failure")
            rows = [dict(row, payload_sealed=sealed) for row in artifacts]
            if self.grant_custody:
                for row in rows:
                    row["custody_receipt"] = custody_receipt(
                        42, row, primary(row["artifact_id"])
                    ).model_dump(mode="json")
            if self.mode == "changed":
                rows[0]["sha256"] = "f" * 64
            elif self.mode == "omitted":
                rows.pop()
            elif self.mode == "unexpected":
                rows.append(dict(rows[0], artifact_id="f" * 64))
            return {"artifacts": rows, "volumes": []}

    def primary(member):
        return CollectionArtifactProvenanceBindingDocument.model_validate(
            {
                "artifact_id": member,
                "journal": {
                    "journal_id": "urn:uuid:aaaabbbb-1234-4234-8234-123456789abc",
                    "through": {
                        "entry_id": "urn:uuid:aaaabbbb-4321-4321-8321-123456789abc",
                        "sequence": "0",
                        "json_sha256": "a" * 64,
                    },
                    "prefix_bytes": "1",
                    "prefix_sha256": hashlib.sha256(b"J").hexdigest(),
                },
                "delivery_association_id": "urn:uuid:bbbbaaaa-1234-4234-8234-123456789abc",
            }
        )

    class Primary:
        def __init__(self):
            self.accepted = []
            self.reclaimed = []

        def load_controls(self, member):
            assert (member.bytes, member.sha256) == originals[member.artifact_id]
            return member

        def accept_custody(self, member, receipt):
            assert receipt.artifact_id == member.artifact_id
            assert receipt.primary == primary(member.artifact_id)
            self.accepted.append(member.artifact_id)

        def reclaim(self, member):
            self.reclaimed.append(member.artifact_id)

    producer = object.__new__(IncrementalCollectionProducer)
    producer._sources = sources
    producer.collection_id = 42
    producer.constraints = SimpleNamespace(pack_member_bytes=1024, raw_part_plaintext_bytes=65536)
    producer.api = api = Api()
    producer.primary_store = store = Primary()
    return producer, api, store, originals


@pytest.mark.parametrize("members", [1, 16, 17, 129, 1000])
@pytest.mark.parametrize("sealed", [False, True])
def test_custody_poll_bounds_each_window_and_total_registration_work(members, sealed):
    producer, api, store, originals = _pending_custody_producer(members, sealed=sealed)
    window = producer_module.COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES
    polls = (members + window - 1) // window
    for _ in range(polls):
        before_calls, before_scanned = len(api.calls), producer._sources.scanned
        assert producer._reconcile_pending_sources() == ()
        assert len(api.calls) == before_calls + 1
        assert producer._sources.scanned - before_scanned <= window
        assert len(api.calls[-1]) <= window
    assert api.seen == set(originals)
    assert sum(map(len, api.calls)) <= members + window - 1
    assert producer._sources.scanned <= members + window - 1
    assert {
        key: (value.bytes, value.sha256) for key, value in producer._sources.items()
    } == originals
    assert len(producer._sources) == members
    assert store.accepted == store.reclaimed == []
    assert all((source.content is None) == sealed for _, source in producer._sources.items())


def test_custody_poll_with_no_pending_members_opens_no_registration():
    producer, api, store, _ = _pending_custody_producer(0)
    assert producer._reconcile_pending_sources() == ()
    assert api.calls == [] and producer._sources.scanned == 0
    assert store.accepted == store.reclaimed == []


def test_custody_poll_rotates_past_waiting_members_and_includes_new_arrivals():
    producer, api, store, originals = _pending_custody_producer(17)
    assert producer._reconcile_pending_sources() == ()
    member = ArtifactId(f"{18:064x}")
    producer._sources[member] = producer_module._Source(
        member, 0, hashlib.sha256(b"").hexdigest(), None, True, content=b""
    )
    producer._sources.admit(producer._sources[member])
    for _ in range(2):
        assert producer._reconcile_pending_sources() == ()
    assert api.seen == {*originals, member}
    assert all(len(batch) <= 16 for batch in api.calls)
    assert len(producer._sources) == 18 and store.accepted == store.reclaimed == []


@pytest.mark.parametrize("mode", ["changed", "omitted", "unexpected"])
def test_custody_poll_keeps_exact_pending_identities_after_invalid_registration(mode):
    producer, api, store, originals = _pending_custody_producer(17)
    api.mode = mode
    before = tuple(producer._sources)
    with pytest.raises(RuntimeError, match="changed|omitted|unexpected"):
        producer._reconcile_pending_sources()
    assert len(api.calls) == 1 and tuple(producer._sources) == before
    assert {
        key: (value.bytes, value.sha256) for key, value in producer._sources.items()
    } == originals
    assert store.accepted == store.reclaimed == []


def test_custody_poll_retries_the_exact_window_after_registration_failure():
    producer, api, store, originals = _pending_custody_producer(17)
    api.mode = "error"
    before = tuple(producer._sources)
    with pytest.raises(OSError, match="fixture registration failure"):
        producer._reconcile_pending_sources()
    assert tuple(producer._sources) == before
    api.mode = None
    assert producer._reconcile_pending_sources() == ()
    assert api.calls[1] == api.calls[0]
    assert producer._reconcile_pending_sources() == ()
    assert api.seen == set(originals)
    assert store.accepted == store.reclaimed == []


def test_custody_poll_without_registration_constraints_does_no_source_work():
    producer, api, store, _ = _pending_custody_producer(17)
    producer.constraints = None
    with pytest.raises(RuntimeError, match="no registration constraints"):
        producer._reconcile_pending_sources()
    assert api.calls == [] and producer._sources.scanned == 0
    assert store.accepted == store.reclaimed == []


def test_custody_poll_returns_late_receipts_in_bounded_windows_without_early_release():
    producer, api, store, originals = _pending_custody_producer(17)
    assert producer._reconcile_pending_sources() == ()
    assert len(producer._sources) == 17 and store.accepted == store.reclaimed == []
    api.grant_custody = True
    first = producer._reconcile_pending_sources()
    assert len(first) == 16 and len(producer._sources) == 1
    second = producer._reconcile_pending_sources()
    assert len(second) == 1 and not producer._sources
    assert {row.artifact.artifact_id for row in (*first, *second)} == set(originals)
    assert len(store.accepted) == len(store.reclaimed) == 17
    assert set(store.accepted) == set(store.reclaimed) == set(originals)


@pytest.mark.parametrize("members", [1, 16, 17, 129, 1000])
def test_sealed_pending_metadata_bounds_live_sources(
    members, monkeypatch, record_testsuite_property
):
    import weakref

    original_type = producer_module._Source
    live = weakref.WeakValueDictionary()
    working_set = {"peak_live_sources": 0}

    class TrackedSource(original_type):
        __slots__ = ("__weakref__",)

        def __init__(self, *args, **kwargs):
            original_type.__init__(self, *args, **kwargs)
            live[id(self)] = self
            working_set["peak_live_sources"] = max(working_set["peak_live_sources"], len(live))

    monkeypatch.setattr(producer_module, "_Source", TrackedSource)
    producer, api, store, originals = _pending_custody_producer(0)
    for index in range(members):
        content = b"x" * (128 // members + (index < 128 % members))
        member = ArtifactId(f"{index + 1:064x}")
        digest = hashlib.sha256(content).hexdigest()
        originals[member] = (len(content), digest)
        source = TrackedSource(member, len(content), digest, None, True, content=content)
        producer._sources[member] = source
        producer._sources.admit(source)
        assert (
            producer._accept_registered_rows(
                iter(
                    (
                        {
                            "artifact_id": member,
                            "bytes": str(len(content)),
                            "sha256": digest,
                            "payload_sealed": True,
                        },
                    )
                ),
                expected=(source,),
            )
            == ()
        )
        del source
    window = producer_module.COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES
    assert working_set["peak_live_sources"] <= 2 * window + 8, (members, working_set)
    assert not store.accepted and not store.reclaimed
    for _ in range((members + window - 1) // window):
        assert producer._reconcile_pending_sources() == ()
    assert api.seen == set(originals)
    assert all(len(batch) <= window for batch in api.calls)
    assert len(producer._sources) == members
    assert not store.accepted and not store.reclaimed
    api.grant_custody = True
    receipts = []
    for _ in range((members + window - 1) // window):
        receipts.extend(producer._reconcile_pending_sources())
    assert {row.artifact.artifact_id for row in receipts} == set(originals)
    assert not producer._sources
    assert set(store.accepted) == set(store.reclaimed) == set(originals)
    assert working_set["peak_live_sources"] <= 2 * window + 8, (members, working_set)
    record_testsuite_property(
        f"sealed_pending_source_working_set_{members}", json.dumps(working_set)
    )


def test_pending_metadata_capacity_retains_the_live_source_before_durable_sealing(monkeypatch):
    from riverhog_client import PrimaryStateCapacity

    index = _MemoryPendingSourceIndex()
    sources = producer_module._PendingSources(index)
    closed = []

    class Source(producer_module._Source):
        def close(self):
            closed.append(self.artifact_id)
            super().close()

    source = Source(
        ArtifactId("c" * 64), 1, hashlib.sha256(b"x").hexdigest(), None, True, content=b"x"
    )
    sources[source.artifact_id] = source
    sources.admit(source)
    record = index.record

    def refuse(metadata, *, payload_sealed):
        if payload_sealed:
            raise PrimaryStateCapacity("injected pending source admission refusal")
        return record(metadata, payload_sealed=payload_sealed)

    with monkeypatch.context() as patch:
        patch.setattr(index, "record", refuse)
        with pytest.raises(PrimaryStateCapacity, match="source admission"):
            sources.record_registration(source, payload_sealed=True)
    assert sources[source.artifact_id] is source
    assert closed == []
    sources.record_registration(source, payload_sealed=True)
    assert closed == [source.artifact_id]
    assert sources[source.artifact_id].content is None
    assert len(sources) == 1


def test_sealed_pending_retry_preserves_the_original_hint_commitment():
    index = _MemoryPendingSourceIndex()
    sources = producer_module._PendingSources(index)
    source = producer_module._Source(
        ArtifactId("d" * 64),
        1,
        hashlib.sha256(b"x").hexdigest(),
        ("source.txt",),
        False,
        content=b"x",
    )
    sources[source.artifact_id] = source
    sources.admit(source)
    sources.record_registration(source, payload_sealed=True)
    assert sources.matches_registration(source)
    changed = producer_module._Source(
        source.artifact_id, source.bytes, source.sha256, ("changed.txt",), False, content=b"x"
    )
    assert not sources.matches_registration(changed)

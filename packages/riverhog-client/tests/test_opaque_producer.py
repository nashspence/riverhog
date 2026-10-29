"""The public producer writes members and their canonical history by opaque ID."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

import pytest
import riverhog_client.producer as producer_module
from riverhog_client.producer import IncrementalCollectionProducer, ProducerStream
from riverhog_protocol.artifact_identity import ArtifactId

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

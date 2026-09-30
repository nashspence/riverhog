from __future__ import annotations

import hashlib
import threading
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_client import ProducerFile
from riverhog_client import producer as owner
from riverhog_protocol import ArtifactId


def _producer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    producer = object.__new__(owner.IncrementalCollectionProducer)
    producer._sources = {}
    producer._restored_sources = OrderedDict()
    producer._source_lock = threading.RLock()
    producer._pending_source_resolver = None
    producer._require_heartbeat = lambda: None
    producer._reconcile_pending_sources = lambda: ()
    producer._heartbeat_stop = threading.Event()
    producer._heartbeat_thread = None
    producer._needs_upload_scan = True
    producer.progress = None
    producer.collection_id = 1
    producer.constraints = SimpleNamespace(raw_part_plaintext_bytes=65536)
    producer.api = SimpleNamespace(spawn=lambda: None)
    paths = {}
    units = []
    for index in range(20):
        member = ArtifactId(format(index + 1, "064x"))
        path = tmp_path / str(member)
        path.write_bytes(b"payload:" + bytes([index]))
        paths[member] = path
        units.append(
            SimpleNamespace(
                sources=[
                    SimpleNamespace(
                        artifact_id=member,
                        artifact_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                        offset=0,
                        bytes=9,
                    )
                ]
            )
        )
    producer.set_pending_source_resolver(
        lambda member: ProducerFile(paths[member], member, allow_missing_materialization_hint=True)
    )

    def upload(_api, _collection_id, *, content_for_unit, **_kwargs):
        for index, unit in enumerate(units):
            assert content_for_unit(unit) == b"payload:" + bytes([index])
            assert len(producer._restored_sources) <= 8
            assert all(source.content is None for source in producer._restored_sources.values())
        return 20

    monkeypatch.setattr(owner, "upload_collection_units", upload)
    return producer, paths, units


def test_pending_source_restart_reads_exact_files_with_a_bounded_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    producer, _paths, _units = _producer(tmp_path, monkeypatch)
    try:
        assert producer._upload_available() == ()
        assert len(producer._restored_sources) == 8
    finally:
        producer.stop()
    assert not producer._restored_sources


@pytest.mark.parametrize("change", ["member", "digest", "bytes"])
def test_pending_source_restart_cannot_substitute_identity_or_changed_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    producer, paths, units = _producer(tmp_path, monkeypatch)
    unit = units[0]
    member = unit.sources[0].artifact_id
    if change == "member":
        producer.set_pending_source_resolver(
            lambda _member: ProducerFile(
                paths[units[1].sources[0].artifact_id],
                units[1].sources[0].artifact_id,
                allow_missing_materialization_hint=True,
            )
        )
    elif change == "digest":
        unit.sources[0].artifact_sha256 = "f" * 64
    else:
        paths[member].write_bytes(b"different")

    def upload(_api, _collection_id, *, content_for_unit, **_kwargs):
        content_for_unit(unit)
        raise AssertionError("changed source must not reach upload")

    monkeypatch.setattr(owner, "upload_collection_units", upload)
    try:
        with pytest.raises((RuntimeError, ValueError), match="changed|artifact ID"):
            producer._upload_available()
    finally:
        producer.stop()

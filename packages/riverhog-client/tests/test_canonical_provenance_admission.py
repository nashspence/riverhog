from __future__ import annotations

import gc
import hashlib
import shutil
import uuid
from io import BytesIO
from pathlib import Path

import pytest
from riverhog_client import canonical_production as member_owner
from riverhog_client.canonical_completion import (
    CompletionRecord,
    build_completion_journal,
    write_completion_journal,
)
from riverhog_client.canonical_production import (
    ProducerAttribution,
    bind_produced_member,
    build_member_journal,
)
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionRequirementDocument,
)
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    JournalStore,
    common,
    journal,
    journal_store,
)

from tests.support.sqlite_admission import Capacity, Population


@pytest.fixture
def paths(tmp_path, monkeypatch):
    make, paths = journal_store.mkdtemp, []

    def created(**options):
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(journal_store, "mkdtemp", created)
    return paths


@pytest.fixture
def member_material(monkeypatch):
    content = b"exact measured original"
    observed = BoundedSourceObserver().observe(BytesSource(content))
    values = dict(
        member=ArtifactMemberIdentityDocument(
            artifact_id="a3" * 32,
            bytes=str(len(content)),
            sha256=hashlib.sha256(content).hexdigest(),
        ),
        observation=observed,
        delivery_context_id="urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31",
        attribution=ProducerAttribution(
            "fixture", "fixture/v1", "1", "event-1", "fixture", {"native": "é" * 70000}, "f2" * 32
        ),
        materialization_hint=("source", "exact.bin"),
    )
    count = 0

    def next_id():
        nonlocal count
        count += 1
        return "urn:uuid:" + str(uuid.UUID(f"00000000-0000-4000-8000-{count:012x}"))

    def reset():
        nonlocal count
        count = 0

    monkeypatch.setattr(common, "new_id", next_id)
    monkeypatch.setattr(journal, "new_id", next_id)
    monkeypatch.setattr(member_owner, "new_id", next_id)
    monkeypatch.setattr(journal, "utc_now", lambda: "2026-10-30T00:00:00Z")
    return values, reset


class Publication:
    def __init__(self, population=None):
        self.population, self.calls, self.raw = population, [], None

    def observe(self, name):
        if self.population is not None:
            assert self.population.active == 0
        self.calls.append(name)

    def upload_collection_upload_session_provenance_journal(
        self, collection_id, journal_id, *, content, byte_count, sha256
    ):
        self.observe("upload")
        self.raw = b"".join(content)
        assert len(self.raw) == byte_count and hashlib.sha256(self.raw).hexdigest() == sha256

    def bind_collection_upload_session_artifact_provenance(self, collection_id, batch):
        self.observe("bind")
        self.binding = batch.bindings[0]

    def set_collection_upload_session_materialization_decisions(self, collection_id, batch):
        self.observe("decide")


def member(publish, material, *, api=None, **ports):
    values, reset = material
    reset()
    if publish:
        return bind_produced_member(
            api, collection_id=12, allow_missing_materialization_hint=False, **values, **ports
        )
    return build_member_journal(**values, **ports)


@pytest.mark.parametrize("publish", [False, True])
def test_canonical_member_composes_four_admitted_connections_and_exact_publication(
    member_material, paths, publish
):
    expected = member(publish, member_material, api=Publication())
    population, handles = Population(limit=1), []
    api = Publication(population)
    actual = member(
        publish,
        member_material,
        api=api,
        connection_factory=population,
        on_storage_open=handles.append,
    )
    assert actual == expected and population.active == 0 and population.peak == 1
    assert len(population.connections) == 4 and len(handles) == 8
    assert all(isinstance(handle, JournalStore) for handle in handles)
    assert all(db.close_calls == 1 for db in population.connections)
    assert all(not path.exists() for path in paths)
    if publish:
        assert api.calls == ["upload", "bind", "decide"]
        assert api.raw == actual.content and api.binding == actual.binding


@pytest.mark.parametrize("stage", [1, 2, 3, 4])
@pytest.mark.parametrize("after", [False, True])
def test_member_close_failure_keeps_actual_reader_and_blocks_remote_publication(
    member_material, paths, stage, after
):
    population, handles = Population(limit=1), []
    api = Publication(population)
    original = member_material[0]["observation"].graph_json

    def retained(store):
        handles.append(store)
        if len(handles) == 2 * stage:
            store._storage.connection.close_failures = 1
            store._storage.connection.close_after = after

    with pytest.raises(OSError, match="close unconfirmed"):
        member(
            True, member_material, api=api, connection_factory=population, on_storage_open=retained
        )
    gc.collect()
    assert not api.calls and population.active == 1 and len(population.connections) == stage
    assert paths[-1].is_dir() and all(not path.exists() for path in paths[:-1])
    assert (
        not handles[-1]._lease.closed and member_material[0]["observation"].graph_json == original
    )
    handles[-1].close()
    assert population.active == 0 and population.connections[-1].close_calls == 2
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize("stage", [1, 2, 3, 4])
def test_member_connection_refusal_releases_confirmed_work_and_preserves_factory_custody(
    member_material, paths, stage
):
    population, handles = Population(limit=1), []
    api = Publication(population)

    def admitted(database, **options):
        population.refuse = len(population.attempts) == stage - 1
        return population(database, **options)

    with pytest.raises(Capacity):
        member(
            True,
            member_material,
            api=api,
            connection_factory=admitted,
            on_storage_open=handles.append,
        )
    assert not api.calls and population.active == 0 and len(population.connections) == stage - 1
    assert len(handles) == 2 * (stage - 1) and all(handle._lease.closed for handle in handles)
    assert all(not path.exists() for path in paths[:-1])
    assert paths[-1].is_dir() and not list(paths[-1].iterdir())
    shutil.rmtree(paths[-1])


@pytest.mark.parametrize("after", [None, False, True])
def test_binding_failure_keeps_the_final_validation_owner_until_close_confirms(
    member_material, paths, monkeypatch, after
):
    population, handles = Population(limit=1), []
    api = Publication(population)

    def rejected(_value):
        assert population.active == 1 and len(handles) == 8
        if after is not None:
            population.connections[-1].close_failures = 1
            population.connections[-1].close_after = after
        raise ValueError("binding validation rejected")

    monkeypatch.setattr(
        member_owner.CollectionArtifactProvenanceBindingDocument, "model_validate", rejected
    )
    with pytest.raises(ValueError if after is None else OSError):
        member(
            True,
            member_material,
            api=api,
            connection_factory=population,
            on_storage_open=handles.append,
        )
    assert not api.calls and len(population.connections) == 4
    if after is not None:
        assert population.active == 1 and paths[-1].is_dir() and not handles[-1]._lease.closed
        handles[-1].close()
    assert population.active == 0 and all(not path.exists() for path in paths)


def completion(stream, *, destination=None, changed=False, **ports):
    raw = b'{"optional":null,"quality":1.2300}'
    records = [CompletionRecord.from_bytes(kind, raw) for kind in COMPLETION_REQUIRED_RECORD_KINDS]
    if changed:
        first = records[0]
        records[0] = CompletionRecord(first.kind, first.bytes, "0" * 64, first.read)
    values = dict(
        requirement=CollectionCompletionRequirementDocument(
            execution_id="a" * 64,
            execution_envelope_sha256="b" * 64,
            controller_evidence_sha256="c" * 64,
            record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
        ),
        records=records,
        execution_sha256="d" * 64,
        output_bindings_sha256="e" * 64,
        input_history_bindings_sha256="f" * 64,
        disposition_set_sha256="0" * 64,
        producer_app="fixture",
        producer_version="1",
        journal_id="urn:uuid:ba123103-fd14-4813-b2d9-b83859e36f31",
        recorded_at="2026-10-30T00:00:00Z",
    )
    if stream:
        destination = BytesIO() if destination is None else destination
        return write_completion_journal(destination, **values, **ports), destination
    return build_completion_journal(**values, **ports)


@pytest.mark.parametrize("stream", [False, True])
def test_completion_preserves_exact_preimages_and_admitted_reader_ownership(paths, stream):
    expected = completion(False)
    population, handles = Population(limit=1), []
    result = completion(stream, connection_factory=population, on_storage_open=handles.append)
    assert len(population.connections) == 3 and len(handles) == 6 and population.peak == 1
    if stream:
        summary, destination = result
        assert destination.getvalue() == expected and destination.tell() == 0
        assert population.active == 1 and summary._store is handles[-1]
        assert summary.journal_bytes == len(expected)
        summary.close()
    else:
        assert result == expected
    assert population.active == 0 and all(db.close_calls == 1 for db in population.connections)
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("stage", [1, 2, 3])
def test_completion_refusal_preserves_factory_directory_and_releases_returned_handles(
    paths, stream, stage
):
    population, handles = Population(limit=1), []

    def admitted(database, **options):
        population.refuse = len(population.attempts) == stage - 1
        return population(database, **options)

    with pytest.raises(Capacity):
        completion(stream, connection_factory=admitted, on_storage_open=handles.append)
    assert population.active == 0 and len(population.connections) == stage - 1
    assert len(handles) == 2 * (stage - 1) and all(handle._lease.closed for handle in handles)
    assert all(not path.exists() for path in paths[:-1])
    assert paths[-1].is_dir() and not list(paths[-1].iterdir())
    shutil.rmtree(paths[-1])


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("after", [False, True])
def test_completion_final_close_retains_actual_reader_without_implicit_retry(paths, stream, after):
    population, handles = Population(limit=1), []

    def retained(store):
        handles.append(store)
        if len(handles) == 6:
            store._storage.connection.close_failures = 1
            store._storage.connection.close_after = after

    with pytest.raises(OSError, match="close unconfirmed"):
        result = completion(stream, connection_factory=population, on_storage_open=retained)
        assert stream
        result[0].close()
    gc.collect()
    assert population.active == 1 and paths[-1].is_dir() and not handles[-1]._lease.closed
    assert all(not path.exists() for path in paths[:-1])
    handles[-1].close()
    assert population.active == 0 and population.connections[-1].close_calls == 2
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize("stream", [False, True])
def test_completion_admission_preserves_sealed_preimage_validation(paths, stream):
    population, handles = Population(limit=1), []
    with pytest.raises(ValueError, match="sealed identity"):
        completion(
            stream, changed=True, connection_factory=population, on_storage_open=handles.append
        )
    assert population.active == 0 and len(population.connections) == 2
    assert all(handle._lease.closed for handle in handles)
    assert all(not path.exists() for path in paths)

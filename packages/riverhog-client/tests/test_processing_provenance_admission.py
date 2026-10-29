from __future__ import annotations

import gc
import hashlib
import shutil
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import canonical_json_sha256
from riverhog_client.canonical_completion import CompletionRecord
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_client.completion_records import CompletionRecords
from riverhog_client.processing import CollectionTransformRuntime, DerivedCollectionSpec
from riverhog_client.processing import writer as owner
from riverhog_protocol import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionRecordingDocument,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.collection_record_preimages import CollectionRecordPreimages
from riverhog_protocol.collection_workflows import (
    ArtifactDispositionSetIdentity,
    CollectionRootIdentity,
    OperationIdentity,
    RecipeIdentity,
)
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    journal_store,
    reference,
    validate_journal,
)

from tests.support.sqlite_admission import Capacity, Population

EXECUTION = "4" * 64
CONTEXT = "urn:uuid:11111111-1111-4111-8111-111111111111"
EVIDENCE = {"execution_id": EXECUTION}
SPEC = DerivedCollectionSpec(
    inputs=(CollectionRootIdentity(1, "1" * 64, "2" * 64),),
    recipe=RecipeIdentity("fixture/v1", 1, "a" * 64),
    operation=OperationIdentity("fixture/v1", "b" * 64),
)
REQUIREMENT = CollectionCompletionRequirementDocument(
    execution_id=EXECUTION,
    execution_envelope_sha256="c" * 64,
    controller_evidence_sha256=canonical_json_sha256(EVIDENCE),
    record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
)
PREIMAGE = b'{"optional":null,"quality":1.2300}'
SUPPLIED = tuple(
    CompletionRecord.from_bytes(kind, PREIMAGE)
    for kind in (
        "implementation",
        "invocation",
        "target-execution",
        "target-output-declarations",
        "target-result",
    )
)
DISPOSITION = ArtifactDispositionSetIdentity(2, 2, 2, "6" * 64)


class Transport:
    """Controlled claim/member transport; provenance validation remains real."""

    def __init__(self, material, population=None):
        self.material = material
        self.population = population
        self.uploads, self.reservations, self.import_reads = [], [], []
        self.fail_upload = False
        self.changed = None

    def get_processing_claim(self, _claim):
        return SimpleNamespace(
            plan=SimpleNamespace(
                execution_id=EXECUTION,
                output_policy=SPEC.output_policy,
                inputs=SimpleNamespace(sha256="7" * 64),
                artifacts=SimpleNamespace(sha256="8" * 64),
            )
        )

    def get_collection_upload_session(self, _collection):
        return {"state": "open", "construction_identity_sha256": "c" * 64}

    def list_collection_upload_session_artifacts(self, _collection, **_options):
        return {
            "artifacts": [m.model_dump(mode="json") for m, _ in self.material],
            "next_page_token": None,
        }

    def get_collection_upload_session_artifact_provenance_binding(self, _collection, artifact):
        result = next(p.binding for m, p in self.material if m.artifact_id == artifact)
        if self.changed == "anchor":
            return result.model_copy(
                update={"journal": result.journal.model_copy(update={"prefix_sha256": "0" * 64})}
            )
        return result

    @contextmanager
    def stream_collection_upload_session_provenance_journal(self, _collection, journal):
        raw = next(p.content for _, p in self.material if p.journal_id == journal)
        yield iter((raw + b"extra" if self.changed == "tail" else raw,))

    def get_collection_upload_session_member_history_inputs(self, _collection, artifact):
        if self.population is not None:
            assert self.population.active == 0
        self.import_reads.append(artifact)
        return SimpleNamespace(model_dump=lambda **_: {"fixture_import": str(artifact)})

    def list_processing_claim_dispositions(self, *_args, **_options):
        return SimpleNamespace(
            next_ordinal=None, model_dump=lambda **_: {"fixture": "dispositions"}
        )

    def list_processing_claim_disposition_outputs(self, *_args, **_options):
        return SimpleNamespace(next_ordinal=None, model_dump=lambda **_: {"fixture": "outputs"})

    def reserve_collection_upload_session_completion_recording(self, _collection, request):
        if self.population is not None:
            assert self.population.active == 0
        self.reservations.append(request)
        return CollectionCompletionRecordingDocument(
            **request.model_dump(),
            journal_id="urn:uuid:99999999-9999-4999-8999-999999999999",
            recorded_at="2026-10-01T00:00:00.000000000Z",
        )

    def upload_collection_upload_session_provenance_journal(
        self,
        _collection,
        journal,
        *,
        content,
        byte_count,
        sha256,
        selection_role,
    ):
        if self.population is not None:
            assert self.population.active == 0
        raw = b"".join(content)
        assert len(raw) == byte_count and hashlib.sha256(raw).hexdigest() == sha256
        assert selection_role == "completion"
        if self.fail_upload:
            raise ConnectionError("upload response lost")
        self.uploads.append((journal, raw))

    def close(self):
        pass


@pytest.fixture(scope="module")
def material(tmp_path_factory):
    # Only immutable observed journal bytes survive setup; measured validation stays per case.
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv(
            "RIVERHOG_PROVENANCE_STATE_HOME",
            str(tmp_path_factory.mktemp("processing-material") / "user-state"),
        )
        return _observed_material()


def _observed_material():
    result = []
    for index in (1, 2):
        content = b"exact derived output" + bytes((index,))
        member = ArtifactMemberIdentityDocument(
            artifact_id=f"{index:064x}",
            bytes=str(len(content)),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        produced = build_member_journal(
            member=member,
            observation=BoundedSourceObserver().observe(BytesSource(content)),
            delivery_context_id=CONTEXT,
            attribution=ProducerAttribution(
                "fixture",
                "fixture/v1",
                "1",
                "event",
                "fixture",
                {},
                "c" * 64,
                REQUIREMENT,
            ),
            materialization_hint=None,
            output_id=f"output-{index}",
        )
        result.append((member, produced))
    return tuple(result)


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
def composed(monkeypatch):
    # Isolate the writer's producer boundary, keeping real member/completion validation.
    class Producer:
        collection_id = 7
        delivery_context_id = CONTEXT

        def __init__(self, _api, **options):
            self.options = options

        def stop(self):
            pass

    monkeypatch.setattr(owner, "IncrementalCollectionProducer", Producer)

    def construct(api, *, runtime=False, **ports):
        values = dict(
            primary_store=object(),
            member_inventory=lambda: (),
            spec=SPEC,
            claim_id="claim",
            fence=1,
            work_id="3" * 64,
            execution_id=EXECUTION,
            controller_evidence=EVIDENCE,
            producer_app="fixture",
            producer_version="1",
            **ports,
        )
        if runtime:
            execution = CollectionTransformRuntime(api, **values)
            writer = execution.open_incremental_publication(execution_envelope_sha256="c" * 64)
            assert (
                execution.open_incremental_publication(execution_envelope_sha256="c" * 64) is writer
            )
        else:
            writer = owner.IncrementalDerivedCollectionWriter(
                api,
                execution_envelope_sha256="c" * 64,
                **values,
            )
        for key, value in ports.items():
            assert writer.producer.options[key] is value
        return writer

    return construct


def seal(writer):
    writer._seal_completion(SUPPLIED[2].sha256, DISPOSITION, SUPPLIED, None)


@pytest.mark.parametrize("runtime", [False, True])
def test_processing_completion_releases_each_reader_and_preserves_exact_bytes(
    material,
    paths,
    composed,
    runtime,
):
    expected = Transport(material)
    seal(composed(expected))
    population, handles = Population(limit=1), []
    api = Transport(material, population)
    writer = composed(
        api,
        runtime=runtime,
        provenance_connection_factory=population,
        on_provenance_storage_open=handles.append,
    )
    seal(writer)
    assert api.uploads == expected.uploads and api.reservations == expected.reservations
    assert len(population.connections) == 5 and len(handles) == 10
    assert population.peak == 1 and population.active == 0
    assert all(db.close_calls == 1 for db in population.connections)
    assert len(api.import_reads) == 2 and all(not path.exists() for path in paths)
    with validate_journal(api.uploads[0][1], require_profiles=False) as summary:
        subject = reference(summary.graph["activities"][0]["id"], "activity")
        with CollectionRecordPreimages(summary.graph["extensions"], subject=subject) as records:
            records.validate(expected_kinds=REQUIREMENT.record_kinds)
            assert b"".join(records.chunks("target-execution")) == PREIMAGE


@pytest.mark.parametrize("stage", [1, 2, 3, 4, 5])
def test_processing_refuses_before_publication_and_retains_factory_custody(
    material,
    paths,
    composed,
    stage,
):
    population, handles = Population(limit=1), []

    def admitted(database, **options):
        population.refuse = len(population.attempts) == stage - 1
        return population(database, **options)

    api = Transport(material, population)
    writer = composed(
        api,
        runtime=True,
        provenance_connection_factory=admitted,
        on_provenance_storage_open=handles.append,
    )
    with pytest.raises(Capacity):
        seal(writer)
    assert not api.uploads and population.active == 0
    assert len(population.connections) == stage - 1 and len(handles) == 2 * (stage - 1)
    assert len(api.reservations) == (0 if stage <= 2 else 1)
    assert all(not path.exists() for path in paths[:-1])
    assert paths[-1].is_dir() and not list(paths[-1].iterdir())
    shutil.rmtree(paths[-1])


@pytest.mark.parametrize("stage", [1, 2, 3, 4, 5])
@pytest.mark.parametrize("after", [False, True])
def test_processing_uncertain_close_blocks_next_reader_or_completion_upload(
    material,
    paths,
    composed,
    stage,
    after,
):
    population, handles = Population(limit=1), []

    def retained(store):
        handles.append(store)
        if len(handles) == 2 * stage:
            store._storage.connection.close_failures = 1
            store._storage.connection.close_after = after

    api = Transport(material, population)
    writer = composed(
        api,
        runtime=True,
        provenance_connection_factory=population,
        on_provenance_storage_open=retained,
    )
    with pytest.raises(OSError, match="close unconfirmed"):
        seal(writer)
    gc.collect()
    assert not api.uploads and population.active == 1 and population.peak == 1
    assert len(population.attempts) == stage and len(handles) == 2 * stage
    assert all(not path.exists() for path in paths[:-1]) and paths[-1].is_dir()
    handles[-1].close()
    assert population.active == 0 and population.connections[-1].close_calls == 2
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize("changed", ["anchor", "tail", "requirement"])
def test_processing_admission_preserves_member_validation_before_completion(
    material,
    paths,
    composed,
    changed,
):
    population, handles = Population(limit=1), []
    api = Transport(material, population)
    api.changed = changed
    writer = composed(
        api, provenance_connection_factory=population, on_provenance_storage_open=handles.append
    )
    if changed == "requirement":
        writer.requirement = REQUIREMENT.model_copy(update={"execution_id": "d" * 64})
    with CompletionRecords() as records, pytest.raises(ValueError):
        writer._record_output_bindings(records)
    assert not api.import_reads and not api.reservations and not api.uploads
    assert len(population.connections) == 1 and population.active == 0
    assert all(not path.exists() for path in paths)


def test_processing_upload_failure_releases_validation_and_retries_exact_preimages(
    material,
    paths,
    composed,
):
    population, handles = Population(limit=1), []
    api = Transport(material, population)
    writer = composed(
        api,
        runtime=True,
        provenance_connection_factory=population,
        on_provenance_storage_open=handles.append,
    )
    api.fail_upload = True
    with pytest.raises(ConnectionError, match="response lost"):
        seal(writer)
    assert population.active == 0 and len(population.connections) == 5 and not api.uploads
    api.fail_upload = False
    seal(writer)
    assert len(api.uploads) == 1 and api.reservations[0] == api.reservations[1]
    assert population.active == 0 and population.peak == 1
    assert len(population.connections) == 10 and len(handles) == 20
    assert all(not path.exists() for path in paths)

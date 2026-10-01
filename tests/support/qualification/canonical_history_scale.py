"""Real HTTP/encrypted-custody witnesses for shared-history index rebuilding."""

from __future__ import annotations

import gc
import hashlib
import time
import tracemalloc
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
from riverhog_api.app import create_app
from riverhog_api.deps import ServiceContainer
from riverhog_archive_contracts import (
    HistoryJournalAnchor,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
    MemberHistoryRoot,
)
from riverhog_client import ApiClient
from riverhog_client.producer import IncrementalCollectionProducer, ProducerFile
from riverhog_core.catalog_db import create_catalog_engine, session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectRecord, CollectionUploadRecord
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexSnapshotRecord as Snapshot,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as State,
)
from riverhog_protocol import ArtifactId, MemberHistoryBindingBatchDocument
from riverhog_provenance import (
    assertion,
    create_journal,
    external_reference,
    new_id,
    validate_journal,
)
from sqlalchemy import delete, func, select, text
from sqlalchemy.engine import make_url

from tests.support.uploaded_archive import UploadedArchiveStore
from tests.unit.test_operation_lifecycle_api import _api, _container

MAX_REBUILD_PEAK_BYTES = 64 * 1024 * 1024
MAX_REBUILD_SECONDS = 900
SHARED_PREDICATE = "https://example.invalid/qualification/member-evidence"


@dataclass
class SharedHistoryFixture:
    container: ServiceContainer
    api: ApiClient
    collection_id: int
    artifact_ids: tuple[str, ...]
    generation_id: str
    shared_bytes: int
    journal_bytes: int
    expected_snapshots: int
    shared_journal_id: str


def publish_shared_history(
    directory: Path, *, members: int, database_url: str | None = None
) -> SharedHistoryFixture:
    """Publish independent primaries and one genuinely member-attached shared root."""
    directory.mkdir(parents=True, exist_ok=True)
    container = _container(directory, database_url=database_url)
    container.bootstrap_token = "shared-history-qualification"
    transport = TestClient(create_app(container=container))
    bootstrap = _api(transport, container.bootstrap_token, workers=container)
    key = bootstrap.create_app_key(
        "shared-history-qualification", access=[{"permission": "*", "resource": "*"}]
    )
    api = _api(transport, key["token"], workers=container)
    sources = []
    for offset in range(members):
        source = directory / f"source-{offset}.bin"
        source.write_bytes(f"exact payload {offset}".encode())
        sources.append(
            ProducerFile(
                source,
                ArtifactId(uuid4().hex + uuid4().hex),
                allow_missing_materialization_hint=True,
            )
        )
    primaries = []
    claims = []
    who = new_id()
    producer = IncrementalCollectionProducer(
        api,
        producer_app="shared-history-qualification",
        adapter_id="qualification-ingress/v1",
        adapter_version="1",
        ingest_source="qualification",
        source_event_id=uuid4().hex,
        use_cache=False,
    )
    try:
        producer.append_inputs(sources)
        collection_id = int(producer.collection_id)
        journal_bytes = 0
        for offset, source in enumerate(sources):
            binding = api.get_collection_upload_session_artifact_provenance_binding(
                collection_id, source.artifact_id
            )
            assert binding is not None
            with api.stream_collection_upload_session_provenance_journal(
                collection_id, binding.journal.journal_id
            ) as chunks:
                raw = b"".join(chunks)
            journal_bytes += len(raw)
            summary = validate_journal(raw, require_profiles=False)
            state = summary.graph_validation.objects[binding.delivery_association_id]["state"]
            claims.append(
                assertion(
                    "extension",
                    who,
                    subject=external_reference(summary, state["object_id"]),
                    property=SHARED_PREDICATE,
                    value={"type": "text", "value": f"member-token-{offset:06d}:" + "x" * 2048},
                )
            )
            primaries.append((source, binding))
        shared = create_journal(
            {
                "agents": [
                    assertion(
                        "agent", who, object_id=who, kind="software", name="qualification producer"
                    )
                ],
                "extensions": claims,
            },
            recorded_by_agent_id=who,
        )
        shared_summary = validate_journal(shared, require_profiles=False)
        api.upload_collection_upload_session_provenance_journal(
            collection_id,
            shared_summary.journal_id,
            content=(shared,),
            byte_count=len(shared),
            sha256=hashlib.sha256(shared).hexdigest(),
        )
        for source, binding in primaries:
            with MemberHistoryBuilder(
                artifact_id=source.artifact_id,
                bytes=source.source.stat().st_size,
                sha256=hashlib.sha256(source.source.read_bytes()).hexdigest(),
                primary=MemberHistoryPrimary.from_mapping(
                    {
                        "journal": binding.journal.model_dump(mode="json"),
                        "delivery_association_id": binding.delivery_association_id,
                    }
                ),
            ) as histories:
                histories.add_root(
                    MemberHistoryRoot(
                        HistoryJournalAnchor.from_mapping(shared_summary.anchor), "bound"
                    )
                )
                selected, descriptor = histories.seal()
                for authority in (descriptor.roots, descriptor.imports):
                    for page in histories.pages(authority):
                        api.stage_collection_upload_session_history_structure(
                            collection_id, page.to_json_bytes()
                        )
                api.stage_collection_upload_session_history_structure(
                    collection_id, descriptor.to_json_bytes()
                )
                api.bind_collection_upload_session_member_histories(
                    collection_id,
                    MemberHistoryBindingBatchDocument.model_validate(
                        {"bindings": [selected.to_mapping()]}
                    ),
                )
        finalized = producer.finish(poll_seconds=0.05, timeout_seconds=900)
        assert finalized.archive_root_sha256 is not None
    finally:
        producer.stop()
    with session_scope(container.session_factory) as session:
        state = session.get(State, collection_id)
        assert state is not None and state.active_build_id is not None
        generation = session.get(Generation, state.active_build_id)
        assert generation is not None and generation.generation_id is not None
        generation_id = generation.generation_id
        expected_snapshots = int(
            session.scalar(
                select(func.count())
                .select_from(Snapshot)
                .where(Snapshot.build_id == state.active_build_id)
            )
            or 0
        )
        # Remove all producer scratch. The following rebuild must use custody.
        session.execute(
            delete(CollectionUploadRecord).where(
                CollectionUploadRecord.collection_id == collection_id
            )
        )
    for source in sources:
        source.source.unlink()
    return SharedHistoryFixture(
        container,
        api,
        collection_id,
        tuple(str(s.artifact_id) for s in sources),
        generation_id,
        len(shared),
        journal_bytes + len(shared),
        expected_snapshots,
        shared_summary.journal_id,
    )


def measure_archive_rebuild(fixture: SharedHistoryFixture) -> dict[str, object]:
    store = fixture.container.collection_uploads._archive_stores.require("primary").store
    assert isinstance(store, UploadedArchiveStore)
    store.read.clear()
    gc.collect()
    tracemalloc.start()
    start = time.perf_counter()
    try:
        generation = fixture.container.provenance.rebuild_index(fixture.collection_id)
        elapsed = time.perf_counter() - start
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert generation == fixture.generation_id
    assert peak <= MAX_REBUILD_PEAK_BYTES and elapsed <= MAX_REBUILD_SECONDS
    reads = Counter(store.read)
    assert reads and max(reads.values()) == 1, "shared archive objects were downloaded repeatedly"
    with session_scope(fixture.container.session_factory) as session:
        state = session.get(State, fixture.collection_id)
        assert state is not None and state.active_build_id is not None
        snapshots = int(
            session.scalar(
                select(func.count())
                .select_from(Snapshot)
                .where(Snapshot.build_id == state.active_build_id)
            )
            or 0
        )
        shared_snapshots = int(
            session.scalar(
                select(func.count())
                .select_from(Snapshot)
                .where(
                    Snapshot.build_id == state.active_build_id,
                    Snapshot.journal_id == fixture.shared_journal_id,
                )
            )
            or 0
        )
        identities = {
            row.object_id: row
            for row in session.scalars(
                select(CollectionArchiveObjectRecord).where(
                    CollectionArchiveObjectRecord.collection_id == fixture.collection_id,
                    CollectionArchiveObjectRecord.store == "primary",
                )
            )
        }
        assert not any(identities[object_id].kind in {"pack", "raw"} for object_id in reads)
        encrypted_bytes = sum(
            identities[object_id].stored_bytes * times for object_id, times in reads.items()
        )
    assert snapshots == fixture.expected_snapshots and shared_snapshots == 1
    found = fixture.api.discover_artifacts(
        {
            "collections": [str(fixture.collection_id)],
            "provenance_all": [
                {
                    "kind": "extension",
                    "scopes": ["member"],
                    "values": [
                        {
                            "pointer": "/value/value",
                            "operator": "contains",
                            "value": "member-token-000000:",
                        }
                    ],
                }
            ],
        }
    )
    assert [row["artifact"]["artifact_id"] for row in found["artifacts"]] == [
        fixture.artifact_ids[0]
    ]
    return {
        "members": len(fixture.artifact_ids),
        "shared_journal_bytes": fixture.shared_bytes,
        "physical_journal_bytes": fixture.journal_bytes,
        "logical_required_history_bytes": fixture.journal_bytes * len(fixture.artifact_ids),
        "elapsed_seconds": round(elapsed, 3),
        "peak_application_bytes": peak,
        "archive_object_reads": sum(reads.values()),
        "encrypted_archive_bytes_read": encrypted_bytes,
        "max_reads_per_object": max(reads.values()),
        "indexed_snapshots": snapshots,
        "index_generation": generation,
        "source_and_upload_scratch_removed": True,
    }


def qualify_shared_history_scaling(database_url: str, directory: Path) -> dict[str, object]:
    """Measure actual encrypted publication and rebuild, separately from plan fixtures."""
    measurements = []
    admin = create_catalog_engine(database_url)
    try:
        for members in (8, 32):
            schema = "riverhog_history_scale_" + uuid4().hex
            with admin.begin() as connection:
                connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            scoped = (
                make_url(database_url)
                .update_query_dict({"options": f"-csearch_path={schema},public"})
                .render_as_string(hide_password=False)
            )
            fixture = None
            try:
                fixture = publish_shared_history(
                    directory / str(members), members=members, database_url=scoped
                )
                first = measure_archive_rebuild(fixture)
                repeated = measure_archive_rebuild(fixture)
                measurements.append(
                    {"members": members, "first_rebuild": first, "repeated_rebuild": repeated}
                )
            finally:
                if fixture is not None:
                    fixture.container.close()
                with admin.begin() as connection:
                    connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    finally:
        admin.dispose()
    low = measurements[0]["first_rebuild"]["peak_application_bytes"]
    high = measurements[1]["first_rebuild"]["peak_application_bytes"]
    assert high <= low * 3 + 8 * 1024 * 1024, "shared-history memory grew with collection size"
    return {
        "max_peak_application_bytes": MAX_REBUILD_PEAK_BYTES,
        "max_rebuild_seconds": MAX_REBUILD_SECONDS,
        "maximum_reads_per_encrypted_object": 1,
        "measurements": measurements,
        "qualification": "passed",
    }

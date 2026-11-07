import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_api import app as api_app
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_rebuild as rebuild
from riverhog_core.canonical_discovery_index import StaleIndexBuild, begin_index_build
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectRecord, CollectionArtifactRecord
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexMemberRecord as Member,
)
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexStateRecord as State,
)
from riverhog_protocol.errors import ServiceUnavailable
from sqlalchemy import delete, func, select

from tests.support.qualification import canonical_history_scale as history_scale
from tests.support.qualification.canonical_history_scale import (
    measure_archive_rebuild,
    publish_shared_history,
)


def test_archive_only_rebuild_retains_shared_roots_and_exact_member_scope(tmp_path: Path) -> None:
    fixture = publish_shared_history(tmp_path, members=2)

    def receipts():
        with session_scope(fixture.container.session_factory) as session:
            return dict(
                session.execute(
                    select(
                        CollectionArtifactRecord.artifact_id,
                        CollectionArtifactRecord.custody_receipt_json,
                    ).where(CollectionArtifactRecord.collection_id == fixture.collection_id)
                ).all()
            )

    try:
        original = receipts()
        assert len(original) == 2 and all(original.values())
        first = measure_archive_rebuild(fixture, cold=True)
        repeated = measure_archive_rebuild(fixture)
        assert first["archive_object_reads"] > 0
        assert repeated["archive_object_reads"] == 0
        assert receipts() == original
    finally:
        fixture.container.close()


def test_archive_rebuild_resumes_after_a_committed_member_and_preserves_the_old_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = publish_shared_history(tmp_path, members=2)
    stage = rebuild.stage_relevant_member
    attempts = 0

    def interrupt(session, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            raise RuntimeError("qualification interruption")
        return stage(session, **kwargs)

    try:
        with session_scope(fixture.container.session_factory) as session:
            active = session.get(State, fixture.collection_id).active_build_id
        with monkeypatch.context() as scoped:
            scoped.setattr(rebuild, "stage_relevant_member", interrupt)
            with pytest.raises(RuntimeError, match="qualification interruption"):
                fixture.container.provenance.rebuild_index(fixture.collection_id)
        with session_scope(fixture.container.session_factory) as session:
            state = session.get(State, fixture.collection_id)
            assert state.active_build_id == active and state.phase == "failed"
            pending = state.pending_build_id
            assert (
                session.scalar(
                    select(func.count()).select_from(Member).where(Member.build_id == pending)
                )
                == 1
            )
        measure_archive_rebuild(fixture)
        with session_scope(fixture.container.session_factory) as session:
            assert session.get(State, fixture.collection_id).active_build_id == pending
    finally:
        fixture.container.close()


def test_superseded_archive_rebuild_cannot_change_the_new_worker_fence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = publish_shared_history(tmp_path, members=1)
    stage = rebuild.stage_relevant_member
    replacement = ""

    def replace_fence(session, **kwargs):
        nonlocal replacement
        with session_scope(fixture.container.session_factory) as newer:
            replacement = begin_index_build(newer, collection_id=fixture.collection_id)
        return stage(session, **kwargs)

    try:
        with monkeypatch.context() as scoped:
            scoped.setattr(rebuild, "stage_relevant_member", replace_fence)
            with pytest.raises(StaleIndexBuild, match="fence"):
                fixture.container.provenance.rebuild_index(fixture.collection_id)
        with session_scope(fixture.container.session_factory) as session:
            state = session.get(State, fixture.collection_id)
            assert state.pending_build_id == replacement and state.phase == "indexing"
        measure_archive_rebuild(fixture)
    finally:
        fixture.container.close()


def test_missing_archive_authority_fails_rebuild_and_retains_previous_publication(
    tmp_path: Path,
) -> None:
    fixture = publish_shared_history(tmp_path, members=1)
    store = fixture.container.collection_uploads._archive_stores.require("primary").store
    try:
        with session_scope(fixture.container.session_factory) as session:
            active = session.get(State, fixture.collection_id).active_build_id
            path = session.scalar(
                select(CollectionArchiveObjectRecord.object_path).where(
                    CollectionArchiveObjectRecord.collection_id == fixture.collection_id,
                    CollectionArchiveObjectRecord.object_id == "provenance-root",
                )
            )
        saved = store.objects.pop(path)
        try:
            with pytest.raises(KeyError):
                fixture.container.provenance.rebuild_index(fixture.collection_id)
            with session_scope(fixture.container.session_factory) as session:
                state = session.get(State, fixture.collection_id)
                assert state.active_build_id == active and state.phase == "failed"
        finally:
            store.objects[path] = saved
        measure_archive_rebuild(fixture)
    finally:
        fixture.container.close()


def test_operator_command_restores_a_lost_index_without_upload_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fixture = publish_shared_history(tmp_path, members=1)
    try:
        with session_scope(fixture.container.session_factory) as session:
            session.execute(delete(State).where(State.collection_id == fixture.collection_id))
            session.execute(
                delete(Generation).where(Generation.collection_id == fixture.collection_id)
            )
        with pytest.raises(ServiceUnavailable):
            fixture.api.discover_artifacts(
                {
                    "collections": [str(fixture.collection_id)],
                    "provenance_all": [
                        {
                            "kind": "extension",
                            "values": [{"operator": "contains", "value": "member-token-000000:"}],
                        }
                    ],
                }
            )
        fixture.api = fixture.api.spawn()
        monkeypatch.setattr(api_app, "default_container", lambda: fixture.container)
        assert (
            api_app.main(["index", "rebuild", "--collection", str(fixture.collection_id), "--json"])
            == 0
        )
        raw = capsys.readouterr().out.strip()
        result = json.loads(raw)
        assert raw.encode() == canonical_json_bytes(result)
        assert result == {
            "collection_id": str(fixture.collection_id),
            "index_generation": fixture.generation_id,
        }
        measure_archive_rebuild(fixture)
    finally:
        fixture.container.close()


@pytest.mark.parametrize("query_seconds", [0.5, 543.0])
def test_native_discovery_target_reports_both_queries_without_failing_valid_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, query_seconds: float
) -> None:
    fixture = publish_shared_history(tmp_path, members=2)
    times = iter((0.0, 1.0, 2.0, 2.0 + query_seconds, 3.0 + query_seconds, 3.0 + 2 * query_seconds))
    monkeypatch.setattr(history_scale, "time", SimpleNamespace(perf_counter=lambda: next(times)))
    try:
        measured = measure_archive_rebuild(fixture, cold=True)
        assert measured["archive_object_reads"] > 0
        assert measured["max_reads_per_object"] == 1
        assert set(measured["native_http"]) == {"first_read", "repeat_read"}
        for query in measured["native_http"].values():
            assert query["elapsed_seconds"] == query_seconds
            assert query["exact_member_matches"] == 1
            assert query["profiling_target"] == {
                "report_only": True,
                "target_seconds": 5,
                "status": "met" if query_seconds <= 5 else "missed",
                "reason": "completed-query",
            }
    finally:
        fixture.container.close()


@pytest.mark.parametrize("failure", ["wrong-result", "query-memory", "archive-reread"])
def test_native_discovery_target_miss_preserves_functional_and_resource_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    fixture = publish_shared_history(tmp_path, members=1)
    times = iter((0.0, 1.0, 2.0, 545.0, 546.0, 1089.0))
    monkeypatch.setattr(history_scale, "time", SimpleNamespace(perf_counter=lambda: next(times)))
    discover = fixture.api.discover_artifacts
    store = fixture.container.collection_uploads._archive_stores.require("primary").store

    def observe(payload):
        found = discover(payload)
        if failure == "wrong-result":
            return {**found, "artifacts": []}
        if failure == "archive-reread":
            with session_scope(fixture.container.session_factory) as session:
                record = session.scalar(
                    select(CollectionArchiveObjectRecord)
                    .where(CollectionArchiveObjectRecord.collection_id == fixture.collection_id)
                    .limit(1)
                )
                assert record is not None
                assert b"".join(
                    store.iter_stored_archive_object(
                        collection_id=fixture.collection_id, object=record
                    )
                )
        return found

    monkeypatch.setattr(fixture.api, "discover_artifacts", observe)
    if failure == "query-memory":
        traced = history_scale.tracemalloc
        samples = 0

        def memory():
            nonlocal samples
            samples += 1
            current, peak = traced.get_traced_memory()
            return current, peak if samples == 1 else history_scale.MAX_REBUILD_PEAK_BYTES + 1

        monkeypatch.setattr(
            history_scale,
            "tracemalloc",
            SimpleNamespace(start=traced.start, stop=traced.stop, get_traced_memory=memory),
        )
    try:
        with pytest.raises(AssertionError):
            measure_archive_rebuild(fixture, cold=True)
    finally:
        fixture.container.close()

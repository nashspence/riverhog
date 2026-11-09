from __future__ import annotations

import json
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord as Terminal
from riverhog_core.catalog_base import Base
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveCopyRecord as Copy
from riverhog_core.catalog_models import CollectionArtifactRecord as Artifact
from riverhog_core.catalog_models import CollectionRecord as Collection
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.incremental_emission import VolumeEmissionCapacity
from riverhog_core.services import archive_upload_projection_cleanup as module
from riverhog_core.services.archive_upload_projection_cleanup import (
    BUILD_TABLES,
    PROJECTION_TABLES,
    ArchiveUploadProjectionCleanup,
)
from riverhog_core.services.archive_upload_terminal_result import ArchiveUploadTerminalResults
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select

from tests.unit.test_archive_upload_cleanup_turn import _bind, _complete
from tests.unit.test_archive_write_admission import _case
from tests.unit.test_collection_registration_cleanup import CLEANUP
from tests.unit.test_upload_discard_authority import _operator, _select

LIMITS = replace(CLEANUP, input_transitions=2)


@contextmanager
def _projection(
    tmp_path,
    *,
    database_url=None,
    count=17,
    width=0,
    provider_ready=True,
    root=True,
    indexed=False,
    linked=False,
):
    with _case(tmp_path, database_url=database_url) as case, ExitStack() as resources:
        build_id = None
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            upload.state = "orphaned"
            if root:
                upload.catalog_artifact_set_identity = "2" * 64
                upload.catalog_inventory_identity = "4" * 64
                upload.provenance_identity = "3" * 64
                upload.final_authority_json = json.dumps(
                    {"root": {"plaintext_sha256": "5" * 64}, "recovery": {}}
                )
                session.add(
                    Collection(
                        id=upload.collection_id,
                        creation_idempotency_key=upload.idempotency_key,
                        creation_identity_sha256=upload.creation_identity_sha256,
                        creation_custody_mode=upload.custody_mode,
                        creation_archive_store=upload.archive_store,
                        creation_use_cache=upload.use_cache,
                        creation_copy_to_json=upload.copy_to_json,
                        archive_generation=upload.archive_generation,
                        delivery_context_id=upload.delivery_context_id,
                        encryption_format=upload.encryption_format,
                        passphrase_id=upload.passphrase_id,
                        created_by_principal_id=upload.initiated_by_principal_id,
                        created_by_key_id=upload.initiated_by_key_id,
                        artifact_set_identity=upload.catalog_artifact_set_identity,
                        inventory_identity=upload.catalog_inventory_identity,
                        provenance_identity=upload.provenance_identity,
                        archive_root_sha256="5" * 64,
                        created_at=upload.opened_at,
                        is_published=False,
                        artifact_count=upload.artifact_count,
                        artifact_bytes=upload.artifact_bytes,
                    )
                )
                session.flush()
                session.add(
                    Copy(
                        collection_id=upload.collection_id,
                        store=upload.archive_store,
                        incarnation_id=upload.archive_incarnation_id,
                        archive_storage_prefix=upload.archive_storage_prefix,
                        state="uploaded",
                    )
                )
                session.add_all(
                    Artifact(
                        collection_id=upload.collection_id,
                        artifact_id=f"{index:064x}",
                        bytes=1,
                        sha256="a" * 64,
                        custody_receipt_json="x" * width if width else None,
                    )
                    for index in range(count)
                )
                session.flush()
                if indexed:
                    build_id = _seed_index(session, upload, count)
        discovery = (
            _seed_linked_discovery(case, build_id, tmp_path / "independent-discovery", resources)
            if linked
            else None
        )
        case.keys, case.principal = _operator(case)
        case.operation = _select(
            case,
            case.principal,
            case.service._discard_write_guard(case.collection_id, case.principal),
        )
        guard = case.service._terminal_write_guard(case.collection_id, case.operation)
        original = ArchiveUploadTerminalResults(case.factory).prepare(
            case.collection_id,
            case.operation,
            {"status": "discarded", "collection_id": str(case.collection_id), "files": count},
            guard=guard,
        )
        progress = case.service._terminal_turn(case.collection_id, case.operation).advance(limit=1)
        assert progress.originals_retired
        ready = _bind(case)
        ready.target.items = ()
        if provider_ready:
            _complete(ready)
        yield SimpleNamespace(
            case=case,
            guard=guard,
            ready=ready,
            original=original,
            build_id=build_id,
            discovery=discovery,
        )


def _advance(ready, *, limits=LIMITS):
    return ArchiveUploadProjectionCleanup(ready.case.factory, ready.ready.owner).advance(
        ready.case.collection_id, ready.case.operation, guard=ready.guard, limits=limits
    )


def _count(ready):
    with session_scope(ready.case.factory) as session:
        total = session.scalar(
            select(func.count())
            .select_from(Collection)
            .where(Collection.id == ready.case.collection_id)
        )
        for name in PROJECTION_TABLES:
            table = Base.metadata.tables[name]
            total += session.scalar(
                select(func.count())
                .select_from(table)
                .where(table.c.collection_id == ready.case.collection_id)
            )
        generations = Base.metadata.tables["collection_provenance_index_generations"]
        scope = select(generations.c.build_id).where(
            generations.c.collection_id == ready.case.collection_id
        )
        for name in (*BUILD_TABLES, "collection_provenance_index_generations"):
            table = Base.metadata.tables[name]
            total += session.scalar(
                select(func.count()).select_from(table).where(table.c.build_id.in_(scope))
            )
        return total


def _finish(ready):
    before = _count(ready)
    for _ in range(before + 3):
        progress = _advance(ready)
        current = _count(ready)
        assert before - current == progress.removed <= LIMITS.input_transitions
        assert progress.metadata_bytes <= LIMITS.metadata_bytes
        assert progress.pending_reason is None
        if progress.exhausted:
            break
        before = current
    else:
        pytest.fail("original unpublished projection did not retire")
    assert current == 0
    with session_scope(ready.case.factory) as session:
        terminal = session.get(Terminal, ready.case.operation)
        assert terminal.original_result_json == ready.original and terminal.result_json is None
        assert terminal.phase == "cleaning"
        assert session.get(Upload, ready.case.collection_id) is not None


@pytest.mark.parametrize("count", (0, 1, 17, 65))
def test_exact_unpublished_projection_cold_resumes_bounded_rows_without_confirming_upload(
    tmp_path, count
):
    with _projection(tmp_path, count=count) as ready:
        _finish(ready)


def test_absent_projection_is_only_local_exhaustion_not_an_absent_upload_result(tmp_path):
    with _projection(tmp_path, root=False) as ready:
        assert _advance(ready).exhausted
        _finish(ready)


def test_projection_cannot_retire_before_positive_original_archive_inventory(tmp_path):
    with _projection(tmp_path, provider_ready=False) as ready:
        before = _count(ready)
        with pytest.raises(Conflict, match="inventory proof"):
            _advance(ready)
        assert _count(ready) == before


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("is_published", True),
        ("creation_identity_sha256", "e" * 64),
        ("creation_idempotency_key", "another-construction"),
        ("archive_generation", "e" * 64),
        ("delivery_context_id", "another-delivery"),
        ("passphrase_id", "another-key"),
        ("archive_root_sha256", "e" * 64),
    ),
)
def test_published_or_different_original_root_is_preserved_before_any_projection_window(
    tmp_path, field, value
):
    with _projection(tmp_path) as ready:
        with session_scope(ready.case.factory) as session:
            setattr(session.get(Collection, ready.case.collection_id), field, value)
        before = _count(ready)
        with pytest.raises(Conflict, match="original|unpublished"):
            _advance(ready)
        assert _count(ready) == before


def test_complete_opaque_custody_metadata_refuses_an_insufficient_projection_grant(tmp_path):
    with _projection(tmp_path, count=1, width=20001) as ready:
        before = _count(ready)
        with pytest.raises(VolumeEmissionCapacity) as refusal:
            _advance(ready)
        assert refusal.value.required > 20001 and _count(ready) == before
        assert _advance(ready, limits=replace(LIMITS, metadata_bytes=40000)).removed == 1
        _finish(ready)


def _lost(ready, monkeypatch, committed):
    actual = module.session_scope

    @contextmanager
    def lose(factory):
        with actual(factory) as session:
            yield session
            if not committed:
                raise OSError("projection rollback")
        raise OSError("projection commit reply lost")

    before = _count(ready)
    with monkeypatch.context() as patch:
        patch.setattr(module, "session_scope", lose)
        with pytest.raises(OSError, match="projection"):
            _advance(ready)
    assert (_count(ready) == before) is not committed
    _finish(ready)


@pytest.mark.parametrize("committed", (False, True))
def test_projection_windows_retry_original_sql_rollback_or_lost_commit_reply(
    tmp_path, monkeypatch, committed
):
    with _projection(tmp_path) as ready:
        _lost(ready, monkeypatch, committed)


def test_projection_routes_cover_the_current_schema_and_reject_an_unreviewed_child():
    from sqlalchemy import Column, ForeignKeyConstraint, Integer, Table

    module.validate_projection_cleanup_routes()
    future = Table(
        "future_unpublished_projection_child",
        Base.metadata,
        Column("collection_id", Integer, primary_key=True),
        ForeignKeyConstraint(["collection_id"], ["collections.id"], ondelete="CASCADE"),
    )
    try:
        with pytest.raises(RuntimeError, match="routes differ"):
            module.validate_projection_cleanup_routes()
    finally:
        Base.metadata.remove(future)
    module.validate_projection_cleanup_routes()


def _seed_index(session, upload, count):
    from riverhog_core.canonical_discovery_index import (
        begin_index_build,
        stage_assertion_page,
        stage_entry_page,
        stage_member,
        stage_membership_page,
        stage_snapshot_header,
    )
    from riverhog_core.canonical_discovery_rows import iter_index_assertions
    from riverhog_provenance import (
        BoundedSourceObserver,
        BytesSource,
        create_journal,
        validate_journal,
    )

    observation = BoundedSourceObserver().observe(BytesSource(b"opaque indexed member"))
    summary = validate_journal(
        create_journal(
            observation.graph_fragment(), recorded_by_agent_id=observation.observer_agent_id
        )
    )
    rows = tuple(iter_index_assertions(summary))
    build_id = begin_index_build(session, collection_id=upload.collection_id)
    session.flush()
    stage_snapshot_header(session, build_id=build_id, summary=summary)
    session.flush()
    assert stage_entry_page(session, build_id=build_id, summary=summary, start=0) > 0
    session.flush()
    stage_assertion_page(session, build_id=build_id, rows=rows)
    for index in range(count):
        artifact_id = f"{index:064x}"
        stage_member(
            session,
            build_id=build_id,
            artifact_id=artifact_id,
            bytes=1,
            sha256="a" * 64,
            journal_id=summary.journal_id,
            prefix_sha256=summary.journal_sha256,
            delivery_association_id="urn:uuid:cabfc827-91d5-4cde-8f0f-81255b75b1c4",
        )
        session.flush()
        stage_membership_page(
            session,
            build_id=build_id,
            rows=[(artifact_id, row.row_key, "member") for row in rows],
        )
    return build_id


def _revoked(ready):
    from riverhog_core.app_permissions import (
        COLLECTIONS_CREATE,
        COLLECTIONS_DELETE,
        ApplicationAccess,
    )
    from riverhog_protocol.errors import NotFound

    from tests.unit.test_app_keys import BOOTSTRAP

    assert _advance(ready).removed == 2
    before = _count(ready)
    case = ready.case
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_CREATE),),
        grantor=BOOTSTRAP,
    )
    with pytest.raises(NotFound):
        _advance(ready)
    assert _count(ready) == before
    case.keys.replace_access(
        app=case.principal.id,
        key_id=case.principal.key_id,
        access=(ApplicationAccess(COLLECTIONS_DELETE),),
        grantor=BOOTSTRAP,
    )
    _finish(ready)


def test_projection_windows_require_current_terminal_permissions_after_resumption(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        _revoked(ready)


def test_actual_staged_discovery_children_retire_before_their_original_generation(tmp_path):
    with _projection(tmp_path, indexed=True) as ready:
        assert ready.build_id is not None
        _finish(ready)


def test_another_collection_and_its_materialized_children_remain_unchanged(tmp_path):
    with _projection(tmp_path) as ready:
        with session_scope(ready.case.factory) as session:
            original = dict(session.execute(select(Collection.__table__)).mappings().one())
            other = {
                **original,
                "id": ready.case.collection_id + 1000000,
                "creation_idempotency_key": "independent-construction",
            }
            session.execute(
                Collection.__table__.insert(),
                {
                    name: value
                    for name, value in other.items()
                    if Collection.__table__.c[name].computed is None
                },
            )
            session.add(
                Artifact(
                    collection_id=other["id"],
                    artifact_id="e" * 64,
                    bytes=99,
                    sha256="f" * 64,
                    custody_receipt_json="original-opaque-receipt",
                )
            )
        with session_scope(ready.case.factory) as session:
            retained = dict(
                session.execute(select(Collection.__table__).where(Collection.id == other["id"]))
                .mappings()
                .one()
            )
        _finish(ready)
        with session_scope(ready.case.factory) as session:
            assert dict(session.execute(select(Collection.__table__)).mappings().one()) == retained
            artifact = session.get(Artifact, (other["id"], "e" * 64))
            assert (
                artifact.bytes == 99 and artifact.custody_receipt_json == "original-opaque-receipt"
            )


def test_changed_source_copy_prefix_prevents_any_unpublished_projection_mutation(tmp_path):
    with _projection(tmp_path) as ready:
        with session_scope(ready.case.factory) as session:
            upload = session.get(Upload, ready.case.collection_id)
            session.get(
                Copy, (upload.collection_id, upload.archive_store)
            ).archive_storage_prefix = "another-original-prefix"
        before = _count(ready)
        with pytest.raises(Conflict, match="original.*source"):
            _advance(ready)
        assert _count(ready) == before


def _independent(ready, kind):
    from riverhog_core.catalog_models import (
        CollectionArchiveObjectRecord,
        RetrievalCacheObjectRecord,
    )
    from riverhog_core.catalog_workflow_models import CollectionProcessingClaimRecord

    with session_scope(ready.case.factory) as session:
        upload = session.get(Upload, ready.case.collection_id)
        if kind == "cache":
            session.add(
                CollectionArchiveObjectRecord(
                    collection_id=upload.collection_id,
                    store=upload.archive_store,
                    object_id="independently-received",
                    object_order=0,
                    kind="raw-volume",
                    object_path="original-archive-object",
                    plaintext_bytes=1,
                    stored_bytes=1,
                    uploaded_at=upload.opened_at,
                )
            )
            session.flush()
            session.add(
                RetrievalCacheObjectRecord(
                    collection_id=upload.collection_id,
                    source_store=upload.archive_store,
                    source_incarnation_id=upload.archive_incarnation_id,
                    object_id="independently-received",
                    cache_store=upload.archive_store,
                    cache_incarnation_id=upload.archive_incarnation_id,
                    object_path="original-encrypted-cache-object",
                    stored_bytes=1,
                    cached_at=upload.opened_at,
                    verified_at=upload.opened_at,
                    state="ready",
                )
            )
            table = Base.metadata.tables["retrieval_cache_objects"]
        else:
            session.add(
                CollectionProcessingClaimRecord(
                    id="c" * 64,
                    work_id="d" * 64,
                    consumer_app=upload.initiated_by_principal_id,
                    purpose="independent-work",
                    work_document_json="{}",
                    work_document_sha256="e" * 64,
                    state="settled",
                    fence=1,
                    expires_at=upload.opened_at,
                    output_collection_id=upload.collection_id,
                    created_at=upload.opened_at,
                    updated_at=upload.opened_at,
                )
            )
            table = Base.metadata.tables["collection_processing_claims"]
    with session_scope(ready.case.factory) as session:
        before = [dict(row) for row in session.execute(select(table)).mappings()]
    total = _count(ready)
    with pytest.raises(Conflict, match="independent work"):
        _advance(ready)
    assert _count(ready) == total
    with session_scope(ready.case.factory) as session:
        assert [dict(row) for row in session.execute(select(table)).mappings()] == before


@pytest.mark.parametrize("kind", ("cache", "processing"))
def test_unpublished_retirement_cannot_consume_independent_cache_or_workflow_receiving(
    tmp_path, kind
):
    with _projection(tmp_path, indexed=True) as ready:
        _independent(ready, kind)


def _seed_linked_discovery(case, build_id, root, resources):
    from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob, discovery_worker_current
    from riverhog_core.catalog_discovery_validation_models import (
        DiscoveryValidationJobRecord as Job,
    )
    from riverhog_core.catalog_provenance_index_models import (
        CollectionProvenanceIndexGenerationRecord as Generation,
    )
    from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace

    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        generation = session.get(Generation, build_id)
        record = Job(
            workspace_id="a" * 32,
            build_id=build_id,
            attempt_id="b" * 32,
            admission_sha256="c" * 64,
            archive_generation=upload.archive_generation,
            archive_root_sha256=generation.archive_root_sha256,
            provenance_identity=upload.provenance_identity,
            artifact_set_identity=upload.catalog_artifact_set_identity,
            journal_count="1",
            build_epoch=generation.expected_epoch,
            policy_sha256=generation.extraction_contract_sha256,
            catalog_sha256="e" * 64,
            worker_epoch=1,
            version="f" * 64,
            state="seeding",
        )
        session.add(record)
        session.flush()
        original = DiscoveryJob.read(record)
    assert discovery_worker_current(case.factory, original)
    custody = resources.enter_context(ExitStack())
    path = custody.enter_context(
        lease_journal_validation_workspace(
            root,
            original.workspace_id,
            create=True,
            creation_current=lambda: discovery_worker_current(case.factory, original),
        )
    )
    assert path is not None
    marker = path / "retained-native-owner"
    marker.write_bytes(b"original independently leased bytes")
    return SimpleNamespace(root=root, marker=marker, original=original, custody=custody)


def _linked_discovery(ready, tmp_path):
    from riverhog_core.canonical_discovery_corpus_jobs import DiscoveryJob, discovery_worker_current
    from riverhog_core.catalog_discovery_validation_models import (
        DiscoveryValidationJobRecord as Job,
    )
    from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace

    held = ready.discovery
    assert held is not None and held.root == tmp_path / "independent-discovery"
    assert not discovery_worker_current(ready.case.factory, held.original)
    total = _count(ready)
    for _ in range(2):
        progress = _advance(ready)
        assert progress.pending_reason == "discovery-custody" and progress.removed == 0
        assert not progress.exhausted and _count(ready) == total
        assert held.marker.read_bytes() == b"original independently leased bytes"
    with lease_journal_validation_workspace(held.root, held.original.workspace_id) as busy:
        assert busy is None
    with session_scope(ready.case.factory) as session:
        assert DiscoveryJob.read(session.get(Job, held.original.workspace_id)) == held.original
    assert held.marker.read_bytes() == b"original independently leased bytes"


def test_linked_discovery_retains_exact_job_files_and_exclusive_kernel_lease(tmp_path):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        _linked_discovery(ready, tmp_path)


@pytest.mark.parametrize("count", (17, 65, 129))
def test_actual_projection_retirement_has_linear_sql_work_without_whole_resource_rescans(
    tmp_path, count, record_property
):
    from sqlalchemy import event

    with _projection(tmp_path, count=count) as ready:
        statements = []
        engine = ready.case.factory.kw["bind"]

        def observed(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", observed)
        try:
            removed = turns = 0
            for _ in range(count + 4):
                progress = _advance(ready)
                turns += 1
                removed += progress.removed
                assert progress.removed <= LIMITS.input_transitions
                assert progress.metadata_bytes <= LIMITS.metadata_bytes
                if progress.exhausted:
                    break
            else:
                pytest.fail("projection retirement did not exhaust bounded first-live windows")
        finally:
            event.remove(engine, "before_cursor_execute", observed)
        assert removed == count + 2 and turns == (count + 1) // 2 + 3
        assert len(statements) <= 90 * turns
        record_property("projection_rows_retired", removed)
        record_property("projection_turns", turns)
        record_property("projection_sql_statements", len(statements))


def test_complete_binary_index_value_is_charged_before_its_retirement_window(tmp_path):
    from riverhog_core.catalog_provenance_index_models import (
        CollectionProvenanceIndexValueRecord as Value,
    )
    from sqlalchemy import delete

    with _projection(tmp_path, indexed=True) as ready:
        with session_scope(ready.case.factory) as session:
            chunks = Base.metadata.tables["collection_provenance_index_text_chunks"]
            session.execute(delete(chunks).where(chunks.c.build_id == ready.build_id))
            value = session.scalars(
                select(Value)
                .where(Value.build_id == ready.build_id)
                .order_by(Value.row_key, Value.ordinal)
                .limit(1)
            ).one()
            value.exact_json = json.dumps("x" * 20001).encode()
        before = _count(ready)
        with pytest.raises(VolumeEmissionCapacity) as refusal:
            _advance(ready)
        assert refusal.value.required > 20001 and _count(ready) == before
        progress = _advance(ready, limits=replace(LIMITS, metadata_bytes=40000))
        assert progress.table == "collection_provenance_index_values"
        assert 1 <= progress.removed <= LIMITS.input_transitions
        _finish(ready)


def _composed_projection(ready, monkeypatch, *, revoked=False, linked=False):
    import threading
    from datetime import timedelta

    from riverhog_core.app_permissions import (
        COLLECTIONS_CREATE,
        COLLECTIONS_DELETE,
        ApplicationAccess,
    )
    from riverhog_core.services import archive_upload_terminal_registry as registry_module
    from riverhog_core.services.archive_upload_catalog_cleanup import ArchiveUploadCatalogCleanup
    from riverhog_core.services.archive_upload_checkpoint_cleanup import (
        ArchiveUploadCheckpointCleanup,
    )
    from riverhog_core.services.archive_upload_copy_intent_cleanup import (
        ArchiveUploadCopyIntentCleanup,
    )
    from riverhog_core.services.archive_upload_registration_cleanup import (
        ArchiveUploadRegistrationCleanup,
    )
    from riverhog_core.services.archive_upload_terminal_turn import ArchiveUploadTerminalTurn
    from riverhog_core.services.physical_execution import PhysicalExecutionKey
    from riverhog_protocol.errors import NotFound
    from sqlalchemy import event

    from tests.unit.test_app_keys import BOOTSTRAP
    from tests.unit.test_archive_upload_maintenance import _join

    case = ready.case
    registry = case.service._archive_terminals
    owner = ArchiveUploadProjectionCleanup(case.factory, ready.ready.owner)
    original = owner.advance
    workers, created, windows = [], [], []

    def advance(*args, **kwargs):
        assert kwargs["guard"] is ready.guard and kwargs["limits"] is LIMITS
        workers.append(threading.current_thread().name)
        result = original(*args, **kwargs)
        windows.append(result)
        return result

    monkeypatch.setattr(owner, "advance", advance)

    def create(collection_id, operation_id):
        created.append(True)
        return ArchiveUploadTerminalTurn(
            case.factory,
            collection_id,
            operation_id,
            guard=ready.guard,
            adjudication=case.service._archive_terminal_adjudication,
            retirement=case.service._archive_terminal_retirement,
            cleanup_create=lambda: ready.ready.turn,
            registration_cleanup=ArchiveUploadRegistrationCleanup(case.factory, ready.ready.owner),
            registration_limits=LIMITS,
            catalog_cleanup=ArchiveUploadCatalogCleanup(case.factory, ready.ready.owner),
            checkpoint_cleanup=ArchiveUploadCheckpointCleanup(case.factory, ready.ready.owner),
            copy_intent_cleanup=ArchiveUploadCopyIntentCleanup(case.factory, ready.ready.owner),
            projection_cleanup=owner,
        )

    monkeypatch.setattr(registry, "_create", create)
    clock = [100.0]
    monkeypatch.setattr(registry_module, "monotonic", lambda: clock[0])
    key = PhysicalExecutionKey(case.collection_id, case.operation)
    calls = len(ready.ready.target.calls)

    def quantum(*, expect_revoked=False):
        for _ in range(16):
            mutations = []

            def observed(
                connection,
                cursor,
                statement,
                parameters,
                context,
                executemany,
                *,
                mutations=mutations,
            ):
                if context.isinsert or context.isupdate or context.isdelete:
                    mutations.append(statement)

            engine = case.factory.kw["bind"]
            event.listen(engine, "before_cursor_execute", observed)
            try:
                assert (
                    registry.submit(
                        key,
                        target_incarnation=case.incarnation,
                        work_limit=1,
                        retry_after=timedelta(seconds=1),
                    )
                    == 1
                )
                slot = case.service._archive_execution._slots[key]
                _join(case.service)
                error = slot.future.exception()
                assert registry.collect(limit=1) == 1
            finally:
                event.remove(engine, "before_cursor_execute", observed)
            clock[0] += 2
            progress = registry._contexts[key].progress
            if error is not None:
                if expect_revoked and isinstance(error, NotFound):
                    assert mutations == []
                    return progress
                raise error
            assert progress is slot.future.result() and not expect_revoked
            if progress.pending_reason not in {"local-execution", "catalog-busy"}:
                return progress
            assert mutations == [] and progress.retired_originals == 0
            assert len(ready.ready.target.calls) == calls
            assert case.service._archive_retention._counts == {False: 0, True: 1}
        pytest.fail("original terminal stayed deferred after bounded joined turns")

    previous = _count(ready)
    restored = False
    for _ in range(previous + 100):
        observed = len(windows)
        progress = quantum()
        current = _count(ready)
        assert 0 <= previous - current <= LIMITS.input_transitions
        if len(windows) > observed:
            assert len(windows) == observed + 1
            assert progress.archive_inventory_exhausted and progress.registration_disposed
            assert progress.upload_children_exhausted and progress.checkpoints_exhausted
            assert progress.copy_intents_exhausted
            assert previous - current == windows[-1].removed
            assert windows[-1].metadata_bytes <= LIMITS.metadata_bytes
            if revoked and not restored:
                before = _count(ready)
                count = len(windows)
                case.keys.replace_access(
                    app=case.principal.id,
                    key_id=case.principal.key_id,
                    access=(ApplicationAccess(COLLECTIONS_CREATE),),
                    grantor=BOOTSTRAP,
                )
                for _ in range(2):
                    quantum(expect_revoked=True)
                assert _count(ready) == before and len(windows) == count
                case.keys.replace_access(
                    app=case.principal.id,
                    key_id=case.principal.key_id,
                    access=(ApplicationAccess(COLLECTIONS_DELETE),),
                    grantor=BOOTSTRAP,
                )
                restored = True
        if linked and progress.pending_reason == "discovery-custody":
            assert not progress.projection_exhausted
            assert (
                current == previous
                and ready.discovery.marker.read_bytes() == b"original independently leased bytes"
            )
            break
        if progress.projection_exhausted:
            assert not linked and current == 0 and progress.pending_reason is None
            break
        previous = current
    else:
        pytest.fail("same original terminal did not compose bounded projection retirement")
    assert created == [True] and set(workers) == {"physical-execution"}
    assert not progress.execution_stopped and progress.phase == "cleaning"
    assert restored is revoked
    assert len(ready.ready.target.calls) == calls
    assert case.service._archive_retention._counts == {False: 0, True: 1}
    with session_scope(case.factory) as session:
        terminal = session.get(Terminal, case.operation)
        assert terminal.original_result_json == ready.original and terminal.result_json is None
        assert session.get(Upload, case.collection_id) is not None


@pytest.mark.parametrize("revoked", (False, True))
def test_same_original_terminal_worker_composes_projection_after_all_current_cleanup_proofs(
    tmp_path, monkeypatch, revoked
):
    with _projection(tmp_path, indexed=True) as ready:
        _composed_projection(ready, monkeypatch, revoked=revoked)


def test_same_original_terminal_keeps_linked_discovery_custody_pending_without_confirmation(
    tmp_path, monkeypatch
):
    with _projection(tmp_path, indexed=True, linked=True) as ready:
        _composed_projection(ready, monkeypatch, linked=True)


def test_projection_composition_requires_the_original_copy_intent_owner(tmp_path):
    from riverhog_core.services.archive_upload_terminal_turn import ArchiveUploadTerminalTurn

    with _projection(tmp_path) as ready:
        case = ready.case
        with pytest.raises(ValueError, match="projection.*copy-intent"):
            ArchiveUploadTerminalTurn(
                case.factory,
                case.collection_id,
                case.operation,
                guard=ready.guard,
                adjudication=case.service._archive_terminal_adjudication,
                retirement=case.service._archive_terminal_retirement,
                projection_cleanup=ArchiveUploadProjectionCleanup(case.factory, ready.ready.owner),
            )

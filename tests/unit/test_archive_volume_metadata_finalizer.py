from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import update_archive_sequence_commitment
from riverhog_core import archive_root
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.archive_volume_metadata_finalizer import ArchiveVolumeMetadataFinalizer
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation
from riverhog_protocol.errors import Conflict, NotFound, ServiceUnavailable
from sqlalchemy import event, select

from tests.unit.finalization_claim_fixtures import _different_claim, _replace_claim
from tests.unit.test_archive_volume_metadata_checkpoint import _snapshot
from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source
from tests.unit.test_finalization_claim_readiness import _current_claim, _fresh_coordinator


def _owner(fixture, *, audit=True, forward_guard=None):
    case = fixture.case
    calls = {"observe": [], "execute": []}

    def committed():
        if not audit:
            return
        with session_scope(case.factory) as session:
            if session.get_bind().dialect.name == "sqlite":
                session.connection(execution_options={"sqlite_immediate_writer": True})
            upload = session.scalar(
                select(Upload)
                .where(Upload.collection_id == case.collection_id)
                .with_for_update(nowait=True)
            )
            effect = session.scalar(
                select(ArchiveWriteEffectRecord).where(
                    ArchiveWriteEffectRecord.collection_id == case.collection_id
                )
            )
            assert upload.state == upload.archive_phase == "finalizing"
            assert upload.lease_expires_at is None and effect.unresolved and effect.state == "armed"

    class Ports:
        def observe_immutable_object(self, request):
            committed()
            calls["observe"].append(request)
            return fixture.physical.observe_immutable_object(request)

        def execute_write(self, invocation, *, content):
            committed()
            calls["execute"].append((invocation, content))
            return fixture.physical.execute_write(invocation, content=content)

    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        name = upload.archive_store
    ports = Ports()
    binding = replace(
        case.service._archive_stores.require(name),
        immutable_objects=fixture.physical,
        immutable_writes=ports,
    )
    registry = ArchiveStoreRegistry({name: binding})
    config = replace(case.service._config, archive_write=fixture.grants)
    owner = ArchiveVolumeMetadataFinalizer(
        case.factory,
        collection_id=case.collection_id,
        config=config,
        archive_stores=registry,
        forward_guard=forward_guard,
    )
    return SimpleNamespace(
        case=case,
        calls=calls,
        ports=ports,
        binding=binding,
        name=name,
        registry=registry,
        config=config,
        owner=owner,
    )


def _fresh(original):
    return ArchiveVolumeMetadataFinalizer(
        original.case.factory,
        collection_id=original.case.collection_id,
        config=original.config,
        archive_stores=original.registry,
    )


@pytest.mark.parametrize("kind", ["pack", "segment"])
@pytest.mark.parametrize("mode", ["direct", "http"])
def test_finalizer_uses_explicit_original_ports_through_actual_requeued_steps(
    tmp_path, monkeypatch, kind, mode
):
    with _metadata_source(tmp_path, monkeypatch, kind, mode) as fixture:
        original = _owner(fixture)
        expected = hashlib.sha256()
        for sequence in range(3):
            with session_scope(original.case.factory) as session:
                source = ArchiveVolumeMetadataSource.load(
                    session, original.case.collection_id, sequence
                )
            update_archive_sequence_commitment(expected, source.document)
            assert original.owner.advance()
            assert not original.owner.has_pending and _count(original.case) == 0
            coordinator = _fresh_coordinator(original.case)
            coordinator._requeue_finalization_step(_current_claim(original.case))
            assert (
                coordinator._claim_due_finalization().collection_id == original.case.collection_id
            )
        assert not original.owner.advance()
        assert _snapshot(original.case)[3] == expected.hexdigest()
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == 3


@pytest.mark.parametrize("phase", ["report", "retirement"])
@pytest.mark.parametrize("committed", [False, True])
def test_finalizer_retains_same_actor_after_reply_loss_and_advanced_source(
    tmp_path, monkeypatch, phase, committed
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        claim = _current_claim(fixture.case)
        original = _owner(fixture, forward_guard=claim.guard)
        transaction = admission_module.session_scope
        position = 3 if phase == "report" else 4
        calls = 0
        encrypted = []
        encrypt = archive_root.encrypt_age_scrypt

        def encryption(*args, **kwargs):
            result = encrypt(*args, **kwargs)
            encrypted.append(result)
            return result

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            selected = calls == position
            with transaction(factory) as session:
                yield session
                if selected and not committed:
                    raise OSError("injected finalizer rollback")
            if selected and committed:
                raise OSError("injected finalizer reply loss")

        monkeypatch.setattr(archive_root, "encrypt_age_scrypt", encryption)
        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="injected finalizer"):
                original.owner.advance()
        _replace_claim(fixture.case, _different_claim(claim))
        original.owner._config = None
        original.owner._archive_stores = None
        actor = original.owner._actor
        assert original.owner.has_pending
        assert _snapshot(original.case)[0] == (int(committed) if phase == "report" else 1)
        assert original.owner.advance()
        assert actor is not None and not actor.has_pending
        assert not original.owner.has_pending and original.owner._passphrase is None
        assert (
            len(encrypted) == len(original.calls["execute"]) == len(original.calls["observe"]) == 1
        )
        assert original.calls["execute"][0][1] == encrypted[0]
        assert _snapshot(original.case)[0] == 1 and _count(original.case) == 0


def test_fresh_finalizer_drains_closed_data_without_rebuilding_live_actor(tmp_path, monkeypatch):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        report = admission_module.ImmutableArchiveAdmission.report

        def lost(self, *args, **kwargs):
            report(self, *args, **kwargs)
            raise OSError("injected report reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module.ImmutableArchiveAdmission, "report", lost)
            with pytest.raises(OSError):
                original.owner.advance()
        cold = _fresh(original)
        assert _snapshot(original.case)[0] == 1 and _count(original.case) == 1
        assert cold.advance() and not cold.has_pending
        assert _count(original.case) == 0 and len(original.calls["execute"]) == 1
        assert original.owner.advance() and not original.owner.has_pending
        assert len(original.calls["execute"]) == 1
        assert cold.advance() and len(original.calls["execute"]) == 2


@pytest.mark.parametrize("cold", [False, True])
def test_unknown_positive_source_progress_cannot_admit_another_metadata_writer(
    tmp_path, monkeypatch, cold
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        execute = original.ports.execute_write

        def unknown(invocation, *, content):
            return execute(invocation, content=content).model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(original.ports, "execute_write", unknown)
        with pytest.raises(Conflict, match="execution remains unknown"):
            original.owner.advance()
        assert _snapshot(original.case)[0] == 1
        owner = _fresh(original) if cold else original.owner
        for _ in range(2):
            with pytest.raises(
                Conflict,
                match="unresolved provider writes" if cold else "execution remains unknown",
            ):
                owner.advance()
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == 1
        assert _count(original.case) == 1 and original.owner.has_pending


def test_missing_explicit_port_is_a_refusal_even_if_legacy_object_has_methods(
    tmp_path, monkeypatch
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        original.registry.admit(
            original.name, replace(original.binding, immutable_writes=None), probe=lambda: None
        )
        with pytest.raises(ServiceUnavailable, match="no immutable execution/observation port"):
            original.owner.advance()
        assert original.calls == {"observe": [], "execute": []} and _count(original.case) == 0
        assert not original.owner.has_pending
        original.registry.admit(original.name, original.binding, probe=lambda: None)
        assert original.owner.advance()


def test_original_receiving_does_not_require_current_provider_binding_or_key_lookup(
    tmp_path, monkeypatch
):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        report = admission_module.ImmutableArchiveAdmission.report

        def lost(self, *args, **kwargs):
            report(self, *args, **kwargs)
            raise OSError("injected report reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module.ImmutableArchiveAdmission, "report", lost)
            with pytest.raises(OSError):
                original.owner.advance()

        def unavailable(*args, **kwargs):
            raise AssertionError("original receiving requested fresh provider/key selection")

        monkeypatch.setattr(original.registry, "require_incarnation", unavailable)
        monkeypatch.setattr(RuntimeConfig, "archive_passphrase_for", unavailable)
        assert original.owner.advance()
        assert not original.owner.has_pending and _count(original.case) == 0
        assert len(original.calls["execute"]) == 1


def test_new_finalizer_rejects_changed_target_before_any_provider_intent(tmp_path, monkeypatch):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        original.registry.admit(
            original.name, replace(original.binding, incarnation_id="changed"), probe=lambda: None
        )
        with pytest.raises(ServiceUnavailable, match="incarnation changed"):
            original.owner.advance()
        assert not original.owner.has_pending and _count(original.case) == 0
        assert original.calls == {"observe": [], "execute": []}


def test_missing_parent_cannot_retire_retained_finalizer_actor_or_result(tmp_path, monkeypatch):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        report = admission_module.ImmutableArchiveAdmission.report

        def lost(self, *args, **kwargs):
            report(self, *args, **kwargs)
            raise OSError("injected report reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module.ImmutableArchiveAdmission, "report", lost)
            with pytest.raises(OSError):
                original.owner.advance()
        actor = original.owner._actor
        response = actor._response_json
        with session_scope(original.case.factory) as session:
            session.delete(session.get(Upload, original.case.collection_id))
        with pytest.raises(NotFound):
            original.owner.advance()
        assert original.owner._actor is actor and actor._response_json == response
        assert original.owner.has_pending and _count(original.case) == 1


def test_current_canonical_planner_must_agree_with_its_mirrored_source_count(tmp_path, monkeypatch):
    with _metadata_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        with session_scope(original.case.factory) as session:
            session.get(Upload, original.case.collection_id).planner_next_sequence += 1
        with pytest.raises(Conflict, match="source differs from its construction"):
            original.owner.advance()
        assert original.calls == {"observe": [], "execute": []} and _count(original.case) == 0


@pytest.mark.parametrize("count", [1, 4, 8])
def test_actual_finalizer_progress_keeps_one_call_grant_and_avoids_volume_censuses(
    tmp_path, monkeypatch, record_property, count
):
    with _metadata_source(tmp_path, monkeypatch, count=count) as fixture:
        original = _owner(fixture, audit=False)
        statements, censuses = [], []
        engine = original.case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            if statement.lstrip().startswith("SELECT"):
                if "FROM archive_write_effects" in statement:
                    statements.append((statement, parameters))
                if "count(" in statement and "collection_archive_object_uploads" in statement:
                    censuses.append(statement)

        event.listen(engine, "before_cursor_execute", capture)
        try:
            for _ in range(count + 1):
                assert original.owner.advance()
            assert not original.owner.advance()
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert _count(original.case) == 0 and not censuses
        assert len(statements) <= 12 * (count + 1) + 2
        assert all("LIMIT" in s or "archive_write_effects.id =" in s for s, _ in statements)
        bounded = [(s, p) for s, p in statements if "unresolved IS 1" in s]
        assert bounded
        with engine.connect() as connection:
            plan = connection.exec_driver_sql(
                "EXPLAIN QUERY PLAN " + bounded[0][0], bounded[0][1]
            ).all()
        assert any("ix_archive_write_effects_pending" in row[-1] for row in plan)
        assert all("SCAN " not in row[-1] and "TEMP B-TREE" not in row[-1] for row in plan)
        record_property(
            "metadata_finalizer_work",
            {
                "volumes": count,
                "metadata_objects": count + 1,
                "call_grant": 1,
                "effect_selects": len(statements),
                "sealed_volume_censuses": len(censuses),
            },
        )


@pytest.mark.parametrize("budget", ["default", "explicit-small"])
def test_actual_many_part_pack_metadata_uses_independent_configured_preparation_grant(
    tmp_path, monkeypatch, record_property, budget
):
    from riverhog_age import CHUNK_SIZE
    from riverhog_core.archive_write_grants import ArchiveWriteGrants
    from riverhog_core.catalog_archive_write_models import ArchiveWritePreparationRecord
    from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
    from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation

    from tests.unit.test_collection_uploads import _CREATOR

    def configure(service):
        service._policy = replace(
            service._policy, pack_artifacts=128, pack_part_plaintext_bytes=CHUNK_SIZE
        )

    def seal(case):
        case.service.complete(case.collection_id)
        volumes = case.service.list_volumes(case.collection_id)["volumes"]
        assert len(volumes) == 2
        for volume in volumes:
            with session_scope(case.factory) as session:
                units = session.get(Volume, (case.collection_id, volume["volume_id"])).total_units
            for number in range(units):
                work = case.service.get_unit(case.collection_id, volume["volume_id"], number)
                content = b"".join(
                    case.content[
                        int(source["offset"]) : int(source["offset"]) + int(source["bytes"])
                    ]
                    for source in work["sources"]
                )
                case.service.upload_unit(
                    case.collection_id,
                    volume["volume_id"],
                    number,
                    principal=_CREATOR,
                    plan_sha256=volume["plan_sha256"],
                    content=content,
                )
        while case.service._advance_archive_tree_checkpoint(case.collection_id):
            pass

    with _metadata_source(
        tmp_path,
        monkeypatch,
        "pack",
        count=129,
        content=b"x" * (CHUNK_SIZE - 512),
        configure_service=configure,
        seal_case=seal,
    ) as fixture:
        original = _owner(fixture)
        with session_scope(original.case.factory) as session:
            source = ArchiveVolumeMetadataSource.load(session, original.case.collection_id, 0)
        plaintext_bytes = len(source.publication.plaintext)
        assert plaintext_bytes > ArchiveWriteGrants().preparation_bytes
        if budget == "explicit-small":
            original.owner._config = replace(
                original.config,
                archive_write=replace(
                    original.config.archive_write,
                    metadata_preparation_bytes=ArchiveWriteGrants().preparation_bytes,
                ),
            )
            with pytest.raises(Conflict, match="preparation exceeds its byte grant"):
                original.owner.advance()
            assert original.calls == {"observe": [], "execute": []} and _count(original.case) == 0
            with session_scope(original.case.factory) as session:
                assert (
                    session.get(
                        ArchiveWritePreparationRecord,
                        (
                            original.case.collection_id,
                            ImmutableArchivePreparation.object_id(source.publication.object_path),
                        ),
                    )
                    is None
                )
        else:
            assert original.owner.advance()
            with session_scope(original.case.factory) as session:
                prepared = session.get(
                    ArchiveWritePreparationRecord,
                    (
                        original.case.collection_id,
                        ImmutableArchivePreparation.object_id(source.publication.object_path),
                    ),
                )
                stored = json.loads(prepared.preparation_json)
                capacity = stored["capacity_bytes"]
                assert (
                    len(prepared.preparation_json.encode()) > ArchiveWriteGrants().preparation_bytes
                )
                assert capacity == ArchiveWriteGrants().metadata_preparation_bytes
            assert original.owner.advance()
            assert original.owner.advance()
            assert not original.owner.advance()
            assert _count(original.case) == 0 and not original.owner.has_pending
        current_grant = original.owner._config.archive_write
        record_property(
            "metadata_preparation_work",
            {
                "members": 129,
                "volume_metadata_plaintext_bytes": plaintext_bytes,
                "payload_preparation_grant": ArchiveWriteGrants().preparation_bytes,
                "metadata_preparation_grant": current_grant.metadata_preparation_bytes,
            },
        )


def test_archive_metadata_selection_requires_its_original_claim(tmp_path, monkeypatch):
    with _metadata_source(tmp_path, monkeypatch) as f:
        claim = _current_claim(f.case)
        original = _owner(f, forward_guard=claim.guard)
        before = _snapshot(f.case)
        _replace_claim(f.case, _different_claim(claim))
        with pytest.raises(Conflict, match="original finalization claim"):
            original.owner.advance()
        assert not original.owner.has_pending and _snapshot(f.case) == before
        assert original.calls == {"observe": [], "execute": []} and _count(f.case) == 0


@pytest.mark.parametrize("boundary", ("preparation", "admission"))
def test_archive_metadata_rechecks_original_claim_after_source_selection(
    tmp_path, monkeypatch, boundary
):
    with _metadata_source(tmp_path, monkeypatch) as f:
        claim = _current_claim(f.case)
        original = _owner(f, forward_guard=claim.guard)
        owner_type, method = (
            (ArchiveWriteAdmission, "bind_epoch")
            if boundary == "preparation"
            else (ImmutableArchivePreparation, "prepare")
        )
        advance = getattr(owner_type, method)

        def changed(owner, *args, **kwargs):
            result = advance(owner, *args, **kwargs)
            _replace_claim(f.case, _different_claim(claim))
            return result

        with monkeypatch.context() as patch:
            patch.setattr(owner_type, method, changed)
            with pytest.raises(Conflict, match="original finalization claim"):
                original.owner.advance()
        actor = original.owner._actor
        assert actor is not None and original.owner.has_pending
        assert (actor._prepared is None) == (boundary == "preparation")
        assert original.calls == {"observe": [], "execute": []} and _count(f.case) == 0
        _replace_claim(f.case, claim.claim_id)
        assert original.owner.advance() and not original.owner.has_pending
        assert len(original.calls["execute"]) == 1

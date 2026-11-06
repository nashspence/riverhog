from __future__ import annotations

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_age import CHUNK_SIZE, UploadState
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWritePreparationRecord,
)
from riverhog_core.catalog_db import dispose_session_factory, session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.pack_upload import PackUploadCheckpoint, PackVolumeUploader
from riverhog_core.raw_upload import RawUploadCheckpoint, RawVolumeUploader
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol import (
    COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX,
    CollectionUploadRawDigestBatchDocument,
)
from riverhog_protocol.errors import Conflict
from riverhog_protocol.pack_ingress import canonical_json_bytes
from riverhog_protocol.raw_ingress import ordered_raw_part_commitment
from sqlalchemy import select

from tests.unit.test_collection_uploads import _CREATOR, _service_with_archive_objects


@contextmanager
def _case(
    tmp_path,
    kind,
    *,
    database_url=None,
    content=None,
    configure_service=None,
    use_cache=False,
    artifact_count=None,
):
    service, config, provider, roots = _service_with_archive_objects(
        tmp_path,
        policy=CollectionVolumePolicy(
            pack_artifacts=1,
            pack_member_bytes=max(64, len(content or b"") + 1) if kind == "pack" else 8,
            raw_part_plaintext_bytes=CHUNK_SIZE,
            raw_volume_plaintext_bytes=2 * CHUNK_SIZE,
        ),
        database_url=database_url,
    )
    try:
        if configure_service is not None:
            configure_service(service)
        opened = service.create_or_resume(
            idempotency_key="retained-preparation",
            ingest_source="fixture",
            archive_store=None,
            use_cache=use_cache,
            initiator=_CREATOR,
            event_context=None,
        )
        collection_id = int(opened["collection_id"])
        content = b"retained original encryption" if content is None else content
        digest = hashlib.sha256(content).hexdigest()
        member = {"artifact_id": "1" * 64, "bytes": str(len(content)), "sha256": digest}
        if kind == "segment":
            part_digests = [
                hashlib.sha256(content[first : first + CHUNK_SIZE]).hexdigest()
                for first in range(0, len(content), CHUNK_SIZE)
            ]
            count, commitment = ordered_raw_part_commitment(part_digests)
            member["raw_parts"] = {
                "part_plaintext_bytes": str(CHUNK_SIZE),
                "part_count": str(count),
                "ordered_sha256": commitment,
            }
        members = [member]
        if kind == "pack":
            members.append(
                {
                    "artifact_id": "2" * 64,
                    "bytes": "0",
                    "sha256": hashlib.sha256(b"").hexdigest(),
                }
            )
        if artifact_count is not None:
            members = [
                {**member, "artifact_id": format(i + 1, "064x")} for i in range(artifact_count)
            ]
        for first in range(0, len(members), COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX):
            service.register_artifacts(
                collection_id, members[first : first + COLLECTION_UPLOAD_ARTIFACT_BATCH_MAX]
            )
        if kind == "segment":
            for current in members:
                service.register_raw_part_digests(
                    collection_id,
                    CollectionUploadRawDigestBatchDocument(
                        artifact_id=current["artifact_id"], first_part="0", sha256s=part_digests
                    ),
                )
        volume = service.list_volumes(collection_id)["volumes"][0]
        assert volume["kind"] == kind
        yield SimpleNamespace(
            service=service,
            provider=provider,
            factory=service._session_factory,
            collection_id=collection_id,
            object_id=volume["volume_id"],
            plan_sha256=volume["plan_sha256"],
            content=content,
        )
    finally:
        service.close()
        dispose_session_factory(service._session_factory)


def _unit(case):
    return case.service.upload_unit(
        case.collection_id,
        case.object_id,
        0,
        principal=_CREATOR,
        plan_sha256=case.plan_sha256,
        content=case.content,
    )


def _preparation(case):
    with session_scope(case.factory) as session:
        row = session.get(ArchiveWritePreparationRecord, (case.collection_id, case.object_id))
        return None if row is None else row.preparation_json


def _checkpoint(case):
    with session_scope(case.factory) as session:
        record = session.get(
            CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
        )
        return record.kind, record.checkpoint_json


def _assert_preparation_committed_before_provider(case, monkeypatch, *, through_http=False):
    observed = []
    original = {}

    def inspect():
        with session_scope(case.factory) as session:
            if session.get_bind().dialect.name == "sqlite":
                session.connection(execution_options={"sqlite_immediate_writer": True})
            upload = session.scalar(
                select(CollectionUploadRecord)
                .where(CollectionUploadRecord.collection_id == case.collection_id)
                .with_for_update(nowait=True)
            )
            assert upload is not None
            row = session.get(ArchiveWritePreparationRecord, (case.collection_id, case.object_id))
            assert row is not None
            state = UploadState.from_json_bytes(
                canonical_json_bytes(json.loads(row.preparation_json)["age_state"])
            )
            assert state.plaintext_size is not None
            return row.preparation_json

    for name in ("find_completed_write", "begin_write"):
        method = getattr(case.provider, name)

        def wrapped(_method=method, _name=name, **arguments):
            retained = inspect()
            if original:
                assert retained == original["preparation"]
            else:
                original["preparation"] = retained
            observed.append(_name)
            return _method(**arguments)

        monkeypatch.setattr(case.provider, name, wrapped)
    if through_http:
        from tests.support.upload_api import UploadServiceApi

        api = UploadServiceApi(case.service, _CREATOR)
        try:
            result = api.put_collection_upload_session_unit(
                case.collection_id,
                case.object_id,
                0,
                plan_sha256=case.plan_sha256,
                content=case.content,
            )
            assert result.state == "committed"
        finally:
            api.close()
    else:
        assert _unit(case)["state"] == "committed"
    assert observed == ["find_completed_write", "begin_write"]
    kind, checkpoint = _checkpoint(case)
    parser = PackUploadCheckpoint if kind == "pack" else RawUploadCheckpoint
    actual = parser.from_json(checkpoint).age_state_json
    assert json.loads(actual) == json.loads(original["preparation"])["age_state"]


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_actual_unit_retains_original_crypto_before_lookup_and_begin(tmp_path, monkeypatch, kind):
    with _case(tmp_path, kind) as case:
        _assert_preparation_committed_before_provider(case, monkeypatch)


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_actual_unit_rejects_exhausted_preparation_grant_before_provider(
    tmp_path, monkeypatch, kind
):
    with _case(tmp_path, kind) as case:
        case.service._config = replace(
            case.service._config,
            archive_write=replace(case.service._config.archive_write, preparation_bytes=1),
        )
        calls = []

        def provider(**arguments):
            calls.append(arguments)
            pytest.fail("exhausted preparation dispatched provider work")

        for name in ("find_completed_write", "begin_write"):
            monkeypatch.setattr(case.provider, name, provider)
        with pytest.raises(Conflict, match="preparation exceeds"):
            _unit(case)
        assert calls == [] and _preparation(case) is None


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_lost_begin_retains_original_preparation_for_cold_recovery(tmp_path, monkeypatch, kind):
    with _case(tmp_path, kind) as case:
        begin = case.provider.begin_write

        def lose_reply(**arguments):
            begin(**arguments)
            raise OSError("injected begin reply loss")

        monkeypatch.setattr(case.provider, "begin_write", lose_reply)
        with pytest.raises(OSError, match="begin reply loss"):
            _unit(case)
        original = _preparation(case)
        assert original is not None and len(case.provider.uploads) == 1
        assert _checkpoint(case)[1] is None
        owner = ArchiveWriteAdmission(case.factory)
        guard = case.service._payload_write_guard(case.collection_id, _CREATOR)
        epoch = owner.bind_epoch(case.collection_id, guard=guard)
        with session_scope(case.factory) as session:
            volume = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            identity = {
                "kind": volume.kind,
                "object_path": volume.object_path,
                "plan_sha256": volume.plan_sha256,
            }
        assert (
            owner.load_preparation(
                case.collection_id, case.object_id, epoch=epoch, identity=identity, guard=guard
            )
            == original
        )
        assert len(case.provider.uploads) == 1


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_terminal_selection_during_crypto_preparation_prevents_provider_dispatch(
    tmp_path, monkeypatch, kind
):
    with _case(tmp_path, kind) as case:
        ready, release = threading.Event(), threading.Event()
        cls = PackVolumeUploader if kind == "pack" else RawVolumeUploader
        prepare = cls.prepare
        calls = []

        def held_prepare(self, **arguments):
            result = prepare(self, **arguments)
            ready.set()
            assert release.wait(10)
            return result

        def provider(**arguments):
            calls.append(arguments)
            pytest.fail("closed actor dispatched provider work")

        monkeypatch.setattr(cls, "prepare", held_prepare)
        for name in ("find_completed_write", "begin_write"):
            monkeypatch.setattr(case.provider, name, provider)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_unit, case)
            try:
                assert ready.wait(10)
                owner = ArchiveWriteAdmission(case.factory)
                selected = owner.select_terminal(
                    case.collection_id,
                    kind="cancel",
                    request={"selected": "exact-request"},
                    principal_id=_CREATOR.id,
                    grants=case.service._config.archive_write,
                    guard=case.service._payload_write_guard(case.collection_id, _CREATOR),
                    validate_selection=lambda session, upload: None,
                )
                with session_scope(case.factory) as session:
                    terminal = session.get(ArchiveUploadTerminalRecord, selected)
                    assert terminal.phase == "draining"
            finally:
                release.set()
            with pytest.raises(Conflict, match="admission is closed"):
                future.result(timeout=10)
        assert calls == [] and _preparation(case) is None


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_existing_checkpoint_preparation_is_validated_without_provider_access(
    tmp_path, monkeypatch, kind
):
    from riverhog_core.pack_volume import parse_pack_volume_plan
    from riverhog_core.raw_volume import parse_raw_volume_plan

    with _case(tmp_path, kind) as case:
        with session_scope(case.factory) as session:
            row = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            upload = session.get(CollectionUploadRecord, case.collection_id)
            arguments = {
                "collection_id": case.collection_id,
                "object_path": row.object_path,
                "relative_path": row.relative_path,
            }
            if kind == "pack":
                arguments["plan"] = parse_pack_volume_plan(row.plan_json)
                uploader = case.service._pack_uploader(
                    case.provider, passphrase_id=upload.passphrase_id
                )
            else:
                arguments["plan"] = parse_raw_volume_plan(row.plan_json)
                arguments["target_part_plaintext_bytes"] = row.unit_plaintext_bytes
                arguments["expected_part_sha256s"] = (hashlib.sha256(case.content).hexdigest(),)
                uploader = case.service._raw_uploader(
                    case.provider, passphrase_id=upload.passphrase_id
                )
        assert uploader.load_preparation(**arguments) is None
        original = uploader.open(**arguments).age_state_json
        assert _preparation(case) is None
        calls = []

        def provider(**arguments):
            calls.append(arguments)
            pytest.fail("checkpoint preparation lookup accessed the provider")

        with monkeypatch.context() as patch:
            for name in ("find_completed_write", "begin_write"):
                patch.setattr(case.provider, name, provider)
            assert uploader.load_preparation(**arguments).to_json_bytes().decode() == original
            with pytest.raises(ValueError, match="does not match"):
                uploader.load_preparation(**{**arguments, "object_path": "different"})
        assert calls == []
        cls = PackVolumeUploader if kind == "pack" else RawVolumeUploader

        def generate(self, **arguments):
            pytest.fail("resuming an original checkpoint regenerated encryption state")

        monkeypatch.setattr(cls, "prepare", generate)
        assert _unit(case)["state"] == "committed"
        retained = _preparation(case)
        assert json.loads(retained)["age_state"] == json.loads(original)


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_actual_binary_http_retains_original_crypto_before_provider_lookup(
    tmp_path, monkeypatch, kind
):
    with _case(tmp_path, kind) as case:
        _assert_preparation_committed_before_provider(case, monkeypatch, through_http=True)


def _assert_competing_candidates_retain_one_original(case, monkeypatch):
    from riverhog_core.pack_volume import parse_pack_volume_plan
    from riverhog_core.raw_volume import parse_raw_volume_plan

    with session_scope(case.factory) as session:
        row = session.get(CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id))
        upload = session.get(CollectionUploadRecord, case.collection_id)
        identity = {
            "kind": row.kind,
            "object_path": row.object_path,
            "plan_sha256": row.plan_sha256,
        }
        if row.kind == "pack":
            plan = parse_pack_volume_plan(row.plan_json)
            uploader = case.service._pack_uploader(
                case.provider, passphrase_id=upload.passphrase_id
            )
        else:
            plan = parse_raw_volume_plan(row.plan_json)
            uploader = case.service._raw_uploader(case.provider, passphrase_id=upload.passphrase_id)
    ready = threading.Barrier(2)
    candidates = []

    def prepare():
        candidate = uploader.prepare(plan=plan)
        candidates.append(candidate.to_json_bytes())
        ready.wait(timeout=10)
        return candidate

    def retain():
        admission = ArchiveWriteAdmission(case.factory)
        guard = case.service._payload_write_guard(case.collection_id, _CREATOR)
        epoch = admission.bind_epoch(case.collection_id, guard=guard)
        return case.service._retain_volume_preparation(
            admission=admission,
            collection_id=case.collection_id,
            volume_id=case.object_id,
            epoch=epoch,
            identity=identity,
            guard=guard,
            load=lambda: None,
            prepare=prepare,
        ).to_json_bytes()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(retain) for _ in range(2)]
        results = [future.result(timeout=15) for future in futures]
    assert len(set(candidates)) == 2
    assert results[0] == results[1] and results[0] in candidates
    retained = canonical_json_bytes(json.loads(_preparation(case))["age_state"])
    assert results == [retained, retained]
    assert case.provider.uploads == {}


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_competing_preparation_candidates_retain_one_exact_original(tmp_path, monkeypatch, kind):
    with _case(tmp_path, kind) as case:
        _assert_competing_candidates_retain_one_original(case, monkeypatch)


@pytest.mark.parametrize("kind", ["pack", "segment"])
def test_lost_preparation_acknowledgement_reopens_original_without_regeneration(
    tmp_path, monkeypatch, kind
):
    from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService

    with _case(tmp_path, kind) as case:
        prepare = ArchiveWriteAdmission.prepare

        def lose_ack(self, *arguments, **keywords):
            prepare(self, *arguments, **keywords)
            raise OSError("injected preparation acknowledgement loss")

        monkeypatch.setattr(ArchiveWriteAdmission, "prepare", lose_ack)
        with pytest.raises(OSError, match="preparation acknowledgement loss"):
            _unit(case)
        original = _preparation(case)
        assert original is not None and case.provider.uploads == {}
        assert _checkpoint(case)[1] is None

        def regenerate(self, **arguments):
            pytest.fail(
                "a retained original preparation was regenerated or reparsed from a checkpoint"
            )

        cls = PackVolumeUploader if kind == "pack" else RawVolumeUploader
        monkeypatch.setattr(cls, "prepare", regenerate)
        monkeypatch.setattr(cls, "load_preparation", regenerate)
        cold = SqlAlchemyCollectionUploadService(
            case.service._config, case.service._archive_stores, policy=case.service._policy
        )
        reopened = SimpleNamespace(
            **{**vars(case), "service": cold, "factory": cold._session_factory}
        )
        try:
            _assert_preparation_committed_before_provider(reopened, monkeypatch)
            assert _preparation(reopened) == original
        finally:
            cold.close()
            dispose_session_factory(cold._session_factory)

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_native_seal_receiving import (
    NativeArchiveSealSource,
    OriginalNativeArchiveSeal,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission, _writer
from riverhog_protocol.errors import Conflict
from sqlalchemy import event

from tests.unit.test_collection_upload_preparation import _case, _unit


def _missing_receipt(case):
    _unit(case)
    with session_scope(case.factory) as session:
        volume = session.get(Volume, (case.collection_id, case.object_id))
        original = volume.sealed_receipt_json
        assert original is not None and volume.checkpoint_json is not None
        volume.sealed_receipt_json = None
        upload = session.get(Upload, case.collection_id)
        upload.lease_expires_at = "2000-01-01T00:00:00Z"
    return original


def _snapshot(case):
    with session_scope(case.factory) as session:
        volume = session.get(Volume, (case.collection_id, case.object_id))
        upload = session.get(Upload, case.collection_id)
        control = session.get(ArchiveWriteControlRecord, case.collection_id)
        return (
            volume.checkpoint_json,
            volume.sealed_receipt_json,
            upload.lease_expires_at,
            upload.payload_sealed_artifact_count,
            control.gate,
            control.epoch,
            control.next_call,
            control.custody_revision,
        )


def _terminal(case):
    return ArchiveWriteAdmission(case.factory).select_terminal(
        case.collection_id,
        kind="cancel",
        request={"fixture": "native-seal"},
        principal_id="uploader",
        key_id=None,
        grants=case.service._config.archive_write,
        guard=lambda session: None,
        validate_selection=lambda *args: None,
    )


def _forbidden(*args, **kwargs):
    pytest.fail("native receipt receiving must not replay provider or source selection")


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("terminal", (False, True))
def test_original_native_seal_survives_lost_reply_without_reselection_or_lease_renewal(
    tmp_path, monkeypatch, kind, committed, terminal, database_url=None
):
    with _case(tmp_path, kind, database_url=database_url) as case:
        expected = _missing_receipt(case)
        before = _snapshot(case)
        for name in (
            "find_completed_write",
            "begin_write",
            "write_segment",
            "complete_write",
            "list_segments",
            "abort_write",
        ):
            if hasattr(case.provider, name):
                monkeypatch.setattr(case.provider, name, _forbidden)
        created, received = [], []
        owner = OriginalNativeArchiveSeal(case.service, case.collection_id)
        factory_name = "_pack_uploader" if kind == "pack" else "_raw_uploader"
        original_factory = getattr(case.service, factory_name)

        def create(*args, **kwargs):
            assert owner.has_pending and owner._source is not None
            # Format/key construction happens after source selection leaves SQL.
            with session_scope(case.factory) as session:
                _writer(session, defer_if_busy=True)
            created.append(True)
            return original_factory(*args, **kwargs)

        monkeypatch.setattr(case.service, factory_name, create)
        record = case.service._record_sealed_volume

        def lose_reply(collection_id, receipt, **kwargs):
            received.append(receipt)
            if len(received) == 1:
                if committed:
                    record(collection_id, receipt, **kwargs)
                raise OSError("original native receipt acknowledgement lost")
            return record(collection_id, receipt, **kwargs)

        monkeypatch.setattr(case.service, "_record_sealed_volume", lose_reply)
        assert created == [] and owner._source is None and not owner.has_pending
        with pytest.raises(OSError):
            owner.advance()
        source, receipt = owner._source, owner._receipt
        assert source is not None and receipt is not None and owner.has_pending
        monkeypatch.setattr(NativeArchiveSealSource, "load", _forbidden)
        monkeypatch.setattr(case.service, factory_name, _forbidden)
        if terminal:
            operation = _terminal(case)
            receiving_before = _snapshot(case)
            assert owner.drain_terminal(operation) == "closed"
            with pytest.raises(Conflict, match="stopped"):
                owner.advance()
        else:
            receiving_before = _snapshot(case)
            assert owner.advance() is True
            assert owner.advance() is True
        after = _snapshot(case)
        assert received == [receipt, receipt] and received[0] is received[1]
        assert len(created) == 1 and owner._source is source and not owner.has_pending
        assert after[0] == before[0] and after[1] == expected
        assert after[2:4] == before[2:4]
        assert after[4:] == receiving_before[4:]
        assert (
            len(source.checkpoint_json.encode()) + len(expected.encode()) <= source.reserved_bytes
        )


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("field", ("collection_id", "object_path", "relative_path"))
def test_native_seal_checkpoint_must_match_independent_catalog_identity(
    tmp_path, monkeypatch, kind, field
):
    with _case(tmp_path, kind) as case:
        _missing_receipt(case)
        with session_scope(case.factory) as session:
            row = session.get(Volume, (case.collection_id, case.object_id))
            checkpoint = json.loads(row.checkpoint_json)
            checkpoint[field] = 999 if field == "collection_id" else "other/opaque.age"
            row.checkpoint_json = json.dumps(checkpoint, sort_keys=True, separators=(",", ":"))
        before = _snapshot(case)
        monkeypatch.setattr(case.service, "_pack_uploader", _forbidden)
        monkeypatch.setattr(case.service, "_raw_uploader", _forbidden)
        owner = OriginalNativeArchiveSeal(case.service, case.collection_id)
        with pytest.raises(Conflict, match="differs from its selected source"):
            owner.advance()
        assert owner.has_pending and _snapshot(case) == before
        operation = _terminal(case)
        assert owner.drain_terminal(operation) == "pending"


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("field", ("plan_json", "checkpoint_json", "object_path"))
def test_native_seal_receiving_rechecks_original_source_and_keeps_exact_receipt(
    tmp_path, monkeypatch, kind, field
):
    with _case(tmp_path, kind) as case:
        expected = _missing_receipt(case)
        record = case.service._record_sealed_volume
        originals, receipts = {}, []

        def changed(collection_id, receipt, **kwargs):
            receipts.append(receipt)
            if not originals:
                with session_scope(case.factory) as session:
                    volume = session.get(Volume, (case.collection_id, case.object_id))
                    originals[field] = getattr(volume, field)
                    setattr(volume, field, originals[field] + " ")
            return record(collection_id, receipt, **kwargs)

        monkeypatch.setattr(case.service, "_record_sealed_volume", changed)
        owner = OriginalNativeArchiveSeal(case.service, case.collection_id)
        with pytest.raises(Conflict, match="source changed"):
            owner.advance()
        assert _snapshot(case)[1] is None and owner.has_pending
        with session_scope(case.factory) as session:
            volume = session.get(Volume, (case.collection_id, case.object_id))
            setattr(volume, field, originals[field])
        monkeypatch.setattr(NativeArchiveSealSource, "load", _forbidden)
        assert owner.advance() is True and not owner.has_pending
        assert receipts[0] is receipts[1] and _snapshot(case)[1] == expected


@pytest.mark.parametrize("kind", ("pack", "segment"))
def test_native_seal_budget_defers_before_full_source_load_and_can_be_increased(
    tmp_path, monkeypatch, kind
):
    with _case(tmp_path, kind) as case:
        expected = _missing_receipt(case)
        grants = case.service._config.archive_write
        source = NativeArchiveSealSource.load(case.factory, case.collection_id, grants)
        assert source is not None
        case.service._config = replace(
            case.service._config,
            archive_write=replace(grants, native_receipt_bytes=source.reserved_bytes - 1),
        )
        statements = []
        engine = case.factory.kw["bind"]

        def executed(connection, cursor, statement, parameters, context, many):
            statements.append(statement)

        event.listen(engine, "after_cursor_execute", executed)
        owner = OriginalNativeArchiveSeal(case.service, case.collection_id)
        try:
            with pytest.raises(Conflict, match="byte grant"):
                owner.advance()
        finally:
            event.remove(engine, "after_cursor_execute", executed)
        assert owner._source is None and not owner.has_pending
        selects = [s.split("FROM")[0] for s in statements if s.startswith("SELECT")]
        assert len(selects) == 2
        assert "length(CAST(" in selects[1] and "checkpoint_json" in selects[1]
        assert "construction_json," not in selects[1]
        assert _snapshot(case)[1] is None
        case.service._config = replace(
            case.service._config,
            archive_write=replace(grants, native_receipt_bytes=source.reserved_bytes),
        )
        assert owner.advance() is True and _snapshot(case)[1] == expected


@pytest.mark.parametrize("kind", ("pack", "segment"))
def test_native_receipt_constructor_loss_is_retained_and_never_reconstructed(
    tmp_path, monkeypatch, kind
):
    with _case(tmp_path, kind) as case:
        _missing_receipt(case)
        calls = []

        def failed(*args, **kwargs):
            calls.append(True)
            raise OSError("original native receiver constructor reply lost")

        name = "_pack_uploader" if kind == "pack" else "_raw_uploader"
        monkeypatch.setattr(case.service, name, failed)
        owner = OriginalNativeArchiveSeal(case.service, case.collection_id)
        with pytest.raises(OSError):
            owner.advance()
        with pytest.raises(Conflict, match="constructor remains unresolved"):
            owner.advance()
        assert len(calls) == 1 and owner.has_pending
        operation = _terminal(case)
        assert owner.drain_terminal(operation) == "pending"
        assert _snapshot(case)[1] is None


def test_native_terminal_drain_before_selection_cannot_construct_or_select(tmp_path, monkeypatch):
    with _case(tmp_path, "segment") as case:
        _missing_receipt(case)
        owner = OriginalNativeArchiveSeal(case.service, case.collection_id)
        monkeypatch.setattr(NativeArchiveSealSource, "load", _forbidden)
        monkeypatch.setattr(case.service, "_raw_uploader", _forbidden)
        operation = _terminal(case)
        assert owner.drain_terminal(operation) == "closed"
        assert not owner.has_pending and _snapshot(case)[1] is None
        with pytest.raises(Conflict, match="stopped"):
            owner.advance()


@pytest.mark.parametrize("kind", ("pack", "segment"))
def test_native_seal_receiving_preserves_unpublished_original_registration(tmp_path, kind):
    with _case(tmp_path, kind) as case:
        expected = _missing_receipt(case)
        with session_scope(case.factory) as session:
            session.get(Upload, case.collection_id).planner_next_sequence = 0
        owner = OriginalNativeArchiveSeal(case.service, case.collection_id)
        assert owner.advance() is True
        with session_scope(case.factory) as session:
            assert session.get(Volume, (case.collection_id, case.object_id)) is None
            session.info["include_unpublished_registration"] = True
            volume = session.get(Volume, (case.collection_id, case.object_id))
            assert volume.sealed_receipt_json == expected
            upload = session.get(Upload, case.collection_id)
            assert upload.planner_next_sequence == 0
            assert upload.lease_expires_at == "2000-01-01T00:00:00Z"


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("terminal", (False, True))
def test_application_pipeline_receives_native_checkpoint_before_later_semantic_stages(
    tmp_path, monkeypatch, kind, terminal
):
    with _case(tmp_path, kind, artifact_count=2 if kind == "pack" else 1) as case:
        expected = _missing_receipt(case)
        case.service.complete(case.collection_id)
        if kind == "pack":
            for volume in case.service.list_volumes(case.collection_id)["volumes"]:
                if volume["volume_id"] == case.object_id:
                    continue
                from tests.unit.test_collection_uploads import _CREATOR

                case.service.upload_unit(
                    case.collection_id,
                    volume["volume_id"],
                    0,
                    principal=_CREATOR,
                    plan_sha256=volume["plan_sha256"],
                    content=case.content,
                )
        claim = case.service._claim_due_finalization()
        assert claim is not None
        owner = case.service._finalization_owner(claim)
        assert owner._original is None and not owner.has_pending
        record = case.service._record_sealed_volume
        received = []

        def lost(collection_id, receipt, **kwargs):
            received.append(receipt)
            record(collection_id, receipt, **kwargs)
            if len(received) == 1:
                raise OSError("pipeline native receipt reply lost")

        monkeypatch.setattr(case.service, "_record_sealed_volume", lost)
        monkeypatch.setattr(case.service, "_catalog_finalization_owner", _forbidden)
        with pytest.raises(OSError, match="pipeline native receipt"):
            owner.advance()
        original = owner._original
        assert isinstance(original, OriginalNativeArchiveSeal) and owner.has_pending
        monkeypatch.setattr(NativeArchiveSealSource, "load", _forbidden)
        if terminal:
            operation = _terminal(case)
            assert owner.drain_terminal(operation) == "closed"
        else:
            assert owner.advance() is True and not owner.has_pending
            assert owner.advance() is True
        assert owner._original is original
        assert received[0] is received[1] and len(received) == 2
        assert _snapshot(case)[1] == expected


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("change", ("collection", "volume", "sequence", "path", "kind"))
def test_completed_native_receipt_cannot_receive_under_another_source_identity(
    tmp_path, kind, change
):
    with _case(tmp_path, kind) as case:
        expected = _missing_receipt(case)
        owner = OriginalNativeArchiveSeal(case.service, case.collection_id)
        assert owner.advance() is True
        receipt, source = owner._receipt, owner._source
        assert receipt is not None and source is not None
        with session_scope(case.factory) as session:
            session.get(Volume, (case.collection_id, case.object_id)).sealed_receipt_json = None
        before = _snapshot(case)
        collection_id = case.collection_id
        if change == "collection":
            collection_id += 1
        elif change == "volume":
            receipt = replace(receipt, volume_id=kind + "-" + "f" * 64)
        elif change == "sequence":
            receipt = replace(receipt, sequence=receipt.sequence + 1)
        elif change == "path":
            receipt = replace(receipt, relative_path="volumes/another-original.bin.age")
        else:
            source = replace(source, kind="segment" if kind == "pack" else "pack")
        statements = []

        def executed(connection, cursor, statement, parameters, context, many):
            statements.append(statement)

        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", executed)
        try:
            with pytest.raises(Conflict, match="receipt differs from its original source"):
                case.service._record_sealed_volume(collection_id, receipt, source=source)
        finally:
            event.remove(engine, "before_cursor_execute", executed)
        assert statements == [] and _snapshot(case) == before
        case.service._record_sealed_volume(case.collection_id, owner._receipt, source=owner._source)
        assert _snapshot(case)[1] == expected

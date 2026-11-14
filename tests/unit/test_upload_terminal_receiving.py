from __future__ import annotations

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord

from tests.unit.test_archive_payload_unit import _service_actor, _stored
from tests.unit.test_archive_upload_invocations import _receive
from tests.unit.test_collection_uploads import _CREATOR


@pytest.mark.parametrize("terminal", (False, True))
@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
@pytest.mark.parametrize("lease", ("2000-01-01T00:00:00Z", "2100-01-01T00:00:00Z"))
def test_original_seal_receiving_preserves_writer_lease(
    tmp_path, monkeypatch, kind, mode, lease, terminal
):
    _receive_original_seal(tmp_path, monkeypatch, kind, mode, lease, terminal=terminal)


def _receive_original_seal(
    tmp_path, monkeypatch, kind, mode, lease, *, database_url=None, terminal=True
):
    with _service_actor(
        tmp_path,
        monkeypatch,
        kind,
        mode,
        database_url=database_url,
        custody_mode="custody-transfer",
    ) as f:
        save = f.service._record_sealed_volume
        receipts = []

        def lose_reply(collection_id, receipt):
            receipts.append(receipt)
            if len(receipts) == 1:
                raise OSError("injected original seal receiving acknowledgement")
            return save(collection_id, receipt)

        monkeypatch.setattr(f.service, "_record_sealed_volume", lose_reply)
        assert f.submit() == "submitted"
        (first,) = _receive(f.registry, f.execution)
        with pytest.raises(OSError, match="seal receiving acknowledgement"):
            first.result()
        owner = f.originals[0]
        assert owner.has_pending and owner._receipt is receipts[0]
        with session_scope(f.case.factory) as session:
            upload = session.get(CollectionUploadRecord, f.case.collection_id)
            upload.lease_expires_at = lease
        if terminal:
            operation = f.admission.select_terminal(
                f.case.collection_id,
                kind="cancel",
                request={"fixture": "original-seal-receiving"},
                principal_id=_CREATOR.id,
                key_id=_CREATOR.key_id,
                grants=f.service._config.archive_write,
                guard=f.service._cancel_write_guard(f.case.collection_id, _CREATOR),
                validate_selection=lambda session, upload: None,
            )
            assert (
                f.registry.fence_terminal(
                    f.key, target_incarnation=f.target, operation_id=operation
                )
                == "fenced"
            )
        with session_scope(f.case.factory) as session:
            upload = session.get(CollectionUploadRecord, f.case.collection_id)
            phase = upload.archive_phase
            control = session.get(ArchiveWriteControlRecord, f.case.collection_id)
            before = control.epoch, control.next_call, control.terminal_id, control.custody_revision
        provider_calls = len(f.seen)
        if terminal:
            assert (
                f.registry.submit_terminal(
                    f.key, target_incarnation=f.target, operation_id=operation
                )
                == "submitted"
            )
        else:
            assert f.submit(lambda: pytest.fail("original receipt reconstructed")) == "submitted"
        (received,) = _receive(f.registry, f.execution)
        if terminal:
            assert received.result() == "closed"
        else:
            assert received.result()["state"] == "committed"
        assert len(f.seen) == provider_calls
        assert len(receipts) == 2 and receipts[0] is receipts[1]
        assert not owner.has_pending
        checkpoint, sealed, _ = _stored(f)
        assert checkpoint is not None and sealed is not None
        with session_scope(f.case.factory) as session:
            upload = session.get(CollectionUploadRecord, f.case.collection_id)
            control = session.get(ArchiveWriteControlRecord, f.case.collection_id)
            if terminal:
                assert control.gate == "closed"
            else:
                assert control.gate == "open"
            assert upload.lease_expires_at == lease
            assert upload.archive_phase == phase
            assert (
                control.epoch,
                control.next_call,
                control.terminal_id,
                control.custody_revision,
            ) == before
            assert int(upload.payload_sealed_artifact_count) > 0

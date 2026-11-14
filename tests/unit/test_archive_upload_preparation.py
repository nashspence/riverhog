from __future__ import annotations

from types import SimpleNamespace

import pytest
from riverhog_age import ResumableAgeScryptSession, UploadState

from tests.unit import test_pack_upload as pack
from tests.unit import test_raw_upload as raw


@pytest.fixture(params=["pack", "raw"])
def prepared_case(request):
    content = b"original prepared plaintext"
    if request.param == "raw":
        plan = raw._plan(content)
        store = raw.MemoryResumableStore()
        checkpoints = raw.MemoryRawCheckpointStore()

        def factory():
            return raw._uploader(store, checkpoints)

        relative_path = f"volumes/{plan.volume_id}.bin.age"
        extra = {"target_part_plaintext_bytes": raw.ARCHIVE_UNIT_BYTES}
        load = "load_raw_upload_checkpoint"
    else:
        plan = pack.plan_pack_volume([pack._artifact("a" * 64, content)], sequence=0)
        store = pack.MemoryResumableStore()
        checkpoints = pack.MemoryCheckpointStore()

        def factory():
            return pack._uploader(store, checkpoints)

        relative_path = f"volumes/{plan.volume_id}.tar.age"
        extra = {}
        load = "load_pack_upload_checkpoint"
    return SimpleNamespace(
        plan=plan,
        store=store,
        checkpoints=checkpoints,
        factory=factory,
        load=load,
        arguments={
            "collection_id": 1,
            "plan": plan,
            "object_path": "archives/original/" + relative_path,
            "relative_path": relative_path,
            **extra,
        },
    )


def _no_effect(*args, **kwargs):
    pytest.fail("encryption preparation must precede checkpoint and provider access")


def test_encryption_preparation_has_no_checkpoint_or_provider_access(prepared_case, monkeypatch):
    case = prepared_case
    monkeypatch.setattr(case.checkpoints, case.load, _no_effect)
    monkeypatch.setattr(case.store, "find_completed_write", _no_effect)
    monkeypatch.setattr(case.store, "begin_write", _no_effect)
    state = case.factory().prepare(plan=case.plan)
    assert state.plaintext_size == case.plan.plaintext_bytes
    assert not case.checkpoints.rows and not case.store.uploads


def test_cold_open_uses_the_exact_retained_preparation(prepared_case, monkeypatch):
    case = prepared_case
    original = case.factory().prepare(plan=case.plan).to_json_bytes()
    retained = UploadState.from_json_bytes(original)
    cold = case.factory()
    monkeypatch.setattr(cold, "_prepare_age_state", _no_effect)
    checkpoint = cold.open(**case.arguments, prepared_state=retained)
    unchanged = checkpoint.age_state_json.encode() == original
    assert unchanged and checkpoint.write_token in case.store.uploads
    resumed = case.factory().open(**case.arguments, prepared_state=retained)
    unchanged = resumed.age_state_json.encode() == original
    assert unchanged and resumed.write_token == checkpoint.write_token
    assert len(case.store.uploads) == 1


def test_changed_preparation_cannot_replace_a_checkpoint(prepared_case, monkeypatch):
    case = prepared_case
    original = case.factory().prepare(plan=case.plan)
    case.factory().open(**case.arguments, prepared_state=original)
    replacement = case.factory().prepare(plan=case.plan)
    monkeypatch.setattr(case.store, "find_completed_write", _no_effect)
    monkeypatch.setattr(case.store, "begin_write", _no_effect)
    with pytest.raises(ValueError, match="original encryption preparation changed"):
        case.factory().open(**case.arguments, prepared_state=replacement)
    assert len(case.store.uploads) == 1


def test_preparation_with_wrong_plaintext_size_is_rejected_before_begin(prepared_case, monkeypatch):
    case = prepared_case
    wrong = ResumableAgeScryptSession.create("archive passphrase", log_n=1).export_state(
        plaintext_size=case.plan.plaintext_bytes + 1
    )
    monkeypatch.setattr(case.checkpoints, case.load, _no_effect)
    monkeypatch.setattr(case.store, "find_completed_write", _no_effect)
    monkeypatch.setattr(case.store, "begin_write", _no_effect)
    with pytest.raises(ValueError, match="preparation does not match the plan size"):
        case.factory().open(**case.arguments, prepared_state=wrong)
    assert not case.checkpoints.rows and not case.store.uploads

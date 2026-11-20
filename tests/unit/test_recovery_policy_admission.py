from __future__ import annotations

import json
from dataclasses import replace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.catalog_db import initialize_db, session_scope, validate_db
from riverhog_core.services import archive_write_admission
from riverhog_recovery_policy import RecoveryPolicy
from sqlalchemy import select, text
from state_schema import StateSchemaError

from tests.unit.test_archive_write_admission import _case
from tests.unit.test_collection_uploads import _CREATOR


def _open(service, key):
    return int(
        service.create_or_resume(
            idempotency_key=key,
            ingest_source="recovery-policy-fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )["collection_id"]
    )


def _snapshot(factory, collection_id):
    with session_scope(factory) as session:
        row = session.get(ArchiveWriteControlRecord, collection_id)
        assert row is not None
        return (
            row.construction_json,
            row.epoch,
            row.recovery_policy_json,
            row.recovery_accounting_json,
            row.recovery_revision,
        )


def test_policy_is_frozen_with_original_construction_and_retry(tmp_path):
    with _case(tmp_path) as case:
        original = RecoveryPolicy(
            recovery_mode="restart",
            max_optional_checkpoints=0,
            retained_checkpoint_generations=1,
        )
        case.service._config = replace(case.config, recovery=original)
        collection_id = _open(case.service, "selected-policy")
        before = _snapshot(case.factory, collection_id)
        assert json.loads(before[2]) == original.to_mapping()
        accounting = json.loads(before[3])
        assert accounting["cost_epoch_open"] is True
        assert accounting["charged_ns"] == accounting["reserved_ns"] == "0"
        assert accounting["useful_ns"] == "0"
        assert accounting["active_grants"] == []
        assert accounting["successful_checkpoints"] == "0"
        assert before[4] == "0"

        case.service._config = replace(case.config, recovery=RecoveryPolicy())
        assert _open(case.service, "selected-policy") == collection_id
        assert _snapshot(case.factory, collection_id) == before
        next_id = _open(case.service, "new-policy")
        assert json.loads(_snapshot(case.factory, next_id)[2]) == RecoveryPolicy().to_mapping()


def test_policy_admission_rolls_back_with_unacknowledged_construction(tmp_path, monkeypatch):
    with _case(tmp_path) as case:
        with session_scope(case.factory) as session:
            before = set(session.scalars(select(ArchiveWriteControlRecord.collection_id)))
        original = archive_write_admission.initialize_archive_write_control

        def interrupted(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError("admission interrupted before commit")

        with monkeypatch.context() as patch:
            patch.setattr(archive_write_admission, "initialize_archive_write_control", interrupted)
            with pytest.raises(RuntimeError, match="before commit"):
                _open(case.service, "interrupted-policy")
        with session_scope(case.factory) as session:
            assert set(session.scalars(select(ArchiveWriteControlRecord.collection_id))) == before
        collection_id = _open(case.service, "interrupted-policy")
        assert (
            json.loads(_snapshot(case.factory, collection_id)[2]) == RecoveryPolicy().to_mapping()
        )


def test_incompatible_policy_storage_is_refused_without_reinitializing(tmp_path):
    with _case(tmp_path) as case:
        before = _snapshot(case.factory, case.collection_id)
        with session_scope(case.factory) as session:
            session.execute(
                text("ALTER TABLE archive_write_controls DROP COLUMN recovery_policy_json")
            )
        for inspect in (validate_db, initialize_db):
            with pytest.raises(StateSchemaError, match="schema|column|metadata|current"):
                inspect(case.config.database_url)
        with session_scope(case.factory) as session:
            row = session.execute(
                text(
                    "SELECT construction_json,epoch,recovery_accounting_json,recovery_revision "
                    "FROM archive_write_controls WHERE collection_id=:collection"
                ),
                {"collection": case.collection_id},
            ).one()
            assert tuple(row) == (before[0], before[1], before[3], before[4])
            assert session.execute(text("SELECT count(*) FROM collection_uploads")).scalar_one() > 0

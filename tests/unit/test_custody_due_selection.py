from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadArtifactProvenanceBindingRecord as Binding
from riverhog_core.catalog_models import CollectionUploadArtifactRecord as Artifact
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_custody_due import (
    ArchiveCustodyDue,
    refresh_custody_member,
    refresh_custody_parent,
    rotate_custody_member,
)
from riverhog_protocol import CollectionArtifactProvenanceBindingBatchDocument
from sqlalchemy import insert, select
from sqlalchemy.orm import load_only

from tests.support.finalization import drive_custody_turn
from tests.unit.test_canonical_upload_custody import _construction, _seal_payload
from tests.unit.test_collection_upload_raw_readiness_work import sqlite_work


@contextmanager
def _due_catalog(tmp_path, prefix, pending, *, history=False, parents=1, database_url=None):
    # These are unverified scheduling hints, never a custody or admission proof.
    case = _construction(tmp_path, database_url=database_url)
    try:
        with session_scope(case.factory) as session:

            def template(model):
                table = model.__table__
                row = dict(session.execute(select(table)).mappings().one())
                return {key: value for key, value in row.items() if table.c[key].computed is None}

            parent = template(Upload)
            control = template(Control)
            artifact = template(Artifact)
            binding = template(Binding)
            journal = template(Journal)
            inactive = []
            for i in range(3 * prefix):
                inactive.append(
                    {
                        **control,
                        "collection_id": 100 + i,
                        "gate": "closed" if i < prefix else "open",
                        "custody_primary_count": 1,
                        "custody_history_count": 1,
                        "custody_due_count": 1 if i < prefix else 0,
                    }
                )
            if inactive:
                session.execute(insert(Control.__table__), inactive)
            expected = []
            for i in range(parents):
                collection_id = 10_000 + i
                size = (3 if history else 2) * prefix + pending
                session.execute(
                    insert(Upload.__table__),
                    [
                        {
                            **parent,
                            "collection_id": collection_id,
                            "idempotency_key": f"custody-selector-{collection_id}",
                            "archive_storage_prefix": f"custody-selector/{collection_id}",
                            "artifact_count": size,
                        }
                    ],
                )
                session.execute(
                    insert(Control.__table__),
                    [
                        {
                            **control,
                            "collection_id": collection_id,
                            "custody_primary_count": prefix + pending if history else pending,
                            "custody_history_count": pending if history else 0,
                            "custody_due_count": pending,
                            "custody_requires_history": history,
                        }
                    ],
                )
                members, bindings, wanted = [], [], []
                for number in range(size):
                    identity = format(number + 1, "064x")
                    ready = number >= size - pending
                    sealed = number < prefix or number >= 2 * prefix
                    stage = (
                        (2 if history else 1) if ready else int(history and number >= 2 * prefix)
                    )
                    members.append(
                        {
                            **artifact,
                            "collection_id": collection_id,
                            "artifact_id": identity,
                            "artifact_order": number,
                            "payload_sealed_at": "2026-11-08T00:00:00Z" if sealed else None,
                            "custody_schedule_stage": stage,
                        }
                    )
                    if number >= prefix:
                        bindings.append(
                            {
                                **binding,
                                "collection_id": collection_id,
                                "artifact_id": identity,
                                "journal_id": binding["journal_id"],
                                "history_imports_ref_json": "{}" if ready and history else None,
                            }
                        )
                    if ready:
                        wanted.append(identity)
                # Member selection does not interpret journal rows. Only selected
                # semantic owners may verify this synthetic, unadmitted source.
                if members:
                    session.execute(insert(Artifact.__table__), members)
                if bindings:
                    # Each binding's journal belongs to its own parent.
                    session.execute(
                        insert(Journal.__table__), [{**journal, "collection_id": collection_id}]
                    )
                    session.execute(insert(Binding.__table__), bindings)
                expected.append((collection_id, wanted))
        yield case, expected
    finally:
        case.factory.kw["bind"].dispose()


@pytest.mark.parametrize("prefix", (0, 32, 1000))
@pytest.mark.parametrize("pending", (0, 1, 7))
@pytest.mark.parametrize("history", (False, True))
def test_custody_selection_skips_disjoint_member_and_ineligible_parent_prefixes(
    tmp_path, record_property, prefix, pending, history
):
    with _due_catalog(tmp_path, prefix, pending, history=history) as (case, expected):
        owner = ArchiveCustodyDue(case.factory)
        with sqlite_work(case.service) as work:
            selected = owner.select(limit=2)
        collection_id, members = expected[0]
        wanted = ((collection_id, members[0]),) if pending else ()
        assert selected == wanted
        assert work["vm_steps"] <= 600, (prefix, pending, history, work)
        record_property("disjoint_prefix_members", prefix)
        record_property("ineligible_parents", 3 * prefix)
        record_property("requires_history", history)
        record_property("sqlite_vm_steps", work["vm_steps"])


@pytest.mark.parametrize("parents", (1, 17, 129))
def test_parent_and_member_hints_rotate_fairly_with_linear_work(tmp_path, record_property, parents):
    with _due_catalog(tmp_path, 32, 2, parents=parents) as (case, expected):
        owner = ArchiveCustodyDue(case.factory)
        found, total = [], 0
        for _ in range(3 * parents):
            with sqlite_work(case.service) as work:
                selected = owner.select(limit=1)
            assert len(selected) == 1
            collection_id, artifact_id = selected[0]
            found.extend(selected)
            total += work["vm_steps"]
            with session_scope(case.factory) as session:
                rotate_custody_member(session, collection_id, artifact_id)
        assert found == [
            (collection_id, members[turn % 2])
            for turn in range(3)
            for collection_id, members in expected
        ]
        assert total <= 600 * len(found), (parents, total)
        with owner._lock:
            assert owner.select(limit=1) == ()
        record_property("parents", parents)
        record_property("fair_visits", len(found))
        record_property("sqlite_vm_steps", total)


def test_readiness_deltas_are_idempotent_and_parent_changes_do_not_visit_members(tmp_path):
    case = _construction(tmp_path)
    try:
        _seal_payload(case)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, 1, with_for_update=True)
            control = session.get(Control, 1)
            assert (
                control.custody_primary_count,
                control.custody_history_count,
                control.custody_due_count,
            ) == (1, 0, 1)
            assert not refresh_custody_member(session, upload, case.member_id)
            upload.state = "orphaned"
            refresh_custody_parent(session, upload)
            assert control.custody_due_count == 0
            upload.state = "open"
            refresh_custody_parent(session, upload)
            assert control.custody_due_count == 1
            control.gate = "closed"
        assert ArchiveCustodyDue(case.factory).select(limit=1) == ()
        with session_scope(case.factory) as session:
            control = session.get(Control, 1)
            control.gate = "open"
            upload = session.get(Upload, 1, with_for_update=True)
            upload.initiated_by_principal_id = "processing:" + "a" * 64
            refresh_custody_parent(session, upload)
            assert control.custody_due_count == 0
            upload.completion_requirement_json = "{}"
            refresh_custody_parent(session, upload)
            assert control.custody_requires_history and control.custody_due_count == 0
            binding = session.get(Binding, (1, case.member_id))
            binding.history_imports_ref_json = "{}"
            assert refresh_custody_member(session, upload, case.member_id)
            assert (
                control.custody_primary_count,
                control.custody_history_count,
                control.custody_due_count,
            ) == (1, 1, 1)
            artifact = session.get(Artifact, (1, case.member_id))
            artifact.custody_receipt_json = "{}"
            assert refresh_custody_member(session, upload, case.member_id)
            assert not refresh_custody_member(session, upload, case.member_id)
            assert (
                control.custody_primary_count,
                control.custody_history_count,
                control.custody_due_count,
            ) == (0, 0, 0)
    finally:
        case.factory.kw["bind"].dispose()


@pytest.mark.parametrize("binding_first", (False, True))
def test_actual_binding_retries_and_verified_receipt_maintain_due_hints(tmp_path, binding_first):
    case = _construction(tmp_path)
    try:
        if not binding_first:
            with session_scope(case.factory) as session:
                binding = session.get(Binding, (1, case.member_id))
                assert binding is not None
                session.delete(binding)
        _seal_payload(case)
        with session_scope(case.factory) as session:
            control = session.get(Control, 1)
            assert control.custody_due_count == int(binding_first)
        batch = CollectionArtifactProvenanceBindingBatchDocument(bindings=[case.primary.binding])
        for _ in range(2):
            case.service.bind_artifact_provenance(1, batch)
        owner = ArchiveCustodyDue(case.factory)
        assert owner.select(limit=1) == ((1, case.member_id),)
        with session_scope(case.factory) as session:
            control = session.get(Control, 1)
            assert (
                control.custody_primary_count,
                control.custody_history_count,
                control.custody_due_count,
            ) == (1, 0, 1)
        for _ in range(3):
            drive_custody_turn(case.service, limit=1)
        with session_scope(case.factory) as session:
            artifact = session.get(Artifact, (1, case.member_id))
            control = session.get(Control, 1)
            assert artifact.custody_receipt_json is not None
            assert artifact.custody_schedule_stage == 0
            assert (
                control.custody_primary_count,
                control.custody_history_count,
                control.custody_due_count,
            ) == (0, 0, 0)
        assert owner.select(limit=1) == ()
        assert drive_custody_turn(case.service, limit=1) == 0
    finally:
        case.factory.kw["bind"].dispose()


def test_readiness_refresh_keeps_large_parent_fields_deferred(tmp_path):
    case = _construction(tmp_path)
    try:
        _seal_payload(case)
        with session_scope(case.factory) as session:
            upload = session.scalar(
                select(Upload)
                .where(Upload.collection_id == 1)
                .options(load_only(Upload.collection_id, raiseload=True))
                .with_for_update()
            )
            assert upload is not None
            refresh_custody_parent(session, upload)
            assert not refresh_custody_member(session, upload, case.member_id)
            control = session.get(Control, 1)
            assert control.custody_due_count == 1
            assert "completion_requirement_json" not in upload.__dict__
            assert "initiated_by_principal_id" not in upload.__dict__
            assert "state" not in upload.__dict__
            upload.state = "orphaned"
            refresh_custody_parent(session, upload)
            assert control.custody_due_count == 0
            assert "completion_requirement_json" not in upload.__dict__
            upload.state = "open"
            refresh_custody_parent(session, upload)
            assert control.custody_due_count == 1
    finally:
        case.service._archive_execution.close()
        case.factory.kw["bind"].dispose()

from __future__ import annotations

from contextlib import contextmanager
from time import monotonic, sleep

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionRecord
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_protocol.errors import Conflict
from riverhog_protocol.paths import text_search_key

from tests.support.finalization import drive_finalization_turn
from tests.unit.test_archive_catalog_finalizer import _lose_commit_reply
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _bind_primary
from tests.unit.test_finalization_claim_readiness import _seal


@contextmanager
def _publication_case(tmp_path, monkeypatch, phase="terminal", *, database_url=None):
    with _case(tmp_path, "segment", artifact_count=1, database_url=database_url) as case:
        _bind_primary(case.service, case.collection_id, format(1, "064x"), case.content)
        _seal(case)
        deadline = monotonic() + 30
        last = None
        while True:
            with session_scope(case.factory) as session:
                upload = session.get(Upload, case.collection_id)
                assert upload is not None
                last = {
                    "state": upload.state,
                    "catalog_phase": upload.catalog_phase,
                    "authority_ready": upload.final_authority_json is not None,
                    "description_ready": upload.description_revision is not None,
                    "tags_ready": upload.tag_head_identity is not None,
                }
                ready = (
                    upload.final_authority_json is not None
                    and upload.description_revision is not None
                    and upload.tag_head_identity is not None
                    and upload.catalog_phase == phase
                )
            if ready:
                claim = case.service._claim_due_finalization()
                assert claim is not None
                owner = case.service._catalog_finalization_owner(claim, kind="catalog-projection")
                yield case, claim, owner
                return
            if monotonic() >= deadline:
                break
            drive_finalization_turn(case.service)
            sleep(0.01)
        pytest.fail(
            "maintained finalization did not reach the selected catalog phase; "
            f"expected={phase!r}, last={last!r}"
        )


@pytest.mark.parametrize("phase", ("artifact-set-identity", "artifacts", "terminal"))
@pytest.mark.parametrize("committed", (False, True))
def test_exact_projection_or_publication_survives_commit_reply_loss(
    tmp_path, monkeypatch, phase, committed
):
    _assert_publication_reply(tmp_path, monkeypatch, phase, committed)


def _assert_publication_reply(tmp_path, monkeypatch, phase, committed, *, database_url=None):
    with _publication_case(tmp_path, monkeypatch, phase, database_url=database_url) as (
        case,
        claim,
        owner,
    ):
        actual = owner._advance
        calls = []

        def advance(session, upload):
            calls.append((upload.catalog_phase, upload.catalog_cursor_json))
            return actual(session, upload)

        owner._advance = advance
        with _lose_commit_reply(monkeypatch, committed=committed):
            with pytest.raises(OSError, match="catalog commit acknowledgement"):
                owner.advance()
        assert owner.has_pending
        after = owner._published_after if phase == "terminal" else owner._after
        assert after is not None
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert (upload is None) is (phase == "terminal" and committed)
            if phase == "terminal":
                assert session.get(CollectionRecord, case.collection_id).is_published is committed
        assert owner.advance() is True and not owner.has_pending
        assert len(calls) == (1 if committed else 2)
        assert owner.advance() is True and len(calls) == (1 if committed else 2)
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            if phase == "terminal":
                assert upload is None
                assert owner._publication(session) == after
            else:
                assert owner._cursor(upload) == after


@pytest.mark.parametrize("change", ("generation", "unpublished", "missing", "construction"))
def test_retired_parent_needs_exact_original_publication_proof(tmp_path, monkeypatch, change):
    from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord

    with _publication_case(tmp_path, monkeypatch) as (case, claim, owner):
        with _lose_commit_reply(monkeypatch, committed=True):
            with pytest.raises(OSError):
                owner.advance()
        assert owner.has_pending and owner._published_after is not None
        original = owner._published_after
        with session_scope(case.factory) as session:
            collection = session.get(CollectionRecord, case.collection_id)
            if change == "generation":
                collection.archive_generation = "f" * 64
            elif change == "unpublished":
                collection.is_published = False
            elif change == "missing":
                session.delete(collection)
            else:
                control = session.get(ArchiveWriteControlRecord, case.collection_id)
                control.construction_json += " "
        with pytest.raises(Conflict, match="original catalog (publication|finalization)"):
            owner.advance()
        assert owner.has_pending and owner._published_after == original
        with session_scope(case.factory) as session:
            assert session.get(Upload, case.collection_id) is None


def test_mutable_presentation_does_not_replace_the_original_archive_publication(
    tmp_path, monkeypatch
):
    with _publication_case(tmp_path, monkeypatch) as (case, claim, owner):
        with _lose_commit_reply(monkeypatch, committed=True):
            with pytest.raises(OSError):
                owner.advance()
        assert owner.has_pending
        with session_scope(case.factory) as session:
            collection = session.get(CollectionRecord, case.collection_id)
            collection.description = "updated presentation"
            collection.description_search = text_search_key(collection.description)
            collection.description_revision += 1
            collection.description_identity = "f" * 64
            collection.tag_revision += 1
        assert owner.advance() is True and not owner.has_pending
        with session_scope(case.factory) as session:
            collection = session.get(CollectionRecord, case.collection_id)
            assert collection.description == "updated presentation"
            assert owner._publication(session) == owner._published_after


def test_an_unselected_owner_cannot_infer_success_from_a_retired_upload(tmp_path, monkeypatch):
    with _publication_case(tmp_path, monkeypatch) as (case, claim, original):
        assert original.advance() is True and not original.has_pending
        replacement = case.service._catalog_finalization_owner(claim, kind="catalog-projection")
        with pytest.raises(Conflict, match="original catalog finalization claim"):
            replacement.advance()
        assert not replacement.has_pending


@pytest.mark.parametrize("field", ("archive_generation", "archive_root_sha256"))
def test_publication_must_match_its_original_upload_authority(tmp_path, monkeypatch, field):
    with _publication_case(tmp_path, monkeypatch) as (case, claim, owner):
        with session_scope(case.factory) as session:
            collection = session.get(CollectionRecord, case.collection_id)
            original = getattr(collection, field)
            setattr(collection, field, "f" * 64)
        with pytest.raises(Conflict, match="no receiving proof"):
            owner.advance()
        assert owner.has_pending
        with session_scope(case.factory) as session:
            assert session.get(Upload, case.collection_id) is not None
            collection = session.get(CollectionRecord, case.collection_id)
            assert not collection.is_published
            setattr(collection, field, original)
        assert owner.advance() is True and not owner.has_pending


def test_rolled_back_publication_cannot_retry_changed_archive_source(tmp_path, monkeypatch):
    with _publication_case(tmp_path, monkeypatch) as (case, claim, owner):
        with _lose_commit_reply(monkeypatch, committed=False):
            with pytest.raises(OSError):
                owner.advance()
        assert owner.has_pending
        with session_scope(case.factory) as session:
            session.get(Upload, case.collection_id).archive_tree_sha256 = "f" * 64
        with pytest.raises(Conflict, match="original catalog finalization cursor changed"):
            owner.advance()
        assert owner.has_pending
        with session_scope(case.factory) as session:
            assert session.get(Upload, case.collection_id) is not None
            assert not session.get(CollectionRecord, case.collection_id).is_published

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionArtifactRecord, CollectionRecord
from riverhog_core.catalog_models import CollectionUploadArtifactProvenanceBindingRecord as Binding
from riverhog_core.catalog_models import CollectionUploadArtifactRecord as Artifact
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_artifact_custody_receipt as module
from riverhog_core.services.archive_artifact_custody_receipt import (
    ArchiveArtifactCustodyReceiptFinalizer as Finalizer,
)
from riverhog_core.services.archive_artifact_custody_receipt import (
    ArchiveArtifactCustodyReceiptSource as Source,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_finalizer_registry import _receive
from tests.unit.test_canonical_upload_custody import _construction, _seal_payload


@contextmanager
def _original(tmp_path, monkeypatch, *, database_url=None):
    case = _construction(tmp_path, database_url=database_url)
    _seal_payload(case)
    assert case.service.process_due_custody_receipts() == 1
    sources = []

    def hold(source, session):
        sources.append(source)
        raise OSError("synthetic custody acknowledgement not started")

    with monkeypatch.context() as patch:
        patch.setattr(Source, "apply", hold)
        with pytest.raises(OSError, match="not started"):
            case.service._advance_artifact_custody(1, case.member_id)
    (source,) = sources
    selections = []

    def select():
        selections.append(source)
        return source

    owner = Finalizer(case.factory, collection_id=1, select_source=select)
    try:
        yield case, source, owner, selections
    finally:
        case.factory.kw["bind"].dispose()


@contextmanager
def _lose_reply(monkeypatch, committed):
    actual = module.session_scope

    @contextmanager
    def lose(factory):
        with actual(factory) as session:
            yield session
            if not committed:
                raise OSError("synthetic custody commit acknowledgement")
        raise OSError("synthetic custody commit acknowledgement")

    with monkeypatch.context() as patch:
        patch.setattr(module, "session_scope", lose)
        yield


def _counts(case):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, 1)
        return upload.safe_release_artifact_count, upload.safe_release_artifact_bytes


def _terminal(case):
    return ArchiveWriteAdmission(case.factory).select_terminal(
        1,
        kind="cancel",
        request={"fixture": "custody-receipt"},
        principal_id="uploader",
        grants=case.config.archive_write,
        guard=lambda session: None,
        validate_selection=lambda *args: None,
    )


def _forbidden(*args):
    pytest.fail("an original custody acknowledgement cannot select a new member")


@pytest.mark.parametrize("committed", (False, True))
def test_original_custody_receipt_survives_commit_reply_loss_once(tmp_path, monkeypatch, committed):
    _assert_receipt_reply(tmp_path, monkeypatch, committed)


def _assert_receipt_reply(tmp_path, monkeypatch, committed, *, database_url=None):
    with _original(tmp_path, monkeypatch, database_url=database_url) as (
        case,
        source,
        owner,
        selections,
    ):
        assert _counts(case) == (0, 0)
        with _lose_reply(monkeypatch, committed):
            with pytest.raises(OSError):
                owner.advance()
        assert owner.has_pending and owner._source is source and selections == [source]
        assert _counts(case) == ((1, 3) if committed else (0, 0))
        owner._select_source = _forbidden
        case.service._config = case.service._archive_stores = None
        assert owner.advance() is True and not owner.has_pending
        assert owner.advance() is True and _counts(case) == (1, 3)
        with session_scope(case.factory) as session:
            artifact = session.get(Artifact, (1, case.member_id))
            assert artifact.custody_receipt_json == source.receipt_json


@pytest.mark.parametrize(
    "change",
    (
        "bytes",
        "sha256",
        "unsealed",
        "primary",
        "inputs",
        "requirement",
        "output",
        "construction",
        "generation",
        "epoch",
    ),
)
def test_pending_receipt_rejects_a_changed_original_source(tmp_path, monkeypatch, change):
    with _original(tmp_path, monkeypatch) as (case, source, owner, selections):
        with _lose_reply(monkeypatch, False):
            with pytest.raises(OSError):
                owner.advance()
        with session_scope(case.factory) as session:
            upload = session.get(Upload, 1)
            artifact = session.get(Artifact, (1, case.member_id))
            binding = session.get(Binding, (1, case.member_id))
            control = session.get(Control, 1)
            if change == "bytes":
                artifact.bytes += 1
            elif change == "sha256":
                artifact.sha256 = "f" * 64
            elif change == "unsealed":
                artifact.payload_sealed_at = None
            elif change == "primary":
                binding.prefix_sha256 = "f" * 64
            elif change == "inputs":
                binding.history_imports_ref_json = "{}"
            elif change == "requirement":
                upload.completion_requirement_json = "{}"
            elif change == "output":
                binding.completion_output_id = "another-output"
            elif change == "construction":
                control.construction_json += " "
            elif change == "generation":
                upload.archive_generation = "f" * 64
            else:
                control.epoch = "f" * 64
        owner._select_source = _forbidden
        with pytest.raises(Conflict):
            owner.advance()
        assert owner.has_pending and owner._source is source and _counts(case) == (0, 0)


@pytest.mark.parametrize("committed", (False, True))
def test_terminal_allows_only_the_committed_original_custody_acknowledgement(
    tmp_path, monkeypatch, committed
):
    with _original(tmp_path, monkeypatch) as (case, source, owner, selections):
        with _lose_reply(monkeypatch, committed):
            with pytest.raises(OSError):
                owner.advance()
        operation = _terminal(case)
        if committed:
            assert owner.advance() is True and not owner.has_pending
        else:
            with pytest.raises(Conflict, match="admission is closed"):
                owner.advance()
            assert owner.has_pending
        assert _counts(case) == ((1, 3) if committed else (0, 0))
        assert owner.drain_terminal(operation) == "closed"
        assert owner._source is source
        with pytest.raises(Conflict, match="stopped for its terminal"):
            owner.advance()


def _publish(case, source):
    construction = json.loads(source.construction_json)
    with session_scope(case.factory) as session:
        session.add(
            CollectionRecord(
                id=1,
                creation_idempotency_key=construction["idempotency_key"],
                creation_custody_mode=construction["custody_mode"],
                creation_identity_sha256=construction["creation_identity"],
                creation_archive_store=construction["archive_store"],
                archive_generation=construction["archive_generation"],
                delivery_context_id=construction["delivery_context_id"],
                artifact_set_identity="b" * 64,
                inventory_identity="c" * 64,
                provenance_identity="d" * 64,
                archive_root_sha256="e" * 64,
                encryption_format=construction["encryption_format"],
                passphrase_id=construction["passphrase_id"],
                created_at="2026-01-01T00:00:00Z",
                is_published=True,
            )
        )
        session.flush()
        original = session.get(Artifact, (1, case.member_id))
        session.add(
            CollectionArtifactRecord(
                collection_id=1,
                artifact_id=case.member_id,
                bytes=original.bytes,
                sha256=original.sha256,
                custody_receipt_json=original.custody_receipt_json,
            )
        )
        session.delete(session.get(Upload, 1))


@pytest.mark.parametrize(
    "change", ("none", "missing-parent", "unpublished", "generation", "receipt", "root")
)
def test_retired_parent_needs_the_exact_published_custody_receipt(tmp_path, monkeypatch, change):
    with _original(tmp_path, monkeypatch) as (case, source, owner, selections):
        with _lose_reply(monkeypatch, True):
            with pytest.raises(OSError):
                owner.advance()
        if change == "missing-parent":
            with session_scope(case.factory) as session:
                session.delete(session.get(Upload, 1))
        else:
            _publish(case, source)
            with session_scope(case.factory) as session:
                collection = session.get(CollectionRecord, 1)
                if change == "unpublished":
                    collection.is_published = False
                elif change == "generation":
                    collection.archive_generation = "f" * 64
                elif change == "root":
                    collection.archive_root_sha256 = None
                elif change == "receipt":
                    session.get(
                        CollectionArtifactRecord, (1, case.member_id)
                    ).custody_receipt_json = None
        owner._select_source = _forbidden
        if change == "none":
            assert owner.advance() is True and not owner.has_pending
        else:
            with pytest.raises(Conflict, match="publication is unavailable"):
                owner.advance()
            assert owner.has_pending and owner._source is source


def test_custody_receipt_terminal_poll_stays_responsive_while_committing(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch) as (case, source, owner, selections):
        entered, release = threading.Event(), threading.Event()
        actual = Source.apply

        def held(current, session):
            entered.set()
            assert release.wait(10)
            return actual(current, session)

        monkeypatch.setattr(Source, "apply", held)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                active = pool.submit(owner.advance)
                assert entered.wait(5) and owner.has_pending
                assert owner.drain_terminal("not-selected") == "busy"
                release.set()
                assert active.result(timeout=5) is True
        finally:
            release.set()
        assert not owner.has_pending and _counts(case) == (1, 3)


def test_foreign_custody_source_cannot_be_applied_on_a_later_retry(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch) as (case, source, owner, selections):
        owner._select_source = lambda: replace(source, collection_id=2)
        for _ in range(2):
            with pytest.raises(Conflict, match="another construction"):
                owner.advance()
        assert owner.has_pending and _counts(case) == (0, 0)


def test_registry_keeps_original_custody_feedback_and_does_not_repeat_the_receipt(
    tmp_path, monkeypatch
):
    with _original(tmp_path, monkeypatch) as (case, source, owner, selections):
        service = case.service
        received = []

        def feedback(progressed):
            received.append(progressed)
            if len(received) == 1:
                raise OSError("synthetic custody workflow acknowledgement")

        def completed():
            (result,) = _receive(service._archive_finalizers, service._archive_execution)
            return result.result()

        assert (
            service._archive_finalizer_work._submit(
                1,
                "custody-receipt",
                lambda: owner,
                lambda current: current.advance(),
                feedback,
            )
            == "submitted"
        )
        with pytest.raises(OSError, match="workflow acknowledgement"):
            completed()
        context = service._archive_finalizers._contexts[1]
        retained = context.owner
        assert retained._original is owner and retained.has_pending and not owner.has_pending
        owner._select_source = _forbidden
        assert (
            service._archive_finalizer_work._submit(
                1,
                "another-kind",
                _forbidden,
                _forbidden,
                _forbidden,
            )
            == "submitted"
        )
        turn = completed()
        assert turn.selection == "custody-receipt" and turn.progressed
        assert context.owner is retained and not retained.has_pending
        assert received == [True, True] and _counts(case) == (1, 3)
        assert service._archive_finalizers.retire_idle(
            context.key, target_incarnation=context.target
        )


def test_custody_receipt_source_is_charged_before_acknowledgement(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch) as (case, source, owner, selections):
        with session_scope(case.factory) as session:
            upload = session.get(Upload, 1)
            artifact = session.get(Artifact, (1, case.member_id))
            binding = session.get(Binding, (1, case.member_id))
            grants = replace(
                case.config.archive_write,
                metadata_preparation_bytes=source.reserved_source_bytes - 1,
            )
            with pytest.raises(Conflict, match="preparation grant"):
                Source.capture(
                    session,
                    upload,
                    artifact,
                    binding,
                    receipt=source._document(),
                    completion_output_id=source.completion_output_id,
                    grants=grants,
                )
        assert _counts(case) == (0, 0)


def test_an_empty_custody_selection_is_a_cached_turn_not_global_completion(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch) as (case, source, owner, selections):
        calls = []

        def empty():
            calls.append(1)
            return None

        owner._select_source = empty
        assert owner.advance() is False and not owner.has_pending
        owner._select_source = _forbidden
        assert owner.advance() is False and calls == [1] and _counts(case) == (0, 0)

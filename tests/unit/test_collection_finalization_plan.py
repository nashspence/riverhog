from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import replace

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store import incarnation as filesystem_incarnation
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionRecord
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_catalog_finalizer import ArchiveCatalogFinalizer
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterArchiveObjectRangeStore,
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol.errors import Conflict
from sqlalchemy import event

from tests.unit.finalization_claim_fixtures import _different_claim, _replace_claim
from tests.unit.test_archive_catalog_finalizer import _lose_commit_reply
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _bind_primary
from tests.unit.test_finalization_claim_readiness import _seal


@contextmanager
def _metadata_case(tmp_path, monkeypatch, *, kind="segment", mode="direct", count=2):
    # Payloads are already sealed by the ingress fixture; this exercises the real
    # metadata pipeline through explicit filesystem execution and observation.
    with _case(tmp_path, kind, artifact_count=count) as case:
        for index in range(count):
            _bind_primary(case.service, case.collection_id, format(index + 1, "064x"), case.content)
        _seal(case)
        claim = case.service._claim_due_finalization()
        assert claim is not None
        binding = case.service._archive_stores.require("archive")
        root = tmp_path / "metadata-store"
        root.mkdir()
        with monkeypatch.context() as patch:
            patch.setattr(
                filesystem_incarnation, "uuid4", lambda: uuid.UUID(binding.incarnation_id)
            )
            assert filesystem_incarnation.provision_storage_root(root) == binding.incarnation_id
        with (
            FilesystemStorageAdapter(
                FilesystemStorageAdapterConfig(root=root, minimum_free_bytes=0)
            ) as adapter,
            _executor(adapter, mode) as transport,
        ):
            physical = StorageAdapterImmutableArchiveObjectStore(transport)
            calls = []

            class Ports:
                def observe_immutable_object(self, request):
                    calls.append(("observe", request))
                    return physical.observe_immutable_object(request)

                def execute_write(self, invocation, *, content):
                    calls.append(("execute", invocation))
                    return physical.execute_write(invocation, content=content)

            case.calls = calls
            case.physical = physical
            case.service._archive_stores = ArchiveStoreRegistry(
                {
                    "archive": replace(
                        binding,
                        immutable_objects=physical,
                        immutable_writes=Ports(),
                        object_ranges=StorageAdapterArchiveObjectRangeStore(transport),
                    )
                }
            )
            yield case, claim


def _until(case, claim, ready):
    for _ in range(256):
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            if ready(upload):
                return
        owner = case.service._finalization_owner(claim)
        assert owner.advance() is True and not owner.has_pending
    pytest.fail("the fixed metadata pipeline did not reach the selected state")


@pytest.mark.parametrize(("kind", "mode"), (("segment", "direct"), ("pack", "http")))
def test_fixed_application_plan_publishes_from_sealed_payloads_with_one_exact_claim(
    tmp_path, monkeypatch, kind, mode
):
    with _metadata_case(tmp_path, monkeypatch, kind=kind, mode=mode) as (case, claim):
        _until(case, claim, lambda upload: upload is None)
        with session_scope(case.factory) as session:
            collection = session.get(CollectionRecord, case.collection_id)
            assert collection.is_published and collection.artifact_count == 2
            assert collection.archive_root_sha256 and collection.provenance_identity
            assert collection.description_identity and collection.tag_head_identity
        assert _count(case) == 0
        assert case.calls and sum(kind == "execute" for kind, _ in case.calls) > 2


def test_application_plan_construction_does_not_read_catalog_or_construct_children(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch) as (case, claim):
        statements = []
        engine = case.factory.kw["bind"]

        def observed(*args):
            statements.append(args[2])

        def forbidden(*args, **kwargs):
            pytest.fail("construction selected semantic work")

        monkeypatch.setattr(case.service, "_catalog_finalization_owner", forbidden)
        monkeypatch.setattr(case.service, "_artifact_custody_owner", forbidden)
        monkeypatch.setattr(case.service, "_select_finalization_custody", forbidden)
        event.listen(engine, "before_cursor_execute", observed)
        try:
            owner = case.service._finalization_owner(claim)
            assert not owner.has_pending and owner._original is None
            assert statements == [] and case.calls == []
        finally:
            event.remove(engine, "before_cursor_execute", observed)


@pytest.mark.parametrize("committed", (False, True))
def test_composed_pipeline_retains_exact_terminal_publication_after_commit_reply_loss(
    tmp_path, monkeypatch, committed
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        _until(case, claim, lambda upload: upload.catalog_phase == "terminal")
        owner = case.service._finalization_owner(claim)
        advance = ArchiveCatalogFinalizer.advance

        def lost(original):
            if original._kind != "catalog-projection":
                return advance(original)
            with _lose_commit_reply(monkeypatch, committed=committed):
                return advance(original)

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveCatalogFinalizer, "advance", lost)
            with pytest.raises(OSError, match="catalog commit acknowledgement"):
                owner.advance()
        original = owner._original
        assert owner.has_pending and original.has_pending
        calls = list(case.calls)
        with session_scope(case.factory) as session:
            assert (session.get(Upload, case.collection_id) is None) is committed
        assert owner.advance() is True and not owner.has_pending
        assert owner._original is original and case.calls == calls
        with session_scope(case.factory) as session:
            assert session.get(Upload, case.collection_id) is None
            assert session.get(CollectionRecord, case.collection_id).is_published
        assert owner.advance() is True and case.calls == calls


@pytest.mark.parametrize("committed", (False, True))
def test_composed_pipeline_receives_original_metadata_without_replacing_its_claim(
    tmp_path, monkeypatch, committed
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        _until(case, claim, lambda upload: upload.archive_tree_sha256 is not None)
        owner = case.service._finalization_owner(claim)
        with _lose_ack(monkeypatch, "report", committed):
            with pytest.raises(OSError):
                owner.advance()
        original = owner._original
        assert owner.has_pending and original.has_pending
        calls = list(case.calls)
        _replace_claim(case, _different_claim(claim))
        assert owner.advance() is True and not owner.has_pending
        assert owner._original is original and case.calls == calls and _count(case) == 0
        with pytest.raises(Conflict, match="claim is unavailable"):
            case.service._finalization_owner(claim).advance()
        assert case.calls == calls


@pytest.mark.parametrize("change", ("receipt", "progress"))
def test_completed_provenance_volumes_require_the_original_terminal_proof(
    tmp_path, monkeypatch, change
):
    from riverhog_core.catalog_provenance_publication_models import (
        CollectionUploadProvenanceAuthorityPublicationRecord as Authority,
    )

    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        _until(
            case,
            claim,
            lambda upload: upload.provenance_archive_terminal_receipt_json is not None,
        )
        calls = list(case.calls)
        with session_scope(case.factory) as session:
            row = session.get(Authority, (case.collection_id, "terminal"))
            if change == "receipt":
                row.receipt_json = None
            else:
                row.progress_receipt_json = None
        with pytest.raises(Conflict):
            case.service._finalization_owner(claim).advance()
        assert case.calls == calls

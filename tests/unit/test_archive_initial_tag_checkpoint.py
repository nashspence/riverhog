from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_age import decrypt_age_scrypt
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_initial_metadata_models import (
    CollectionUploadInitialTagPublicationRecord as Tag,
)
from riverhog_core.catalog_models import (
    CollectionTagNodeRecord as Node,
)
from riverhog_core.catalog_models import (
    CollectionUploadRecord as Upload,
)
from riverhog_core.catalog_models import (
    CollectionUploadTagPublicationFrontierRecord as Frontier,
)
from riverhog_core.services.archive_initial_tag_checkpoint import ArchiveInitialTagSource as Source
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_protocol import (
    CollectionTagHeadDocument,
    CollectionTagSet,
    CollectionTagSetRoot,
    collection_tag_node_path,
    collection_tag_set_identity,
    decode_collection_tag_node,
)
from riverhog_protocol.errors import Conflict
from riverhog_storage_adapter_protocol import ObjectLocator, ObjectReadRequest
from sqlalchemy import event, select
from sqlalchemy.exc import IntegrityError

from tests.unit.test_archive_authority_checkpoint import _authority_source
from tests.unit.test_archive_authority_finalizer import _owner as _authority_owner
from tests.unit.test_archive_initial_description_checkpoint import (
    _admit,
    _commit,
    _orphan,
    _publish,
    _received,
)
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_collection_uploads import _TAGGED_CREATOR

_TAGS = ("camera/first", "camera/second", "location/é")


def _select(f, source):
    f.source = source
    f.actor = (
        None
        if not source.has_publication
        else ImmutableArchiveWrites(
            f.case.factory,
            source=source,
            epoch=f.epoch,
            grants=source.write_grants(f.grants),
            executor=f.ports,
            observer=f.ports,
        )
    )


def _tag_stage(f):
    with session_scope(f.case.factory) as session:
        return Source.stage_next(session, f.case.collection_id, grants=f.grants)


@contextmanager
def _tag_source(
    tmp_path, monkeypatch, role="state", mode="direct", *, tags=_TAGS, database_url=None
):
    def configure(service):
        create = service.create_or_resume

        def tagged(**kwargs):
            kwargs["tags"] = tags
            kwargs["initiator"] = replace(kwargs["initiator"], access=_TAGGED_CREATOR.access)
            return create(**kwargs)

        service.create_or_resume = tagged

    with _authority_source(
        tmp_path, monkeypatch, mode, database_url=database_url, configure_service=configure
    ) as fixture:
        prior = _authority_owner(fixture)
        assert prior.owner.advance() and prior.owner.advance()
        f = SimpleNamespace(
            case=fixture.case,
            fixture=fixture,
            owner=ImmutableArchiveAdmission(fixture.case.factory),
            epoch=fixture.epoch,
            grants=fixture.grants,
            ports=prior.ports,
            calls=prior.calls,
            prior=prior,
            physical=fixture.physical,
            tags=tags,
            before_calls={k: len(v) for k, v in prior.calls.items()},
        )
        with session_scope(f.case.factory) as session:
            count = len(list(session.scalars(select(Node.digest))))
        # The test derives its completion bound from the finite fixture tree.
        for _ in range(2 * count + 2):
            source = _tag_stage(f)
            assert source is not None
            _select(f, source)
            if source.role == role:
                f.before_calls = {k: len(v) for k, v in prior.calls.items()}
                yield f
                return
            if source.has_publication:
                _publish(f)
            assert _commit(f)
        raise AssertionError("fixture did not reach requested tag role")


def _snapshot(f):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        return (
            tuple(
                getattr(upload, k)
                for k in (
                    "tag_revision",
                    "tag_root_sha256",
                    "tag_set_identity",
                    "tag_head_identity",
                    "tag_publication_receipt_json",
                )
            ),
            tuple(
                (
                    row.node_digest,
                    row.expanded,
                    row.published,
                    row.object_path,
                    row.provider_revision,
                    row.stored_bytes,
                    row.stored_sha256,
                    row.published_at,
                )
                for row in session.scalars(
                    select(Frontier)
                    .where(Frontier.collection_id == f.case.collection_id)
                    .order_by(Frontier.node_digest)
                )
            ),
            tuple(
                (row.object_id, row.progress_receipt_json)
                for row in session.scalars(
                    select(Tag)
                    .where(Tag.collection_id == f.case.collection_id)
                    .order_by(Tag.object_id)
                )
            ),
        )


def _complete_step(f):
    before = _snapshot(f)
    if f.source.has_publication:
        assert _publish(f) == _received(f)
        assert _snapshot(f) == before
    else:
        assert _received(f) is None
        with pytest.raises(Conflict, match="no provider write"):
            _ = f.source.publication
    assert _commit(f)
    after = _snapshot(f)
    assert after != before and not _commit(f) and _snapshot(f) == after
    with session_scope(f.case.factory) as session:
        f.source.verify_progress(session)
    assert _count(f.case) == 0


@pytest.mark.parametrize("role", ("state", "expand", "node", "head"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_tag_receiving_is_separate_from_once_only_progress(
    tmp_path, monkeypatch, role, mode
):
    with _tag_source(tmp_path, monkeypatch, role, mode) as f:
        _complete_step(f)


def _cold_receive(f):
    admitted, response = _admit(f)
    f.owner.report(admitted, response, apply_receipt=lambda *args: None)
    assert _received(f) is None and _count(f.case) == 1
    _orphan(f)
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        upload.tag_staging_root_sha256 = None
        upload.tag_staging_set_identity = collection_tag_set_identity(None)
        upload.final_authority_json = None
    before = _snapshot(f)
    assert drain_original_immutable_report(f.case.factory, f.case.collection_id)
    assert _received(f).stored_sha256 == response.result.stored_sha256
    assert _snapshot(f) == before and _count(f.case) == 0


@pytest.mark.parametrize("role", ("node", "head"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_cold_tag_receiving_uses_original_facts_after_selection_loss(
    tmp_path, monkeypatch, role, mode
):
    with _tag_source(tmp_path, monkeypatch, role, mode) as f:
        _cold_receive(f)


@pytest.mark.parametrize("role", ("node", "head"))
def test_unknown_tag_execution_retains_charge_and_refuses_progress(tmp_path, monkeypatch, role):
    with _tag_source(tmp_path, monkeypatch, role) as f:
        admitted, response = _admit(f)
        f.owner.report(
            admitted,
            response.model_copy(update={"execution": "unknown"}),
            apply_receipt=f.source.apply_prepared_receipt,
        )
        before = _snapshot(f)
        assert _received(f) is not None and _count(f.case) == 1
        for step in (_commit, _tag_stage):
            with pytest.raises(Conflict, match="unresolved provider writes"):
                step(f)
        assert _snapshot(f) == before


@pytest.mark.parametrize("role", ("state", "expand", "node", "head"))
@pytest.mark.parametrize("committed", (False, True))
def test_tag_progress_atomicity_and_original_ack_before_fresh_authority(
    tmp_path, monkeypatch, role, committed
):
    with _tag_source(tmp_path, monkeypatch, role) as f:
        if f.source.has_publication:
            _publish(f)
        before = _snapshot(f)
        with pytest.raises(OSError, match="tag progress"):
            with session_scope(f.case.factory) as session:
                assert f.source.commit_progress(session)
                if not committed:
                    raise OSError("injected tag progress rollback")
            raise OSError("injected tag progress reply loss")
        if committed:
            after = _snapshot(f)
            _orphan(f)
            with session_scope(f.case.factory) as session:
                upload = session.get(Upload, f.case.collection_id)
                upload.tag_staging_root_sha256 = None
                upload.tag_staging_set_identity = collection_tag_set_identity(None)
                upload.final_authority_json = None
            assert not _commit(f) and _snapshot(f) == after
        else:
            assert _snapshot(f) == before and _commit(f)


@pytest.mark.parametrize("role", ("state", "expand", "node", "head"))
@pytest.mark.parametrize("change", ("orphan", "gate", "selection", "authority"))
def test_fresh_tag_progress_rechecks_current_rights_and_selection(
    tmp_path, monkeypatch, role, change
):
    from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control

    with _tag_source(tmp_path, monkeypatch, role) as f:
        if f.source.has_publication:
            _publish(f)
        before = _snapshot(f)
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            if change == "orphan":
                upload.state = upload.archive_phase = "orphaned"
            elif change == "gate":
                session.get(Control, f.case.collection_id).gate = "closed"
            elif change == "selection":
                upload.tag_staging_root_sha256 = None
            else:
                upload.final_authority_json = None
        with pytest.raises(Conflict):
            _commit(f)
        assert _snapshot(f) == before


def test_owned_tag_children_remain_retained_before_expansion(tmp_path, monkeypatch):
    with _tag_source(tmp_path, monkeypatch, "expand") as f:
        children = decode_collection_tag_node(f.source.document_bytes).children
        assert len(children) > 1
        before = _snapshot(f)
        with pytest.raises(IntegrityError):
            with session_scope(f.case.factory) as session:
                session.delete(session.get(Node, children[-1].digest))
        assert _snapshot(f) == before
        assert _commit(f)


def _interrupted_expansion(f):
    children = decode_collection_tag_node(f.source.document_bytes).children
    assert len(children) > 1
    before = _snapshot(f)
    engine = f.case.factory.kw["bind"]

    def interrupt(connection, cursor, statement, parameters, context, executemany):
        if "SELECT collection_tag_nodes.digest" in statement and children[-1].digest in (
            parameters.values() if isinstance(parameters, dict) else parameters
        ):
            raise OSError("injected child lookup interruption")

    event.listen(engine, "before_cursor_execute", interrupt)
    try:
        with pytest.raises(OSError, match="child lookup interruption"):
            _commit(f)
    finally:
        event.remove(engine, "before_cursor_execute", interrupt)
    assert _snapshot(f) == before
    assert _commit(f) and not _commit(f)
    with session_scope(f.case.factory) as session:
        f.source.verify_progress(session)


def test_interrupted_expansion_rolls_back_every_child_and_retries_once(tmp_path, monkeypatch):
    with _tag_source(tmp_path, monkeypatch, "expand") as f:
        _interrupted_expansion(f)


@pytest.mark.parametrize("role", ("node", "head"))
def test_original_tag_document_and_prepared_identity_cannot_be_substituted(
    tmp_path, monkeypatch, role
):
    with _tag_source(tmp_path, monkeypatch, role) as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        with session_scope(f.case.factory) as session:
            row = session.get(Tag, (f.case.collection_id, f.source.object_id))
            row.document_bytes += b" "
        with pytest.raises(Conflict, match="original document changed"):
            drain_original_immutable_report(f.case.factory, f.case.collection_id)
        assert _count(f.case) == 1


def test_tag_source_and_ciphertext_share_operator_grant(tmp_path, monkeypatch):
    with _tag_source(tmp_path, monkeypatch, "node") as f:
        with pytest.raises(Conflict, match="source exceeds"):
            f.source.write_grants(
                replace(f.grants, metadata_preparation_bytes=f.source.reserved_source_bytes)
            )
        assert _count(f.case) == 0


def _recover(f, *, passphrase="fixture"):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        prefix = upload.archive_storage_prefix
        head_receipt = json.loads(upload.tag_publication_receipt_json)
        nodes = {}
        for row in session.scalars(
            select(Tag).where(Tag.collection_id == f.case.collection_id, Tag.role == "node")
        ):
            nodes[json.loads(row.source_json)["node_digest"]] = json.loads(row.receipt_json)
        count = len(
            list(
                session.scalars(
                    select(Frontier.node_digest).where(
                        Frontier.collection_id == f.case.collection_id
                    )
                )
            )
        )

    def read(receipt):
        revision = receipt.get("revision", receipt.get("provider_revision"))
        content = b"".join(
            f.physical._adapter.read_object(
                ObjectReadRequest(
                    object=ObjectLocator(object_path=receipt["object_path"], revision=revision),
                    expected_bytes=receipt["stored_bytes"],
                    offset=0,
                    size=receipt["stored_bytes"],
                )
            ).content
        )
        assert hashlib.sha256(content).hexdigest() == receipt["stored_sha256"]
        return decrypt_age_scrypt(content, passphrase)

    head = CollectionTagHeadDocument.from_json_bytes(read(head_receipt))

    class ReadOnlyNodes:
        def get(self, digest):
            receipt = nodes[digest]
            assert receipt["object_path"] == prefix + "/" + collection_tag_node_path(digest)
            return read(receipt)

        def put(self, digest, encoded):
            raise AssertionError("recovery cannot write")

    recovered = CollectionTagSet(ReadOnlyNodes(), CollectionTagSetRoot.seal(head.root_sha256))
    assert set(recovered.iter_tags()) == set(f.tags) and recovered.identity == head.tag_set_identity
    assert len(nodes) == count
    return count


def _complete_tree(f):
    with session_scope(f.case.factory) as session:
        nodes = len(list(session.scalars(select(Node.digest))))
    steps = []
    for _ in range(2 * nodes + 2):
        source = _tag_stage(f)
        if source is None:
            break
        _select(f, source)
        steps.append(source.role)
        _complete_step(f)
    else:
        assert _tag_stage(f) is None
    count = _recover(f)
    assert steps.count("state") == 1 and steps.count("head") == 1
    assert steps.count("expand") == steps.count("node") == count


@pytest.mark.parametrize("tags", ((), _TAGS))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_complete_initial_tags_recover_from_actual_encrypted_objects(
    tmp_path, monkeypatch, tags, mode
):
    with _tag_source(tmp_path, monkeypatch, mode=mode, tags=tags) as f:
        _complete_tree(f)


def test_original_tag_receiving_does_not_depend_on_current_node_cache(tmp_path, monkeypatch):
    with _tag_source(tmp_path, monkeypatch, "node") as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        digest = json.loads(f.source.source_json)["node_digest"]
        with session_scope(f.case.factory) as session:
            session.get(Node, digest).encoded = b"unusable current cache bytes"
        _orphan(f)
        before = _snapshot(f)
        assert drain_original_immutable_report(f.case.factory, f.case.collection_id)
        assert _received(f).stored_sha256 == response.result.stored_sha256
        assert _snapshot(f) == before and _count(f.case) == 0


@pytest.mark.parametrize("work", ("expand", "publish"))
def test_head_progress_cannot_skip_pending_frontier_work(tmp_path, monkeypatch, work):
    with _tag_source(tmp_path, monkeypatch, "head") as f:
        _publish(f)
        with session_scope(f.case.factory) as session:
            row = session.scalar(
                select(Frontier).where(Frontier.collection_id == f.case.collection_id)
            )
            row.published = False
            row.object_path = row.provider_revision = row.stored_bytes = None
            row.stored_sha256 = row.published_at = None
            row.expanded = work != "expand"
        before = _snapshot(f)
        with pytest.raises(Conflict, match="not completely published"):
            _commit(f)
        assert _snapshot(f) == before


@pytest.mark.parametrize("role", ("state", "expand", "node", "head"))
def test_unmarked_tag_projection_is_not_a_progress_acknowledgement(tmp_path, monkeypatch, role):
    with _tag_source(tmp_path, monkeypatch, role) as f:
        _complete_step(f)
        with session_scope(f.case.factory) as session:
            session.get(
                Tag, (f.case.collection_id, f.source.object_id)
            ).progress_receipt_json = None
        before = _snapshot(f)
        with pytest.raises(Conflict):
            _commit(f)
        assert _snapshot(f) == before

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.app_permissions import COLLECTION_TAGS_MANAGE, ApplicationAccess
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveUploadTerminalTagRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionTagNodeReclamationRecord,
    CollectionTagNodeRecord,
    CollectionUploadRecord,
    CollectionUploadTagNodeReferenceRecord,
    CollectionUploadTagRecord,
)
from riverhog_core.services.archive_upload_drain import lock_terminal_selection
from riverhog_core.services.archive_upload_terminal_tags import load_terminal_tag_selector
from riverhog_core.services.catalog_sync import (
    _reap_unreferenced_tag_history,
    _tag_node_has_live_owner,
)
from riverhog_core.services.collection_tags import read_collection_tag_set
from riverhog_protocol import CollectionTagSetRoot
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import delete, event, func, select

from tests.unit.test_archive_write_admission import GRANTS, _case, _guard
from tests.unit.test_collection_uploads import _CREATOR


@contextmanager
def _tagged(tmp_path, *, count=8, database_url=None):
    with _case(tmp_path, database_url=database_url) as case:
        principal = replace(
            _CREATOR,
            access=_CREATOR.access | {ApplicationAccess(COLLECTION_TAGS_MANAGE)},
        )
        tags = tuple(f"selected/{index:04d}/" + "x" * 80 for index in range(count))
        for start in range(0, len(tags), 32):
            case.service.add_tags(case.collection_id, tags[start : start + 32], principal=principal)
        case.tags = tags
        yield case


def _select(case, *, grants=GRANTS, validate_selection=None):
    return case.owner.select_terminal(
        case.collection_id,
        kind="cancel",
        request={"selected": "original-tag-scope"},
        principal_id=_CREATOR.id,
        grants=grants,
        guard=_guard,
        validate_selection=validate_selection or (lambda session, upload: None),
    )


def _read(case, operation):
    with session_scope(case.factory) as session:
        selected = load_terminal_tag_selector(session, operation)
        tree = read_collection_tag_set(session, root_sha256=selected.root_sha256)
        assert tree.identity == selected.tag_set_identity
        return selected, set(tree.iter_tags())


def _control(case):
    with session_scope(case.factory) as session:
        row = session.get(ArchiveWriteControlRecord, case.collection_id)
        return row.gate, row.epoch, row.terminal_id


def _teardown_upload(case):
    with session_scope(case.factory) as session:
        session.delete(session.get(CollectionUploadRecord, case.collection_id))


def _reap(case):
    for _ in range(5000):
        with session_scope(case.factory) as session:
            metrics = _reap_unreferenced_tag_history(
                session,
                limit=4,
                cleanup_before="2999-01-01T00:00:00Z",
                cleanup_started_at="2999-01-02T00:00:00Z",
            )
            assert metrics.changed_rows <= 4
        if metrics.changed_rows == 0:
            return
    pytest.fail("bounded tag reclamation did not exhaust the fixture")


def test_original_selector_and_descendants_survive_upload_teardown_and_actual_reclamation(tmp_path):
    with _tagged(tmp_path, count=128) as case:
        operation = _select(case)
        original, tags = _read(case, operation)
        assert tags == set(case.tags)
        _teardown_upload(case)
        _reap(case)
        assert _read(case, operation) == (original, tags)
        with session_scope(case.factory) as session:
            assert session.get(CollectionUploadRecord, case.collection_id) is None
            assert (
                session.scalar(
                    select(func.count()).select_from(CollectionUploadTagNodeReferenceRecord)
                )
                == 0
            )
            assert _tag_node_has_live_owner(session, original.root_sha256)
            assert session.get(ArchiveUploadTerminalRecord, operation) is not None
            session.delete(session.get(ArchiveUploadTerminalTagRecord, operation))
        _reap(case)
        with session_scope(case.factory) as session:
            assert session.get(CollectionTagNodeRecord, original.root_sha256) is None


def test_reclamation_rechecks_terminal_root_ownership_before_dismantling_its_edges(tmp_path):
    with _tagged(tmp_path, count=32) as case:
        operation = _select(case)
        original, tags = _read(case, operation)
        _teardown_upload(case)
        with session_scope(case.factory) as session:
            session.add(
                CollectionTagNodeReclamationRecord(
                    node_digest=original.root_sha256,
                    claimed_at="2000-01-01T00:00:00Z",
                )
            )
        _reap(case)
        assert _read(case, operation) == (original, tags)
        with session_scope(case.factory) as session:
            assert session.get(CollectionTagNodeReclamationRecord, original.root_sha256) is None


def test_exact_retry_keeps_original_selector_after_partial_tag_cleanup_and_lowered_grant(tmp_path):
    with _tagged(tmp_path) as case:
        operation = _select(case)
        original = _read(case, operation)
        before = _control(case)
        with session_scope(case.factory) as session:
            session.execute(
                delete(CollectionUploadTagRecord).where(
                    CollectionUploadTagRecord.collection_id == case.collection_id
                )
            )
            upload = session.get(CollectionUploadRecord, case.collection_id)
            upload.tag_staging_root_sha256 = None
            upload.tag_staging_set_identity = CollectionTagSetRoot.seal(None).tag_set_identity

        def forbidden(*args):
            pytest.fail("exact selected retry attempted fresh eligibility validation")

        assert (
            _select(
                case,
                grants=replace(GRANTS, terminal_bytes=1),
                validate_selection=forbidden,
            )
            == operation
        )
        assert _control(case) == before and _read(case, operation) == original


@pytest.mark.parametrize("empty", (False, True))
def test_missing_retained_selector_is_unknown_even_when_the_original_set_was_empty(tmp_path, empty):
    with _tagged(tmp_path, count=0 if empty else 8) as case:
        operation = _select(case)
        selected, tags = _read(case, operation)
        assert bool(tags) is not empty
        with session_scope(case.factory) as session:
            session.delete(session.get(ArchiveUploadTerminalTagRecord, operation))
        _teardown_upload(case)
        with pytest.raises(NotFound, match="original tag selector is unavailable"):
            _select(case)
        with session_scope(case.factory) as session:
            with pytest.raises(NotFound, match="original tag selector is unavailable"):
                load_terminal_tag_selector(session, operation)
        with session_scope(case.factory) as session:
            with pytest.raises(NotFound, match="original tag selector is unavailable"):
                lock_terminal_selection(
                    session, case.collection_id, operation, require_upload=False
                )


def test_failed_original_root_retention_rolls_back_terminal_and_gate_closure(tmp_path):
    with _tagged(tmp_path) as case:
        before = _control(case)
        with session_scope(case.factory) as session:
            session.execute(
                delete(CollectionUploadTagNodeReferenceRecord).where(
                    CollectionUploadTagNodeReferenceRecord.collection_id == case.collection_id
                )
            )
        with pytest.raises(NotFound, match="original tag root is unavailable"):
            _select(case)
        assert _control(case) == before
        with session_scope(case.factory) as session:
            assert (
                session.scalar(select(func.count()).select_from(ArchiveUploadTerminalRecord)) == 0
            )
            assert (
                session.scalar(select(func.count()).select_from(ArchiveUploadTerminalTagRecord))
                == 0
            )


def test_inconsistent_original_selector_identity_fails_closed(tmp_path):
    with _tagged(tmp_path) as case:
        before = _control(case)
        with session_scope(case.factory) as session:
            session.get(CollectionUploadRecord, case.collection_id).tag_staging_set_identity = (
                "f" * 64
            )
        with pytest.raises(Conflict, match="tag selector identity changed"):
            _select(case)
        assert _control(case) == before


@pytest.mark.parametrize("count", (8, 32, 128))
def test_selector_admission_pins_one_root_without_reading_tag_members_or_nodes(tmp_path, count):
    with _tagged(tmp_path, count=count) as case:
        reads = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            if statement.lstrip().upper().startswith("SELECT"):
                reads.append(statement)
                for table in (
                    "collection_tag_nodes",
                    "collection_tag_node_edges",
                    "collection_upload_tags",
                ):
                    assert "FROM " + table not in statement

        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            operation = _select(case)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert len(reads) <= 12
        with session_scope(case.factory) as session:
            assert (
                session.scalar(select(func.count()).select_from(ArchiveUploadTerminalTagRecord))
                == 1
            )
        assert _read(case, operation)[1] == set(case.tags)


def test_terminal_observation_refuses_a_changed_original_selector_before_local_probes(
    tmp_path, monkeypatch
):
    with _tagged(tmp_path) as case:
        operation = _select(case)
        before = _control(case)
        with session_scope(case.factory) as session:
            session.get(ArchiveUploadTerminalTagRecord, operation).tag_set_identity = "f" * 64

        def forbidden(*args, **kwargs):
            pytest.fail("changed original selector reached local resource probes")

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", forbidden)
        with session_scope(case.factory) as session:
            with pytest.raises(Conflict, match="tag selector identity changed"):
                case.service._archive_terminal_observation.observe(
                    session, case.collection_id, operation
                )
        assert _control(case) == before

from __future__ import annotations

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import AppKeyRecord
from riverhog_protocol.errors import NotFound
from sqlalchemy import event

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_terminal_tags import _reap, _tagged, _teardown_upload
from tests.unit.test_upload_terminal_read_authority import (
    _creator_key,
    _scoped,
    _select_creator,
    _shrink_live_tags,
)

pytestmark = pytest.mark.integration


def test_postgres_original_read_scope_survives_partial_cleanup_and_actual_teardown(
    database_url, tmp_path
):
    with _tagged(tmp_path, count=8, database_url=database_url) as case:
        _, principal, _ = _creator_key(case)
        operation = _select_creator(case, principal)
        _shrink_live_tags(case)
        with pytest.raises(NotFound, match="collection creation"):
            case.service.require_terminal_read_access(
                case.collection_id, operation, _scoped(case.tags[:1])
            )
        case.service.require_read_access(case.collection_id, principal)
        _teardown_upload(case)
        _reap(case)
        case.service.require_terminal_read_access(case.collection_id, operation, principal)


def test_postgres_original_outcome_read_is_read_only_and_rechecks_current_key(
    database_url, tmp_path
):
    with _tagged(tmp_path, count=3, database_url=database_url) as case:
        _, principal, _ = _creator_key(case)
        operation = _select_creator(case, principal)
        _teardown_upload(case)
        statements = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)
            assert not statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            assert "FOR UPDATE" not in statement

        engine = case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", capture)
        try:
            case.service.require_terminal_read_access(case.collection_id, operation, principal)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert "SET TRANSACTION READ ONLY" in statements
        with session_scope(case.factory) as session:
            session.delete(session.get(AppKeyRecord, principal.key_id))
        with pytest.raises(NotFound, match="application key"):
            case.service.require_terminal_read_access(case.collection_id, operation, principal)

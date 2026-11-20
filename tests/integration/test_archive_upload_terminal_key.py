from __future__ import annotations

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import AppKeyRecord
from riverhog_protocol.errors import Conflict, NotFound

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_app_keys import BOOTSTRAP
from tests.unit.test_archive_upload_terminal_key import _context, _raw_retry
from tests.unit.test_archive_upload_terminal_tags import _teardown_upload
from tests.unit.test_archive_write_admission import _case
from tests.unit.test_upload_discard_authority import _operator, _select

pytestmark = pytest.mark.integration


def test_postgres_selecting_key_binds_exact_retry_and_survives_credential_rotation(
    database_url, tmp_path
):
    with _case(tmp_path, database_url=database_url) as case:
        keys, principal = _operator(case)
        _, other = _operator(case)
        operation = _select(
            case, principal, case.service._discard_write_guard(case.collection_id, principal)
        )
        original = _context(case, operation)
        assert original[0] == principal.key_id
        assert principal.id == other.id and principal.key_id != other.key_id
        with pytest.raises(Conflict, match="different archive upload terminal request"):
            _select(case, other, case.service._discard_write_guard(case.collection_id, other))
        keys.rotate(app=principal.id, key_id=principal.key_id, grantor=BOOTSTRAP)
        assert _raw_retry(case, principal) == operation
        assert _context(case, operation) == original


def test_postgres_selecting_key_context_outlives_actual_key_and_upload_deletion(
    database_url, tmp_path
):
    with _case(tmp_path, database_url=database_url) as case:
        _, principal = _operator(case)
        operation = _select(
            case, principal, case.service._discard_write_guard(case.collection_id, principal)
        )
        original = _context(case, operation)
        _teardown_upload(case)
        assert _raw_retry(case, principal) == operation
        with session_scope(case.factory) as session:
            session.delete(session.get(AppKeyRecord, principal.key_id))
        with pytest.raises(NotFound, match="application key"):
            _raw_retry(case, principal)
        assert _context(case, operation) == original

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from riverhog_core.catalog_db import create_catalog_engine, initialize_db
from sqlalchemy import text
from sqlalchemy.engine import make_url

from tests.unit import test_operation_lifecycle_api as lifecycle

pytestmark = pytest.mark.integration


@pytest.fixture
def database_url() -> Iterator[str]:
    value = os.environ.get("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    schema = "riverhog_http_lifecycle_" + uuid4().hex
    admin = create_catalog_engine(value)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    scoped = (
        make_url(value)
        .update_query_dict({"options": f"-csearch_path={schema},public"})
        .render_as_string(hide_password=False)
    )
    initialize_db(scoped)
    try:
        yield scoped
    finally:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def test_official_http_lifecycle_uses_real_postgresql_transactions(
    database_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    container = lifecycle._container
    monkeypatch.setattr(
        lifecycle, "_container", lambda root: container(root, database_url=database_url)
    )
    lifecycle.test_riverhog_official_client_positive_disposable_lifecycle(tmp_path)

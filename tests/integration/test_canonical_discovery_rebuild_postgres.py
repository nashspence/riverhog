from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from riverhog_core.catalog_db import create_catalog_engine
from sqlalchemy import text
from sqlalchemy.engine import make_url

from tests.support.qualification.canonical_history_scale import (
    measure_archive_rebuild,
    publish_shared_history,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def database_url() -> Iterator[str]:
    value = os.environ.get("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    schema = "riverhog_archive_rebuild_" + uuid4().hex
    admin = create_catalog_engine(value)
    with admin.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    scoped = (
        make_url(value)
        .update_query_dict({"options": f"-csearch_path={schema},public"})
        .render_as_string(hide_password=False)
    )
    try:
        yield scoped
    finally:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def test_postgresql_rebuilds_shared_member_history_from_encrypted_custody(
    database_url: str, tmp_path: Path
) -> None:
    fixture = publish_shared_history(tmp_path, members=2, database_url=database_url)
    try:
        measure_archive_rebuild(fixture)
        measure_archive_rebuild(fixture)
    finally:
        fixture.container.close()

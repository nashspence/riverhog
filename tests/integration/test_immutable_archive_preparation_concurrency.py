from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from riverhog_core.catalog_db import create_catalog_engine, initialize_db
from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation
from sqlalchemy import event, text
from sqlalchemy.engine import make_url

from tests.unit.test_collection_uploads import _CREATOR
from tests.unit.test_immutable_archive_preparation import (
    _assert_publisher,
    _metadata,
    _prepare,
    _replacement,
    _stored,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def database_url() -> Iterator[str]:
    value = os.environ.get("RIVERHOG_TEST_POSTGRES_URL", "").strip()
    if not value:
        pytest.skip("RIVERHOG_TEST_POSTGRES_URL is required")
    schema = "riverhog_immutable_preparation_" + uuid4().hex
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


def test_postgres_actual_encrypted_metadata_replays_original_prepared_ciphertext(
    database_url, tmp_path, monkeypatch
):
    _assert_publisher(tmp_path, monkeypatch, "direct", database_url=database_url)


def test_postgres_concurrent_randomized_preparations_retain_one_original(
    database_url, tmp_path, monkeypatch
):
    with _metadata(tmp_path, monkeypatch, database_url=database_url) as case:
        factory = case.actor.case.factory
        engine = factory.kw["bind"]
        execution = threading.local()
        entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
        pids = {}
        replacement_operation, replacement = _replacement(case)

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(execution, "lane", None) == "second"
                and "FROM collection_uploads" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["second"] = cursor.fetchone()[0]
                waiting.set()

        def validate(session, upload, operation):
            case.validate(session, upload, operation)
            if getattr(execution, "lane", None) == "first":
                pids["first"] = session.scalar(text("SELECT pg_backend_pid()"))
                entered.set()
                assert release.wait(timeout=10)

        def prepare(lane):
            execution.lane = lane
            owner = ImmutableArchivePreparation(factory)
            guard = case.actor.case.service._payload_write_guard(
                case.actor.case.collection_id, _CREATOR
            )
            return owner.prepare(
                case.actor.case.collection_id,
                operation=case.operation if lane == "first" else replacement_operation,
                content=case.content if lane == "first" else replacement,
                epoch=case.epoch,
                grants=case.grants,
                guard=guard,
                validate_metadata=validate,
            )

        def blocked():
            deadline = time.monotonic() + 10
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["first"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["second"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(prepare, "first")
                second = None
                try:
                    assert entered.wait(timeout=10)
                    second = pool.submit(prepare, "second")
                    assert waiting.wait(timeout=10)
                    assert blocked()
                    assert not second.done()
                finally:
                    release.set()
                original = first.result(timeout=10)
                assert second is not None
                assert second.result(timeout=10) == original
        finally:
            release.set()
            event.remove(engine, "before_cursor_execute", capture)
        assert original.content == case.content and original.content != replacement
        assert _stored(case)[1] == original.preparation_json
        assert _prepare(case, operation=replacement_operation, content=replacement) == original

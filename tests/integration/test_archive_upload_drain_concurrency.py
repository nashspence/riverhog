from __future__ import annotations

import pytest

from tests.integration.test_collection_upload_custody_concurrency import (
    database_url as database_url,
)
from tests.unit.test_archive_upload_drain import (
    _archive_transition,
    _cache_begin_transition,
    _cache_write_transition,
)
from tests.unit.test_archive_write_admission import _case
from tests.unit.test_cache_population_begin import _case as _cache_case
from tests.unit.test_cache_population_writes import _writing

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_archive_drain_facts_preserve_original_closure(database_url, tmp_path, unknown):
    with _case(tmp_path, database_url=database_url) as case:
        _archive_transition(case, unknown)


@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_cache_begin_drain_facts_are_independent(database_url, tmp_path, unknown):
    with _cache_case(tmp_path, database_url=database_url) as case:
        _cache_begin_transition(case, unknown)


@pytest.mark.parametrize("unknown", (False, True))
def test_postgres_cache_child_drain_facts_keep_original_receiving(database_url, tmp_path, unknown):
    with _writing(tmp_path, database_url=database_url) as writing:
        _cache_write_transition(writing, unknown)


def test_postgres_terminal_record_inspection_waits_for_parent_and_rechecks_selection(
    database_url, tmp_path
):
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_models import CollectionUploadRecord as Upload
    from riverhog_protocol.errors import Conflict
    from sqlalchemy import event, select, text

    from tests.unit.test_archive_upload_drain import _inspect
    from tests.unit.test_archive_write_admission import _selected

    with _case(tmp_path, database_url=database_url) as case:
        operation = _selected(case)
        engine = case.factory.kw["bind"]
        lane = threading.local()
        waiting = threading.Event()
        pids = {}

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "contender", False)
                and "FROM collection_uploads" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["contender"] = cursor.fetchone()[0]
                waiting.set()

        def inspect():
            lane.contender = True
            return _inspect(case, operation)

        def blocked():
            deadline = time.monotonic() + 10
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["holder"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["contender"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                with session_scope(case.factory) as session:
                    session.scalar(
                        select(Upload)
                        .where(Upload.collection_id == case.collection_id)
                        .with_for_update()
                    )
                    pids["holder"] = session.scalar(text("SELECT pg_backend_pid()"))
                    terminal = session.get(ArchiveUploadTerminalRecord, operation)
                    terminal.frontier = "999"
                    session.flush()
                    future = pool.submit(inspect)
                    assert waiting.wait(10) and blocked() and not future.done()
                with pytest.raises(Conflict, match="original selection changed"):
                    future.result(timeout=10)
        finally:
            event.remove(engine, "before_cursor_execute", capture)


@pytest.mark.parametrize("change", ("control", "construction"))
def test_postgres_terminal_inspection_refreshes_previously_loaded_owner_rows(
    database_url, tmp_path, change
):
    from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
    from riverhog_core.catalog_db import session_scope
    from riverhog_core.catalog_models import CollectionUploadRecord as Upload
    from riverhog_core.services.archive_upload_drain import inspect_terminal_write_records
    from riverhog_protocol.errors import Conflict

    from tests.unit.test_archive_write_admission import _selected

    with _case(tmp_path, database_url=database_url) as case:
        operation = _selected(case)
        with session_scope(case.factory) as stale:
            upload = stale.get(Upload, case.collection_id)
            control = stale.get(ArchiveWriteControlRecord, case.collection_id)
            assert upload is not None and control.next_call == "0"
            with session_scope(case.factory) as current:
                if change == "control":
                    current.get(ArchiveWriteControlRecord, case.collection_id).next_call = "1"
                else:
                    current.get(Upload, case.collection_id).archive_storage_prefix += "-changed"
            with pytest.raises(Conflict):
                inspect_terminal_write_records(stale, case.collection_id, operation)


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_postgres_terminal_custody_keeps_native_completion_before_logical_receipt(
    database_url, tmp_path, monkeypatch, kind, mode
):
    from tests.unit.test_archive_upload_drain import _native_seal_before_logical_receipt

    _native_seal_before_logical_receipt(
        tmp_path, monkeypatch, kind, mode, database_url=database_url
    )


@pytest.mark.parametrize("sealed", (False, True))
def test_postgres_terminal_sealed_payload_probe_is_indexed_across_unsealed_prefix(
    database_url, tmp_path, sealed, record_property
):
    from tests.unit.test_archive_upload_drain import _sealed_probe_growth

    _sealed_probe_growth(tmp_path, sealed, record_property, database_url=database_url)


@pytest.mark.parametrize("action", ("register", "complete", "tags", "heartbeat", "journal"))
def test_postgres_selected_terminal_closes_legacy_forward_control_mutations(
    database_url, tmp_path, action
):
    from tests.unit.test_archive_upload_drain import _forward_mutation_after_selected_terminal

    _forward_mutation_after_selected_terminal(tmp_path, action, database_url=database_url)


@pytest.mark.parametrize("processing", (False, True))
def test_postgres_terminal_current_key_contention_defers_before_parent_or_local_work(
    database_url, tmp_path, monkeypatch, processing
):
    from concurrent.futures import ThreadPoolExecutor

    from riverhog_core.catalog_db import catalog_write_contention, session_scope
    from riverhog_core.catalog_models import AppKeyRecord
    from sqlalchemy import select
    from sqlalchemy.exc import OperationalError

    from tests.unit.test_archive_upload_terminal_observation import (
        _current_key,
        _guarded_observation,
    )
    from tests.unit.test_archive_write_admission import _selected

    with _case(tmp_path, database_url=database_url) as case:
        operation = _selected(case)
        _, principal = _current_key(case, processing=processing)
        probes = []
        original = case.service._archive_finalizers.local_terminal_status

        def probe(*args, **kwargs):
            probes.append(True)
            return original(*args, **kwargs)

        monkeypatch.setattr(case.service._archive_finalizers, "local_terminal_status", probe)
        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as holder:
                holder.scalar(
                    select(AppKeyRecord)
                    .where(AppKeyRecord.id == principal.key_id)
                    .with_for_update()
                )
                attempt = workers.submit(_guarded_observation, case, operation, principal)
                with pytest.raises(OperationalError) as failed:
                    attempt.result(timeout=5)
                assert catalog_write_contention(failed.value)
                assert not probes
        assert _guarded_observation(case, operation, principal).records.operation_id == operation
        assert probes == [True]


@pytest.mark.parametrize("processing", (False, True))
def test_postgres_terminal_observation_retains_current_key_through_its_parent_transaction(
    database_url, tmp_path, processing
):
    from concurrent.futures import ThreadPoolExecutor

    from riverhog_core.catalog_db import catalog_write_contention, session_scope
    from riverhog_core.catalog_models import AppKeyRecord
    from riverhog_protocol.errors import NotFound
    from sqlalchemy import select
    from sqlalchemy.exc import OperationalError

    from tests.unit.test_archive_upload_terminal_observation import (
        _current_key,
        _guarded_observation,
        _key_guard,
    )
    from tests.unit.test_archive_write_admission import _selected

    with _case(tmp_path, database_url=database_url) as case:
        operation = _selected(case)
        keys, principal = _current_key(case, processing=processing)

        def acquire_for_revocation():
            with session_scope(case.factory) as session:
                return session.scalar(
                    select(AppKeyRecord.id)
                    .where(AppKeyRecord.id == principal.key_id)
                    .with_for_update(nowait=True)
                )

        with ThreadPoolExecutor(max_workers=1) as workers:
            with session_scope(case.factory) as session:
                observed = case.service._archive_terminal_observation.observe(
                    session, case.collection_id, operation, guard=_key_guard(principal)
                )
                assert observed.records.operation_id == operation
                attempt = workers.submit(acquire_for_revocation)
                with pytest.raises(OperationalError) as failed:
                    attempt.result(timeout=5)
                assert catalog_write_contention(failed.value)
        keys.revoke(app="terminal-observer", key_id=principal.key_id)
        with pytest.raises(NotFound, match="no longer active"):
            _guarded_observation(case, operation, principal)

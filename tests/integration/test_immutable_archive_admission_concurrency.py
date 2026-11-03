from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.storage_incarnations import reconcile_storage_incarnations
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from sqlalchemy import event, text

from tests.integration.test_immutable_archive_preparation_concurrency import (
    database_url as database_url,
)
from tests.unit.test_immutable_archive_admission import (
    _arm,
    _assert_actual_metadata_dispatch,
    _checkpoint,
    _effect,
    _select,
    _verify_checkpoint,
)
from tests.unit.test_immutable_archive_observation import _assert_observed_existing, _seed
from tests.unit.test_immutable_archive_preparation import _metadata, _prepare
from tests.unit.test_immutable_archive_receiving import (
    _assert_cold_receiving,
    _closed_source,
    _load,
)

pytestmark = pytest.mark.integration


def test_postgres_actual_metadata_dispatch_and_original_checkpoint(
    database_url, tmp_path, monkeypatch
):
    _assert_actual_metadata_dispatch(tmp_path, monkeypatch, "direct", database_url=database_url)


@pytest.mark.parametrize("contender", ("terminal", "configuration"))
def test_postgres_metadata_admission_holds_construction_and_binding_through_commit(
    database_url, tmp_path, monkeypatch, contender
):
    with _metadata(tmp_path, monkeypatch, database_url=database_url) as case:
        factory = case.actor.case.factory
        engine = factory.kw["bind"]
        owner = ImmutableArchiveAdmission(factory)
        prepared = _prepare(case)
        lane = threading.local()
        entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
        pids = {}

        def capture(connection, cursor, statement, parameters, context, executemany):
            target = "collection_uploads" if contender == "terminal" else "storage_incarnations"
            if (
                getattr(lane, "name", None) == "contender"
                and "FROM " + target in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["contender"] = cursor.fetchone()[0]
                waiting.set()

        def validate(session, upload, operation):
            case.validate(session, upload, operation)
            pids["admission"] = session.scalar(text("SELECT pg_backend_pid()"))
            entered.set()
            assert release.wait(timeout=10)

        def compete():
            lane.name = "contender"
            if contender == "terminal":
                return _select(case)
            return reconcile_storage_incarnations(factory, {})

        def blocked():
            deadline = time.monotonic() + 10
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["admission"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["contender"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(_arm, case, owner, prepared, validate_metadata=validate)
                second = None
                try:
                    assert entered.wait(timeout=10)
                    second = pool.submit(compete)
                    assert waiting.wait(timeout=10)
                    assert blocked() and not second.done()
                finally:
                    release.set()
                admitted = first.result(timeout=10)
                assert second is not None
                selected = second.result(timeout=10)
        finally:
            release.set()
            event.remove(engine, "before_cursor_execute", capture)
        assert _effect(case, admitted).state == "armed" and _effect(case, admitted).unresolved
        if contender == "terminal":
            with session_scope(factory) as session:
                terminal = session.get(ArchiveUploadTerminalRecord, selected)
                assert terminal.phase == "draining" and int(terminal.frontier) == 1
        proof = owner.withdraw(admitted)
        owner.settle_withdrawal(proof)
        owner.acknowledge_report(admitted)


def test_postgres_existing_ciphertext_retains_original_unused_dispatch_proof(
    database_url, tmp_path, monkeypatch
):
    _assert_observed_existing(tmp_path, monkeypatch, "direct", database_url=database_url)


def test_postgres_existing_receipt_and_unused_report_commit_before_terminal_adjudication(
    database_url, tmp_path, monkeypatch
):
    with _metadata(tmp_path, monkeypatch, database_url=database_url) as case:
        factory = case.actor.case.factory
        engine = factory.kw["bind"]
        owner = ImmutableArchiveAdmission(factory)
        physical = StorageAdapterImmutableArchiveObjectStore(case.actor.adapter)
        _seed(case, physical)
        admitted = _arm(case, owner)
        observed = physical.observe_immutable_object(admitted.prepared.operation.request)
        proof = owner.withdraw(admitted)
        lane = threading.local()
        entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
        pids = {}

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "name", None) == "terminal"
                and "FROM collection_uploads" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["terminal"] = cursor.fetchone()[0]
                waiting.set()

        def receive(session, prepared, receipt):
            pids["receipt"] = session.scalar(text("SELECT pg_backend_pid()"))
            _checkpoint(session, prepared, receipt)
            entered.set()
            assert release.wait(timeout=10)

        def terminal():
            lane.name = "terminal"
            return _select(case)

        def blocked():
            deadline = time.monotonic() + 10
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["receipt"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["terminal"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(
                    owner.accept_existing, admitted, proof, observed, apply_receipt=receive
                )
                second = None
                try:
                    assert entered.wait(timeout=10)
                    second = pool.submit(terminal)
                    assert waiting.wait(timeout=10) and blocked()
                    assert not second.done()
                finally:
                    release.set()
                first.result(timeout=10)
                assert second is not None
                selected = second.result(timeout=10)
        finally:
            release.set()
            event.remove(engine, "before_cursor_execute", capture)
        assert not _effect(case, admitted).unresolved
        with session_scope(factory) as session:
            assert session.get(ArchiveUploadTerminalRecord, selected).phase == "draining"
            _verify_checkpoint(session, admitted.prepared, observed)
        owner.acknowledge_report(admitted, verify_receipt=_verify_checkpoint)


def test_postgres_actual_cold_metadata_receiving_preserves_original_closed_receipt(
    database_url, tmp_path, monkeypatch
):
    _assert_cold_receiving(tmp_path, monkeypatch, "direct", database_url=database_url)


def test_postgres_cold_receiving_checkpoint_serializes_before_terminal_selection(
    database_url, tmp_path, monkeypatch
):
    with _closed_source(tmp_path, monkeypatch, database_url=database_url) as source:
        factory = source.factory
        engine = factory.kw["bind"]
        receiver = ImmutableArchiveAdmission(factory)
        settled = _load(source, receiver)
        lane = threading.local()
        entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
        pids = {}

        def capture(connection, cursor, statement, parameters, context, executemany):
            if (
                getattr(lane, "name", None) == "terminal"
                and "FROM collection_uploads" in statement
                and "FOR UPDATE" in statement
            ):
                cursor.execute("SELECT pg_backend_pid()")
                pids["terminal"] = cursor.fetchone()[0]
                waiting.set()

        def receive(session, prepared, receipt):
            pids["receiver"] = session.scalar(text("SELECT pg_backend_pid()"))
            _checkpoint(session, prepared, receipt)
            entered.set()
            assert release.wait(timeout=10)

        def select_terminal():
            lane.name = "terminal"
            return _select(source.case)

        def blocked():
            deadline = time.monotonic() + 10
            with engine.connect() as observer:
                while time.monotonic() < deadline:
                    if pids["receiver"] in observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["terminal"]}
                    ):
                        return True
                    time.sleep(0.01)
            return False

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(
                    receiver.receive_settled,
                    settled,
                    apply_receipt=receive,
                    verify_receipt=_verify_checkpoint,
                )
                second = None
                try:
                    assert entered.wait(timeout=10)
                    second = pool.submit(select_terminal)
                    assert waiting.wait(timeout=10) and blocked()
                    assert not second.done()
                finally:
                    release.set()
                first.result(timeout=10)
                assert second is not None
                selected = second.result(timeout=10)
        finally:
            release.set()
            event.remove(engine, "before_cursor_execute", capture)
        with session_scope(factory) as session:
            assert session.get(ArchiveUploadTerminalRecord, selected).phase == "draining"
            _verify_checkpoint(session, settled.prepared, settled.receipt)
        assert _effect(source.case, source.admitted).result == settled.result_json
        receiver.acknowledge_settled(settled, verify_receipt=_verify_checkpoint)
        assert _load(source, receiver) is None
        assert len(source.calls) == 1

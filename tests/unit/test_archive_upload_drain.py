from __future__ import annotations

from dataclasses import asdict

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.services.archive_upload_drain import inspect_terminal_write_records
from riverhog_protocol.errors import Conflict
from sqlalchemy import event

from tests.unit.test_archive_write_admission import (
    _apply,
    _arm,
    _case,
    _closure,
    _prepare,
    _selected,
)
from tests.unit.test_cache_population_begin import (
    _arm as _cache_arm,
)
from tests.unit.test_cache_population_begin import (
    _case as _cache_case,
)
from tests.unit.test_cache_population_begin import (
    _execute as _cache_execute,
)
from tests.unit.test_cache_population_begin import (
    _terminal as _cache_terminal,
)
from tests.unit.test_cache_population_writes import (
    _arm as _write_arm,
)
from tests.unit.test_cache_population_writes import (
    _execute as _write_execute,
)
from tests.unit.test_cache_population_writes import (
    _part,
    _writing,
)


def _inspect(case, operation_id):
    with session_scope(case.factory) as session:
        return inspect_terminal_write_records(session, case.collection_id, operation_id)


def _archive_transition(case, unknown):
    _prepare(case)
    ticket = _arm(case)
    operation = _selected(case)
    before = _inspect(case, operation)
    assert before.archive_unclosed and before.archive_retained
    assert not before.cache_begin_unclosed and not before.cache_write_unclosed
    if unknown:
        case.owner.record_unknown(ticket)
        assert _inspect(case, operation) == before
    case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
    settled = _inspect(case, operation)
    assert not settled.archive_unclosed and settled.archive_retained
    case.owner.acknowledge_report(ticket)
    retired = _inspect(case, operation)
    assert not retired.archive_unclosed and not retired.archive_retained
    with session_scope(case.factory) as session:
        terminal = session.get(ArchiveUploadTerminalRecord, operation)
        assert terminal.phase == "draining" and terminal.result_json is None


@pytest.mark.parametrize("unknown", (False, True))
def test_archive_unknown_and_closed_reports_keep_distinct_terminal_facts(tmp_path, unknown):
    with _case(tmp_path) as case:
        _archive_transition(case, unknown)


def _cache_begin_transition(case, unknown):
    ticket = _cache_arm(case)
    operation = _cache_terminal(case)
    before = _inspect(case, operation)
    assert before.cache_begin_unclosed and before.cache_begin_retained
    assert not before.archive_unclosed and not before.archive_retained
    response = case.owner.execute(ticket, candidate=case.candidate)
    if unknown:
        case.owner.record_response(ticket, response.model_copy(update={"execution": "unknown"}))
        assert _inspect(case, operation) == before
    case.owner.record_response(ticket, response)
    after = _inspect(case, operation)
    assert not after.cache_begin_unclosed and after.cache_begin_retained
    assert not after.archive_unclosed and not after.archive_retained


@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_cache_begin_remains_visible_independently_of_archive_calls(tmp_path, mode, unknown):
    with _cache_case(tmp_path, mode=mode) as case:
        _cache_begin_transition(case, unknown)


def _cache_write_transition(writing, unknown):
    case = writing.case
    ticket = _write_arm(writing, _part(writing))
    operation = _cache_terminal(case)
    before = _inspect(case, operation)
    assert before.cache_write_unclosed and before.cache_write_retained
    assert not before.archive_unclosed and not before.cache_begin_unclosed
    response = writing.owner.execute(ticket, candidate=case.candidate, content=b"abc")
    if unknown:
        writing.owner.record_response(ticket, response.model_copy(update={"execution": "unknown"}))
        assert _inspect(case, operation) == before
    writing.owner.record_response(ticket, response)
    after = _inspect(case, operation)
    assert not after.cache_write_unclosed and after.cache_write_retained
    writing.owner.acknowledge_report(ticket)
    retired = _inspect(case, operation)
    assert not retired.cache_write_retained and retired.cache_begin_retained


@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("unknown", (False, True))
def test_cache_child_receiving_and_retirement_have_distinct_terminal_facts(tmp_path, mode, unknown):
    with _writing(tmp_path, mode=mode) as writing:
        _cache_write_transition(writing, unknown)


@pytest.mark.parametrize(
    "change", ("gate", "operation", "request", "frontier", "epoch", "construction")
)
def test_terminal_record_inspection_requires_exact_selected_original(tmp_path, change):
    with _case(tmp_path) as case:
        operation = _selected(case)
        with session_scope(case.factory) as session:
            control = session.get(ArchiveWriteControlRecord, case.collection_id)
            terminal = session.get(ArchiveUploadTerminalRecord, operation)
            if change == "gate":
                control.terminal_id = None
                control.gate = "open"
            elif change == "operation":
                control.terminal_id = "f" * 64
            elif change == "request":
                terminal.request_json += " "
            elif change == "frontier":
                control.next_call = "10"
            elif change == "epoch":
                control.epoch = terminal.original_epoch
            else:
                control.construction_json += " "
        with pytest.raises(Conflict):
            _inspect(case, operation)


def test_empty_catalog_records_are_read_only_and_do_not_confirm_terminal_success(tmp_path):
    with _case(tmp_path) as case:
        operation = _selected(case)
        statements = []
        engine = case.factory.kw["bind"]

        def capture(connection, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(engine, "before_cursor_execute", capture)
        try:
            state = _inspect(case, operation)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        assert not any(
            value
            for name, value in asdict(state).items()
            if name.endswith(("_unclosed", "_retained"))
        )
        effects = [
            sql
            for sql in statements
            if any(
                table in sql
                for table in (
                    "archive_write_effects",
                    "cache_population_begins",
                    "cache_population_write_effects",
                )
            )
        ]
        assert len(effects) == 6 and all(
            "LIMIT" in sql and "request_json" not in sql for sql in effects
        )
        assert not any(
            sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for sql in statements
        )
        with session_scope(case.factory) as session:
            terminal = session.get(ArchiveUploadTerminalRecord, operation)
            assert terminal.phase == "draining" and terminal.result_json is None


@pytest.mark.parametrize("owner", ("archive", "cache-begin", "cache-write"))
def test_catalog_record_queries_do_not_rescan_a_completed_prefix(tmp_path, owner, record_property):
    import hashlib
    from contextlib import ExitStack

    from riverhog_core.catalog_archive_write_models import ArchiveWriteEffectRecord
    from riverhog_core.catalog_cache_write_models import (
        CachePopulationBeginRecord,
        CachePopulationWriteEffectRecord,
    )
    from riverhog_core.services.archive_upload_drain import _record_query
    from sqlalchemy import insert, select

    with ExitStack() as stack:
        if owner == "archive":
            case = stack.enter_context(_case(tmp_path))
            _prepare(case)
            ticket = _arm(case)
            case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
            model = ArchiveWriteEffectRecord
        elif owner == "cache-begin":
            case = stack.enter_context(_cache_case(tmp_path))
            ticket = _cache_arm(case)
            _cache_execute(case, ticket)
            model = CachePopulationBeginRecord
        else:
            writing = stack.enter_context(_writing(tmp_path))
            case = writing.case
            ticket = _write_arm(writing, _part(writing))
            _write_execute(writing, ticket, content=b"abc")
            model = CachePopulationWriteEffectRecord
        with session_scope(case.factory) as session:
            original = (
                session.execute(select(model.__table__).where(model.id == ticket.call_id))
                .mappings()
                .one()
            )
            template = dict(original)
        counts = []
        prior = 1
        for count in (16, 256, 4096):
            # Structural query-cost rows are not provider closure evidence.
            rows = []
            for index in range(prior, count):
                identity = hashlib.sha256(f"drain-query-{index}".encode()).hexdigest()
                values = {**template, "id": identity}
                if model is CachePopulationBeginRecord:
                    values["population_generation"] = identity
                rows.append(values)
            with session_scope(case.factory) as session:
                session.execute(insert(model), rows)
            prior = count
            instructions = 0
            with session_scope(case.factory) as session:
                connection = session.connection().connection.driver_connection

                def step():
                    nonlocal instructions
                    instructions += 1
                    return 0

                connection.set_progress_handler(step, 1)
                try:
                    assert (
                        session.scalar(_record_query(model, case.collection_id, unclosed=True))
                        is None
                    )
                    assert (
                        session.scalar(_record_query(model, case.collection_id, unclosed=False))
                        is not None
                    )
                finally:
                    connection.set_progress_handler(None, 0)
                for unclosed in (True, False):
                    query = _record_query(model, case.collection_id, unclosed=unclosed)
                    sql = str(
                        query.compile(session.get_bind(), compile_kwargs={"literal_binds": True})
                    )
                    plan = session.connection().exec_driver_sql("EXPLAIN QUERY PLAN " + sql).all()
                    prefix = (
                        "ix_"
                        + {
                            ArchiveWriteEffectRecord: "archive_write_effects",
                            CachePopulationBeginRecord: "cache_population_begins",
                            CachePopulationWriteEffectRecord: "cache_write_effects",
                        }[model]
                        + "_"
                    )
                    allowed = (
                        (prefix + "pending",)
                        if unclosed
                        else (
                            prefix + "pending",
                            prefix + "capacity",
                        )
                    )
                    assert any(
                        "SEARCH" in item[3]
                        and "collection_id" in item[3]
                        and any(index in item[3] for index in allowed)
                        for item in plan
                    ), plan
            counts.append(instructions)
        # Keep first-probe startup in the measured reference; more retained
        # history must not increase the instruction budget.
        assert max(counts) <= counts[0] + 4
        record_property("completed_prefix_counts", [16, 256, 4096])
        record_property("sqlite_selection_vm_instructions", counts)


def _native_seal_before_logical_receipt(tmp_path, monkeypatch, kind, mode, *, database_url=None):
    from tests.unit.test_archive_payload_unit import _select_terminal, _service_actor, _stored
    from tests.unit.test_archive_upload_invocations import _receive

    with _service_actor(tmp_path, monkeypatch, kind, mode, database_url=database_url) as f:

        def missing_receipt(*args, **kwargs):
            raise OSError("original logical receipt acknowledgement missing")

        with monkeypatch.context() as patch:
            patch.setattr(f.service, "_record_sealed_volume", missing_receipt)
            assert f.submit() == "submitted"
            (forward,) = _receive(f.registry, f.execution)
            with pytest.raises(OSError, match="logical receipt acknowledgement"):
                forward.result()
        checkpoint, receipt, _ = _stored(f)
        assert checkpoint is not None and receipt is None
        assert f.originals[0].has_pending
        operation = _select_terminal(f)
        before = _inspect(f.case, operation)
        assert before.payload_sealed
        assert not before.archive_unclosed
        with session_scope(f.case.factory) as session:
            original = session.get(ArchiveUploadTerminalRecord, operation)
            assert original.phase == "draining" and original.result_json is None
        calls = list(f.seen)
        assert (
            f.registry.submit_terminal(f.key, target_incarnation=f.target, operation_id=operation)
            == "submitted"
        )
        (terminal,) = _receive(f.registry, f.execution)
        assert terminal.result() == "closed"
        assert f.seen == calls and _stored(f)[1] is not None
        assert _inspect(f.case, operation).payload_sealed
        with session_scope(f.case.factory) as session:
            original = session.get(ArchiveUploadTerminalRecord, operation)
            assert original.phase == "draining" and original.result_json is None


@pytest.mark.parametrize("kind", ("pack", "segment"))
@pytest.mark.parametrize("mode", ("direct", "owned-http"))
def test_terminal_custody_keeps_native_completion_before_logical_receipt(
    tmp_path, monkeypatch, kind, mode
):
    _native_seal_before_logical_receipt(tmp_path, monkeypatch, kind, mode)


@pytest.mark.parametrize("sealed", (False, True))
def test_terminal_sealed_payload_probe_is_indexed_across_unsealed_prefix(
    tmp_path, sealed, record_property
):
    _sealed_probe_growth(tmp_path, sealed, record_property)


def _sealed_probe_growth(tmp_path, sealed, record_property, *, database_url=None):
    from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord as Volume
    from riverhog_core.services.archive_upload_drain import _sealed_payload_query
    from sqlalchemy import insert, select

    from tests.unit.test_collection_uploads import _CREATOR

    with _case(tmp_path, database_url=database_url) as case:
        with session_scope(case.factory) as session:
            originals = list(
                session.execute(
                    select(Volume.__table__).where(Volume.collection_id == case.collection_id)
                ).mappings()
            )
            template = dict(next(row for row in originals if row["object_id"] == case.object_id))
            first_sequence = max(int(row["sequence"]) for row in originals) + 1
        template.pop("source_last_part")
        # These rows witness query work, not provider closure or custody authority.
        foreign = case.service.create_or_resume(
            idempotency_key="foreign-sealed-probe",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )
        with session_scope(case.factory) as session:
            session.execute(
                insert(Volume.__table__),
                [
                    {
                        **template,
                        "collection_id": int(foreign["collection_id"]),
                        "object_id": "foreign-sealed",
                        "state": "sealed",
                        "sequence": 0,
                    }
                ],
            )
            if sealed:
                session.execute(
                    insert(Volume.__table__),
                    [
                        {
                            **template,
                            "object_id": "selected-sealed",
                            "state": "sealed",
                            "sequence": first_sequence,
                        }
                    ],
                )
        counts = []
        prior = 0
        for count in (16, 256, 4096):
            with session_scope(case.factory) as session:
                session.execute(
                    insert(Volume.__table__),
                    [
                        {
                            **template,
                            "object_id": "unsealed-" + str(index),
                            "sequence": first_sequence + 1 + index,
                            "state": "planned",
                        }
                        for index in range(prior, count)
                    ],
                )
            prior = count
            with session_scope(case.factory) as session:
                query = _sealed_payload_query(case.collection_id)
                sql = str(query.compile(session.get_bind(), compile_kwargs={"literal_binds": True}))
                assert "checkpoint_json" not in sql and "plan_json" not in sql
                if session.get_bind().dialect.name == "sqlite":
                    instructions = 0
                    connection = session.connection().connection.driver_connection

                    def step():
                        nonlocal instructions
                        instructions += 1
                        return 0

                    connection.set_progress_handler(step, 1)
                    try:
                        found = session.scalar(query)
                    finally:
                        connection.set_progress_handler(None, 0)
                    plan = session.connection().exec_driver_sql("EXPLAIN QUERY PLAN " + sql).all()
                    assert any(
                        "ix_collection_archive_object_uploads_sealed" in row[-1] for row in plan
                    )
                    cost = instructions
                    metric = "sealed_payload_probe_sqlite_vm_steps"
                else:
                    session.connection().exec_driver_sql(
                        "ANALYZE collection_archive_object_uploads"
                    )
                    found = session.scalar(query)
                    document = (
                        session.connection()
                        .exec_driver_sql("EXPLAIN (ANALYZE TRUE, BUFFERS TRUE, FORMAT JSON) " + sql)
                        .scalar_one()[0]
                    )
                    pending = [document["Plan"]]
                    nodes = []
                    while pending:
                        node = pending.pop()
                        nodes.append(node)
                        pending.extend(node.get("Plans", []))
                    if count == 4096:
                        assert any(
                            node.get("Index Name") == "ix_collection_archive_object_uploads_sealed"
                            for node in nodes
                        )
                    root = document["Plan"]
                    cost = root.get("Shared Hit Blocks", 0) + root.get("Shared Read Blocks", 0)
                    assert cost <= 32
                    metric = "sealed_payload_probe_postgresql_shared_blocks"
                assert found == ("selected-sealed" if sealed else None)
            counts.append((count + len(originals) + int(sealed), cost))
        record_property(metric, counts)
        assert max(n for _, n in counts) <= min(n for _, n in counts) + 32

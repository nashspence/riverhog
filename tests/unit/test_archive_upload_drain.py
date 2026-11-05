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

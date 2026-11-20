from __future__ import annotations

import pytest
from riverhog_core.catalog_db import create_catalog_engine
from riverhog_core.services.archive_write_admission import _writer
from sqlalchemy import event, text
from sqlalchemy.exc import SAWarning
from sqlalchemy.orm import Session


def _engine(tmp_path):
    return create_catalog_engine("sqlite:///" + str(tmp_path / "catalog.sqlite3"))


def test_nested_owner_checks_share_the_actual_early_sqlite_writer(tmp_path):
    engine = _engine(tmp_path)
    statements = []

    def observed(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", observed)
    try:
        with Session(engine) as session:
            _writer(session, defer_if_busy=True)
            assert session.scalar(text("SELECT 1")) == 1
            _writer(session, defer_if_busy=True)
            _writer(session, defer_if_busy=False)
            assert session.scalar(text("SELECT 2")) == 2
        assert sum(statement.upper() == "BEGIN IMMEDIATE" for statement in statements) == 1
        assert statements.index("BEGIN IMMEDIATE") < statements.index("SELECT 1")
    finally:
        engine.dispose()


@pytest.mark.parametrize("end", ("commit", "rollback"))
def test_early_writer_proof_belongs_to_only_the_original_transaction(tmp_path, end):
    engine = _engine(tmp_path)
    begins = []

    def observed(connection, cursor, statement, parameters, context, executemany):
        if statement.upper() == "BEGIN IMMEDIATE":
            begins.append(statement)

    event.listen(engine, "before_cursor_execute", observed)
    try:
        with Session(engine) as session:
            _writer(session)
            first = session.get_transaction()
            assert session.scalar(text("SELECT 1")) == 1
            getattr(session, end)()
            assert session.get_transaction() is None
            _writer(session, defer_if_busy=True)
            assert session.get_transaction() is not first
            assert session.scalar(text("SELECT 2")) == 2
        assert len(begins) == 2
    finally:
        engine.dispose()


def test_an_existing_read_transaction_cannot_be_relabelled_as_an_early_writer(tmp_path):
    engine = _engine(tmp_path)
    try:
        with Session(engine) as session:
            assert session.scalar(text("SELECT 1")) == 1
            with pytest.raises(SAWarning, match="already established"):
                _writer(session, defer_if_busy=True)
            session.rollback()
            _writer(session, defer_if_busy=True)
            assert session.scalar(text("SELECT 2")) == 2
    finally:
        engine.dispose()


@pytest.mark.parametrize("alter_options", (False, True))
def test_read_first_rejection_does_not_depend_on_python_warning_policy(tmp_path, alter_options):
    import warnings

    from riverhog_protocol.errors import Conflict

    engine = _engine(tmp_path)
    try:
        with Session(engine) as session:
            assert session.scalar(text("SELECT 1")) == 1
            if alter_options:
                session.connection().execution_options(sqlite_immediate_writer=True)
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always", SAWarning)
                with pytest.raises(Conflict, match="precede catalog reads"):
                    _writer(session, defer_if_busy=True)
    finally:
        engine.dispose()

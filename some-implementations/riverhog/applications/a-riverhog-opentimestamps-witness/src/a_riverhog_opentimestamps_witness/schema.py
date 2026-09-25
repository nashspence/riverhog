"""The OpenTimestamps witness owns one SQLite state and one forward migration chain."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Column, Index, Integer, LargeBinary, MetaData, Table, Text
from state_schema import (
    StateConnection,
    StateEngine,
    StateSchema,
    StateStatus,
    assert_schema_matches_metadata,
    sqlite_engine,
)

STATE_VERSION_TABLE = "state_schema_revision"
STATE_MIGRATIONS = Path(__file__).with_name("state_migrations")
METADATA = MetaData()

Table(
    "progress",
    METADATA,
    Column("id", Integer, primary_key=True),
    Column("generation", Integer, nullable=False),
    Column("serial", Integer, nullable=False),
    Column("phase", Text, nullable=False),
    Column("source_identity", Text),
    Column("authorization_view_identity", Text),
    Column("cursor", Text),
    Column("through_revision", Text, nullable=False),
    Column("last_collection_id", Integer),
    Column("reset_reason", Text),
)
Table(
    "observations",
    METADATA,
    Column("generation", Integer, primary_key=True),
    Column("collection_id", Integer, primary_key=True),
    Column("revision", Text, nullable=False),
    Column("document", LargeBinary, nullable=False),
    Column("statement_digest", Text),
    Column("departed", Integer, nullable=False),
    Column("departure_cause", Text),
)
statements = Table(
    "statements",
    METADATA,
    Column("digest", Text, primary_key=True),
    Column("payload", LargeBinary, nullable=False),
    Column("job_state", LargeBinary, nullable=False),
    Column("job_version", Integer, nullable=False),
    Column("due", Integer),
)
Index("ix_statements_due", statements.c.due, statements.c.digest)
Table(
    "proof_history",
    METADATA,
    Column("digest", Text, primary_key=True),
    Column("revision", Integer, primary_key=True),
    Column("proof", LargeBinary, nullable=False),
    Column("proof_digest", Text, nullable=False),
    Column("recorded_at", Integer, nullable=False),
)


def _verify(connection: StateConnection) -> None:
    assert_schema_matches_metadata(connection, METADATA, version_table=STATE_VERSION_TABLE)


def state_schema(database: Path) -> StateSchema:
    path = Path(database)

    def engine_factory() -> StateEngine:
        path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite_engine(path)

    return StateSchema(
        name="a-riverhog-opentimestamps-witness",
        engine_factory=engine_factory,
        script_location=STATE_MIGRATIONS,
        verify=_verify,
        is_empty=lambda: not path.exists() or path.stat().st_size == 0,
        version_table=STATE_VERSION_TABLE,
    )


def upgrade_state(database: Path) -> StateStatus:
    return state_schema(database).upgrade()


def validate_state(database: Path) -> StateStatus:
    return state_schema(database).validate()

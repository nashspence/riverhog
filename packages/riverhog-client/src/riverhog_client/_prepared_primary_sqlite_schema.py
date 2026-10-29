"""Indexed schema for one application's transactional prepared-primary byte state."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
)
from state_schema import EngineFactory, StateConnection, StateSchema
from state_schema.sqlalchemy_schema import assert_schema_matches_metadata

VERSION_TABLE = "primary_schema_revision"
MIGRATIONS = Path(__file__).with_name("_prepared_primary_migrations")
METADATA = MetaData()

Table(
    "primary_format",
    METADATA,
    Column("id", Integer, primary_key=True),
    Column("allocation", LargeBinary, nullable=False),
    Column("geometry", Text, nullable=False),
    Column("barrier", LargeBinary, nullable=False),
    Column("journal_reserve", LargeBinary, nullable=False),
    Column("resource_grants", LargeBinary, nullable=False),
    Column("database_reserve", LargeBinary, nullable=False),
    Column("progress_journal", LargeBinary, nullable=False),
    Column("physical_readiness", LargeBinary, nullable=False),
    CheckConstraint(
        "typeof(progress_journal) = 'blob' AND length(progress_journal) = 64",
        name="ck_primary_format_progress_journal",
    ),
    CheckConstraint(
        "typeof(physical_readiness) = 'blob' AND length(physical_readiness) = 64",
        name="ck_primary_format_physical_readiness",
    ),
    CheckConstraint("id = 1", name="ck_primary_format_singleton"),
    CheckConstraint(
        "typeof(barrier) = 'blob' AND length(barrier) = 8", name="ck_primary_format_barrier"
    ),
    CheckConstraint("length(journal_reserve) = 16", name="ck_primary_format_journal_reserve"),
    CheckConstraint(
        "typeof(resource_grants) = 'blob' AND length(resource_grants) = 64",
        name="ck_primary_format_resource_grants",
    ),
    CheckConstraint(
        "typeof(database_reserve) = 'blob' AND length(database_reserve) = 72",
        name="ck_primary_format_database_reserve",
    ),
)
Table(
    "primary_namespaces",
    METADATA,
    Column("id", Integer, primary_key=True),
    Column("parent", Integer, ForeignKey("primary_namespaces.id")),
    Column("name", Text, nullable=False),
    UniqueConstraint("parent", "name", name="uq_primary_namespace_name"),
)
Table(
    "primary_objects",
    METADATA,
    Column("generation", Text, primary_key=True),
    Column("namespace", Integer, ForeignKey("primary_namespaces.id"), nullable=False),
    Column("name", Text, nullable=False),
    Column("bytes", Integer, nullable=False),
    Column("sha256", Text, nullable=False),
    Column("complete", Integer, nullable=False),
    CheckConstraint("bytes >= 0", name="ck_primary_object_bytes"),
    CheckConstraint("complete IN (0, 1)", name="ck_primary_object_complete"),
)
Table(
    "primary_chunks",
    METADATA,
    Column("generation", Text, ForeignKey("primary_objects.generation"), primary_key=True),
    Column("ordinal", Integer, primary_key=True),
    Column("body", LargeBinary, nullable=False),
    CheckConstraint("ordinal >= 0", name="ck_primary_chunk_ordinal"),
    CheckConstraint("length(body) BETWEEN 1 AND 4096", name="ck_primary_chunk_bytes"),
)
Table(
    "primary_names",
    METADATA,
    Column("namespace", Integer, ForeignKey("primary_namespaces.id"), primary_key=True),
    Column("name", Text, primary_key=True),
    Column("generation", Text, ForeignKey("primary_objects.generation"), nullable=False),
    UniqueConstraint("generation", name="uq_primary_name_generation"),
)
Table(
    "primary_operation",
    METADATA,
    Column("id", Integer, primary_key=True),
    Column("generation", Text, ForeignKey("primary_objects.generation"), nullable=False),
    Column("cursor", Integer, nullable=False),
    Column("phase", Text, nullable=False),
    CheckConstraint("phase IN ('writing', 'cleanup')", name="ck_primary_operation_phase"),
    CheckConstraint("id = 1", name="ck_primary_operation_singleton"),
    CheckConstraint("cursor >= 0", name="ck_primary_operation_cursor"),
)
Table(
    "primary_controls",
    METADATA,
    Column("namespace", Integer, ForeignKey("primary_namespaces.id"), primary_key=True),
    Column("name", Text, primary_key=True),
    Column("body", LargeBinary, nullable=False),
)


Table(
    "primary_ordering",
    METADATA,
    Column("id", Integer, primary_key=True),
    Column("generation", Text, nullable=False),
    Column("namespace", Integer, ForeignKey("primary_namespaces.id"), nullable=False),
    Column("phase", Text, nullable=False),
    Column("cursor", Text),
    Column("base_pages", Integer, nullable=False),
    Column("byte_limit", Text, nullable=False),
    UniqueConstraint("generation", name="uq_primary_ordering_generation"),
    CheckConstraint("id = 1", name="ck_primary_ordering_singleton"),
    CheckConstraint("phase IN ('building', 'ready', 'cleanup')", name="ck_primary_ordering_phase"),
    CheckConstraint("base_pages >= 0", name="ck_primary_ordering_base_pages"),
)
Table(
    "primary_order_rows",
    METADATA,
    Column("generation", Text, ForeignKey("primary_ordering.generation"), primary_key=True),
    Column("artifact_id", Text, primary_key=True),
    Column("bytes", Text, nullable=False),
    Column("sha256", Text, nullable=False),
    sqlite_with_rowid=False,
)


def _verify_shape(connection: StateConnection) -> None:
    """Bounded schema inspection; full integrity scans belong to explicit audits."""
    assert_schema_matches_metadata(connection, METADATA, version_table=VERSION_TABLE)


def _primary_state_schema(engine_factory: EngineFactory) -> StateSchema:
    return StateSchema(
        name="prepared-primary byte store",
        engine_factory=engine_factory,
        script_location=MIGRATIONS,
        verify=_verify_shape,
        version_table=VERSION_TABLE,
    )


Table(
    "primary_reserve_rows",
    METADATA,
    Column("id", Integer, primary_key=True),
    Column("body", LargeBinary, nullable=False),
    CheckConstraint("id > 0", name="ck_primary_reserve_row_id"),
    CheckConstraint(
        "typeof(body) = 'blob' AND length(body) = 4096", name="ck_primary_reserve_row_body"
    ),
)


Table(
    "primary_journal_pad",
    METADATA,
    Column("id", Integer, primary_key=True),
    Column("body", LargeBinary, nullable=False),
    CheckConstraint("id > 0", name="ck_primary_journal_pad_id"),
    CheckConstraint(
        "typeof(body) = 'blob' AND length(body) = 4096", name="ck_primary_journal_pad_body"
    ),
)

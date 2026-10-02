"""Exact current local materialization state; pre-v1 state is rebaselined."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    text,
)
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
LOCAL_STATE_METADATA = MetaData()

Table(
    "settings",
    LOCAL_STATE_METADATA,
    Column("key", Text, primary_key=True),
    Column("value", Text, nullable=False),
)
Table(
    "desired_collections",
    LOCAL_STATE_METADATA,
    Column("collection_id", Integer, primary_key=True),
    Column("archive_root_sha256", Text, nullable=False),
    Column("inventory_identity", Text, nullable=False),
    Column("artifact_set_identity", Text, nullable=False),
    Column("provenance_identity", Text, nullable=False),
    Column("created_at", Text, nullable=False),
    Column("layout_mode", Text, nullable=False),
    Column("rules_json", Text, nullable=False),
    Column("remote_unavailable", Integer, nullable=False, server_default=text("0")),
    CheckConstraint("collection_id > 0", name="ck_desired_collections_id"),
    CheckConstraint(
        "layout_mode IN ('declared-hints', 'id-layout')", name="ck_desired_collections_layout"
    ),
    CheckConstraint("remote_unavailable IN (0, 1)", name="ck_desired_collections_unavailable"),
)
Table(
    "desired_collection_tags",
    LOCAL_STATE_METADATA,
    Column(
        "collection_id",
        Integer,
        ForeignKey("desired_collections.collection_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("tag", Text, primary_key=True),
)
Table(
    "desired_artifacts",
    LOCAL_STATE_METADATA,
    Column(
        "collection_id",
        Integer,
        ForeignKey("desired_collections.collection_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("artifact_id", Text, primary_key=True),
    Column("bytes", Integer, nullable=False),
    Column("sha256", Text, nullable=False),
    Column("destination_json", Text, nullable=False),
    Column("reason", Text, nullable=False),
    Column("hint_json", Text),
    Column("binding_json", Text, nullable=False),
    Column("primary_bytes", Integer, nullable=False),
    Column("primary_sha256", Text, nullable=False),
    Column("history_binding_json", Text, nullable=False),
    Column("history_extent", Text, nullable=False),
    Column("history_proof_json", Text, nullable=False),
    CheckConstraint("bytes >= 0", name="ck_desired_artifacts_bytes"),
    CheckConstraint("primary_bytes > 0", name="ck_desired_artifacts_primary_bytes"),
    CheckConstraint(
        "history_extent = 'complete-retained-history'", name="ck_desired_artifacts_history_extent"
    ),
    UniqueConstraint("collection_id", "destination_json", name="uq_desired_artifact_destination"),
)
Table(
    "desired_history_objects",
    LOCAL_STATE_METADATA,
    Column(
        "collection_id",
        Integer,
        ForeignKey("desired_collections.collection_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("object_id", Text, primary_key=True),
    Column("bytes", Integer, nullable=False),
    Column("sha256", Text, nullable=False),
    CheckConstraint("bytes > 0", name="ck_desired_history_objects_bytes"),
)
Table(
    "desired_journals",
    LOCAL_STATE_METADATA,
    Column(
        "collection_id",
        Integer,
        ForeignKey("desired_collections.collection_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("journal_id", Text, primary_key=True),
    Column("bytes", Integer, nullable=False),
    Column("sha256", Text, nullable=False),
    CheckConstraint("bytes > 0", name="ck_desired_journals_bytes"),
)
Table(
    "retrieval_jobs",
    LOCAL_STATE_METADATA,
    Column("id", Text, primary_key=True),
    Column("state", Text, nullable=False),
    Column("updated_at", Text, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    CheckConstraint(
        "state IN ('requested', 'ready', 'completed', 'expired', 'failed', 'canceled')",
        name="ck_retrieval_jobs_state",
    ),
)
Table(
    "retrieval_job_artifacts",
    LOCAL_STATE_METADATA,
    Column(
        "retrieval_job_id",
        Text,
        ForeignKey("retrieval_jobs.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("ordinal", Integer, primary_key=True),
    Column("collection_id", Integer, nullable=False),
    Column("artifact_id", Text, nullable=False),
    Column("bytes", Integer, nullable=False),
    Column("sha256", Text, nullable=False),
    CheckConstraint("ordinal >= 0", name="ck_retrieval_job_artifacts_ordinal"),
    CheckConstraint("collection_id > 0", name="ck_retrieval_job_artifacts_collection"),
    CheckConstraint("bytes >= 0", name="ck_retrieval_job_artifacts_bytes"),
    UniqueConstraint(
        "retrieval_job_id", "collection_id", "artifact_id", name="uq_retrieval_job_artifacts_member"
    ),
)


def _verify(connection: StateConnection) -> None:
    assert_schema_matches_metadata(
        connection,
        LOCAL_STATE_METADATA,
        version_table=STATE_VERSION_TABLE,
    )


def state_schema(database: Path) -> StateSchema:
    path = Path(database)

    def engine_factory() -> StateEngine:
        path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite_engine(path)

    return StateSchema(
        name="a-riverhog-cli local",
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

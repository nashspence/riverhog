from __future__ import annotations

import ast
import hashlib
import sqlite3
import tomllib
from contextlib import closing
from pathlib import Path

import pytest
from a_riverhog_cli.local_state import state_schema as local_state_schema
from a_riverhog_event_relay.relay import CursorState
from a_riverhog_event_relay.schema import state_schema as a_riverhog_event_relay_state_schema
from a_riverhog_ftp_spool.state_contract import FTP_OPERATIONAL_STATE_DDL
from a_riverhog_minisign_witness.schema import state_schema as minisign_witness_schema
from a_riverhog_opentimestamps_witness.schema import state_schema as ots_witness_schema
from gogurt_listener_runtime import ListenerStore
from riverhog_core.state_migrations.v1_ddl import POSTGRESQL_DDL
from riverhog_provenance import load_or_create_installation_id
from sqlalchemy.dialects import postgresql
from stove0_core.state_migrations.v1_ddl import POSTGRESQL_DDL as STOVE0_POSTGRESQL_DDL

from scripts import state_contract

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "tests/fixtures/state/v1_0001"

MIGRATION_BASELINES = {
    "riverhog/src/riverhog_core/state_migrations/versions/v1_0001.py": (
        "alembic",
        "riverhog_core.state_migrations.v1_ddl",
    ),
    (
        "some-implementations/stove0/application/server/src/stove0_core/"
        "state_migrations/versions/v1_0001.py"
    ): (
        "alembic",
        "stove0_core.state_migrations.v1_ddl",
    ),
    (
        "some-implementations/riverhog/applications/a-riverhog-cli/"
        "src/a_riverhog_cli/state_migrations/versions/v1_0001.py"
    ): (
        "alembic",
        "a_riverhog_cli.state_migrations.v1_ddl",
    ),
    (
        "some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/state_migrations/"
        "versions/v1_0001.py"
    ): (
        "alembic",
        "a_riverhog_event_relay.state_migrations.v1_ddl",
    ),
    (
        "some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/"
        "a_riverhog_minisign_witness/state_migrations/versions/v1_0001.py"
    ): ("alembic", "a_riverhog_minisign_witness.state_migrations.v1_ddl"),
    (
        "some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/"
        "a_riverhog_opentimestamps_witness/state_migrations/versions/v1_0001.py"
    ): ("alembic", "a_riverhog_opentimestamps_witness.state_migrations.v1_ddl"),
}


def _restore_sqlite(fixture: Path, database: Path) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript(fixture.read_text(encoding="utf-8"))


def _connect(database: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    return connection


def test_a_riverhog_cli_current_v1_fixture_restarts_with_selection_and_retrieval_state(
    tmp_path: Path,
) -> None:
    database = tmp_path / "a-riverhog-cli.sqlite3"
    _restore_sqlite(FIXTURES / "a-riverhog-cli.sqlite.sql", database)

    status = local_state_schema(database).upgrade()
    with closing(_connect(database)) as connection:
        collection = connection.execute(
            "SELECT inventory_identity, remote_deleted "
            "FROM desired_collections WHERE collection_id = 1"
        ).fetchone()
        file = connection.execute(
            "SELECT path, bytes, sha256 FROM desired_files WHERE collection_id = 1"
        ).fetchone()
        retrieval = connection.execute(
            "SELECT state FROM retrieval_jobs WHERE id = 'fixture-retrieval'"
        ).fetchone()
        retrieval_file = connection.execute(
            "SELECT collection_id, path, bytes, sha256 FROM retrieval_job_files "
            "WHERE retrieval_job_id = 'fixture-retrieval' ORDER BY ordinal"
        ).fetchone()

    assert status.condition == "current"
    assert collection is not None
    assert tuple(collection) == ("b" * 64, 0)
    assert file is not None
    assert tuple(file) == ("notes/fixture.txt", 12, "a" * 64)
    assert retrieval is not None
    assert tuple(retrieval) == ("ready",)
    assert retrieval_file is not None
    assert tuple(retrieval_file) == (1, "notes/fixture.txt", 12, "a" * 64)


def test_a_riverhog_event_relay_current_v1_fixture_restarts_with_source_cursor(
    tmp_path: Path,
) -> None:
    database = tmp_path / "a-riverhog-event-relay.sqlite3"
    _restore_sqlite(FIXTURES / "a-riverhog-event-relay.sqlite.sql", database)

    status = a_riverhog_event_relay_state_schema(database).upgrade()
    cursor_state = CursorState(database)

    assert status.condition == "current"
    assert cursor_state.cursor("stove0") == "23"


@pytest.mark.parametrize(
    ("name", "schema"),
    [
        ("a-riverhog-minisign-witness", minisign_witness_schema),
        ("a-riverhog-opentimestamps-witness", ots_witness_schema),
    ],
)
def test_witness_v1_fixture_reopens_at_current_schema(
    tmp_path: Path, name: str, schema: object
) -> None:
    database = tmp_path / f"{name}.sqlite3"
    _restore_sqlite(FIXTURES / f"{name}.sqlite.sql", database)
    assert schema(database).validate().condition == "current"


def test_gogurt_listener_v1_fixture_preserves_uncertain_dispatch_custody(
    tmp_path: Path,
) -> None:
    database = tmp_path / "listener.sqlite3"
    _restore_sqlite(FIXTURES / "gogurt-listener.sqlite.sql", database)

    store = ListenerStore(database)
    store.create()

    assert store.summary() == {
        "counts": {"uncertain": 1},
        "attention": [
            {
                "dispatch_id": "b" * 64,
                "mount_point": "/fixture/mounted-volume",
                "route": "camera",
                "state": "uncertain",
                "attempts": 1,
                "exit_code": None,
                "error": "listener exited while the action process had custody",
            }
        ],
    }


def test_provenance_installation_v1_fixture_retains_exact_identity(tmp_path: Path) -> None:
    fixture = FIXTURES / "provenance-installation-id"
    destination = tmp_path / "provenance-installation-id"
    destination.write_bytes(fixture.read_bytes())

    assert load_or_create_installation_id(destination) == (
        "urn:uuid:00000000-0000-4000-8000-000000000001"
    )


def test_release_inventory_accounts_for_every_v1_state_fixture() -> None:
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    inventory = release["state"]
    assert inventory["format"] == "riverhog-durable-state-inventory/v1"
    owners = inventory["owners"]
    assert {owner["classification"] for owner in owners} == {
        "durable-user-content",
        "durable-user-evidence",
        "operational-state",
        "installation-identity",
    }
    assert all(
        set(owner)
        == {
            "id",
            "distribution",
            "classification",
            "format",
            "head",
            "transition",
            "structure",
            "fixtures",
        }
        for owner in owners
    )
    assert {owner["transition"] for owner in owners} == {
        "forward-migration-chain",
        "backward-readable-documents",
        "immutable-identity",
    }
    fixture_paths = {fixture for owner in owners for fixture in owner["fixtures"]}
    assert fixture_paths == {
        path.relative_to(REPO_ROOT).as_posix() for path in FIXTURES.rglob("*") if path.is_file()
    }
    assert all(
        len(hashlib.sha256((REPO_ROOT / path).read_bytes()).hexdigest()) == 64
        for path in fixture_paths
    )


def test_every_v1_state_owner_projects_its_component_owned_exact_structure() -> None:
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    projected = {
        owner["id"]: state_contract.project_owner(owner) for owner in release["state"]["owners"]
    }

    catalog = projected["riverhog-catalog"]
    assert "fixtures" not in catalog
    catalog_structure = catalog["structure"]
    collections = next(
        table for table in catalog_structure["tables"] if table["name"] == "collections"
    )
    description_search = next(
        column for column in collections["columns"] if column["name"] == "description_search"
    )
    assert description_search["nullable"] is False
    assert description_search["default"] == "''"
    assert any(
        constraint.get("name") == "ck_collections_archive_root_sha256"
        for constraint in collections["constraints"]
    )
    assert any(
        index["name"] == "ux_app_keys_token_sha256" for index in catalog_structure["unique_indexes"]
    )
    assert not any(
        index["name"] == "ix_retrieval_cache_objects_cleanup"
        for index in catalog_structure["unique_indexes"]
    )

    target_documents = projected["stove0-target-jobs"]["structure"]["documents"]
    assert {document["id"] for document in target_documents} == {
        "AcceptedTargetJob",
        "TargetJobStatus",
    }
    assert all(document["schema"]["type"] == "object" for document in target_documents)

    ftp_units = projected["a-riverhog-ftp-spool-custody"]["structure"]["units"]
    assert {unit["id"] for unit in ftp_units} == {
        "operational-database",
        "completion-log",
        "claim",
        "receipt",
        "payload",
    }
    assert (
        next(unit for unit in ftp_units if unit["id"] == "operational-database")["kind"]
        == "relational-schema"
    )

    assert projected["riverhog-provenance-installation"]["structure"] == {
        "kind": "text-document",
        "encoding": "ascii",
        "line_count": 1,
        "value": {
            "kind": "canonical-uuid-urn",
            "pattern": (
                r"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                r"[0-9a-f]{4}-[0-9a-f]{12}$"
            ),
        },
        "terminator": "LF",
    }
    assert projected["gogurt-listener"]["structure"]["dialect"] == "sqlite"


@pytest.mark.parametrize("owner", ["riverhog", "stove0"])
def test_postgresql_state_nullability_and_generation_match_runtime_models(owner: str) -> None:
    from riverhog_core.catalog_db import Base
    from stove0_core.persistence import _Base as Stove0Base

    ddl, metadata = (
        (POSTGRESQL_DDL, Base.metadata)
        if owner == "riverhog"
        else (STOVE0_POSTGRESQL_DDL, Stove0Base.metadata)
    )
    projected = state_contract.relational_schema(ddl, dialect="postgresql")
    dialect = postgresql.dialect()
    compiler = dialect.ddl_compiler(dialect, None)
    assert {table["name"] for table in projected["tables"]} == set(metadata.tables)
    for table in projected["tables"]:
        model = metadata.tables[table["name"]]
        assert {column["name"] for column in table["columns"]} == set(model.c.keys())
        for column in table["columns"]:
            source = model.c[column["name"]]
            identity = f"{owner}:{model.name}.{source.name}"
            assert column["nullable"] is source.nullable, identity
            if source.identity is not None:
                assert column["generated"] == compiler.visit_identity_column(source.identity)
                assert "default" not in column, identity
            elif source.computed is not None:
                assert column["generated"] == compiler.visit_computed_column(source.computed)
            else:
                assert "generated" not in column, identity


@pytest.mark.parametrize("mode", ["BY DEFAULT", "ALWAYS"])
def test_postgresql_identity_is_nonnullable_without_a_primary_key(mode: str) -> None:
    projected = state_contract.relational_schema(
        f"CREATE TABLE items (id BIGINT GENERATED {mode} AS IDENTITY, value INTEGER DEFAULT 7)",
        dialect="postgresql",
    )
    identity, value = projected["tables"][0]["columns"]
    assert identity["generated"] == f"GENERATED {mode} AS IDENTITY"
    assert identity["nullable"] is False
    assert "default" not in identity
    assert value["default"] == "7"
    assert value["nullable"] is True


def test_postgresql_table_primary_key_makes_each_member_nonnullable() -> None:
    projected = state_contract.relational_schema(
        'CREATE TABLE items ("group" TEXT, id BIGINT, value TEXT, '
        'CONSTRAINT items_pk PRIMARY KEY ("group", id))',
        dialect="postgresql",
    )
    assert {column["name"]: column["nullable"] for column in projected["tables"][0]["columns"]} == {
        "group": False,
        "id": False,
        "value": True,
    }


@pytest.mark.parametrize(
    ("table", "key", "insert"),
    [
        ("adapter_state", "key", "INSERT INTO adapter_state VALUES (NULL, 'value')"),
        ("completion_events", "event_id", "INSERT INTO completion_events VALUES (NULL, 'claim')"),
        ("claims", "ordinal", "INSERT INTO claims VALUES (NULL, 'claim', '{}', 0)"),
    ],
)
def test_ftp_primary_key_nullability_matches_sqlite_storage(
    table: str, key: str, insert: str
) -> None:
    projected = state_contract.relational_schema(FTP_OPERATIONAL_STATE_DDL, dialect="sqlite")
    projected_table = next(item for item in projected["tables"] if item["name"] == table)
    column = next(item for item in projected_table["columns"] if item["name"] == key)
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.executescript(FTP_OPERATIONAL_STATE_DDL)
        connection.execute(insert)
        stored_key = connection.execute(f'SELECT "{key}" FROM "{table}"').fetchone()[0]
    # SQLite permits stored NULLs in ordinary text primary keys, but assigns
    # an integer rowid when NULL is supplied for an INTEGER PRIMARY KEY.
    assert column["nullable"] is (stored_key is None)


@pytest.mark.parametrize(
    "columns",
    [
        "id INTEGER PRIMARY KEY, value TEXT",
        "id INTEGER, value TEXT, PRIMARY KEY (id)",
        "id TEXT PRIMARY KEY, value TEXT",
        "id TEXT, value TEXT, PRIMARY KEY (id)",
        "id INTEGER, value TEXT, PRIMARY KEY (id, value)",
    ],
)
def test_sqlite_primary_key_nullability_matches_storage(columns: str) -> None:
    ddl = f"CREATE TABLE items ({columns})"
    projected = state_contract.relational_schema(ddl, dialect="sqlite")
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(ddl)
        connection.execute("INSERT INTO items VALUES (NULL, NULL)")
        stored = connection.execute("SELECT id, value FROM items").fetchone()
    assert [column["nullable"] for column in projected["tables"][0]["columns"]] == [
        value is None for value in stored
    ]


def test_relational_state_projection_requires_a_supported_dialect() -> None:
    with pytest.raises(state_contract.StateContractError, match="unsupported SQL dialect"):
        state_contract.relational_schema("CREATE TABLE items (id INTEGER)", dialect="v1")


def test_relational_state_projection_preserves_semantics_not_migration_operations() -> None:
    projected = state_contract.relational_schema(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY,
            value TEXT DEFAULT 'new' NOT NULL,
            parent_id INTEGER REFERENCES items(id),
            CONSTRAINT ck_items_value CHECK (length(value) > 0)
        );
        INSERT INTO items (id, value) VALUES (1, 'backfill');
        CREATE INDEX ix_items_value ON items (value);
        CREATE UNIQUE INDEX ux_items_parent ON items (parent_id);
        """,
        dialect="sqlite",
    )

    assert projected == {
        "kind": "relational-schema",
        "dialect": "sqlite",
        "tables": [
            {
                "name": "items",
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "nullable": False,
                        "definition": "id INTEGER PRIMARY KEY",
                        "primary_key": True,
                    },
                    {
                        "name": "value",
                        "type": "TEXT",
                        "nullable": False,
                        "definition": "value TEXT DEFAULT 'new' NOT NULL",
                        "default": "'new'",
                    },
                    {
                        "name": "parent_id",
                        "type": "INTEGER",
                        "nullable": True,
                        "definition": "parent_id INTEGER REFERENCES items(id)",
                        "references": "items(id)",
                    },
                ],
                "constraints": [
                    {
                        "kind": "check",
                        "name": "ck_items_value",
                        "definition": "CONSTRAINT ck_items_value CHECK (length(value) > 0)",
                        "expression": "(length(value) > 0)",
                    }
                ],
            }
        ],
        "unique_indexes": [
            {
                "name": "ux_items_parent",
                "table": "items",
                "columns": ["parent_id"],
                "definition": "CREATE UNIQUE INDEX ux_items_parent ON items (parent_id)",
            }
        ],
    }


def test_relational_state_projection_fails_closed_on_an_unknown_schema_operation() -> None:
    with pytest.raises(state_contract.StateContractError, match="unsupported SQL authority"):
        state_contract.relational_schema(
            "CREATE TABLE items (id INTEGER); ALTER TABLE items ADD COLUMN value TEXT;",
            dialect="sqlite",
        )


def test_riverhog_postgresql_fixture_is_the_exact_current_migration_authority() -> None:
    expected = (
        "-- Exact current Riverhog PostgreSQL v1 baseline conformance fixture.\n\n"
        "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;\n\n"
        "CREATE TABLE state_schema_revision (\n"
        "    version_num VARCHAR(32) NOT NULL,\n"
        "    CONSTRAINT state_schema_revision_pkc PRIMARY KEY (version_num)\n"
        ");\n\n" + ";\n\n".join(POSTGRESQL_DDL) + ";\n\n"
        "INSERT INTO state_schema_revision (version_num) VALUES ('v1_0001');\n"
    )

    assert (FIXTURES / "riverhog.postgresql.sql").read_text(encoding="utf-8") == expected


def test_stove0_postgresql_fixture_is_the_exact_current_migration_authority() -> None:
    expected = (
        "-- Exact current Stove0 PostgreSQL v1 baseline conformance fixture.\n\n"
        "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;\n\n"
        "CREATE TABLE stove0_state_schema_revision (\n"
        "    version_num VARCHAR(32) NOT NULL,\n"
        "    CONSTRAINT stove0_state_schema_revision_pkc PRIMARY KEY (version_num)\n"
        ");\n\n" + ";\n\n".join(STOVE0_POSTGRESQL_DDL) + ";\n\n"
        "INSERT INTO stove0_state_schema_revision (version_num) VALUES ('v1_0001');\n"
    )

    assert (FIXTURES / "stove0.postgresql.sql").read_text(encoding="utf-8") == expected


def test_v1_baseline_revisions_depend_only_on_migration_owned_ddl() -> None:
    observed: dict[str, tuple[str, ...]] = {}
    for relative_path in MIGRATION_BASELINES:
        tree = ast.parse((REPO_ROOT / relative_path).read_text(encoding="utf-8"))
        observed[relative_path] = tuple(
            sorted(
                node.module
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.module is not None
            )
        )

    assert observed == {
        path: tuple(sorted(modules)) for path, modules in MIGRATION_BASELINES.items()
    }

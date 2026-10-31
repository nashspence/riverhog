from __future__ import annotations

import json
import os
from contextlib import closing

import pytest
from a_riverhog_cli import main
from a_riverhog_cli.upload_sqlite import UploadSqlitePolicy, UploadSqliteResources
from riverhog_client.initial_tags import PreparedInitialCollectionTags
from riverhog_provenance.journal_store import JournalStore
from typer.testing import CliRunner


@pytest.mark.parametrize("cache_kib", [0, 13])
def test_cli_resource_owner_applies_cache_target_and_file_temp_to_actual_consumers(cache_kib):
    with UploadSqliteResources(UploadSqlitePolicy(cache_kib=cache_kib)) as resources:
        with PreparedInitialCollectionTags(
            ["z", "a"], connection_factory=resources.connect, on_storage_open=resources.retain
        ) as tags:
            assert tags._one("PRAGMA cache_size") == (-cache_kib,)
            assert tags._one("PRAGMA temp_store") == (1,)
        with closing(
            JournalStore(connection_factory=resources.connect, on_storage_open=resources.retain)
        ) as journal:
            with closing(journal.db.execute("PRAGMA cache_size")) as cursor:
                assert cursor.fetchone() == (-cache_kib,)
            with closing(journal.db.execute("PRAGMA temp_store")) as cursor:
                assert cursor.fetchone() == (1,)


@pytest.mark.parametrize("payload", ['{"cache_kib":true}', '{"cache_kib":-1}'])
def test_invalid_cli_cache_target_precedes_state_and_remote_work(tmp_path, monkeypatch, payload):
    root = tmp_path / "source"
    root.mkdir()
    (root / "member").write_bytes(b"exact original")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv(
        "A_RIVERHOG_CLI_PRIMARY_STATE",
        json.dumps({"scope": {"service": "fixture-service", "account": "fixture-account"}}),
    )

    def forbidden(*args, **kwargs):
        pytest.fail("invalid cache target reached state or remote work")

    monkeypatch.setattr(main, "prepare_primary_upload", forbidden)
    monkeypatch.setattr(main, "client", forbidden)
    result = CliRunner().invoke(
        main.app, ["collection", "upload", "start", str(root), "--sqlite-resources", payload]
    )
    assert result.exit_code == 2 and not (tmp_path / "state").exists()


@pytest.mark.skipif(os.name != "posix", reason="actual CLI upload retains POSIX identity/primary")
def test_cli_upload_cache_selection_reaches_authoring_and_keeps_resume_identity(
    tmp_path, monkeypatch
):
    from tests.unit.test_incremental_collection_producer import _CustodyApi

    root = tmp_path / "source"
    root.mkdir()
    (root / "member").write_bytes(b"exact original")
    state = tmp_path / "state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    monkeypatch.setenv(
        "A_RIVERHOG_CLI_PRIMARY_STATE",
        json.dumps({"scope": {"service": "fixture-service", "account": "fixture-account"}}),
    )
    owners, pragmas = [], []

    class Resources(UploadSqliteResources):
        def __init__(self, policy):
            super().__init__(policy)
            owners.append(self)

        def connect(self, database, **options):
            db = super().connect(database, **options)
            db.set_trace_callback(
                lambda sql: pragmas.append(sql) if sql.startswith("PRAGMA ") else None
            )
            return db

    api = _CustodyApi()
    monkeypatch.setattr(main, "UploadSqliteResources", Resources)
    monkeypatch.setattr(main, "client", lambda: api)
    monkeypatch.setenv("A_RIVERHOG_CLI_SQLITE_RESOURCES", '{"cache_kib":13}')
    arguments = [
        "collection",
        "upload",
        "start",
        str(root),
        "--idempotency-key",
        "retained",
        "--tag",
        "z",
        "--tag",
        "a",
        "--json",
    ]
    first = CliRunner().invoke(main.app, [*arguments, "--initialize-state"])
    assert first.exit_code == 0, first.exception
    assert api.initial_tags == ("a", "z")
    assert "PRAGMA main.cache_size=-13" in pragmas
    assert "PRAGMA mmap_size=0" in pragmas and "PRAGMA temp_store=FILE" in pragmas
    (identity_path,) = (state / "a-riverhog-cli/uploads").glob("*.json")
    exact_identity = identity_path.read_bytes()
    ids = tuple(api.rows)
    monkeypatch.setenv("A_RIVERHOG_CLI_SQLITE_RESOURCES", '{"cache_kib":31}')
    second = CliRunner().invoke(main.app, arguments)
    assert second.exit_code == 0, second.exception
    assert identity_path.read_bytes() == exact_identity and tuple(api.rows) == ids
    assert api.session_calls == 1 and len(api.journals) == 1
    assert all(owner.status.connections == 0 for owner in owners)

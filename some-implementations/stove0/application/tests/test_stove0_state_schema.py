from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from stove0_api.app import main
from stove0_core import SqlAlchemyStateStore, stove0_state_schema


def test_stove0_state_upgrade_establishes_exact_current_v1_schema(tmp_path: Path) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'stove0.sqlite3'}"
    schema = stove0_state_schema(database_url)

    upgraded = schema.upgrade()
    verified = schema.validate()
    store = SqlAlchemyStateStore(database_url, initialize=False)
    try:
        assert upgraded.condition == "current"
        assert verified == upgraded
        assert store.list_work()["work"] == []
        assert store.list_evaluations()["evaluations"] == []
    finally:
        store.engine.dispose()


def test_state_cli_enforces_the_postgresql_deployment_boundary(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    database_url = f"sqlite+pysqlite:///{tmp_path / 'stove0.sqlite3'}"
    database_url_file = tmp_path / "database-url"
    database_url_file.write_text(database_url, encoding="utf-8")
    config = tmp_path / "stove0.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "database_url_file": str(database_url_file),
                "riverhog_base_url": "https://riverhog.invalid",
                "riverhog_token_file": str(tmp_path / "riverhog-token"),
                "browse_token_signing_key_file": str(tmp_path / "browse-key"),
                "declared_workspace_protection": "encrypted-at-rest",
                "recipes": {"format": "stove0-recipes/v1", "operations": [], "recipes": []},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("STOVE0_CONFIG", str(config))

    with pytest.raises(ValueError, match="Stove0 database URL must use postgresql"):
        main(["state", "upgrade", "--json"])

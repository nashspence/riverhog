from __future__ import annotations

import importlib.metadata

from a_riverhog_cli.main import app
from typer.testing import CliRunner


def test_cli_version_matches_the_installed_a_riverhog_cli_distribution() -> None:
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout == f"{importlib.metadata.version('a-riverhog-cli')}\n"

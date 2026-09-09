from __future__ import annotations

import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

from scripts.release_installation import INSTALLATION_ROOTS

REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASE = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
CLASSIFIED_PROJECTS = {
    tomllib.loads((REPO_ROOT / path / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "name"
    ]: path
    for paths in RELEASE["python"].values()
    for path in paths
}
END_USER_PROJECTS = tuple(CLASSIFIED_PROJECTS[name] for name in INSTALLATION_ROOTS)


def _entrypoints() -> tuple[str, ...]:
    result: list[str] = []
    for project_path in END_USER_PROJECTS:
        project = tomllib.loads(
            (REPO_ROOT / project_path / "pyproject.toml").read_text(encoding="utf-8")
        )
        result.extend(project["project"]["scripts"])
    return tuple(sorted(result))


@pytest.mark.parametrize("command", _entrypoints())
def test_end_user_entrypoint_loads_natively(command: str) -> None:
    executable = shutil.which(command)

    assert executable is not None
    completed = subprocess.run(
        [executable, "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert "help" in completed.stdout.casefold()


def test_end_user_platforms_are_qualified_by_this_ci_matrix() -> None:
    assert RELEASE["platforms"]["end_user_artifacts"] == [
        "linux-x64",
        "macos-arm64",
        "windows-x64",
    ]

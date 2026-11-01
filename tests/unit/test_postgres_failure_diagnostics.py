from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PREFIX = "::error title=PostgreSQL qualification::"


def _run(tmp_path, phase):
    owner = tmp_path / "test_postgres_concurrency.sh"
    owner.write_bytes((ROOT / "scripts/test_postgres_concurrency.sh").read_bytes())
    (tmp_path / "_compose_env.sh").write_text(
        """
if [[ "$FAIL_PHASE" == "initialization" ]]; then return 23; fi
setup_test_compose_project() { :; }
configure_compose_tty() { COMPOSE_RUN_TTY_ARGS=(); }
ensure_compose_image() { [[ "$FAIL_PHASE" != "image" ]] || return 23; }
compose_env_value() { printf private-payload; }
compose() {
  case "$1" in
    up) [[ "$FAIL_PHASE" != "database" ]] || return 23 ;;
    run) [[ "$FAIL_PHASE" != "tests" ]] || return 23 ;;
    down) [[ "$FAIL_PHASE" != "cleanup" ]] || return 23 ;;
  esac
  return 0
}
"""
    )
    binary = tmp_path / "bin"
    binary.mkdir()
    mise = binary / "mise"
    mise.write_text('#!/usr/bin/env bash\n[[ "$FAIL_PHASE" != "roundtrip" ]] || exit 23\n')
    mise.chmod(0o755)
    result = subprocess.run(
        ["bash", str(owner)],
        env={
            **os.environ,
            "PATH": str(binary) + os.pathsep + os.environ["PATH"],
            "FAIL_PHASE": phase,
            "POSTGRES_TESTS": "",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert "private-payload" not in result.stdout + result.stderr
    annotations = [
        json.loads(line.removeprefix(PREFIX))
        for line in result.stderr.splitlines()
        if line.startswith(PREFIX)
    ]
    return result, annotations


@pytest.mark.parametrize(
    "phase", ["initialization", "image", "database", "tests", "roundtrip", "cleanup"]
)
def test_postgres_failure_retains_phase_and_exit_without_command_or_database_values(
    tmp_path, phase
):
    result, annotations = _run(tmp_path, phase)
    assert result.returncode == 23
    assert annotations == [
        {
            "format": "riverhog-postgres-failure-diagnostic/v1",
            "phase": phase,
            "exit_status": 23,
        }
    ]


def test_successful_postgres_qualification_emits_no_failure_annotation(tmp_path):
    result, annotations = _run(tmp_path, "success")
    assert result.returncode == 0 and result.stderr == ""
    assert not annotations

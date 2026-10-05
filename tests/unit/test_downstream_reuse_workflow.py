# SPDX-FileCopyrightText: 2026 Nash Spence
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / ".github/workflows/downstream-reuse-watch.yml"


def workflow() -> dict[str, Any]:
    return yaml.load(PATH.read_text(), Loader=yaml.BaseLoader)


def test_watch_is_opt_in_upstream_default_branch_only_not_a_commit_gate() -> None:
    value = workflow()
    assert set(value["on"]) == {"schedule", "workflow_dispatch"}
    assert value["on"]["schedule"] == [{"cron": "23 7 * * 3"}]
    job = value["jobs"]["observe"]
    for guard in ("github.repository == 'nashspence/riverhog'", "github.ref == 'refs/heads/main'",
                  "vars.REUSE_WATCH_ENABLED == 'true'"):
        assert guard in job["if"]
    assert int(job["timeout-minutes"]) <= 15
    for name in ("ci.yml", "release-qualification.yml"):
        assert "downstream-reuse-watch" not in (PATH.parent / name).read_text()
    assert "downstream-reuse-watch" not in (ROOT / "release.toml").read_text()


def test_all_actions_are_pinned_and_search_token_has_one_step_scope() -> None:
    value = workflow()
    job = value["jobs"]["observe"]
    assert value["permissions"] == {"contents": "read"}
    assert job["permissions"] == {"contents": "read", "id-token": "write", "attestations": "write"}
    steps = job["steps"]
    for step in steps:
        if "uses" in step:
            assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"])
    assert steps[0]["with"]["persist-credentials"] == "false"
    assert "env" not in value and "env" not in job
    token_steps = [s["name"] for s in steps if "REUSE_WATCH_SEARCH_TOKEN" in s.get("env", {})]
    assert token_steps == [
        "Collect and encrypt observations",
    ]


def test_only_exact_ciphertext_is_attested_then_uploaded() -> None:
    steps = workflow()["jobs"]["observe"]["steps"]
    collect, attest, upload, status = steps[2:]
    assert collect["id"] == "collect" and "--output" in collect["run"]
    assert attest["uses"] == "actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6"
    assert attest["with"]["show-summary"] == "false"
    exact = "${{ runner.temp }}/reuse-watch-upload/observations.age"
    assert attest["with"]["subject-path"] == upload["with"]["path"] == exact
    assert "if" not in attest and "if" not in upload  # Never publish after failed prerequisites.
    assert upload["with"]["retention-days"] == "30"
    assert upload["with"]["if-no-files-found"] == "error"
    expected_name = "reuse-observations-${{ github.run_id }}-${{ github.run_attempt }}"
    assert upload["with"]["name"] == expected_name
    assert status["if"] == "steps.collect.outputs.coverage != 'complete'"
    assert "exit 1" in status["run"]
    assert not any("cache" in step.get("uses", "") for step in steps)

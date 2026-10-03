from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from contract_atlas.model import ContractAtlasError, canonical_bytes  # noqa: E402
from contract_atlas.release_publication import collect_qualification  # noqa: E402
from contract_atlas.workflow_evidence import source_workflow_checks  # noqa: E402

from tests.actions_evidence import ArtifactRemote, execution  # noqa: E402


@pytest.fixture
def qualification_remote(tmp_path):
    folder = tmp_path / "qualification"
    folder.mkdir()
    (folder / "qualification.json").write_bytes(
        canonical_bytes(
            {
                "format": "riverhog-release-qualification/v1",
                "source_sha": "1" * 40,
                "version": "1.0.0",
                "qualification_mode": "prospective",
                "published": False,
                "required_checks": "passed",
                "operation_matrix": "passed",
                "database_contract": "passed",
                "release_evidence": "passed",
                "github_governance": "actions-observable-passed",
                "workflow_execution": execution("qualification"),
            }
        )
    )
    remote = ArtifactRemote(tmp_path / "api")
    remote.register("qualification", folder)
    return remote


@pytest.mark.parametrize(
    "field,value",
    [
        ("event", "pull_request"),
        ("head_repository", {"id": 99, "full_name": "outsider/riverhog"}),
        ("repository", {"id": 99, "full_name": "other/riverhog"}),
        ("head_branch", "untrusted"),
        ("head_sha", "4" * 40),
        ("workflow_id", 999),
        ("path", ".github/workflows/pretend.yml"),
        ("run_attempt", 0),
        ("conclusion", "failure"),
        ("status", "in_progress"),
    ],
)
def test_forged_payload_cannot_replace_trusted_workflow_authority(
    qualification_remote, tmp_path, field, value
):
    remote = qualification_remote
    remote.runs[11][field] = value
    with pytest.raises(ContractAtlasError, match="trusted successful"):
        collect_qualification(remote, 11, "1" * 40, "1.0.0", tmp_path / "collected")
    assert not remote.downloads


@pytest.mark.parametrize(
    "field,value",
    [
        ("digest", "sha256:" + "0" * 64),
        ("expired", True),
        ("name", "release-qualification-" + "1" * 40 + "-attempt-0"),
        ("workflow_run", {"id": 999}),
    ],
)
def test_artifact_identity_and_attempt_are_verified(qualification_remote, tmp_path, field, value):
    qualification_remote.artifacts[11][field] = value
    with pytest.raises(ContractAtlasError):
        collect_qualification(qualification_remote, 11, "1" * 40, "1.0.0", tmp_path / "collected")
    assert not (tmp_path / "collected").exists()


def test_latest_attempt_cannot_reuse_an_earlier_artifact(qualification_remote, tmp_path):
    qualification_remote.runs[11]["run_attempt"] = 2
    with pytest.raises(ContractAtlasError, match="one exact artifact"):
        collect_qualification(qualification_remote, 11, "1" * 40, "1.0.0", tmp_path / "collected")


@pytest.mark.parametrize("source,version", [("4" * 40, "1.0.0"), ("1" * 40, "1.1.0")])
def test_expected_product_source_is_separate_from_workflow_revision(
    qualification_remote, tmp_path, source, version
):
    with pytest.raises(ContractAtlasError):
        collect_qualification(qualification_remote, 11, source, version, tmp_path / "collected")


def test_collection_rechecks_attempt_after_download(qualification_remote, tmp_path, monkeypatch):
    download = qualification_remote.download_workflow_artifact

    def rerun(artifact_id, destination):
        download(artifact_id, destination)
        qualification_remote.runs[11]["run_attempt"] = 2

    monkeypatch.setattr(qualification_remote, "download_workflow_artifact", rerun)
    with pytest.raises(ContractAtlasError, match="attempt changed"):
        collect_qualification(qualification_remote, 11, "1" * 40, "1.0.0", tmp_path / "collected")
    assert not (tmp_path / "collected").exists()


class _SourceRuns:
    repository = "nashspence/riverhog"

    def __init__(self):
        self.runs = {}
        self.checks = {}
        for name, workflow_id in (("ci.yml", 10), ("codeql.yml", 20)):
            for branch, run_id in (("main", workflow_id + 1), ("release/v1", workflow_id + 2)):
                self.add(name, workflow_id, branch, run_id)

    def add(self, name, workflow_id, branch, run_id):
        self.runs[run_id] = {
            "id": run_id,
            "workflow_id": workflow_id,
            "path": ".github/workflows/" + name,
            "event": "push",
            "head_branch": branch,
            "head_sha": "1" * 40,
            "repository": {"id": 42, "full_name": self.repository},
            "head_repository": {"id": 42, "full_name": self.repository},
            "run_attempt": 1,
            "check_suite_id": run_id + 100,
            "status": "completed",
            "conclusion": "success",
        }
        self.checks[run_id + 100] = [
            {"name": context, "app": {"id": 15368}, "conclusion": "success", "status": "completed"}
            for context in ("Analyze (actions)", "Analyze (python)")
        ]

    def api(self, endpoint):
        if endpoint.startswith("check-suites/"):
            return {"check_runs": copy.deepcopy(self.checks[int(endpoint.split("/")[1])])}
        if endpoint.startswith("actions/runs/"):
            return copy.deepcopy(self.runs[int(endpoint.rsplit("/", 1)[1])])
        name = endpoint.split("/")[2]
        workflow_id = 10 if name == "ci.yml" else 20
        if "/runs?" in endpoint:
            # Include other branch successes to witness correct run selection.
            return {
                "workflow_runs": [
                    copy.deepcopy(run)
                    for run in self.runs.values()
                    if run["workflow_id"] == workflow_id
                ]
            }
        return {"id": workflow_id, "path": ".github/workflows/" + name}


def test_both_protected_branches_can_qualify_the_same_sha():
    remote = _SourceRuns()
    assert source_workflow_checks(remote, "1" * 40, "main", ("codeql.yml",))
    assert source_workflow_checks(remote, "1" * 40, "release/v1", ("codeql.yml",))


@pytest.mark.parametrize("status,conclusion", [("in_progress", None), ("completed", "failure")])
def test_pages_waits_for_other_prerequisite_and_never_accepts_old_success(status, conclusion):
    remote = _SourceRuns()
    remote.add("ci.yml", 10, "main", 100)
    remote.runs[100].update(status=status, conclusion=conclusion)
    assert not source_workflow_checks(remote, "1" * 40, "main", ("ci.yml", "codeql.yml"))


@pytest.mark.parametrize("change", ["foreign-app", "failed", "missing"])
def test_selected_codeql_attempt_requires_its_own_successful_checks(change):
    remote = _SourceRuns()
    checks = remote.checks[121]
    if change == "foreign-app":
        checks[0]["app"]["id"] = 999
    elif change == "failed":
        checks[0]["conclusion"] = "failure"
    else:
        checks.pop()
    assert not source_workflow_checks(remote, "1" * 40, "main", ("codeql.yml",))


def test_trusted_qualification_preserves_verified_bytes(qualification_remote, tmp_path):
    record = collect_qualification(
        qualification_remote, 11, "1" * 40, "1.0.0", tmp_path / "collected"
    )
    assert json.loads(record.read_bytes())["workflow_execution"]["workflow_sha"] == "3" * 40
    assert json.loads(record.read_bytes())["source_sha"] == "1" * 40

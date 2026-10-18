"""Disposable Actions API and exact artifact bytes for publication witnesses."""

from __future__ import annotations

import json
import shutil
import zipfile

from contract_atlas.github_publication import GitHubPublication
from contract_atlas.model import canonical_bytes
from contract_atlas.publication import file_sha256
from contract_atlas.workflow_evidence import WORKFLOWS


def execution(kind, *, run_id=None, attempt=1, source_sha="1" * 40, version="1.0.0"):
    path = ".github/workflows/" + WORKFLOWS[kind]
    return {
        "format": "riverhog-workflow-execution/v1",
        "repository": "nashspence/riverhog",
        "workflow": path,
        "workflow_ref": "nashspence/riverhog/" + path + "@refs/heads/main",
        "workflow_sha": "3" * 40,
        "event": "workflow_dispatch",
        "run_id": run_id or (11 if kind == "qualification" else 10),
        "run_attempt": attempt,
        "source_sha": source_sha,
        "version": version,
    }


class ArtifactRemote(GitHubPublication):
    def __init__(self, scratch):
        super().__init__("nashspence/riverhog")
        self.scratch = scratch
        scratch.mkdir(parents=True, exist_ok=True)
        self.runs = {}
        self.artifacts = {}
        self.downloads = []
        self.workflows = {
            kind: {"id": number, "path": ".github/workflows/" + name}
            for number, (kind, name) in enumerate(WORKFLOWS.items(), 100)
        }

    def register(
        self, kind, directory, *, run_id=None, attempt=1, source_sha="1" * 40, version="1.0.0"
    ):
        record = execution(
            kind, run_id=run_id, attempt=attempt, source_sha=source_sha, version=version
        )
        run_id = record["run_id"]
        (directory / "execution.json").write_bytes(canonical_bytes(record))
        path = self.scratch / f"{run_id}.zip"
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            for file in directory.rglob("*"):
                if file.is_file():
                    archive.write(file, file.relative_to(directory).as_posix())
        run = {
            "id": run_id,
            "path": record["workflow"],
            "workflow_id": self.workflows[kind]["id"],
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_dispatch",
            "head_branch": "main",
            "head_sha": record["workflow_sha"],
            "repository": {"full_name": self.repository, "id": 42},
            "head_repository": {"full_name": self.repository, "id": 42},
            "run_attempt": attempt,
        }
        self.runs[run_id] = run
        self.artifacts[run_id] = {
            "id": 1000 + run_id,
            "name": f"release-{kind}-{source_sha}-attempt-{attempt}",
            "digest": "sha256:" + file_sha256(path),
            "size_in_bytes": path.stat().st_size,
            "expired": False,
            "workflow_run": {
                "id": run_id,
                "repository_id": 42,
                "head_repository_id": 42,
                "head_branch": "main",
                "head_sha": record["workflow_sha"],
            },
        }

    def api(self, endpoint):
        if endpoint == "git/ref/heads/main":
            return {"object": {"sha": "3" * 40}}
        if endpoint.startswith("actions/workflows/"):
            name = endpoint.rsplit("/", 1)[1]
            return self.workflows[next(kind for kind, value in WORKFLOWS.items() if value == name)]
        run_id = int(endpoint.split("/")[2])
        if "/artifacts?" in endpoint:
            return {"total_count": 1, "artifacts": [self.artifacts[run_id]]}
        return json.loads(json.dumps(self.runs[run_id]))

    def download_workflow_artifact(self, artifact_id, destination):
        run_id = artifact_id - 1000
        self.downloads.append(artifact_id)
        shutil.copyfile(self.scratch / f"{run_id}.zip", destination)

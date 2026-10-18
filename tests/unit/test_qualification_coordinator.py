from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tomllib
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_new_coordinator_records_a_source_without_its_evidence_helper(
    tmp_path, generated_contract_closure
):
    # This isolates coordinator placement; it does not claim full product qualification.
    source = tmp_path / "source"
    source.mkdir()
    tools = tomllib.loads((REPO_ROOT / "mise.toml").read_text())["tools"]
    (source / "mise.toml").write_text(
        "[tools]\n"
        + "\n".join(f"{name} = {json.dumps(tools[name])}" for name in ("python", "uv"))
        + "\n[settings]\nlockfile = true\n"
    )
    shutil.copyfile(REPO_ROOT / "mise.lock", source / "mise.lock")
    (source / "pyproject.toml").write_text(
        '[project]\nname = "qualification-source-fixture"\nversion = "0.0.0"\n'
        'requires-python = ">=3.12"\ndependencies = []\n[dependency-groups]\ndev = []\n'
    )
    (source / ".gitignore").write_text(".venv/\nbuild/\n")
    (source / "native-value.txt").write_text("selected source contract\n")
    (source / "Makefile").write_text(
        "contract:\n\tmise x -- uv run --locked --group dev python native_contract.py $(args)\n"
    )
    (source / "native_contract.py").write_text(
        "import json, os, shutil, subprocess, sys\nfrom pathlib import Path\n"
        "root = Path.cwd()\n"
        "sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()\n"
        "assert sys.argv[1:] == ['--source-sha', sha]\n"
        "assert (root / 'native-value.txt').read_text() == 'selected source contract\\n'\n"
        "output = root / 'build/contracts'\noutput.mkdir(parents=True)\n"
        "for name in ('riverhog-v1.json', 'riverhog-v1-audit.json'):\n"
        "    shutil.copyfile(Path(os.environ['CONTRACT_FIXTURE_ROOT']) / name, output / name)\n"
        "(root / 'build/native-observation.json').write_text(json.dumps({\n"
        "    'source_sha': sha, 'cwd': str(root), 'python_environment': sys.prefix}))\n"
    )
    environment = {
        **os.environ,
        "MISE_TRUSTED_CONFIG_PATHS": os.pathsep.join((str(REPO_ROOT), str(source))),
    }
    subprocess.run(
        ["mise", "x", "--", "uv", "lock", "--offline"],
        cwd=source,
        env=environment,
        check=True,
        capture_output=True,
    )

    def git(*args):
        return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()

    git("init", "--quiet")
    git("add", ".")
    git(
        "-c",
        "user.name=Qualification fixture",
        "-c",
        "user.email=qualification@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Older source without coordinator helper",
    )
    source_sha = git("rev-parse", "HEAD")
    coordinator_sha = subprocess.check_output(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], text=True
    ).strip()
    assert source_sha != coordinator_sha
    assert not (source / "scripts/workflow_evidence.py").exists()
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    environment.update(
        SOURCE_SHA=source_sha,
        SOURCE_DIR=str(source),
        SOURCE_REF="release/v1",
        RELEASE_VERSION="1.0.0",
        QUALIFICATION_MODE="prospective",
        QUALIFICATION_DIR=str(evidence),
        CONTRACT_FIXTURE_ROOT=str(generated_contract_closure["root"]),
        GITHUB_REPOSITORY="nashspence/riverhog",
        GITHUB_WORKFLOW_REF=(
            "nashspence/riverhog/.github/workflows/release-qualification.yml@refs/heads/main"
        ),
        GITHUB_WORKFLOW_SHA=coordinator_sha,
        GITHUB_REF="refs/heads/main",
        GITHUB_EVENT_NAME="workflow_dispatch",
        GITHUB_RUN_ID="11",
        GITHUB_RUN_ATTEMPT="2",
    )
    for name in ("operations", "database", "release"):
        path = evidence / f"{name}.json"
        path.write_text(json.dumps({"fixture": name}))
        environment[f"{name.upper()}_SUMMARY"] = str(path)
    workflow = yaml.load(
        (REPO_ROOT / ".github/workflows/release-qualification.yml").read_text(),
        Loader=yaml.BaseLoader,
    )["jobs"]["release-audit"]
    directories = {"source": source, "coordinator": REPO_ROOT}
    for name in ("Generate exact-source Closure and Audit", "Record the completed qualification"):
        step = next(step for step in workflow["steps"] if step["name"] == name)
        directory = step.get("working-directory", workflow["defaults"]["run"]["working-directory"])
        completed = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step["run"]],
            cwd=directories[directory],
            env=environment,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
    observation = json.loads((source / "build/native-observation.json").read_bytes())
    assert observation == {
        "source_sha": source_sha,
        "cwd": str(source),
        "python_environment": str(source / ".venv"),
    }
    record = json.loads((evidence / "qualification.json").read_bytes())
    assert record["workflow_execution"]["workflow_sha"] == coordinator_sha
    assert record["source_sha"] == record["workflow_execution"]["source_sha"] == source_sha
    for field, name in (
        ("contract_closure_sha256", "riverhog-v1.json"),
        ("contract_audit_sha256", "riverhog-v1-audit.json"),
    ):
        payload = (source / "build/contracts" / name).read_bytes()
        assert record[field] == hashlib.sha256(payload).hexdigest()
    assert git("status", "--porcelain") == ""

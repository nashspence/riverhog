from __future__ import annotations

import hashlib
import importlib
import os
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import yaml


@pytest.fixture(scope="session")
def generated_contract_closure(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """Generate native records once, without a checked-product bootstrap dependency."""

    repo_root = Path(__file__).resolve().parents[1]
    scripts = repo_root / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    generation = importlib.import_module("contract_atlas.generation")
    candidate = generation.build_candidate()
    output = tmp_path_factory.mktemp("generated-contract")
    candidate.write(output)
    return {
        "discovered": candidate.discovered,
        "bundle": candidate.bundle,
        "projection": candidate.projection,
        "trace": candidate.trace,
        "candidate": candidate,
        "root": output,
    }


@pytest.fixture(scope="session")
def documented_source_plan(generated_contract_closure):
    """Complete synthetic prose selects the real staged-source dependency owners."""
    from contract_atlas.documentation import AuthoredDocumentation
    from contract_atlas.documentation_preparation import preparation_plan

    from tests.documentation_fixtures import synthetic_corpus

    code = generated_contract_closure["bundle"].closure
    authored = AuthoredDocumentation("v1.0.0", "b" * 40, synthetic_corpus(code))
    compiled, requirements = authored.compile(code, final=True)
    return preparation_plan(
        Path(__file__).resolve().parents[1], code, compiled, requirements, authored.tag
    )


@pytest.fixture(scope="session")
def release_contract_factory(generated_contract_closure, tmp_path_factory):
    """Isolated release witnesses reuse discovery and exercise native rendering/binding."""

    import contract_freeze
    from contract_atlas.documentation import DOCUMENTATION_FILENAME
    from contract_atlas.documentation_audit import build_audit, build_snapshot
    from contract_atlas.documentation_native import expected_outputs, native_slots
    from contract_atlas.html_rendering import render_contract
    from contract_atlas.model import canonical_bytes

    original = generated_contract_closure["candidate"]

    def create(source_sha="1" * 40, *, documentation=None, preparation=None):
        document = binding = None
        documentation_audit = None
        extras = {}
        if documentation is not None:
            document, binding = documentation.bind(
                original.bundle.closure,
                original.bundle.audit,
                contract_freeze._cli_parsers(),
                source_sha,
            )
            slots = native_slots(document["compiled"], document["requirements"], documentation.tag)
            expected = expected_outputs(slots)
            from tests.documentation_fixtures import prepared_readout_model

            readouts, artifacts = prepared_readout_model(
                original.bundle.closure, slots, expected, documentation
            )
            artifacts.update(source_sha=source_sha, compiler={"fixture": "synthetic"})
            documentation_audit = build_audit(
                build_snapshot(
                    original.bundle.closure,
                    document["compiled"],
                    document["requirements"],
                    identity={
                        "source_sha": source_sha,
                        "documentation_commit": documentation.commit,
                        "tag": documentation.tag,
                        "compiler": {"fixture": "synthetic"},
                    },
                    expected=expected,
                    observed=expected,
                    artifacts=artifacts,
                ),
                stage="prepared",
                initial=True,
            )
            binding["documentation_audit_sha256"] = hashlib.sha256(
                canonical_bytes(documentation_audit)
            ).hexdigest()
            extras = {
                **readouts,
                DOCUMENTATION_FILENAME: documentation.payload,
                "documentation-record.json": canonical_bytes(document),
                "documentation-audit.json": canonical_bytes(documentation_audit),
                **{
                    "documentation-source/" + name: raw for name, raw in documentation.files.items()
                },
            }
        files = {
            **{
                name: value
                for name, value in original.files.items()
                if not name.startswith("riverhog-v1/")
            },
            **extras,
            **render_contract(
                original.bundle.closure,
                original.bundle.audit,
                document,
                source_revision=source_sha,
                documentation_audit=documentation_audit,
            ),
        }
        manifest = {
            **original.manifest,
            "source_sha": source_sha,
            "build_scope": "revision",
            "documentation": binding,
            "preparation": preparation,
            "files": {name: hashlib.sha256(value).hexdigest() for name, value in files.items()},
        }
        candidate = replace(original, files=files, manifest=manifest)
        root = tmp_path_factory.mktemp("synthetic-release-contract")
        candidate.write(root)
        return root

    return create


@pytest.fixture(autouse=True)
def _explicit_riverhog_test_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Give isolated tests an explicit disposable config document."""

    if os.environ.get("RIVERHOG_CONFIG"):
        return
    secrets = {
        "database-url": "postgresql+psycopg://riverhog:riverhog@127.0.0.1:5432/riverhog",
        "bootstrap-token": "riverhog-pytest-bootstrap-token",
        "browse-key": "riverhog-pytest-browse-token-signing-key-v1",
        "archive-passphrase": "riverhog-pytest-archive-passphrase",
        "adapter-token": "riverhog-pytest-adapter-token",
    }
    for name, value in secrets.items():
        (tmp_path / name).write_text(value + "\n", encoding="utf-8")
    config = {
        "database_url_file": str(tmp_path / "database-url"),
        "bootstrap_token_file": str(tmp_path / "bootstrap-token"),
        "browse_token_signing_key_file": str(tmp_path / "browse-key"),
        "archive_passphrase_files": {
            "riverhog-pytest-key-v1": str(tmp_path / "archive-passphrase")
        },
        "archive_active_passphrase_id": "riverhog-pytest-key-v1",
        "archive_write_store": "archive",
        "archive_stores": {
            "archive": {
                "base_url": "https://archive.invalid",
                "token_file": str(tmp_path / "adapter-token"),
            }
        },
    }
    path = tmp_path / "riverhog.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("RIVERHOG_CONFIG", str(path))


@pytest.fixture(scope="session")
def executed_raw_source_growth(tmp_path_factory):
    """Run the maintained native owner witness in an isolated clean source checkout."""
    import json
    import shutil
    import subprocess

    from riverhog_canonical_json import canonical_json_bytes

    repo_root = Path(__file__).resolve().parents[1]
    temporary = tmp_path_factory.mktemp("raw-source-growth")
    root, output = temporary / "source", temporary / "observations"
    root.mkdir()
    for owner in ("riverhog-protocol", "riverhog-client"):
        relative = Path("packages") / owner / "src"
        shutil.copytree(
            repo_root / relative, root / relative, ignore=shutil.ignore_patterns("__pycache__")
        )
    relative = Path("tests/unit/test_raw_ingress_manifest.py")
    (root / relative).parent.mkdir(parents=True)
    shutil.copy2(repo_root / relative, root / relative)
    (root / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n")
    (root / "pytest.ini").write_text(
        "[pytest]\nmarkers =\n    growth_witness(identity, gates, operation): owner observations\n"
    )
    for command in (
        ["git", "init", "-q"],
        ["git", "add", "."],
        [
            "git",
            "-c",
            "user.name=Example",
            "-c",
            "user.email=example@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-qm",
            "Define native owner witness",
        ],
    ):
        subprocess.run(command, cwd=root, check=True, capture_output=True)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            (
                str(root / "packages/riverhog-protocol/src"),
                str(root / "packages/riverhog-client/src"),
                str(repo_root),
            )
        ),
        "RIVERHOG_CI_RUN_ID": "native-owner-execution",
        "RIVERHOG_CI_LANE": "unit",
        "RIVERHOG_CI_TIMING_DIR": str(output),
    }
    observed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "scripts.ci_timing"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert observed.returncode == 0, observed.stdout + observed.stderr
    evidence_path = output / "growth.json"
    # Resolve and verify against the same checkout that actually executed; the
    # parent interpreter retains its own workspace imports.
    collect = """
import sys
from pathlib import Path
from riverhog_canonical_json import canonical_json_bytes
from scripts.validation_witnesses import collect_test_witnesses
root, output = Path(sys.argv[1]), Path(sys.argv[2])
source, destination = sys.argv[3], Path(sys.argv[4])
evidence = []
results = collect_test_witnesses(
    tuple((output / "pytest").glob("*/native-owner-execution/*/*.json")), root=root,
    expected=("riverhog-raw-source-primitive-work/v1",), source_sha=source,
    run_id="native-owner-execution", evidence=evidence,
)
assert results == {"riverhog-raw-source-primitive-work/v1": "pass"}
destination.write_bytes(canonical_json_bytes(evidence))
"""
    collected = subprocess.run(
        [sys.executable, "-c", collect, str(root), str(output), source, str(evidence_path)],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert collected.returncode == 0, collected.stdout + collected.stderr
    records = json.loads(evidence_path.read_bytes())
    assert len(records) == 1
    assert canonical_json_bytes(records) == evidence_path.read_bytes()
    return {
        "source": {"source_sha": source, "source_clean": True},
        "records": records,
        "root": root,
    }

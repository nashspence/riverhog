from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import tomllib
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = REPO_ROOT / ".github/workflows/ci.yml"
CODEQL_WORKFLOW = REPO_ROOT / ".github/workflows/codeql.yml"
QUALIFICATION_WORKFLOW = REPO_ROOT / ".github/workflows/release-qualification.yml"
PROVIDER_QUALIFICATION_WORKFLOW = REPO_ROOT / ".github/workflows/provider-qualification.yml"
CONTRACT_PAGES_WORKFLOW = REPO_ROOT / ".github/workflows/contract-pages.yml"
PREPARATION_WORKFLOW = REPO_ROOT / ".github/workflows/release-preparation.yml"
DOCUMENTATION_WORKFLOW = REPO_ROOT / ".github/workflows/documentation-check.yml"
PROVIDER_QUALIFICATION_COMPOSE = REPO_ROOT / "tests/harness/provider-qualification.compose.yaml"
MISE_LOCK = REPO_ROOT / "mise.lock"
DATABASE_QUALIFICATION_SCRIPT = REPO_ROOT / "scripts/database_qualification.py"
UPLOAD_ARTIFACT_USE = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"


def test_artifact_uploads_use_one_pinned_node24_action() -> None:
    uploads = [
        step["uses"]
        for path in (REPO_ROOT / ".github/workflows").glob("*.yml")
        for job in yaml.load(path.read_text(), Loader=yaml.BaseLoader)["jobs"].values()
        for step in job.get("steps", [])
        if step.get("uses", "").startswith("actions/upload-artifact@")
    ]
    assert uploads
    assert all(use == UPLOAD_ARTIFACT_USE for use in uploads)


def test_contract_pages_uses_exact_green_main_and_protected_publication_environment() -> None:
    workflow = yaml.load(CONTRACT_PAGES_WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    assert set(workflow["on"]) == {"workflow_dispatch", "workflow_run"}
    assert workflow["jobs"]["build"]["permissions"] == {"actions": "read", "contents": "read"}
    build = workflow["jobs"]["build"]["steps"]
    assert all(
        re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"]) for step in build if "uses" in step
    )
    readiness = workflow["jobs"]["readiness"]
    gate = next(
        step for step in readiness["steps"] if step["name"] == "Require a green commit on main"
    )
    assert "git merge-base --is-ancestor" in gate["run"]
    assert "--workflow ci.yml --workflow codeql.yml" in gate["run"]
    assert "--allow-pending --github-output" in gate["run"]
    assert workflow["jobs"]["build"]["if"] == "needs.readiness.outputs.ready == 'true'"
    assert "make contract-check" in next(
        step["run"] for step in build if step["name"].startswith("Generate and independently check")
    )
    deploy = workflow["jobs"]["deploy"]
    assert deploy["needs"] == "build"
    assert deploy["environment"]["name"] == "github-pages"
    assert deploy["permissions"] == {
        "actions": "read",
        "contents": "read",
        "pages": "write",
        "id-token": "write",
    }
    assert workflow["concurrency"] == {"group": "riverhog-pages", "cancel-in-progress": "false"}
    assert any("--check-fresh" in step.get("run", "") for step in deploy["steps"])


def test_every_buildx_setup_uses_the_pinned_docker_engine() -> None:
    for path in (CI_WORKFLOW, QUALIFICATION_WORKFLOW, PROVIDER_QUALIFICATION_WORKFLOW):
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        buildx_steps = [
            step
            for job in workflow["jobs"].values()
            for step in job.get("steps", ())
            if str(step.get("uses", "")).startswith("docker/setup-buildx-action@")
        ]

        assert buildx_steps
        assert all(
            step["with"] == {"version": "v0.36.0", "driver": "docker"} for step in buildx_steps
        )


def test_pages_dispatch_enters_the_environment_through_main_instead_of_a_product_tag() -> None:
    workflow = yaml.load(CONTRACT_PAGES_WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    readiness = workflow["jobs"]["readiness"]
    guard = readiness["steps"][0]["run"]
    assert readiness["permissions"]["checks"] == "read"
    assert "release" not in workflow["on"]
    for ref, status in (("refs/tags/v1.0.0", 1), ("refs/heads/main", 0)):
        observed = subprocess.run(
            ["bash", "-e", "-c", guard],
            env={**os.environ, "GITHUB_REF": ref},
            capture_output=True,
        )
        assert observed.returncode == status
    assert workflow["jobs"]["deploy"]["environment"]["name"] == "github-pages"
    assert workflow["jobs"]["deploy"]["needs"] == "build"


def test_preparation_separates_trusted_authority_from_the_exact_qualified_source() -> None:
    workflow = yaml.load(PREPARATION_WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    assert set(workflow["on"]) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"actions": "read", "contents": "read"}
    job = workflow["jobs"]["prepare"]
    guard = job["steps"][0]["run"]
    for ref, status in (("refs/heads/main", 0), ("refs/tags/v1.0.0", 1)):
        result = subprocess.run(
            ["bash", "-e", "-c", guard],
            env={**os.environ, "GITHUB_REF": ref},
            capture_output=True,
        )
        assert result.returncode == status
    checkouts = [
        step["with"]
        for step in job["steps"]
        if step.get("uses", "").startswith("actions/checkout@")
    ]
    assert [(c["path"], c["ref"]) for c in checkouts] == [
        ("coordinator", "${{ github.workflow_sha }}"),
        ("source", "${{ inputs.source_sha }}"),
    ]
    assert all(c["persist-credentials"] == "false" and c["fetch-depth"] == "0" for c in checkouts)
    preparation = next(step for step in job["steps"] if step.get("working-directory"))
    assert preparation["working-directory"] == "coordinator"
    assert preparation["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "--source-directory ../source" in preparation["run"]
    assert 'test "$(git -C ../source rev-parse HEAD)" = "$SOURCE_SHA"' in preparation["run"]
    assert 'test "$(git rev-parse HEAD)" = "$GITHUB_WORKFLOW_SHA"' in preparation["run"]
    assert "--qualification-run" in preparation["run"]
    assert "secrets." not in PREPARATION_WORKFLOW.read_text()
    assert "environment" not in job
    assert all(
        re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"]) for step in job["steps"] if "uses" in step
    )


def test_documentation_validation_has_read_only_authoring_access() -> None:
    workflow = yaml.load(DOCUMENTATION_WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    assert set(workflow["on"]) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["validate"]
    assert "environment" not in job
    validation = next(step for step in job["steps"] if "run" in step)
    assert validation["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "validate-authoring" in validation["run"]
    assert "--coordinate-baseline" in validation["run"]
    assert "secrets." not in DOCUMENTATION_WORKFLOW.read_text()


def test_prepared_native_extraction_is_called_by_the_trusted_coordinator() -> None:
    tree = ast.parse((REPO_ROOT / "scripts/release.py").read_text())
    preparation = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "build_release_evidence"
    )
    imports = {
        (node.module, alias.name)
        for node in ast.walk(preparation)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert ("contract_atlas.documentation_artifacts", "inspect_prepared_artifacts") in imports
    extractor = [
        node
        for node in ast.walk(preparation)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "inspect_prepared_artifacts"
    ]
    assert len(extractor) == 1
    assert {k.arg for k in extractor[0].keywords} >= {"baseline", "custody", "image_records"}


def test_provider_qualification_runs_isolated_storage_adapter_images() -> None:
    compose = yaml.safe_load(PROVIDER_QUALIFICATION_COMPOSE.read_text(encoding="utf-8"))
    services = compose["services"]

    assert services["aws-deep-archive-adapter"]["image"] == ("a-riverhog-aws-store:dev")
    assert services["b2-archive-adapter"]["image"] == ("a-riverhog-b2-store:dev")
    assert services["b2-retrieval-cache-adapter"]["image"] == ("a-riverhog-b2-store:dev")
    assert services["qualification-filesystem-cache-adapter"]["image"] == (
        "a-riverhog-filesystem-store:dev"
    )
    assert set(services["app"]["depends_on"]) == {
        "aws-deep-archive-adapter",
        "b2-archive-adapter",
        "b2-retrieval-cache-adapter",
        "qualification-filesystem-cache-adapter",
    }
    for name, config_setting, config_host_path, secret_host_path in (
        (
            "aws-deep-archive-adapter",
            "A_RIVERHOG_AWS_STORE_CONFIG",
            "A_RIVERHOG_AWS_STORE_CONFIG_HOST_PATH",
            "A_RIVERHOG_AWS_STORE_ACCESS_KEY_ID_HOST_PATH",
        ),
        (
            "b2-archive-adapter",
            "A_RIVERHOG_B2_STORE_CONFIG",
            "A_RIVERHOG_B2_ARCHIVE_CONFIG_HOST_PATH",
            "A_RIVERHOG_B2_ARCHIVE_ACCESS_KEY_ID_HOST_PATH",
        ),
        (
            "b2-retrieval-cache-adapter",
            "A_RIVERHOG_B2_STORE_CONFIG",
            "A_RIVERHOG_B2_CACHE_CONFIG_HOST_PATH",
            "A_RIVERHOG_B2_CACHE_ACCESS_KEY_ID_HOST_PATH",
        ),
    ):
        service = services[name]
        assert service["environment"] == {
            config_setting: "/etc/riverhog/"
            + ("aws-store.yaml" if name == "aws-deep-archive-adapter" else "b2-store.yaml")
        }
        assert "env_file" not in service
        assert (
            f"${{{config_host_path}}}:{service['environment'][config_setting]}:ro"
            in service["volumes"]
        )
        assert any(
            str(volume).startswith(f"${{{secret_host_path}}}:/run/secrets/")
            and str(volume).endswith(":ro")
            for volume in service["volumes"]
        )


def test_codeql_covers_every_governed_branch_with_stable_checks() -> None:
    text = CODEQL_WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)

    assert workflow["on"] == {
        "pull_request": {"branches": ["main", "release/v1"]},
        "push": {"branches": ["main", "release/v1"]},
        "schedule": [{"cron": "41 6 * * 4"}],
        "workflow_dispatch": "",
    }
    assert workflow["permissions"] == {
        "actions": "read",
        "contents": "read",
        "packages": "read",
        "security-events": "write",
    }
    assert workflow["concurrency"] == {
        "group": "codeql-${{ github.workflow }}-${{ github.ref }}",
        "cancel-in-progress": "true",
    }
    assert set(workflow["jobs"]) == {"analyze"}
    job = workflow["jobs"]["analyze"]
    assert job["name"] == "Analyze (${{ matrix.language }})"
    assert job["runs-on"] == "ubuntu-24.04"
    assert job["strategy"] == {
        "fail-fast": "false",
        "matrix": {"language": ["actions", "python"]},
    }
    steps = job["steps"]
    assert [step["uses"].split("@", 1)[0] for step in steps] == [
        "actions/checkout",
        "github/codeql-action/init",
        "github/codeql-action/analyze",
    ]
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"]) for step in steps)
    assert steps[0]["with"] == {"persist-credentials": "false"}
    assert steps[1]["with"] == {
        "languages": "${{ matrix.language }}",
        "queries": "security-extended",
    }
    assert steps[2]["with"] == {"category": "/language:${{ matrix.language }}"}


def test_ci_uses_thin_repository_and_image_build_adapters() -> None:
    text = CI_WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)

    assert workflow["on"] == {
        "pull_request": "",
        "push": {"branches": ["main", "release/v1"]},
        "workflow_dispatch": "",
        "workflow_call": {
            "inputs": {
                "ref": {
                    "description": "Exact commit to check out and validate.",
                    "required": "false",
                    "type": "string",
                }
            }
        },
    }
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["concurrency"]["cancel-in-progress"] == "true"

    assert set(workflow["jobs"]) == {
        "gate",
        "plan",
        "repository",
        "units",
        "compose",
        "client-platforms",
        "images",
    }
    jobs = workflow["jobs"]
    caps = {"repository": "2", "units": "3", "compose": "7", "images": "2", "client-platforms": "3"}
    assert sum(map(int, caps.values())) == 17
    for name, cap in caps.items():
        job = jobs[name]
        assert job["strategy"]["fail-fast"] == "false"
        assert job["strategy"]["max-parallel"] == cap
        if name != "client-platforms":
            assert job["runs-on"] == "ubuntu-24.04"
            assert job["needs"] == "plan"
            assert job["strategy"]["matrix"] == "${{ fromJSON(needs.plan.outputs." + name + ") }}"
    plan = next(step for step in jobs["plan"]["steps"] if step.get("id") == "plan")
    assert 'python -m scripts.ci_qualification plan --github-output "$GITHUB_OUTPUT"' in plan["run"]
    action_steps = [step for job in jobs.values() for step in job["steps"] if "uses" in step]
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"]) for step in action_steps)
    for step in action_steps:
        if step["uses"].startswith("actions/checkout@"):
            assert step["with"] == {
                "persist-credentials": "false",
                "ref": "${{ inputs.ref || github.sha }}",
            }
        if step["uses"].startswith("docker/setup-docker-action@"):
            assert step["with"]["version"] == "v29.3.1"
            assert json.loads(step["with"]["daemon-config"]) == {
                "features": {"containerd-snapshotter": True},
            }
        if step["uses"].startswith("docker/setup-compose-action@"):
            assert step["with"] == {"version": "v5.1.1"}
    repository = jobs["repository"]
    run = next(step for step in repository["steps"] if step["name"] == "Run repository target")
    assert 'python scripts/ci_timing.py run --lane "$CI_TARGET"' in run["run"]
    assert '-- make "$CI_TARGET"' in run["run"]
    for name in ("repository", "units", "compose", "images"):
        retained = jobs[name]["steps"][-1]
        assert retained["if"] == "always()"
        assert retained["uses"] == UPLOAD_ARTIFACT_USE
        assert retained["with"]["retention-days"] == "7"
        assert retained["with"]["path"] == "${{ runner.temp }}/ci-timing"
    unit_run = next(step for step in jobs["units"]["steps"] if "run" in step)
    assert 'make unit-shard UNIT_SHARD="$UNIT_SHARD"' in unit_run["run"]
    compose_run = next(
        step
        for step in jobs["compose"]["steps"]
        if step["name"] == "Qualify the complete independent lifecycle"
    )
    assert 'make compose-shard COMPOSE_LANE="$COMPOSE_LANE"' in compose_run["run"]
    client = jobs["client-platforms"]
    assert client["strategy"]["matrix"] == {"os": ["ubuntu-24.04", "macos-15", "windows-2025"]}
    assert client["env"] == {"MISE_AUTO_INSTALL": "0"}
    assert client["steps"][1]["with"] == {"install_args": "python uv age"}
    native = next(step for step in client["steps"] if "run" in step)
    assert native["run"] == (
        "mise x python uv age -- uv run --locked --all-packages --group dev "
        "python -m scripts.ci_qualification native"
    )
    assert native["env"] == {
        "GOGURT_FAILURE_EVIDENCE_DIR": "${{ runner.temp }}/gogurt-failure-evidence"
    }
    evidence_step = client["steps"][-1]
    assert evidence_step["if"] == "failure()"
    assert evidence_step["with"] == {
        "name": "gogurt-lifecycle-${{ runner.os }}-${{ runner.arch }}-${{ github.sha }}",
        "path": "${{ runner.temp }}/gogurt-failure-evidence",
        "if-no-files-found": "warn",
        "retention-days": "14",
    }

    assert "secrets." not in text


def test_ci_gate_covers_every_job_and_rejects_non_successful_dependencies() -> None:
    workflow = yaml.load(CI_WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    gate = workflow["jobs"]["gate"]
    assert gate["name"] == "CI gate"
    assert gate["if"] == "always()"
    assert gate["runs-on"] == "ubuntu-24.04"
    assert set(gate["needs"]) == set(workflow["jobs"]) - {"gate"}
    step = gate["steps"][0]
    assert step["env"] == {"CI_DEPENDENCIES": "${{ toJSON(needs) }}"}

    def observe(dependencies):
        return subprocess.run(
            ["bash", "-c", step["run"]],
            env={**os.environ, "CI_DEPENDENCIES": json.dumps(dependencies)},
            capture_output=True,
            text=True,
            check=False,
        )

    success = {name: {"result": "success"} for name in gate["needs"]}
    assert observe(success).returncode == 0
    assert observe({}).returncode != 0
    for name in gate["needs"]:
        for result in ("failure", "cancelled", "skipped", None):
            observed = observe({**success, name: {"result": result}})
            assert observed.returncode != 0, (name, result)
            assert name in observed.stderr


def test_release_qualification_reuses_ci_and_publishes_only_sha_bound_summaries() -> None:
    text = QUALIFICATION_WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)

    assert workflow["on"]["schedule"] == [{"cron": "17 7 1,15 * *"}]
    assert workflow["permissions"] == {
        "actions": "read",
        "checks": "read",
        "contents": "read",
        "deployments": "read",
    }
    assert workflow["concurrency"]["cancel-in-progress"] == "false"
    assert set(workflow["jobs"]) == {"resolve", "ci", "release-audit"}
    assert workflow["jobs"]["ci"] == {
        "name": "required checks",
        "needs": "resolve",
        "uses": "./.github/workflows/ci.yml",
        "with": {"ref": "${{ needs.resolve.outputs.sha }}"},
        "permissions": {"contents": "read"},
    }
    audit = workflow["jobs"]["release-audit"]
    assert "environment" not in audit
    assert audit["needs"] == ["resolve", "ci"]
    assert audit["env"]["SOURCE_SHA"] == "${{ needs.resolve.outputs.sha }}"
    assert audit["env"]["SOURCE_REF"] == "${{ needs.resolve.outputs.ref }}"
    assert audit["env"]["QUALIFICATION_MODE"] == "${{ needs.resolve.outputs.mode }}"
    assert audit["env"]["RIVERHOG_RELEASE_GHA_CACHE"] == "true"
    locate_evidence = next(
        step
        for step in audit["steps"]
        if step["name"] == "Locate qualification evidence outside the source tree"
    )
    assert 'qualification_dir="$RUNNER_TEMP/release-qualification"' in locate_evidence["run"]
    assert '>> "$GITHUB_ENV"' in locate_evidence["run"]
    assert "OPERATIONS_SUMMARY" in locate_evidence["run"]
    assert "OPERATIONS_TIMINGS" in locate_evidence["run"]
    assert "DATABASE_SUMMARY" in locate_evidence["run"]
    assert "RELEASE_HISTORY_DIR" in locate_evidence["run"]
    assert "RELEASE_EXPECTED_MANIFEST" in locate_evidence["run"]
    stage_history = next(
        step
        for step in audit["steps"]
        if step["name"] == "Stage immutable v1 release-manifest history"
    )
    assert "gh api --paginate" in stage_history["run"]
    assert 'test("^v1\\\\.[0-9]+\\\\.[0-9]+$")' in stage_history["run"]
    assert "release-manifest.json" in stage_history["run"]
    assert "Accept: application/octet-stream" in stage_history["run"]
    assert "RELEASE_PREVIOUS_TAG" in stage_history["run"]
    assert "RELEASE_PREVIOUS_MANIFEST_SHA256" in stage_history["run"]
    assert 'QUALIFICATION_MODE" == "prospective' in stage_history["run"]
    assert 'QUALIFICATION_MODE" == "historical' in stage_history["run"]
    assert "does not follow current v1 head" in stage_history["run"]
    assert "published-candidate.json" in stage_history["run"]
    assert "Historical predecessor digest differs" in stage_history["run"]
    assert "canonical_bytes(json.loads(payload))" in stage_history["run"]
    assert "gh release verify-asset" in stage_history["run"]
    lifecycle_evidence = next(
        step
        for step in audit["steps"]
        if step["name"] == "Exercise disposable operation lifecycles and record timings"
    )
    assert "test_operation_lifecycle_api.py" in lifecycle_evidence["run"]
    assert "test_stove0_api_parity.py" in lifecycle_evidence["run"]
    assert "test_ftp_spool_api_parity.py" in lifecycle_evidence["run"]
    assert "test_collection_reads.py" in lifecycle_evidence["run"]
    assert "test_unified_state_store_is_restart_safe" in lifecycle_evidence["run"]
    assert "test_unified_evaluation_store_is_restart_safe" in lifecycle_evidence["run"]
    assert (
        "test_scheduler_uses_bounded_keyset_scans_and_rotates_past_a_permanent_noop"
        in lifecycle_evidence["run"]
    )
    assert "test_landing_adapter_reconciles_lost_response" in lifecycle_evidence["run"]
    assert "tests.operation_observer" in lifecycle_evidence["run"]
    operation_evidence = next(
        step
        for step in audit["steps"]
        if step["name"] == "Verify and record the complete operation matrix"
    )
    assert "make operation-qualification" in operation_evidence["run"]
    assert "--source-sha $SOURCE_SHA" in operation_evidence["run"]
    assert "--timings $OPERATIONS_TIMINGS" in operation_evidence["run"]
    assert '"$OPERATIONS_SUMMARY.md" >> "$GITHUB_STEP_SUMMARY"' in operation_evidence["run"]
    verify_operations = next(
        step for step in audit["steps"] if step["name"] == "Verify exact-SHA operation evidence"
    )
    assert "riverhog-operation-qualification/v1" in verify_operations["run"]
    assert ".source_sha == $sha" in verify_operations["run"]
    assert "positive_local_lifecycles.status" in verify_operations["run"]
    assert "contract_inputs.status" in verify_operations["run"]
    assert "riverhog-extent-contract/v1" in verify_operations["run"]
    assert "contract_inputs.closure_sha256 == $contract" in verify_operations["run"]
    assert "contract_inputs.audit_sha256 == $audit" in verify_operations["run"]
    assert "contract_inputs.extent_analysis_sha256 == $extent" in verify_operations["run"]
    assert "cli_human_json_projection.status" in verify_operations["run"]
    assert "bounded_state_access.status" in verify_operations["run"]
    assert "event_cursor_restart_resume.status" in verify_operations["run"]
    database_evidence = next(
        step
        for step in audit["steps"]
        if step["name"] == "Qualify exact database schemas, selectors, and bounded pages"
    )
    assert "make database-qualification" in database_evidence["run"]
    assert 'DATABASE_QUALIFICATION_SOURCE_SHA="$SOURCE_SHA"' in database_evidence["run"]
    assert 'DATABASE_QUALIFICATION_OUTPUT="$DATABASE_SUMMARY"' in database_evidence["run"]
    verify_database = next(
        step for step in audit["steps"] if step["name"] == "Verify exact-SHA database evidence"
    )
    database_module = ast.parse(
        DATABASE_QUALIFICATION_SCRIPT.read_text(encoding="utf-8"),
        filename=str(DATABASE_QUALIFICATION_SCRIPT),
    )
    cardinalities = next(
        ast.literal_eval(node.value)
        for node in database_module.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "CARDINALITIES" for target in node.targets
        )
    )
    assert "riverhog-database-qualification/v1" in verify_database["run"]
    assert f".cardinalities == {json.dumps(list(cardinalities))}" in verify_database["run"]
    assert "bounded_page_streams" in verify_database["run"]
    assert "official_client_bounded_pages" in verify_database["run"]
    assert ".page_streams" in verify_database["run"]
    assert "peak_application_bytes" in verify_database["run"]
    assert "cancellation.connection_reusable" in verify_database["run"]
    governance = next(
        step for step in audit["steps"] if step["name"] == "Verify live release governance"
    )
    assert "RELEASE_GOVERNANCE_SCOPE=actions-observable" in governance["run"]
    release_evidence = next(
        step for step in audit["steps"] if step["name"] == "Build and verify release evidence"
    )
    assert "--previous-tag" in release_evidence["run"]
    assert "--previous-manifest-sha256" in release_evidence["run"]
    assert "--history-manifest" in release_evidence["run"]
    assert "--expected-release-manifest" in release_evidence["run"]
    assert 'QUALIFICATION_MODE" == "historical' in release_evidence["run"]
    assert "sort -Vr" in release_evidence["run"]
    verify_summary = next(
        step for step in audit["steps"] if step["name"] == "Verify exact-SHA nonpublication summary"
    )
    assert 'immutable_releases == "operator-preflight-required"' in verify_summary["run"]
    assert ".license_coordinates > 0" in verify_summary["run"]
    assert ".release_history_manifests > 0" in verify_summary["run"]
    resolve_source = next(
        step
        for step in workflow["jobs"]["resolve"]["steps"]
        if step["name"] == "Resolve the selected ref once"
    )
    assert resolve_source["env"]["WORKFLOW_REF"] == "${{ github.ref }}"
    assert resolve_source["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert '[[ "$WORKFLOW_REF" != refs/heads/main ]]' in resolve_source["run"]
    assert "latest_published_tag" in resolve_source["run"]
    assert "mode=prospective" in resolve_source["run"]
    assert "mode=historical" in resolve_source["run"]
    assert '"$ref" != "v$version"' in resolve_source["run"]
    assert "Release-candidate qualification ref and version differ" in resolve_source["run"]
    assert "${BASH_REMATCH[1]}" in resolve_source["run"]
    assert workflow["jobs"]["resolve"]["outputs"]["mode"] == "${{ steps.source.outputs.mode }}"
    audit_checkout = next(
        step for step in audit["steps"] if step["name"] == "Check out workflow authority"
    )
    assert audit_checkout["with"] == {
        "ref": "${{ github.workflow_sha }}",
        "path": "coordinator",
        "fetch-depth": "0",
        "persist-credentials": "false",
    }
    exact_checkout = next(
        step for step in audit["steps"] if step["name"] == "Check out verified exact source"
    )
    assert exact_checkout["with"] == {
        "ref": "${{ needs.resolve.outputs.sha }}",
        "path": "source",
        "fetch-depth": "0",
        "persist-credentials": "false",
    }
    assert audit["defaults"] == {"run": {"working-directory": "source"}}
    assert audit["env"]["SOURCE_DIR"] == "${{ github.workspace }}/source"
    verification = next(step for step in audit["steps"] if step["name"] == "Verify exact checkouts")
    assert verification["run"] == (
        'test "$(git rev-parse --verify HEAD)" = "$SOURCE_SHA"\n'
        'test "$(git -C ../coordinator rev-parse --verify HEAD)" = "$GITHUB_WORKFLOW_SHA"\n'
    )
    toolchains = {
        step["name"]: step["with"]
        for step in audit["steps"]
        if step.get("uses", "").startswith("jdx/mise-action@")
    }
    assert toolchains == {
        "Install workflow verification tools": {
            "working_directory": "coordinator",
            "install_args": "python uv",
        },
        "Install repository toolchain": {"working_directory": "source"},
    }
    assert all(
        re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"])
        for step in workflow["jobs"]["resolve"]["steps"] + audit["steps"]
        if "uses" in step
    )
    upload = next(step for step in audit["steps"] if step["name"] == "Upload SHA-bound summaries")
    record = next(
        step for step in audit["steps"] if step["name"] == "Record the completed qualification"
    )
    assert record["working-directory"] == "coordinator"
    assert '"$SOURCE_DIR/build/contracts/' in record["run"]
    assert '> "$QUALIFICATION_DIR/qualification.json"' in record["run"]
    assert "contract_closure_sha256" in record["run"]
    assert "contract_audit_sha256" in record["run"]
    assert "qualification_mode" in record["run"]
    assert "contract_trace_sha256" in record["run"]
    assert "extent_analysis_sha256" in record["run"]
    assert "operation_evidence_sha256" in record["run"]
    assert "database_evidence_sha256" in record["run"]
    assert "release_evidence_sha256" in record["run"]
    assert upload["uses"].startswith("actions/upload-artifact@")
    assert upload["if"] == "always()"
    assert upload["with"]["path"].splitlines() == [
        "${{ runner.temp }}/release-qualification/*.json",
        "${{ runner.temp }}/release-qualification/*.md",
    ]
    assert "published == false" in text
    assert "riverhog-release-qualification/v1" in text
    assert 'operation_matrix: "passed"' in text
    assert 'database_contract: "passed"' in text
    assert "scripts/workflow_evidence.py checks" in text
    assert '--branch "$branch" --workflow codeql.yml' in text
    assert "-attempt-${{ github.run_attempt }}" in upload["with"]["name"]
    assert 'record["workflow_execution"]' in record["run"]
    assert "release/v1" in text
    assert "v1\\.[0-9]+\\.[0-9]+" in text


def test_release_required_checks_bind_the_exhaustive_ci_gate_and_codeql() -> None:
    workflow = yaml.load(CI_WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    codeql = yaml.load(CODEQL_WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    gate = workflow["jobs"]["gate"]
    assert gate["if"] == "always()"
    assert set(gate["needs"]) == set(workflow["jobs"]) - {"gate"}
    analyze = codeql["jobs"]["analyze"]
    assert analyze["name"] == "Analyze (${{ matrix.language }})"
    actual = {
        gate["name"],
        *(
            analyze["name"].replace("${{ matrix.language }}", language)
            for language in analyze["strategy"]["matrix"]["language"]
        ),
    }
    assert release["governance"]["required_checks"] == sorted(actual)


def test_provider_qualification_is_resumable_dummy_only_and_cloudfront_required() -> None:
    text = PROVIDER_QUALIFICATION_WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)

    def references(job: dict[str, object], context: str) -> set[str]:
        rendered = json.dumps(job)
        return set(re.findall(rf"\$\{{\{{ {context}\.([A-Z0-9_]+) \}}\}}", rendered))

    assert workflow["on"]["schedule"] == [{"cron": "23 1,7,13,19 * * *"}]
    assert set(workflow["on"]["workflow_dispatch"]["inputs"]["corpus_profile"]["options"]) == {
        "regular",
        "resumable",
    }
    assert set(workflow["on"]["workflow_dispatch"]["inputs"]["mode"]["options"]) == {
        "auto",
        "start",
        "poll",
        "restart",
    }
    assert workflow["permissions"] == {
        "actions": "read",
        "contents": "read",
    }
    assert workflow["concurrency"] == {
        "group": "provider-qualification",
        "cancel-in-progress": "false",
    }
    assert set(workflow["jobs"]) == {"resolve", "provision_aws", "qualify"}
    resolve_job = workflow["jobs"]["resolve"]
    provision_job = workflow["jobs"]["provision_aws"]
    job = workflow["jobs"]["qualify"]
    assert "environment" not in resolve_job and "permissions" not in resolve_job
    assert resolve_job["outputs"] == {
        "action": "${{ steps.mode.outputs.action }}",
        "artifact_id": "${{ steps.mode.outputs.artifact_id }}",
        "profile": "${{ steps.mode.outputs.profile }}",
        "source_ref": "${{ steps.mode.outputs.source_ref }}",
        "source_sha": "${{ steps.mode.outputs.source_sha }}",
    }
    assert references(resolve_job, "secrets") == set()

    assert provision_job["needs"] == "resolve"
    assert "needs.resolve.outputs.action == 'start'" in provision_job["if"]
    assert "needs.resolve.outputs.action == 'restart'" in provision_job["if"]
    assert provision_job["environment"] == "provider-qualification-provisioning"
    assert provision_job["permissions"] == {"contents": "read", "id-token": "write"}
    assert references(provision_job, "vars") == {
        "RIVERHOG_QUALIFICATION_AWS_DEEP_ARCHIVE_BUCKET",
        "RIVERHOG_QUALIFICATION_AWS_PROVISION_ROLE_ARN",
        "RIVERHOG_QUALIFICATION_AWS_REGION",
        "RIVERHOG_QUALIFICATION_CLOUDFRONT_PUBLIC_KEY",
    }
    assert references(provision_job, "secrets") == set()
    assert "RIVERHOG_QUALIFICATION_B2_" not in json.dumps(provision_job)

    assert job["needs"] == ["resolve", "provision_aws"]
    assert "needs.resolve.outputs.action != 'skip'" in job["if"]
    assert "needs.provision_aws.result == 'skipped'" in job["if"]
    assert job["environment"] == "provider-qualification"
    assert job["permissions"] == {
        "actions": "read",
        "contents": "read",
        "id-token": "write",
    }
    assert job["runs-on"] == "ubuntu-24.04"
    assert {name for name in job["env"] if name.endswith("_BUCKET")} == {
        "RIVERHOG_QUALIFICATION_AWS_DEEP_ARCHIVE_BUCKET",
        "RIVERHOG_QUALIFICATION_B2_ARCHIVE_BUCKET",
        "RIVERHOG_QUALIFICATION_B2_RETRIEVAL_CACHE_BUCKET",
    }
    assert references(job, "vars") == {
        "RIVERHOG_QUALIFICATION_AWS_DEEP_ARCHIVE_BUCKET",
        "RIVERHOG_QUALIFICATION_AWS_REGION",
        "RIVERHOG_QUALIFICATION_AWS_RUNTIME_ROLE_ARN",
        "RIVERHOG_QUALIFICATION_B2_ARCHIVE_ACCESS_KEY_ID",
        "RIVERHOG_QUALIFICATION_B2_ARCHIVE_BUCKET",
        "RIVERHOG_QUALIFICATION_B2_REGION",
        "RIVERHOG_QUALIFICATION_B2_RETRIEVAL_CACHE_ACCESS_KEY_ID",
        "RIVERHOG_QUALIFICATION_B2_RETRIEVAL_CACHE_BUCKET",
        "RIVERHOG_QUALIFICATION_B2_S3_ENDPOINT_URL",
        "RIVERHOG_QUALIFICATION_CLOUDFRONT_PUBLIC_KEY",
    }
    assert references(job, "secrets") == {
        "RIVERHOG_QUALIFICATION_ARCHIVE_PASSPHRASE",
        "RIVERHOG_QUALIFICATION_B2_ARCHIVE_SECRET_ACCESS_KEY",
        "RIVERHOG_QUALIFICATION_B2_RETRIEVAL_CACHE_SECRET_ACCESS_KEY",
        "RIVERHOG_QUALIFICATION_BOOTSTRAP_TOKEN",
        "RIVERHOG_QUALIFICATION_CLOUDFRONT_PRIVATE_KEY",
    }
    steps = job["steps"]
    action_steps = [
        step
        for workflow_job in workflow["jobs"].values()
        for step in workflow_job["steps"]
        if "uses" in step
    ]
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", step["uses"]) for step in action_steps)

    resolve = next(
        step
        for step in resolve_job["steps"]
        if step["name"] == "Resolve continuation and exact source"
    )
    assert '"$WORKFLOW_REF" != refs/heads/main' in resolve["run"]
    assert "actions/workflows/provider-qualification.yml/runs?branch=main" in resolve["run"]
    assert "actions/runs/$run_id/artifacts" in resolve["run"]
    assert "release/v1" in resolve["run"]
    assert "source_sha" in resolve["run"]
    download = next(
        step for step in steps if step["name"] == "Download verified continuation state"
    )
    assert "needs.resolve.outputs.action == 'poll'" in download["if"]
    assert "needs.resolve.outputs.action == 'restart'" in download["if"]
    assert "actions/artifacts/$ARTIFACT_ID/zip" in download["run"]
    assert ".active == true" in download["run"]
    assert "SUPERSEDED_SOURCE_SHA" in download["run"]
    assert 'git merge-base --is-ancestor "$superseded_source_sha" "$SOURCE_SHA"' in download["run"]
    cleanup = next(step for step in steps if step["name"] == "Clean superseded dummy B2 state")
    assert cleanup["if"] == "needs.resolve.outputs.action == 'restart'"
    assert "cleanup-b2" in cleanup["run"]
    assert 'git worktree add --detach "$cleanup_root" "$SUPERSEDED_SOURCE_SHA"' in cleanup["run"]
    assert 'MISE_TRUSTED_CONFIG_PATHS="$cleanup_root" make -C "$cleanup_root"' in cleanup["run"]
    exact = next(step for step in steps if step["name"] == "Check out verified exact source")
    assert 'test "$(git rev-parse --verify HEAD)" = "$SOURCE_SHA"' in exact["run"]

    provision = next(
        step
        for step in provision_job["steps"]
        if step["name"] == "Configure AWS provisioning identity"
    )
    runtime = next(step for step in steps if step["name"] == "Configure AWS runtime identity")
    reconcile = next(
        step
        for step in provision_job["steps"]
        if step["name"] == "Reconcile dedicated AWS infrastructure"
    )
    b2_check = next(
        step for step in steps if step["name"] == "Verify manually provisioned B2 infrastructure"
    )
    assert "AWS_PROVISION_ROLE_ARN" in provision["with"]["role-to-assume"]
    assert "AWS_RUNTIME_ROLE_ARN" in runtime["with"]["role-to-assume"]
    assert runtime["with"]["unset-current-credentials"] == "true"
    assert "env" not in reconcile
    assert set(b2_check["env"]) == {
        "RIVERHOG_QUALIFICATION_B2_ARCHIVE_ACCESS_KEY_ID",
        "RIVERHOG_QUALIFICATION_B2_ARCHIVE_SECRET_ACCESS_KEY",
        "RIVERHOG_QUALIFICATION_B2_RETRIEVAL_CACHE_ACCESS_KEY_ID",
        "RIVERHOG_QUALIFICATION_B2_RETRIEVAL_CACHE_SECRET_ACCESS_KEY",
    }
    assert b2_check["run"] == 'make provider-qualification args="b2-check $CONFIG_PATH"'

    state = next(step for step in steps if step["name"] == "Create and verify deterministic state")
    image_build = next(
        step for step in steps if step["name"] == "Build the disposable Riverhog storage boundary"
    )
    key_material = next(
        step
        for step in steps
        if step["name"] == "Materialize adapter authentication and CloudFront signing keys"
    )
    deployment_env = next(
        step for step in steps if step["name"] == "Generate the disposable deployment environment"
    )
    runtime_secrets = next(
        step for step in steps if step["name"] == "Hand adapter secrets to the non-root runtime"
    )
    deployment = next(
        step for step in steps if step["name"] == "Start the disposable Riverhog deployment"
    )
    snapshot = next(
        step for step in steps if step["name"] == "Snapshot bounded disposable database state"
    )
    assert "corpus-create" in state["run"] and "checkpoint-start" in state["run"]
    assert {
        "riverhog",
        "a-riverhog-aws-store",
        "a-riverhog-b2-store",
        "a-riverhog-filesystem-store",
    } == {item.strip() for item in image_build["with"]["targets"].split(",")}
    assert "RIVERHOG_QUALIFICATION_STORAGE_ADAPTER_TOKEN_PATH" in key_material["run"]
    assert 'test -z "${RIVERHOG_DATABASE_URL:-}"' in deployment_env["run"]
    assert "runtime-config $CONFIG_PATH" in deployment_env["run"]
    assert 'test -s "$runtime_env.database-url"' in deployment_env["run"]
    assert "RIVERHOG_DATABASE_URL=" in deployment_env["run"]
    assert "docker volume ls" in deployment_env["run"]
    assert "sudo chown 65532:65532" in runtime_secrets["run"]
    assert "sudo chmod 0400" in runtime_secrets["run"]
    assert "RIVERHOG_QUALIFICATION_CLOUDFRONT_PRIVATE_KEY_PATH" in runtime_secrets["run"]
    assert "RIVERHOG_QUALIFICATION_STORAGE_ADAPTER_TOKEN_PATH" in runtime_secrets["run"]
    assert "postgresql+psycopg://riverhog:riverhog@postgres:5432/riverhog" in deployment["run"]
    assert ".services.app.environment.RIVERHOG_CONFIG" in deployment["run"]
    assert '"/run/secrets/riverhog-database-url"' in deployment["run"]
    assert "tests/harness/provider-qualification.compose.yaml" in deployment["run"]
    assert "logs --no-color --tail 80" in deployment["run"]
    assert "aws-deep-archive-adapter" in deployment["run"]
    assert "b2-archive-adapter" in deployment["run"]
    assert "b2-retrieval-cache-adapter" in deployment["run"]
    assert "default_container; default_container()" in deployment["run"]
    assert "timeout 30s" in deployment["run"]
    assert 'provider_qualification_checkpoint.py capture "$STATE_DIR"' in snapshot["run"]
    assert snapshot["id"] == "snapshot"
    assert "steps.terminal_key.outcome == 'success'" in snapshot["if"]
    terminal_key = next(step for step in steps if step.get("id") == "terminal_key")
    assert 'revoke-terminal "$STATE_DIR"' in terminal_key["run"]
    assert terminal_key["if"] == "always() && steps.deployment.outcome == 'success'"
    assert set(terminal_key["env"]) == {"RIVERHOG_QUALIFICATION_BOOTSTRAP_TOKEN"}
    pair_check = next(
        step
        for step in steps
        if step["name"] == "Verify exact continuation pair before provider access"
    )
    assert pair_check["if"] == "needs.resolve.outputs.action == 'poll'"
    assert 'verify-pair "$STATE_INPUT_DIR"' in pair_check["run"]
    assert steps.index(pair_check) < steps.index(b2_check)
    assert 'verify-pair "$state_dir"' in state["run"]
    restore = next(step for step in steps if step["name"] == "Restore disposable database state")
    assert "pg_restore --exit-on-error" in restore["run"]
    operate = next(step for step in steps if step.get("id") == "operate")
    assert steps.index(operate) < steps.index(terminal_key) < steps.index(snapshot)

    package = next(
        step
        for step in steps
        if step["name"] == "Package resumable dummy state and public evidence"
    )
    assert package["if"] == (
        "always() && env.STATE_DIR != '' && steps.deployment.outcome == 'success' "
        "&& steps.snapshot.outcome == 'success'"
    )
    upload = next(step for step in steps if step["name"] == "Upload bounded qualification state")
    assert 'if [[ "$phase" == cleaned || "$phase" == failed ]]' in package["run"]
    assert 'cp "$STATE_DIR/checkpoint.json" "$STATE_DIR/database.dump"' in package["run"]
    assert '"$STATE_DIR/continuation.json" "$artifact_dir/"' in package["run"]
    assert 'verify-pair "$artifact_dir"' in package["run"]
    assert upload["if"] == "always() && steps.package.outcome == 'success'"
    assert steps.index(snapshot) < steps.index(package) < steps.index(upload)
    assert "retention_days=90" in package["run"]
    assert upload["with"] == {
        "name": "provider-qualification-state",
        "path": "${{ runner.temp }}/provider-public-artifact",
        "if-no-files-found": "error",
        "retention-days": "${{ steps.package.outputs.retention_days }}",
    }
    assert "age --encrypt" not in text and "age --decrypt" not in text
    assert "RIVERHOG_QUALIFICATION_CLOUDFRONT_PRIVATE_KEY" in text
    assert not any(
        name.endswith("SECRET_ACCESS_KEY")
        or name
        in {
            "RIVERHOG_QUALIFICATION_ARCHIVE_PASSPHRASE",
            "RIVERHOG_QUALIFICATION_BOOTSTRAP_TOKEN",
        }
        for name in job["env"]
    )


def test_client_platform_toolchain_is_locked_for_every_matrix_os() -> None:
    lock = tomllib.loads(MISE_LOCK.read_text(encoding="utf-8"))

    for tool in ("python", "uv"):
        entries = lock["tools"][tool]
        assert len(entries) == 1
        platforms = {
            key.removeprefix("platforms."): value
            for key, value in entries[0].items()
            if key.startswith("platforms.")
        }
        assert set(platforms) == {"linux-x64", "macos-arm64", "windows-x64"}
        windows = platforms["windows-x64"]
        assert windows["url"].startswith("https://github.com/")
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", windows["checksum"])

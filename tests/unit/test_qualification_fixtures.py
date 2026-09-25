from __future__ import annotations

import ast
import importlib.util
import sys
import tomllib
from pathlib import Path

import pytest
import yaml
from a_review0_materializer.app import load_config as load_materializer_config
from a_review0_rclone_target.app import load_config as load_rclone_config
from a_riverhog_aws_store.app import load_config as load_aws_config
from a_riverhog_b2_store.app import load_config as load_b2_config
from a_riverhog_event_relay.relay import load_config as load_event_relay_config
from a_riverhog_ftp_spool.config import load_config as load_adapter_config
from gogurt_core.core import execute_gogurt_action, load_gogurt_actions, plan_gogurt_action
from riverhog_core.runtime_document import load_runtime_document
from stove0_core import RecipeCatalog
from stove0_core.runtime_config import load_stove0_config
from stove0_operator_contracts import AdmissionCatalog

from tests.gogurt_provider import path_mounted_volume_provider

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_ROOT = REPO_ROOT / "qualification/contracts/riverhog-v1.json"
CONTRACT_FILES = {CONTRACT_ROOT, CONTRACT_ROOT.with_name("riverhog-v1-audit.json")}
QUALIFICATION_INPUTS = {
    REPO_ROOT / "qualification/contract-freeze-exceptions.toml",
    REPO_ROOT / "qualification/fixtures/gogurt/gogurt-routes.yaml",
    REPO_ROOT / "qualification/fixtures/gogurt/scripts/fake_archive_device.py",
    REPO_ROOT / "qualification/fixtures/a-riverhog-ftp-spool/config.yaml",
    REPO_ROOT / "qualification/fixtures/a-riverhog-aws-store/config.yaml",
    REPO_ROOT / "qualification/fixtures/a-riverhog-b2-store/config.yaml",
    REPO_ROOT / "qualification/fixtures/a-riverhog-event-relay/config.yaml",
    REPO_ROOT / "qualification/fixtures/a-review0-materializer/config.yaml",
    REPO_ROOT / "qualification/fixtures/a-review0-rclone-target/config.yaml",
    REPO_ROOT / "qualification/fixtures/riverhog/config.yaml",
    REPO_ROOT / "qualification/fixtures/stove0/config.yaml",
    REPO_ROOT / "qualification/fixtures/stove0/recipes.yaml",
    REPO_ROOT / "qualification/fixtures/stove0/admissions.json",
    *CONTRACT_FILES,
    REPO_ROOT / "qualification/provider/config.toml",
}


DATABASE_AUTHORITY = "tests.support.qualification.database_selector_plans"
RECOVERY_AUTHORITY = "tests.support.qualification.recovery_archive"
DATABASE_RUNNER_SYMBOLS = {
    "DATABASE_PLAN_OPERATIONS",
    "PlanCase",
    "catalog_sync_plan_cases",
    "index_names",
    "node_types",
    "plan_cases",
    "seed_selector_relations",
    "seed_stove0_selector_relations",
}
DATABASE_TEST_SYMBOLS = DATABASE_RUNNER_SYMBOLS | {
    "DATABASE_FILTER_SELECTORS",
    "NON_PLAN_QUERY_OPERATIONS",
    "riverhog_plan_statement",
}
RECOVERY_SYMBOLS = {"PASSPHRASE", "PASSPHRASE_ID", "write_archive"}
QUALIFICATION_CONSUMERS = {
    "scripts/database_qualification.py": (DATABASE_AUTHORITY, DATABASE_RUNNER_SYMBOLS),
    "tests/integration/test_public_selector_plans_postgres.py": (
        DATABASE_AUTHORITY,
        DATABASE_TEST_SYMBOLS,
    ),
    "scripts/qualify_installation.py": (RECOVERY_AUTHORITY, RECOVERY_SYMBOLS),
    "some-implementations/riverhog/recovery/tests/test_recovery.py": (
        RECOVERY_AUTHORITY,
        RECOVERY_SYMBOLS,
    ),
    "tests/harness/filesystem_recovery_materialization.py": (RECOVERY_AUTHORITY, RECOVERY_SYMBOLS),
}
FORMER_AUTHORITIES = {
    "tests.integration.test_public_selector_plans_postgres",
    "reference.riverhog.recovery.tests.test_recovery",
    "some-implementations.riverhog.recovery.tests.test_recovery",
}
LOADER_NAMES = {
    "spec_from_file_location",
    "SourceFileLoader",
    "run_path",
    "import_module",
    "__import__",
    "open",
    "read_text",
}


def _check_qualification_import_edges(source: str, *, authority: str, symbols: set[str]) -> None:
    tree = ast.parse(source)
    owned_imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            assert node.level == 0 or not module.startswith("tests"), (
                "qualification authority must use an absolute repository import"
            )
            assert not any(
                module == old or module.startswith(old + ".") for old in FORMER_AUTHORITIES
            )
            names = {alias.name for alias in node.names}
            if module == authority and node.level == 0:
                assert "*" not in names
                owned_imports.update(names)
            else:
                assert not names.intersection(symbols), (
                    f"qualification symbols imported from another module: {module}"
                )
        elif isinstance(node, ast.Import):
            assert not any(
                alias.name == old or alias.name.startswith(old + ".")
                for alias in node.names
                for old in FORMER_AUTHORITIES
            )
        elif isinstance(node, ast.Call):
            callable_name = (
                node.func.attr
                if isinstance(node.func, ast.Attribute)
                else node.func.id
                if isinstance(node.func, ast.Name)
                else ""
            )
            if callable_name not in LOADER_NAMES:
                continue
            literals = {
                part.value
                for part in ast.walk(node)
                if isinstance(part, ast.Constant) and isinstance(part.value, str)
            }
            assert not any(
                "test_public_selector_plans_postgres" in literal
                or "test_recovery.py" in literal
                or "reference.riverhog.recovery.tests.test_recovery" in literal
                for literal in literals
            ), "qualification authority dynamically loaded from a test file"
    assert owned_imports == symbols, (
        f"qualification owner {authority} imports {sorted(owned_imports)}, "
        f"expected {sorted(symbols)}"
    )


def test_qualification_support_import_edges_use_repository_owned_modules() -> None:
    for authority in {item[0] for item in QUALIFICATION_CONSUMERS.values()}:
        spec = importlib.util.find_spec(authority)
        assert spec is not None and spec.origin is not None
        assert Path(spec.origin).resolve() == (
            REPO_ROOT / "tests/support/qualification" / f"{authority.rsplit('.', 1)[-1]}.py"
        )
    for relative, (authority, symbols) in QUALIFICATION_CONSUMERS.items():
        _check_qualification_import_edges(
            (REPO_ROOT / relative).read_text(encoding="utf-8"),
            authority=authority,
            symbols=symbols,
        )


@pytest.mark.parametrize(
    "other_import",
    [
        "from tests.integration.test_public_selector_plans_postgres import plan_cases",
        "from tests.unit.test_other import plan_cases",
        "import tests.integration.test_public_selector_plans_postgres",
        'importlib.import_module("tests.integration.test_public_selector_plans_postgres")',
        'importlib.util.spec_from_file_location("old", Path("tests") / "integration" / '
        '"test_public_selector_plans_postgres.py")',
        'Path("tests/integration/test_public_selector_plans_postgres.py").read_text()',
        'importlib.import_module("reference.riverhog.recovery.tests.test_recovery")',
        'runpy.run_path("some-implementations/riverhog/recovery/tests/test_recovery.py")',
    ],
)
def test_qualification_owner_witness_rejects_test_module_reuse(other_import: str) -> None:
    source = f"from {DATABASE_AUTHORITY} import plan_cases\n{other_import}\n"
    with pytest.raises(AssertionError):
        _check_qualification_import_edges(
            source, authority=DATABASE_AUTHORITY, symbols={"plan_cases"}
        )


def test_every_checked_qualification_input_runs_through_its_real_consumer(
    tmp_path: Path,
    checked_contract_closure,
) -> None:  # type: ignore[no-untyped-def]
    checked_inputs = {
        path
        for path in (REPO_ROOT / "qualification").rglob("*")
        if path.is_file()
        and path.suffix != ".md"
        and "riverhog-v1" not in path.parts
        and "__pycache__" not in path.parts
    }
    assert checked_inputs == QUALIFICATION_INPUTS

    gogurt_root = REPO_ROOT / "qualification/fixtures/gogurt"
    actions = load_gogurt_actions(gogurt_root / "gogurt-routes.yaml")
    assert [action.route for action in actions] == ["example-camera-card"]

    mounted_device = tmp_path / "mounted-device"
    mounted_device.mkdir()
    (mounted_device / ".gogurt").write_text("example-camera-card\n", encoding="utf-8")
    provider = path_mounted_volume_provider(lambda: [mounted_device])
    action_plan = plan_gogurt_action(
        gogurt_root / "gogurt-routes.yaml",
        mounted_device,
        provider=provider,
    )
    completed = execute_gogurt_action(action_plan, provider=provider, capture_output=True)
    assert completed.returncode == 0
    assert "archive example-camera" in completed.stdout

    recipes = RecipeCatalog.load(REPO_ROOT / "qualification/fixtures/stove0/recipes.yaml")
    assert {recipe.id for recipe in recipes.recipes} == {
        "stove0.audio-archive/v1",
        "stove0.conformance-media/v1",
        "stove0.review-effect/v1",
        "stove0.review/v1",
        "stove0.video-archive/v1",
    }
    admissions = AdmissionCatalog.model_validate_json(
        (REPO_ROOT / "qualification/fixtures/stove0/admissions.json").read_text(encoding="utf-8")
    )
    assert [policy.id for policy in admissions.policies] == ["conformance-media"]
    assert (
        admissions.policies[0].recipe_sha256
        == recipes.recipe("stove0.conformance-media/v1", 1).sha256
    )

    def materialize_config(name: str) -> Path:
        fixture = REPO_ROOT / "qualification/fixtures" / name / "config.yaml"
        document = yaml.safe_load(fixture.read_text(encoding="utf-8"))

        def mount_secrets(value: object) -> object:
            if isinstance(value, dict):
                return {key: mount_secrets(item) for key, item in value.items()}
            if isinstance(value, list):
                return [mount_secrets(item) for item in value]
            if isinstance(value, str) and value.startswith("/run/secrets/"):
                path = tmp_path / value.rsplit("/", 1)[-1]
                secret = (
                    "postgresql+psycopg://app:password@postgres:5432/app"
                    if "database-url" in path.name or "database_url" in path.name
                    else (
                        "https://webhook.example.invalid/hook"
                        if "webhook-url" in path.name
                        else "fake-qualification-secret-with-at-least-32-bytes"
                    )
                )
                path.write_text(secret + "\n", encoding="utf-8")
                return str(path)
            return value

        path = tmp_path / f"{name}.yaml"
        path.write_text(yaml.safe_dump(mount_secrets(document)), encoding="utf-8")
        return path

    assert load_runtime_document(materialize_config("riverhog")).archive_write_store == "archive"
    assert "review" in load_stove0_config(materialize_config("stove0")).targets
    assert (
        load_aws_config(materialize_config("a-riverhog-aws-store")).client_config().region
        == "us-east-1"
    )
    assert (
        load_b2_config(materialize_config("a-riverhog-b2-store")).client_config().region
        == "us-west-004"
    )
    assert (
        load_materializer_config(materialize_config("a-review0-materializer")).samplers[0].id
        == "fake-sampler"
    )
    assert (
        load_rclone_config(materialize_config("a-review0-rclone-target")).rclone_remote
        == "fake-remote:"
    )
    assert (
        load_event_relay_config(materialize_config("a-riverhog-event-relay")).sources[0].name
        == "stove0"
    )

    ftp_fixture = REPO_ROOT / "qualification/fixtures/a-riverhog-ftp-spool/config.yaml"
    ftp_document = yaml.safe_load(ftp_fixture.read_text(encoding="utf-8"))
    for name in ("riverhog_token", "api_token"):
        secret = tmp_path / f"{name}.token"
        secret.write_text(f"fake-{name}-token\n", encoding="utf-8")
        ftp_document[f"{name}_file"] = str(secret)
    ftp_config = tmp_path / "ftp-spool.yaml"
    ftp_config.write_text(yaml.safe_dump(ftp_document), encoding="utf-8")
    adapters = load_adapter_config(ftp_config)
    assert [source.id for source in adapters.sources] == ["ftp-intake"]

    script = REPO_ROOT / "scripts/provider_qualification.py"
    spec = importlib.util.spec_from_file_location("provider_qualification_config", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    qualification = module.load_config(REPO_ROOT / "qualification/provider/config.toml")
    assert qualification.cloudfront.enabled is True
    assert {bucket.logical_name for bucket in qualification.buckets} == {
        "b2-archive",
        "b2-retrieval-cache",
        "aws-deep-archive",
    }
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    assert set(release["qualification"]["storage_providers"]["cases"]) == {
        *(bucket.logical_name for bucket in qualification.buckets),
        "aws-cloudfront-egress",
        "filesystem-retrieval-cache",
    }

    contract_script = REPO_ROOT / "scripts/contract_freeze.py"
    if str(contract_script.parent) not in sys.path:
        sys.path.insert(0, str(contract_script.parent))
    contract_spec = importlib.util.spec_from_file_location(
        "qualification_contract_freeze", contract_script
    )
    assert contract_spec is not None and contract_spec.loader is not None
    contract_module = importlib.util.module_from_spec(contract_spec)
    sys.modules[contract_spec.name] = contract_module
    contract_spec.loader.exec_module(contract_module)
    projection = checked_contract_closure["projection"]
    trace = checked_contract_closure["trace"]
    checked = contract_module.load_bundle(CONTRACT_ROOT)
    assert checked == checked_contract_closure["bundle"]
    assert checked.closure["series"] == projection["series"]
    assert checked.audit["trace"]["format"] == trace["format"]

from __future__ import annotations

import ast
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tomllib
from dataclasses import replace
from pathlib import Path
from types import ModuleType

import pytest
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/release.py"


def load_script() -> ModuleType:
    if str(SCRIPT.parent) not in sys.path:
        sys.path.insert(0, str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location("riverhog_release", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _copy_release_contract(module: ModuleType, destination: Path) -> None:
    for relative in (
        "pyproject.toml",
        "uv.lock",
        "mise.lock",
        "release.toml",
        "docker-bake.hcl",
    ):
        shutil.copy2(REPO_ROOT / relative, destination / relative)
    for source in module._workspace_pyprojects(REPO_ROOT):
        relative = source.relative_to(REPO_ROOT)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    for relative in release["python"]["reusable_library"]:
        pyproject = REPO_ROOT / relative / "pyproject.toml"
        for package in module._public_python_modules(pyproject):
            source = pyproject.parent / "src" / Path(*package.split(".")) / "__init__.py"
            target = destination / source.relative_to(REPO_ROOT)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    for image in release["images"]["runtime"]:
        source = module._bake_dockerfile(REPO_ROOT, image)
        relative = source.relative_to(REPO_ROOT)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    for owner in release["state"]["owners"]:
        for relative in owner["fixtures"]:
            source = REPO_ROOT / relative
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)


def test_release_contract_classifies_every_coordinated_distribution() -> None:
    module = load_script()

    projects = module.validate_release_contract(REPO_ROOT)

    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    declared_projects = {path: role for role, paths in release["python"].items() for path in paths}
    assert {project.path: project.role for project in projects} == declared_projects
    assert {project.version for project in projects} == {"0.1.0"}
    assert release["compatibility"]["python_api"].startswith(
        "Freeze-protected declared public-module exports"
    )
    assert "first shipped in v1" in release["compatibility"]["licensing"]
    assert {owner["id"] for owner in release["state"]["owners"]} == {
        "gogurt-listener",
        "a-riverhog-event-relay-cursor",
        "a-riverhog-minisign-witness-ledger",
        "a-riverhog-opentimestamps-witness-ledger",
        "riverhog-catalog",
        "a-riverhog-ftp-spool-custody",
        "a-riverhog-cli-local",
        "riverhog-provenance-installation",
        "stove0-control",
        "stove0-target-jobs",
    }
    signing = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))["signing"]
    assert signing["checksums"] == "SHA-256"
    assert signing["signature"] == "minisign"
    assert "outside the repository, GitHub, CI logs" in signing["secret_key"]
    assert "signed by both old and new keys" in signing["rotation"]
    assert "without moving an existing tag" in signing["compromise"]
    governance = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))[
        "governance"
    ]
    assert governance["workflow_source_branch"] == "main"
    assert governance["branch_delivery"] == "pre-v1-main-convergence"
    assert governance["required_check_integration_id"] == 15368
    assert governance["release"]["required_approvals"] == 0
    assert governance["tags"]["release_candidate"] == "v{version}-rc.{candidate}"
    assert governance["tags"]["final"] == "v{version}"
    assert governance["environments"] == {
        "release": "release-publication",
        "pages": "github-pages",
        "provider_qualification_provisioning": "provider-qualification-provisioning",
        "provider_qualification_runtime": "provider-qualification",
    }
    platforms = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))["platforms"]
    assert platforms == {
        "end_user_artifacts": ["linux-x64", "macos-arm64", "windows-x64"],
        "runtime_images": ["linux/amd64"],
    }
    assert all(
        project.role in {"application", "component", "reusable_library"}
        for project in projects
        if project.path.startswith("some-implementations/")
    )
    qualification = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))[
        "qualification"
    ]
    assert qualification["storage_providers"] == module.STORAGE_PROVIDER_QUALIFICATION


def test_release_rejects_unclassified_durable_state(tmp_path: Path) -> None:
    module = load_script()
    _copy_release_contract(module, tmp_path)
    release_path = tmp_path / "release.toml"
    release_path.write_text(
        release_path.read_text(encoding="utf-8").replace(
            'classification = "durable-user-evidence"\n', "", 1
        ),
        encoding="utf-8",
    )
    with pytest.raises(module.ReleaseError, match="durable-state owner is incomplete"):
        module.validate_release_contract(tmp_path)


def test_release_rejects_apache_custodial_distribution(tmp_path: Path) -> None:
    module = load_script()
    _copy_release_contract(module, tmp_path)
    pyproject = (
        tmp_path
        / "some-implementations/riverhog/applications/a-riverhog-minisign-witness/pyproject.toml"
    )
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8").replace(
            'license = "CAL-1.0"', 'license = "Apache-2.0"'
        ),
        encoding="utf-8",
    )
    with pytest.raises(module.ReleaseError, match="custodial durable-state owner must ship"):
        module.validate_release_contract(tmp_path)


def test_shared_supplied_library_cannot_claim_unprefixed_family_name() -> None:
    module = load_script()
    projects = module.validate_release_contract(REPO_ROOT)
    renamed = [
        replace(project, name="gogurt-path-volume-support")
        if project.name == "a-gogurt-path-volume-lib"
        else project
        for project in projects
    ]

    naming = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))["naming"]
    assert "review0-planner" in naming["supplied_family_machinery_distributions"]
    with pytest.raises(module.ReleaseError, match=r"unregistered=\['gogurt-path-volume-support'\]"):
        module._validate_supplied_distribution_names(renamed, naming)


@pytest.mark.parametrize(
    "naming",
    [
        None,
        {},
        {"supplied_family_machinery_distributions": ["gogurt", "gogurt"]},
        {"supplied_family_machinery_distributions": ["review0-planner", "gogurt"]},
        {"supplied_family_machinery_distributions": ["a-gogurt-helper"]},
        {"supplied_family_machinery_distributions": ["Gogurt"]},
        {"supplied_family_machinery_distributions": [], "unreviewed": True},
    ],
)
def test_supplied_family_naming_registry_is_fail_closed(naming: object) -> None:
    module = load_script()
    projects = module.validate_release_contract(REPO_ROOT)
    with pytest.raises(module.ReleaseError, match="supplied family naming registry"):
        module._validate_supplied_distribution_names(projects, naming)


def test_python_distribution_identities_use_pep_503_canonical_names(
    tmp_path: Path,
) -> None:
    module = load_script()

    assert module._canonical_distribution_name("Riverhog.Client") == "riverhog-client"
    assert module._canonical_distribution_name("riverhog_client") == "riverhog-client"
    assert module._canonical_distribution_name("riverhog--client") == "riverhog-client"
    with pytest.raises(module.ReleaseError, match="built evidence repeats a canonical"):
        module._canonical_distribution_versions(
            [("foo.bar", "1.0.0"), ("foo_bar", "1.0.0")],
            source="built evidence",
        )

    _copy_release_contract(module, tmp_path)
    pyproject = tmp_path / "some-implementations/gogurt/packages/listener-runtime/pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8").replace(
            'name = "gogurt-listener-runtime"',
            'name = "riverhog_client"',
            1,
        ),
        encoding="utf-8",
    )
    with pytest.raises(module.ReleaseError, match="repeats distribution name: riverhog-client"):
        module.validate_release_contract(tmp_path)


def test_publication_license_inventory_requires_canonical_distribution_coordinates() -> None:
    module = load_script()
    publication = {
        "distributions": {
            "riverhog-client": {
                "publication_identity": {
                    "kind": "python-distribution",
                    "coordinate": "riverhog_client",
                },
                "license_expression": "Apache-2.0",
            }
        },
        "runtime_images": {},
    }

    with pytest.raises(module.ReleaseError, match="noncanonical Python distribution"):
        module._publication_license_inventory(publication)

    publication["distributions"]["second-coordinate"] = {
        "publication_identity": {
            "kind": "python-distribution",
            "coordinate": "riverhog-client",
        },
        "license_expression": "Apache-2.0",
    }
    publication["distributions"]["riverhog-client"]["publication_identity"]["coordinate"] = (
        "riverhog-client"
    )
    with pytest.raises(module.ReleaseError, match="repeats a license coordinate"):
        module._publication_license_inventory(publication)


def test_reusable_library_requires_explicit_exports_for_every_public_module(
    tmp_path: Path,
) -> None:
    module = load_script()
    _copy_release_contract(module, tmp_path)
    public_root = tmp_path / "some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py"
    public_root.write_text('"""No declared public surface."""\n', encoding="utf-8")

    with pytest.raises(module.ReleaseError, match="explicit public __all__"):
        module.validate_release_contract(tmp_path)


def test_runtime_image_cannot_silently_override_its_published_platforms(
    tmp_path: Path,
) -> None:
    module = load_script()
    _copy_release_contract(module, tmp_path)
    bake = tmp_path / "docker-bake.hcl"
    text = bake.read_text(encoding="utf-8")
    text = text.replace(
        'target "riverhog" {\n  inherits   = ["image-common"]',
        'target "riverhog" {\n  inherits   = ["image-common"]\n  platforms  = ["linux/arm64"]',
        1,
    )
    bake.write_text(text, encoding="utf-8")

    with pytest.raises(module.ReleaseError, match="exact common platform set: riverhog"):
        module.validate_release_contract(tmp_path)


def test_release_role_dependency_direction_is_exact() -> None:
    module = load_script()
    projects = module.validate_release_contract(REPO_ROOT)

    _internal, artifact_dependencies, _licenses = module._project_dependency_graph(
        REPO_ROOT,
        projects,
    )
    roles = {project.name: project.role for project in projects}
    paths = {project.name: project.path for project in projects}
    supplied = {name for name, path in paths.items() if path.startswith("some-implementations/")}
    components = {name for name, role in roles.items() if role == "component"}
    implementations = {
        name
        for name, role in roles.items()
        if role in {"end_user_artifact", "deployed_implementation", "application"}
    }

    assert all(
        not (artifact_dependencies[name] & components)
        for name, role in roles.items()
        if role != "component"
    )
    assert all(
        not (artifact_dependencies[name] & supplied)
        for name, path in paths.items()
        if not path.startswith("some-implementations/")
    )
    assert all(
        not (artifact_dependencies[name] & implementations)
        for name, role in roles.items()
        if role in {"application", "component"}
    )
    architecture = " ".join(
        (REPO_ROOT / "docs/architecture.md").read_text(encoding="utf-8").split()
    )
    assert "Supplied applications own their workflows" in architecture
    assert "enter Riverhog only through public contracts" in architecture


def test_release_contract_rejects_optional_supplied_dependency_from_product(
    tmp_path: Path,
) -> None:
    module = load_script()
    _copy_release_contract(module, tmp_path)
    pyproject = tmp_path / "riverhog/pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8")
        + "\n[project.optional-dependencies]\n"
        + 'fixture = ["a-riverhog-linux-provenance-observer>=0.1,<0.2"]\n',
        encoding="utf-8",
    )

    with pytest.raises(
        module.ReleaseError, match="product-owned release unit depends on supplied implementations"
    ):
        module.validate_release_contract(tmp_path)


@pytest.mark.parametrize("dependency", ["a-stove0-cli", "stove0-server"])
def test_release_contract_rejects_application_dependency_from_supplied_component(
    tmp_path: Path,
    dependency: str,
) -> None:
    module = load_script()
    _copy_release_contract(module, tmp_path)
    pyproject = tmp_path / "some-implementations/stove0/review0/planning/pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8")
        + "\n[project.optional-dependencies]\n"
        + f'product = ["{dependency}>=0.1,<0.2"]\n',
        encoding="utf-8",
    )

    with pytest.raises(module.ReleaseError, match="depends on application release units"):
        module.validate_release_contract(tmp_path)


def test_reusable_library_public_annotations_do_not_leak_internal_build_units() -> None:
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    internal_modules = {
        Path(path).name.replace("-", "_") for path in release["python"]["internal_build_unit"]
    }
    failures: list[str] = []

    def annotation_leaks(annotation: ast.expr | None, imports: dict[str, str]) -> bool:
        if annotation is None:
            return False
        return any(
            isinstance(node, ast.Name) and node.id in imports for node in ast.walk(annotation)
        )

    def check_function(
        source: Path,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        imports: dict[str, str],
    ) -> None:
        annotations = [
            *(argument.annotation for argument in (*node.args.posonlyargs, *node.args.args)),
            *(argument.annotation for argument in node.args.kwonlyargs),
            node.args.vararg.annotation if node.args.vararg else None,
            node.args.kwarg.annotation if node.args.kwarg else None,
            node.returns,
        ]
        if any(annotation_leaks(annotation, imports) for annotation in annotations):
            failures.append(f"{source.relative_to(REPO_ROOT)}:{node.lineno} {node.name}")

    for relative in release["python"]["reusable_library"]:
        for source in sorted((REPO_ROOT / relative).glob("src/**/*.py")):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            imports: dict[str, str] = {}
            for node in tree.body:
                if isinstance(node, ast.ImportFrom) and node.module:
                    root = node.module.split(".", 1)[0]
                    if root in internal_modules:
                        for name in node.names:
                            imports[name.asname or name.name] = root
                elif isinstance(node, ast.Import):
                    for name in node.names:
                        root = name.name.split(".", 1)[0]
                        if root in internal_modules:
                            imports[name.asname or root] = root
            if not imports:
                continue
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if not node.name.startswith("_"):
                        check_function(source, node, imports)
                elif isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
                    for member in node.body:
                        if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            if not member.name.startswith("_"):
                                check_function(source, member, imports)
                        elif (
                            isinstance(member, ast.AnnAssign)
                            and isinstance(member.target, ast.Name)
                            and not member.target.id.startswith("_")
                            and annotation_leaks(member.annotation, imports)
                        ):
                            failures.append(
                                f"{source.relative_to(REPO_ROOT)}:{member.lineno} "
                                f"{node.name}.{member.target.id}"
                            )

    assert failures == []


def test_image_notices_require_the_exact_locked_standalone_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    payload = {
        "python": [],
        "deb": [],
        "standalone": [
            {
                "kind": "standalone",
                "name": "minisign",
                "version": "0.11",
                "notices": [
                    {
                        "source": "/usr/share/licenses/minisign/LICENSE",
                        "content": module.base64.b64encode(b"license\n").decode("ascii"),
                    }
                ],
            }
        ],
        "missing": [],
    }
    monkeypatch.setattr(
        module,
        "_run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(payload)
        ),
    )

    with pytest.raises(module.ReleaseError, match="differs from its exact mise lock"):
        module._image_notice_components(
            REPO_ROOT,
            "example:test",
            first_party=set(),
            expected_standalone={"minisign": "0.12"},
        )


def test_image_notices_accept_exact_locked_standalone_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    payload = {
        "python": [],
        "deb": [],
        "standalone": [
            {
                "kind": "standalone",
                "name": "minisign",
                "version": "0.12",
                "notices": [
                    {
                        "source": "/usr/share/licenses/minisign/LICENSE",
                        "content": module.base64.b64encode(b"license\n").decode("ascii"),
                    }
                ],
            }
        ],
        "missing": [],
    }
    monkeypatch.setattr(
        module,
        "_run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(payload)
        ),
    )

    assert module._image_notice_components(
        REPO_ROOT,
        "example:test",
        first_party=set(),
        expected_standalone={"minisign": "0.12"},
    ) == [
        {
            "kind": "standalone",
            "name": "minisign",
            "version": "0.12",
            "notices": [
                {
                    "source": "/usr/share/licenses/minisign/LICENSE",
                    "content": b"license\n",
                }
            ],
        }
    ]


def test_dry_run_can_write_the_same_sha_bound_summary_it_prints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    module = load_script()
    payload = {"source_sha": "1" * 40, "published": False}
    monkeypatch.setattr(module, "dry_run", lambda _root, _version, **_kwargs: payload)
    summary = tmp_path / "qualification" / "release.json"

    assert module.main(["dry-run", "--version", "1.0.0", "--summary", str(summary)]) == 0

    assert module.json.loads(summary.read_text(encoding="utf-8")) == payload
    assert module.json.loads(capsys.readouterr().out) == payload


def test_published_release_manifest_must_match_regenerated_canonical_bytes(
    tmp_path: Path,
) -> None:
    module = load_script()
    generated = tmp_path / "generated.json"
    expected = tmp_path / "published.json"
    payload = {"format": "riverhog-release/v1", "version": "1.0.0"}
    module._write_json(generated, payload)
    module._write_json(expected, payload)

    module._verify_reproduced_release_manifest(generated, expected)

    module._write_json(expected, {**payload, "version": "1.0.1"})
    with pytest.raises(module.ReleaseError, match="differs from the published canonical"):
        module._verify_reproduced_release_manifest(generated, expected)


def test_published_release_manifest_comparison_rejects_noncanonical_json(
    tmp_path: Path,
) -> None:
    module = load_script()
    generated = tmp_path / "generated.json"
    expected = tmp_path / "published.json"
    payload = {"format": "riverhog-release/v1", "version": "1.0.0"}
    module._write_json(generated, payload)
    expected.write_text(module.json.dumps(payload), encoding="utf-8")

    with pytest.raises(module.ReleaseError, match="is not canonical JSON"):
        module._verify_reproduced_release_manifest(generated, expected)


def test_release_plan_is_exact_sha_bound_and_excludes_the_test_image() -> None:
    module = load_script()

    plan = module.build_release_plan(REPO_ROOT, "1.0.0", allow_dirty=True)

    assert plan["tag"] == "v1.0.0"
    assert len(plan["source_sha"]) == 40
    assert all(character in "0123456789abcdef" for character in plan["source_sha"])
    release = tomllib.loads((REPO_ROOT / "release.toml").read_text(encoding="utf-8"))
    assert {project["path"] for project in plan["python"]} == {
        path for paths in release["python"].values() for path in paths
    }
    assert all(len(project["artifacts"]) == 2 for project in plan["python"])
    publication = module.publication_contract(REPO_ROOT)
    assert plan["publication"] == publication
    assert {image["target"] for image in plan["images"]} == set(publication["runtime_images"])
    assert all(project["requires_python"] == ">=3.12" for project in plan["python"])
    assert all(project["license_expression"] for project in plan["python"])
    assert all(project["publication_identity"] for project in plan["python"])
    assert {image["role"] for image in plan["images"]} == {
        "product",
        "application",
        "component",
    }
    assert {image["target"] for image in plan["images"] if image["role"] == "product"} == {
        "riverhog"
    }
    assert all(image["description"] for image in plan["images"])
    assert all(image["license_expression"] for image in plan["images"])
    assert all(image["publication_identity"] for image in plan["images"])
    assert next(image for image in plan["images"] if image["target"] == "stove0")[
        "distributions"
    ] == ["stove0-server"]
    assert next(
        image for image in plan["images"] if image["target"] == "a-stove0-nvenc-av1-opus-target"
    )["distributions"] == [
        "a-stove0-nvenc-av1-opus-target",
        "a-review0-nvenc-av1-opus-sampler",
    ]
    assert next(image for image in plan["images"] if image["target"] == "a-stove0-opus-target")[
        "distributions"
    ] == [
        "a-stove0-opus-target",
        "a-review0-opus-sampler",
    ]
    assert (
        module._image_distribution_roots_label(["a-stove0-opus-target", "a-review0-opus-sampler"])
        == '["a-stove0-opus-target","a-review0-opus-sampler"]'
    )
    assert all(image["platforms"] == ["linux/amd64"] for image in plan["images"])
    assert all(
        image["tags"]
        == [
            f"{image['repository']}:1.0.0",
            f"{image['repository']}:sha-{plan['source_sha']}",
        ]
        for image in plan["images"]
    )
    assert "riverhog-test:dev" not in str(plan)
    assert plan["supporting_artifacts"] == {
        "documentation": "riverhog-docs-v1.0.0.tar.gz",
        "source": "riverhog-source-v1.0.0.tar.gz",
        "contract": "riverhog-v1-contract.tar.gz",
        "installation": {
            "manifest": "install-manifest.json",
            "locks": [f"pylock.{root}.toml" for root in release["installation"]["roots"]],
            "index_snapshot": "riverhog-python-index-v1.0.0.tar.gz",
            "gogurt_listener_reference": "gogurt-listener-v1.0.0.md",
        },
        "notices": {
            "format": "riverhog-artifact-notices/v1",
            "directory": "notices",
            "archive_format": "tar.gz",
            "basis": "exact-artifact-contents",
            "required_for": ["wheel", "image"],
        },
        "evidence": [
            "riverhog-v1-contract.tar.gz",
            "install-manifest.json",
            "release-manifest.json",
            "SHA256SUMS",
            "SHA256SUMS.minisig",
            "release.spdx.json",
            "release.intoto.jsonl",
            "THIRD_PARTY_NOTICES.md",
        ],
    }
    markdown = module.render_release_markdown(plan)
    assert markdown.startswith("# Riverhog v1.0.0\n\n")
    assert f"Source: `{plan['source_sha']}`" in markdown
    assert "## Runtime images" in markdown
    assert "AWS-backed Riverhog archive and retrieval store." in markdown
    assert "Initial v1 release; there is no previous release tag." in markdown


@pytest.mark.parametrize(
    ("baseline", "candidate", "preserved"),
    [
        ("Apache-2.0 OR MIT", "MIT OR Apache-2.0", True),
        ("Apache-2.0 OR MIT", "BSD-3-Clause OR Apache-2.0 OR MIT", True),
        (
            "(Apache-2.0 AND MIT) OR BSD-3-Clause",
            "BSD-3-Clause OR (MIT AND Apache-2.0) OR CAL-1.0",
            True,
        ),
        ("Apache-2.0 OR MIT", "Apache-2.0", False),
        ("Apache-2.0", "Apache-2.0 AND MIT", False),
        (
            "Apache-2.0 WITH LLVM-exception",
            "Apache-2.0 OR LLVM-exception",
            False,
        ),
    ],
)
def test_v1_license_grants_use_only_structural_spdx_disjunct_inclusion(
    baseline: str, candidate: str, preserved: bool
) -> None:
    module = load_script()

    assert module._license_grant_preserved(baseline, candidate) is preserved


def _history_manifest(
    module: ModuleType,
    version: str,
    licenses: dict[tuple[str, str], str],
    previous: tuple[str, str] | None,
) -> dict[str, object]:
    return {
        "format": module.RELEASE_FORMAT,
        "version": version,
        "tag": f"v{version}",
        "v1_history": (
            {"kind": "genesis"}
            if previous is None
            else {
                "kind": "continuation",
                "previous_tag": previous[0],
                "previous_manifest_sha256": previous[1],
            }
        ),
        "publication_licenses": [
            {
                "publication_identity": {"kind": kind, "coordinate": coordinate},
                "license_expression": expression,
            }
            for (kind, coordinate), expression in sorted(licenses.items())
        ],
    }


def _write_history_manifest(module: ModuleType, path: Path, value: dict[str, object]) -> str:
    payload = module._canonical_release_manifest_bytes(value)
    path.write_bytes(payload)
    return module.hashlib.sha256(payload).hexdigest()


def test_v1_release_manifest_history_is_offline_exact_and_coordinate_scoped(
    tmp_path: Path,
) -> None:
    module = load_script()
    distribution = ("python-distribution", "riverhog-client")
    image = ("oci-repository", "ghcr.io/nashspence/riverhog")
    v1 = _history_manifest(module, "1.0.0", {distribution: "Apache-2.0"}, None)
    v1_path = tmp_path / "v1.0.0.json"
    v1_digest = _write_history_manifest(module, v1_path, v1)
    v1_1 = _history_manifest(
        module,
        "1.1.0",
        {distribution: "MIT OR Apache-2.0", image: "CAL-1.0"},
        ("v1.0.0", v1_digest),
    )
    v1_1_path = tmp_path / "v1.1.0.json"
    v1_1_digest = _write_history_manifest(module, v1_1_path, v1_1)
    candidate = _history_manifest(
        module,
        "1.2.0",
        {distribution: "Apache-2.0 OR MIT", image: "CAL-1.0 OR Apache-2.0"},
        ("v1.1.0", v1_1_digest),
    )
    expected = {"tag": "v1.1.0", "manifest_sha256": v1_1_digest}

    result = module._verify_v1_manifest_history(
        candidate,
        expected_previous=expected,
        historical_manifest_paths=[v1_1_path, v1_path],
    )

    assert result["manifests"] == 3
    assert result["coordinates"] == 2
    assert result["baselines"]["python-distribution:riverhog-client"] == {
        "tag": "v1.0.0",
        "license_expression": "Apache-2.0",
    }
    assert result["baselines"]["oci-repository:ghcr.io/nashspence/riverhog"] == {
        "tag": "v1.1.0",
        "license_expression": "CAL-1.0",
    }

    with pytest.raises(module.ReleaseError, match="reordered, forked, or extraneous"):
        module._verify_v1_manifest_history(
            candidate,
            expected_previous=expected,
            historical_manifest_paths=[v1_path, v1_1_path],
        )
    with pytest.raises(module.ReleaseError, match="incomplete"):
        module._verify_v1_manifest_history(
            candidate,
            expected_previous=expected,
            historical_manifest_paths=[v1_1_path],
        )

    withdrawn = _history_manifest(
        module,
        "1.2.0",
        {distribution: "MIT", image: "CAL-1.0"},
        ("v1.1.0", v1_1_digest),
    )
    with pytest.raises(module.ReleaseError, match="license grant was withdrawn"):
        module._verify_v1_manifest_history(
            withdrawn,
            expected_previous=expected,
            historical_manifest_paths=[v1_1_path, v1_path],
        )


def test_v1_release_manifest_history_rejects_wrong_heads_duplicates_and_cycles(
    tmp_path: Path,
) -> None:
    module = load_script()
    coordinate = {("python-distribution", "riverhog-client"): "Apache-2.0"}
    v1 = _history_manifest(module, "1.0.0", coordinate, None)
    v1_path = tmp_path / "v1.json"
    v1_digest = _write_history_manifest(module, v1_path, v1)
    candidate = _history_manifest(module, "1.1.0", coordinate, ("v1.0.0", v1_digest))

    with pytest.raises(module.ReleaseError, match="another predecessor"):
        module._verify_v1_manifest_history(
            candidate,
            expected_previous={"tag": "v1.0.0", "manifest_sha256": "0" * 64},
            historical_manifest_paths=[v1_path],
        )
    with pytest.raises(module.ReleaseError, match="duplicated"):
        module._verify_v1_manifest_history(
            candidate,
            expected_previous={"tag": "v1.0.0", "manifest_sha256": v1_digest},
            historical_manifest_paths=[v1_path, v1_path],
        )

    cyclic = _history_manifest(module, "1.0.1", coordinate, ("v1.0.1", "0" * 64))
    cyclic_path = tmp_path / "cycle.json"
    cyclic_digest = _write_history_manifest(module, cyclic_path, cyclic)
    cycle_candidate = _history_manifest(module, "1.1.0", coordinate, ("v1.0.1", cyclic_digest))
    with pytest.raises(module.ReleaseError, match="does not increase"):
        module._verify_v1_manifest_history(
            cycle_candidate,
            expected_previous={"tag": "v1.0.1", "manifest_sha256": cyclic_digest},
            historical_manifest_paths=[cyclic_path],
        )


def test_v1_release_manifest_history_requires_strictly_increasing_versions(
    tmp_path: Path,
) -> None:
    module = load_script()
    coordinate = {("python-distribution", "riverhog-client"): "Apache-2.0"}
    future = _history_manifest(
        module,
        "1.2.0",
        coordinate,
        ("v1.3.0", "0" * 64),
    )
    future_path = tmp_path / "v1.2.0.json"
    future_digest = _write_history_manifest(module, future_path, future)
    candidate = _history_manifest(
        module,
        "1.3.0",
        coordinate,
        ("v1.2.0", future_digest),
    )

    with pytest.raises(module.ReleaseError, match="must increase its predecessor"):
        module._release_history_declaration(
            "1.1.0",
            {"tag": "v1.2.0", "manifest_sha256": future_digest},
        )
    with pytest.raises(module.ReleaseError, match="continuation does not increase"):
        module._verify_v1_manifest_history(
            candidate,
            expected_previous={"tag": "v1.2.0", "manifest_sha256": future_digest},
            historical_manifest_paths=[future_path],
        )


def test_v1_release_manifest_history_rejects_digest_drift_and_coordinate_omission(
    tmp_path: Path,
) -> None:
    module = load_script()
    coordinate = ("python-distribution", "riverhog-client")
    baseline = _history_manifest(module, "1.0.0", {coordinate: "Apache-2.0"}, None)
    baseline_path = tmp_path / "v1.0.0.json"
    baseline_digest = _write_history_manifest(module, baseline_path, baseline)
    candidate = _history_manifest(module, "1.1.0", {coordinate: "Apache-2.0"}, ("v1.0.0", "0" * 64))
    with pytest.raises(module.ReleaseError, match="digest differs"):
        module._verify_v1_manifest_history(
            candidate,
            expected_previous={"tag": "v1.0.0", "manifest_sha256": "0" * 64},
            historical_manifest_paths=[baseline_path],
        )

    omitted = _history_manifest(module, "1.1.0", {}, ("v1.0.0", baseline_digest))
    with pytest.raises(module.ReleaseError, match="omits a previously published"):
        module._verify_v1_manifest_history(
            omitted,
            expected_previous={"tag": "v1.0.0", "manifest_sha256": baseline_digest},
            historical_manifest_paths=[baseline_path],
        )


def test_built_publication_license_evidence_is_exact_and_complete() -> None:
    module = load_script()
    inventory = [
        {
            "publication_identity": {
                "kind": "python-distribution",
                "coordinate": "riverhog-client",
            },
            "license_expression": "Apache-2.0",
        },
        {
            "publication_identity": {
                "kind": "oci-repository",
                "coordinate": "ghcr.io/nashspence/riverhog",
            },
            "license_expression": "CAL-1.0",
        },
    ]
    subjects = [
        {
            "kind": "wheel",
            "distribution": "riverhog-client",
            "license": "Apache-2.0",
        },
        {
            "kind": "sdist",
            "distribution": "riverhog-client",
            "license": "Apache-2.0",
        },
        {
            "kind": "image",
            "name": "ghcr.io/nashspence/riverhog",
            "license": "CAL-1.0",
        },
    ]

    module._verify_built_publication_licenses(subjects, inventory)

    with pytest.raises(module.ReleaseError, match="incomplete"):
        module._verify_built_publication_licenses(subjects[:1] + subjects[2:], inventory)
    changed = [dict(item) for item in subjects]
    changed[0]["license"] = "MIT"
    with pytest.raises(module.ReleaseError, match="differs from its authority"):
        module._verify_built_publication_licenses(changed, inventory)


def test_coordinated_version_application_updates_all_internal_ranges(tmp_path: Path) -> None:
    module = load_script()
    _copy_release_contract(module, tmp_path)
    original_lock = tomllib.loads((tmp_path / "uv.lock").read_text(encoding="utf-8"))

    projects = module.apply_release_version(tmp_path, "1.0.0")

    internal_names = {project.name for project in projects}
    for pyproject in module._workspace_pyprojects(tmp_path):
        metadata = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]
        assert metadata["version"] == "1.0.0"
        for dependency in metadata.get("dependencies", []):
            if module._dependency_name(dependency) in internal_names:
                assert Requirement(dependency).specifier == SpecifierSet(">=1.0,<2.0")
    updated_lock = tomllib.loads((tmp_path / "uv.lock").read_text(encoding="utf-8"))
    original_external = [
        package for package in original_lock["package"] if package["name"] not in internal_names
    ]
    updated_external = [
        package for package in updated_lock["package"] if package["name"] not in internal_names
    ]
    assert updated_external == original_external
    assert {
        package["name"]: package["version"]
        for package in updated_lock["package"]
        if package["name"] in internal_names
    } == dict.fromkeys(internal_names, "1.0.0")
    for package in updated_lock["package"]:
        for requirement in package.get("metadata", {}).get("requires-dist", []):
            if requirement["name"] in internal_names and "specifier" in requirement:
                assert requirement["specifier"] == ">=1.0,<2.0"

    environment = os.environ.copy()
    environment["UV_CACHE_DIR"] = str(tmp_path / "empty-uv-cache")
    subprocess.run(
        ["uv", "lock", "--offline", "--check"],
        cwd=tmp_path,
        check=True,
        env=environment,
    )


def test_v1_release_rail_rejects_another_major() -> None:
    module = load_script()

    with pytest.raises(module.ReleaseError, match="only 1.x.y"):
        module.build_release_plan(REPO_ROOT, "2.0.0", allow_dirty=True)


def test_dry_run_trust_is_scoped_to_the_exact_sha_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_script()
    monkeypatch.setenv("MISE_TRUSTED_CONFIG_PATHS", "/already/trusted")

    assert module._trusted_config_paths(tmp_path) == (
        f"{tmp_path}{module.os.pathsep}/already/trusted"
    )


def test_dry_run_removes_each_temporary_image_tag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    tags = ["dry-run/riverhog:1.0.0", "dry-run/riverhog:sha-example"]
    remaining = set(tags)
    removed: list[str] = []

    monkeypatch.setattr(
        module,
        "_docker_image_exists",
        lambda tag, *, cwd: cwd == tmp_path and tag in remaining,
    )

    def remove(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        assert command[:3] == ["docker", "image", "rm"]
        tag = command[3]
        remaining.remove(tag)
        removed.append(tag)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(module.subprocess, "run", remove)

    module._remove_release_image_tags(tags, cwd=tmp_path)

    assert removed == list(reversed(tags))
    assert remaining == set()


def test_source_archive_is_deterministic_and_commit_time_normalized(tmp_path: Path) -> None:
    module = load_script()
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "README.md").write_text("Riverhog\n", encoding="utf-8")
    script = checkout / "run"
    script.write_text("#!/bin/sh\n", encoding="utf-8")
    script.chmod(0o755)
    first = tmp_path / "first.tar.gz"
    second = tmp_path / "second.tar.gz"

    module._write_source_archive(checkout, first, version="1.0.0", source_epoch=1234567890)
    module._write_source_archive(checkout, second, version="1.0.0", source_epoch=1234567890)

    assert first.read_bytes() == second.read_bytes()
    with tarfile.open(first, mode="r:gz") as archive:
        members = archive.getmembers()
    assert [member.name for member in members] == [
        "riverhog-1.0.0",
        "riverhog-1.0.0/README.md",
        "riverhog-1.0.0/run",
    ]
    assert all(member.mtime == 1234567890 for member in members)
    assert all(member.uid == 0 and member.gid == 0 for member in members)
    assert members[-1].mode == 0o755


def test_artifact_notice_bundle_is_deterministic_and_subject_bound(tmp_path: Path) -> None:
    module = load_script()
    record = {
        "kind": "wheel",
        "name": "python/riverhog_client-1.0.0-py3-none-any.whl",
        "sha256": "2" * 64,
    }
    components = [
        {
            "kind": "python",
            "name": "riverhog-client",
            "version": "1.0.0",
            "notices": [
                {
                    "source": "riverhog_client-1.0.0.dist-info/licenses/LICENSE",
                    "content": b"license text\n",
                }
            ],
        }
    ]
    first = tmp_path / "first.tar.gz"
    second = tmp_path / "second.tar.gz"

    module._write_notice_bundle(first, record, components, source_epoch=1234567890)
    module._write_notice_bundle(second, record, components, source_epoch=1234567890)

    assert first.read_bytes() == second.read_bytes()
    assert module._verify_notice_bundle(first, record) == 1
    with tarfile.open(first, mode="r:gz") as archive:
        index_stream = archive.extractfile("NOTICE.json")
        assert index_stream is not None
        index = json.loads(index_stream.read())
    assert index["format"] == "riverhog-artifact-notices/v1"
    assert index["subject"] == record


def test_required_artifact_notice_input_fails_closed(tmp_path: Path) -> None:
    module = load_script()
    record = {
        "kind": "wheel",
        "name": "python/artifact.whl",
        "sha256": "3" * 64,
    }

    with pytest.raises(module.ReleaseError, match="notice input is absent"):
        module._write_subject_notices(tmp_path, [record], source_epoch=1234567890)


def test_generated_install_reference_has_a_file_sbom(tmp_path: Path) -> None:
    module = load_script()
    record = {
        "kind": "install-reference",
        "name": "installation/gogurt-listener-v1.0.0.md",
        "sha256": "4" * 64,
        "version": "1.0.0",
        "license": "Apache-2.0",
    }

    module._write_subject_sboms(
        tmp_path,
        [record],
        source_sha="1" * 40,
        created="2009-02-13T23:31:30Z",
    )

    sbom = json.loads((tmp_path / record["sbom"]).read_text(encoding="utf-8"))
    assert sbom["packages"][0]["primaryPackagePurpose"] == "FILE"


def test_release_evidence_is_complete_and_minisign_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_script()
    output = tmp_path / "evidence"
    output.mkdir()
    payload = output / "artifact.whl"
    payload.write_bytes(b"release artifact\n")
    source_payload = output / "artifact.tar.gz"
    source_payload.write_bytes(b"release source artifact\n")
    keys = tmp_path / "keys"
    keys.mkdir()
    public_key = keys / "release.pub"
    signing_key = keys / "release.key"
    subprocess.run(
        [
            "minisign",
            "-G",
            "-W",
            "-p",
            str(public_key),
            "-s",
            str(signing_key),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    records = [
        {
            "kind": "wheel",
            "name": "artifact.whl",
            "sha256": module._sha256_file(payload),
            "size": payload.stat().st_size,
            "distribution": "riverhog-client",
            "version": "1.0.0",
            "license": "CAL-1.0",
            "dependencies": [],
            "_components": [
                {
                    "kind": "python",
                    "name": "riverhog-protocol",
                    "version": "1.0.0",
                    "license": "CAL-1.0",
                }
            ],
            "_notice_components": [
                {
                    "kind": "python",
                    "name": "riverhog-client",
                    "version": "1.0.0",
                    "notices": [
                        {
                            "source": "riverhog_client.dist-info/licenses/LICENSE",
                            "content": b"CAL\n",
                        }
                    ],
                }
            ],
        },
        {
            "kind": "sdist",
            "name": "artifact.tar.gz",
            "sha256": module._sha256_file(source_payload),
            "size": source_payload.stat().st_size,
            "distribution": "riverhog-client",
            "version": "1.0.0",
            "license": "CAL-1.0",
            "dependencies": [],
            "_components": [],
        },
    ]
    publication = {
        "distributions": {
            "riverhog-client": {
                "publication_identity": {
                    "kind": "python-distribution",
                    "coordinate": "riverhog-client",
                },
                "license_expression": "CAL-1.0",
            }
        },
        "runtime_images": {},
    }
    install_manifest = {"format": "riverhog-installation/v1"}
    (output / "install-manifest.json").write_text(
        module.json.dumps(install_manifest), encoding="utf-8"
    )
    monkeypatch.setattr(
        module.installation,
        "verify_installation_artifacts",
        lambda _output, _manifest: None,
    )

    verification = module._generate_release_evidence(
        REPO_ROOT,
        output,
        records,
        version="1.0.0",
        source_sha="1" * 40,
        source_epoch=1234567890,
        spdx_created="2009-02-13T23:31:30Z",
        install_manifest=install_manifest,
        signing_key=signing_key,
        public_key=public_key,
        publication=publication,
    )

    assert verification["subjects"] == 2
    assert verification["notice_components"] == 1
    assert verification["license_coordinates"] == 1
    assert verification["release_history_manifests"] == 1
    assert verification["signature_verified"] is True
    assert (output / "SHA256SUMS.minisig").is_file()
    assert (output / records[0]["sbom"]).is_file()
    assert (output / records[0]["notices"]).is_file()
    manifest = module.json.loads((output / "release-manifest.json").read_text(encoding="utf-8"))
    assert "published" not in manifest
    assert manifest["subjects"] == sorted(
        records, key=lambda item: (str(item["kind"]), str(item["name"]))
    )
    assert manifest["v1_history"] == {"kind": "genesis"}
    assert manifest["publication_licenses"] == module._publication_license_inventory(publication)
    assert manifest["contract"] == {
        "file": "riverhog-v1-contract.tar.gz",
        "sha256": module._sha256_file(output / "riverhog-v1-contract.tar.gz"),
    }
    with module.tarfile.open(output / "riverhog-v1-contract.tar.gz", mode="r:gz") as archive:
        contract_members = {member.name for member in archive.getmembers() if member.isfile()}
    assert contract_members == {
        "riverhog-v1.json",
        "riverhog-v1-audit.json",
        *(
            path.relative_to(REPO_ROOT / "qualification/contracts").as_posix()
            for path in (REPO_ROOT / "qualification/contracts/riverhog-v1").rglob("*")
            if path.is_file()
        ),
    }

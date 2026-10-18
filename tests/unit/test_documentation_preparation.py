"""Prepared sources, real wheels and an isolated sdist rebuild keep native behavior."""

import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from contract_atlas.documentation_native import owned_python_declaration  # noqa: E402
from contract_atlas.documentation_preparation import (  # noqa: E402
    PREPARATION_FORMAT,
    _stage_module,
    stage_documentation,
)
from contract_atlas.model import ContractAtlasError, canonical_bytes  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PROSE = "Literal 50% %(prog)s {value} [brackets] Résumé 東京."
SOURCE = '''from __future__ import annotations
import argparse
import typer
from fastapi import FastAPI
from pydantic import BaseModel

__all__ = ["transform", "Record", "FLAG"]
FLAG = 17

class Record(BaseModel):
    """A record. The description wire field is required."""
    description: str

def transform(value: int, factor: int = 3) -> int:
    """Multiply. No mutable global state is changed."""
    return value * factor

def _parser():
    parser = argparse.ArgumentParser(prog="native-fixture", description="Developer context.")
    parser.add_argument("--value", type=int, default=3)
    parser.add_argument("--secret", help=argparse.SUPPRESS)
    return parser

app = typer.Typer(add_completion=False)

@app.command()
def run(value: int = 3):
    """Original command context."""
    print(transform(value))

def create_app():
    application = FastAPI()
    @application.post("/records", operation_id="create_record")
    def create(record: Record):
        return record
    return application

http_app = create_app()

group = typer.Typer(add_completion=False)
child = typer.Typer(add_completion=False)
group.add_typer(child, name="child")

@child.command()
def compute(value: int = 2):
    print(transform(value))
'''


def instructions(source):
    return {
        "source_sha256": hashlib.sha256(source).hexdigest(),
        "python": {"transform": {"docstring": PROSE}, "Record": {"docstring": "Record prose."}},
        "references": {"native_fixture.FLAG": {"docstring": "Immutable constant reference."}},
        "cli": {
            "_parser": {
                "root": "native-fixture",
                "commands": {
                    "native-fixture": {
                        "summary": PROSE,
                        "plain": "Exact help body.",
                        "parameters": {"value": PROSE},
                    }
                },
            }
        },
        "openapi": {
            "create_app": {
                "/paths/~1records/post": {"summary": PROSE, "markdown": "Exact API detail."}
            }
        },
        "typer": {
            "app": {
                "root": "native-typer",
                "commands": {
                    "native-typer": {
                        "summary": PROSE,
                        "plain": "Exact Typer body.",
                        "parameters": {"value": PROSE},
                    }
                },
            },
            "group": {
                "root": "native-group",
                "commands": {
                    "native-group": {"summary": "Group prose.", "parameters": {}},
                    "native-group child": {"summary": "Child prose.", "parameters": {}},
                    "native-group child compute": {
                        "summary": PROSE,
                        "parameters": {"value": PROSE},
                    },
                },
            },
        },
    }


def invoke(argv, *, cwd=None):
    return subprocess.run(
        argv,
        cwd=cwd,
        check=True,
        text=True,
        capture_output=True,
        env={k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "VIRTUAL_ENV"}},
    )


@pytest.fixture(scope="module")
def prepared_native(tmp_path_factory):
    scratch = tmp_path_factory.mktemp("documentation-native-products")
    source = scratch / "source"
    module = source / "src/native_fixture/__init__.py"
    module.parent.mkdir(parents=True)
    module.write_text(SOURCE)
    (source / "pyproject.toml").write_text("""[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
[project]
name = "riverhog-documentation-native-fixture"
version = "1.0.0"
description = "Developer purpose."
readme = {text = "Developer context.", content-type = "text/markdown"}
requires-python = ">=3.12"
[tool.hatch.build.targets.wheel]
packages = ["src/native_fixture"]
""")
    plan = {
        "format": PREPARATION_FORMAT,
        "tag": "v1.0.0",
        "modules": {"src/native_fixture/__init__.py": instructions(SOURCE.encode())},
        "metadata": {".": {"summary": PROSE, "markdown": "Reviewed long description."}},
    }
    record = stage_documentation(source, plan)
    distributions = scratch / "dist"
    invoke(["uv", "build", "--no-sources", "--out-dir", str(distributions)], cwd=source)
    invoke(
        ["uv", "build", "--wheel", "--no-sources", "--out-dir", str(distributions)],
        cwd=ROOT / "packages/release-documentation-lib",
    )
    environment = scratch / "environment"
    invoke(["uv", "venv", "--python", sys.executable, str(environment)])
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    invoke(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            *map(str, distributions.glob("*.whl")),
            *(
                name + "==" + importlib.metadata.version(name)
                for name in ("fastapi", "typer", "httpx", "pydantic")
            ),
        ]
    )
    return scratch, source, python, record


def test_actual_installed_native_destinations_and_behavior(prepared_native):
    scratch, source, python, _ = prepared_native
    script = """import argparse, inspect, json, sys
import importlib.metadata
from pathlib import Path
import native_fixture as native
from fastapi.testclient import TestClient
from release_documentation_lib import markdown_text
from typer.main import get_command
from typer.testing import CliRunner

assert Path(native.__file__).is_relative_to(Path(sys.prefix))
assert native.transform(7) == 21 and native.FLAG == 17
assert str(inspect.signature(native.transform)) == "(value: 'int', factor: 'int' = 3) -> 'int'"
assert native.transform.__doc__ == PROSE
parser = native._parser()
assert parser.parse_args(["--value", "9", "--secret", "hidden"]).value == 9
assert PROSE in parser.format_help() and "--secret" not in parser.format_help()
assert type(native.app).__name__ == "Typer" and native.run.__name__ == "run"
runner = CliRunner()
help_result = runner.invoke(native.app, ["--help"], color=False)
assert help_result.exit_code == 0, help_result.output
assert "[brackets]" in help_result.output and "50%" in help_result.output
assert runner.invoke(native.app, ["--value", "8"]).output.strip() == "24"
error = runner.invoke(native.app, ["--value", "bad"])
assert error.exit_code == 2 and "Invalid value" in error.output
assert get_command(native.app).params[0].type.name == "integer"
assert native.group.__class__.__name__ == "Typer"
nested = runner.invoke(native.group, ["child", "compute", "--help"], color=False)
assert nested.exit_code == 0 and "[brackets]" in nested.output
assert runner.invoke(native.group, ["child", "compute", "--value", "4"]).output.strip() == "12"
assert native.http_app.openapi()["paths"]["/records"]["post"]["summary"] == PROSE
reference = native._riverhog_release_resource["references"]["native_fixture.FLAG"]
assert reference["docstring"] == "Immutable constant reference."
with TestClient(native.create_app()) as client:
    assert client.post("/records", json={"description": "wire"}).json() == {"description": "wire"}
    document = client.get("/openapi.json").json()
description = document["paths"]["/records"]["post"]["description"]
assert description == markdown_text(PROSE) + "\\n\\nExact API detail."
assert "description" in document["components"]["schemas"]["Record"]["properties"]
assert document["components"]["schemas"]["Record"]["required"] == ["description"]
metadata = importlib.metadata.metadata("riverhog-documentation-native-fixture")
assert metadata["Summary"] == PROSE
assert metadata.get_payload().strip() == "Reviewed long description."
values = {"help": help_result.output, "prose": native.transform.__doc__}
print(json.dumps(values, ensure_ascii=False))
"""
    script = "PROSE = " + repr(PROSE) + "\n" + script
    observed = json.loads(invoke([str(python), "-I", "-c", script], cwd=scratch).stdout)
    assert observed["prose"] == PROSE
    assert (
        "No mutable global state is changed."
        in (source / "src/native_fixture/__init__.py").read_text()
    )


def test_sdist_rebuild_is_offline_and_self_contained(prepared_native):
    scratch, source, python, _ = prepared_native
    archive = next((scratch / "dist").glob("riverhog_documentation_native_fixture*.tar.gz"))
    rebuilt = scratch / "rebuilt"
    rebuilt.mkdir()
    with tarfile.open(archive) as stream:
        stream.extractall(rebuilt, filter="data")
    extracted = next(rebuilt.iterdir())
    assert not (extracted / ".git").exists()
    rebuilt_dist = scratch / "rebuilt-dist"
    invoke(
        ["uv", "build", "--offline", "--no-sources", "--wheel", "--out-dir", str(rebuilt_dist)],
        cwd=extracted,
    )
    assert (
        next(rebuilt_dist.glob("*.whl")).read_bytes()
        == next((scratch / "dist").glob("riverhog_documentation_native_fixture*.whl")).read_bytes()
    )
    invoke(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--reinstall",
            "--no-deps",
            str(next(rebuilt_dist.glob("*.whl"))),
        ]
    )
    assert (
        invoke(
            [
                str(python),
                "-I",
                "-c",
                "import native_fixture; print(native_fixture.transform.__doc__)",
            ],
            cwd=rebuilt,
        ).stdout.strip()
        == PROSE
    )


@pytest.mark.parametrize("mutation", ["missing", "corrupt"])
def test_final_package_refuses_missing_or_corrupt_local_resource(prepared_native, mutation):
    scratch, _, python, _ = prepared_native
    path = Path(
        invoke(
            [str(python), "-I", "-c", "import native_fixture; print(native_fixture.__file__)"],
            cwd=scratch,
        ).stdout.strip()
    )
    resource = next(path.parent.glob("_release_documentation_*.json"))
    original = resource.read_bytes()
    try:
        if mutation == "missing":
            resource.unlink()
        else:
            resource.write_bytes(b"{}")
        result = subprocess.run(
            [str(python), "-I", "-c", "import native_fixture"],
            cwd=scratch,
            text=True,
            capture_output=True,
        )
        assert result.returncode != 0 and "required package-local release prose" in result.stderr
    finally:
        resource.write_bytes(original)


def test_staging_is_deterministic_and_refuses_unowned_changes(tmp_path):
    source = SOURCE.encode()
    raw = canonical_bytes({"format": "riverhog-package-documentation/v1", **instructions(source)})
    first = _stage_module(
        source, instructions(source), "resource.json", hashlib.sha256(raw).hexdigest()
    )
    assert first == _stage_module(
        source, instructions(source), "resource.json", hashlib.sha256(raw).hexdigest()
    )
    module = tmp_path / "src/native_fixture/__init__.py"
    module.parent.mkdir(parents=True)
    module.write_bytes(source + b"\nFLAG = 18\n")
    with pytest.raises(ContractAtlasError, match="source changed"):
        stage_documentation(
            tmp_path,
            {
                "format": PREPARATION_FORMAT,
                "tag": "v1.0.0",
                "modules": {"src/native_fixture/__init__.py": instructions(source)},
                "metadata": {},
            },
        )


def test_dynamic_or_third_party_python_objects_are_explicit_references():
    assert owned_python_declaration(str) is None
    assert owned_python_declaration(17) is None


def test_adapter_library_can_load_its_own_staged_resource(tmp_path):
    source = (
        ROOT / "packages/release-documentation-lib/src/release_documentation_lib/__init__.py"
    ).read_bytes()
    instruction = {
        "python": {"load_resource": {"docstring": PROSE}},
        "references": {},
        "cli": {},
        "openapi": {},
        "typer": {},
    }
    payload = canonical_bytes({"format": "riverhog-package-documentation/v1", **instruction})
    package = tmp_path / "release_documentation_lib"
    package.mkdir()
    (package / "prose.json").write_bytes(payload)
    (package / "__init__.py").write_bytes(
        _stage_module(source, instruction, "prose.json", hashlib.sha256(payload).hexdigest())
    )
    script = (
        "import sys; sys.path.insert(0, "
        + repr(str(tmp_path))
        + "); import release_documentation_lib as native; assert native.load_resource.__doc__ == "
        + repr(PROSE)
    )
    invoke([sys.executable, "-I", "-c", script], cwd=tmp_path)


def test_staging_refuses_path_escape_before_writing(tmp_path):
    destination = tmp_path / "prepared-source"
    destination.mkdir()
    plan = {
        "format": PREPARATION_FORMAT,
        "tag": "v1.0.0",
        "modules": {"../outside.py": instructions(SOURCE.encode())},
        "metadata": {},
    }
    with pytest.raises(ContractAtlasError, match="unsafe corpus path"):
        stage_documentation(destination, plan)
    assert not list(destination.iterdir())


def test_repository_plan_stages_every_selected_native_declaration(tmp_path, documented_source_plan):
    import shutil
    import tomllib

    plan = documented_source_plan
    destination = tmp_path / "prepared-source"
    for name in plan["modules"]:
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, path)
    before = {}
    for name in plan["metadata"]:
        path = destination / name / "pyproject.toml"
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name / "pyproject.toml", path)
        before[name] = tomllib.loads(path.read_text())["project"]
    record = stage_documentation(destination, plan)
    assert record["files"]
    for name in plan["modules"]:
        assert "_riverhog_release_resource" in (destination / name).read_text()
    for name, expected in before.items():
        actual = tomllib.loads((destination / name / "pyproject.toml").read_text())["project"]
        assert {k: v for k, v in actual.items() if k not in {"description", "readme"}} == {
            k: v for k, v in expected.items() if k not in {"description", "readme"}
        }


def test_documented_source_replays_and_all_installed_native_surfaces_match(
    tmp_path, documented_source_plan, generated_contract_closure
):
    """Synthetic prose exercises real repository products, without selecting a release."""
    import copy
    import shutil

    import release
    from contract_atlas.documentation import source_ledger
    from contract_atlas.documentation_artifacts import (
        READER_DEPENDENCIES,
        native_request,
        verify_readout_semantics,
    )
    from contract_atlas.documentation_native import expected_outputs, select_observations
    from contract_atlas.generation import documented_prepared_source

    from tests.documentation_fixtures import synthetic_corpus

    code = generated_contract_closure["bundle"].closure
    files = synthetic_corpus(code)
    plan = copy.deepcopy(documented_source_plan)
    plan["source_capture"] = {"tag": plan["tag"], "commit": "b" * 40, "files": source_ledger(files)}
    original = tmp_path / "original"
    original.mkdir()
    paths = (
        subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT
        )
        .decode()
        .split("\0")
    )
    for name in filter(None, paths):
        destination = original / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    invoke(["git", "init", "-q", str(original)])
    git = [
        "git",
        "-C",
        str(original),
        "-c",
        "user.name=Documentation fixture",
        "-c",
        "user.email=documentation@example.invalid",
        "-c",
        "commit.gpgsign=false",
    ]
    invoke([*git, "add", "."])
    invoke([*git, "commit", "-qm", "Synthetic source replay fixture"])
    revision = invoke([*git, "rev-parse", "HEAD"]).stdout.strip()
    epoch = int(invoke([*git, "show", "-s", "--format=%ct", revision]).stdout)
    prepared = tmp_path / "prepared"
    shutil.copytree(original, prepared, ignore=shutil.ignore_patterns(".git"))
    release.apply_release_version(prepared, "1.0.0")
    invoke(["uv", "lock", "--offline"], cwd=prepared)
    # Observe the version-prepared, unaugmented products independently; their
    # package versions and served API version are already the selected version.
    unaugmented = tmp_path / "unaugmented"
    invoke(
        ["uv", "build", "--all-packages", "--wheel", "--out-dir", str(unaugmented)], cwd=prepared
    )
    environment = tmp_path / "installed"
    invoke(["uv", "venv", "--python", sys.executable, str(environment)])
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    constraints = tmp_path / "constraints.txt"
    invoke(
        [
            "uv",
            "export",
            "--locked",
            "--all-packages",
            "--all-groups",
            "--no-emit-workspace",
            "--no-hashes",
            "--output-file",
            str(constraints),
        ],
        cwd=ROOT,
    )
    invoke(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--constraint",
            str(constraints),
            *map(str, unaugmented.glob("*.whl")),
            *READER_DEPENDENCIES,
        ]
    )
    request = native_request(code, plan)
    request_file = tmp_path / "request.json"
    request_file.write_bytes(canonical_bytes(request))
    code_file = tmp_path / "source-closure.json"
    code_file.write_bytes(canonical_bytes(code))
    source_readout = tmp_path / "source-semantics.json"
    program = (
        "import sys; sys.path.insert(0, "
        + repr(str(ROOT / "scripts"))
        + ")\n"
        + """
import json
from pathlib import Path
from contract_atlas.documentation_artifacts import source_native_semantics
from contract_atlas.documentation_native import semantic_frame
from contract_atlas.model import canonical_bytes
from documentation_readouts import installed
from operation_qualification import application_surfaces
code, request = [json.loads(Path(name).read_bytes()) for name in sys.argv[1:3]]
for name in request['modules']:
    installed(name)
observed = source_native_semantics(code, request)
observed['openapi'] = {
    surface.name: semantic_frame(surface.app.openapi())
    for surface in application_surfaces()
}
Path(sys.argv[3]).write_bytes(canonical_bytes(observed))
"""
    )
    invoke(
        [str(python), "-I", "-c", program, str(code_file), str(request_file), str(source_readout)],
        cwd=tmp_path,
    )
    staging = stage_documentation(prepared, plan, source_files=files)
    archive = tmp_path / "source.tar.gz"
    release._write_source_archive(prepared, archive, version="1.0.0", source_epoch=epoch)
    proof = documented_prepared_source(original, revision, "1.0.0", archive, plan, files)
    assert (
        proof["documented_source_archive_sha256"]
        == hashlib.sha256(archive.read_bytes()).hexdigest()
    )
    assert (
        proof["documentation_transform_sha256"]
        == hashlib.sha256(canonical_bytes(staging)).hexdigest()
    )

    distributions = tmp_path / "distributions"
    invoke(
        ["uv", "build", "--all-packages", "--wheel", "--out-dir", str(distributions)], cwd=prepared
    )
    invoke(
        [
            "uv",
            "pip",
            "install",
            "--reinstall",
            "--no-deps",
            "--python",
            str(python),
            "--constraint",
            str(constraints),
            *map(str, distributions.glob("*.whl")),
        ]
    )
    readout_file = tmp_path / "readouts.json"
    invoke(
        [
            str(python),
            "-I",
            str(ROOT / "scripts/documentation_readouts.py"),
            "--request",
            str(request_file),
            "--output",
            str(readout_file),
        ],
        cwd=tmp_path,
    )
    actual = json.loads(readout_file.read_bytes())
    assert verify_readout_semantics(
        json.loads(source_readout.read_bytes()), actual["semantics"], request
    )
    expected = {
        key: value
        for key, value in expected_outputs(plan["slots"]).items()
        if not key.startswith("oci/")
    }
    assert select_observations(expected, actual["observed"]) == expected
    assert actual["cli_journeys"] and all(
        value["exit"] == 0 for value in actual["cli_journeys"].values()
    )
    # The fixture is machinery evidence, not a prepared or published product.
    assert not (original / "release-manifest.json").exists()
    archive.write_bytes(archive.read_bytes() + b"altered")
    with pytest.raises(ContractAtlasError, match="differs from exact staged preparation"):
        documented_prepared_source(original, revision, "1.0.0", archive, plan, files)

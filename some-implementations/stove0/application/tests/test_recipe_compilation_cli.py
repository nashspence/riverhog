"""Local recipe tools emit the production compiler's exact executable documents."""

from __future__ import annotations

import json

import pytest
from a_stove0_cli import main
from jsonschema import Draft202012Validator
from stove0_protocol import canonical_json_bytes
from stove0_recipe_config.catalog import CompiledRecipeCatalog, load_recipe_catalog
from stove0_recipe_config.source_map import recipe_source_map
from typer.testing import CliRunner

SOURCE = """format: stove0-recipe-source-catalog/v1
resources: {}
recipes:
  decision:
    format: stove0-recipe/v1
    id: example.decision/v1
    revision: 1
    description: Auxiliary description.
    decisions:
      - when: true
        no_output:
          code: example.checked/v1
          message: No target is required.
"""


def test_compile_validate_explain_are_offline_and_round_trip_exact_documents(tmp_path, monkeypatch):
    def no_client(*_args, **_kwargs):
        pytest.fail("offline compilation tried to construct a network client")

    monkeypatch.setattr(main, "Stove0ApiClient", no_client)
    path, output, source_map = (
        tmp_path / name for name in ("source.yaml", "compiled.json", "map.json")
    )
    path.write_text(SOURCE)
    runner = CliRunner()
    compiled = runner.invoke(
        main.app,
        [
            "--json",
            "recipe",
            "compile",
            str(path),
            "--output",
            str(output),
            "--source-map",
            str(source_map),
        ],
    )
    assert compiled.exit_code == 0, compiled.output
    document = json.loads(compiled.stdout)
    assert compiled.stdout.encode() == canonical_json_bytes(document) + b"\n"
    catalog = CompiledRecipeCatalog.model_validate(document)
    assert catalog == CompiledRecipeCatalog.load(output)
    assert output.read_bytes() == compiled.stdout.encode()
    assert catalog.recipes[0].contract.outcomes.normal is None
    assert document["recipes"][0]["contract"]["outcomes"]["normal"] is None
    for command, authority in (("validate", "recipe validate"), ("explain", "recipe explain")):
        result = runner.invoke(main.app, ["--json", "recipe", command, str(output)])
        assert result.exit_code == 0, result.output
        value = json.loads(result.stdout)
        Draft202012Validator(
            main._CLI_RESULT_CONTRACT["output_authorities"][authority]["schema"]
        ).validate(value)
        assert value["catalog_sha256"] == catalog.sha256
    Draft202012Validator(
        main._CLI_RESULT_CONTRACT["output_authorities"]["recipe compile"]["schema"]
    ).validate(document)
    assert (
        json.loads(source_map.read_bytes())["locations"]["/recipes/decision/decisions/0/when"][
            "line"
        ]
        == "10"
    )


def test_source_locations_and_descriptions_do_not_change_compiled_meaning(tmp_path):
    path = tmp_path / "source.yaml"
    path.write_text(SOURCE)
    first = load_recipe_catalog(path)
    first_locations = recipe_source_map(path)
    path.write_text(
        "# A comment moves the source locations.\n"
        + SOURCE.replace(
            "Auxiliary description.",
            "A changed auxiliary description.",
        )
    )
    second = load_recipe_catalog(path)
    assert first == second and first.sha256 == second.sha256
    assert first_locations.locations != recipe_source_map(path).locations


def test_local_tools_reject_retired_grammar_and_preserve_sources_on_path_collision(tmp_path):
    path = tmp_path / "source.yaml"
    path.write_text("format: stove0-recipe-catalog/v1\nrecipes: []\n")
    runner = CliRunner()
    result = runner.invoke(main.app, ["--json", "recipe", "compile", str(path)])
    assert result.exit_code == 1 and not result.stdout
    assert "current source or compiled format" in result.stderr
    path.write_text(SOURCE)
    result = runner.invoke(
        main.app, ["--json", "recipe", "compile", str(path), "--output", str(path)]
    )
    assert result.exit_code == 1 and not result.stdout
    assert path.read_text() == SOURCE


def _independent_sources(tmp_path):
    source = tmp_path / "recipe.yaml"
    source.write_text("\n".join(line[4:] for line in SOURCE.splitlines()[4:]) + "\n")
    resources = tmp_path / "resources.yaml"
    resources.write_text("format: stove0-recipe-resources/v1\nresources: {}\n")
    auxiliary = tmp_path / "auxiliary.yaml"
    auxiliary.write_text(source.read_text().replace("example.decision/v1", "example.auxiliary/v1"))
    return source, resources, auxiliary


def test_separate_sources_and_exact_resources_use_one_offline_compiler(tmp_path, monkeypatch):
    def no_client(*args, **kwargs):
        pytest.fail("offline assembly constructed a network client")

    monkeypatch.setattr(main, "Stove0ApiClient", no_client)
    source, resources, auxiliary = _independent_sources(tmp_path)
    runner = CliRunner()
    assembly = [
        str(source),
        "--resources",
        str(resources),
        "--recipe",
        "auxiliary=" + str(auxiliary),
    ]
    result = runner.invoke(
        main.app,
        [
            "--json",
            "recipe",
            "compile",
            *assembly,
            "--source-map",
            str(tmp_path / "locations.json"),
        ],
    )
    assert result.exit_code == 0, result.output
    compiled = CompiledRecipeCatalog.model_validate(json.loads(result.stdout))
    assert len(compiled.recipes) == 2
    original = load_recipe_catalog(
        source, resources_path=resources, recipe_paths={"auxiliary": auxiliary}
    )
    assert compiled == original
    locations = json.loads((tmp_path / "locations.json").read_text())["locations"]
    assert locations["/recipes/main/decisions/0/when"]["source"] == str(source)
    assert locations["/recipes/auxiliary/decisions/0/when"]["source"] == str(auxiliary)
    for command in ("validate", "explain"):
        checked = runner.invoke(main.app, ["--json", "recipe", command, *assembly])
        assert checked.exit_code == 0, checked.output
        assert json.loads(checked.stdout)["catalog_sha256"] == compiled.sha256
    from_resources = runner.invoke(
        main.app,
        [
            "--json",
            "recipe",
            "compile",
            str(resources),
            "--recipe",
            "main=" + str(source),
            "--recipe",
            "auxiliary=" + str(auxiliary),
        ],
    )
    assert from_resources.exit_code == 0, from_resources.output
    assert json.loads(from_resources.stdout) == json.loads(result.stdout)


def test_semantic_compile_errors_locate_the_original_recipe_before_success(tmp_path):
    source, resources, _ = _independent_sources(tmp_path)
    source.write_text("""format: stove0-recipe/v1
id: example.bad/v1
revision: 1
fork:
  deliver:
    call:
      operation: missing
""")
    result = CliRunner().invoke(
        main.app, ["--json", "recipe", "compile", str(source), "--resources", str(resources)]
    )
    assert result.exit_code == 1 and not result.stdout
    assert str(source) + ":7:" in result.stderr
    assert "/recipes/main/fork/deliver/call/operation" in result.stderr
    assert "resource missing" in result.stderr


@pytest.mark.parametrize("destination", ["resources", "auxiliary"])
def test_assembly_cannot_overwrite_any_source_input(tmp_path, destination):
    source, resources, auxiliary = _independent_sources(tmp_path)
    target = resources if destination == "resources" else auxiliary
    original = target.read_bytes()
    result = CliRunner().invoke(
        main.app,
        [
            "--json",
            "recipe",
            "compile",
            str(source),
            "--resources",
            str(resources),
            "--recipe",
            "auxiliary=" + str(auxiliary),
            "--output",
            str(target),
        ],
    )
    assert result.exit_code == 1 and not result.stdout
    assert target.read_bytes() == original


def test_assembly_rejects_duplicate_aliases_and_preserves_closed_compiled_input(tmp_path):
    source, resources, auxiliary = _independent_sources(tmp_path)
    runner = CliRunner()
    duplicate = runner.invoke(
        main.app,
        [
            "--json",
            "recipe",
            "compile",
            str(resources),
            "--recipe",
            "same=" + str(source),
            "--recipe",
            "same=" + str(auxiliary),
        ],
    )
    assert duplicate.exit_code == 1 and "more than once" in duplicate.stderr
    compiled = tmp_path / "compiled.json"
    compiled.write_bytes(canonical_json_bytes(load_recipe_catalog(source).model_dump(mode="json")))
    result = runner.invoke(
        main.app,
        ["--json", "recipe", "compile", str(compiled), "--recipe", "auxiliary=" + str(auxiliary)],
    )
    assert result.exit_code == 1 and "cannot assemble" in result.stderr

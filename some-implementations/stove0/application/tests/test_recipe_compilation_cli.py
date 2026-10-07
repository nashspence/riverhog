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

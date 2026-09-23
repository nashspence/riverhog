from __future__ import annotations

import argparse
import copy
import os
import sys
from pathlib import Path

import pytest
from a_riverhog_recovery_tool.cli import _parser as recovery_parser
from a_stove0_cli.main import app as stove0_app
from gogurt.cli import app as gogurt_app
from riverhog_api.app import _parser as riverhog_parser
from stove0_api.app import _parser as stove0_parser
from typer._click.exceptions import BadParameter, UsageError
from typer.main import get_command

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import contract_freeze as contract  # noqa: E402


def test_recovery_mutual_exclusion_changes_acceptance_and_projection() -> None:
    parser = recovery_parser()
    relaxed = copy.deepcopy(parser)
    relaxed._mutually_exclusive_groups.clear()
    argv = ["archive", "--description-only", "--tags-only"]
    with pytest.raises(SystemExit) as rejected:
        parser.parse_args(argv)
    assert rejected.value.code == 2
    accepted = relaxed.parse_args(argv)
    assert accepted.description_only and accepted.tags_only
    projected = contract._argparse_command(parser)
    assert projected != contract._argparse_command(relaxed)
    group = projected["mutually_exclusive_groups"][0]
    assert group["required"] is False
    assert [projected["parameters"][index]["options"] for index in group["parameters"]] == [
        ["--description-only"],
        ["--tags-only"],
    ]


def test_requiring_a_recovery_metadata_mode_changes_acceptance_and_projection() -> None:
    parser = recovery_parser()
    required = copy.deepcopy(parser)
    required._mutually_exclusive_groups[0].required = True
    assert parser.parse_args(["archive"]).archive == Path("archive")
    with pytest.raises(SystemExit):
        required.parse_args(["archive"])
    assert required.parse_args(["archive", "--tags-only"]).tags_only
    assert contract._argparse_command(parser) != contract._argparse_command(required)


def test_mutually_exclusive_group_requires_accounted_parameter_members() -> None:
    parser = recovery_parser()
    group = parser._mutually_exclusive_groups[0]
    parser._actions.remove(group._group_actions[0])
    with pytest.raises(contract.ContractFreezeError, match="unaccounted member"):
        contract._argparse_command(parser)


def test_recovery_abbreviation_changes_acceptance_and_projection() -> None:
    parser = recovery_parser()
    exact = copy.deepcopy(parser)
    exact.allow_abbrev = False
    argv = ["archive", "--description"]
    assert parser.parse_args(argv).description_only
    with pytest.raises(SystemExit):
        exact.parse_args(argv)
    assert contract._argparse_command(parser) != contract._argparse_command(exact)


@pytest.mark.parametrize("factory", [riverhog_parser, stove0_parser])
def test_argparse_subcommand_requirement_changes_acceptance_and_projection(factory) -> None:
    parser = factory()
    changed = copy.deepcopy(parser)
    action = next(a for a in changed._actions if isinstance(a, argparse._SubParsersAction))
    action.required = not action.required
    required, optional = (changed, parser) if action.required else (parser, changed)
    with pytest.raises(SystemExit):
        required.parse_args([])
    assert optional.parse_args([]).command is None
    assert contract._argparse_command(required)["subcommand_required"] is True
    assert contract._argparse_command(optional)["subcommand_required"] is False


def test_typer_subcommand_requirement_changes_dispatch_and_projection() -> None:
    command = get_command(stove0_app)
    # Exercise framework dispatch without running the application's callback.
    command.callback = None
    relaxed = copy.deepcopy(command)
    relaxed.invoke_without_command = True
    with command.make_context("stove0", ["--json"]) as context:
        with pytest.raises(UsageError, match="Missing command"):
            command.invoke(context)
    with relaxed.make_context("stove0", ["--json"]) as context:
        assert relaxed.invoke(context) is None
    assert contract._click_command(command, name="stove0")["subcommand_required"] is True
    assert contract._click_command(relaxed, name="stove0")["subcommand_required"] is False


def test_typer_extra_arguments_change_acceptance_and_projection(tmp_path: Path) -> None:
    command = get_command(stove0_app).commands["recipe"].commands["validate"]
    relaxed = copy.deepcopy(command)
    # Context settings override command defaults during actual parsing.
    relaxed.context_settings["allow_extra_args"] = True
    path = tmp_path / "recipe.json"
    path.write_text("{}", encoding="utf-8")
    argv = [str(path), "extra"]
    with pytest.raises(UsageError, match="unexpected extra argument"):
        command.make_context(command.name, argv.copy())
    with relaxed.make_context(relaxed.name, argv.copy()) as context:
        assert context.args == ["extra"]
    assert contract._click_command(command, name=command.name)["allow_extra_args"] is False
    assert contract._click_command(relaxed, name=relaxed.name)["allow_extra_args"] is True


def test_typer_unknown_options_change_acceptance_and_projection() -> None:
    command = get_command(stove0_app).commands["preview"]
    relaxed = copy.deepcopy(command)
    relaxed.ignore_unknown_options = True
    argv = ["recipe", "--unknown"]
    with pytest.raises(UsageError, match="No such option"):
        command.make_context(command.name, argv.copy())
    with relaxed.make_context(relaxed.name, argv.copy()) as context:
        assert context.params["inputs"] == ("--unknown",)
    assert contract._click_command(command, name=command.name) != contract._click_command(
        relaxed, name=relaxed.name
    )


def test_typer_interspersed_options_change_acceptance_and_projection(tmp_path: Path) -> None:
    command = get_command(gogurt_app).commands["run"]
    restricted = copy.deepcopy(command)
    restricted.allow_interspersed_args = False
    argv = [str(tmp_path), "--dry-run"]
    with command.make_context(command.name, argv.copy()) as context:
        assert context.params["dry_run"] is True
    with pytest.raises(UsageError, match="unexpected extra argument"):
        restricted.make_context(restricted.name, argv.copy())
    assert contract._click_command(command, name=command.name) != contract._click_command(
        restricted, name=restricted.name
    )


@pytest.mark.parametrize(
    "attribute", ["exists", "file_okay", "dir_okay", "readable", "writable", "allow_dash"]
)
def test_stove0_path_checks_change_conversion_and_projection(
    attribute: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    command = get_command(stove0_app).commands["recipe"].commands["validate"]
    parameter = next(item for item in command.params if item.name == "path")
    changed = copy.deepcopy(parameter)
    setattr(changed.type, attribute, not getattr(parameter.type, attribute))
    file = tmp_path / "recipe.json"
    file.write_text("{}", encoding="utf-8")
    value = str(file)
    if attribute == "exists":
        value = str(tmp_path / "missing.json")
    elif attribute == "dir_okay":
        value = str(tmp_path)
    elif attribute == "allow_dash":
        monkeypatch.chdir(tmp_path)
        value = "-"
    elif attribute in {"readable", "writable"}:
        # Model an access denial consistently even when tests run as root.
        access = os.access
        denied_mode = os.R_OK if attribute == "readable" else os.W_OK
        monkeypatch.setattr(
            os, "access", lambda path, mode: False if mode == denied_mode else access(path, mode)
        )
    accepted, rejected = (
        (parameter, changed) if attribute in {"file_okay", "writable"} else (changed, parameter)
    )
    assert accepted.type.convert(value, accepted, None) == value
    with pytest.raises(BadParameter):
        rejected.type.convert(value, rejected, None)
    assert contract._click_parameter(parameter) != contract._click_parameter(changed)


def test_stove0_path_resolution_changes_conversion_and_projection(tmp_path: Path) -> None:
    command = get_command(stove0_app).commands["recipe"].commands["validate"]
    parameter = next(item for item in command.params if item.name == "path")
    resolved = copy.deepcopy(parameter)
    resolved.type.resolve_path = True
    (tmp_path / "child").mkdir()
    file = tmp_path / "recipe.json"
    file.write_text("{}", encoding="utf-8")
    value = str(tmp_path / "child" / ".." / file.name)
    assert parameter.type.convert(value, parameter, None) == value
    assert resolved.type.convert(value, resolved, None) == str(file)
    assert contract._click_parameter(parameter) != contract._click_parameter(resolved)


@pytest.mark.parametrize("attribute", ["min_open", "max_open", "clamp"])
def test_stove0_range_rules_change_conversion_and_projection(attribute: str) -> None:
    command = get_command(stove0_app).commands["work"].commands["list"]
    parameter = next(item for item in command.params if item.name == "page_size")
    changed = copy.deepcopy(parameter)
    setattr(changed.type, attribute, True)
    if attribute == "clamp":
        value = str(parameter.type.min - 1)
        with pytest.raises(BadParameter):
            parameter.type.convert(value, parameter, None)
        assert changed.type.convert(value, changed, None) == parameter.type.min
    else:
        endpoint = parameter.type.min if attribute == "min_open" else parameter.type.max
        assert parameter.type.convert(str(endpoint), parameter, None) == endpoint
        with pytest.raises(BadParameter):
            changed.type.convert(str(endpoint), changed, None)
    assert contract._click_parameter(parameter) != contract._click_parameter(changed)

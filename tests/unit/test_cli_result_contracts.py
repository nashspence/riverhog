from __future__ import annotations

import copy
import importlib
import importlib.metadata
import importlib.util
import json
import sys
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from http_api_contracts import ErrorOut
from jsonschema import Draft202012Validator
from typer.testing import CliRunner

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_SCRIPT = REPO_ROOT / "scripts/contract_freeze.py"


def _contract_module() -> ModuleType:
    if str(CONTRACT_SCRIPT.parent) not in sys.path:
        sys.path.insert(0, str(CONTRACT_SCRIPT.parent))
    spec = importlib.util.spec_from_file_location("riverhog_cli_result_contracts", CONTRACT_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def cli_surfaces() -> Mapping[str, Mapping[str, object]]:
    return _contract_module()._cli_surfaces()


@pytest.mark.parametrize("stale", ["command", "option", "operation", "query", "query-shape"])
def test_cli_occurrence_authorities_must_resolve_to_discovered_inputs(
    cli_surfaces: Mapping[str, Mapping[str, object]],
    monkeypatch,
    stale: str,
) -> None:
    import a_riverhog_cli.main

    module = _contract_module()
    bindings = copy.deepcopy(a_riverhog_cli.main._CLI_OCCURRENCE_AUTHORITIES)
    if stale == "command":
        bindings["collection missing"] = bindings.pop("collection list")
    elif stale == "option":
        bindings["collection list"]["missing"] = bindings["collection list"].pop("tag")
    elif stale == "operation":
        bindings["collection list"]["tag"]["operation_id"] = "get_collection"
    elif stale == "query":
        bindings["collection list"]["tag"]["parameter"] = "missing"
    monkeypatch.setattr(a_riverhog_cli.main, "_CLI_OCCURRENCE_AUTHORITIES", bindings)
    openapi = module._openapi_surfaces()
    if stale == "query-shape":
        schema = openapi["riverhog"]["components"]["schemas"]["SearchCollectionsRequest"]
        schema["properties"]["tags"] = {
            "type": "object",
            "properties": {"tags": schema["properties"]["tags"]},
        }
    with pytest.raises(module.ContractFreezeError, match="CLI occurrence"):
        module._apply_cli_occurrence_authorities(
            "a-riverhog-cli",
            copy.deepcopy(cli_surfaces["a-riverhog-cli"]),
            operations=module.operation_qualification.operation_matrix(),
            openapi=openapi,
        )


def _nodes(
    node: Mapping[str, object], path: tuple[str, ...] = ()
) -> Iterator[tuple[tuple[str, ...], Mapping[str, object]]]:
    yield path, node
    for name, child in sorted(node.get("commands", {}).items()):
        assert isinstance(child, Mapping)
        yield from _nodes(child, (*path, str(name)))


def _result(
    surfaces: Mapping[str, Mapping[str, object]], authority: str, path: Sequence[str]
) -> Mapping[str, object]:
    node = surfaces[authority]
    for name in path:
        child = node["commands"]
        assert isinstance(child, Mapping)
        node = child[name]
        assert isinstance(node, Mapping)
    result = node["result_contract"]
    assert isinstance(result, Mapping)
    return result


def _outcome(contract: Mapping[str, object], kind: str, identity: str) -> Mapping[str, object]:
    outcomes = contract[kind]
    assert isinstance(outcomes, list)
    matches = [item for item in outcomes if isinstance(item, Mapping) and item["id"] == identity]
    assert len(matches) == 1
    return matches[0]


def test_every_released_cli_leaf_has_one_implementation_owned_result_contract(
    cli_surfaces: Mapping[str, Mapping[str, object]],
) -> None:
    executable: list[Mapping[str, object]] = []
    groups_with_contracts: set[tuple[str, tuple[str, ...]]] = set()
    for authority, root in cli_surfaces.items():
        assert root["name"] == authority
        for path, node in _nodes(root):
            children = node.get("commands", {})
            result = node.get("result_contract")
            if not children:
                assert isinstance(result, Mapping), (authority, path)
            if result is not None:
                assert isinstance(result, Mapping)
                executable.append(result)
                if children:
                    groups_with_contracts.add((authority, path))
                success = result["success"]
                assert isinstance(success, list) and success
                assert all(
                    isinstance(outcome, Mapping) and outcome["exit_status"] == 0
                    for outcome in success
                )
                assert _outcome(result, "failures", "usage")["exit_status"] == 2
                assert result["structured_output"] in {
                    "always-json",
                    "mode-specific",
                    "none",
                    "optional-json",
                }

    assert len(executable) == 153
    assert len({str(item["identity"]) for item in executable}) == len(executable)
    assert groups_with_contracts == {
        ("a-riverhog-event-relay", ()),
        ("riverhog-api", ()),
        ("a-riverhog-ftp-spool", ()),
    }
    mango_commands = cli_surfaces["a-riverhog-event-relay"]["commands"]
    assert isinstance(mango_commands, Mapping)
    assert set(mango_commands) == {"state"}
    mango_state = mango_commands["state"]
    assert isinstance(mango_state, Mapping)
    assert set(mango_state["commands"]) == {
        "status",
        "upgrade",
        "verify",
    }


def test_cli_result_contracts_preserve_nonzero_terminal_semantics(
    cli_surfaces: Mapping[str, Mapping[str, object]],
) -> None:
    upload = _result(cli_surfaces, "a-riverhog-cli", ("collection", "upload", "start"))
    assert _outcome(upload, "failures", "custody-timeout")["exit_status"] == 124
    action = _result(cli_surfaces, "gogurt", ("run",))
    assert _outcome(action, "failures", "action-exit")["exit_status"] == {
        "kind": "delegated",
        "minimum": 1,
        "maximum": 255,
    }


def test_cli_result_declaration_drift_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    contract = _contract_module()
    planning = importlib.import_module("review0_planner.conformance")
    declaration = dict(planning._CLI_RESULT_CONTRACT)
    declaration["command_overrides"] = {"missing-command": {}}
    monkeypatch.setattr(planning, "_CLI_RESULT_CONTRACT", declaration)
    root = contract._argparse_command(planning._parser())

    with pytest.raises(contract.ContractFreezeError, match="unknown commands"):
        contract._apply_cli_result_contract(
            "review0-planner",
            root,
            operations=contract.operation_qualification.operation_matrix(),
            openapi=contract._openapi_surfaces(),
        )


def test_machine_report_success_matches_its_discovered_contract(
    cli_surfaces: Mapping[str, Mapping[str, object]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    planning = importlib.import_module("review0_planner.conformance")
    status = planning.main([])
    captured = capsys.readouterr()
    outcome = _outcome(
        _result(cli_surfaces, "review0-planner", ()),
        "success",
        "reported",
    )
    assert status == outcome["exit_status"] == 0
    assert captured.err == ""
    authority = outcome["stdout"]["json"]
    assert isinstance(authority, Mapping)
    assert json.loads(captured.out)["format"] == authority["identity"]


def test_argparse_usage_failure_matches_its_discovered_contract(
    cli_surfaces: Mapping[str, Mapping[str, object]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    materialize = importlib.import_module("a_riverhog_filesystem_store.materialize_cli")
    with pytest.raises(SystemExit) as raised:
        materialize.main([])
    captured = capsys.readouterr()
    outcome = _outcome(
        _result(cli_surfaces, "a-riverhog-filesystem-store-materialize", ()),
        "failures",
        "usage",
    )
    assert raised.value.code == outcome["exit_status"] == 2
    assert captured.out == ""
    assert captured.err


def test_a_riverhog_cli_json_failure_matches_its_discovered_contract(
    cli_surfaces: Mapping[str, Mapping[str, object]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    a_riverhog_cli = importlib.import_module("a_riverhog_cli.main")

    def fail() -> None:
        raise FileNotFoundError("missing fixture")

    monkeypatch.setattr(a_riverhog_cli, "app", fail)
    monkeypatch.setattr(sys, "argv", ["a-riverhog-cli", "local", "show", "1", "--json"])
    status = a_riverhog_cli.main()
    captured = capsys.readouterr()
    outcome = _outcome(
        _result(cli_surfaces, "a-riverhog-cli", ("local", "show")),
        "failures",
        "operational",
    )
    assert status == outcome["exit_status"] == 1
    assert captured.err == ""
    authority = outcome["stdout"]["json"]
    assert isinstance(authority, Mapping)
    assert authority["identity"] == "http-api-contracts.ErrorOut"
    assert ErrorOut.model_validate_json(captured.out).error.code == "error"


def test_gogurt_json_failure_matches_its_discovered_contract(
    cli_surfaces: Mapping[str, Mapping[str, object]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    gogurt = importlib.import_module("gogurt.cli")

    def fail() -> None:
        raise FileNotFoundError("missing fixture")

    monkeypatch.setattr(gogurt, "app", fail)
    monkeypatch.setattr(sys, "argv", ["gogurt", "list", "--json"])
    with pytest.raises(SystemExit) as raised:
        gogurt.main()
    captured = capsys.readouterr()
    outcome = _outcome(_result(cli_surfaces, "gogurt", ("list",)), "failures", "operational")
    assert raised.value.code == outcome["exit_status"] == 1
    assert captured.err == ""
    authority = outcome["stdout"]["json"]
    assert isinstance(authority, Mapping)
    assert authority["identity"] == "gogurt-cli-error/v1"
    assert json.loads(captured.out) == {
        "error": {"code": "config_error", "message": "missing fixture"}
    }


def test_every_json_result_resolves_to_an_exact_authority_and_selector(
    cli_surfaces: Mapping[str, Mapping[str, object]],
) -> None:
    allowed = {
        "cli-local-exact-json",
        "cli-local-json-schema",
        "cli-local-json-sequence",
        "document-authority",
        "http-operation-response",
        "openapi-schema",
        "python-model",
        "schema-authority",
    }
    resolved = 0
    for authority, root in cli_surfaces.items():
        for path, node in _nodes(root):
            result = node.get("result_contract")
            if not isinstance(result, Mapping):
                continue
            for outcome_kind in ("success", "failures"):
                outcomes = result[outcome_kind]
                assert isinstance(outcomes, list)
                for outcome in outcomes:
                    assert isinstance(outcome, Mapping)
                    selector = outcome["selected_by"]
                    assert isinstance(selector, Mapping) and selector.get("kind")
                    for channel in ("stdout", "stderr"):
                        semantics = outcome[channel]
                        assert isinstance(semantics, Mapping)
                        json_authority = semantics.get("json")
                        if json_authority is None:
                            continue
                        if isinstance(json_authority, str) and (
                            json_authority == "empty"
                            or json_authority.startswith("noncontractual-")
                        ):
                            continue
                        assert isinstance(json_authority, Mapping), (authority, path, outcome)
                        assert json_authority["kind"] in allowed
                        assert "$command-json-output" not in json.dumps(json_authority)
                        if json_authority["kind"] in {"cli-local-json-schema", "python-model"}:
                            Draft202012Validator.check_schema(json_authority["schema"])
                        resolved += 1
    assert resolved > 100


def test_unresolved_json_format_and_unknown_runtime_selector_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _contract_module()
    planning = importlib.import_module("review0_planner.conformance")
    original = planning._CLI_RESULT_CONTRACT

    unresolved = json.loads(json.dumps(original))
    unresolved["profiles"]["machine-report"]["success"][0]["stdout"]["json"] = "unowned-format/v1"
    monkeypatch.setattr(planning, "_CLI_RESULT_CONTRACT", unresolved)
    with pytest.raises(contract.ContractFreezeError, match="does not resolve"):
        contract._apply_cli_result_contract(
            "review0-planner",
            contract._argparse_command(planning._parser()),
            operations=contract.operation_qualification.operation_matrix(),
            openapi=contract._openapi_surfaces(),
        )

    unknown_selector = json.loads(json.dumps(original))
    unknown_selector["outcome_selectors"]["reported"] = {"kind": "misspelled-completion"}
    monkeypatch.setattr(planning, "_CLI_RESULT_CONTRACT", unknown_selector)
    with pytest.raises(contract.ContractFreezeError, match="selector kind is unknown"):
        contract._apply_cli_result_contract(
            "review0-planner",
            contract._argparse_command(planning._parser()),
            operations=contract.operation_qualification.operation_matrix(),
            openapi=contract._openapi_surfaces(),
        )


def test_framework_terminating_controls_are_discovered_without_completion_side_effects(
    cli_surfaces: Mapping[str, Mapping[str, object]],
) -> None:
    control_ids: set[str] = set()
    version_distributions: set[str] = set()
    for root in cli_surfaces.values():
        for _path, node in _nodes(root):
            controls = node["terminating_controls"]
            assert isinstance(controls, list)
            for control in controls:
                assert isinstance(control, Mapping)
                identity = str(control["id"])
                control_ids.add(identity)
                assert control["exit_status"] in {0, 2}
                trigger = control["trigger"]
                assert isinstance(trigger, Mapping) and trigger.get("kind")
                if identity == "version":
                    stdout = control["stdout"]
                    assert isinstance(stdout, Mapping)
                    assert stdout["kind"] == "installed-coordinated-release-version"
                    assert stdout["serialization"] == "noncontractual"
                    version_distributions.add(str(stdout["distribution"]))
                assert "completion" not in json.dumps(control).casefold()
        root_parameters = root["parameters"]
        assert isinstance(root_parameters, list)
        assert all(
            "completion" not in json.dumps(parameter).casefold() for parameter in root_parameters
        )
    assert {"help", "implicit-help", "version"} <= control_ids
    assert {
        "gogurt",
        "a-riverhog-event-relay",
        "a-riverhog-cli",
        "a-stove0-cli",
    } <= version_distributions


@pytest.mark.parametrize(
    ("module_name", "application_name", "distribution"),
    [
        ("gogurt.cli", "app", "gogurt"),
        ("a_riverhog_cli.main", "app", "a-riverhog-cli"),
        ("a_stove0_cli.main", "app", "a-stove0-cli"),
    ],
)
def test_typer_version_control_reports_the_installed_distribution_version(
    module_name: str,
    application_name: str,
    distribution: str,
) -> None:
    module = importlib.import_module(module_name)
    result = CliRunner().invoke(getattr(module, application_name), ["--version"])
    assert result.exit_code == 0
    assert importlib.metadata.version(distribution) in result.stdout.split()


def test_argparse_version_control_reports_the_installed_distribution_version(
    capsys: pytest.CaptureFixture[str],
) -> None:
    a_riverhog_event_relay = importlib.import_module("a_riverhog_event_relay.cli")
    with pytest.raises(SystemExit) as raised:
        a_riverhog_event_relay.parser().parse_args(["--version"])
    captured = capsys.readouterr()
    assert raised.value.code == 0
    assert importlib.metadata.version("a-riverhog-event-relay") in captured.out.split()
    assert captured.err == ""


def test_http_backed_and_local_outputs_keep_their_own_authorities(
    cli_surfaces: Mapping[str, Mapping[str, object]],
) -> None:
    remote = _outcome(
        _result(cli_surfaces, "a-riverhog-cli", ("collection", "show")),
        "success",
        "completed",
    )["stdout"]["json"]
    assert isinstance(remote, Mapping)
    assert remote["kind"] == "http-operation-response"
    assert remote["application"] == "riverhog"
    assert remote["operation_id"] == "get_collection"

    local = _outcome(
        _result(cli_surfaces, "a-riverhog-cli", ("local", "list")),
        "success",
        "completed",
    )["stdout"]["json"]
    assert isinstance(local, Mapping)
    assert local["kind"] == "cli-local-json-schema"
    assert local["identity"] == "a-riverhog-cli-local-collection-list/v1"

    retire = _result(cli_surfaces, "a-riverhog-cli", ("archive", "retire"))
    planned = _outcome(retire, "success", "planned")
    executed = _outcome(retire, "success", "executed")
    assert planned["selected_by"] == {
        "kind": "option-equals",
        "parameter": "dry_run",
        "value": True,
    }
    assert planned["stdout"]["json"]["operation_id"] == "plan_archive_copy_retirement"
    assert executed["stdout"]["json"]["operation_id"] == "retire_archive_copy"


def test_stove0_human_and_json_success_and_failure_match_the_profile(
    cli_surfaces: Mapping[str, Mapping[str, object]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stove0 = importlib.import_module("a_stove0_cli.main")

    class HealthyClient:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def health_live(self) -> dict[str, str]:
            return {"service": "stove0", "status": "ok"}

    monkeypatch.setattr(stove0, "Stove0ApiClient", HealthyClient)
    runner = CliRunner()
    human = runner.invoke(stove0.app, ["health"])
    machine = runner.invoke(stove0.app, ["--json", "health"])
    contract = _result(cli_surfaces, "stove0", ("health",))
    success = _outcome(contract, "success", "completed")
    assert human.exit_code == machine.exit_code == success["exit_status"] == 0
    assert human.stdout.strip()
    assert json.loads(machine.stdout) == {"service": "stove0", "status": "ok"}
    assert human.stderr == machine.stderr == ""

    def fail(_state: object, _operation: object, **_kwargs: object) -> None:
        stove0._fail("service unavailable")

    monkeypatch.setattr(stove0, "_call", fail)
    failed = runner.invoke(stove0.app, ["--json", "health"])
    failure = _outcome(contract, "failures", "operational")
    assert failed.exit_code == failure["exit_status"] == 1
    assert failed.stdout == ""
    assert failed.stderr == "service unavailable\n"

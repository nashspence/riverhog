from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest
from jsonschema import Draft202012Validator
from riverhog_canonical_json import canonical_json_bytes
from typer._click.core import Context
from typer._click.exceptions import UsageError
from typer.core import TyperArgument, TyperCommand, TyperOption

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/contract_freeze.py"
_CHECKED_PROJECTION: dict[str, Any] | None = None
_CHECKED_TRACE: dict[str, Any] | None = None


def load_script() -> ModuleType:
    if str(SCRIPT.parent) not in sys.path:
        sys.path.insert(0, str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location("riverhog_extent_freeze", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module", autouse=True)
def _bind_checked_contract(checked_contract_closure: dict[str, Any]) -> None:
    global _CHECKED_PROJECTION, _CHECKED_TRACE
    _CHECKED_PROJECTION = cast(dict[str, Any], checked_contract_closure["projection"])
    _CHECKED_TRACE = cast(dict[str, Any], checked_contract_closure["trace"])


def _checked_projection() -> dict[str, Any]:
    assert _CHECKED_PROJECTION is not None
    return _CHECKED_PROJECTION


def _checked_trace() -> dict[str, Any]:
    assert _CHECKED_TRACE is not None
    return _CHECKED_TRACE


def _resolve_pointer(document: object, pointer: str) -> object:
    current = document
    assert pointer.startswith("/")
    for encoded in pointer[1:].split("/"):
        part = encoded.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            current = current[int(part)]
        else:
            assert isinstance(current, dict)
            current = current[part]
    return current


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def test_extent_projection_is_exhaustive_source_linked_and_self_identifying() -> None:
    projection = _checked_projection()
    extents = projection["external_contract"]["extents"]
    decisions: list[dict[str, Any]] = extents["decisions"]
    content = {key: value for key, value in extents.items() if key != "sha256"}

    assert extents["sha256"] == _canonical_sha256(content)
    assert extents["coverage"]["classified"] == len(decisions)
    assert extents["coverage"]["discovered"] == len(decisions)
    assert {extents["coverage"][key] for key in ("missing", "duplicate", "stale", "undecided")} == {
        0
    }
    assert len({decision["id"] for decision in decisions}) == len(decisions)
    assert set(extents["rules"]) == {
        "bounded-segment/v1",
        "configuration-composition/v1",
        "configured-capacity/v1",
        "extension-contract/v1",
        "no-semantic-maximum/v1",
        "route-progression/v1",
        "schema-bound/v1",
    }
    assert {decision["policy"] for decision in decisions} == {
        "contract_max",
        "extension_owned",
        "fixed",
        "operational_policy",
        "segmented_no_total_max",
    }

    for decision in decisions:
        assert decision["rule"] in extents["rules"]
        assert decision["reason"]
        source = _resolve_pointer(projection, decision["source_pointer"])
        assert isinstance(source, dict)
        constraint_pointer = decision.get("source_constraint", {}).get("pointer")
        if constraint_pointer is not None:
            assert source["occurrences_authority"] == constraint_pointer
            source = _resolve_pointer(projection, constraint_pointer)
            assert isinstance(source, dict)
        policy = decision["policy"]
        if policy in {"fixed", "contract_max"}:
            source_field = decision.get("source_constraint", {}).get("field")
            if decision["unit"] == "values-per-occurrence":
                # Raw parser fields are not themselves the consumed-value count.
                # The parser probes below check that interpretation independently.
                assert source_field in source
            elif source_field == "type.maximum":
                assert source["type"]["maximum"] == decision["maximum"]
            elif decision["dimension"] == "cardinality":
                keyword = "maxProperties" if decision["unit"] == "entries" else "maxItems"
                assert source[keyword] == decision["maximum"]
            elif decision["dimension"] == "encoded-size":
                assert source["x-riverhog-encoded-bytes-max"] == decision["maximum"]
            elif decision["dimension"] == "length":
                if "source_constraint" in decision:
                    assert source["pattern"] == decision["source_constraint"]["pattern"]
                else:
                    assert source["maxLength"] == decision["maximum"]
            else:
                assert source["maximum"] == decision["maximum"]
        elif policy == "segmented_no_total_max" and "maximum" in decision:
            keyword = "maxProperties" if decision["unit"] == "entries" else "maxItems"
            assert source[keyword] == decision["maximum"]
        elif decision["dimension"] == "cardinality":
            if decision["unit"] == "items" and source.get("type") == "array":
                assert "maxItems" not in source
            elif decision["unit"] == "entries" and source.get("type") == "object":
                assert "maxProperties" not in source
        assert policy != "segmented_no_total_max" or "progression" in decision


def test_operation_parameter_extents_are_covered_from_the_openapi_authority() -> None:
    projection = _checked_projection()
    decisions = {
        decision["id"]: decision
        for decision in projection["external_contract"]["extents"]["decisions"]
    }

    page_size = decisions[
        "http:riverhog:operation:list_collections:parameter:query:page_size:/:value"
    ]
    append_bytes = decisions[
        "http:riverhog:operation:append_collection_upload_session_provenance_journal:"
        "parameter:header:Content-Length:/:value"
    ]
    assert (page_size["policy"], page_size["maximum"]) == ("contract_max", 100)
    assert (append_bytes["policy"], append_bytes["maximum"]) == (
        "contract_max",
        1024 * 1024,
    )
    cli_page_size = decisions[
        "cli:a-riverhog-cli:a-riverhog-cli:collection:list:parameter:page_size:value"
    ]
    assert (cli_page_size["policy"], cli_page_size["maximum"]) == (
        page_size["policy"],
        page_size["maximum"],
    )


def test_collection_list_separates_logical_total_page_carrier_and_selector_batch() -> None:
    projection = _checked_projection()
    decisions = {
        decision["id"]: decision
        for decision in projection["external_contract"]["extents"]["decisions"]
    }
    total = decisions["http:riverhog:operation:list_collections:logical-result"]
    page = decisions[
        "http:riverhog:components:/schemas/ListCollectionsResponse/properties/collections:cardinality"
    ]
    tags = decisions[
        "http:riverhog:components:/schemas/ListCollectionsResponse/properties/tags:cardinality"
    ]
    cli_tags = decisions[
        "cli:a-riverhog-cli:a-riverhog-cli:collection:list:parameter:tag:occurrences"
    ]
    assert total["policy"] == page["policy"] == "segmented_no_total_max"
    assert "maximum" not in total and "maximum" not in page
    assert total["progression"] == page["progression"]
    assert page["progression"]["maximum_page_size"] == 100
    assert page["progression"]["response_items_field"] == "collections"
    assert tags["policy"] == cli_tags["policy"] == "contract_max"
    assert tags["maximum"] == cli_tags["maximum"] == 100
    schema = _resolve_pointer(projection, cli_tags["source_constraint"]["pointer"])
    assert isinstance(schema, dict)
    assert schema["maxItems"] == cli_tags["maximum"]


@pytest.mark.parametrize("failure", ["ambiguous", "stale", "conflicting", "response", "media"])
def test_route_page_binding_rejects_ambiguous_stale_or_conflicting_authority(failure: str) -> None:
    module = load_script().extent_contract
    openapi = copy.deepcopy(_checked_projection()["external_contract"]["http_openapi"]["riverhog"])
    operation = openapi["paths"]["/v1/collections:search"]["post"]
    read = operation["x-riverhog-read-collection"]
    if failure == "ambiguous":
        del read["response_items_field"]
    elif failure == "stale":
        read["response_items_field"] = "missing"
    elif failure == "response":
        operation["responses"]["200"]["content"]["application/json"]["schema"] = {"type": "string"}
    elif failure == "media":
        operation["responses"]["200"]["content"]["application/fixture+json"] = {
            "schema": {"type": "string"},
        }
    else:
        other = copy.deepcopy(operation)
        other["x-riverhog-read-collection"]["maximum_page_size"] = 50
        openapi["paths"]["/fixture-conflict"] = {"get": other}
    with pytest.raises(module.ExtentContractError, match="route page"):
        module._direct_response_array_policies(openapi)


def test_cli_occurrence_bound_tracks_its_source_and_rejects_missing_authority() -> None:
    module = load_script().extent_contract
    external = copy.deepcopy(_checked_projection()["external_contract"])
    parameter = next(
        parameter
        for parameter in external["cli"]["a-riverhog-cli"]["commands"]["collection"]["commands"][
            "list"
        ]["parameters"]
        if parameter["name"] == "tag"
    )
    schema = _resolve_pointer({"external_contract": external}, parameter["occurrences_authority"])
    assert isinstance(schema, dict)
    schema["maxItems"] = 7
    decision = next(
        item
        for item in module._cli_decisions(external)
        if item["id"]
        == "cli:a-riverhog-cli:a-riverhog-cli:collection:list:parameter:tag:occurrences"
    )
    assert decision["maximum"] == 7
    del schema["maxItems"]
    with pytest.raises(module.ExtentContractError, match="has no bound"):
        module._cli_decisions(external)
    parameter["occurrences_authority"] += "/missing"
    with pytest.raises(module.ExtentContractError, match="does not resolve"):
        module._cli_decisions(external)


@pytest.mark.parametrize(
    "framework,options,positional,minimum,maximum,valid,expected,invalid,repeatable",
    [
        ("typer", {"is_flag": True}, False, 0, 0, ["--value"], True, ["--value", "x"], False),
        ("typer", {}, False, 1, 1, ["--value", "a"], "a", ["--value"], False),
        (
            "typer",
            {"nargs": 2},
            False,
            2,
            2,
            ["--value", "a", "b"],
            ("a", "b"),
            ["--value", "a"],
            False,
        ),
        (
            "typer",
            {"nargs": -1, "required": True},
            True,
            1,
            None,
            ["a", "b"],
            ("a", "b"),
            [],
            False,
        ),
        ("typer", {"nargs": -1}, True, 0, None, [], (), None, False),
        (
            "typer",
            {"multiple": True},
            False,
            1,
            1,
            ["--value", "a", "--value", "b"],
            ("a", "b"),
            ["--value"],
            True,
        ),
        ("typer", {"count": True}, False, 0, 0, ["--value", "--value"], 2, ["--value", "x"], True),
        (
            "argparse",
            {"action": "store_true"},
            False,
            0,
            0,
            ["--value"],
            True,
            ["--value", "x"],
            False,
        ),
        ("argparse", {}, False, 1, 1, ["--value", "a"], "a", ["--value"], False),
        ("argparse", {"nargs": "?"}, True, 0, 1, [], None, ["a", "b"], False),
        ("argparse", {"nargs": "?"}, True, 0, 1, ["a"], "a", ["a", "b"], False),
        ("argparse", {"nargs": "+"}, True, 1, None, ["a", "b"], ["a", "b"], [], False),
        ("argparse", {"nargs": "*"}, True, 0, None, [], [], None, False),
        (
            "argparse",
            {"action": "append"},
            False,
            1,
            1,
            ["--value", "a", "--value", "b"],
            ["a", "b"],
            ["--value"],
            True,
        ),
    ],
)
def test_cli_extents_agree_with_actual_parser_consumption(
    framework: str,
    options: dict[str, Any],
    positional: bool,
    minimum: int,
    maximum: int | None,
    valid: list[str],
    expected: object,
    invalid: list[str] | None,
    repeatable: bool,
) -> None:
    module = load_script()
    if framework == "typer":
        parameter = (TyperArgument if positional else TyperOption)(
            param_decls=["value" if positional else "--value"], **options
        )
        command = TyperCommand("probe", params=[parameter], add_help_option=False)
        projected = module._click_parameter(parameter)

        def parse(argv: list[str]) -> object:
            context = Context(command)
            command.parse_args(context, argv.copy())
            return context.params["value"]

        rejected = UsageError
    else:
        parser = argparse.ArgumentParser(add_help=False)
        action = parser.add_argument("value" if positional else "--value", **options)
        projected = module._argparse_action(action)

        def parse(argv: list[str]) -> object:
            return parser.parse_args(argv).value

        rejected = SystemExit
    decisions = module.extent_contract._cli_decisions(
        {"cli": {"probe": {"parameters": [projected]}}}
    )
    arity = next(item for item in decisions if item["unit"] == "values-per-occurrence")
    assert (arity["minimum"], arity["maximum"]) == (minimum, maximum)
    assert parse(valid) == expected
    if invalid is not None:
        with pytest.raises(rejected):
            parse(invalid)
    if maximum is None:
        assert arity["policy"] == "operational_policy"
        assert len(cast(Any, parse(["a"] * 50))) == 50
    occurrences = [item for item in decisions if item["unit"] == "occurrences"]
    assert bool(occurrences) == repeatable
    if repeatable:
        assert occurrences[0]["maximum"] is None
        args = ["--value"] if options.get("count") else ["--value", "a"]
        result = parse(args * 50)
        assert (result if options.get("count") else len(cast(Any, result))) == 50


def test_every_discovered_cli_parameter_has_one_arity_decision() -> None:
    projection = _checked_projection()
    decisions = projection["external_contract"]["extents"]["decisions"]
    arities = [item for item in decisions if item["unit"] == "values-per-occurrence"]
    parameters: set[str] = set()

    def visit(command: dict[str, Any], pointer: str) -> None:
        parameters.update(f"{pointer}/parameters/{i}" for i in range(len(command["parameters"])))
        for name, child in command.get("commands", {}).items():
            visit(child, f"{pointer}/commands/{name}")

    for name, command in projection["external_contract"]["cli"].items():
        visit(command, f"/external_contract/cli/{name}")
    assert len(arities) == len(parameters)
    assert {item["source_pointer"] for item in arities} == parameters


@pytest.mark.parametrize("change", [{"kind": "CustomAction"}, {"nargs": "..."}])
def test_cli_extent_discovery_rejects_uninterpreted_parser_forms(change: dict[str, object]) -> None:
    module = load_script()
    parameter = module._argparse_action(argparse.ArgumentParser().add_argument("value"))
    parameter.update(change)
    with pytest.raises(
        module.extent_contract.ExtentContractError, match="unsupported CLI parameter"
    ):
        module.extent_contract._cli_decisions({"cli": {"probe": {"parameters": [parameter]}}})


def test_schema_bounds_accept_the_boundary_and_reject_the_next_value() -> None:
    projection = _checked_projection()
    decisions = projection["external_contract"]["extents"]["decisions"]

    exercised = 0
    for decision in decisions:
        if decision["policy"] not in {"fixed", "contract_max"}:
            continue
        _resolve_pointer(projection, decision["source_pointer"])
        source_field = decision.get("source_constraint", {}).get("field")
        if decision["unit"] == "values-per-occurrence" or source_field == "type.maximum":
            continue
        maximum = decision["maximum"]
        minimum = decision.get("minimum")
        dimension = decision["dimension"]
        if dimension == "cardinality":
            if decision["unit"] == "entries":
                validator = Draft202012Validator(
                    {
                        "type": "object",
                        "maxProperties": maximum,
                        **({"minProperties": minimum} if minimum is not None else {}),
                    }
                )
                exact = {str(index): None for index in range(maximum)}
                above = {str(index): None for index in range(maximum + 1)}
            else:
                validator = Draft202012Validator(
                    {
                        "type": "array",
                        "maxItems": maximum,
                        **({"minItems": minimum} if minimum is not None else {}),
                    }
                )
                exact = [None] * maximum
                above = [None] * (maximum + 1)
        elif dimension == "encoded-size":
            assert maximum > 0
            continue
        elif dimension == "length":
            source_pattern = decision.get("source_constraint", {}).get("pattern")
            if source_pattern is not None:
                exact_text = "0" * maximum
                assert re.fullmatch(source_pattern, exact_text)
                validator = Draft202012Validator({"type": "string", "pattern": source_pattern})
                exact = exact_text
                above = f"{exact_text}0"
            else:
                validator = Draft202012Validator(
                    {
                        "type": "string",
                        "maxLength": maximum,
                        **({"minLength": minimum} if minimum is not None else {}),
                    }
                )
                exact = "x" * maximum
                above = f"{exact}x"
        else:
            validator = Draft202012Validator(
                {
                    "type": "number",
                    "maximum": maximum,
                    **({"minimum": minimum} if minimum is not None else {}),
                }
            )
            exact = maximum
            above = maximum + 1
        assert validator.is_valid(exact), decision["id"]
        assert not validator.is_valid(above), decision["id"]
        exercised += 1

    assert exercised > 500


def test_every_open_schema_map_is_classified_and_hidden_maxima_are_forbidden() -> None:
    projection = _checked_projection()
    decisions = projection["external_contract"]["extents"]["decisions"]
    maps = [
        decision
        for decision in decisions
        if decision["dimension"] == "cardinality" and decision["unit"] == "entries"
    ]

    assert len(maps) > 100
    for decision in maps:
        if decision["policy"] == "operational_policy":
            assert decision["capacity_authority"] == {
                "owner": decision["owner"],
                "declared_maximum": None,
                "hidden_maximum": "forbidden",
            }


def test_bounded_carriers_do_not_become_domain_cardinality_maxima() -> None:
    projection = _checked_projection()
    decisions = {
        decision["id"]: decision
        for decision in projection["external_contract"]["extents"]["decisions"]
    }

    registration = decisions[
        "http:riverhog:components:/schemas/RegisterCollectionUploadSessionFilesRequest/"
        "properties/files:cardinality"
    ]
    assert registration["policy"] == "segmented_no_total_max"
    assert registration["reason"] == "bounded-upload-registration"
    archive_parts = decisions[
        "protocol:https://nashspence.github.io/riverhog/v1/schemas/"
        "collection-archive-volume-v1.schema.json:/$defs/pack/properties/parts:cardinality"
    ]
    assert archive_parts["policy"] == "segmented_no_total_max"
    assert archive_parts["maximum"] == 1024

    semantic_set_maxima = {
        decision["reason"]
        for decision in decisions.values()
        if decision["dimension"] == "cardinality" and decision["policy"] == "contract_max"
    }
    assert semantic_set_maxima == {
        "bounded-diagnostic-sample-with-explicit-overflow-markers",
        "bounded-deployment-admission-catalog",
        "bounded-exact-tag-selector-batch",
        "bounded-exact-classification-admission-predicate",
        "bounded-object-identity-assertion-envelope",
        "state-conditioned-empty-set",
        "wildcard-access-grant-is-exclusive",
        "optional-command-argument-arity",
    }


def test_generated_protocols_remain_owned_by_the_product_contract_packages() -> None:
    projection = _checked_projection()
    decisions = projection["external_contract"]["extents"]["decisions"]
    expected = {
        "generated:riverhog-storage-adapter": "riverhog-storage-adapter-protocol",
        "generated:stove0-observer": "stove0-observer-protocol",
        "generated:review0-sampler": "review0-sampler-protocol",
        "generated:stove0-target": "stove0-target-protocol",
    }

    for authority, owner in expected.items():
        selected = [
            decision for decision in decisions if f"protocol:{authority}:" in decision["id"]
        ]
        assert selected
        assert {decision["owner"] for decision in selected} == {owner}


def test_extent_relevant_deployment_configuration_is_source_linked() -> None:
    projection = _checked_projection()
    decisions = {
        decision["id"]: decision
        for decision in projection["external_contract"]["extents"]["decisions"]
    }

    cache_lease = decisions[
        "configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE:value"
    ]
    assert cache_lease["policy"] == "operational_policy"
    assert cache_lease["configuration"] == "RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"
    source = _resolve_pointer(projection, cache_lease["source_pointer"])
    assert "riverhog-server" in source["consumers"]
    trace = _checked_trace()
    trace_sources = {item["id"]: item for item in trace["sources"]}
    cache_lease_trace = trace_sources[
        "configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE"
    ]
    assert cache_lease_trace["bindings"]
    assert all((REPO_ROOT / binding["path"]).exists() for binding in cache_lease_trace["bindings"])

    configuration_decisions = [
        decision
        for decision in decisions.values()
        if decision["id"].startswith(("configuration-environment:", "configuration-pattern:"))
    ]
    assert configuration_decisions
    for decision in configuration_decisions:
        assert decision["policy"] == "operational_policy"
        assert decision["owner"]
        source = _resolve_pointer(projection, decision["source_pointer"])
        if decision["id"].startswith("configuration-environment:"):
            assert decision["configuration"] == source["name"]
            assert decision["owner"] == source["owner"]
            assert decision["consumers"] == source["consumers"]
            assert source["consumers"]
            binding = trace_sources[f"configuration-environment:{source['owner']}:{source['name']}"]
            assert binding["bindings"]
        else:
            assert decision["owner"] == source["owner"]
            assert decision["consumers"] == source["consumers"]
            assert decision["configuration"] in source["parameters"]["setting"]


def test_every_route_owned_collection_extent_is_projected_with_exact_progression() -> None:
    projection = _checked_projection()
    decisions = {
        decision["id"]: decision
        for decision in projection["external_contract"]["extents"]["decisions"]
    }

    observed: set[str] = set()
    for application, openapi in projection["external_contract"]["http_openapi"].items():
        for path_item in openapi["paths"].values():
            for operation in path_item.values():
                if not isinstance(operation, dict):
                    continue
                progression = operation.get("x-riverhog-read-collection")
                if progression is None:
                    continue
                identity = f"http:{application}:operation:{operation['operationId']}:logical-result"
                observed.add(identity)
                decision = decisions[identity]
                assert decision["policy"] == "segmented_no_total_max"
                assert decision["progression"] == progression

    assert observed
    assert observed <= set(decisions)


def test_trace_index_covers_every_extent_and_only_current_source_paths() -> None:
    projection = _checked_projection()
    trace = _checked_trace()
    decisions = projection["external_contract"]["extents"]["decisions"]
    links = trace["extent_sources"]

    boundary_payload = canonical_json_bytes(projection["boundaries"])
    assert trace["boundary_canonical_sha256"] == hashlib.sha256(boundary_payload).hexdigest()
    assert {link["id"] for link in links} == {decision["id"] for decision in decisions}
    assert len(links) == len(decisions)
    witnesses = {item["id"]: item for item in trace["segmented_extent_witnesses"]}
    segmented = [
        decision for decision in decisions if decision["policy"] == "segmented_no_total_max"
    ]
    linked_segmented = [link for link in links if "segmented_extent_witnesses" in link]
    assert {link["id"] for link in linked_segmented} == {decision["id"] for decision in segmented}
    assert all(link["segmented_extent_witnesses"] for link in linked_segmented)
    assert {
        witness_id for link in linked_segmented for witness_id in link["segmented_extent_witnesses"]
    } == set(witnesses)
    for witness in witnesses.values():
        assert set(witness["unestablished_claims"]) == {
            "bounded_step",
            "forward_progress",
            "multiple_segments",
            "no_silent_truncation",
            "restart",
        }
        assert witness["gates"]
        assert witness["test_node_ids"]
        if witness["test_scopes"]:
            assert [item["node_id"] for item in witness["test_scopes"]] == witness["test_node_ids"]
            assert all(item["scope"] for item in witness["test_scopes"])
        for node_id in witness["test_node_ids"]:
            assert (REPO_ROOT / node_id.split("::", 1)[0]).is_file()
    assert len({source["id"] for source in trace["sources"]}) == len(trace["sources"])
    for source in trace["sources"]:
        direct = source.get("source", {})
        if "path" in direct:
            assert (REPO_ROOT / direct["path"]).exists()
        for route in source.get("routes", []):
            route_source = route["source"]
            if "path" in route_source:
                assert (REPO_ROOT / route_source["path"]).exists()
        for fixture in source.get("fixtures", []):
            assert (REPO_ROOT / fixture["path"]).exists()
        for binding in source.get("bindings", []):
            assert (REPO_ROOT / binding["path"]).exists()


def test_semantic_protocol_and_state_authorities_do_not_freeze_source_layout() -> None:
    projection = _checked_projection()
    external = projection["external_contract"]

    assert all(
        authority.startswith(("generated:", "https://"))
        for authority in external["protocol_schemas"]
    )
    assert all(
        "structure" in owner
        and "fixtures" not in owner
        and "fixture_sha256s" not in owner
        and "module" not in owner["structure"]
        and "symbol" not in owner["structure"]
        for owner in external["durable_state"]["owners"]
    )


def test_extent_generation_is_deterministic_from_current_authorities() -> None:
    module = load_script()

    first = module.contract_projection()
    second = module.contract_projection()

    assert first == second
    assert module.trace_projection(first) == module.trace_projection(second)

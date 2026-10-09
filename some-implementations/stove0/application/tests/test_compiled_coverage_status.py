"""Explicit accepted status is routable without treating incomplete facts as negatives."""

import pytest
from compiled_program_fixture import run_program
from pydantic import TypeAdapter
from stove0_core.planning_program import condition_truth
from stove0_observer_protocol import SemanticFactsConformanceVectors
from stove0_observer_protocol.interfaces import subject_interface
from stove0_protocol.observation_views import SubjectView
from stove0_protocol.predicates import Predicate, Truth
from stove0_recipe_config.dependencies import ObserverResource, RecipeDependencyCatalog
from test_compiled_program import _catalog, _member, _source, _test


def _status(*, scope="self", status="unsupported"):
    condition = _test("probe.artifacts", "/status", status, scope=scope)
    condition["facts"]["inspect"] = "status"
    return condition


def _status_catalog():
    catalog = _catalog()
    owner = catalog.resources["facts"].contract
    vectors = SemanticFactsConformanceVectors(
        profile_id="example.status-facts/v1",
        vectors=sorted(
            [
                {
                    "id": "missing",
                    "accepted": False,
                    "subjects": [_member("one")],
                    "facts": {"artifacts": []},
                },
                {
                    "id": "complete",
                    "accepted": True,
                    "subjects": [_member("one")],
                    "facts": {
                        "artifacts": [{"subject_id": "one", "kind": "media", "discard": False}]
                    },
                },
                {
                    "id": "unsupported",
                    "accepted": True,
                    "subjects": [_member("one")],
                    "facts": {
                        "artifacts": [
                            {"subject_id": "one", "kind": "unsupported", "discard": False}
                        ]
                    },
                },
            ],
            key=lambda vector: vector["id"],
        ),
    )
    interface, interface_vectors = subject_interface(
        contract=owner,
        facts_vectors=vectors,
        subject_at="/subject_id",
        id="example.status-interface/v1",
        status={
            "kind": "records",
            "records_at": "/artifacts",
            "subject_at": "/subject_id",
            "value_at": "/kind",
            "values": {"media": "complete", "unsupported": "unsupported"},
        },
    )
    return RecipeDependencyCatalog(
        resources={
            **catalog.resources,
            "facts": ObserverResource(
                contract=owner, interface=interface, interface_vectors=interface_vectors
            ),
        }
    )


def test_mixed_collection_processes_supported_member_and_retains_explicit_unsupported(tmp_path):
    source = _source(
        roles={"media": "example.media/v1", "retained": "example.retained/v1"},
        classify={
            "cases": [
                {"role": "retained", "when": _status()},
                {"role": "media", "when": _test("probe.artifacts", "/kind", "media", scope="self")},
            ],
            "otherwise": None,
        },
        groups={"media": {"primary": "media"}},
        fork={"deliver": {"select": {"groups": "media"}, "call": {"operation": "effect"}}},
    )
    planner, progress, work = run_program(
        tmp_path,
        source,
        _status_catalog(),
        (_member("one"), _member("two")),
        facts_by_task={
            "probe": {
                "one": {"kind": "media", "discard": False},
                "two": {"kind": "unsupported", "discard": False},
            }
        },
    )
    assert progress.state == "ready"
    assert planner.state.compiled_planning.inventory_ref(work.work_id).artifact_count == 2
    branch = progress.decision.plan.branches[0]
    assert [
        s.id
        for s in planner.state.load_selection(branch.artifact_selection.selection_sha256).artifacts
    ] == ["one"]
    assert progress.decision.plan.source_collection_retirement_policy == "retain"
    authority = planner.state.accepted_observations.accepted(work.work_id, "probe")
    assert authority.question.scope.artifact_count == 2
    planner.state.engine.dispose()


@pytest.mark.parametrize(
    "views",
    [
        lambda *_: None,
        lambda *_: SubjectView(rows={"one": ()}, statuses={}),
        lambda *_: SubjectView(rows={}, statuses={}),
    ],
)
def test_missing_status_stays_indeterminate_even_under_negation(views):
    condition = TypeAdapter(Predicate).validate_python({"not": _status()})
    member = _member("one")
    assert (
        condition_truth(condition, views=views, inputs=(member,), subject=member)
        == Truth.INDETERMINATE
    )


def test_unsupported_status_does_not_make_record_negation_a_complete_negative():
    condition = TypeAdapter(Predicate).validate_python(
        {"not": _test("probe.artifacts", "/kind", "media", scope="self")}
    )
    view = SubjectView(
        rows={"one": ({"subject_id": "one", "kind": "unsupported", "discard": False},)},
        statuses={"one": "unsupported"},
    )
    member = _member("one")
    assert (
        condition_truth(condition, views=lambda *_: view, inputs=(member,), subject=member)
        == Truth.INDETERMINATE
    )

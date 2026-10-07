"""A waiting question cannot prevent independent compiled tasks from progressing."""

from pathlib import Path

from a_stove0_magic_facts_contract_lib import (
    MAGIC_INTERFACE,
    MAGIC_INTERFACE_VECTORS,
    MAGIC_OBSERVER_CONTRACT,
)
from a_stove0_magic_facts_contract_lib.contracts import MAGIC_CONFORMANCE_VECTORS
from stove0_core.compiled_observations import CompiledObservationPlanning
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.recipes import RecipePlanner
from stove0_protocol import ArtifactSelection, WorkIdentity, WorkPayload
from stove0_recipe_config import RecipeSource, compile_recipe
from stove0_recipe_config.catalog import CompiledRecipeCatalog, RecipeSourceCatalog
from stove0_recipe_config.dependencies import ObserverResource, RecipeDependencyCatalog
from test_accepted_observations import _subject
from test_compiled_observation_planning import _answer, _descriptor


def test_pending_first_task_does_not_starve_independent_task_after_controller_restarts(
    tmp_path: Path,
):
    recipe, closure = compile_recipe(
        RecipeSource.model_validate(
            {
                "format": "stove0-recipe/v1",
                "id": "fixture.fair-tasks/v1",
                "revision": 1,
                "observe": {
                    "aaa": {"use": "magic"},
                    "after_aaa": {"use": "magic", "after": ["aaa"]},
                    "zzz": {"use": "magic"},
                },
                "decisions": [
                    {
                        "when": True,
                        "no_output": {
                            "code": "fixture.checked/v1",
                            "message": "Every task completed.",
                        },
                    }
                ],
            }
        ),
        RecipeDependencyCatalog(
            resources={
                "magic": ObserverResource(
                    contract=MAGIC_OBSERVER_CONTRACT,
                    interface=MAGIC_INTERFACE,
                    interface_vectors=MAGIC_INTERFACE_VECTORS,
                    facts_vectors=MAGIC_CONFORMANCE_VECTORS,
                )
            }
        ),
    )
    members = tuple(_subject(index) for index in range(131))
    inventory = ArtifactSelection.seal(members)
    work = WorkIdentity.seal(
        WorkPayload(recipe=recipe.ref, inputs=inventory.roots(), effective_intent={})
    )
    url = f"sqlite:///{tmp_path / 'state.db'}"
    state = SqlAlchemyStateStore(url).planning_context("work", work.work_id)
    state.recipe_definitions.retain(recipe, closure)
    state.retain_selection(inventory)
    row = state.compiled_planning.ensure(work.work_id, recipe)
    state.compiled_planning.bind_inventory(
        work.work_id, expected_revision=row["revision"], scope=inventory.ref()
    )
    submitted = 0
    pending_seen = 0
    for step in range(150):
        progress = CompiledObservationPlanning(state, object()).step(work)
        assert progress.state != "complete"
        if progress.state == "question":
            question = progress.question
            assert question.task_id != "after_aaa"
            if question.task_id == "aaa":
                pending_seen += 1
            else:
                assert question.task_id == "zzz"
                page = members[submitted : submitted + 64]
                assert page
                _answer(state, question, _descriptor(), page)
                submitted += len(page)
        accepted = state.accepted_observations.accepted(work.work_id, "zzz")
        if accepted is not None:
            break
        if step % 7 == 0:
            state.engine.dispose()
            state = SqlAlchemyStateStore(url).planning_context("work", work.work_id)
    else:
        raise AssertionError("independent named task stopped making bounded progress")
    assert submitted == 131 and pending_seen >= 3
    assert accepted.question.scope == inventory.ref()
    assert accepted.results.result_count == 3
    assert state.accepted_observations.accepted(work.work_id, "aaa") is None
    assert state.accepted_observations.question(work.work_id, "after_aaa") is None
    state.engine.dispose()


def test_pending_nested_recipe_does_not_starve_independent_sibling(tmp_path: Path):
    catalog = RecipeSourceCatalog(
        resources={
            "magic": ObserverResource(
                contract=MAGIC_OBSERVER_CONTRACT,
                interface=MAGIC_INTERFACE,
                interface_vectors=MAGIC_INTERFACE_VECTORS,
                facts_vectors=MAGIC_CONFORMANCE_VECTORS,
            ),
        },
        recipes={
            "pending": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "fixture.pending/v1",
                    "revision": 1,
                    "observe": {"question": {"use": "magic"}},
                    "decisions": [
                        {"when": True, "no_output": {"code": "done", "message": "Checked."}}
                    ],
                }
            ),
            "ready": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "fixture.ready/v1",
                    "revision": 1,
                    "decisions": [
                        {"when": True, "no_output": {"code": "done", "message": "Checked."}}
                    ],
                }
            ),
            "parent": RecipeSource.model_validate(
                {
                    "format": "stove0-recipe/v1",
                    "id": "fixture.parent/v1",
                    "revision": 1,
                    "fork": {
                        "aaa": {"call": {"recipe": "pending"}},
                        "zzz": {"call": {"recipe": "ready"}},
                    },
                }
            ),
        },
    ).compile()
    subject = _subject(1)
    inventory = ArtifactSelection.seal((subject,))
    url = f"sqlite:///{tmp_path / 'state.db'}"
    planner = RecipePlanner(
        catalog=catalog,
        state=SqlAlchemyStateStore(url),
        riverhog=object(),
        observers=object(),
        targets=object(),
    )
    work = planner.create_work("fixture.parent/v1", (subject.collection,))
    planner = planner.for_invocation("work", work.work_id)
    planner.state.retain_selection(inventory)
    row = planner.state.compiled_planning.ensure(work.work_id, catalog.recipe(work.recipe.id))
    planner.state.compiled_planning.bind_inventory(
        work.work_id, expected_revision=row["revision"], scope=inventory.ref()
    )
    pending_questions = 0
    for step in range(300):
        progress = planner.step(work)
        assert progress.state in {"pending", "question"}
        if progress.state == "question":
            pending_questions += 1
        branches = planner.state.compiled_runtime.branches(work.work_id)
        ready = next((branch for branch in branches if branch["branch_id"] == "zzz"), None)
        if ready and ready["child_work_id"]:
            child = planner.state.compiled_runtime.load(ready["child_work_id"])
            if child["phase"] == "no-output" and pending_questions:
                break
        if step % 7 == 0:
            planner.state.engine.dispose()
            planner = RecipePlanner(
                catalog=CompiledRecipeCatalog(),
                state=SqlAlchemyStateStore(url).planning_context("work", work.work_id),
                riverhog=object(),
                observers=object(),
                targets=object(),
            )
    else:
        raise AssertionError("pending nested recipe prevented sibling progress")
    assert pending_questions > 0
    pending = next(branch for branch in branches if branch["branch_id"] == "aaa")
    assert planner.state.compiled_runtime.load(pending["child_work_id"])["phase"] == "observations"
    planner.state.engine.dispose()

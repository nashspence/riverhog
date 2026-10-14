"""A large classification cannot restart at zero or expose a complete prefix."""

from __future__ import annotations

import pytest
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_protocol import ArtifactSelection, CollectionRootIdentityRef, WorkArtifactSubject
from stove0_recipe_config.compiler import compile_recipe
from stove0_recipe_config.dependencies import RecipeDependencyCatalog
from stove0_recipe_config.source import RecipeSource


def _recipe():
    return compile_recipe(
        RecipeSource.model_validate(
            {
                "format": "stove0-recipe/v1",
                "id": "example.recipe/v1",
                "revision": 1,
                "decisions": [
                    {
                        "when": True,
                        "no_output": {
                            "code": "example.no-output/v1",
                            "message": "No target is needed.",
                        },
                    }
                ],
            }
        ),
        RecipeDependencyCatalog(),
    )[0]


def test_classification_resumes_and_role_ports_cannot_use_a_partial_inventory(tmp_path):
    url = f"sqlite:///{tmp_path / 'state.db'}"
    recipe = _recipe()
    root = CollectionRootIdentityRef(
        collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
    )
    subjects = tuple(
        WorkArtifactSubject(
            id=f"member-{i:04}",
            role="stove0.source/v1",
            collection=root,
            artifact_id=f"{i + 1:064x}",
            bytes="1",
            sha256="c" * 64,
        )
        for i in range(257)
    )
    selection = ArtifactSelection.seal(subjects)
    state = SqlAlchemyStateStore(url)
    state.retain_selection(selection)
    row = state.compiled_planning.ensure("d" * 64, recipe)
    state.compiled_planning.bind_inventory(
        "d" * 64, expected_revision=row["revision"], scope=selection.ref()
    )
    for count, complete in ((100, False), (200, False), (257, True)):
        assert (
            state.compiled_planning.classify_step(
                "d" * 64, recipe=recipe, views=lambda *_: None, limit=100
            )
            == complete
        )
        assert state.compiled_planning.ensure("d" * 64, recipe)["classified_count"] == count
        if not complete:
            with pytest.raises(ValueError, match="complete classification"):
                state.compiled_planning.role_page("d" * 64, roles=("stove0.source/v1",))
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    seen = []
    cursor = ""
    while True:
        page, cursor, complete = state.compiled_planning.role_page(
            "d" * 64, roles=("stove0.source/v1",), after_subject_id=cursor, limit=100
        )
        seen.extend(page)
        if complete:
            break
    assert tuple(seen) == subjects
    assert len({subject.id for subject in seen}) == 257
    state.engine.dispose()


def test_role_input_ports_and_disjoint_union_preserve_complete_named_scope(tmp_path):
    from stove0_core.compiled_input_scopes import CompiledInputScopes
    from stove0_recipe_config.compiled import CompiledRoleSelection

    recipe = _recipe()
    root = CollectionRootIdentityRef(
        collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
    )
    subjects = tuple(
        WorkArtifactSubject(
            id=f"member-{i:04}",
            role="stove0.source/v1",
            collection=root,
            artifact_id=f"{i + 1:064x}",
            bytes="1",
            sha256="c" * 64,
        )
        for i in range(203)
    )
    selection = ArtifactSelection.seal(subjects)
    from stove0_protocol import WorkIdentity, WorkPayload

    work = WorkIdentity.seal(WorkPayload(recipe=recipe.ref, inputs=(root,), effective_intent={}))
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    state.retain_selection(selection)
    planning = state.compiled_planning.ensure(work.work_id, recipe)
    state.compiled_planning.bind_inventory(
        work.work_id, expected_revision=planning["revision"], scope=selection.ref()
    )
    while not state.compiled_planning.classify_step(
        work.work_id, recipe=recipe, views=lambda *_: None
    ):
        pass
    scopes = CompiledInputScopes(state)
    for _ in range(20):
        reference = scopes.subject_port(
            work=work,
            task_id="probe",
            port_id="subjects",
            binding=CompiledRoleSelection(roles=("stove0.source/v1",)),
            inventory=selection.ref(),
        )
        if reference is not None:
            break
    else:
        pytest.fail("role input port failed to complete its bounded selection")
    assert reference == selection.ref()
    empty = ArtifactSelection.seal(())
    state.retain_selection(empty)
    for _ in range(20):
        union = scopes.subject_union(
            work=work, task_id="relate", ports={"primary": reference, "associated": empty.ref()}
        )
        if union is not None:
            break
    else:
        pytest.fail("disjoint subject union failed to progress")
    assert union == selection.ref()
    with pytest.raises(ValueError, match="repeats"):
        for _ in range(20):
            scopes.subject_union(
                work=work, task_id="overlap", ports={"primary": reference, "associated": reference}
            )
        pytest.fail("overlapping subject ports were not rejected")
    state.engine.dispose()

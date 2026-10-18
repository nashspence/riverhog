from __future__ import annotations

from types import SimpleNamespace

import pytest
from stove0_core.persistence import SqlAlchemyStateStore
from stove0_core.work_metadata import WorkInventory
from stove0_core.work_state import ConcurrentWorkUpdate
from stove0_protocol import (
    ArtifactSelection,
    CollectionRootIdentityRef,
    RecipeIdentityRef,
    WorkArtifactSubject,
    WorkIdentity,
    WorkPayload,
)


def _subjects(count):
    root = CollectionRootIdentityRef(
        collection_id="1", archive_root_sha256="a" * 64, artifact_set_identity="b" * 64
    )
    return tuple(
        WorkArtifactSubject(
            id=f"member-{i}",
            role="stove0.source/v1",
            collection=root,
            artifact_id=f"{i + 1:064x}",
            bytes=str(i + 1),
            sha256="c" * 64,
        )
        for i in range(count)
    )


def test_bounded_selection_seal_matches_existing_authority_and_resumes_after_restart(tmp_path):
    url = f"sqlite:///{tmp_path / 'state.db'}"
    state = SqlAlchemyStateStore(url)
    subjects = _subjects(307)
    builders = state.metadata_selections
    row = builders.ensure("d" * 64, binding={"work_id": "e" * 64}, source={"page": 0})
    row = builders.append(
        "d" * 64,
        expected_revision=row["revision"],
        subjects=subjects,
        source={"page": 1},
        complete=True,
    )
    expected = ArtifactSelection.seal(subjects)
    steps = 0
    while (authority := state.metadata_selections.seal_step("d" * 64, limit=100)) is None:
        steps += 1
        assert state.load_selection_ref(expected.selection_sha256) is None
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    assert steps == 7
    assert authority == expected.ref()
    assert state.load_selection(authority.selection_sha256) == expected
    assert tuple(state.iter_selection_artifacts(authority.selection_sha256)) == expected.artifacts
    state.engine.dispose()


def test_stale_append_cursor_cannot_duplicate_inventory_and_incomplete_header_is_hidden(tmp_path):
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    builders = state.metadata_selections
    builders.ensure("d" * 64, binding={"work_id": "e" * 64}, source={"page": 0})
    row = builders.append(
        "d" * 64, expected_revision=1, subjects=_subjects(2), source={"page": 1}, complete=False
    )
    with pytest.raises(ConcurrentWorkUpdate):
        builders.append(
            "d" * 64, expected_revision=1, subjects=(), source={"page": 1}, complete=False
        )
    with pytest.raises(ValueError, match="repeats"):
        builders.append(
            "d" * 64,
            expected_revision=row["revision"],
            subjects=_subjects(1),
            source={"page": 2},
            complete=False,
        )
    assert builders.load("d" * 64)["member_count"] == 2
    builders.append(
        "d" * 64, expected_revision=row["revision"], subjects=(), source={"page": 1}, complete=True
    )
    assert builders.seal_step("d" * 64) is None
    expected = ArtifactSelection.seal(_subjects(2))
    assert state.load_selection_ref(expected.selection_sha256) is None
    with pytest.raises(ValueError, match="not sealed"):
        state.selection_artifact_page(expected.selection_sha256, continuation=None, limit=100)
    assert builders.seal_step("d" * 64) == expected.ref()
    state.engine.dispose()


def test_work_inventory_reads_each_source_page_once_across_controller_restarts(tmp_path):
    url = f"sqlite:///{tmp_path / 'state.db'}"
    subjects = _subjects(205)
    work = WorkIdentity.seal(
        WorkPayload(
            recipe=RecipeIdentityRef(id="example.recipe/v1", revision="1", sha256="d" * 64),
            inputs=(subjects[0].collection,),
            effective_intent={},
        )
    )
    calls = []

    class Source:
        def get_collection(self, collection_id):
            return {"archive_root_sha256": "a" * 64, "artifact_set_identity": "b" * 64}

        def get_portable_collection_inventory(
            self, collection_id, *, cursor, limit, inventory_identity
        ):
            calls.append(cursor)
            start = 0 if cursor is None else int(cursor)
            end = min(start + 100, len(subjects))
            return SimpleNamespace(
                authority=SimpleNamespace(inventory_identity="f" * 64),
                artifacts=subjects[start:end],
                complete=end == len(subjects),
                next_cursor=str(end) if end < len(subjects) else None,
            )

    state = SqlAlchemyStateStore(url)
    source = Source()
    for _step in range(20):
        authority = WorkInventory(state, source).step(work)
        if authority is not None:
            break
        state.engine.dispose()
        state = SqlAlchemyStateStore(url)
    else:
        pytest.fail("inventory did not progress through its bounded continuation")
    assert calls == [None, "100", "200"]
    assert authority.artifact_count == 205
    assert state.load_selection(authority.selection_sha256).total_bytes == sum(
        subject.bytes for subject in subjects
    )
    state.engine.dispose()

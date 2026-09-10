from __future__ import annotations

import pytest
from riverhog_client.initial_tags import (
    create_or_resume_with_initial_collection_tags,
    prepare_initial_collection_tags,
)
from riverhog_protocol import COLLECTION_TAG_REQUEST_MEMBERS_MAX


def test_initial_tags_have_one_identity_across_deterministic_bounded_batches() -> None:
    tags = [f"camera/{index:04d}" for index in range(205)]
    with prepare_initial_collection_tags(reversed(tags)) as reversed_prepared:
        reversed_identity = reversed_prepared.tag_set_identity
        batches = tuple(reversed_prepared.iter_batches())
        replayed_batches = tuple(reversed_prepared.iter_batches())
    with prepare_initial_collection_tags(tags) as ordered_prepared:
        ordered_identity = ordered_prepared.tag_set_identity

    assert reversed_identity == ordered_identity
    assert replayed_batches == batches
    assert tuple(tag for batch in batches for tag in batch) == tuple(tags)
    assert tuple(map(len, batches)) == (
        COLLECTION_TAG_REQUEST_MEMBERS_MAX,
        COLLECTION_TAG_REQUEST_MEMBERS_MAX,
        5,
    )


@pytest.mark.parametrize("tail", ["camera/0000", " invalid"])
def test_initial_tags_reject_a_late_invalid_member_before_replay(tail: str) -> None:
    observed = 0

    def values():
        nonlocal observed
        for tag in [*(f"camera/{index:04d}" for index in range(150)), tail]:
            observed += 1
            yield tag

    with pytest.raises(ValueError):
        prepare_initial_collection_tags(values())
    assert observed == 151


@pytest.mark.parametrize(
    "state",
    ["closing", "uploading", "finalizing", "finalized"],
)
def test_initial_tag_continuation_never_mutates_a_non_open_session(state: str) -> None:
    tags = tuple(f"camera/{index:04d}" for index in range(205))
    opened: list[tuple[tuple[str, ...], str]] = []
    added: list[tuple[int, tuple[str, ...]]] = []

    result = create_or_resume_with_initial_collection_tags(
        tags,
        create_or_resume=lambda first, identity: (
            opened.append((tuple(first), identity)) or {"collection_id": 17, "state": state}
        ),
        add_tags=lambda collection_id, batch: added.append((collection_id, tuple(batch))),
    )

    assert result == {"collection_id": 17, "state": state}
    assert opened[0][0] == tags[:COLLECTION_TAG_REQUEST_MEMBERS_MAX]
    assert len(opened[0][1]) == 64
    assert added == []


def test_initial_tag_continuation_stages_every_remaining_bounded_open_batch() -> None:
    tags = tuple(f"camera/{index:04d}" for index in range(205))
    added: list[tuple[int, tuple[str, ...]]] = []

    create_or_resume_with_initial_collection_tags(
        reversed(tags),
        create_or_resume=lambda first, _identity: {
            "collection_id": 17,
            "state": "open",
        },
        add_tags=lambda collection_id, batch: added.append((collection_id, tuple(batch))),
    )

    assert added == [
        (17, tags[COLLECTION_TAG_REQUEST_MEMBERS_MAX:200]),
        (17, tags[200:]),
    ]

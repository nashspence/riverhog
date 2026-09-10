from __future__ import annotations

import pytest
from riverhog_client.initial_tags import prepare_initial_collection_tags
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

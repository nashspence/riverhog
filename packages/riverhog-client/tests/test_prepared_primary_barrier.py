"""Cold allocation can contract without losing the fixed barrier's backing."""

from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_client._prepared_primary_sqlite_space import SqlitePrimarySpace
from riverhog_client._prepared_primary_types import PrimaryFileSpace

from tests.support.prepared_primary_barrier import assert_cold_space


def _space():
    return SqlitePrimarySpace(
        page_bytes=4096,
        database_pages=8,
        reusable_pages=0,
        database=PrimaryFileSpace(end_bytes=32768, allocated_bytes=65536),
        journal=PrimaryFileSpace(end_bytes=12000, allocated_bytes=65536),
        ordering_grant_bytes=0,
        journal_reserved_bytes=10000,
    )


def test_cold_barrier_allows_released_excess_allocation_with_exact_extent_and_reserve():
    before = _space()
    assert_cold_space(before, before)
    after = replace(
        before,
        database=PrimaryFileSpace(end_bytes=32768, allocated_bytes=32768),
        journal=PrimaryFileSpace(end_bytes=12000, allocated_bytes=12288),
    )
    assert_cold_space(before, after)


@pytest.mark.parametrize(
    "changes",
    [
        {"database": PrimaryFileSpace(end_bytes=36864, allocated_bytes=65536)},
        {"journal": PrimaryFileSpace(end_bytes=16000, allocated_bytes=65536)},
        {"database": PrimaryFileSpace(end_bytes=32768, allocated_bytes=131072)},
        {"journal": PrimaryFileSpace(end_bytes=12000, allocated_bytes=131072)},
        {"database": PrimaryFileSpace(end_bytes=32768, allocated_bytes=16384)},
        {"journal": PrimaryFileSpace(end_bytes=12000, allocated_bytes=8192)},
        {"journal_reserved_bytes": 12001},
        {"reusable_pages": 1},
    ],
)
def test_cold_barrier_rejects_any_growth_lost_backing_or_changed_logical_state(changes):
    before = _space()
    with pytest.raises(AssertionError) as failure:
        assert_cold_space(before, replace(before, **changes), start=127)
    assert "before" in str(failure.value) and "after" in str(failure.value)

from __future__ import annotations

from riverhog_core.placement_choices import common_write_constraints
from riverhog_core.ports.archive_objects import ResumableWriteConstraints


def test_cache_mirror_uses_only_segment_sizes_every_store_can_accept() -> None:
    archive = ResumableWriteConstraints(5, 20, 100)
    first = ResumableWriteConstraints(8, 16, 50)
    second = ResumableWriteConstraints(10, 12, 20)

    assert common_write_constraints(archive, (first, second)) == ResumableWriteConstraints(
        10, 12, 20
    )
    assert common_write_constraints(archive, (ResumableWriteConstraints(21, 30, 50),)) is None
    assert common_write_constraints(archive, ()) is None

from __future__ import annotations

from pathlib import Path

import pytest
from a_riverhog_filesystem_store.inventory import FilesystemInventory
from riverhog_storage_adapter_protocol import CleanupCandidate, StorageAdapterRejection


@pytest.mark.parametrize("dimension", ["history", "unrelated"])
@pytest.mark.parametrize("count", [16, 256, 4096])
def test_inventory_pages_seek_past_history_without_rescanning_unrelated_objects(
    tmp_path: Path,
    dimension: str,
    count: int,
    record_property,
) -> None:
    inventory = FilesystemInventory(tmp_path)
    history = count if dimension == "history" else 3
    unrelated = count if dimension == "unrelated" else 3
    try:
        inventory._connection.execute("BEGIN")
        for index in range(history):
            inventory.add(
                CleanupCandidate(
                    object_path="middle/owned", kind="object_revision", token=f"{index:08d}"
                )
            )
        for index in range(unrelated):
            inventory.add(
                CleanupCandidate(
                    object_path=f"outside/{index:08d}", kind="object_revision", token="revision"
                )
            )
        inventory._connection.execute("COMMIT")
        assert (
            inventory._connection.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
            == history + unrelated
        )
        steps = [0]
        statements: list[str] = []

        def tick() -> int:
            steps[0] += 1
            return 0

        inventory._connection.set_trace_callback(statements.append)
        inventory._connection.set_progress_handler(tick, 1)
        try:
            page = inventory.page(
                "middle/",
                after=CleanupCandidate(
                    object_path="middle/owned", kind="object_revision", token=f"{history - 2:08d}"
                ),
                limit=1,
            )
        finally:
            inventory._connection.set_progress_handler(None, 0)
            inventory._connection.set_trace_callback(None)
        assert page == (
            CleanupCandidate(
                object_path="middle/owned", kind="object_revision", token=f"{history - 1:08d}"
            ),
        )
        assert len(statements) == 1
        plan = inventory._connection.execute("EXPLAIN QUERY PLAN " + statements[0]).fetchall()
        assert any("USING PRIMARY KEY" in str(row) for row in plan)
        # B-tree descent is logarithmic; the SQL program must not visit a
        # growing prefix/history before applying its full continuation.
        assert steps[0] < 120
        assert inventory.page("middle/absent/", after=None, limit=1) == ()
        record_property("history_rows", history)
        record_property("unrelated_rows", unrelated)
        record_property("selection_vm_instructions", steps[0])
    finally:
        inventory.close()
    assert not (tmp_path / "inventory-cache").exists()


def test_inventory_is_disposable_and_failure_is_not_empty_success(tmp_path: Path) -> None:
    original = FilesystemInventory(tmp_path)
    original.add(CleanupCandidate(object_path="archive/old", kind="open_write", token="old"))
    # Model process loss by closing its cache connection without retaining the cache as authority.
    original._connection.close()
    reopened = FilesystemInventory(tmp_path)
    try:
        assert reopened.page("archive/", after=None, limit=1) == ()
        reopened._connection.close()
        with pytest.raises(StorageAdapterRejection, match="owner recovery"):
            reopened.page("archive/", after=None, limit=1)
        with pytest.raises(StorageAdapterRejection, match="unavailable"):
            reopened.check()
    finally:
        reopened.close()

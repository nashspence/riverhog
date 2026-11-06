from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest
from a_riverhog_filesystem_store import FilesystemStorageAdapter, FilesystemStorageAdapterConfig
from a_riverhog_filesystem_store.incarnation import provision_storage_root
from riverhog_storage_adapter_protocol import (
    CleanupCandidate,
    CleanupInventoryPage,
    CleanupInventoryRequest,
    CleanupInvocation,
    CleanupStepRequest,
    SmallObjectWriteRequest,
    WriteCallIdentity,
    WriteStartRequest,
    cleanup_step_sha256,
)


def _invocation(page: CleanupInventoryPage) -> CleanupInvocation:
    request = CleanupStepRequest(
        selected_terminal_sha256="1" * 64,
        inventory=page,
        maximum_provider_requests=1,
        maximum_control_bytes=1024 * 1024,
        maximum_in_flight=1,
    )
    return CleanupInvocation(
        call=WriteCallIdentity(
            call_id="2" * 64,
            exact_request_sha256=cleanup_step_sha256(request),
            dispatch_owner="3" * 64,
            target_incarnation=page.target_incarnation,
        ),
        request=request,
    )


def test_filesystem_selected_cleanup_replays_after_reopen_and_preserves_other_scope(
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    root.mkdir()
    provision_storage_root(root)
    config = FilesystemStorageAdapterConfig(
        root=root,
        segment_bytes=65536,
        read_chunk_bytes=65536,
        minimum_free_bytes=0,
    )
    with FilesystemStorageAdapter(config) as adapter:
        for path in ("archive/owned/a", "unrelated/keep"):
            adapter.put_small_object(
                SmallObjectWriteRequest(
                    object_path=path,
                    stored_bytes=1,
                    stored_sha256=hashlib.sha256(b"x").hexdigest(),
                    content_type="application/octet-stream",
                    required_identity_assertions={},
                    placement_policy="immediate_default",
                    mode="create_only",
                ),
                b"x",
            )
        session = adapter.begin_write(
            WriteStartRequest(
                object_path="archive/owned/b",
                expected_bytes=1,
                content_type="application/octet-stream",
                required_identity_assertions={},
                placement_policy="archive_default",
            )
        )
        page = adapter.list_cleanup_candidates(
            CleanupInventoryRequest(
                object_prefix="archive/owned/",
                target_incarnation=adapter.descriptor().storage_incarnation_id,
            )
        )
        assert len(page.candidates) == 2 and page.exhausted
        invocation = _invocation(page)
        first = adapter.execute_cleanup(invocation)
        assert first.execution == "closed" and first.result is not None
        assert first.result.selection_complete and first.result.exhausted
        assert first.result.provider_requests == 1
        assert not adapter._write_dir_by_key(adapter._object_key(session.object_path)).exists()
    with FilesystemStorageAdapter(config) as reopened:
        retry = invocation.model_copy(
            update={"call": invocation.call.model_copy(update={"call_id": "4" * 64})}
        )
        replay = reopened.execute_cleanup(retry)
        assert replay.execution == first.execution and replay.result == first.result
        assert replay.call == retry.call and replay.call != first.call
        untouched = reopened.list_cleanup_candidates(
            CleanupInventoryRequest(
                object_prefix="unrelated/",
                target_incarnation=page.target_incarnation,
            )
        )
        assert len(untouched.candidates) == 1
        assert not reopened.list_cleanup_candidates(
            CleanupInventoryRequest(
                object_prefix="archive/owned/",
                target_incarnation=page.target_incarnation,
            )
        ).candidates


@pytest.mark.parametrize("history", [16, 256, 4096])
def test_filesystem_exact_cleanup_does_not_enumerate_revision_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    history: int,
    record_property,
) -> None:
    root = tmp_path / "store"
    root.mkdir()
    provision_storage_root(root)
    with FilesystemStorageAdapter(
        FilesystemStorageAdapterConfig(
            root=root,
            segment_bytes=65536,
            read_chunk_bytes=65536,
            minimum_free_bytes=0,
        )
    ) as adapter:
        receipt = adapter.put_small_object(
            SmallObjectWriteRequest(
                object_path="archive/owned",
                stored_bytes=1,
                stored_sha256=hashlib.sha256(b"x").hexdigest(),
                content_type="application/octet-stream",
                required_identity_assertions={},
                placement_policy="immediate_default",
                mode="create_only",
            ),
            b"x",
        )
        assert receipt.revision is not None and adapter._inventory is not None
        revisions = adapter._revision_dir(
            adapter._object_key(receipt.object_path), receipt.revision
        ).parent
        # Retained, provider-private provisional revision directories are real
        # namespace entries. Exact retirement must not enumerate them.
        for index in range(history):
            (revisions / f"retained-{index:08d}").mkdir()
        with os.scandir(revisions) as entries:
            assert sum(1 for _ in entries) == history + 1
        original = Path.iterdir

        def bounded_iterdir(path: Path):
            assert path != revisions, "Path.iterdir materializes complete revision history"
            return original(path)

        original_scandir = os.scandir
        visited = [0]

        class CountedEntries:
            def __init__(self, iterator):
                self.iterator = iterator

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.iterator.close()

            def __iter__(self):
                return self

            def __next__(self):
                entry = next(self.iterator)
                visited[0] += 1
                return entry

        def bounded_scandir(path):
            entries = original_scandir(path)
            return CountedEntries(entries) if path == revisions else entries

        selected = CleanupCandidate(
            object_path=receipt.object_path,
            kind="object_revision",
            token=receipt.revision,
        )
        page = CleanupInventoryPage(
            object_prefix="archive/",
            target_incarnation=adapter.descriptor().storage_incarnation_id,
            candidates=(selected,),
            scanned_items=1,
            exhausted=True,
        )
        with monkeypatch.context() as scoped:
            scoped.setattr(Path, "iterdir", bounded_iterdir)
            scoped.setattr(os, "scandir", bounded_scandir)
            result = adapter.execute_cleanup(_invocation(page))
        assert visited[0] == 1
        record_property("visited_retained_revision_directories", visited[0])
        assert result.execution == "closed" and result.result is not None
        assert result.result.selection_complete
        with os.scandir(revisions) as entries:
            assert sum(1 for _ in entries) == history
        record_property("retained_revision_directories", history)

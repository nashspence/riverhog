from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import riverhog_storage_adapter_filesystem.materialize as materialize_module
from riverhog_storage_adapter_filesystem import (
    FilesystemStorageAdapter,
    FilesystemStorageAdapterConfig,
)
from riverhog_storage_adapter_filesystem.materialize import (
    MaterializationError,
    MaterializationInterrupted,
    MaterializationSelection,
    materialize_committed_objects,
)
from riverhog_storage_adapter_filesystem.materialize_cli import main
from riverhog_storage_adapter_protocol import (
    SmallObjectWriteRequest,
    WriteCompleteRequest,
    WriteStartRequest,
)


def _adapter(root: Path) -> FilesystemStorageAdapter:
    return FilesystemStorageAdapter(
        FilesystemStorageAdapterConfig(
            root=root,
            segment_bytes=64 * 1024,
            read_chunk_bytes=64 * 1024,
            minimum_free_bytes=0,
        )
    )


def _put(adapter: FilesystemStorageAdapter, path: str, payload: bytes) -> None:
    adapter.put_small_object(
        SmallObjectWriteRequest(
            object_path=path,
            content_type="application/octet-stream",
            required_identity_assertions={"riverhog-object": path},
            placement="archive",
            mode="create_only",
            stored_bytes=len(payload),
            stored_sha256=hashlib.sha256(payload).hexdigest(),
        ),
        payload,
    )


def _put_segmented(
    adapter: FilesystemStorageAdapter,
    path: str,
    segments: tuple[bytes, ...],
) -> None:
    payload = b"".join(segments)
    request = WriteStartRequest(
        object_path=path,
        expected_bytes=len(payload),
        content_type="application/octet-stream",
        required_identity_assertions={"riverhog-object": path},
        placement="archive",
    )
    session = adapter.begin_write(request)
    receipts = tuple(
        adapter.write_segment(
            session=session,
            number=number,
            stored_bytes=len(segment),
            content=segment,
        )
        for number, segment in enumerate(segments, start=1)
    )
    adapter.complete_write(
        WriteCompleteRequest(
            session=session,
            segments=receipts,
            expected_bytes=len(payload),
            expected_content_type=request.content_type,
            required_identity_assertions=request.required_identity_assertions,
            expected_placement=request.placement,
        )
    )


def _metadata_for(root: Path, path: str) -> Path:
    key = hashlib.sha256(path.encode()).hexdigest()
    object_dir = root / "objects" / key[:2] / key[2:4] / key
    revision = (object_dir / "current").read_text(encoding="utf-8")
    return object_dir / "revisions" / revision / "metadata.json"


def test_materializes_only_current_selected_objects_as_logical_paths(tmp_path: Path) -> None:
    root = tmp_path / "store"
    with _adapter(root) as adapter:
        _put(adapter, "archives/one/recovery.json", b"descriptor")
        _put(adapter, "archives/one/manifest.json.age", b"root")
        _put(adapter, "archives/two/recovery.json", b"other")

    output = tmp_path / "export"
    summary = materialize_committed_objects(
        source=root,
        destination=output,
        selection=MaterializationSelection(prefixes=("archives/one/",)),
    )

    assert (output / "archives/one/recovery.json").read_bytes() == b"descriptor"
    assert (output / "archives/one/manifest.json.age").read_bytes() == b"root"
    assert not (output / "archives/two").exists()
    assert summary.selected_objects == 2
    assert summary.selected_bytes == len(b"descriptorroot")
    assert summary.copied_bytes == summary.selected_bytes
    assert summary.destination_verified_bytes == 0
    assert not (tmp_path / ".export.riverhog-materialization").exists()


def test_active_adapter_must_be_quiesced(tmp_path: Path) -> None:
    root = tmp_path / "store"
    with _adapter(root) as adapter:
        _put(adapter, "archives/one/recovery.json", b"descriptor")
        with pytest.raises(MaterializationError, match="active; quiesce"):
            materialize_committed_objects(
                source=root,
                destination=tmp_path / "export",
                selection=MaterializationSelection(all_objects=True),
            )


def test_every_explicit_selector_must_match_current_committed_state(tmp_path: Path) -> None:
    root = tmp_path / "store"
    with _adapter(root) as adapter:
        _put(adapter, "archives/one/recovery.json", b"descriptor")

    for selection in (
        MaterializationSelection(paths=("archives/one/missing.json.age",)),
        MaterializationSelection(prefixes=("archives/missing/",)),
    ):
        with pytest.raises(MaterializationError, match="selector has no current committed object"):
            materialize_committed_objects(
                source=root,
                destination=tmp_path / f"export-{len(selection.paths)}-{len(selection.prefixes)}",
                selection=selection,
            )


def test_restart_revalidates_projection_and_exact_published_payload(tmp_path: Path) -> None:
    root = tmp_path / "store"
    values = {
        "archives/one/a": b"alpha",
        "archives/one/b": b"beta",
        "archives/one/c": b"charlie",
    }
    with _adapter(root) as adapter:
        for path, payload in values.items():
            _put(adapter, path, payload)

    output = tmp_path / "export"
    selection = MaterializationSelection(prefixes=("archives/one/",))
    with pytest.raises(MaterializationInterrupted):
        materialize_committed_objects(
            source=root,
            destination=output,
            selection=selection,
            _interrupt_after_objects=1,
        )
    published = tuple(path for path in output.rglob("*") if path.is_file())
    assert len(published) == 1

    published_path = published[0].relative_to(output).as_posix()
    unpublished_path = next(path for path in values if path != published_path)
    unpublished_metadata = _metadata_for(root, unpublished_path)
    original_unpublished_metadata = unpublished_metadata.read_bytes()
    value = json.loads(original_unpublished_metadata)
    value["completed_at"] = "2026-09-10T01:02:03Z"
    unpublished_metadata.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(MaterializationError, match="projection changed"):
        materialize_committed_objects(
            source=root,
            destination=output,
            selection=selection,
        )

    unpublished_metadata.write_bytes(original_unpublished_metadata)
    published_metadata = _metadata_for(root, published_path)
    original_published_metadata = published_metadata.read_bytes()
    value = json.loads(original_published_metadata)
    value["completed_at"] = "2026-09-10T01:02:03Z"
    published_metadata.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(MaterializationError, match="projection changed"):
        materialize_committed_objects(
            source=root,
            destination=output,
            selection=selection,
        )

    published_metadata.write_bytes(original_published_metadata)
    published[0].write_bytes(b"wrong")
    with pytest.raises(MaterializationError, match="conflicts"):
        materialize_committed_objects(
            source=root,
            destination=output,
            selection=selection,
        )


@pytest.mark.parametrize("change", ["add", "remove"])
def test_restart_rejects_selected_namespace_changes(tmp_path: Path, change: str) -> None:
    root = tmp_path / "store"
    with _adapter(root) as adapter:
        _put(adapter, "archives/one/a", b"alpha")
        _put(adapter, "archives/one/b", b"beta")
    output = tmp_path / "export"
    selection = MaterializationSelection(prefixes=("archives/one/",))
    with pytest.raises(MaterializationInterrupted):
        materialize_committed_objects(
            source=root,
            destination=output,
            selection=selection,
            _interrupt_after_objects=1,
        )

    if change == "add":
        with _adapter(root) as adapter:
            _put(adapter, "archives/one/c", b"charlie")
    else:
        key = hashlib.sha256(b"archives/one/b").hexdigest()
        (root / "objects" / key[:2] / key[2:4] / key / "current").unlink()

    with pytest.raises(MaterializationError, match="projection changed"):
        materialize_committed_objects(
            source=root,
            destination=output,
            selection=selection,
        )


def test_materializes_current_revision_across_segments_and_copy_chunks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    path = "archives/one/segmented.age"
    old = b"obsolete"
    current_segments = (b"0" * (64 * 1024), b"56789abcdef")
    current = b"".join(current_segments)
    with _adapter(root) as adapter:
        _put(adapter, path, old)
        adapter.put_small_object(
            SmallObjectWriteRequest(
                object_path=path,
                content_type="application/octet-stream",
                required_identity_assertions={"riverhog-object": path},
                placement="archive",
                mode="replace_current",
                expected_current_stored_sha256=hashlib.sha256(old).hexdigest(),
                stored_bytes=len(current),
                stored_sha256=hashlib.sha256(current).hexdigest(),
            ),
            current,
        )

        segmented_path = "archives/one/segmented-write.age"
        _put_segmented(adapter, segmented_path, current_segments)

    monkeypatch.setattr(materialize_module, "_COPY_CHUNK_BYTES", 3)
    output = tmp_path / "export"
    materialize_committed_objects(
        source=root,
        destination=output,
        selection=MaterializationSelection(prefixes=("archives/one/",)),
    )

    assert (output / path).read_bytes() == current
    assert (output / segmented_path).read_bytes() == current


def test_corrupt_current_metadata_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "store"
    path = "archives/one/recovery.json"
    with _adapter(root) as adapter:
        _put(adapter, path, b"descriptor")
    _metadata_for(root, path).write_text('{"schema":"wrong"}', encoding="utf-8")

    with pytest.raises(MaterializationError, match="invalid shape"):
        materialize_committed_objects(
            source=root,
            destination=tmp_path / "export",
            selection=MaterializationSelection(paths=(path,)),
        )


def test_resume_reports_destination_revalidation_separately(tmp_path: Path) -> None:
    root = tmp_path / "store"
    values = {"archives/one/a": b"alpha", "archives/one/b": b"beta"}
    with _adapter(root) as adapter:
        for path, payload in values.items():
            _put(adapter, path, payload)
    output = tmp_path / "export"
    selection = MaterializationSelection(all_objects=True)
    with pytest.raises(MaterializationInterrupted):
        materialize_committed_objects(
            source=root,
            destination=output,
            selection=selection,
            _interrupt_after_objects=1,
        )

    summary = materialize_committed_objects(
        source=root,
        destination=output,
        selection=selection,
    )

    assert summary.destination_verified_objects == 1
    assert summary.destination_verified_bytes in {len(value) for value in values.values()}
    assert summary.copied_objects == 1
    assert summary.copied_bytes + summary.destination_verified_bytes == sum(
        len(value) for value in values.values()
    )


def test_restart_adopts_exact_object_published_before_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    values = {"archives/one/a": b"alpha", "archives/one/b": b"beta"}
    with _adapter(root) as adapter:
        for path, payload in values.items():
            _put(adapter, path, payload)
    original_copy = materialize_module._copy_object

    def publish_then_interrupt(*args: object, **kwargs: object) -> None:
        original_copy(*args, **kwargs)  # type: ignore[arg-type]
        raise MaterializationInterrupted("simulated crash before checkpoint")

    monkeypatch.setattr(materialize_module, "_copy_object", publish_then_interrupt)
    output = tmp_path / "export"
    with pytest.raises(MaterializationInterrupted, match="before checkpoint"):
        materialize_committed_objects(
            source=root,
            destination=output,
            selection=MaterializationSelection(all_objects=True),
        )
    monkeypatch.setattr(materialize_module, "_copy_object", original_copy)

    summary = materialize_committed_objects(
        source=root,
        destination=output,
        selection=MaterializationSelection(all_objects=True),
    )

    assert summary.destination_verified_objects == 1
    assert summary.copied_objects == 1
    assert {path: (output / path).read_bytes() for path in values} == values


def test_incomplete_and_no_current_objects_are_not_materialized(tmp_path: Path) -> None:
    root = tmp_path / "store"
    adapter = _adapter(root)
    _put(adapter, "archives/one/current", b"current")
    _put(adapter, "archives/one/abandoned", b"abandoned")
    adapter.begin_write(
        WriteStartRequest(
            object_path="archives/one/incomplete",
            expected_bytes=64 * 1024,
            content_type="application/octet-stream",
            required_identity_assertions={"riverhog-object": "incomplete"},
            placement="archive",
        )
    )
    adapter.close()

    abandoned_key = hashlib.sha256(b"archives/one/abandoned").hexdigest()
    abandoned = root / "objects" / abandoned_key[:2] / abandoned_key[2:4] / abandoned_key
    (abandoned / "current").unlink()

    output = tmp_path / "export"
    summary = materialize_committed_objects(
        source=root,
        destination=output,
        selection=MaterializationSelection(prefixes=("archives/one/",)),
    )

    assert summary.selected_objects == 1
    assert (output / "archives/one/current").read_bytes() == b"current"
    assert not (output / "archives/one/incomplete").exists()


def test_selected_corrupt_payload_fails_and_unselected_payload_is_never_opened(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    with _adapter(root) as adapter:
        _put(adapter, "archives/one/recovery.json", b"selected")
        _put(adapter, "archives/two/large.age", b"unselected")
    selected_metadata = _metadata_for(root, "archives/one/recovery.json")
    selected_payload = selected_metadata.parent / "payload.data"
    selected_payload.write_bytes(b"corrupt!")

    opened_payloads: list[Path] = []
    original_open = materialize_module.os.open

    def recording_open(path: object, flags: int, mode: int = 0o777) -> int:
        candidate = Path(path) if isinstance(path, (str, Path)) else None
        if candidate is not None and candidate.name == "payload.data":
            opened_payloads.append(candidate)
        return original_open(path, flags, mode)

    monkeypatch.setattr(materialize_module.os, "open", recording_open)
    with pytest.raises(MaterializationError, match="SHA-256"):
        materialize_committed_objects(
            source=root,
            destination=tmp_path / "export",
            selection=MaterializationSelection(paths=("archives/one/recovery.json",)),
        )

    assert opened_payloads == [selected_payload]


def test_cli_has_equivalent_human_and_json_results(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    root = tmp_path / "store"
    with _adapter(root) as adapter:
        _put(adapter, "archives/one/recovery.json", b"descriptor")

    assert main([str(root), str(tmp_path / "human"), "--all"]) == 0
    human = capsys.readouterr().out
    assert "Materialized 1 objects" in human
    assert "source-metadata=" in human

    assert main([str(root), str(tmp_path / "json"), "--all", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["format"] == "riverhog-filesystem-materialization-result/v1"
    assert payload["selected_objects"] == 1
    assert payload["selected_bytes"] == len(b"descriptor")

"""Encrypted full recovery of all four selected-copy collection contents."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import a_riverhog_recovery_tool.recovery as recovery_module
import pytest
from a_riverhog_recovery_tool import (
    RecoveryError,
    recover_archive,
    recover_collection_description,
    recover_collection_tags,
)

from tests.support.qualification.recovery_archive import (
    PASSPHRASE,
    PASSPHRASE_ID,
    FixtureArchive,
    write_archive,
)

pytestmark = pytest.mark.skipif(
    shutil.which("age") is None or shutil.which("age-plugin-batchpass") is None,
    reason="official age and age-plugin-batchpass are required",
)

_KEYS = {PASSPHRASE_ID: PASSPHRASE}


@pytest.fixture(scope="module")
def source_archive(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, FixtureArchive]:
    archive = tmp_path_factory.mktemp("recovery-v1") / "archive"
    fixture = write_archive(
        archive,
        description="Résumé of 東京 footage",
        tags=("camera:七", "source:ftp"),
    )
    return archive, fixture


def _copy_source(
    source: tuple[Path, FixtureArchive], destination: Path
) -> tuple[Path, FixtureArchive]:
    archive = destination / "archive"
    shutil.copytree(source[0], archive)
    return archive, source[1]


def _records(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_bytes().splitlines()]


@pytest.mark.parametrize("layout", ["declared-hints", "id-layout"])
def test_full_recovery_preserves_explicit_supplementary_history_and_structural_sets(
    tmp_path: Path, layout: str
) -> None:
    archive = tmp_path / "archive"
    fixture = write_archive(archive, late_shared_history=True, tags=("retained:history",))
    output = tmp_path / "recovered"
    summary = recover_archive(archive, output, passphrases=_KEYS, layout_mode=layout)
    assert summary.provenance_journals == 4
    for path, raw in fixture.history_objects.items():
        restored = output / "structure" / path.removesuffix(".age")
        assert restored.read_bytes() == raw
    for raw in fixture.journals.values():
        digest = hashlib.sha256(raw).hexdigest()
        assert (output / "provenance" / "journals" / (digest + ".jsonseq")).read_bytes() == raw
    for row in _records(output / "recovery-members.jsonseq"):
        history = json.loads(output.joinpath(*row["history"]).read_bytes())
        assert history["artifact_id"] == row["artifact_id"]
        assert int(history["roots"]["record_count"]) == 2
        assert (
            output.joinpath(*row["sidecar"]).read_bytes()
            == fixture.journals[history["primary"]["journal"]["journal_id"]]
        )


def test_full_recovery_cannot_substitute_payload_plus_primary_for_missing_selected_history(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "archive"
    fixture = write_archive(archive, late_shared_history=True)
    required = next(path for path in fixture.history_objects if path.startswith("provenance/sets/"))
    (archive / required).unlink()
    output = tmp_path / "recovered"
    with pytest.raises(RecoveryError):
        recover_archive(archive, output, passphrases=_KEYS)
    assert not output.exists()
    assert not (tmp_path / "recovered.partial" / "recovery.json").exists()


@pytest.mark.parametrize("layout", ["declared-hints", "id-layout"])
def test_full_recovery_preserves_inherited_late_history_without_the_source_archive(
    tmp_path: Path, layout: str
) -> None:
    source_dir = tmp_path / "source"
    source = write_archive(source_dir, late_shared_history=True)
    archive = tmp_path / "derived"
    derived = write_archive(archive, inherited_history=source)
    shutil.rmtree(source_dir)
    output = tmp_path / "recovered"
    summary = recover_archive(archive, output, passphrases=_KEYS, layout_mode=layout)
    assert summary.provenance_journals == 5
    proofs = [
        path for path in derived.history_objects if path.startswith("provenance/source-proofs/")
    ]
    assert len(proofs) == 1
    for path, raw in derived.history_objects.items():
        assert (output / "structure" / path.removesuffix(".age")).read_bytes() == raw
    for raw in derived.journals.values():
        digest = hashlib.sha256(raw).hexdigest()
        assert (output / "provenance" / "journals" / (digest + ".jsonseq")).read_bytes() == raw
    assert not source_dir.exists()


def test_complete_recovery_preserves_members_history_description_and_tags(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path
) -> None:
    archive, fixture = _copy_source(source_archive, tmp_path)
    output = tmp_path / "recovered"
    summary = recover_archive(archive, output, passphrases=_KEYS)

    assert summary.artifacts == len(fixture.members)
    assert summary.bytes == sum(map(len, fixture.members.values()))
    assert summary.volumes == 3
    assert summary.provenance_journals == len(fixture.journals)
    assert summary.tag_count == 2
    assert summary.layout_mode == "declared-hints"
    assert summary.archive_root_sha256 == fixture.archive_root_sha256
    assert summary.description_revision == summary.tag_revision == 1
    assert (output / "metadata/description.txt").read_text() == "Résumé of 東京 footage"
    description = json.loads((output / "metadata/description-state.json").read_bytes())
    assert description["status"] == "present"
    assert hashlib.sha256((output / "metadata/description.json").read_bytes()).hexdigest() == (
        fixture.description_sha256
    )
    tags = _records(output / "metadata/tags.jsonseq")
    assert [row["tag"] for row in tags if row["record"] == "tag"] == ["camera:七", "source:ftp"]
    assert tags[-1] == {"record": "complete", "tag_count": 2}
    assert hashlib.sha256((output / "metadata/tags/head.json").read_bytes()).hexdigest() == (
        fixture.tag_head_sha256
    )
    mapping = _records(output / "recovery-members.jsonseq")
    assert len(mapping) == len(fixture.members)
    for row in mapping:
        artifact_id = str(row["artifact_id"])
        payload = output.joinpath(*row["components"])
        assert payload.read_bytes() == fixture.members[artifact_id]
        history = json.loads(output.joinpath(*row["history"]).read_bytes())
        journal_id = history["primary"]["journal"]["journal_id"]
        assert output.joinpath(*row["sidecar"]).read_bytes() == fixture.journals[journal_id]
    for content in fixture.journals.values():
        sha256 = hashlib.sha256(content).hexdigest()
        assert (output / f"provenance/journals/{sha256}.jsonseq").read_bytes() == content
    receipt = json.loads((output / "recovery.json").read_bytes())
    assert receipt["complete"] is True
    assert receipt["archive_root_sha256"] == fixture.archive_root_sha256
    assert receipt["metadata_freshness"] == "selected-copy-heads-not-global-latest"


def test_id_layout_keeps_same_four_contents(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path
) -> None:
    archive, fixture = _copy_source(source_archive, tmp_path)
    output = tmp_path / "recovered"
    summary = recover_archive(archive, output, passphrases=_KEYS, layout_mode="id-layout")
    assert summary.layout_mode == "id-layout"
    for row in _records(output / "recovery-members.jsonseq"):
        artifact_id = str(row["artifact_id"])
        assert row["components"] == ["artifacts", artifact_id[:2], artifact_id]
        assert row["reason"] == "id-layout"
        assert output.joinpath(*row["components"]).read_bytes() == fixture.members[artifact_id]
    assert (output / "metadata/description.json").is_file()
    assert (output / "metadata/tags.jsonseq").is_file()
    assert len(list((output / "provenance/primary").rglob("*.jsonseq"))) == 3


def test_equal_hints_fall_back_for_every_conflicting_artifact(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    write_archive(
        archive,
        hints={"1" * 64: ("same.txt",), "2" * 64: ("same.txt",)},
    )
    output = tmp_path / "recovered"
    recover_archive(archive, output, passphrases=_KEYS)
    rows = {row["artifact_id"]: row for row in _records(output / "recovery-members.jsonseq")}
    for artifact_id in ("1" * 64, "2" * 64):
        assert rows[artifact_id]["reason"] == "destination-collision"
        assert rows[artifact_id]["components"] == ["artifacts", artifact_id[:2], artifact_id]
    assert not (output / "files/same.txt").exists()


@pytest.mark.parametrize("description_document,description", [(False, None), (True, None)])
def test_unset_and_absent_description_are_distinct_valid_states(
    tmp_path: Path, description_document: bool, description: str | None
) -> None:
    archive = tmp_path / "archive"
    write_archive(
        archive,
        description=description,
        description_document=description_document,
        tags=(),
    )
    output = tmp_path / "recovered"
    summary = recover_archive(archive, output, passphrases=_KEYS)
    state = json.loads((output / "metadata/description-state.json").read_bytes())
    assert state["status"] == ("unset" if description_document else "absent-in-copy")
    assert (output / "metadata/description.txt").read_bytes() == b""
    assert (output / "metadata/description.json").exists() is description_document
    assert _records(output / "metadata/tags.jsonseq")[-1]["tag_count"] == 0
    assert summary.tag_count == 0


def test_missing_required_tag_head_fails_without_completion(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path
) -> None:
    archive, _fixture = _copy_source(source_archive, tmp_path)
    (archive / "tags/head.json.age").unlink()
    output = tmp_path / "recovered"
    with pytest.raises(RecoveryError, match="tag head|missing"):
        recover_archive(archive, output, passphrases=_KEYS)
    assert not output.exists()


def test_missing_tag_node_fails_without_completion(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path
) -> None:
    archive, _fixture = _copy_source(source_archive, tmp_path)
    next((archive / "tags/nodes").rglob("*.age")).unlink()
    output = tmp_path / "recovered"
    with pytest.raises(RecoveryError, match="tag|missing"):
        recover_archive(archive, output, passphrases=_KEYS)
    assert not output.exists()


def test_metadata_head_change_during_transfer_requires_new_snapshot(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _fixture = _copy_source(source_archive, tmp_path)
    original = recovery_module._stage_mapping

    def change_head(**kwargs: object) -> tuple[int, int]:
        result = original(**kwargs)  # type: ignore[arg-type]
        head = archive / "tags/head.json.age"
        head.write_bytes(head.read_bytes() + b"changed")
        return result

    monkeypatch.setattr(recovery_module, "_stage_mapping", change_head)
    output = tmp_path / "recovered"
    with pytest.raises(RecoveryError, match="metadata changed"):
        recover_archive(archive, output, passphrases=_KEYS)
    assert not output.exists()


def test_trusted_expected_identifiers_detect_an_older_or_other_copy(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path
) -> None:
    archive, fixture = _copy_source(source_archive, tmp_path)
    for arguments in (
        {"expected_archive_root_sha256": "f" * 64},
        {"expected_description_sha256": "f" * 64},
        {"expected_tag_head_sha256": "f" * 64},
    ):
        output = tmp_path / "recovered"
        with pytest.raises(RecoveryError, match="trusted expectation"):
            recover_archive(archive, output, passphrases=_KEYS, **arguments)
        assert not output.exists()
    output = tmp_path / "recovered"
    recover_archive(
        archive,
        output,
        passphrases=_KEYS,
        expected_archive_root_sha256=fixture.archive_root_sha256,
        expected_description_sha256=fixture.description_sha256,
        expected_tag_head_sha256=fixture.tag_head_sha256,
    )


def test_existing_output_is_never_overwritten(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path
) -> None:
    archive, _fixture = _copy_source(source_archive, tmp_path)
    output = tmp_path / "recovered"
    output.mkdir()
    (output / "local-edit").write_text("keep")
    with pytest.raises(RecoveryError, match="already exists"):
        recover_archive(archive, output, passphrases=_KEYS)
    assert (output / "local-edit").read_text() == "keep"


def test_failed_write_leaves_no_complete_output_and_retry_can_finish(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive, _fixture = _copy_source(source_archive, tmp_path)
    original = recovery_module._copy_exact

    def interrupt(*args: object, **kwargs: object) -> Path:
        raise OSError("simulated disk interruption")

    monkeypatch.setattr(recovery_module, "_copy_exact", interrupt)
    output = tmp_path / "recovered"
    with pytest.raises(RecoveryError, match="simulated disk interruption"):
        recover_archive(archive, output, passphrases=_KEYS)
    assert not output.exists()
    assert not (tmp_path / ".recovered.riverhog-recovery/output/recovery.json").exists()
    monkeypatch.setattr(recovery_module, "_copy_exact", original)
    recover_archive(archive, output, passphrases=_KEYS)
    assert (output / "recovery.json").is_file()


def test_metadata_only_commands_remain_partial_and_exact(
    source_archive: tuple[Path, FixtureArchive], tmp_path: Path
) -> None:
    archive, fixture = _copy_source(source_archive, tmp_path)
    description = recover_collection_description(archive, passphrases=_KEYS)
    tags = recover_collection_tags(archive, passphrases=_KEYS)
    assert description is not None
    assert description.description == "Résumé of 東京 footage"
    assert description.archive_root_sha256 == fixture.archive_root_sha256
    assert list(tags.iter_tags()) == ["camera:七", "source:ftp"]
    assert tags.head.archive_root_sha256 == fixture.archive_root_sha256

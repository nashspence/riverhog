"""Causal source-owner witnesses; observed gaps are intentionally named as gaps.

Only the official-age subprocess adapter is substituted in the two full recovery
tests. It is replaced with the repository's authenticated age stream primitive,
not plaintext passthrough. These tests do not qualify installed isolation, child
receiving, another platform, or power-loss durability.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import a_riverhog_recovery_tool._archive_io as archive_io
import a_riverhog_recovery_tool._publication as recovery_publication
import a_riverhog_recovery_tool.recovery as recovery_owner
import pytest
from a_riverhog_cli.local import _quarantine
from a_riverhog_recovery_tool import RecoveryError, recover_archive
from materialization_experiment import (
    observe_planner,
    observe_publication_barriers,
    plan_trie_spooled,
)
from riverhog_age import iter_decrypt_age_scrypt
from riverhog_client._file_download import DownloadDestinationExists, verified_download
from riverhog_client.downloads import RetrievalDownload, download_retrieval_files
from riverhog_materialization import DestinationRules, MemberAdvice, plan_materialization
from riverhog_protocol import ArtifactId

from tests.support.qualification.recovery_archive import (
    PASSPHRASE,
    PASSPHRASE_ID,
    FixtureArchive,
    write_archive,
)


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@pytest.mark.parametrize(
    ("paths", "rules", "mode"),
    [
        (
            (("same",), ("same",), ("same", "child")),
            DestinationRules(False, True, "exact", 255, 4096),
            "declared-hints",
        ),
        (
            (("Camera", "one"), ("camera", "two"), ("safe",)),
            DestinationRules(False, False, "exact", 255, 4096),
            "declared-hints",
        ),
        (
            (("e\u0301", "one"), ("\u00e9", "two"), ("safe",)),
            DestinationRules(False, True, "NFC", 255, 4096),
            "declared-hints",
        ),
        (
            (("CON", "tail. "), ("%43ON", "safe"), None),
            DestinationRules(True, False, "NFC", 255, 4096),
            "declared-hints",
        ),
        (
            (("x" * 100, "y" * 100), ("short",), None),
            DestinationRules(False, True, "exact", 255, 160),
            "declared-hints",
        ),
        (
            (("same",), ("same",), None),
            DestinationRules(False, True, "exact", 255, 4096),
            "id-layout",
        ),
    ],
)
def test_trie_preserves_actual_all_participant_collision_semantics(
    tmp_path: Path,
    paths: tuple[tuple[str, ...] | None, ...],
    rules: DestinationRules,
    mode: Any,
) -> None:
    advice = [
        MemberAdvice(f"{number:064x}", {"components": list(path)} if path is not None else None)
        for number, path in enumerate(paths, 1)
    ]
    state = sqlite3.connect(tmp_path / "trie.sqlite3")
    try:
        actual = tuple(plan_trie_spooled(iter(advice), rules=rules, state=state, mode=mode))
        assert actual == plan_materialization(iter(advice), rules=rules, mode=mode)
    finally:
        state.close()


def test_trie_rejects_duplicate_exact_member(tmp_path: Path) -> None:
    member = MemberAdvice("1" * 64, {"components": ["one"]})
    state = sqlite3.connect(tmp_path / "trie.sqlite3")
    try:
        with pytest.raises(ValueError, match="duplicate member"):
            tuple(
                plan_trie_spooled(
                    [member, member],
                    rules=DestinationRules(False, True, "exact", 255, 4096),
                    state=state,
                )
            )
    finally:
        state.close()


def test_actual_prefix_encoding_repeats_bounded_ancestry(tmp_path: Path) -> None:
    current = [
        observe_planner(tmp_path / f"current-{depth}", members=4, depth=depth, trie=False)
        for depth in (8, 16, 32)
    ]
    proposed = [
        observe_planner(tmp_path / f"trie-{depth}", members=4, depth=depth, trie=True)
        for depth in (8, 16, 32)
    ]
    assert [row.result_sha256 for row in current] == [row.result_sha256 for row in proposed]
    assert current[-1].input_hint_bytes < 2 * current[-2].input_hint_bytes
    assert current[-1].index_key_spelling_bytes > 3 * current[-2].index_key_spelling_bytes
    assert proposed[-1].index_key_spelling_bytes < 2 * proposed[-2].index_key_spelling_bytes
    assert proposed[-1].index_rows <= proposed[-1].total_hint_components + 1


def test_actual_download_refuses_destination_created_during_transfer(tmp_path: Path) -> None:
    output = tmp_path / "output"
    payload = b"original requested bytes"

    def chunks() -> Iterator[bytes]:
        yield payload[:4]
        output.write_bytes(b"operator edit")
        yield payload[4:]

    with pytest.raises(DownloadDestinationExists):
        verified_download(
            chunks(), output=output, expected_bytes=len(payload), expected_sha256=_digest(payload)
        )
    assert output.read_bytes() == b"operator edit"
    assert not list(tmp_path.glob(".*.part"))


def test_actual_download_lost_link_reply_leaves_exact_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "output"
    payload = b"requested exact bytes"
    original = os.link

    def lost_reply(source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        original(source, destination, *args, **kwargs)
        raise OSError("injected lost link reply")

    monkeypatch.setattr(os, "link", lost_reply)
    with pytest.raises(OSError, match="lost link reply"):
        verified_download(
            (payload,), output=output, expected_bytes=len(payload), expected_sha256=_digest(payload)
        )
    assert output.read_bytes() == payload
    monkeypatch.setattr(os, "link", original)
    with pytest.raises(DownloadDestinationExists):
        verified_download(
            (payload,), output=output, expected_bytes=len(payload), expected_sha256=_digest(payload)
        )
    assert output.read_bytes() == payload


@pytest.mark.skipif(os.name == "nt", reason="Linux/macOS directory fsync observation")
def test_actual_sdk_and_cli_have_distinct_directory_sync_boundaries(tmp_path: Path) -> None:
    observation = observe_publication_barriers(tmp_path / "observation")
    assert observation["sdk_sync_targets"] == ["file"]
    assert observation["cli_sync_targets"] == ["directory"]


def test_actual_recovery_parent_sync_failure_preserves_published_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "recovery.json").write_bytes(b"complete synthetic phase witness")
    output = tmp_path / "output"
    original = recovery_publication._sync_directory

    def fail_parent(path: Path) -> None:
        if path == tmp_path:
            raise OSError("injected parent sync failure")
        original(path)

    monkeypatch.setattr(recovery_publication, "_sync_directory", fail_parent)
    with pytest.raises(OSError, match="parent sync failure"):
        recovery_publication.publish_complete_recovery(staging, output)
    assert not staging.exists()
    assert (output / "recovery.json").read_bytes() == b"complete synthetic phase witness"


def test_actual_recovery_fixed_scratch_deletes_unowned_staging_before_input_validation(
    tmp_path: Path,
) -> None:
    output = tmp_path / "recovered"
    scratch = tmp_path / ".recovered.riverhog-recovery"
    staging = scratch / "output"
    staging.mkdir(parents=True)
    sentinel = staging / "unowned"
    sentinel.write_bytes(b"unowned operator data")
    with pytest.raises(RecoveryError, match="does not exist"):
        recover_archive(tmp_path / "absent-archive", output, passphrases={})
    # This asserts the existing gap; proposed ownership must instead refuse it.
    assert not sentinel.exists()
    assert not output.exists()


def test_actual_quarantine_can_remove_concurrent_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "edited"
    output.write_bytes(b"first operator edit")
    replacement = tmp_path / "next-edit"
    replacement.write_bytes(b"later operator edit")
    original = os.link

    def replace_after_pin(source: Any, destination: Any, *args: Any, **kwargs: Any) -> None:
        original(source, destination, *args, **kwargs)
        os.replace(replacement, output)

    monkeypatch.setattr(os, "link", replace_after_pin)
    retained = _quarantine(tmp_path, output, collection_id=1)
    assert retained.read_bytes() == b"first operator edit"
    assert not output.exists()
    # The later file was not captured by the link and the current source unlinks
    # it. Integration must invert this witness using atomic move into quarantine.
    assert all(
        path.read_bytes() != b"later operator edit"
        for path in tmp_path.rglob("*")
        if path.is_file()
    )


@pytest.fixture(scope="module")
def encrypted_archive(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, FixtureArchive]:
    path = tmp_path_factory.mktemp("encrypted-materialization") / "archive"
    return path, write_archive(path, late_shared_history=True, tags=("reference:999",))


@pytest.fixture
def authenticated_test_decrypt(monkeypatch: pytest.MonkeyPatch) -> None:
    def decrypt(source: Path, destination: Path, *, passphrase: str, command: str) -> None:
        del command
        with source.open("rb") as encrypted, destination.open("wb") as output:
            chunks = iter(lambda: encrypted.read(1024 * 1024), b"")
            for chunk in iter_decrypt_age_scrypt(chunks, passphrase):
                output.write(chunk)

    monkeypatch.setattr(archive_io, "age_decrypt", decrypt)


def test_actual_complete_recovery_lost_reply_retains_all_selected_contents(
    tmp_path: Path,
    encrypted_archive: tuple[Path, FixtureArchive],
    authenticated_test_decrypt: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive, fixture = encrypted_archive
    output = tmp_path / "recovered"
    original = recovery_publication.publish_complete_recovery

    def lost_reply(staging: Path, destination: Path) -> None:
        original(staging, destination)
        raise OSError("injected lost complete-publication reply")

    monkeypatch.setattr(recovery_owner, "publish_complete_recovery", lost_reply)
    with pytest.raises(RecoveryError, match="lost complete-publication reply"):
        recover_archive(archive, output, passphrases={PASSPHRASE_ID: PASSPHRASE})
    receipt = json.loads((output / "recovery.json").read_bytes())
    assert receipt["complete"] is True
    assert receipt["archive_root_sha256"] == fixture.archive_root_sha256
    assert receipt["mapping_sha256"] == _digest((output / "recovery-members.jsonseq").read_bytes())
    records = [
        json.loads(line) for line in (output / "recovery-members.jsonseq").read_bytes().splitlines()
    ]
    assert len(records) == len(fixture.members)
    for row in records:
        assert (
            output.joinpath(*row["components"]).read_bytes() == fixture.members[row["artifact_id"]]
        )
        assert output.joinpath(*row["sidecar"]).is_file()
        assert output.joinpath(*row["history"]).is_file()
    for relative, raw in fixture.history_objects.items():
        assert (output / "structure" / relative.removesuffix(".age")).read_bytes() == raw
    assert (output / "metadata/description.json").is_file()
    assert (output / "metadata/tags/head.json").is_file()
    with pytest.raises(RecoveryError, match="already exists"):
        recover_archive(archive, output, passphrases={PASSPHRASE_ID: PASSPHRASE})


def test_actual_mapping_failure_loses_constructed_sqlite_close_owner(
    tmp_path: Path,
    encrypted_archive: tuple[Path, FixtureArchive],
    authenticated_test_decrypt: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection(sqlite3.Connection):
        received = False

        def close(self) -> None:
            super().close()
            self.received = True

    originals: list[Connection] = []
    real_connect = sqlite3.connect

    def connect(database: Any, *args: Any, **kwargs: Any) -> sqlite3.Connection:
        if Path(str(database)).name == "selected.sqlite3":
            result = real_connect(database, *args, factory=Connection, **kwargs)
            assert isinstance(result, Connection)
            originals.append(result)
            return result
        return cast(sqlite3.Connection, real_connect(database, *args, **kwargs))

    def fail_copy(*args: Any, **kwargs: Any) -> Path:
        raise OSError("injected exact-copy failure")

    monkeypatch.setattr(sqlite3, "connect", connect)
    monkeypatch.setattr(recovery_owner, "_copy_exact", fail_copy)
    try:
        with pytest.raises(RecoveryError, match="exact-copy failure"):
            recover_archive(
                encrypted_archive[0],
                tmp_path / "recovered",
                passphrases={PASSPHRASE_ID: PASSPHRASE},
            )
        assert len(originals) == 1
        assert originals[0].received is False
        assert originals[0].execute("SELECT COUNT(*) FROM selected").fetchone()[0] == len(
            encrypted_archive[1].members
        )
        assert not (tmp_path / "recovered").exists()
    finally:
        # Test instrumentation retains and positively closes the otherwise lost
        # original connection. No leaked resource is left by the witness.
        for connection in originals:
            connection.close()
            assert connection.received


def test_actual_download_pool_stops_receiving_after_first_client_close_failure(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"exact download")
    barrier = threading.Barrier(2)
    lock = threading.Lock()
    originals: list[Any] = []
    completed: list[Path] = []

    class Client:
        def __init__(self) -> None:
            self.source = source.open("rb")
            self.close_calls = 0

        def download_retrieval_artifact(
            self,
            job_id: str,
            *,
            collection_id: Any,
            artifact_id: Any,
            output: Path,
            expected_bytes: int,
            expected_sha256: str,
        ) -> int:
            barrier.wait(timeout=10)
            receipt = verified_download(
                (self.source.read(),),
                output=output,
                expected_bytes=expected_bytes,
                expected_sha256=expected_sha256,
            )
            with lock:
                completed.append(output)
            return receipt.bytes

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise OSError("injected known pre-close failure")
            self.source.close()

    def create() -> Client:
        client = Client()
        with lock:
            originals.append(client)
        return client

    downloads = [
        RetrievalDownload(
            1,
            ArtifactId(f"{number:064x}"),
            tmp_path / f"output-{number}",
            len(b"exact download"),
            _digest(b"exact download"),
        )
        for number in (1, 2)
    ]
    primary = Client()
    try:
        with pytest.raises(OSError, match="pre-close failure"):
            download_retrieval_files(
                primary, "exact-job", downloads, concurrency=2, window=2, client_factory=create
            )
        assert len(completed) == 2  # Executor did receive both original workers.
        assert len(originals) == 2
        assert sorted(client.close_calls for client in originals) == [0, 1]
        assert all(not client.source.closed for client in originals)
    finally:
        primary.source.close()
        for client in originals:
            try:
                client.close()
            except OSError:
                client.close()  # Injected failure is explicitly before close.
            assert client.source.closed

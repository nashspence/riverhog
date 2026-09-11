from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import riverhog_recover.recovery as recovery_module
from riverhog_age import encrypt_age_scrypt
from riverhog_archive_contracts import format_archive_sequence
from riverhog_protocol import (
    COLLECTION_DESCRIPTION_RELATIVE_PATH,
    COLLECTION_TAG_HEAD_RELATIVE_PATH,
    CollectionTagHeadDocument,
)
from riverhog_provenance import (
    ProvenanceRootDocument,
    create_derivative_journal_from_identity,
    create_observation_journal,
    prepare_file_provenance,
    validate_journal,
)
from riverhog_recover import (
    RecoveryError,
    recover_archive,
    recover_collection_description,
    recover_collection_tags,
)

from tests.provenance_observer import native_provenance_observer
from tests.support.qualification.recovery_archive import (
    PASSPHRASE,
    PASSPHRASE_ID,
    write_archive,
)

OFFICIAL_AGE = shutil.which("age")
OFFICIAL_BATCHPASS = shutil.which("age-plugin-batchpass")

pytestmark = pytest.mark.skipif(
    OFFICIAL_AGE is None or OFFICIAL_BATCHPASS is None,
    reason="official age and age-plugin-batchpass are required",
)


def test_recovers_complete_collection_without_server_or_database(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    expected, _journal = write_archive(archive)
    output = tmp_path / "recovered"

    summary = recover_archive(
        archive,
        output,
        passphrases={PASSPHRASE_ID: PASSPHRASE},
    )

    assert summary.files == len(expected)
    assert summary.bytes == sum(len(content) for content in expected.values())
    assert summary.volumes == 3
    assert {path: (output / path).read_bytes() for path in expected} == expected


def test_recovers_description_without_reading_collection_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "archive"
    write_archive(archive, description="Résumé of 東京 footage")
    decrypted: list[str] = []
    original_decrypt = recovery_module._age_decrypt

    def decrypt(source: Path, *args: object, **kwargs: object) -> None:
        decrypted.append(source.relative_to(archive).as_posix())
        original_decrypt(source, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(recovery_module, "_age_decrypt", decrypt)
    document = recover_collection_description(
        archive,
        passphrases={PASSPHRASE_ID: PASSPHRASE},
    )

    assert document is not None
    assert document.description == "Résumé of 東京 footage"
    assert len(document.archive_root_sha256) == 64
    assert decrypted == ["manifest.json.age", COLLECTION_DESCRIPTION_RELATIVE_PATH]


def test_missing_description_is_an_explicitly_absent_optional_sidecar(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    write_archive(archive)

    assert (
        recover_collection_description(
            archive,
            passphrases={PASSPHRASE_ID: PASSPHRASE},
        )
        is None
    )


def test_recovers_exact_tags_without_reading_collection_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "archive"
    expected = ("camera:七", "source:ftp", "z" * 65_536)
    write_archive(archive, tags=expected)
    decrypted: list[str] = []
    original_decrypt = recovery_module._age_decrypt

    def decrypt(source: Path, *args: object, **kwargs: object) -> None:
        decrypted.append(source.relative_to(archive).as_posix())
        original_decrypt(source, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(recovery_module, "_age_decrypt", decrypt)
    recovered = recover_collection_tags(
        archive,
        passphrases={PASSPHRASE_ID: PASSPHRASE},
    )

    assert set(recovered.iter_tags()) == set(expected)
    assert recovered.head.revision == 1
    assert decrypted[0:2] == ["manifest.json.age", COLLECTION_TAG_HEAD_RELATIVE_PATH]
    assert all(path.startswith("tags/nodes/") for path in decrypted[2:])
    assert not any(path.startswith("volumes/") for path in decrypted)


def test_tag_recovery_rejects_a_head_for_another_archive(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    write_archive(archive, tags=("source:ftp",))
    wrong = CollectionTagHeadDocument.seal(
        archive_root_sha256="f" * 64,
        revision=1,
        root_sha256=None,
    )
    (archive / COLLECTION_TAG_HEAD_RELATIVE_PATH).write_bytes(
        encrypt_age_scrypt(wrong.to_json_bytes(), PASSPHRASE, log_n=1)
    )

    with pytest.raises(RecoveryError, match="another archive root"):
        recover_collection_tags(
            archive,
            passphrases={PASSPHRASE_ID: PASSPHRASE},
        )


def test_tag_recovery_rejects_a_missing_authenticated_node(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    write_archive(archive, tags=("source:ftp",))
    node = next((archive / "tags/nodes").glob("*/*.age"))
    node.unlink()
    recovered = recover_collection_tags(
        archive,
        passphrases={PASSPHRASE_ID: PASSPHRASE},
    )

    with pytest.raises(RecoveryError, match="archive file is missing"):
        tuple(recovered.iter_tags())


def test_recovery_selects_exact_key_generations_without_trial_decryption(tmp_path: Path) -> None:
    second_id = "recovery-test-key-v2"
    second_passphrase = "second independent archive secret"
    first_archive = tmp_path / "archive-one"
    second_archive = tmp_path / "archive-two"
    expected, _journal = write_archive(first_archive)
    write_archive(
        second_archive,
        passphrase=second_passphrase,
        passphrase_id=second_id,
    )
    passphrases = {
        PASSPHRASE_ID: PASSPHRASE,
        second_id: second_passphrase,
    }

    for archive, output in (
        (first_archive, tmp_path / "recovered-one"),
        (second_archive, tmp_path / "recovered-two"),
    ):
        recover_archive(
            archive,
            output,
            passphrases=passphrases,
        )
        assert {path: (output / path).read_bytes() for path in expected} == expected

    with pytest.raises(RecoveryError, match=second_id):
        recover_archive(
            second_archive,
            tmp_path / "missing-key-output",
            passphrases={PASSPHRASE_ID: PASSPHRASE},
        )


def test_cli_recovers_with_permission_restricted_passphrase_file(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    expected, _journal = write_archive(archive)
    output = tmp_path / "recovered"
    passphrases_file = tmp_path / "passphrases.json"
    passphrases_file.write_text(
        f'{{"{PASSPHRASE_ID}":"{PASSPHRASE}"}}',
        encoding="utf-8",
    )
    passphrases_file.chmod(0o600)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "riverhog_recover.cli",
            str(archive),
            str(output),
            "--passphrases-file",
            str(passphrases_file),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Recovered 3 files" in completed.stdout
    assert {path: (output / path).read_bytes() for path in expected} == expected


def test_windows_recovery_uses_batchpass_environment_without_unix_fds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.age"
    source.write_bytes(b"ciphertext")
    destination = tmp_path / "destination"

    def run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        env: dict[str, str],
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        assert command == [
            "age",
            "--decrypt",
            "-j",
            "batchpass",
            "-o",
            str(destination),
            str(source),
        ]
        assert check is False
        assert capture_output is True
        assert text is True
        assert env["AGE_PASSPHRASE"] == PASSPHRASE
        assert "AGE_PASSPHRASE_FD" not in env
        assert pass_fds == ()
        destination.write_bytes(b"plaintext")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(recovery_module, "_windows_host", lambda: True)
    monkeypatch.setattr(recovery_module.subprocess, "run", run)

    recovery_module._age_decrypt(
        source,
        destination,
        passphrase=PASSPHRASE,
        command="age",
    )

    assert destination.read_bytes() == b"plaintext"


def test_ciphertext_corruption_fails_without_publishing_partial_output(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "archive"
    write_archive(archive)
    damaged = archive / f"volumes/segment-{format_archive_sequence(1)}.bin.age"
    damaged.write_bytes(damaged.read_bytes() + b"damage")
    output = tmp_path / "recovered"
    with pytest.raises(RecoveryError, match="stored volume byte count mismatch"):
        recover_archive(
            archive,
            output,
            passphrases={PASSPHRASE_ID: PASSPHRASE},
        )

    assert not output.exists()


def test_recovery_resumes_after_last_durable_volume_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "archive"
    expected, _journal = write_archive(archive)
    damaged = archive / f"volumes/segment-{format_archive_sequence(1)}.bin.age"
    original = damaged.read_bytes()
    damaged.write_bytes(original + b"damage")
    output = tmp_path / "recovered"
    with pytest.raises(RecoveryError, match="stored volume byte count mismatch"):
        recover_archive(
            archive,
            output,
            passphrases={PASSPHRASE_ID: PASSPHRASE},
        )

    checkpoint = tmp_path / ".recovered.riverhog-recovery" / "state.sqlite3"
    assert checkpoint.is_file()
    damaged.write_bytes(original)
    original_recover_pack = recovery_module._recover_pack
    pack_calls = 0

    def recover_pack(*args: object, **kwargs: object) -> object:
        nonlocal pack_calls
        pack_calls += 1
        return original_recover_pack(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(recovery_module, "_recover_pack", recover_pack)
    recover_archive(
        archive,
        output,
        passphrases={PASSPHRASE_ID: PASSPHRASE},
    )

    assert pack_calls == 0
    assert not checkpoint.parent.exists()
    assert {path: (output / path).read_bytes() for path in expected} == expected


def test_recovery_descriptor_rejects_changed_root_before_decryption(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    write_archive(archive)
    root = archive / "manifest.json.age"
    root.write_bytes(root.read_bytes() + b"changed")

    with pytest.raises(RecoveryError, match="does not match recovery descriptor"):
        recover_archive(
            archive,
            tmp_path / "output",
            passphrases={PASSPHRASE_ID: PASSPHRASE},
        )


def test_client_transform_riverhog_recovery_restores_exact_derivative_history(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source-alpha.txt"
    source.write_bytes(b"original alpha\n")
    client_journal = create_observation_journal(
        source,
        relative_path="camera/alpha.txt",
        host_id="urn:uuid:00000000-0000-4000-8000-000000000001",
        agent_name="riverhog-client",
        agent_version="1.0.0",
        observer=native_provenance_observer(),
    )
    transformed = b"alpha\n"
    target_journal = create_derivative_journal_from_identity(
        relative_path="notes/alpha.txt",
        byte_count=len(transformed),
        sha256=hashlib.sha256(transformed).hexdigest(),
        source_journals=(client_journal,),
        agent_name="target-server",
        agent_version="1.0.0",
        event_label="Target canonical archive transformation",
        started_at="2026-08-10T01:00:00Z",
        ended_at="2026-08-10T01:01:00Z",
    )
    client_summary = validate_journal(client_journal)
    target_summary = validate_journal(target_journal)
    journals = {
        client_summary.journal_id: client_journal,
        target_summary.journal_id: target_journal,
    }

    archive = tmp_path / "archive"
    expected, exact_journal = write_archive(
        archive,
        with_provenance=True,
        provenance_journal=target_journal,
        provenance_journals=journals,
    )
    assert exact_journal is not None
    exact_summary = validate_journal(exact_journal)
    assert exact_summary.primary_lineage_id != client_summary.primary_lineage_id
    assert {item.journal_id for item in exact_summary.external_states} == {
        client_summary.journal_id
    }
    output = tmp_path / "recovered"

    summary = recover_archive(
        archive,
        output,
        passphrases={PASSPHRASE_ID: PASSPHRASE},
    )

    provenance_root = output / ".riverhog" / "provenance"
    restored_root = ProvenanceRootDocument.from_json_bytes(
        (provenance_root / "root.json").read_bytes()
    )
    restored_journals = {
        journal_id: (provenance_root / "journals" / f"{journal_id}.json-seq").read_bytes()
        for journal_id in journals
    }
    assert summary.provenance_mode == "mixed"
    assert summary.provenance_journals == 2
    assert restored_journals == journals
    assert restored_root.ordered_volume_sha256
    assert len(list((provenance_root / "metadata").glob("*.json"))) == (
        len(list((provenance_root / "payloads").glob("*.bin"))) + 1
    )
    prepared = prepare_file_provenance(
        output / "notes" / "alpha.txt",
        relative_path="notes/alpha.txt",
        host_id="urn:uuid:00000000-0000-4000-8000-000000000002",
        agent_name="riverhog-client",
        agent_version="1.0.0",
        observer=native_provenance_observer(),
        provenance=provenance_root,
    )
    assert prepared.journals == journals
    assert {path: (output / path).read_bytes() for path in expected} == expected

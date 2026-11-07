from __future__ import annotations

from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_journal_registration as owner
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_journal_source import DiscoveryJournalSource
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata
from riverhog_core.journal_validation_workspace import lease_journal_validation_workspace
from riverhog_provenance import JournalValidationSession
from riverhog_provenance_contracts import ContractCatalog

from tests.unit.test_canonical_discovery_journal_source import source_case

DIRECTORY = "a" * 32


def register(case, work, binding):
    with case.metadata.transaction() as (db, key):
        return owner.register_discovery_journal(db, key, work, binding)


def read(case, work):
    with case.metadata.read() as (db, key):
        return owner.find_discovery_journal(db, key, work)


def test_exact_registration_precedes_native_storage_and_preserves_retries(tmp_path):
    with source_case(tmp_path / "registration", b"\x1e{}\n") as (case, work, source):
        binding = replace(source.binding, directory_id=DIRECTORY)
        assert read(case, work) is None
        assert not (case.metadata.custody.directory / "journals").exists()
        selected = register(case, work, binding)
        assert selected == owner.DiscoveryJournalRegistration(binding, False)
        assert read(case, work) == selected
        assert register(case, work, binding) == selected
        assert not (case.metadata.custody.directory / "journals").exists()
        with pytest.raises(StaleArchiveTask, match="cannot be replaced"):
            register(case, work, replace(binding, directory_id="b" * 32))
        assert read(case, work) == selected


def test_registration_and_confirmation_obey_actual_owner_transaction_rollback(tmp_path):
    with source_case(tmp_path / "rollback", b"\x1e{}\n") as (case, work, source):
        binding = replace(source.binding, directory_id=DIRECTORY)
        with pytest.raises(InterruptedError):
            with case.metadata.transaction() as (db, key):
                owner.register_discovery_journal(db, key, work, binding)
                raise InterruptedError("registration confirmation lost before commit")
        assert read(case, work) is None
        selected = register(case, work, binding)
        with pytest.raises(InterruptedError):
            with case.metadata.transaction() as (db, key):
                assert owner.confirm_discovery_journal(db, key, work, selected)
                raise InterruptedError("opening confirmation lost before commit")
        assert read(case, work) == selected
        with case.metadata.transaction() as (db, key):
            assert owner.confirm_discovery_journal(db, key, work, selected)
        confirmed = replace(selected, confirmed=True)
        assert read(case, work) == confirmed
        with case.metadata.transaction() as (db, key):
            assert owner.confirm_discovery_journal(db, key, work, selected)
        assert register(case, work, binding) == confirmed


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("attempt_id", "other"),
        ("directory_id", "../outside"),
        ("journal_id", "urn:uuid:22222222-2222-4222-8222-222222222222"),
        ("source_bytes", "123456789"),
        ("source_sha256", "0" * 64),
        ("source_version", "123456789"),
        ("frame_count", "123456789"),
        ("catalog_sha256", "0" * 64),
        ("require_profiles", True),
    ],
)
def test_registration_rejects_other_original_inputs_without_creating_rows(tmp_path, field, value):
    with source_case(tmp_path / "binding", b"\x1e{}\n") as (case, work, source):
        binding = replace(source.binding, directory_id=DIRECTORY)
        with pytest.raises(StaleArchiveTask):
            register(case, work, replace(binding, **{field: value}))
        assert read(case, work) is None
        assert register(case, work, binding).binding == binding


@pytest.mark.parametrize(
    ("field", "value"),
    [("worker_id", "other"), ("directory_id", "b" * 32), ("binding_json", "{}")],
)
def test_existing_invalid_registration_is_preserved_and_never_reset(tmp_path, field, value):
    with source_case(tmp_path / "damaged", b"\x1e{}\n") as (case, work, source):
        binding = replace(source.binding, directory_id=DIRECTORY)
        selected = register(case, work, binding)
        with case.metadata.transaction() as (db, _key):
            db.execute(f"UPDATE discovery_journal_children SET {field}=?", (value,)).close()
        before = (case.metadata.custody.directory / "discovery.sqlite3").read_bytes()
        with pytest.raises(StaleArchiveTask):
            read(case, work)
        with pytest.raises(StaleArchiveTask):
            register(case, work, binding)
        with pytest.raises(StaleArchiveTask):
            with case.metadata.transaction() as (db, key):
                owner.confirm_discovery_journal(db, key, work, selected)
        assert (case.metadata.custody.directory / "discovery.sqlite3").read_bytes() == before


def test_mutation_requires_transaction_and_confirmation_cannot_select_another_directory(tmp_path):
    with source_case(tmp_path / "transaction", b"\x1e{}\n") as (case, work, source):
        binding = replace(source.binding, directory_id=DIRECTORY)
        with case.metadata.read() as (db, key):
            with pytest.raises(ValueError, match="owner-managed"):
                owner.register_discovery_journal(db, key, work, binding)
        selected = register(case, work, binding)
        wrong = replace(selected, binding=replace(binding, directory_id="b" * 32))
        with case.metadata.transaction() as (db, key):
            assert not owner.confirm_discovery_journal(db, key, work, wrong)
        assert read(case, work) == selected
        with case.metadata.read() as (db, key):
            with pytest.raises(ValueError, match="owner-managed"):
                owner.confirm_discovery_journal(db, key, work, selected)


def test_actual_native_open_confirmation_does_not_grant_semantic_acceptance(tmp_path):
    catalog = ContractCatalog()
    with source_case(tmp_path / "native", b"\x1e{}\n", catalog=catalog) as (case, work, original):
        binding = replace(original.binding, directory_id=DIRECTORY)
        selected = register(case, work, binding)
        source = DiscoveryJournalSource(work, directory_id=selected.binding.directory_id)
        root = case.metadata.custody.directory / "journals"
        with lease_journal_validation_workspace(root, DIRECTORY, create=True) as directory:
            assert directory is not None and read(case, work) == selected
            with JournalValidationSession.open_retained(
                directory,
                source,
                attempt_id=binding.attempt_id,
                worker_epoch=case.metadata.job.worker_epoch,
                create=True,
                catalog=catalog,
            ) as native:
                with case.metadata.transaction() as (db, key):
                    assert owner.confirm_discovery_journal(db, key, work, selected)
                assert read(case, work).confirmed
                progress = native.progress()
                assert progress.accepted_frames == 0 and not progress.complete
        assert (directory / "snapshot.sqlite3").is_file()


def test_actual_worker_handoff_preserves_registered_identity_and_open_confirmation(tmp_path):
    with source_case(tmp_path / "handoff", b"\x1e{}\n", segments=2) as (case, work, source):
        binding = replace(source.binding, directory_id=DIRECTORY)
        selected = register(case, work, binding)
        with case.metadata.transaction() as (db, key):
            assert owner.confirm_discovery_journal(db, key, work, selected)
        expected = replace(selected, confirmed=True)
        previous = case.metadata
        previous.close()
        claimed = jobs.claim_discovery_worker(case.factory, previous.job)
        assert claimed is not None and claimed.worker_epoch > previous.job.worker_epoch
        current = DiscoveryMetadata(case.factory, claimed, case.admission, previous.custody)
        case.metadata = current
        try:
            current.open()
            resumed = framing.ArchiveJournalFramingWork(
                ArchiveJournalSource(current, work.source.reader, work.source.identity.journal_id)
            )
            assert read(case, resumed) == expected
            assert register(case, resumed, binding) == expected
            with pytest.raises(StaleArchiveTask):
                with previous.read() as (db, key):
                    owner.find_discovery_journal(db, key, work)
        finally:
            current.close()
            case.metadata = previous


def test_a_child_directory_cannot_be_shared_by_unrelated_original_journals(tmp_path):
    with source_case(tmp_path / "collision", b"\x1e{}\n") as (case, work, source):
        binding = replace(source.binding, directory_id=DIRECTORY)
        other = replace(binding, journal_id="urn:uuid:22222222-2222-4222-8222-222222222222")
        with case.metadata.transaction() as (db, key):
            db.execute(
                "INSERT INTO discovery_journal_children VALUES(?,?,?,?,?,?,1)",
                (
                    key.build_id,
                    key.admission_sha256,
                    other.journal_id,
                    key.worker_id,
                    other.directory_id,
                    other.to_json(),
                ),
            ).close()
        with pytest.raises(StaleArchiveTask, match="another original journal"):
            register(case, work, binding)
        assert read(case, work) is None
        with case.metadata.read() as (db, _key):
            assert db.execute(
                "SELECT journal_id,directory_id,binding_json,confirmed "
                "FROM discovery_journal_children"
            ).fetchall() == [(other.journal_id, DIRECTORY, other.to_json(), 1)]


def test_framing_is_frozen_before_directory_registration(tmp_path):
    from tests.unit.test_canonical_discovery_archive_framing import begin, framing_case

    with framing_case(tmp_path / "scanning", b"\x1e{}\n") as (case, work):
        begin(case, work)
        with case.metadata.read() as (db, key):
            with pytest.raises(StaleArchiveTask, match="frozen original framing"):
                owner.find_discovery_journal(db, key, work)


def test_deferred_original_framing_can_register_without_semantic_acceptance(tmp_path):
    with source_case(tmp_path / "deferred", b"unframed original") as (case, work, source):
        assert source._progress.phase == "deferred"
        binding = replace(source.binding, directory_id=DIRECTORY)
        selected = register(case, work, binding)
        assert read(case, work) == selected and not selected.confirmed
        assert selected.binding.frame_count == "0"
        assert selected.binding.source_bytes == str(len(b"unframed original"))

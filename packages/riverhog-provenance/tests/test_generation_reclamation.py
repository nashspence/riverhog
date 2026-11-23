"""Generation-only physical disposal preserves its live semantic workspace."""

from __future__ import annotations

import errno
import os

import pytest
import workspace_reclamation as files_owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import journal_reclamation as owner
from riverhog_provenance import received_generation_reclamation as receive
from riverhog_provenance import reclaim_retired_generation_storage as reclaim
from riverhog_provenance.errors import JournalValidationCapacity
from riverhog_provenance.journal_store import InvalidJournalWorkspace

from scripts.validation_numeric import numeric_policy_from_environment
from tests.support.retained_resource import fault_native as fault_native
from tests.support.workspace_reclamation import (
    execution_sample,
    observe_file_delegate,
    observe_reclamation,
)
from tests.support.workspace_reclamation import retained_file_receivers as retained_file_receivers

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX generation file custody")

NAMES = tuple("generations.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))


def private_file(directory, name, extent):
    path = directory / name
    path.write_bytes(b"x" * extent)
    path.chmod(0o600)
    return path


def private_directory(path):
    path.mkdir(mode=0o700)
    identity = path.stat()
    return identity.st_dev, identity.st_ino


@pytest.mark.parametrize("extent", [0, 17, 65537])
def test_whole_disposal_visits_only_the_four_original_generation_names(
    tmp_path, retained_file_receivers, extent
):
    directory = tmp_path / "workspace"
    identity = private_directory(directory)
    kept = ("snapshot.sqlite3", "snapshot.sqlite3-journal", "corpus.sqlite3", ".lease", "other")
    for name in kept:
        private_file(directory, name, 19)
    for name in NAMES:
        private_file(directory, name, extent)
    calls = removed = unlinked = 0
    while True:
        original = retained_file_receivers()
        progress = reclaim(
            directory, receiver=original, expected_directory_identity=identity, octet_budget=4096
        )
        assert original.received and progress.removed_octets <= 4096
        assert progress.removed_files <= 1
        calls += 1
        removed += progress.removed_octets
        unlinked += progress.removed_files
        if progress.files_reclaimed:
            break
    assert calls == 4 * max(1, (extent + 4095) // 4096)
    assert removed == 4 * extent and unlinked == 4
    assert directory.is_dir() and all(not (directory / name).exists() for name in NAMES)
    assert all((directory / name).read_bytes() == b"x" * 19 for name in kept)


@pytest.mark.parametrize("identity", [None, (1,), (True, 1), (-1, 1)])
def test_original_directory_identity_is_mandatory_before_file_effects(
    tmp_path, retained_file_receivers, identity
):
    directory = tmp_path / "workspace"
    private_directory(directory)
    data = private_file(directory, NAMES[-1], 17)
    original = retained_file_receivers()
    with pytest.raises(ValueError, match="identity"):
        reclaim(directory, receiver=original, expected_directory_identity=identity, octet_budget=1)
    assert data.read_bytes() == b"x" * 17
    assert original._directory.metrics["open_calls"] == 0


def test_replaced_directory_is_refused_before_any_named_file_probe(
    tmp_path, monkeypatch, retained_file_receivers
):
    directory = tmp_path / "workspace"
    identity = private_directory(directory)
    old = private_file(directory, NAMES[-1], 17)
    directory.rename(tmp_path / "original")
    private_directory(directory)
    replacement = private_file(directory, NAMES[-1], 29)

    def forbidden(*_args):
        pytest.fail("replacement reached the Native filename namespace")

    original = retained_file_receivers()
    with monkeypatch.context() as patch:
        patch.setattr(files_owner, "_files", forbidden)
        with pytest.raises(InvalidJournalWorkspace, match="replaced"):
            reclaim(
                directory, receiver=original, expected_directory_identity=identity, octet_budget=1
            )
    assert not original.received
    original.close()
    assert original.received and replacement.read_bytes() == b"x" * 29
    assert (tmp_path / "original" / old.name).read_bytes() == b"x" * 17


@pytest.mark.parametrize("extent", [17, 65537], ids=["unlink", "truncate"])
@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
def test_uncertain_mutation_keeps_the_same_receiver_and_never_repeats_on_close(
    tmp_path, monkeypatch, retained_file_receivers, extent, code
):
    directory = tmp_path / "workspace"
    identity = private_directory(directory)
    data = private_file(directory, NAMES[-1], extent)
    semantic = private_file(directory, "snapshot.sqlite3", 29)
    original = retained_file_receivers()

    def failed(_descriptor):
        raise OSError(code, "injected synchronization capacity")

    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", failed)
        with pytest.raises(JournalValidationCapacity):
            reclaim(
                directory,
                receiver=original,
                expected_directory_identity=identity,
                octet_budget=4096,
            )
    assert not original.received
    assert original._directory_borrow.active and original._file_borrow.active
    remaining = max(0, extent - 4096)
    assert (data.stat().st_size if data.exists() else 0) == remaining
    original.close()
    original.close()
    assert original.received and original.receipt is None
    assert (data.stat().st_size if data.exists() else 0) == remaining
    assert semantic.read_bytes() == b"x" * 29
    with pytest.raises(RuntimeError, match="fresh retained owner"):
        reclaim(
            directory, receiver=original, expected_directory_identity=identity, octet_budget=4096
        )
    next_original = retained_file_receivers()
    progress = reclaim(
        directory, receiver=next_original, expected_directory_identity=identity, octet_budget=4096
    )
    assert next_original.received and progress.remaining_octets == max(0, remaining - 4096)


@pytest.mark.parametrize("slot", ["_directory", "_file"])
@pytest.mark.parametrize("fault", ["before_close", "after_close", "signal_after_close"])
def test_native_close_refusal_or_lost_reply_keeps_the_original_attempt(
    tmp_path, monkeypatch, fault_native, slot, fault
):
    native, _path = fault_native
    monkeypatch.setattr(files_owner, "Resource", native.Resource)
    monkeypatch.setattr(files_owner, "Borrow", native.Borrow)
    directory = tmp_path / "workspace"
    identity = private_directory(directory)
    data = private_file(directory, NAMES[-1], 17)
    original = files_owner.FileReclamationOwner()
    getattr(original, slot)._test_fault(fault)
    try:
        with pytest.raises((RuntimeError, KeyboardInterrupt)):
            reclaim(
                directory, receiver=original, expected_directory_identity=identity, octet_budget=3
            )
        assert not original.received and original.receipt is None
        assert receive(original, expected_directory_identity=identity) is None
        assert data.read_bytes() == b"x" * 14
        original.close()
        original.close()
        assert original.received and data.read_bytes() == b"x" * 14
        assert original.receipt is not None
        assert original.receipt.directory_identity == identity
        assert original.receipt.names == tuple(name.encode() for name in NAMES)
        assert original.receipt.progress.removed_octets == 3
        assert not original.receipt.progress.files_reclaimed
        progress = receive(original, expected_directory_identity=identity)
        assert progress is not None and not progress.files_reclaimed
        assert progress.removed_octets == 3 and progress.remaining_octets == 14
        assert original._file.metrics["close_calls"] == 1
        assert original._directory.metrics["close_calls"] == 1
    finally:
        original.close()


@pytest.mark.growth_witness(
    "riverhog-generation-storage-reclamation-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_reclamation.reclaim_retired_generation_storage",
)
def test_generation_file_work_is_bounded_independently_of_other_workspace_files(
    tmp_path, monkeypatch, record_property, retained_file_receivers
):
    binding = growth_binding(reclaim)
    policy = numeric_policy_from_environment(os.environ)
    projection = project_bindings((binding,), numeric_policy=policy)
    samples = []
    for unrelated in (0, 128):
        for files in (0, 1, 4):
            for extent in (0, 17, 65537):
                directory = tmp_path / f"{unrelated}-{files}-{extent}"
                identity = private_directory(directory)
                for index in range(unrelated):
                    private_file(directory, f"unrelated-{index}", 1)
                for name in NAMES[:files]:
                    private_file(directory, name, extent)
                semantic = private_file(directory, "snapshot.sqlite3", 29)
                dimensions = {
                    "retained_octets": files * extent,
                    "retained_files": files,
                    "octet_budget": 4096,
                    "directory_octets": len(os.fsencode(directory)),
                }
                original = retained_file_receivers()
                with observe_reclamation(monkeypatch, reclaim) as metrics:
                    with observe_file_delegate(
                        monkeypatch, owner, projection, dimensions, metrics
                    ) as children:
                        progress = reclaim(
                            directory,
                            receiver=original,
                            expected_directory_identity=identity,
                            octet_budget=4096,
                        )
                assert original.received
                assert metrics["sync_calls"] == 1
                assert progress.removed_octets == metrics["reclaimed_extent_octets"]
                assert progress.remaining_octets == files * extent - progress.removed_octets
                assert semantic.read_bytes() == b"x" * 29
                sample = execution_sample(
                    binding, projection, dimensions, metrics, {"files": children}
                )
                validate_execution_samples(
                    projection, binding.declaration.id, [sample], numeric_policy=policy
                )
                samples.append(sample)
    record_property("growth_samples", samples)


@pytest.mark.parametrize("identity", [None, (1,), (True, 1), (-1, 1)])
def test_receiving_requires_an_exact_directory_pin_before_reading_its_receipt(
    monkeypatch, retained_file_receivers, identity
):
    def forbidden(_owner):
        pytest.fail("invalid identity reached an original work receipt")

    original = retained_file_receivers()
    monkeypatch.setattr(files_owner.FileReclamationOwner, "receipt", property(forbidden))
    with pytest.raises(ValueError, match="identity"):
        receive(original, expected_directory_identity=identity)


def measured_receiving(monkeypatch, original, identity, *, failure=None):
    metrics = dict.fromkeys(
        (
            "receipt_reads",
            "directory_checks",
            "namespace_checks",
            "storage_calls",
            "payload_octets",
        ),
        0,
    )
    receipt = files_owner.FileReclamationOwner.receipt.fget
    same = owner._same_reclamation_directory
    names = owner._generation_reclamation_namespace
    native_before = [record.metrics for record in (original._directory, original._file)]

    def read(selected):
        assert selected is original
        metrics["receipt_reads"] += 1
        return receipt(selected)

    def directory_check(actual, expected):
        metrics["directory_checks"] += 1
        return same(actual, expected)

    def namespace_check(actual):
        metrics["namespace_checks"] += 1
        return names(actual)

    def payload(*args, **kwargs):
        pytest.fail("generation receiving read or enumerated storage")

    with monkeypatch.context() as patch:
        patch.setattr(files_owner.FileReclamationOwner, "receipt", property(read))
        patch.setattr(owner, "_same_reclamation_directory", directory_check)
        patch.setattr(owner, "_generation_reclamation_namespace", namespace_check)
        for name in ("open", "stat", "fstat", "close", "fsync", "ftruncate", "unlink"):
            actual = getattr(os, name)

            def counted(*args, _actual=actual, **kwargs):
                metrics["storage_calls"] += 1
                return _actual(*args, **kwargs)

            patch.setattr(os, name, counted)
        for name in ("read", "pread", "readv", "listdir", "scandir"):
            patch.setattr(os, name, payload)
        if failure is None:
            result = receive(original, expected_directory_identity=identity)
        else:
            with pytest.raises(InvalidJournalWorkspace, match=failure):
                receive(original, expected_directory_identity=identity)
            result = None
    for record, before in zip((original._directory, original._file), native_before, strict=True):
        metrics["storage_calls"] += sum(
            record.metrics[key] - before[key]
            for key in ("open_calls", "close_calls", "fstat_calls", "dup_calls")
        )
    assert metrics["storage_calls"] == metrics["payload_octets"] == 0
    return result, metrics


@pytest.mark.growth_witness(
    "riverhog-generation-reclamation-receiving-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance.journal_reclamation.received_generation_reclamation",
)
def test_received_generation_work_is_bounded_and_never_borrows_another_namespace(
    tmp_path, monkeypatch, record_property, retained_file_receivers
):
    import hashlib

    from riverhog_canonical_json import canonical_json_bytes

    binding = growth_binding(receive)
    policy = numeric_policy_from_environment(os.environ)
    projection = project_bindings((binding,), numeric_policy=policy)
    samples = []
    for unrelated in (0, 128):
        for extent in (0, 65537):
            for state in (
                "passive",
                "closed",
                "complete",
                "partial",
                "workspace",
                "subset",
                "foreign",
                "wrong-pin",
                "failed-sync",
            ):
                directory = tmp_path / f"{unrelated}-{extent}-{state}"
                identity = private_directory(directory)
                private_file(directory, "semantic-state", extent)
                for index in range(unrelated):
                    private_file(directory, f"unrelated-{index}", 0)
                original = retained_file_receivers()
                failure = None
                if state == "closed":
                    original.close()
                elif state in {"complete", "partial", "wrong-pin", "failed-sync"}:
                    if state in {"partial", "failed-sync"}:
                        private_file(directory, NAMES[-1], 17)
                    if state == "failed-sync":

                        def refused(_descriptor):
                            raise OSError(errno.ENOSPC, "original synchronization failed")

                        with monkeypatch.context() as patch:
                            patch.setattr(os, "fsync", refused)
                            with pytest.raises(JournalValidationCapacity):
                                reclaim(
                                    directory,
                                    receiver=original,
                                    expected_directory_identity=identity,
                                    octet_budget=4096,
                                )
                        original.close()
                    else:
                        reclaim(
                            directory,
                            receiver=original,
                            expected_directory_identity=identity,
                            octet_budget=3,
                        )
                        if state == "wrong-pin":
                            identity = identity[0], identity[1] + 1
                            failure = "directory"
                elif state in {"workspace", "subset", "foreign"}:
                    if state == "workspace":
                        owner.reclaim_retired_journal_workspace(
                            directory, receiver=original, octet_budget=3
                        )
                    else:
                        files_owner.reclaim_retired_files(
                            directory,
                            receiver=original,
                            names=(NAMES[-1],)
                            if state == "subset"
                            else ("foreign", "names", "are", "different"),
                            octet_budget=3,
                            expected_directory_identity=identity,
                        )
                    failure = "file set"
                progress, metrics = measured_receiving(
                    monkeypatch, original, identity, failure=failure
                )
                if state in {"complete", "partial"}:
                    assert progress is not None
                    assert progress.files_reclaimed is (state == "complete")
                else:
                    assert progress is None
                sample = {
                    "declaration": binding.declaration.id,
                    "declaration_sha256": hashlib.sha256(
                        canonical_json_bytes(projection["declarations"][binding.declaration.id])
                    ).hexdigest(),
                    "dimensions": {"unrelated_files": unrelated, "retained_octets": extent},
                    "metrics": metrics,
                    "delegates": {},
                }
                validate_execution_samples(
                    projection, binding.declaration.id, [sample], numeric_policy=policy
                )
                samples.append(sample)
    record_property("growth_samples", samples)

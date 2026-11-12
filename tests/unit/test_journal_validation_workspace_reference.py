from __future__ import annotations

import os
from contextlib import contextmanager

import pytest
from riverhog_core import journal_validation_workspace as kernel
from riverhog_core import journal_validation_workspace_reference as owner

from tests.support.validation_workspace import leased_workspace, retained_workspace_frames


@pytest.fixture(autouse=True)
def original_workspace_receivers():
    with retained_workspace_frames() as frames:
        yield frames


ONE, TWO = "1" * 32, "2" * 32


@contextmanager
def held(root, *, shared=False):
    if shared:
        with leased_workspace(root, ONE, create=True) as directory:
            assert directory is not None
    captured = []
    with leased_workspace(
        root, ONE, create=not shared, shared=shared, on_custody=captured.append
    ) as directory:
        assert directory is not None
        yield captured[0]


def available(root, *, busy, shared=False):
    with leased_workspace(root, ONE, shared=shared) as directory:
        assert (directory is None) is busy


class CloseFault:
    def __init__(self, stream, *, after=False):
        self.stream, self.after, self.calls = stream, after, 0

    def __getattr__(self, name):
        return getattr(self.stream, name)

    def close(self):
        self.calls += 1
        if self.calls == 1:
            if self.after:
                self.stream.close()
            raise OSError("lease close unconfirmed")
        self.stream.close()


@pytest.mark.parametrize("shared", [False, True])
def test_independent_references_keep_the_actual_original_lock_until_last_close(tmp_path, shared):
    root = tmp_path / "retained"
    references = []
    with held(root, shared=shared) as custody:
        first = owner.retain_validation_workspace(custody, on_reference=references.append)
        last = owner.retain_validation_workspace(custody, on_reference=references.append)
        assert references == [first, last]
        assert first is not last and not first.closed and not last.closed
        assert custody.verify_current()
    assert not custody.verify_current()
    available(root, busy=True)
    if shared:
        available(root, busy=False, shared=True)
    with leased_workspace(root, TWO, create=True) as independent:
        assert independent is not None
    first.close()
    available(root, busy=True)
    last.close()
    available(root, busy=False)
    first.close()
    last.close()
    assert first.closed and last.closed


@pytest.mark.parametrize("after", [False, True])
def test_failed_close_retries_the_same_stream_without_closing_a_recycled_descriptor(
    tmp_path, after
):
    root = tmp_path / "retained"
    references = []
    with held(root) as custody:
        reference = owner.retain_validation_workspace(custody, on_reference=references.append)
        original = reference._resource
        descriptor = original.fileno()
        fault = CloseFault(original, after=after)
        reference._resource = fault
    replacement = None
    try:
        with pytest.raises(OSError, match="unconfirmed"):
            reference.close()
        assert not reference.closed and reference._resource is fault and fault.calls == 1
        available(root, busy=not after)
        if after:
            replacement = os.open(tmp_path / "replacement", os.O_RDWR | os.O_CREAT, 0o600)
            if replacement != descriptor:
                os.dup2(replacement, descriptor)
            identity = os.fstat(descriptor)
        reference.close()
        assert reference.closed and fault.calls == 2
        available(root, busy=False)
        if after:
            retained = os.fstat(descriptor)
            assert (retained.st_dev, retained.st_ino) == (identity.st_dev, identity.st_ino)
        reference.close()
        assert fault.calls == 2
    finally:
        reference.close()
        if replacement is not None:
            if replacement != descriptor:
                os.close(descriptor)
            os.close(replacement)


def test_closed_or_replaced_custody_cannot_retain_by_identity(tmp_path):
    root = tmp_path / "retained"
    references = []
    with held(root) as custody:
        lease = custody.directory / ".lease"
        lease.rename(custody.directory / ".retired")
        lease.touch(mode=0o600)
        with pytest.raises(kernel.LostJournalValidationWorkspace, match="no longer held"):
            owner.retain_validation_workspace(custody, on_reference=references.append)
        assert references == []
    with pytest.raises(kernel.LostJournalValidationWorkspace, match="no longer held"):
        owner.retain_validation_workspace(custody, on_reference=references.append)
    assert references == []


@pytest.mark.parametrize("failed_cleanup", [False, True])
def test_post_acquisition_custody_loss_keeps_unconfirmed_cleanup_reachable(
    tmp_path, monkeypatch, failed_cleanup
):
    root = tmp_path / "retained"
    references, faults, checks = [], [], []
    current = kernel.ValidationWorkspaceCustody.verify_current
    actual_duplicate = owner.RetainedWorkspaceResource.duplicate
    with held(root) as custody:

        def changed(selected):
            if selected is not custody:
                return current(selected)
            checks.append(1)
            return current(selected) if len(checks) == 1 else False

        def duplicate(selected, source):
            result = actual_duplicate(selected, source)
            references[-1]._resource = CloseFault(selected)
            faults.append(references[-1]._resource)
            return result

        with monkeypatch.context() as patch:
            patch.setattr(kernel.ValidationWorkspaceCustody, "verify_current", changed)
            if failed_cleanup:
                patch.setattr(owner.RetainedWorkspaceResource, "duplicate", duplicate)
                expected, message = OSError, "close unconfirmed"
            else:
                expected, message = (
                    kernel.LostJournalValidationWorkspace,
                    "changed during retention",
                )
            with pytest.raises(expected, match=message):
                owner.retain_validation_workspace(custody, on_reference=references.append)
        assert len(references) == 1 and len(checks) == 2
        assert references[0].closed is not failed_cleanup
    available(root, busy=failed_cleanup)
    references[0].close()
    available(root, busy=False)
    if failed_cleanup:
        assert faults[0].calls == 2


@pytest.mark.parametrize("failure", ["callback", "duplicate", "callback-close"])
def test_early_rejection_releases_only_its_acquisition_and_preserves_original_custody(
    tmp_path, monkeypatch, failure
):
    root = tmp_path / "retained"
    references = []
    with held(root) as custody:

        def retain(reference):
            references.append(reference)
            assert reference._resource.owner.state == "new"
            if failure == "callback":
                raise InterruptedError("retention callback interrupted")
            if failure == "callback-close":
                reference.close()

        def duplicate(_resource, _source):
            raise OSError("descriptor acquisition failed")

        with monkeypatch.context() as patch:
            if failure == "duplicate":
                patch.setattr(owner.RetainedWorkspaceResource, "duplicate", duplicate)
                expected, message = OSError, "descriptor acquisition failed"
            elif failure == "callback-close":
                expected, message = (
                    kernel.LostJournalValidationWorkspace,
                    "closed before acquisition",
                )
            else:
                expected, message = InterruptedError, "retention callback interrupted"
            with pytest.raises(expected, match=message):
                owner.retain_validation_workspace(custody, on_reference=retain)
        assert len(references) == 1 and references[0].closed
        assert custody.verify_current()
        available(root, busy=True)
    available(root, busy=False)


@pytest.mark.parametrize(
    "fault",
    [None, ("child", False), ("child", True), ("parent", False), ("parent", True)],
)
def test_actual_frozen_native_borrowers_retain_both_parent_and_child_custody(
    tmp_path, fault, *, journal_constructions
):
    from riverhog_core import canonical_discovery_journal_registration as registration
    from riverhog_core.canonical_discovery_journal_source import DiscoveryJournalSource
    from riverhog_provenance import (
        JournalValidationBudget,
        JournalValidationSession,
        iter_journal_frames,
    )

    from tests.support.canonical_discovery_work import corrected_journal_fixture
    from tests.unit.test_canonical_discovery_journal_source import source_case

    fixture, raw, _ = corrected_journal_fixture()
    journal_id = tuple(iter_journal_frames((raw,)))[0].document["journal_id"]
    child_identity = "a" * 32
    references, handles, results = [], [], []
    try:
        with source_case(
            tmp_path / "native-result",
            raw,
            segments=17,
            catalog=fixture.catalog,
            journal_id=journal_id,
        ) as (case, work, _original):
            parent_root = case.metadata.custody.directory.parent
            parent_identity = case.metadata.job.workspace_id
            parent = owner.retain_validation_workspace(
                case.metadata.custody, on_reference=references.append
            )
            source = DiscoveryJournalSource(work, directory_id=child_identity)
            with case.metadata.transaction() as (db, key):
                selected = registration.register_discovery_journal(db, key, work, source.binding)
            child_root = case.metadata.custody.directory / "journals"
            captured = []
            with leased_workspace(
                child_root, child_identity, create=True, on_custody=captured.append
            ) as directory:
                assert directory is not None
                child = owner.retain_validation_workspace(
                    captured[0], on_reference=references.append
                )

                def release():
                    child.close()
                    parent.close()

                session = JournalValidationSession.open_retained(
                    directory,
                    source,
                    attempt_id=source.binding.attempt_id,
                    worker_epoch=case.metadata.job.worker_epoch,
                    create=True,
                    catalog=fixture.catalog,
                    on_storage_open=handles.append,
                    on_storage_close=release,
                    construction=journal_constructions.retain(),
                )
                with case.metadata.transaction() as (db, key):
                    assert registration.confirm_discovery_journal(db, key, work, selected)
                for _ in range(100000):
                    if session.advance(JournalValidationBudget(rows=64)).complete:
                        break
                else:
                    pytest.fail("small actual Native journal did not complete")
                first = session.result()
                last = first.borrow()
                results.extend((first, last))
                session.close()
                first.close()
                assert not parent.closed and not child.closed
                assert last.journal_sha256 == source.binding.source_sha256
                identity = (directory / "snapshot.sqlite3").stat().st_ino
            case.metadata.close()
        # Both original kernel contexts and actual Core metadata are now closed.
        # Only the last independently owned Native result retains these locks.
        for root, workspace in ((parent_root, parent_identity), (child_root, child_identity)):
            with leased_workspace(root, workspace) as blocked:
                assert blocked is None
        assert last.journal_sha256 == source.binding.source_sha256
        if fault is not None:
            selected_reference = child if fault[0] == "child" else parent
            selected_reference._resource = CloseFault(selected_reference._resource, after=fault[1])
            with pytest.raises(OSError, match="close unconfirmed"):
                last.close()
            assert not selected_reference.closed
            with pytest.raises(RuntimeError, match="closed"):
                last.borrow()
        last.close()
        assert child.closed and parent.closed
        for root, workspace in ((parent_root, parent_identity), (child_root, child_identity)):
            with leased_workspace(root, workspace) as released:
                assert released is not None
        assert (directory / "snapshot.sqlite3").stat().st_ino == identity
    finally:
        for result in results:
            result.close()
        for handle in handles:
            handle.close()
        for reference in references:
            reference.close()

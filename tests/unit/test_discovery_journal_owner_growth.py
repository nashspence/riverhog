from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.canonical_discovery_journal_source import DiscoveryJournalSource
from riverhog_core.journal_validation_workspace_reference import ValidationWorkspaceReference
from riverhog_core.journal_validation_workspace_references import JournalWorkspaceReferences
from riverhog_core.services import discovery_journal_owner as child
from riverhog_core.services import discovery_validation_owner as owner
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import JournalValidationSession

from tests.unit.test_discovery_journal_owner import prepared, quantum
from tests.unit.test_discovery_validation_owner_growth import Recorder, Slots, dimensions
from tests.unit.test_journal_validation_workspace_reference_growth import sample

SOURCE = owner.DiscoveryValidationWork.journal_source
JOURNAL = owner.DiscoveryValidationWork.journal
OPEN = child.DiscoveryJournalOwner.open
CLOSE = child.DiscoveryJournalOwner.close
REFERENCES = JournalWorkspaceReferences.close
CURRENT = child.DiscoveryJournalCustodySource.verify_current
WINDOW = child.DiscoveryJournalCustodySource.read_window
EXACT = child.DiscoveryJournalCustodySource.read_exact


@contextmanager
def observe(operation, monkeypatch):
    metrics = {bound.metric: 0 for bound in growth_binding(operation).declaration.bounds}
    depth = 0

    def wrap(function, metric):
        def measured(*args, **kwargs):
            nonlocal depth
            outer = depth == 0
            if outer and metric in metrics:
                metrics[metric] += 1
            depth += 1
            try:
                return function(*args, **kwargs)
            finally:
                depth -= 1

        return measured

    source_owner = (
        DiscoveryJournalSource
        if operation in {CURRENT, WINDOW, EXACT}
        else child.DiscoveryJournalCustodySource
    )
    targets = (
        (owner.DiscoveryValidationWork, "prepare_journal", "preparation_dispatches"),
        (owner.DiscoveryValidationWork, "journal", "journal_dispatches"),
        (owner.DiscoveryJournalEpochWork, "__init__", "epoch_constructions"),
        (
            owner.journal_preparation.DiscoveryJournalPreparationWork,
            "__init__",
            "preparation_constructions",
        ),
        (owner.journal_preparation, "begin_discovery_journal_preparation", "preparation_begins"),
        (
            owner.journal_preparation,
            "prepare_discovery_journal_preparation",
            "preparation_prepares",
        ),
        (owner.journal_preparation, "apply_discovery_journal_preparation", "preparation_applies"),
        (
            owner.journal_preparation,
            "discovery_journal_preparation_progress",
            "preparation_progress_calls",
        ),
        (owner.DiscoveryJournalGraphCertificates, "__init__", "certificate_constructions"),
        (owner.DiscoveryJournalGraphCertificates, "verify_current", "certificate_checks"),
        (JournalValidationSession, "advance", "native_advances"),
        (child.ValidationWorkspaceCustody, "verify_current", "custody_checks"),
        (DiscoveryJournalSource, "read_window", "source_windows"),
        (DiscoveryJournalSource, "read_exact", "source_exact_reads"),
        (owner.DiscoveryMetadata, "read", "metadata_reads"),
        (owner.DiscoveryMetadata, "transaction", "metadata_transactions"),
        (owner.registration, "find_discovery_journal", "registration_finds"),
        (owner.registration, "register_discovery_journal", "registration_writes"),
        (owner.registration, "confirm_discovery_journal", "registration_confirms"),
        (DiscoveryJournalSource, "__init__", "source_constructions"),
        (owner, "uuid4", "uuid_values"),
        (owner.DiscoveryValidationWork, "journal_source", "source_preparations"),
        (child.DiscoveryJournalOwner, "__init__", "child_constructions"),
        (child.DiscoveryJournalOwner, "open", "child_opens"),
        (child.DiscoveryJournalOwner, "close", "child_closes"),
        (JournalValidationSession, "progress", "native_progress_calls"),
        (JournalValidationSession, "close", "native_closes"),
        (source_owner, "verify_current", "source_checks"),
        (owner.jobs, "lose_discovery_corpus", "lost_catalog_calls"),
        (child, "retain_validation_workspace", "reference_retains"),
        (child, "lease_journal_validation_workspace", "lease_contexts"),
        (child.Path, "exists", "path_probes"),
        (child.ExitStack, "close", "context_closes"),
        (JournalWorkspaceReferences, "close", "reference_group_closes"),
        (ValidationWorkspaceReference, "close", "reference_closes"),
    )
    with monkeypatch.context() as patch:
        for module, name, metric in targets:
            function = getattr(module, name)
            # The root is called through its captured operation, so wrapping its
            # class port still measures nested calls to another selected owner.
            patch.setattr(module, name, wrap(function, metric))
        native_open = JournalValidationSession.open_retained

        def opened(cls, *args, **kwargs):
            return wrap(native_open, "native_opens")(*args, **kwargs)

        patch.setattr(JournalValidationSession, "open_retained", classmethod(opened))
        yield metrics


def seed_independent_registrations(work, framing, retained):
    source = work.journal_source(framing)
    with work.metadata.transaction() as (db, key):
        for ordinal in range(retained):
            other = replace(
                source.binding,
                journal_id=f"urn:uuid:{ordinal:08x}-2222-4222-8222-222222222222",
                directory_id=f"{ordinal:032x}",
            )
            db.execute(
                "INSERT INTO discovery_journal_children VALUES(?,?,?,?,?,?,0)",
                (
                    key.build_id,
                    key.admission_sha256,
                    other.journal_id,
                    key.worker_id,
                    other.directory_id,
                    other.to_json(),
                ),
            ).close()


def exercise(tmp_path, monkeypatch, record_property, operation):
    samples = []
    for retained in (1, 17, 1025):
        with prepared(tmp_path / str(retained)) as case:
            with quantum(case) as (work, framing):
                # Actual unrelated durable registrations, not an invented count.
                seed_independent_registrations(work, framing, retained)
                values = {"retained_journals": retained + 1}
                if operation is SOURCE:
                    for _ in range(2):
                        with observe(operation, monkeypatch) as metrics:
                            source = SOURCE(work, framing)
                        assert source.verify_current()
                        samples.append(sample(operation, values, metrics))
                elif operation is JOURNAL:
                    for _ in range(2):
                        with observe(operation, monkeypatch) as metrics:
                            native = JOURNAL(work, framing)
                        assert native is not None
                        samples.append(sample(operation, values, metrics))
                    assert metrics["child_opens"] == 0
                elif operation is OPEN:
                    source = work.journal_source(framing)
                    retained_child = child.DiscoveryJournalOwner(source)
                    work._slot.journal = retained_child
                    with observe(operation, monkeypatch) as metrics:
                        native = OPEN(
                            retained_child,
                            work.metadata,
                            case.catalog,
                            confirmed=False,
                            certificates=None,
                        )
                    assert native is not None and metrics["reference_retains"] == 2
                    samples.append(sample(operation, values, metrics))
                else:
                    assert work.journal(framing) is not None
                    retained_child = work._slot.journal
                    target = retained_child if operation is CLOSE else retained_child.references
                    if operation is REFERENCES:
                        # The actual Native handle closes first. Keep both real
                        # references with this caller to observe their own release.
                        retained_child.session._store._storage.on_close = None
                        retained_child.session.close()
                        retained_child.session = None
                        retained_child.transferred = False
                        retained_child.resources.close()
                    for _ in range(2):
                        with observe(operation, monkeypatch) as metrics:
                            operation(target)
                        samples.append(sample(operation, values, metrics))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-source-dispatch/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.journal_source",
)
def test_original_source_preparation_dispatch(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, SOURCE)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-warm-dispatch/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationWork.journal",
)
def test_actual_child_open_and_warm_dispatch(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, JOURNAL)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-child-open/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_owner.DiscoveryJournalOwner.open",
)
def test_actual_child_open_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, OPEN)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-child-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_owner.DiscoveryJournalOwner.close",
)
def test_actual_child_close_and_retry_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, CLOSE)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-reference-group-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.journal_validation_workspace_references.JournalWorkspaceReferences.close",
)
def test_actual_child_parent_reference_release_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, REFERENCES)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-composed-owner-close/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_validation_owner.DiscoveryValidationOwner.close",
)
def test_actual_child_cleanup_in_composed_application_close(tmp_path, monkeypatch, record_property):
    samples = []
    for retained in (1, 17, 1025):
        with prepared(tmp_path / str(retained)) as case:
            recorder = Recorder()
            case.readers = [case.reader]
            case.pool._slots = Slots(recorder)
            with recorder.connect_only():
                with quantum(case) as (work, framing):
                    seed_independent_registrations(work, framing, retained)
                    assert work.journal(framing) is not None
                    references = work._slot.journal.references
                    child_path_octets = len(
                        os.fsencode(work._slot.journal.directory / "snapshot.sqlite3")
                    )
                    storage = work._slot.journal.session._store._storage
                    callback = storage.on_close

                    def release(_callback=callback, _recorder=recorder):
                        if _recorder.active:
                            _recorder.metrics["native_release_callbacks"] += 1
                        return _callback()

                    storage.on_close = release
                values = dimensions(case, retained=retained, width=17)
                values["path_octets"] = max(values["path_octets"], child_path_octets)

                class Stream:
                    def __init__(self, actual):
                        self.actual = actual

                    def close(self, _recorder=recorder):
                        if _recorder.active:
                            _recorder.metrics["stream_close_calls"] += 1
                        return self.actual.close()

                references.parent._stream = Stream(references.parent._stream)
                references.child._stream = Stream(references.child._stream)
                with recorder.observe(case, cold=False):
                    case.pool.close()
                assert recorder.metrics["native_closed_connections"] == 1
                assert recorder.metrics["stream_close_calls"] == 2
                samples.append(recorder.sample(type(case.pool).close, values))
    record_property("growth_samples", samples)


def custody_exercise(tmp_path, monkeypatch, record_property, operation):
    samples = []
    for retained in (1, 17, 1025):
        with prepared(tmp_path / str(retained)) as case:
            with quantum(case) as (work, framing):
                seed_independent_registrations(work, framing, retained)
                assert work.journal(framing) is not None
                source = work._slot.journal.native_source
                directory = source.custody.directory
                dimensions = {"retained_journals": retained + 1, "requested_octets": 17}
                for stale in (False, True):
                    if stale:
                        (directory / ".lease").rename(directory / ".held-original-lease")
                    try:
                        with observe(operation, monkeypatch) as metrics:
                            if operation is CURRENT:
                                assert operation(source) is not stale
                            elif stale:
                                with pytest.raises(owner.StaleArchiveTask, match="custody"):
                                    operation(source, 0, 17)
                            else:
                                result = operation(source, 0, 17)
                                metrics["returned_octets"] += len(result)
                        samples.append(sample(operation, dimensions, metrics))
                        if stale:
                            assert metrics["custody_checks"] == 1
                            assert metrics.get("source_windows", 0) == 0
                            assert metrics.get("source_exact_reads", 0) == 0
                    finally:
                        if stale:
                            (directory / ".held-original-lease").rename(directory / ".lease")
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-custody-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_owner.DiscoveryJournalCustodySource.verify_current",
)
def test_actual_child_source_currentness_ports(tmp_path, monkeypatch, record_property):
    custody_exercise(tmp_path, monkeypatch, record_property, CURRENT)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-custody-window/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_owner.DiscoveryJournalCustodySource.read_window",
)
def test_actual_child_source_window_ports(tmp_path, monkeypatch, record_property):
    custody_exercise(tmp_path, monkeypatch, record_property, WINDOW)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-custody-exact/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.discovery_journal_owner.DiscoveryJournalCustodySource.read_exact",
)
def test_actual_child_source_exact_ports(tmp_path, monkeypatch, record_property):
    custody_exercise(tmp_path, monkeypatch, record_property, EXACT)

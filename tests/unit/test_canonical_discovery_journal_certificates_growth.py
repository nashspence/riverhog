from __future__ import annotations

import hashlib
from contextlib import ExitStack, contextmanager

import pytest
from riverhog_core import canonical_discovery_journal_certificates as owner
from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_rank as rank
from riverhog_growth_contracts import growth_binding
from riverhog_provenance.replay_binding import JournalReplayBinding

from tests.unit.test_canonical_discovery_archive_framing_growth import (
    dimensions as framing_dimensions,
)
from tests.unit.test_canonical_discovery_archive_framing_growth import observe as observe_framing
from tests.unit.test_canonical_discovery_journal_certificates import certificates
from tests.unit.test_canonical_discovery_journal_rank import edge, frame, rank_case, uid
from tests.unit.test_canonical_discovery_journal_source_growth import sample
from tests.unit.test_canonical_discovery_journal_union_growth import width

OPERATIONS = {
    "construct": owner.DiscoveryJournalGraphCertificates.__init__,
    "current": owner.DiscoveryJournalGraphCertificates.verify_current,
    "edge": owner.DiscoveryJournalGraphCertificates.edge,
}


@pytest.fixture(scope="module")
def completed_cases(tmp_path_factory):
    path = tmp_path_factory.mktemp("certificate-growth-originals")
    with ExitStack() as kept:
        cases = []
        for size, retained, mode in (
            (1, 1, "acyclic"),
            (17, 17, "acyclic"),
            (1025, 1, "acyclic"),
            (1, 1025, "acyclic"),
            (1025, 17, "cyclic"),
            (1, 17, "deferred"),
        ):
            rows = (
                [edge(n, n, (n + 1) % size) for n in range(size)]
                if mode == "cyclic"
                else [edge(n, 0, n + 1) for n in range(size)]
            )
            if mode == "deferred":
                rows.append(rows[0])
            raw = frame(rows, kind="journal_init")
            case, task = kept.enter_context(
                rank_case(
                    path / f"{size}-{retained}-{mode}",
                    raw,
                    retained=retained,
                )
            )
            provider = certificates(case, task)
            cases.append((case, task, provider, raw, mode))
        yield cases


@contextmanager
def observe(case, task, operation, monkeypatch):
    with observe_framing(case, task.union.topology.epochs.framing, monkeypatch) as recorder:
        for bound in growth_binding(operation).declaration.bounds:
            recorder.metrics.setdefault(bound.metric, 0)
        with monkeypatch.context() as patch:
            actual_read = case.metadata.read

            @contextmanager
            def read():
                before, completed = dict(recorder.metrics), None
                try:
                    with actual_read() as (_, key):
                        recorder.metrics.update(before)
                        recorder.metrics["metadata_read_calls"] += 1
                        try:
                            yield recorder, key
                        finally:
                            completed = dict(recorder.metrics)
                finally:
                    recorder.metrics.update(before if completed is None else completed)

            patch.setattr(case.metadata, "read", read)
            children = [
                (rank, "discovery_journal_rank_progress", "rank_progress_calls"),
                (epochs, "discovery_journal_epoch_frame", "epoch_frame_calls"),
                (JournalReplayBinding, "to_json", "native_calls"),
                (owner.JournalGraphCertificateBinding, "checkpoint_sha256", "native_calls"),
                (owner.JournalGraphCertificate, "verify", "native_calls"),
                (owner.JournalGraphCertificate, "encoded_octets", "native_calls"),
            ]
            for module, name, metric in children:
                actual = getattr(module, name)

                def call(*args, _actual=actual, _metric=metric, **kwargs):
                    before = dict(recorder.metrics)
                    result = None
                    try:
                        result = _actual(*args, **kwargs)
                        return result
                    finally:
                        recorder.metrics.update(before)
                        recorder.metrics[_metric] = before[_metric] + 1
                        if _metric == "native_calls":
                            recorder.metrics["native_input_octets"] += width((args, kwargs))
                            recorder.metrics["native_output_octets"] += width(result)

                patch.setattr(module, name, call)
            for module in (owner, rank):
                for name, metric in (
                    ("parse_scalar", "scalar_decoded_octets"),
                    ("format_scalar", "scalar_encoded_octets"),
                ):
                    if not hasattr(module, name):
                        continue
                    actual = getattr(module, name)

                    def scalar(domain, value, _actual=actual, _metric=metric):
                        if _metric == "scalar_decoded_octets" and isinstance(value, str):
                            recorder.metrics[_metric] += len(value.encode())
                        result = _actual(domain, value)
                        if _metric == "scalar_encoded_octets":
                            recorder.metrics[_metric] += len(result.encode())
                        return result

                    patch.setattr(module, name, scalar)
                actual_identity = module.require_canonical_uuid_urn

                def identity(value, _actual=actual_identity):
                    recorder.metrics["identity_checks"] += 1
                    if isinstance(value, str):
                        recorder.metrics["identity_input_octets"] += len(value.encode())
                    return _actual(value)

                patch.setattr(module, "require_canonical_uuid_urn", identity)
            actual_json, actual_hash = owner.canonical_json_bytes, owner.hashlib.sha256

            def encode(value):
                recorder.metrics["json_encodes"] += 1
                recorder.metrics["json_input_octets"] += width(value)
                result = actual_json(value)
                recorder.metrics["json_output_octets"] += len(result)
                return result

            def digest(value=b"", *args, **kwargs):
                recorder.metrics["hashed_octets"] += len(value)
                return actual_hash(value, *args, **kwargs)

            patch.setattr(owner, "canonical_json_bytes", encode)
            patch.setattr(owner.hashlib, "sha256", digest)
            yield recorder


def exercise(completed_cases, monkeypatch, record_property, mode):
    samples = []
    operation = OPERATIONS[mode]
    for case, task, provider, raw, graph_mode in completed_cases:
        for _retry in range(3):
            case.metadata._db.execute("PRAGMA shrink_memory").close()
            base = framing_dimensions(case, task.union.topology.epochs.framing, 1)
            values = {
                "retained_rows": base["registered_inputs"],
                "metadata_octets": max(
                    base["metadata_octets"],
                    width((case.metadata.key, provider.binding, provider._manifest)),
                ),
            }
            request = dict(
                frame_ordinal=0,
                frame_sha256=hashlib.sha256(raw[1:-1]).hexdigest(),
                kind="derivation",
                source=uid(0),
                target=uid(1),
                byte_budget=8192,
            )
            with observe(case, task, operation, monkeypatch) as recorder:
                before = case.metadata._db.total_changes
                if mode == "construct":
                    result = owner.DiscoveryJournalGraphCertificates(case.metadata, task)
                    assert result.binding == provider.binding
                elif mode == "current":
                    assert provider.verify_current()
                else:
                    proof = provider.edge(**request)
                    assert (proof is None) == (graph_mode != "acyclic")
                recorder.metrics["changed_rows"] = case.metadata._db.total_changes - before
                metrics = dict(recorder.metrics)
            samples.append(sample(operation, values, metrics))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-certificate-construct/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.canonical_discovery_journal_certificates."
        "DiscoveryJournalGraphCertificates.__init__"
    ),
)
def test_actual_completed_original_manifest_construction(
    completed_cases,
    monkeypatch,
    record_property,
):
    exercise(completed_cases, monkeypatch, record_property, "construct")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-certificate-current/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.canonical_discovery_journal_certificates."
        "DiscoveryJournalGraphCertificates.verify_current"
    ),
)
def test_actual_manifest_currentness_uses_only_qualified_point_reads(
    completed_cases,
    monkeypatch,
    record_property,
):
    exercise(completed_cases, monkeypatch, record_property, "current")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-certificate-edge/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.canonical_discovery_journal_certificates."
        "DiscoveryJournalGraphCertificates.edge"
    ),
)
def test_actual_acyclic_cyclic_and_deferred_point_proofs(
    completed_cases,
    monkeypatch,
    record_property,
):
    exercise(completed_cases, monkeypatch, record_property, "edge")

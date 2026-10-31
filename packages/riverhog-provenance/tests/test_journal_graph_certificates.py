from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace

import pytest
from riverhog_provenance import (
    ProvenanceValidationError,
    append_assertions,
    assertion,
    encode_entry,
    iter_journal_frames,
    new_id,
    reference,
    validate_journal,
)
from riverhog_provenance._online_dag import OnlineDag
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint
from riverhog_provenance.graph_certificates import (
    InvalidJournalGraphCertificate,
    JournalGraphCertificate,
    JournalGraphCertificateBinding,
)
from riverhog_provenance.journal_session import JournalValidationSession
from test_retained_journal_session import ATTEMPT, OwnedSource, directory, finish


def related(journal, catalog, who):
    with validate_journal(journal, catalog=catalog) as summary:
        old = summary.graph["artifacts"][0]
        new = copy.deepcopy(old)
        new["id"], new["assertion_id"] = new_id(), new_id()
        relation = assertion(
            "specialization",
            who,
            specific=reference(new["id"], "artifact"),
            general=reference(old["id"], "artifact"),
        )
    raw = append_assertions(
        journal,
        {"artifacts": [new], "relations": [relation]},
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    return raw, relation


class Certificates:
    def __init__(self, source, *, disabled=False, fault=None):
        self.binding = JournalGraphCertificateBinding(source.binding, "d" * 64)
        self.calls = []
        self.disabled, self.fault = disabled, fault

    def edge(self, *, frame_ordinal, frame_sha256, kind, source, target, byte_budget):
        self.calls.append((frame_ordinal, frame_sha256, kind, source, target))
        assert byte_budget > 4096
        if self.disabled:
            return None
        proof = JournalGraphCertificate(
            self.binding.checkpoint_sha256(),
            frame_ordinal,
            frame_sha256,
            0,
            kind,
            source,
            target,
            0,
            1,
            2,
            1,
        )
        if self.fault is not None:
            proof = replace(proof, **self.fault)
        return proof


def test_certificate_binding_round_trip_preserves_its_exact_source(journal, catalog):
    original = Certificates(OwnedSource(journal, catalog)).binding
    assert JournalGraphCertificateBinding.from_json(original.to_json()) == original


@pytest.mark.parametrize(
    "fault", ["algorithm", "source", "digest", "extra", "spacing", "duplicate"]
)
def test_certificate_binding_restoration_rejects_changed_protocol_or_noncanonical_json(
    journal, catalog, fault
):
    original = Certificates(OwnedSource(journal, catalog)).binding
    raw = original.to_json()
    value = json.loads(raw)
    if fault == "algorithm":
        value["algorithm"] = "another-protocol"
    elif fault == "source":
        value["source"] = "{}"
    elif fault == "digest":
        value["manifest_sha256"] = "invalid"
    elif fault == "extra":
        value["another"] = None
    elif fault == "duplicate":
        raw = raw[:-1] + ',"manifest_sha256":"' + original.manifest_sha256 + '"}'
    if fault != "duplicate":
        raw = json.dumps(value)
    with pytest.raises(InvalidJournalGraphCertificate):
        JournalGraphCertificateBinding.from_json(raw)


def open_session(path, source, certificates, catalog, *, create=False, epoch=0):
    return JournalValidationSession.open_retained(
        path,
        source,
        attempt_id=ATTEMPT,
        worker_epoch=epoch,
        create=create,
        catalog=catalog,
        certificates=certificates,
    )


def test_acyclic_rank_evidence_replaces_only_native_graph_traversal(
    tmp_path, journal, catalog, who, monkeypatch
):
    raw, _ = related(journal, catalog, who)
    with validate_journal(raw, catalog=catalog) as expected:
        source = OwnedSource(raw, catalog)
        certificates = Certificates(source)

        def online(*_args, **_kwargs):
            pytest.fail("a certified epoch entered online graph traversal")

        monkeypatch.setattr(OnlineDag, "_prepare_start", online)
        with open_session(
            directory(tmp_path), source, certificates, catalog, create=True
        ) as session:
            finish(session)
            with session.result() as result:
                assert result.materialize() == expected.materialize()
                assert result.journal_sha256 == hashlib.sha256(raw).hexdigest()
        assert len(certificates.calls) == 1


def test_uncertified_epoch_uses_native_online_graph_units(
    tmp_path, journal, catalog, who, monkeypatch
):
    raw, _ = related(journal, catalog, who)
    source = OwnedSource(raw, catalog)
    certificates = Certificates(source, disabled=True)
    starts = []
    original = OnlineDag._prepare_start

    def online(self, *args, **kwargs):
        starts.append(args)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(OnlineDag, "_prepare_start", online)
    with open_session(directory(tmp_path), source, certificates, catalog, create=True) as session:
        finish(session)
        session.result().close()
    assert len(starts) == len(certificates.calls) == 1


@pytest.mark.parametrize(
    "fault",
    [
        {"frame_sha256": "0" * 64},
        {"binding_sha256": "0" * 64},
        {"frame_ordinal": 0},
        {"source_rank": 1},
        {"target_rank": 2},
        {"arc_count": 0},
        {"epoch": 2},
    ],
)
def test_missing_or_changed_rank_evidence_cannot_accept_the_frame(
    tmp_path, journal, catalog, who, fault
):
    raw, _ = related(journal, catalog, who)
    source = OwnedSource(raw, catalog)
    certificates = Certificates(source, fault=fault)
    with open_session(directory(tmp_path), source, certificates, catalog, create=True) as session:
        with pytest.raises(InvalidJournalGraphCertificate):
            finish(session)
        assert session._ledger.load().checkpoint.frame_ordinal == 1
        with pytest.raises(RuntimeError, match="terminal result"):
            session.result()


def test_reference_failure_retains_native_order_ahead_of_graph_evidence(
    tmp_path, journal, catalog, who
):
    raw, _ = related(journal, catalog, who)
    frames = list(iter_journal_frames((raw,)))
    document = frames[-1].document
    document["body"]["assertions"]["relations"][0]["general"] = reference(new_id(), "artifact")
    bad = frames[0].encoded + encode_entry(document, catalog=catalog)
    with pytest.raises(ProvenanceValidationError) as expected:
        validate_journal(bad, catalog=catalog)
    source = OwnedSource(bad, catalog)
    certificates = Certificates(source)
    with open_session(directory(tmp_path), source, certificates, catalog, create=True) as session:
        with pytest.raises(type(expected.value)) as actual:
            finish(session)
        assert str(actual.value) == str(expected.value)
        assert not certificates.calls


def test_replacement_cannot_adopt_a_different_rank_manifest(tmp_path, journal, catalog, who):
    raw, _ = related(journal, catalog, who)
    path = directory(tmp_path)
    source = OwnedSource(raw, catalog)
    certificates = Certificates(source)
    with open_session(path, source, certificates, catalog, create=True) as session:
        session.advance()
    certificates.binding = replace(certificates.binding, manifest_sha256="e" * 64)
    before = (path / "snapshot.sqlite3").read_bytes()
    with pytest.raises(StaleValidationCheckpoint):
        open_session(path, source, certificates, catalog, epoch=1)
    assert (path / "snapshot.sqlite3").read_bytes() == before


def test_cyclic_union_fallback_retains_the_native_cycle_failure(tmp_path, journal, catalog, who):
    raw, relation = related(journal, catalog, who)
    frames = list(iter_journal_frames((raw,)))
    document = frames[-1].document
    reverse = assertion(
        "specialization",
        who,
        specific=relation["general"],
        general=relation["specific"],
    )
    document["body"]["assertions"]["relations"].append(reverse)
    bad = frames[0].encoded + encode_entry(document, catalog=catalog)
    with pytest.raises(ProvenanceValidationError) as expected:
        validate_journal(bad, catalog=catalog)
    source = OwnedSource(bad, catalog)
    certificates = Certificates(source, disabled=True)
    with open_session(directory(tmp_path), source, certificates, catalog, create=True) as session:
        with pytest.raises(type(expected.value)) as actual:
            finish(session)
        assert str(actual.value) == str(expected.value)
        assert len(certificates.calls) == 2


def test_reopening_after_a_committed_certificate_does_not_repeat_that_unit(
    tmp_path, journal, catalog, who
):
    raw, _ = related(journal, catalog, who)
    path = directory(tmp_path)
    source = OwnedSource(raw, catalog)
    certificates = Certificates(source)
    with open_session(path, source, certificates, catalog, create=True) as first:
        from riverhog_provenance.journal_session import JournalValidationBudget

        for _ in range(100000):
            first.advance(JournalValidationBudget(rows=1))
            if certificates.calls:
                break
        else:
            raise AssertionError("the fixture did not reach its graph check")
        before = first._ledger.load().checkpoint
        assert before.frame_ordinal == 1
    source.calls.clear()
    with open_session(path, source, certificates, catalog, epoch=1) as resumed:
        assert resumed._ledger.load().checkpoint.version == before.version + 1
        finish(resumed)
        with resumed.result() as result:
            assert result.journal_sha256 == hashlib.sha256(raw).hexdigest()
    assert len(certificates.calls) == 1
    assert not source.calls

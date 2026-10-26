from __future__ import annotations

import copy
import hashlib

import pytest
from riverhog_core import canonical_discovery_journal_certificates as owner
from riverhog_provenance import (
    JournalValidationBudget,
    JournalValidationSession,
    ProvenanceValidationError,
    append_assertions,
    assertion,
    encode_entry,
    iter_journal_frames,
    new_id,
    reference,
    validate_journal_chunks,
)
from riverhog_provenance._online_dag import OnlineDag
from riverhog_provenance.errors import JournalValidationCapacity
from riverhog_provenance.graph_certificates import InvalidJournalGraphCertificate
from riverhog_provenance_contracts import ContractCatalog

from tests.unit.test_canonical_discovery_journal_rank import (
    begin,
    edge,
    finish,
    frame,
    rank_case,
    uid,
)
from tests.unit.test_journal_summary_consumers import source as original_journal


def certificates(case, task):
    begin(case.metadata, task)
    finish(case.metadata, task)
    return owner.DiscoveryJournalGraphCertificates(case.metadata, task)


def point(provider, raw, *, ordinal=0, left=0, right=1, kind="derivation", **changes):
    document = list(iter_journal_frames((raw,)))[ordinal]
    request = dict(
        frame_ordinal=ordinal,
        frame_sha256=hashlib.sha256(document.encoded[1:-1]).hexdigest(),
        kind=kind,
        source=uid(left),
        target=uid(right),
        byte_budget=8192,
    )
    request.update(changes)
    return provider.edge(**request)


def test_exact_original_frame_and_correction_epoch_ranks_survive_reconstruction(tmp_path):
    raw = frame([edge(0), edge(1)], kind="journal_init")
    raw += frame([edge(2, 1, 0)], kind="correction", retracts=[0, 1])
    with rank_case(tmp_path / "source", raw) as (case, task):
        provider = certificates(case, task)
        for ordinal, left, right in [(0, 0, 1), (1, 1, 0)]:
            proof = point(provider, raw, ordinal=ordinal, left=left, right=right)
            assert proof.epoch == ordinal and proof.source_rank < proof.target_rank
        rebuilt = owner.DiscoveryJournalGraphCertificates(case.metadata, task)
        assert provider.binding == rebuilt.binding and rebuilt.verify_current()
        assert point(provider, raw) == point(rebuilt, raw)
        assert case.metadata._db.in_transaction is False


def test_cyclic_and_deferred_fallback_is_consistent_across_calls_and_restarts(tmp_path):
    for name, rows in [("cycle", [edge(0), edge(1, 1, 0)]), ("defer", [edge(0), edge(0)])]:
        raw = frame(rows, kind="journal_init")
        with rank_case(tmp_path / name, raw) as (case, task):
            provider = certificates(case, task)
            assert point(provider, raw) is None
            assert point(provider, raw) is None
            restored = owner.DiscoveryJournalGraphCertificates(case.metadata, task)
            assert restored.binding == provider.binding and point(restored, raw) is None


@pytest.mark.parametrize(
    "damage", ["manifest", "frame", "arc", "vertex", "rank", "counts", "graph"]
)
def test_changed_original_or_graph_evidence_fails_closed(tmp_path, damage):
    raw = frame([edge(0)], kind="journal_init")
    with rank_case(tmp_path / "source", raw) as (case, task):
        provider = certificates(case, task)
        with case.metadata.transaction() as (db, _):
            if damage == "manifest":
                db.execute("UPDATE discovery_journal_rank SET union_version='999'").close()
            elif damage == "frame":
                db.execute(
                    "UPDATE discovery_journal_epoch_frames SET json_sha256=?",
                    ("0" * 64,),
                ).close()
            elif damage == "arc":
                db.execute("DELETE FROM discovery_journal_union_arcs").close()
            elif damage == "vertex":
                db.execute("DELETE FROM discovery_journal_rank_vertices").close()
            elif damage == "rank":
                db.execute("UPDATE discovery_journal_rank_vertices SET rank='0'").close()
            elif damage == "counts":
                db.execute("UPDATE discovery_journal_union_graphs SET vertices='7'").close()
            else:
                db.execute("DELETE FROM discovery_journal_rank_graphs").close()
        with pytest.raises(InvalidJournalGraphCertificate):
            point(provider, raw)


def test_readiness_request_extent_budget_and_old_worker_are_explicit(tmp_path, monkeypatch):
    raw = frame([edge(0)], kind="journal_init")
    with rank_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        with pytest.raises(InvalidJournalGraphCertificate):
            owner.DiscoveryJournalGraphCertificates(case.metadata, task)
        finish(case.metadata, task)
        provider = owner.DiscoveryJournalGraphCertificates(case.metadata, task)
        with pytest.raises(JournalValidationCapacity):
            point(provider, raw, byte_budget=4095)
        for changes in [
            {"frame_ordinal": True},
            {"frame_ordinal": 1},
            {"frame_sha256": "0" * 64},
            {"kind": "other"},
            {"source": "bad"},
            {"target": uid(99)},
        ]:
            with pytest.raises(InvalidJournalGraphCertificate):
                point(provider, raw, **changes)
        monkeypatch.setattr(case.metadata, "_claimed", ())
        assert provider.verify_current() is False
        with pytest.raises(InvalidJournalGraphCertificate):
            point(provider, raw)


def related(catalog, *, mode="valid"):
    initial = original_journal()
    with validate_journal_chunks((initial,), catalog=catalog, require_profiles=False) as summary:
        old = summary.graph["artifacts"][0]
        who = summary.graph["agents"][0]["id"]
        new = copy.deepcopy(old)
        new["id"], new["assertion_id"] = new_id(), new_id()
        relation = assertion(
            "specialization",
            who,
            specific=reference(new["id"], "artifact"),
            general=reference(old["id"], "artifact"),
        )
    raw = append_assertions(
        initial,
        {"artifacts": [new], "relations": [relation]},
        recorded_by_agent_id=who,
        catalog=catalog,
    )
    if mode != "valid":
        frames = list(iter_journal_frames((raw,)))
        document = frames[-1].document
        if mode == "reference":
            document["body"]["assertions"]["relations"][0]["general"] = reference(
                new_id(),
                "artifact",
            )
        else:
            document["body"]["assertions"]["relations"].append(
                assertion(
                    "specialization",
                    who,
                    specific=relation["general"],
                    general=relation["specific"],
                )
            )
        raw = frames[0].encoded + encode_entry(document, catalog=catalog)
    return raw


def finish_native(session):
    while not session.advance(JournalValidationBudget(rows=64)).complete:
        pass


@pytest.mark.parametrize("mode", ["valid", "reference", "cycle"])
def test_actual_native_replay_uses_only_current_acyclic_evidence_and_keeps_error_order(
    tmp_path,
    monkeypatch,
    mode,
):
    catalog = ContractCatalog()
    raw = related(catalog, mode=mode)
    identity = next(iter_journal_frames((raw,))).document["journal_id"]
    with rank_case(
        tmp_path / "source",
        raw,
        catalog=catalog,
        journal_id=identity,
    ) as (case, task):
        provider = certificates(case, task)
        calls, starts = [], []
        actual_edge, actual_online = provider.edge, OnlineDag._prepare_start

        def checked(**request):
            calls.append(request)
            return actual_edge(**request)

        def online(self, *args, **kwargs):
            starts.append(args)
            if mode == "valid":
                pytest.fail("certified acyclic replay entered online traversal")
            return actual_online(self, *args, **kwargs)

        expected_value = None
        expected_error = None
        if mode == "valid":
            with validate_journal_chunks(
                (raw,), catalog=catalog, require_profiles=False
            ) as expected:
                expected_value = expected.materialize()
        else:
            with pytest.raises(ProvenanceValidationError) as expected:
                validate_journal_chunks((raw,), catalog=catalog, require_profiles=False)
            expected_error = expected.value
        monkeypatch.setattr(provider, "edge", checked)
        monkeypatch.setattr(OnlineDag, "_prepare_start", online)
        original = task.union.topology.epochs.source
        path = tmp_path / "native"
        path.mkdir(mode=0o700)
        if mode == "valid":
            with JournalValidationSession.open_retained(
                path,
                original,
                attempt_id=case.metadata.job.workspace_id,
                worker_epoch=case.metadata.job.worker_epoch,
                create=True,
                catalog=catalog,
                certificates=provider,
            ) as session:
                finish_native(session)
                with session.result() as result:
                    assert result.materialize() == expected_value
                    assert result.journal_sha256 == hashlib.sha256(raw).hexdigest()
            assert len(calls) == 1 and not starts
        else:
            assert expected_error is not None
            with JournalValidationSession.open_retained(
                path,
                original,
                attempt_id=case.metadata.job.workspace_id,
                worker_epoch=case.metadata.job.worker_epoch,
                create=True,
                catalog=catalog,
                certificates=provider,
            ) as session:
                with pytest.raises(type(expected_error)) as actual:
                    finish_native(session)
                assert str(actual.value) == str(expected_error)
            if mode == "reference":
                assert not calls and not starts
            else:
                assert len(calls) == len(starts) == 2


def prepared_for_owned_work(work, framing):
    from riverhog_core import canonical_discovery_journal_epochs as epochs
    from riverhog_core import canonical_discovery_journal_rank as rank
    from riverhog_core import canonical_discovery_journal_topology as topology
    from riverhog_core import canonical_discovery_journal_union as union

    from tests.unit.test_canonical_discovery_journal_epochs import begin as begin_epochs
    from tests.unit.test_canonical_discovery_journal_epochs import finish as finish_epochs
    from tests.unit.test_canonical_discovery_journal_topology import begin as begin_topology
    from tests.unit.test_canonical_discovery_journal_topology import finish as finish_topology
    from tests.unit.test_canonical_discovery_journal_union import begin as begin_union
    from tests.unit.test_canonical_discovery_journal_union import finish as finish_union

    original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
    begin_epochs(work.metadata, original)
    finish_epochs(work.metadata, original)
    facts = topology.DiscoveryJournalTopologyWork(original)
    begin_topology(work.metadata, facts)
    finish_topology(work.metadata, facts)
    graphs = union.DiscoveryJournalUnionWork(facts)
    begin_union(work.metadata, graphs)
    finish_union(work.metadata, graphs)
    ranks = rank.DiscoveryJournalRankWork(graphs)
    return certificates(work, ranks)


def test_real_worker_takeover_keeps_certificate_identity_and_resumes_same_native_checkpoint(
    tmp_path,
):
    from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner

    from tests.unit.test_discovery_journal_owner import prepared as owned
    from tests.unit.test_discovery_journal_owner import quantum

    with owned(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            first = prepared_for_owned_work(work, framing)
            native = work.journal(framing, certificates=first)
            before = native.advance(JournalValidationBudget(rows=1))
            assert not before.complete
            binding = work.journal_source(framing).binding
            directory = work.metadata.custody.directory / "journals" / binding.directory_id
            inode = (directory / "snapshot.sqlite3").stat().st_ino
            worker_epoch = work.metadata.job.worker_epoch
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (work, framing):
            restored = prepared_for_owned_work(work, framing)
            assert work.metadata.job.worker_epoch == worker_epoch + 1
            assert first._manifest[0] == restored._manifest[0]
            assert first.binding == restored.binding
            assert not first.verify_current() and restored.verify_current()
            resumed = work.journal(framing, certificates=restored)
            assert resumed is not native
            assert resumed.progress().accepted_frames == before.accepted_frames
            assert (directory / "snapshot.sqlite3").stat().st_ino == inode
            finish_native(resumed)
            assert resumed.progress().complete

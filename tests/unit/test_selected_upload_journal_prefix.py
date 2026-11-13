from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import replace
from time import perf_counter, thread_time

import pytest
from riverhog_archive_contracts import (
    HistoryJournalAnchor,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
    MemberHistoryRoot,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceJournalChunkRecord as Chunk,
)
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.services import collection_uploads as service_owner
from riverhog_core.services import upload_journal_prefix as prefix_owner
from riverhog_core.services.upload_journal_result_receiving import UploadJournalResultResources
from riverhog_protocol import MemberHistoryBindingBatchDocument
from riverhog_protocol.errors import Conflict
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    JournalSummary,
    JournalValidationCapacity,
    JournalValidationSession,
    ProvenanceValidationError,
    append_assertions,
    assertion,
    create_journal,
    reference,
    validate_journal,
)
from riverhog_provenance import (
    history as history_owner,
)
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import delete, update

from tests.support.validation_workspace import leased_workspace
from tests.unit.test_archive_catalog_finalizer import _lose_commit_reply
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _bind_primary, _upload_provenance_journal
from tests.unit.test_finalization_claim_readiness import _seal
from tests.unit.test_upload_journal_result import JOURNAL, sealed


def _shared_source(text_octets: int, *, journal_id=None):
    observed = BoundedSourceObserver().observe(BytesSource(b"shared selected history"))
    graph = observed.graph_fragment()
    subject = reference(graph["artifacts"][0]["id"], "artifact")

    def documentary(text):
        return assertion(
            "extension",
            observed.observer_agent_id,
            subject=subject,
            property="urn:test:selected-prefix",
            value={"type": "text", "value": text},
        )

    graph["extensions"] = [documentary("x" * text_octets)]
    prefix = create_journal(
        graph, recorded_by_agent_id=observed.observer_agent_id, journal_id=journal_id
    )
    raw = append_assertions(
        prefix,
        {"extensions": [documentary("later unselected entry")]},
        recorded_by_agent_id=observed.observer_agent_id,
    )
    with validate_journal(raw) as summary:
        anchor = HistoryJournalAnchor.from_mapping(summary.anchor_at(0))
        assert anchor.prefix_bytes == len(prefix) < len(raw)
        assert anchor.to_mapping() != summary.anchor
    return raw, anchor


@contextmanager
def _shared_finalization(tmp_path, monkeypatch, *, members=4, fanout=4, text_octets=0):
    monkeypatch.setattr(service_owner, "_FINALIZATION_FILE_BATCH", 1)
    with _case(tmp_path, "segment", artifact_count=members) as case:
        primaries = [
            _bind_primary(case.service, case.collection_id, format(i + 1, "064x"), case.content)
            for i in range(members)
        ]
        raw, anchor = _shared_source(text_octets)
        _upload_provenance_journal(
            case.service,
            case.collection_id,
            anchor.journal_id,
            raw,
            hashlib.sha256(raw).hexdigest(),
        )
        for index, produced in enumerate(primaries):
            with MemberHistoryBuilder(
                artifact_id=produced.binding.artifact_id,
                bytes=len(case.content),
                sha256=hashlib.sha256(case.content).hexdigest(),
                primary=MemberHistoryPrimary.from_mapping(
                    {
                        "journal": produced.binding.journal.model_dump(mode="json"),
                        "delivery_association_id": produced.binding.delivery_association_id,
                    }
                ),
            ) as histories:
                if index < fanout:
                    histories.add_root(MemberHistoryRoot(anchor, "bound"))
                selected, descriptor = histories.seal()
                for authority in (descriptor.roots, descriptor.imports):
                    for page in histories.pages(authority):
                        case.service.stage_history_structure(
                            case.collection_id, page.to_json_bytes()
                        )
                case.service.stage_history_structure(case.collection_id, descriptor.to_json_bytes())
                case.service.bind_member_histories(
                    case.collection_id,
                    MemberHistoryBindingBatchDocument.model_validate(
                        {"bindings": [selected.to_mapping()]}
                    ),
                )
        _seal(case)
        claim = case.service._claim_due_finalization()
        assert claim is not None
        for _ in range(members + 2):
            if not case.service._stage_next_member_histories(case.collection_id):
                break
        else:
            pytest.fail("bounded member history staging did not terminate")
        case.claim, case.shared_anchor, case.shared_raw = claim, anchor, raw
        case.members, case.fanout = members, fanout
        case.primaries = primaries
        yield case


def _owner(case):
    return case.service._catalog_finalization_owner(case.claim, kind="provenance-closure")


def _state(case):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        assert upload is not None
        return (
            upload.provenance_validation_next_artifact_order,
            upload.provenance_closure_validated,
            upload.archive_root_receipt_json,
        )


def _finish(case):
    owners = []
    for _ in range(case.members + 2):
        owner = _owner(case)
        owners.append(owner)
        if not owner.advance():
            break
    else:
        pytest.fail("bounded selected history finalization did not terminate")
    assert _state(case) == (case.members, True, None)
    assert all(not owner.has_pending and not owner.result_resources.pending for owner in owners)
    return owners


@pytest.mark.parametrize("fanout", [1, 4])
@pytest.mark.parametrize("text_octets", [0, 4096])
@pytest.mark.growth_witness(
    "riverhog-selected-prefix-finalization-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
)
def test_complete_finalization_reuses_selected_semantics_independently_of_fanout_and_extent(
    tmp_path, monkeypatch, record_property, fanout, text_octets
):
    with _shared_finalization(
        tmp_path, monkeypatch, fanout=fanout, text_octets=text_octets
    ) as case:
        validations, selected, advances = [], [], []
        active_validation = []
        active_selected = []
        actual_advance = JournalValidationSession.advance
        actual_proof = prefix_owner.verify_selected_upload_journal_prefix
        actual_open = prefix_owner.open_upload_journal_result
        actual_read = service_owner._iter_upload_journal_chunks
        actual_anchor = JournalSummary.anchor_at

        def observe_advance(self, *args, **kwargs):
            advances.append(1)
            if active_validation:
                active_validation[-1]["native_advances"] += 1
            if active_selected:
                active_selected[-1]["native_advances"] += 1
            return actual_advance(self, *args, **kwargs)

        def observe_proof(*args, **kwargs):
            measured = {"opens": 0, "anchor_lookups": 0, "raw_octets": 0, "native_advances": 0}
            selected.append(measured)
            active_selected.append(measured)
            wall, cpu = perf_counter(), thread_time()
            try:
                return actual_proof(*args, **kwargs)
            finally:
                measured["elapsed_seconds"] = perf_counter() - wall
                measured["thread_cpu_seconds"] = thread_time() - cpu
                active_selected.pop()

        def observe_open(*args, **kwargs):
            if active_selected:
                active_selected[-1]["opens"] += 1
            return actual_open(*args, **kwargs)

        def observe_anchor(self, sequence):
            if active_selected:
                active_selected[-1]["anchor_lookups"] += 1
            return actual_anchor(self, sequence)

        def observe_read(*args, **kwargs):
            for chunk in actual_read(*args, **kwargs):
                if active_selected:
                    active_selected[-1]["raw_octets"] += len(chunk)
                yield chunk

        def observe_validation(actual, owner):
            def observed(chunks, **options):
                measured = {
                    "owner": owner,
                    "anchor": options.get("expected_anchor"),
                    "octets": 0,
                    "native_advances": 0,
                }
                validations.append(measured)

                def measured_chunks():
                    for chunk in chunks:
                        measured["octets"] += len(chunk)
                        yield chunk

                active_validation.append(measured)
                try:
                    result = actual(measured_chunks(), **options)
                    measured["journal_id"] = result.journal_id
                    return result
                finally:
                    active_validation.pop()

            return observed

        with monkeypatch.context() as patch:
            patch.setattr(JournalValidationSession, "advance", observe_advance)
            patch.setattr(JournalSummary, "anchor_at", observe_anchor)
            patch.setattr(prefix_owner, "verify_selected_upload_journal_prefix", observe_proof)
            patch.setattr(prefix_owner, "open_upload_journal_result", observe_open)
            patch.setattr(service_owner, "_iter_upload_journal_chunks", observe_read)
            for module, label in ((service_owner, "finalizer"), (history_owner, "closure")):
                patch.setattr(
                    module,
                    "validate_journal_chunks",
                    observe_validation(module.validate_journal_chunks, label),
                )
            owners = _finish(case)
        assert len(owners) == case.members + 2
        assert len(selected) == fanout
        for measured in selected:
            assert {
                name: measured[name]
                for name in ("opens", "anchor_lookups", "raw_octets", "native_advances")
            } == {"opens": 1, "anchor_lookups": 1, "raw_octets": 0, "native_advances": 0}
            assert measured["elapsed_seconds"] >= 0
            assert measured["thread_cpu_seconds"] >= 0
        shared = [row for row in validations if row["journal_id"] == case.shared_anchor.journal_id]
        assert len(shared) == 2
        assert [(row["owner"], row["anchor"], row["octets"]) for row in shared] == [
            ("closure", case.shared_anchor.to_mapping(), case.shared_anchor.prefix_bytes),
            ("finalizer", None, len(case.shared_raw)),
        ]
        assert all(row["native_advances"] > 0 for row in validations)
        assert sum(row["native_advances"] for row in validations) == len(advances)
        assert len(validations) == 3 * case.members + 2
        per_journal = {case.shared_anchor.journal_id: len(shared)}
        for primary in case.primaries:
            rows = [row for row in validations if row["journal_id"] == primary.journal_id]
            expected = primary.binding.journal.model_dump(mode="json")
            assert [(row["owner"], row["anchor"]) for row in rows] == [
                ("finalizer", expected),
                ("closure", expected),
                ("finalizer", None),
            ]
            per_journal[primary.journal_id] = len(rows)
        assert {row["journal_id"] for row in validations} == set(per_journal)
        record_property(
            "selected_prefix_profile",
            {
                "selected_proofs": selected,
                "per_journal_validation_calls": per_journal,
                "shared_validations": shared,
            },
        )
        record_property(
            "growth_observations",
            {
                "members": case.members,
                "fanout": fanout,
                "documentary_text_octets": text_octets,
                "shared_prefix_octets": case.shared_anchor.prefix_bytes,
                "shared_source_octets": len(case.shared_raw),
                "distinct_selected_anchors": 1,
                "complete_units": len(owners),
                "selected_proof_calls": len(selected),
                "selected_result_openings": sum(row["opens"] for row in selected),
                "selected_anchor_lookups": sum(row["anchor_lookups"] for row in selected),
                "selected_source_payload_octets": sum(row["raw_octets"] for row in selected),
                "selected_native_advances": sum(row["native_advances"] for row in selected),
                "native_validation_calls": len(validations),
                "unique_validated_journals": len(per_journal),
                "native_advances": len(advances),
                "validation_input_octets": sum(row["octets"] for row in validations),
                "shared_validation_calls": len(shared),
                "shared_validation_input_octets": sum(row["octets"] for row in shared),
            },
        )


def test_changed_raw_selected_bytes_cannot_cross_the_complete_closure_barrier(
    tmp_path, monkeypatch
):
    with _shared_finalization(tmp_path, monkeypatch, members=2, fanout=2, text_octets=32) as case:
        assert _owner(case).advance()

        def source_metadata(session):
            journal = session.get(Journal, (case.collection_id, case.shared_anchor.journal_id))
            return tuple(getattr(journal, column.key) for column in Journal.__table__.columns)

        with session_scope(case.factory) as session:
            before = source_metadata(session)
            chunk = session.get(Chunk, (case.collection_id, case.shared_anchor.journal_id, 0))
            original = bytes(chunk.content)
            changed = original.replace(b"x" * 32, b"y" + b"x" * 31, 1)
            assert changed != original and len(changed) == len(original)
            chunk.content = changed
        with session_scope(case.factory) as session:
            assert source_metadata(session) == before
        with validate_journal(changed[: case.shared_anchor.prefix_bytes]) as valid:
            assert valid.journal_id == case.shared_anchor.journal_id
            assert valid.journal_bytes == case.shared_anchor.prefix_bytes
            assert valid.anchor != case.shared_anchor.to_mapping()
        # Frozen semantic evidence does not authorize current raw archive bytes.
        assert _owner(case).advance()
        assert _state(case) == (2, False, None)
        with pytest.raises(
            ProvenanceValidationError, match="externally supplied prefix anchor mismatch"
        ):
            _owner(case).advance()
        assert _state(case) == (2, False, None)
        with session_scope(case.factory) as session:
            session.get(
                Chunk, (case.collection_id, case.shared_anchor.journal_id, 0)
            ).content = original
        _finish(case)


def test_selected_proof_reopens_after_service_restart_without_revalidation(tmp_path, monkeypatch):
    with _shared_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        assert _owner(case).advance()
        previous = case.service
        previous.close()
        current = service_owner.SqlAlchemyCollectionUploadService(
            previous._config,
            previous._archive_stores,
            policy=previous._policy,
            session_factory=case.factory,
        )
        case.service = current
        actual_proof = prefix_owner.verify_selected_upload_journal_prefix
        calls = []

        def forbid_replay(*args, **kwargs):
            pytest.fail("reopened selected proof repeated Native semantic validation")

        def observed(*args, **kwargs):
            calls.append(1)
            with monkeypatch.context() as patch:
                patch.setattr(JournalValidationSession, "advance", forbid_replay)
                return actual_proof(*args, **kwargs)

        try:
            with monkeypatch.context() as patch:
                patch.setattr(prefix_owner, "verify_selected_upload_journal_prefix", observed)
                _finish(case)
            assert calls == [1]
        finally:
            current.close()


@pytest.mark.parametrize("committed", [False, True])
def test_selected_proof_keeps_the_original_catalog_commit_outcome(tmp_path, monkeypatch, committed):
    with _shared_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        owner = _owner(case)
        actual = prefix_owner.open_upload_journal_result
        openings = []

        def observed(*args, **kwargs):
            openings.append(kwargs["receiver"])
            return actual(*args, **kwargs)

        monkeypatch.setattr(prefix_owner, "open_upload_journal_result", observed)
        with _lose_commit_reply(monkeypatch, committed=committed):
            with pytest.raises(OSError, match="catalog commit acknowledgement"):
                owner.advance()
        assert owner.has_pending and len(openings) == 1
        assert _state(case) == (int(committed), False, None)
        assert owner.advance()
        assert len(openings) == (1 if committed else 2)
        assert not owner.has_pending and not owner.result_resources.pending
        assert _state(case) == (1, False, None)
        _finish(case)


def _verify(session, root, anchor, resources, *, catalog=None):
    prefix_owner.verify_selected_upload_journal_prefix(
        session,
        root,
        1,
        anchor,
        catalog=service_owner.admission_provenance_catalog() if catalog is None else catalog,
        result_resources=resources,
    )


@pytest.mark.parametrize("fault", ["capture-attempt", "capture-version", "missing-job", "catalog"])
def test_selected_proof_rejects_changed_current_authority(
    tmp_path, fault, *, journal_constructions
):
    raw, anchor = _shared_source(0, journal_id=JOURNAL)
    with sealed(tmp_path, raw=raw, journal_constructions=journal_constructions) as (
        _,
        factory,
        root,
        job,
        _,
    ):
        resources = UploadJournalResultResources(journal_constructions.retain)
        try:
            with factory() as session, session.begin():
                _verify(session, root, anchor, resources)
            with factory() as session, session.begin():
                if fault == "missing-job":
                    session.execute(delete(Job).where(Job.workspace_id == job.workspace_id))
                elif fault != "catalog":
                    values = (
                        {"capture_attempt_id": "replacement"}
                        if fault == "capture-attempt"
                        else {"capture_version": 0}
                    )
                    session.execute(
                        update(Journal).where(Journal.collection_id == 1).values(**values)
                    )
            with factory() as session, session.begin():
                with pytest.raises(LostJournalValidationWorkspace):
                    _verify(
                        session,
                        root,
                        anchor,
                        resources,
                        catalog=ContractCatalog(()) if fault == "catalog" else None,
                    )
            resources.close()
            assert not resources.pending
        finally:
            resources.close()


def test_selected_prefix_identity_is_exact_and_capacity_retry_does_not_replay(
    tmp_path, monkeypatch, *, journal_constructions
):
    raw, anchor = _shared_source(0, journal_id=JOURNAL)
    with sealed(tmp_path, raw=raw, journal_constructions=journal_constructions) as (
        _,
        factory,
        root,
        job,
        _,
    ):
        resources = UploadJournalResultResources(journal_constructions.retain)

        def forbidden(*args, **kwargs):
            pytest.fail("selected frozen prefix started semantic validation")

        monkeypatch.setattr(JournalValidationSession, "advance", forbidden)
        try:
            with leased_workspace(root, job.workspace_id):
                with factory() as session, session.begin():
                    with pytest.raises(JournalValidationCapacity):
                        _verify(session, root, anchor, resources)
            resources.close()
            assert not resources.pending
            with factory() as session, session.begin():
                _verify(session, root, anchor, resources)
                with pytest.raises(Conflict):
                    _verify(session, root, replace(anchor, prefix_sha256="0" * 64), resources)
            assert not resources.pending
        finally:
            resources.close()


@pytest.mark.parametrize("after", [False, True])
def test_selected_proof_retains_uncertain_original_native_receiving(
    tmp_path, monkeypatch, after, *, journal_constructions
):
    raw, anchor = _shared_source(0, journal_id=JOURNAL)
    with sealed(tmp_path, raw=raw, journal_constructions=journal_constructions) as (
        _,
        factory,
        root,
        job,
        _,
    ):
        resources = UploadJournalResultResources(journal_constructions.retain)
        actual = prefix_owner.open_upload_journal_result
        originals, callbacks = [], []

        def observed(*args, **kwargs):
            result = actual(*args, **kwargs)
            original = kwargs["receiver"]
            originals.append(original)
            receive = result._store._storage.on_close

            def uncertain():
                callbacks.append(1)
                if len(callbacks) == 1 and not after:
                    raise InterruptedError("original selected proof close is unconfirmed")
                receive()
                if len(callbacks) == 1:
                    raise InterruptedError("original selected proof close reply was lost")

            result._store._storage.on_close = uncertain
            return result

        monkeypatch.setattr(prefix_owner, "open_upload_journal_result", observed)
        try:
            with factory() as session, session.begin():
                with pytest.raises(InterruptedError, match="original selected proof close"):
                    _verify(session, root, anchor, resources)
                assert resources.pending and len(originals) == len(callbacks) == 1
                original = originals[0]
                assert original.lease.received is after
                resources.close()
                assert len(originals) == 1 and len(callbacks) == 2
                assert not resources.pending and original.lease.received
                assert original.result is None and original.construction.received
            with leased_workspace(root, job.workspace_id) as free:
                assert free is not None
        finally:
            resources.close()

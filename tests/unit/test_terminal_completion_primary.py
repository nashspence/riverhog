import copy
import hashlib
import time

import pytest
from riverhog_archive_contracts import (
    HistoryJournalAnchor,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
    MemberHistoryRoot,
)
from riverhog_canonical_json import canonical_json_bytes, canonical_json_sha256
from riverhog_client.canonical_completion import CompletionRecord, build_completion_journal
from riverhog_client.canonical_production import (
    ProducerAttribution,
    build_member_journal,
    member_materialization_decision,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadArtifactProvenanceBindingRecord,
    CollectionUploadRecord,
)
from riverhog_core.catalog_workflow_models import (
    CollectionProcessingClaimRecord,
    CollectionProcessingDispositionSetRecord,
)
from riverhog_core.services import collection_uploads as owner
from riverhog_core.services import upload_terminal_primary as terminal_primary_owner
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingBatchDocument,
    MemberHistoryBindingBatchDocument,
)
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionPublicationReceiptDocument,
    CollectionCompletionRecordingRequestDocument,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.collection_production_provenance import COLLECTION_PRODUCTION_CONTRACT_ID
from riverhog_protocol.collection_record_preimages import (
    canonical_record_sequence,
    completion_record_inventory_sha256,
)
from riverhog_protocol.collection_upload_transport import (
    CollectionUploadProvenanceJournalCreateDocument,
)
from riverhog_protocol.collection_workflows import (
    ArtifactDisposition,
    ArtifactDispositionOutput,
    ArtifactDispositionSetIdentity,
)
from riverhog_protocol.errors import Conflict
from riverhog_protocol.provenance_transport import CollectionArtifactProvenanceBindingDocument
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    append_assertions,
    external_reference,
    software_agent_id,
    validate_journal,
)
from riverhog_provenance.history import MemberHistoryClosure
from sqlalchemy import select

from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _upload_provenance_journal
from tests.unit.test_finalization_claim_readiness import _seal
from tests.unit.test_imported_history_growth import _measure
from tests.unit.test_shared_primary_admission import (
    _finish,
    _next_member,
    _reclaim_primary,
    _runtime_work,
)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def primary(case, requirement, members, text_octets):
    opened = case.service.get(case.collection_id)
    catalog = owner.admission_provenance_catalog()
    context = opened["delivery_context_id"]
    known = set()
    combined = None
    associations = []
    for index in range(members):
        produced = build_member_journal(
            member=ArtifactMemberIdentityDocument(
                artifact_id=format(index + 1, "064x"),
                bytes=str(len(case.content)),
                sha256=digest(case.content),
            ),
            observation=BoundedSourceObserver().observe(BytesSource(case.content)),
            delivery_context_id=context,
            attribution=ProducerAttribution(
                "completion-fixture",
                "bounded-bytes",
                "1",
                str(case.collection_id),
                "fixture",
                {"documentary": "x" * text_octets if index == 0 else ""},
                opened["construction_identity_sha256"],
                completion_requirement=requirement,
            ),
            output_id=f"output-{index + 1}",
            materialization_hint=None,
        )
        associations.append(produced.binding.delivery_association_id)
        with validate_journal(produced.content, catalog=catalog) as summary:
            graph = copy.deepcopy(summary.graph)
        if combined is None:
            combined = produced.content
            shared_journal = produced.journal_id
            known = {row["id"] for rows in graph.values() for row in rows}
            continue
        added = {}
        for category, rows in graph.items():
            selected = []
            for row in rows:
                if row["id"] in known:
                    continue
                if (
                    category == "extensions"
                    and row["property"] != COLLECTION_PRODUCTION_CONTRACT_ID + "/execution-output"
                ):
                    continue
                if row.get("type") == "journal_subject":
                    row["journal_id"] = shared_journal
                selected.append(row)
                known.add(row["id"])
            if selected:
                added[category] = selected
        combined = append_assertions(
            combined,
            added,
            recorded_by_agent_id=software_agent_id("completion-fixture", "1"),
            catalog=catalog,
        )
    with validate_journal(combined, catalog=catalog) as summary:
        anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
        states = [
            external_reference(
                summary, summary.graph_validation.objects[association]["state"]["object_id"]
            )
            for association in associations
        ]
    bindings = [
        CollectionArtifactProvenanceBindingDocument.model_validate(
            {
                "artifact_id": format(i + 1, "064x"),
                "journal": anchor.to_mapping(),
                "delivery_association_id": association,
            }
        )
        for i, association in enumerate(associations)
    ]
    return combined, anchor, bindings, states


def completion(case, requirement, bindings, states, controller):
    opened = case.service.get(case.collection_id)
    disposition = ArtifactDisposition(1, "a" * 64, "b" * 64, "transformed")
    edges = [
        ArtifactDispositionOutput(1, "a" * 64, "b" * 64, binding.artifact_id)
        for binding in bindings
    ]
    identity = ArtifactDispositionSetIdentity(
        1,
        len(edges),
        len(edges),
        canonical_json_sha256(
            {
                "format": "riverhog-artifact-disposition-set/v1",
                "disposition_count": "1",
                "dispositions_sha256": digest(canonical_json_bytes(disposition.as_dict()) + b"\n"),
                "output_edge_count": str(len(edges)),
                "output_artifact_count": str(len(edges)),
                "outputs_sha256": digest(
                    b"".join(canonical_json_bytes(edge.as_dict()) + b"\n" for edge in edges)
                ),
            }
        ),
    )
    outputs = []
    imports = []
    for index, (binding, state) in enumerate(zip(bindings, states, strict=True)):
        selected = MemberHistoryPrimary.from_mapping(
            {
                "journal": binding.journal.model_dump(mode="json"),
                "delivery_association_id": binding.delivery_association_id,
            }
        )
        with MemberHistoryBuilder(
            artifact_id=binding.artifact_id,
            bytes=len(case.content),
            sha256=digest(case.content),
            primary=selected,
        ) as histories:
            _, descriptor = histories.seal()
            imports.append(
                {
                    "artifact_id": str(binding.artifact_id),
                    "imports": descriptor.imports.to_mapping(),
                }
            )
        outputs.append(
            {
                "output_id": f"output-{index + 1}",
                "artifact_id": str(binding.artifact_id),
                "bytes": str(len(case.content)),
                "sha256": digest(case.content),
                "primary": selected.to_mapping(),
                "state": state,
            }
        )
    construction = {
        "format": "riverhog-execution-construction/v1",
        "collection_id": str(case.collection_id),
        "construction_identity_sha256": opened["construction_identity_sha256"],
        "delivery_context_id": opened["delivery_context_id"],
        "completion_requirement": requirement.model_dump(mode="json"),
    }
    preimages = {kind: b"{}" for kind in requirement.record_kinds}
    preimages.update(
        {
            "accepted-construction": canonical_json_bytes(construction),
            "controller": controller,
            "output-bindings": b"".join(canonical_record_sequence(outputs)),
            "input-history-bindings": b"".join(canonical_record_sequence(imports)),
            "disposition-identity": canonical_json_bytes(identity.as_dict()),
            "disposition-pages": b"".join(
                canonical_record_sequence(
                    (
                        {
                            "kind": "dispositions",
                            "page": {
                                "identity": identity.as_dict(),
                                "start_ordinal": "0",
                                "next_ordinal": None,
                                "dispositions": [disposition.as_dict()],
                            },
                        },
                        {
                            "kind": "outputs",
                            "page": {
                                "identity": identity.as_dict(),
                                "start_ordinal": "0",
                                "next_ordinal": None,
                                "outputs": [edge.as_dict() for edge in edges],
                            },
                        },
                    )
                )
            ),
        }
    )
    inventory = completion_record_inventory_sha256(
        (kind, digest(raw), len(raw)) for kind, raw in sorted(preimages.items())
    )
    recording = case.service.reserve_completion_recording(
        case.collection_id,
        CollectionCompletionRecordingRequestDocument(
            requirement_sha256=requirement.identity, records_sha256=inventory
        ),
    )
    records = [
        CompletionRecord(kind, len(raw), digest(raw), lambda content=raw: (content,))
        for kind, raw in sorted(preimages.items())
    ]
    raw = build_completion_journal(
        requirement=requirement,
        records=records,
        execution_sha256=digest(preimages["target-execution"]),
        output_bindings_sha256=digest(preimages["output-bindings"]),
        input_history_bindings_sha256=digest(preimages["input-history-bindings"]),
        disposition_set_sha256=identity.sha256,
        producer_app="completion-fixture",
        producer_version="1",
        journal_id=recording.journal_id,
        recorded_at=recording.recorded_at,
    )
    now = "2026-01-01T00:00:00Z"
    claim_id = "c" * 64
    with session_scope(case.factory) as session:
        session.add(
            CollectionProcessingClaimRecord(
                id=claim_id,
                work_id="d" * 64,
                consumer_app="completion-fixture",
                purpose="fixture",
                work_document_json="{}",
                work_document_sha256=digest(b"{}"),
                execution_id=requirement.execution_id,
                controller_evidence_sha256=requirement.controller_evidence_sha256,
                state="active",
                fence=1,
                expires_at="2099-01-01T00:00:00Z",
                created_at=now,
                updated_at=now,
            )
        )
        session.flush()
        session.add(
            CollectionProcessingDispositionSetRecord(
                claim_id=claim_id,
                state="sealed",
                disposition_count=1,
                output_edge_count=len(edges),
                output_artifact_count=len(edges),
                identity_sha256=identity.sha256,
                created_at=now,
                updated_at=now,
                sealed_at=now,
            )
        )
    authority = CollectionUploadProvenanceJournalCreateDocument(
        bytes=str(len(raw)), sha256=digest(raw), selection_role="completion"
    )
    case.service.create_provenance_journal(case.collection_id, recording.journal_id, authority)
    case.service.append_provenance_journal(
        case.collection_id, recording.journal_id, offset=0, content=raw
    )
    case.service.seal_provenance_journal(case.collection_id, recording.journal_id)
    for _ in range(10000):
        status = case.service.get_provenance_journal(case.collection_id, recording.journal_id)
        if status["state"] in {"sealed", "failed"}:
            assert status["state"] == "sealed", status["failure"]
            break
        assert case.service.process_due_provenance_journal_validations() == 1
    else:
        raise AssertionError("small completion did not validate")
    with validate_journal(raw, catalog=owner.admission_provenance_catalog()) as summary:
        anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
    return anchor, inventory


def _exercise_completion(
    tmp_path, monkeypatch, record_property, members, text_octets, *, substitute_primary=False
):
    from tests.support.runtime_config import write_test_runtime_config

    monkeypatch.setenv("RIVERHOG_PROVENANCE_STATE_HOME", str(tmp_path / "user-state"))
    monkeypatch.setenv("RIVERHOG_CONFIG", str(write_test_runtime_config(tmp_path)))
    monkeypatch.setattr(owner, "_FINALIZATION_FILE_BATCH", 1)
    started = (time.perf_counter(), time.process_time())

    def elapsed(start):
        return {
            "wall_seconds": time.perf_counter() - start[0],
            "process_cpu_seconds": time.process_time() - start[1],
        }

    controller = b'{"controller":"completion-fixture"}'
    requirement = CollectionCompletionRequirementDocument(
        execution_id="e" * 64,
        execution_envelope_sha256="f" * 64,
        controller_evidence_sha256=digest(controller),
        record_kinds=COMPLETION_REQUIRED_RECORD_KINDS,
    )
    with (
        _runtime_work(monkeypatch) as work,
        _case(tmp_path, "segment", artifact_count=members) as case,
    ):
        case.service.set_completion_requirement(case.collection_id, requirement)
        raw, anchor, bindings, states = primary(case, requirement, members, text_octets)
        _upload_provenance_journal(
            case.service, case.collection_id, anchor.journal_id, raw, digest(raw)
        )
        completion_anchor, inventory = completion(case, requirement, bindings, states, controller)
        for binding in bindings:
            case.service.bind_artifact_provenance(
                case.collection_id,
                CollectionArtifactProvenanceBindingBatchDocument(bindings=[binding]),
            )
            case.service.set_artifact_materialization_decisions(
                case.collection_id,
                member_materialization_decision(
                    artifact_id=binding.artifact_id,
                    materialization_hint=None,
                    allow_missing_materialization_hint=True,
                ),
            )
            primary_binding = MemberHistoryPrimary.from_mapping(
                {
                    "journal": binding.journal.model_dump(mode="json"),
                    "delivery_association_id": binding.delivery_association_id,
                }
            )
            with MemberHistoryBuilder(
                artifact_id=binding.artifact_id,
                bytes=len(case.content),
                sha256=digest(case.content),
                primary=primary_binding,
            ) as histories:
                histories.add_root(MemberHistoryRoot(completion_anchor, "bound"))
                selected, descriptor = histories.seal()
                for authority in (descriptor.roots, descriptor.imports):
                    for page in histories.pages(authority):
                        case.service.stage_history_structure(
                            case.collection_id, page.to_json_bytes()
                        )
                case.service.stage_history_structure(case.collection_id, descriptor.to_json_bytes())
                case.service.set_member_history_inputs(
                    case.collection_id, binding.artifact_id, descriptor.imports
                )
                case.service.bind_member_histories(
                    case.collection_id,
                    MemberHistoryBindingBatchDocument.model_validate(
                        {"bindings": [selected.to_mapping()]}
                    ),
                )
        _seal(case)
        case.claim = case.service._claim_due_finalization()
        assert case.claim is not None
        case.members = members
        for _ in range(members + 2):
            if not case.service._stage_next_member_histories(case.collection_id):
                break
        else:
            raise AssertionError("small completion history staging did not terminate")
        spans = {"fixture_preparation": elapsed(started)}
        phase_start = (time.perf_counter(), time.process_time())
        work.phase = "member"
        for _ in range(members):
            _next_member(case)
        spans["member_preparation"] = elapsed(phase_start)
        actual_completion = owner._validate_staged_execution_completion
        actual_summary = MemberHistoryClosure.summary_at
        summary_calls = []
        intervals = []
        completion_thread_cpu = []
        completion_payload_octets = {}
        samples = []
        actual_point = terminal_primary_owner.selected_upload_terminal_primary_state

        def measured_point(session, collection_id, source, *, catalog, corpus):
            if work.phase != "completion":
                return actual_point(session, collection_id, source, catalog=catalog, corpus=corpus)
            result, sample, _metadata = _measure(
                session, corpus, source, catalog, monkeypatch, collection_id=collection_id
            )
            samples.append(sample)
            return result

        actual_chunks = owner._iter_upload_journal_chunks

        def completion_chunks(session, record, *args, **kwargs):
            for content in actual_chunks(session, record, *args, **kwargs):
                if work.phase == "completion":
                    completion_payload_octets[record.journal_id] = completion_payload_octets.get(
                        record.journal_id, 0
                    ) + len(content)
                yield content

        def observed_summary(self, selected):
            if work.phase == "completion":
                assert selected.journal_id != anchor.journal_id, (
                    "completion output replayed its already validated primary"
                )
                summary_calls.append(
                    {"journal_id": selected.journal_id, "prefix_bytes": selected.prefix_bytes}
                )
            return actual_summary(self, selected)

        def observed_completion(*args, **kwargs):
            previous = work.phase
            work.phase = "completion"
            start = time.monotonic()
            cpu_start = time.thread_time()
            try:
                if substitute_primary:
                    session = args[0]
                    binding = session.get(
                        CollectionUploadArtifactProvenanceBindingRecord,
                        (case.collection_id, str(bindings[0].artifact_id)),
                    )
                    binding.delivery_association_id = (
                        "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"
                    )
                    session.flush()
                return actual_completion(*args, **kwargs)
            finally:
                intervals.append(time.monotonic() - start)
                completion_thread_cpu.append(time.thread_time() - cpu_start)
                work.phase = previous

        monkeypatch.setattr(MemberHistoryClosure, "summary_at", observed_summary)
        monkeypatch.setattr(owner, "_validate_staged_execution_completion", observed_completion)
        monkeypatch.setattr(owner, "_iter_upload_journal_chunks", completion_chunks)
        monkeypatch.setattr(
            terminal_primary_owner, "selected_upload_terminal_primary_state", measured_point
        )
        phase_start = (time.perf_counter(), time.process_time())
        work.phase = "terminal"
        if substitute_primary:
            with pytest.raises(Conflict, match="current member primary"):
                _finish(case)
            with session_scope(case.factory) as session:
                upload = session.get(CollectionUploadRecord, case.collection_id)
                assert not upload.provenance_closure_validated
                assert upload.completion_receipt_json is None
            _reclaim_primary(case)
            return
        _finish(case)
        spans["terminal_inclusive"] = elapsed(phase_start)
        with session_scope(case.factory) as session:
            upload = session.get(CollectionUploadRecord, case.collection_id)
            receipt = CollectionCompletionPublicationReceiptDocument.model_validate_json(
                upload.completion_receipt_json
            )
            assert (
                receipt.requirement_sha256 == requirement.identity
                and receipt.records_sha256 == inventory
            )
            output_ids = list(
                session.scalars(
                    select(CollectionUploadArtifactProvenanceBindingRecord.completion_output_id)
                    .where(
                        CollectionUploadArtifactProvenanceBindingRecord.collection_id
                        == case.collection_id
                    )
                    .order_by(CollectionUploadArtifactProvenanceBindingRecord.artifact_id)
                )
            )
            assert output_ids == [f"output-{i + 1}" for i in range(members)]
        primary_calls = [v for v in summary_calls if v["journal_id"] == anchor.journal_id]
        assert primary_calls == [] and len(samples) == members
        assert completion_payload_octets.get(anchor.journal_id, 0) == 0
        assert len(summary_calls) == 1
        record_property("growth_samples", samples)
        result = {
            "members": members,
            "text_octets": text_octets,
            "primary_bytes": len(raw),
            "primary_summary_calls": len(primary_calls),
            "completion_summary_calls": len(summary_calls) - len(primary_calls),
            "completion_intervals": intervals,
            "completion_thread_cpu_seconds": completion_thread_cpu,
            "completion_primary_payload_octets": completion_payload_octets.get(
                anchor.journal_id, 0
            ),
            "phase_spans": spans,
            "phase_work": {p: dict(v) for p, v in work.metrics.items()},
            "actual_receipt_validated": True,
            "accepted_claim_and_disposition_fixture_seeded": True,
            "full_claim_HTTP_lifecycle_qualified": False,
            "primary_workspace_received": True,
        }
        phase_start = (time.perf_counter(), time.process_time())
        work.phase = "primary_reclamation"
        _reclaim_primary(case)
        spans["primary_reclamation"] = elapsed(phase_start)
        assert not case.service._primary_facts.pending
    spans["whole_fixture_inclusive"] = elapsed(started)
    record_property("completion_fixture", result)


@pytest.mark.growth_witness(
    "riverhog-terminal-completion-primary-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_terminal_primary.selected_upload_terminal_primary_state",
)
@pytest.mark.parametrize(
    "members,text_octets",
    [(1, 0), (4, 0), (1, 4096), (4, 4096)],
    ids=["n1-base", "n4-base", "n1-producer", "n4-producer"],
)
def test_required_completion_projects_exact_outputs_without_shared_primary_replay(
    tmp_path, monkeypatch, record_property, members, text_octets
):
    _exercise_completion(tmp_path, monkeypatch, record_property, members, text_octets)


def test_required_completion_rejects_changed_current_member_primary(
    tmp_path, monkeypatch, record_property
):
    _exercise_completion(tmp_path, monkeypatch, record_property, 1, 0, substitute_primary=True)

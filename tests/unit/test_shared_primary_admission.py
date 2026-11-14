from __future__ import annotations

import hashlib
from collections import defaultdict
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import (
    HistoryJournalAnchor,
    MemberHistoryBuilder,
    MemberHistoryPrimary,
    MemberHistoryRoot,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client.canonical_production import (
    ProducerAttribution,
    build_member_journal,
    member_materialization_decision,
)
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_journal_validation_models import JournalValidationJobRecord as Job
from riverhog_core.catalog_models import (
    CollectionUploadArtifactMaterializationDecisionRecord as Decision,
)
from riverhog_core.catalog_models import CollectionUploadArtifactProvenanceBindingRecord as Binding
from riverhog_core.catalog_models import CollectionUploadArtifactRecord as Artifact
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalChunkRecord as Chunk
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_primary_facts_models import PrimaryFactsJobRecord as PrimaryJob
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.services import collection_uploads as service_owner
from riverhog_core.services import primary_facts_jobs
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingBatchDocument,
    MemberHistoryBindingBatchDocument,
)
from riverhog_protocol.collection_completion import (
    COMPLETION_REQUIRED_RECORD_KINDS,
    CollectionCompletionRequirementDocument,
)
from riverhog_protocol.collection_production_provenance import COLLECTION_PRODUCTION_CONTRACT_ID
from riverhog_protocol.errors import Conflict
from riverhog_protocol.provenance_transport import CollectionArtifactProvenanceBindingDocument
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    CanonicalCorpusValidator,
    JournalValidationCapacity,
    JournalValidationSession,
    ObservationResult,
    ProvenanceValidationError,
    append_assertions,
    append_checkpoint,
    append_correction,
    assertion,
    assertion_reference,
    journal_store,
    software_agent_id,
    validate_journal,
)
from riverhog_provenance import history as history_owner
from riverhog_provenance.corpus_selection import HistoricalSelection
from sqlalchemy import event, select
from workspace_reclamation import FileReclamationOwner

from tests.support.catalog_work import octets
from tests.support.validation_workspace import leased_workspace
from tests.unit.test_archive_catalog_finalizer import _lose_commit_reply
from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _upload_provenance_journal
from tests.unit.test_finalization_claim_readiness import _seal
from tests.unit.test_selected_upload_journal_prefix import _owner, _state


def _member_journal(case, index, *, text_octets=0, opaque_octets=0, hint=None):
    opened = case.service.get(case.collection_id)
    observation = BoundedSourceObserver().observe(BytesSource(case.content))
    if opaque_octets:
        graph = observation.graph_fragment()
        occurrence = next(
            row for row in graph["occurrences"] if row["id"] == observation.occurrence_id
        )
        occurrence["identifiers"] = [
            {
                "scheme": "urn:test:opaque-documentary",
                "value": {"kind": "text", "text": "x" * opaque_octets},
                "scope": "global",
            }
        ]
        selected = next(
            row for row in graph["descriptions"] if row["id"] == observation.observation_id
        )
        selected["note"] = "x" * opaque_octets
        observation = ObservationResult.from_graph(
            graph,
            observation_id=observation.observation_id,
            observer_agent_id=observation.observer_agent_id,
            catalog=observation.catalog,
        )
    return build_member_journal(
        member=ArtifactMemberIdentityDocument(
            artifact_id=format(index + 1, "064x"),
            bytes=str(len(case.content)),
            sha256=hashlib.sha256(case.content).hexdigest(),
        ),
        observation=observation,
        delivery_context_id=opened["delivery_context_id"],
        attribution=ProducerAttribution(
            producer_app="shared-primary-test",
            adapter_id="bounded-bytes-test",
            adapter_version="1",
            source_event_id=str(case.collection_id),
            ingest_source="fixture",
            source_context={"documentary": "x" * text_octets},
            construction_identity=opened["construction_identity_sha256"],
        ),
        materialization_hint=hint,
    )


def _shared_material(
    case,
    *,
    members,
    text_octets,
    opaque_octets=0,
    historical=False,
    hint=None,
    record_fault=None,
    anchor_steps=False,
    late_record_fault=False,
):
    opened = case.service.get(case.collection_id)
    who = software_agent_id("shared-primary-test", "1")
    catalog = service_owner.admission_provenance_catalog()
    produced = _member_journal(
        case, 0, text_octets=text_octets, opaque_octets=opaque_octets, hint=hint
    )
    with validate_journal(produced.content, catalog=catalog) as summary:
        original = summary.graph_validation.objects[produced.binding.delivery_association_id]
    associations = [original]
    for index in range(1, members):
        associations.append(
            assertion(
                "delivery_association",
                who,
                delivery_context_id=opened["delivery_context_id"],
                slot={"kind": "text", "text": format(index + 1, "064x")},
                role=original["role"],
                state=original["state"],
                verification_observation_id=original["verification_observation_id"],
                evidence_items=original["evidence"],
            )
        )
    prefix = produced.content
    if len(associations) > 1:
        prefix = append_assertions(
            prefix,
            {"delivery_associations": associations[1:]},
            recorded_by_agent_id=who,
            catalog=catalog,
        )
    if record_fault is not None:
        with validate_journal(prefix, catalog=catalog) as summary:
            fragment = next(
                row
                for row in summary.graph_validation.view["extensions"]
                if row["property"] == COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment"
            )
            fragment_ref = assertion_reference(summary, fragment["assertion_id"])
        if record_fault == "missing-fragment":
            replacements = {}
        else:
            assert record_fault == "altered-fragment"
            profile = fragment["value"]["value"]
            encoded = profile["data"]["data_base64"]
            changed = ("A" if encoded[0] != "A" else "B") + encoded[1:]
            replacements = {
                "extensions": [
                    assertion(
                        "extension",
                        who,
                        subject=fragment["subject"],
                        property=fragment["property"],
                        value={
                            "type": "json",
                            "value": {
                                "profile": profile["profile"],
                                "data": {**profile["data"], "data_base64": changed},
                            },
                        },
                        evidence_items=fragment["evidence"],
                    )
                ]
            }
        prefix = append_correction(
            prefix,
            [fragment_ref],
            assertions=replacements or None,
            reason="selected production record negative fixture",
            recorded_by_agent_id=who,
            catalog=catalog,
        )
    with validate_journal(prefix, catalog=catalog) as summary:
        anchor = HistoryJournalAnchor.from_mapping(summary.anchor)
        target = assertion_reference(summary, original["assertion_id"])
        fragment_target = None
        if late_record_fault:
            fragment = next(
                row
                for row in summary.graph_validation.view["extensions"]
                if row["property"] == COLLECTION_PRODUCTION_CONTRACT_ID + "/record-fragment"
            )
            fragment_target = assertion_reference(summary, fragment["assertion_id"])
    raw = prefix
    selections = [anchor]
    if anchor_steps:
        for index in range(1, members):
            raw = append_checkpoint(
                raw,
                purpose=f"urn:test:unrelated-checkpoint:{index}",
                recorded_by_agent_id=who,
                catalog=catalog,
            )
            with validate_journal(raw, catalog=catalog) as summary:
                selections.append(HistoryJournalAnchor.from_mapping(summary.anchor))
    if historical:
        raw = append_correction(
            prefix,
            [target],
            reason="delivery withdrawn after the exact member selection",
            recorded_by_agent_id=who,
            catalog=catalog,
        )
    if late_record_fault:
        assert not historical and not anchor_steps and fragment_target is not None
        raw = append_correction(
            raw,
            [fragment_target],
            reason="required producer fragment withdrawn after one selected primary",
            recorded_by_agent_id=who,
            catalog=catalog,
        )
    with validate_journal(raw, catalog=catalog) as summary:
        terminal = HistoryJournalAnchor.from_mapping(summary.anchor)
        assert (original["id"] not in summary.graph_validation.objects) is historical
    if late_record_fault:
        selections = [anchor, *([terminal] * (members - 1))]
    bindings = [
        CollectionArtifactProvenanceBindingDocument.model_validate(
            {
                "artifact_id": format(index + 1, "064x"),
                "journal": selections[min(index, len(selections) - 1)].to_mapping(),
                "delivery_association_id": association["id"],
            }
        )
        for index, association in enumerate(associations)
    ]
    return SimpleNamespace(
        raw=raw,
        prefix=prefix,
        anchor=anchor,
        terminal=terminal,
        selections=selections,
        bindings=bindings,
        hint=hint,
        source_context_octets=len(b'{"documentary":"' + b"x" * text_octets + b'"}'),
    )


@contextmanager
def shared_primary_finalization(
    tmp_path,
    monkeypatch,
    *,
    members=4,
    fanout=4,
    text_octets=0,
    opaque_octets=0,
    historical=False,
    hint=None,
    record_fault=None,
    anchor_steps=False,
    late_record_fault=False,
    work=None,
    existing_case=None,
):
    monkeypatch.setattr(service_owner, "_FINALIZATION_FILE_BATCH", 1)
    context = (
        _case(tmp_path, "segment", artifact_count=members)
        if existing_case is None
        else nullcontext(existing_case)
    )
    with context as case:
        material = _shared_material(
            case,
            members=members,
            text_octets=text_octets,
            opaque_octets=opaque_octets,
            historical=historical,
            hint=hint,
            record_fault=record_fault,
            anchor_steps=anchor_steps,
            late_record_fault=late_record_fault,
        )
        independent = {index: _member_journal(case, index) for index in range(fanout, members)}
        if work is not None:
            work.phase = "admission"
        _upload_provenance_journal(
            case.service,
            case.collection_id,
            material.anchor.journal_id,
            material.raw,
            hashlib.sha256(material.raw).hexdigest(),
        )
        primaries = []
        for index in range(members):
            if index < fanout:
                binding = material.bindings[index]
                selected_hint = hint
            else:
                produced = independent[index]
                _upload_provenance_journal(
                    case.service,
                    case.collection_id,
                    produced.journal_id,
                    produced.content,
                    hashlib.sha256(produced.content).hexdigest(),
                )
                binding, selected_hint = produced.binding, None
            case.service.bind_artifact_provenance(
                case.collection_id,
                CollectionArtifactProvenanceBindingBatchDocument(bindings=[binding]),
            )
            case.service.set_artifact_materialization_decisions(
                case.collection_id,
                member_materialization_decision(
                    artifact_id=binding.artifact_id,
                    materialization_hint=selected_hint,
                    allow_missing_materialization_hint=selected_hint is None,
                ),
            )
            primaries.append(binding)
            with MemberHistoryBuilder(
                artifact_id=binding.artifact_id,
                bytes=len(case.content),
                sha256=hashlib.sha256(case.content).hexdigest(),
                primary=MemberHistoryPrimary.from_mapping(
                    {
                        "journal": binding.journal.model_dump(mode="json"),
                        "delivery_association_id": binding.delivery_association_id,
                    }
                ),
            ) as histories:
                if (
                    (historical or anchor_steps or late_record_fault)
                    and index < fanout
                    and binding.journal.model_dump(mode="json") != material.terminal.to_mapping()
                ):
                    histories.add_root(MemberHistoryRoot(material.terminal, "retained"))
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
        if work is not None:
            work.phase = "preparation"
        _seal(case)
        case.claim = case.service._claim_due_finalization()
        assert case.claim is not None
        for _ in range(members + 2):
            if not case.service._stage_next_member_histories(case.collection_id):
                break
        else:
            pytest.fail("bounded shared primary history staging did not terminate")
        case.members, case.fanout = members, fanout
        case.material, case.primaries = material, primaries
        case.sources = {
            material.anchor.journal_id: material.raw,
            **{source.journal_id: source.content for source in independent.values()},
        }
        if work is not None:
            work.phase = "member"
        try:
            yield case
        finally:
            if work is not None:
                work.phase = "receiving"


@contextmanager
def _runtime_work(monkeypatch):
    from riverhog_core.services import primary_production as production_owner
    from riverhog_core.services import upload_primary_facts as primary_owner

    measured = SimpleNamespace(
        phase=None,
        metrics=defaultdict(lambda: defaultdict(int)),
        calls=[],
        samples=[],
        object_reads=[],
        extension_reads=[],
        frame_reads=[],
    )
    actual_advance = JournalValidationSession.advance
    actual_chunks = service_owner._iter_upload_journal_chunks
    actual_records = service_owner.validate_collection_production_records
    actual_completion = service_owner.validate_member_completion_requirement
    actual_frame, actual_frames = journal_store._Frames.__getitem__, journal_store._Frames.__iter__
    actual_object = HistoricalSelection.object
    actual_read = CanonicalCorpusValidator._source_read
    actual_turn = primary_owner.UploadPrimaryFactsOwner.verify
    actual_open = primary_owner.open_upload_journal_result
    actual_admission = primary_owner.prepare_upload_journal_result
    actual_member = primary_owner.verify_selected_member_binding
    actual_index_close = primary_owner._PrimaryIndex.close
    actual_production = production_owner.PrimaryProductionWork.advance
    actual_production_read = production_owner.PrimaryProductionWork._read
    actual_production_write = production_owner.PrimaryProductionWork._write
    actual_hash = production_owner.CheckpointSHA256.update
    actual_reclaim = FileReclamationOwner.reclaim
    active = []
    active_object = []
    active_production = []

    def index_close(self):
        previous = measured.phase
        measured.phase = "primary_receiving"
        values = measured.metrics[measured.phase]
        values["index_close_calls"] += 1
        paths = {self.corpus_directory, self.memo_directory}
        try:
            result = actual_index_close(self)
            assert self.corpus_scope.received and self.memo_scope.received
            assert self.workspace.received
            assert all(path.is_dir() for path in paths)
            values["database_constructions_received"] += 2
            values["namespace_leases_received"] += len(paths)
            return result
        finally:
            measured.phase = previous

    def reclaim(self, directory, *, names, octet_budget, **options):
        result = actual_reclaim(self, directory, names=names, octet_budget=octet_budget, **options)
        assert self.received and result.removed_octets <= octet_budget
        assert result.removed_files <= 1
        if measured.phase is not None:
            values = measured.metrics[measured.phase]
            values["file_reclamation_calls"] += 1
            values["reclaimed_octets"] += result.removed_octets
            values["reclaimed_files"] += result.removed_files
        return result

    def production(self, row=None):
        active_production.append(True)
        if measured.phase is not None:
            measured.metrics[measured.phase]["producer_steps"] += 1
            measured.metrics[measured.phase]["producer_extension_visits"] += int(row is not None)
        try:
            return actual_production(self, row)
        finally:
            active_production.pop()

    def production_read(self, query, values=()):
        result = actual_production_read(self, query, values)
        if measured.phase is not None:
            metrics = measured.metrics[measured.phase]
            metrics["producer_metadata_queries"] += 1
            metrics["producer_metadata_rows"] += int(result is not None)
            metrics["producer_metadata_octets"] += 0 if result is None else octets(result)
            if query.startswith("SELECT width,offset,assertion,bytes,payload"):
                metrics["producer_fragment_visits"] += int(result is not None)
        return result

    def production_write(self, query, values=()):
        if measured.phase is not None:
            measured.metrics[measured.phase]["producer_metadata_writes"] += 1
            measured.metrics[measured.phase]["producer_parameter_octets"] += octets(values)
        return actual_production_write(self, query, values)

    def record_hash(self, content):
        if active_production and measured.phase is not None:
            measured.metrics[measured.phase]["producer_record_hash_octets"] += len(content)
        return actual_hash(self, content)

    def frame_read(frame, position):
        if measured.phase is not None:
            measured.metrics[measured.phase]["accepted_frame_reads"] += 1
            measured.metrics[measured.phase]["accepted_frame_octets"] += len(frame.encoded)
            measured.frame_reads.append((measured.phase, position, len(frame.encoded)))

    def frame(self, position):
        result = actual_frame(self, position)
        if isinstance(position, int):
            frame_read(result, position)
        return result

    def frames(self):
        for position, result in enumerate(actual_frames(self)):
            frame_read(result, position)
            yield result

    def object_point(self, identity):
        active_object.append(True)
        try:
            result = actual_object(self, identity)
        finally:
            active_object.pop()
        if measured.phase is not None:
            values = measured.metrics[measured.phase]
            values["primary_object_lookups"] += 1
            values["primary_object_rows"] += int(result is not None)
            size = 0 if result is None else len(canonical_json_bytes(result))
            values["primary_object_octets"] += size
            measured.object_reads.append((measured.phase, identity, size))
        return result

    def read(self, query, parameters=()):
        if active_object and "assertion_payloads" in query:
            pytest.fail("member point selection loaded an opaque full assertion")
        result = actual_read(self, query, parameters)
        if measured.phase is not None:
            values = measured.metrics[measured.phase]
            values["native_index_queries"] += 1
            values["native_index_rows"] += int(result is not None)
            values["native_index_octets"] += 0 if result is None else octets(result)
            values["native_index_parameter_octets"] += octets(parameters)
        return result

    def selected_rows(actual, kind):
        def selected(self, *args, **kwargs):
            if measured.phase is not None:
                measured.metrics[measured.phase][kind + "_queries"] += 1
            for row in actual(self, *args, **kwargs):
                if measured.phase is not None:
                    measured.metrics[measured.phase][kind + "_rows"] += 1
                    measured.metrics[measured.phase][kind + "_octets"] += len(
                        canonical_json_bytes(row)
                    )
                    if kind == "selected_extensions":
                        measured.extension_reads.append(
                            (measured.phase, row["property"], row["assertion_id"])
                        )
                yield row

        return selected

    def count_call(actual, metric):
        def observed(*args, **kwargs):
            if measured.phase is not None:
                measured.metrics[measured.phase][metric] += 1
            return actual(*args, **kwargs)

        return observed

    def turn(self, *args, **kwargs):
        before = dict(measured.metrics[measured.phase])
        source_before = dict(self.metrics)
        session, upload = args

        def catalog_query(_connection, _cursor, _statement, parameters, _context, _many):
            values = measured.metrics[measured.phase]
            values["primary_catalog_statements"] += 1
            values["primary_catalog_parameter_octets"] += octets(parameters)

        engine = session.get_bind()
        event.listen(engine, "before_cursor_execute", catalog_query)
        try:
            result = actual_turn(self, *args, **kwargs)
        finally:
            event.remove(engine, "before_cursor_execute", catalog_query)
        after = measured.metrics[measured.phase]
        delta = {key: after[key] - before.get(key, 0) for key in after}
        selected = {
            "source_frame_reads": delta.get("accepted_frame_reads", 0),
            "source_frame_octets": delta.get("accepted_frame_octets", 0),
            "native_validation_advances": delta.get("native_advances", 0),
            "source_payload_octets": delta.get("catalog_source_octets", 0),
            "native_openings": delta.get("primary_native_openings", 0),
            "source_admissions": delta.get("primary_source_admissions", 0),
            "member_checks": delta.get("primary_member_checks", 0),
            "producer_steps": delta.get("producer_steps", 0),
        }
        assert selected["source_frame_reads"] == (
            self.metrics["source_frame_reads"] - source_before["source_frame_reads"]
        )
        assert selected["source_frame_octets"] == (
            self.metrics["source_frame_octets"] - source_before["source_frame_octets"]
        )
        binding = growth_binding(actual_turn)
        projection = project_bindings((binding,))
        journal = session.get(Journal, (upload.collection_id, kwargs["binding"].journal.journal_id))
        sample = {
            "declaration": binding.declaration.id,
            "declaration_sha256": hashlib.sha256(
                canonical_json_bytes(projection["declarations"][binding.declaration.id])
            ).hexdigest(),
            "dimensions": {
                "source_frames": journal.validation_sequence,
                "source_octets": journal.bytes,
            },
            "metrics": selected,
            "delegates": {},
        }
        validate_execution_samples(projection, binding.declaration.id, [sample])
        measured.samples.append(sample)
        return result

    def advance(*args, **kwargs):
        if measured.phase is not None:
            measured.metrics[measured.phase]["native_advances"] += 1
            if active:
                active[-1]["native_advances"] += 1
        return actual_advance(*args, **kwargs)

    def chunks(*args, **kwargs):
        for content in actual_chunks(*args, **kwargs):
            if measured.phase is not None:
                measured.metrics[measured.phase]["catalog_source_octets"] += len(content)
            yield content

    def records(*args, **kwargs):
        if measured.phase is not None:
            measured.metrics[measured.phase]["producer_record_checks"] += 1
        return actual_records(*args, **kwargs)

    def completion(*args, **kwargs):
        if measured.phase is not None:
            measured.metrics[measured.phase]["member_completion_checks"] += 1
        return actual_completion(*args, **kwargs)

    def validation(actual, owner):
        def observed(chunks, **options):
            row = {
                "phase": measured.phase,
                "owner": owner,
                "anchor": options.get("expected_anchor"),
                "input_octets": 0,
                "native_advances": 0,
            }

            def counted():
                for content in chunks:
                    row["input_octets"] += len(content)
                    yield content

            active.append(row)
            try:
                result = actual(counted(), **options)
                row["journal_id"] = result.journal_id
                measured.calls.append(row)
                return result
            finally:
                active.pop()

        return observed

    with monkeypatch.context() as patch:
        patch.setattr(JournalValidationSession, "advance", advance)
        patch.setattr(service_owner, "_iter_upload_journal_chunks", chunks)
        patch.setattr(service_owner, "validate_collection_production_records", records)
        patch.setattr(service_owner, "validate_member_completion_requirement", completion)
        patch.setattr(journal_store._Frames, "__getitem__", frame)
        patch.setattr(journal_store._Frames, "__iter__", frames)
        patch.setattr(HistoricalSelection, "object", object_point)
        patch.setattr(CanonicalCorpusValidator, "_source_read", read)
        patch.setattr(
            HistoricalSelection,
            "extensions",
            selected_rows(HistoricalSelection.extensions, "selected_extensions"),
        )
        patch.setattr(
            HistoricalSelection,
            "journal_subjects",
            selected_rows(HistoricalSelection.journal_subjects, "selected_journal_subjects"),
        )
        patch.setattr(primary_owner.UploadPrimaryFactsOwner, "verify", turn)
        patch.setattr(primary_owner._PrimaryIndex, "close", index_close)
        patch.setattr(production_owner.PrimaryProductionWork, "advance", production)
        patch.setattr(production_owner.PrimaryProductionWork, "_read", production_read)
        patch.setattr(production_owner.PrimaryProductionWork, "_write", production_write)
        patch.setattr(production_owner.CheckpointSHA256, "update", record_hash)
        patch.setattr(FileReclamationOwner, "reclaim", reclaim)
        patch.setattr(
            primary_owner,
            "open_upload_journal_result",
            count_call(actual_open, "primary_native_openings"),
        )
        patch.setattr(
            primary_owner,
            "prepare_upload_journal_result",
            count_call(actual_admission, "primary_source_admissions"),
        )
        patch.setattr(
            primary_owner,
            "verify_selected_member_binding",
            count_call(actual_member, "primary_member_checks"),
        )
        for module, label in ((service_owner, "finalizer"), (history_owner, "history")):
            patch.setattr(
                module, "validate_journal_chunks", validation(module.validate_journal_chunks, label)
            )
        yield measured


def _next_member(case):
    before = _state(case)[0]
    owners = []
    for _ in range(64 * (case.members + 1)):
        owner = _owner(case)
        owners.append(owner)
        assert owner.advance()
        assert not owner.has_pending and not owner.result_resources.pending
        after = _state(case)
        assert after[1:] == (False, None)
        if after[0] != before:
            assert after[0] == before + 1
            return owners
    pytest.fail("small shared primary fixture did not finish bounded source preparation")


def _finish(case):
    owners = []
    for _ in range(64 * (case.members + 1)):
        owner = _owner(case)
        owners.append(owner)
        if not owner.advance():
            break
    else:
        pytest.fail("small shared primary fixture did not complete closure")
    assert _state(case) == (case.members, True, None)
    assert all(not owner.has_pending and not owner.result_resources.pending for owner in owners)
    return owners


def _reclaim_primary(case):
    """Intentionally drain this exact registered namespace through actual workers."""
    from tests.unit.test_primary_facts_lifecycle import finish_scheduled

    with session_scope(case.factory) as session:
        job = primary_facts_jobs.find_pending(session, case.collection_id)
    assert job is not None
    assert job.state in primary_facts_jobs.RETIRED_STATES or (
        job.owner_id != case.service._primary_facts.owner_id
    )
    root = case.service._journal_validation_owner.root
    finish_scheduled(
        SimpleNamespace(factory=case.factory, root=root),
        case.service._primary_facts_retirement,
        case.service._archive_execution,
        (job.workspace_id,),
    )
    assert not (root / job.workspace_id).exists()


@pytest.mark.parametrize("fanout", [1, 4])
@pytest.mark.parametrize(
    "text_octets,opaque_octets", [(0, 0), (4096, 0), (0, 4096)], ids=["base", "producer", "opaque"]
)
@pytest.mark.growth_witness(
    "riverhog-shared-primary-member-turn-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_primary_facts.UploadPrimaryFactsOwner.verify",
)
def test_real_member_turns_share_primary_admission_with_independent_record_extent(
    tmp_path, monkeypatch, record_property, fanout, text_octets, opaque_octets
):
    with (
        _runtime_work(monkeypatch) as work,
        shared_primary_finalization(
            tmp_path,
            monkeypatch,
            fanout=fanout,
            text_octets=text_octets,
            opaque_octets=opaque_octets,
            work=work,
        ) as case,
    ):
        owners = []
        for _ in range(case.members):
            owners.extend(_next_member(case))
        assert _state(case) == (case.members, False, None)
        primary_owner = case.service._primary_facts
        preparation_paths = {
            path
            for index in primary_owner._owners.values()
            for path in (index.corpus_directory, index.memo_directory)
        }
        assert len(preparation_paths) == 1
        assert all(path.is_dir() for path in preparation_paths)
        member_work = work.metrics["member"]
        assert member_work["native_advances"] == member_work["catalog_source_octets"] == 0
        assert member_work["accepted_frame_reads"] == sum(
            source.count(b"\x1e") for source in case.sources.values()
        )
        assert member_work["accepted_frame_octets"] == sum(map(len, case.sources.values()))
        assert member_work["primary_member_checks"] == case.members
        assert member_work["producer_extension_visits"] == 3 * len(case.sources)
        assert member_work["producer_fragment_visits"] == len(case.sources)
        assert member_work["producer_record_hash_octets"] == (
            case.material.source_context_octets
            + (len(case.sources) - 1) * len(b'{"documentary":""}')
        )
        assert member_work["primary_object_lookups"] <= 8 * case.members
        assert all(size < 2048 for phase, _identity, size in work.object_reads if phase == "member")
        production_properties = {
            COLLECTION_PRODUCTION_CONTRACT_ID + "/" + name
            for name in ("producer", "record-manifest", "record-fragment")
        }
        production_rows = [
            identity
            for phase, property_name, identity in work.extension_reads
            if phase == "member" and property_name in production_properties
        ]
        assert len(production_rows) == len(set(production_rows)) == 3 * len(case.sources)
        record_property("growth_samples", work.samples)
        work.phase = "terminal"
        owners.extend(_finish(case))
        assert not primary_owner.pending
        assert all(path.is_dir() for path in preparation_paths)
        receiving = work.metrics["primary_receiving"]
        assert receiving["index_close_calls"] == 1
        assert receiving["database_constructions_received"] == 2
        assert receiving["namespace_leases_received"] == len(preparation_paths)
        assert receiving["accepted_frame_reads"] == receiving["catalog_source_octets"] == 0
        assert receiving["native_advances"] == 0
        work.phase = "primary_reclamation"
        _reclaim_primary(case)
        assert all(not path.exists() for path in preparation_paths)
        reclamation = work.metrics["primary_reclamation"]
        assert reclamation["file_reclamation_calls"] >= 2
        assert reclamation["reclaimed_files"] == 2 and reclamation["reclaimed_octets"] > 0
        assert reclamation["accepted_frame_reads"] == reclamation["catalog_source_octets"] == 0
        assert reclamation["native_advances"] == 0
        assert all(not owner.has_pending and not owner.result_resources.pending for owner in owners)
        shared = [row for row in work.calls if row["journal_id"] == case.material.anchor.journal_id]
        terminal = [row for row in shared if row["phase"] == "terminal"]
        assert [(row["owner"], row["input_octets"]) for row in terminal] == [
            ("finalizer", len(case.material.raw)),
            ("history", len(case.material.prefix)),
        ]
        assert all(row["native_advances"] > 0 for row in terminal)
        record_property(
            "growth_observations",
            {
                "members": case.members,
                "shared_fanout": fanout,
                "distinct_staged_journals": case.members - fanout + 1,
                "shared_prefix_octets": len(case.material.prefix),
                "producer_record_octets": case.material.source_context_octets,
                "opaque_occurrence_and_observation_octets": 2 * opaque_octets,
                "phase_work": {phase: dict(values) for phase, values in work.metrics.items()},
                "shared_validation_calls": shared,
                "primary_scratch_received": all(not path.exists() for path in preparation_paths),
            },
        )
    assert not case.service._primary_facts.pending
    storage = case.service._journal_storage.status
    assert not (storage.connections or storage.cursors or storage.blobs or storage.operations)


@pytest.mark.parametrize("hint", [None, ("exact", "selected-name")])
def test_shared_primary_uses_the_selected_prefix_before_a_later_delivery_correction(
    tmp_path, monkeypatch, hint
):
    with shared_primary_finalization(
        tmp_path, monkeypatch, members=2, fanout=2, historical=True, hint=hint
    ) as case:
        assert case.material.anchor != case.material.terminal
        with validate_journal(
            case.material.raw, catalog=service_owner.admission_provenance_catalog()
        ) as complete:
            assert (
                case.primaries[0].delivery_association_id not in complete.graph_validation.objects
            )
        _finish(case)


@pytest.mark.growth_witness(
    "riverhog-shared-primary-cross-prefix-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_primary_facts.UploadPrimaryFactsOwner.verify",
)
def test_distinct_primary_anchors_share_unchanged_selected_producer_facts(
    tmp_path, monkeypatch, record_property
):
    with (
        _runtime_work(monkeypatch) as work,
        shared_primary_finalization(
            tmp_path,
            monkeypatch,
            members=4,
            fanout=4,
            text_octets=4096,
            anchor_steps=True,
            work=work,
        ) as case,
    ):
        assert len({binding.journal.prefix_sha256 for binding in case.primaries}) == case.members
        for _ in range(case.members):
            _next_member(case)
        metrics = work.metrics["member"]
        assert metrics["catalog_source_octets"] == metrics["native_advances"] == 0
        assert metrics["accepted_frame_octets"] == len(case.material.raw)
        assert metrics["accepted_frame_reads"] == case.material.raw.count(b"\x1e")
        assert metrics["producer_extension_visits"] == 3
        assert metrics["producer_fragment_visits"] == 1
        assert metrics["producer_record_hash_octets"] == case.material.source_context_octets
        assert metrics["primary_member_checks"] == case.members
        record_property("growth_samples", work.samples)
        record_property(
            "growth_observations",
            {
                "members": case.members,
                "distinct_primary_anchors": len(case.material.selections),
                "source_octets": len(case.material.raw),
                "producer_record_octets": case.material.source_context_octets,
                "member_and_preparation_work": dict(metrics),
            },
        )
        work.phase = "terminal"
        _finish(case)


def test_later_primary_correction_invalidates_earlier_producer_success(tmp_path, monkeypatch):
    with shared_primary_finalization(
        tmp_path, monkeypatch, members=2, fanout=2, late_record_fault=True
    ) as case:
        assert case.primaries[0].journal != case.primaries[1].journal
        _next_member(case)
        with pytest.raises(ProvenanceValidationError, match="producer"):
            _next_member(case)
        assert _state(case) == (1, False, None)


@pytest.mark.parametrize(
    "fault",
    ["association", "slot", "size", "digest", "context", "hint", "omission", "completion"],
)
def test_shared_primary_keeps_each_members_exact_checks(tmp_path, monkeypatch, fault):
    with shared_primary_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        _next_member(case)
        artifact_id = format(2, "064x")
        with session_scope(case.factory) as session:
            binding = session.get(Binding, (case.collection_id, artifact_id))
            member = session.get(Artifact, (case.collection_id, artifact_id))
            if fault == "association":
                binding.delivery_association_id = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"
            elif fault == "slot":
                binding.delivery_association_id = case.primaries[0].delivery_association_id
            elif fault == "size":
                member.bytes += 1
            elif fault == "digest":
                member.sha256 = "0" * 64
            elif fault == "context":
                session.get(
                    Upload, case.collection_id
                ).delivery_context_id = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"
            elif fault == "completion":
                requirement = CollectionCompletionRequirementDocument(
                    execution_id="1" * 64,
                    execution_envelope_sha256="2" * 64,
                    controller_evidence_sha256="3" * 64,
                    record_kinds=tuple(sorted(COMPLETION_REQUIRED_RECORD_KINDS)),
                )
                session.get(
                    Upload, case.collection_id
                ).completion_requirement_json = requirement.model_dump_json()
            elif fault == "omission":
                session.delete(session.get(Decision, (case.collection_id, artifact_id)))
            else:
                decision = session.get(Decision, (case.collection_id, artifact_id))
                decision.hint_json = '{"components":["not-the-selected-hint"]}'
                decision.allow_missing_materialization_hint = False
        with pytest.raises((Conflict, ProvenanceValidationError, ValueError)):
            _next_member(case)
        assert _state(case) == (1, False, None)


@pytest.mark.parametrize("record_fault", ["missing-fragment", "altered-fragment"])
def test_shared_primary_keeps_exact_required_producer_preimages(
    tmp_path, monkeypatch, record_fault
):
    with shared_primary_finalization(
        tmp_path, monkeypatch, members=2, fanout=2, record_fault=record_fault
    ) as case:
        with pytest.raises(ProvenanceValidationError, match="producer"):
            _next_member(case)
        assert _state(case) == (0, False, None)


@pytest.mark.parametrize("field", ["capture_attempt_id", "capture_version", "job_version"])
def test_shared_primary_rechecks_current_source_and_job_after_a_previous_member(
    tmp_path, monkeypatch, field
):
    with shared_primary_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        _next_member(case)
        with session_scope(case.factory) as session:
            journal = session.get(Journal, (case.collection_id, case.material.anchor.journal_id))
            if field == "job_version":
                job = session.scalar(
                    select(Job).where(
                        Job.collection_id == case.collection_id,
                        Job.journal_id == case.material.anchor.journal_id,
                    )
                )
                job.version = "e" * 64
            elif field == "capture_version":
                journal.capture_version += 1
            else:
                setattr(journal, field, "e" * 64)
        with pytest.raises(
            (
                Conflict,
                ProvenanceValidationError,
                ValueError,
                KeyError,
                LostJournalValidationWorkspace,
            )
        ):
            _next_member(case)
        assert _state(case) == (1, False, None)


def test_shared_primary_rejects_substitution_of_its_registered_namespace_original(
    tmp_path, monkeypatch
):
    from riverhog_core.primary_facts_workspace import DATA_FILES

    with shared_primary_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        _next_member(case)
        preparation = case.service._primary_facts
        original = preparation._owners[case.collection_id]
        path = original.workspace.path
        assert original.ready and original.workspace.custody.verify_current()

        def contents():
            return {
                name: (path / name).read_bytes() for name in DATA_FILES if (path / name).exists()
            }

        before = contents()
        with session_scope(case.factory) as session:
            job = primary_facts_jobs.find_pending(session, case.collection_id)
            row = session.get(PrimaryJob, job.workspace_id)
            row.epoch = "f" * 64
            row.construction = job.construction + " "
        turn = _owner(case)
        try:
            with pytest.raises(Conflict, match="registered original identity"):
                turn.advance()
            assert _state(case) == (1, False, None)
            assert preparation._owners[case.collection_id] is original
            assert original.ready and not original.closing
            assert not original.workspace.received
            assert original.workspace.custody.verify_current()
            assert original.corpus_scope.borrow.active
            assert case.service._journal_storage.status.connections == 2
            assert contents() == before
        finally:
            with session_scope(case.factory) as session:
                row = session.get(PrimaryJob, job.workspace_id)
                row.epoch, row.construction = job.epoch, job.construction
        assert turn.advance()
        assert not turn.has_pending and not turn.result_resources.pending
        assert _state(case) == (2, False, None)
        _finish(case)
        _reclaim_primary(case)


def test_shared_primary_restart_uses_current_identity_and_complete_terminal_barrier(
    tmp_path, monkeypatch
):
    with shared_primary_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        _next_member(case)
        previous = case.service
        previous.close()
        current = service_owner.SqlAlchemyCollectionUploadService(
            previous._config,
            previous._archive_stores,
            policy=previous._policy,
            session_factory=case.factory,
        )
        case.service = current
        try:
            _reclaim_primary(case)
            _finish(case)
        finally:
            current.close()


@pytest.mark.parametrize("committed", [False, True])
def test_shared_primary_keeps_the_original_catalog_result_after_lost_acknowledgement(
    tmp_path, monkeypatch, committed
):
    with shared_primary_finalization(tmp_path, monkeypatch, members=3, fanout=3) as case:
        _next_member(case)
        owner = _owner(case)
        with _lose_commit_reply(monkeypatch, committed=committed):
            with pytest.raises(OSError, match="catalog commit acknowledgement"):
                owner.advance()
        assert owner.has_pending
        assert _state(case) == (1 + int(committed), False, None)
        assert owner.advance()
        assert not owner.has_pending and not owner.result_resources.pending
        assert _state(case) == (2, False, None)
        _finish(case)


def test_shared_primary_retries_actual_workspace_capacity_with_the_same_original_turn(
    tmp_path, monkeypatch
):
    with shared_primary_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        _next_member(case)
        with session_scope(case.factory) as session:
            job = session.scalar(
                select(Job).where(
                    Job.collection_id == case.collection_id,
                    Job.journal_id == case.material.anchor.journal_id,
                )
            )
            workspace_id = job.workspace_id
        root = case.service._journal_validation_owner.root
        owner = _owner(case)
        with leased_workspace(root, workspace_id) as held:
            assert held is not None
            with pytest.raises(JournalValidationCapacity):
                owner.advance()
        assert _state(case) == (1, False, None)
        assert owner.advance()
        assert not owner.has_pending and not owner.result_resources.pending
        assert _state(case) == (2, False, None)
        with leased_workspace(root, workspace_id) as available:
            assert available is not None
        _finish(case)


def test_refused_first_primary_open_returns_empty_index_capacity_before_retry(
    tmp_path, monkeypatch
):
    from riverhog_core.services import upload_primary_facts as primary_owner

    with shared_primary_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        storage = case.service._journal_storage
        assert storage.status.connections == 0
        # Registration and actual namespace pinning each commit without either
        # database. The next fresh catalog turn exercises real SQLite pressure.
        for _ in range(2):
            metadata = _owner(case)
            assert metadata.advance()
            assert not metadata.has_pending and not metadata.result_resources.pending
            assert storage.status.connections == 0 and _state(case) == (0, False, None)
        with session_scope(case.factory) as session:
            assert primary_facts_jobs.find_pending(session, case.collection_id).state == "pinned"
        originals = [storage.journal() for _ in range(storage.status.connection_limit - 2)]
        closed_paths = []
        actual_close = primary_owner._PrimaryIndex.close

        def close(index):
            closed_paths.extend(
                path for path in (index.corpus_directory, index.memo_directory) if path is not None
            )
            return actual_close(index)

        monkeypatch.setattr(primary_owner._PrimaryIndex, "close", close)
        owner = _owner(case)
        try:
            for original in originals:
                original.scope.reserve()
            assert storage.status.connections == len(originals)
            assert owner.advance()
            assert _state(case) == (0, False, None)
            assert not case.service._primary_facts.pending
            assert not owner.has_pending and not owner.result_resources.pending
            assert storage.status.connections == len(originals)
            assert closed_paths and all(path.is_dir() for path in closed_paths)
            _reclaim_primary(case)
            assert closed_paths and all(not path.exists() for path in closed_paths)
        finally:
            for original in reversed(originals):
                assert original.close_step()
        assert storage.status.connections == 0
        owner = _owner(case)
        assert owner.advance()
        assert _state(case) == (0, False, None)
        assert not owner.has_pending
        _finish(case)


@pytest.mark.parametrize("after", [False, True])
def test_shared_primary_retains_original_native_receiving_across_uncertain_close(
    tmp_path, monkeypatch, after
):
    from riverhog_core.services import upload_primary_facts as primary_owner

    with shared_primary_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        _next_member(case)
        actual = primary_owner.open_upload_journal_result
        originals, callbacks = [], []

        def observed(*args, **kwargs):
            result = actual(*args, **kwargs)
            original = kwargs["receiver"]
            originals.append(original)
            receive = result._store._storage.on_close

            def uncertain():
                callbacks.append(1)
                if len(callbacks) == 1 and not after:
                    raise InterruptedError("shared primary original close is unconfirmed")
                receive()
                if len(callbacks) == 1:
                    raise InterruptedError("shared primary original close reply was lost")

            result._store._storage.on_close = uncertain
            return result

        monkeypatch.setattr(primary_owner, "open_upload_journal_result", observed)
        owner = _owner(case)
        with pytest.raises(InterruptedError, match="shared primary original close"):
            owner.advance()
        assert _state(case) == (1, False, None)
        assert owner.has_pending and owner.result_resources.pending
        assert len(originals) == len(callbacks) == 1
        original = originals[0]
        assert original.lease.received is after
        owner.result_resources.close()
        assert len(originals) == 1 and len(callbacks) == 2
        assert not owner.result_resources.pending
        assert (
            original.result is None and original.lease.received and original.construction.received
        )
        assert owner.advance()
        assert len(originals) == 2 and not owner.has_pending
        assert _state(case) == (2, False, None)
        _finish(case)


def test_decided_terminal_receives_primary_preparation_after_original_writers(
    tmp_path, monkeypatch
):
    from tests.unit.test_archive_finalizer_registry import _operation, _scope
    from tests.unit.test_archive_upload_terminal_adjudication import _decision, _result
    from tests.unit.test_archive_upload_terminal_retirement import _retire

    with shared_primary_finalization(tmp_path, monkeypatch, members=2, fanout=2) as case:
        _next_member(case)
        primary = case.service._primary_facts
        paths = {
            path
            for index in primary._owners.values()
            for path in (index.corpus_directory, index.memo_directory)
        }
        assert primary.pending and all(path.is_dir() for path in paths)
        retirement = case.service._archive_terminal_retirement
        receive = retirement._retire_preparation
        key, target = _scope(case)
        operation = _operation(case)
        calls = []

        def receive_preparation(session, collection_id):
            assert collection_id == case.collection_id
            for registry in (case.service._archive_finalizers, case.service._archive_invocations):
                assert (
                    registry.local_terminal_status(
                        key, target_incarnation=target, operation_id=operation
                    )
                    == "untracked"
                )
            calls.append(primary.pending)
            return receive(session, collection_id)

        monkeypatch.setattr(retirement, "_retire_preparation", receive_preparation)
        _result(case, operation)
        waiting = _retire(case, operation)
        assert waiting.pending_reason == "global-decision"
        assert primary.pending and not calls
        decision = _decision(case, operation)
        assert decision.phase in {"refused-sealed", "needs-reconfirmation", "cleanup-ready"}
        assert decision.pending_reason is None
        retired = _retire(case, operation)
        assert retired.pending_reason is None
        assert retired.execution_stopped is (
            decision.phase in {"refused-sealed", "needs-reconfirmation"}
        )
        assert calls == [True] and not primary.pending
        assert all(path.is_dir() for path in paths)
        _reclaim_primary(case)
        assert all(not path.exists() for path in paths)
        assert _state(case) == (1, False, None)


def test_shared_primary_current_raw_tampering_cannot_cross_terminal_acceptance(
    tmp_path, monkeypatch
):
    with shared_primary_finalization(
        tmp_path, monkeypatch, members=2, fanout=2, text_octets=32
    ) as case:
        _next_member(case)
        with session_scope(case.factory) as session:
            journal = session.get(Journal, (case.collection_id, case.material.anchor.journal_id))
            before = tuple(getattr(journal, column.key) for column in Journal.__table__.columns)
            chunks = session.scalars(
                select(Chunk).where(
                    Chunk.collection_id == case.collection_id,
                    Chunk.journal_id == case.material.anchor.journal_id,
                )
            ).all()
            target = next(row for row in chunks if b"shared-primary-test" in bytes(row.content))
            original = bytes(target.content)
            target.content = original.replace(b"shared-primary-test", b"shared-primary-fail", 1)
            changed_key = target.ordinal
        with session_scope(case.factory) as session:
            journal = session.get(Journal, (case.collection_id, case.material.anchor.journal_id))
            assert (
                tuple(getattr(journal, column.key) for column in Journal.__table__.columns)
                == before
            )
        _next_member(case)
        assert _state(case) == (2, False, None)
        with pytest.raises((ProvenanceValidationError, Conflict)):
            _owner(case).advance()
        assert _state(case) == (2, False, None)
        with session_scope(case.factory) as session:
            session.get(
                Chunk, (case.collection_id, case.material.anchor.journal_id, changed_key)
            ).content = original
        _finish(case)
